"""
异步批量写入队列模块 (I-06)
支持合并写入和异步刷新的高效写入队列
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Callable
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
    from memory_system.core_abstractions.memory_data_models import MemoryItem

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class BatchWriteItem:
    """批量写入项"""
    item: Any  # MemoryItem 或类似对象
    future: Optional[asyncio.Future] = None
    timestamp: float = field(default_factory=time.time)


class AsyncBatchWriter:
    """
    异步批量写入队列

    功能:
        - 异步批量写入，减少I/O开销
        - 支持合并相似写入请求
        - 定时刷新和容量触发刷新
        - 支持写入确认（通过Future）
        - 优雅关闭时确保数据不丢失

    使用示例:
        >>> writer = AsyncBatchWriter(storage, max_batch_size=100, flush_interval=1.0)
        >>> await writer.start()
        >>> await writer.enqueue(memory_item)
        >>> await writer.stop()  # 确保所有数据写入
    """

    def __init__(
        self,
        storage: "StorageBackend",
        max_batch_size: int = 100,
        flush_interval: float = 1.0,
        enable_merge: bool = True,
        merge_window_seconds: float = 0.5,
        on_flush_error: Optional[Callable[[Exception, List[Any]], None]] = None,
    ):
        """
        初始化批量写入器

        Args:
            storage: 存储后端
            max_batch_size: 最大批量大小
            flush_interval: 刷新间隔（秒）
            enable_merge: 是否启用写入合并
            merge_window_seconds: 合并窗口时间（秒）
            on_flush_error: 刷新错误回调函数
        """
        self.storage = storage
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval
        self.enable_merge = enable_merge
        self.merge_window_seconds = merge_window_seconds
        self.on_flush_error = on_flush_error

        self._queue: asyncio.Queue[BatchWriteItem] = asyncio.Queue()
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._stats = {
            "total_enqueued": 0,
            "total_flushed": 0,
            "total_batches": 0,
            "total_errors": 0,
            "merge_hits": 0,
        }
        self._stats_lock = asyncio.Lock()

    async def start(self) -> None:
        """启动批量写入服务"""
        if self._running:
            return

        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info(f"异步批量写入器已启动，刷新间隔: {self.flush_interval}s, 批量大小: {self.max_batch_size}")

    async def stop(self, timeout: float = 30.0) -> None:
        """
        停止批量写入服务，确保所有数据写入

        Args:
            timeout: 最终刷新超时时间
        """
        if not self._running:
            return

        self._running = False

        # 取消刷新循环
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await asyncio.wait_for(self._flush_task, timeout=1.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # 最终刷新
        try:
            await asyncio.wait_for(self._flush_batch(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("批量写入器停止时刷新超时")

        logger.info("异步批量写入器已停止")

    async def enqueue(self, item: Any) -> Optional[asyncio.Future]:
        """
        将项目加入写入队列

        Args:
            item: 要写入的记忆项

        Returns:
            Future对象，可用于等待写入完成
        """
        future = asyncio.get_event_loop().create_future()
        batch_item = BatchWriteItem(item=item, future=future)

        await self._queue.put(batch_item)

        async with self._stats_lock:
            self._stats["total_enqueued"] += 1

        # 如果队列已满，立即触发刷新
        if self._queue.qsize() >= self.max_batch_size:
            asyncio.create_task(self._flush_batch())

        return future

    async def enqueue_batch(self, items: List[Any]) -> List[asyncio.Future]:
        """
        批量加入写入队列

        Args:
            items: 要写入的记忆项列表

        Returns:
            Future对象列表
        """
        futures = []
        for item in items:
            future = await self.enqueue(item)
            futures.append(future)
        return futures

    async def _flush_loop(self) -> None:
        """后台刷新循环"""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                if not self._queue.empty():
                    await self._flush_batch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"批量写入循环错误: {e}")

    async def _flush_batch(self) -> None:
        """执行批量刷新"""
        batch = []
        futures = []

        # 收集待写入项目
        while len(batch) < self.max_batch_size and not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                batch.append(item.item)
                if item.future:
                    futures.append(item.future)
            except asyncio.QueueEmpty:
                break

        if not batch:
            return

        # 如果启用合并，尝试合并相似项目
        if self.enable_merge and len(batch) > 1:
            batch, merged_count = self._merge_similar_items(batch)
            if merged_count > 0:
                async with self._stats_lock:
                    self._stats["merge_hits"] += merged_count

        # 执行批量写入
        try:
            start_time = time.time()

            # 调用存储的批量添加方法
            if hasattr(self.storage, 'batch_add'):
                await self.storage.batch_add(batch)
            else:
                # 降级为逐个写入
                for item in batch:
                    await self.storage.add(item)

            duration = time.time() - start_time

            # 通知所有等待的Future
            for future in futures:
                if not future.done():
                    future.set_result(True)

            async with self._stats_lock:
                self._stats["total_flushed"] += len(batch)
                self._stats["total_batches"] += 1

            logger.debug(f"批量写入完成: {len(batch)} 条, 耗时: {duration:.3f}s")

        except Exception as e:
            logger.error(f"批量写入失败: {e}")

            async with self._stats_lock:
                self._stats["total_errors"] += 1

            # 通知所有等待的Future失败
            for future in futures:
                if not future.done():
                    future.set_exception(e)

            # 调用错误回调
            if self.on_flush_error:
                try:
                    if asyncio.iscoroutinefunction(self.on_flush_error):
                        await self.on_flush_error(e, batch)
                    else:
                        self.on_flush_error(e, batch)
                except Exception as cb_error:
                    logger.warning(f"批量写入错误回调失败: {cb_error}")

    def _merge_similar_items(self, items: List[Any]) -> tuple:
        """
        合并相似的记忆项

        合并策略:
            - 相同内容的记忆合并为一个
            - 相似度极高的记忆合并（可选）

        Args:
            items: 待合并的项目列表

        Returns:
            (合并后的列表, 合并数量)
        """
        if not items:
            return items, 0

        # 使用内容作为键进行去重
        seen_content: Dict[str, Any] = {}
        merged = []
        merge_count = 0

        for item in items:
            content = getattr(item, 'content', str(item))
            if content in seen_content:
                # 合并：保留重要性更高的
                existing = seen_content[content]
                existing_importance = getattr(existing, 'importance', 0)
                new_importance = getattr(item, 'importance', 0)

                if hasattr(new_importance, 'value'):
                    new_importance = new_importance.value
                if hasattr(existing_importance, 'value'):
                    existing_importance = existing_importance.value

                if new_importance > existing_importance:
                    seen_content[content] = item
                merge_count += 1
            else:
                seen_content[content] = item
                merged.append(item)

        return merged, merge_count

    async def flush_now(self) -> Dict[str, Any]:
        """
        立即刷新所有待写入数据

        Returns:
            刷新统计
        """
        flushed = 0
        while not self._queue.empty():
            await self._flush_batch()
            flushed += 1
        return {"flushed_batches": flushed}

    def get_stats(self) -> Dict[str, Any]:
        """获取写入统计信息"""
        return {
            **self._stats,
            "queue_size": self._queue.qsize(),
            "is_running": self._running,
        }

    def get_queue_size(self) -> int:
        """获取当前队列大小"""
        return self._queue.qsize()

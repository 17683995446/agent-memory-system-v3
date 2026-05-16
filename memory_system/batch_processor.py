"""通用批处理框架

Agent Memory System V2.29.0 重构项目 - R-11
重构批处理抽象层，提供通用的批处理框架
"""

from typing import TypeVar, Generic, Callable, List, Optional, Dict, Any, AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import time
from collections import deque

from memory_system.exceptions import BatchProcessingError
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class BatchStatus(Enum):
    """批处理状态"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BatchItem(Generic[T]):
    """批处理项"""
    item_id: str
    data: T
    status: BatchStatus = BatchStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    processed_at: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "processed_at": self.processed_at,
            "wait_time": (self.processed_at or time.time()) - self.created_at
        }


@dataclass
class BatchResult(Generic[R]):
    """批处理结果"""
    successful: List[R] = field(default_factory=list)
    failed: List[Dict[str, Any]] = field(default_factory=list)
    total_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    processing_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_count": self.total_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": self.success_count / self.total_count if self.total_count > 0 else 0,
            "processing_time_ms": round(self.processing_time * 1000, 2),
            "failed_items": self.failed
        }


class BatchProcessor(Generic[T, R]):
    """通用批处理框架
    
    支持异步批处理、超时控制、错误处理和背压机制。
    
    Example:
        async def process_batch(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        processor = BatchProcessor(process_batch, batch_size=100)
        await processor.start()
        
        future = await processor.submit("hello")
        result = await future
        
        await processor.stop()
    """
    
    def __init__(
        self,
        processor: Callable[[List[T]], List[R]],
        batch_size: int = 100,
        max_wait_time: float = 1.0,
        max_concurrent_batches: int = 3,
        continue_on_error: bool = True,
        error_handler: Optional[Callable[[T, Exception], None]] = None
    ):
        """
        Args:
            processor: 批处理函数，接收List[T]返回List[R]
            batch_size: 批处理大小
            max_wait_time: 最大等待时间（秒）
            max_concurrent_batches: 最大并发批次数
            continue_on_error: 出错时是否继续处理
            error_handler: 错误处理回调
        """
        self.processor = processor
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.max_concurrent_batches = max_concurrent_batches
        self.continue_on_error = continue_on_error
        self.error_handler = error_handler
        
        self._queue: asyncio.Queue = asyncio.Queue()
        self._results: Dict[str, asyncio.Future] = {}
        self._items: Dict[str, BatchItem] = {}
        self._task: Optional[asyncio.Task] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._running = False
        self._stats = {
            "total_processed": 0,
            "total_failed": 0,
            "batches_processed": 0,
            "total_wait_time": 0.0
        }
    
    async def start(self):
        """启动批处理器"""
        if self._running:
            return
        
        self._running = True
        self._semaphore = asyncio.Semaphore(self.max_concurrent_batches)
        self._task = asyncio.create_task(self._process_loop())
        logger.info(f"BatchProcessor started (batch_size={self.batch_size})")
    
    async def stop(self):
        """停止批处理器"""
        if not self._running:
            return
        
        self._running = False
        
        # 取消处理循环
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        # 取消所有待处理的future
        for future in self._results.values():
            if not future.done():
                future.cancel()
        
        logger.info("BatchProcessor stopped")
    
    async def submit(self, item: T, item_id: Optional[str] = None) -> asyncio.Future:
        """提交单个项目处理
        
        Args:
            item: 要处理的项目
            item_id: 项目ID，None则自动生成
            
        Returns:
            Future对象，可以通过await获取结果
        """
        if not self._running:
            raise BatchProcessingError("BatchProcessor is not running")
        
        item_id = item_id or f"item_{id(item)}_{time.time()}"
        batch_item = BatchItem(item_id=item_id, data=item)
        
        future = asyncio.get_event_loop().create_future()
        self._results[item_id] = future
        self._items[item_id] = batch_item
        
        await self._queue.put(batch_item)
        return future
    
    async def submit_batch(self, items: List[T]) -> List[asyncio.Future]:
        """批量提交项目
        
        Args:
            items: 要处理的项目列表
            
        Returns:
            Future对象列表
        """
        futures = []
        for item in items:
            future = await self.submit(item)
            futures.append(future)
        return futures
    
    async def process_sync(self, item: T, timeout: Optional[float] = None) -> R:
        """同步方式处理单个项目（阻塞等待结果）
        
        Args:
            item: 要处理的项目
            timeout: 超时时间
            
        Returns:
            处理结果
        """
        future = await self.submit(item)
        return await asyncio.wait_for(future, timeout=timeout)
    
    async def _process_loop(self):
        """主处理循环"""
        while self._running:
            try:
                batch = await self._collect_batch()
                if batch:
                    async with self._semaphore:
                        await self._process_batch(batch)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in process loop: {e}")
                await asyncio.sleep(0.1)
    
    async def _collect_batch(self) -> List[BatchItem]:
        """收集一批待处理项目"""
        batch = []
        start_time = asyncio.get_event_loop().time()
        
        while len(batch) < self.batch_size:
            elapsed = asyncio.get_event_loop().time() - start_time
            remaining = self.max_wait_time - elapsed
            
            if remaining <= 0 and batch:
                break
            
            try:
                timeout = max(0, remaining) if batch else self.max_wait_time
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                batch.append(item)
            except asyncio.TimeoutError:
                break
        
        return batch
    
    async def _process_batch(self, batch: List[BatchItem]):
        """处理一批项目"""
        if not batch:
            return
        
        start_time = time.time()
        item_ids = [item.item_id for item in batch]
        items_data = [item.data for item in batch]
        
        # 更新状态
        for item in batch:
            item.status = BatchStatus.PROCESSING
        
        try:
            results = await self._execute_processor(items_data)
            
            # 分发结果
            for item, result in zip(batch, results):
                item.status = BatchStatus.COMPLETED
                item.result = result
                item.processed_at = time.time()
                
                future = self._results.pop(item.item_id, None)
                if future and not future.done():
                    future.set_result(result)
                
                self._stats["total_processed"] += 1
            
            self._stats["batches_processed"] += 1
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            await self._handle_batch_error(batch, e)
        
        processing_time = time.time() - start_time
        self._stats["total_wait_time"] += processing_time
    
    async def _execute_processor(self, items: List[T]) -> List[R]:
        """执行处理器"""
        if asyncio.iscoroutinefunction(self.processor):
            return await self.processor(items)
        else:
            # 在线程池中执行同步函数
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.processor, items)
    
    async def _handle_batch_error(self, batch: List[BatchItem], error: Exception):
        """处理批次错误"""
        if self.continue_on_error:
            # 逐个处理
            for item in batch:
                try:
                    result = await self._execute_processor([item.data])
                    item.status = BatchStatus.COMPLETED
                    item.result = result[0] if result else None
                    item.processed_at = time.time()
                    
                    future = self._results.pop(item.item_id, None)
                    if future and not future.done():
                        future.set_result(item.result)
                    
                    self._stats["total_processed"] += 1
                    
                except Exception as item_error:
                    await self._handle_item_error(item, item_error)
        else:
            # 全部标记为失败
            for item in batch:
                await self._handle_item_error(item, error)
    
    async def _handle_item_error(self, item: BatchItem, error: Exception):
        """处理单个项目错误"""
        item.status = BatchStatus.FAILED
        item.error = str(error)
        item.processed_at = time.time()
        
        future = self._results.pop(item.item_id, None)
        if future and not future.done():
            future.set_exception(error)
        
        self._stats["total_failed"] += 1
        
        if self.error_handler:
            try:
                if asyncio.iscoroutinefunction(self.error_handler):
                    await self.error_handler(item.data, error)
                else:
                    self.error_handler(item.data, error)
            except Exception as e:
                logger.error(f"Error handler failed: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self._stats.copy()
        stats["queue_size"] = self._queue.qsize()
        stats["pending_results"] = len(self._results)
        
        if stats["batches_processed"] > 0:
            stats["avg_batch_time"] = stats["total_wait_time"] / stats["batches_processed"]
        
        return stats
    
    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self._running


class StreamingBatchProcessor(Generic[T, R]):
    """流式批处理器
    
    支持异步迭代器接口，适合处理大量数据流。
    """
    
    def __init__(
        self,
        processor: Callable[[List[T]], List[R]],
        batch_size: int = 100,
        max_buffer_size: int = 1000
    ):
        self.processor = processor
        self.batch_size = batch_size
        self.max_buffer_size = max_buffer_size
        self._buffer: deque = deque(maxlen=max_buffer_size)
        self._output_queue: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
    
    async def start(self):
        """启动处理器"""
        self._task = asyncio.create_task(self._process_loop())
    
    async def stop(self):
        """停止处理器"""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def feed(self, item: T):
        """喂入数据"""
        self._buffer.append(item)
        if len(self._buffer) >= self.batch_size:
            await self._flush()
    
    async def feed_batch(self, items: List[T]):
        """批量喂入数据"""
        for item in items:
            await self.feed(item)
    
    async def _flush(self):
        """刷新缓冲区"""
        if not self._buffer:
            return
        
        batch = []
        while self._buffer and len(batch) < self.batch_size:
            batch.append(self._buffer.popleft())
        
        if batch:
            try:
                results = await self._execute_processor(batch)
                for result in results:
                    await self._output_queue.put((result, None))
            except Exception as e:
                for _ in batch:
                    await self._output_queue.put((None, str(e)))
    
    async def _process_loop(self):
        """处理循环"""
        while True:
            try:
                if self._buffer:
                    await self._flush()
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                await self._flush()  # 最后刷新
                break
    
    async def _execute_processor(self, items: List[T]) -> List[R]:
        """执行处理器"""
        if asyncio.iscoroutinefunction(self.processor):
            return await self.processor(items)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.processor, items)
    
    async def __aiter__(self) -> AsyncIterator[R]:
        """异步迭代器"""
        return self
    
    async def __anext__(self) -> R:
        """获取下一个结果"""
        result, error = await self._output_queue.get()
        if error:
            raise BatchProcessingError(error)
        return result


class ParallelBatchProcessor(Generic[T, R]):
    """并行批处理器
    
    同时处理多个批次，提高吞吐量。
    """
    
    def __init__(
        self,
        processor: Callable[[List[T]], List[R]],
        batch_size: int = 100,
        max_workers: int = 4
    ):
        self.processor = processor
        self.batch_size = batch_size
        self.max_workers = max_workers
        self._semaphore = asyncio.Semaphore(max_workers)
    
    async def process(self, items: List[T]) -> BatchResult[R]:
        """处理项目列表
        
        Args:
            items: 要处理的项目列表
            
        Returns:
            批处理结果
        """
        # 分割批次
        batches = [
            items[i:i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]
        
        # 并行处理
        tasks = [
            self._process_batch_with_semaphore(batch, i)
            for i, batch in enumerate(batches)
        ]
        
        start_time = time.time()
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        processing_time = time.time() - start_time
        
        # 合并结果
        result = BatchResult[R](
            total_count=len(items),
            processing_time=processing_time
        )
        
        for batch_result in batch_results:
            if isinstance(batch_result, Exception):
                result.failure_count += 1
                result.failed.append({"error": str(batch_result)})
            else:
                successful, failed = batch_result
                result.successful.extend(successful)
                result.failed.extend(failed)
                result.success_count += len(successful)
                result.failure_count += len(failed)
        
        return result
    
    async def _process_batch_with_semaphore(
        self,
        batch: List[T],
        batch_id: int
    ) -> tuple:
        """带信号量的批次处理"""
        async with self._semaphore:
            return await self._process_batch(batch, batch_id)
    
    async def _process_batch(
        self,
        batch: List[T],
        batch_id: int
    ) -> tuple:
        """处理单个批次"""
        try:
            if asyncio.iscoroutinefunction(self.processor):
                results = await self.processor(batch)
            else:
                loop = asyncio.get_event_loop()
                results = await loop.run_in_executor(None, self.processor, batch)
            
            return results, []
        except Exception as e:
            logger.error(f"Batch {batch_id} failed: {e}")
            return [], [{"batch_id": batch_id, "error": str(e)}]


# 便捷函数
async def process_batch(
    items: List[T],
    processor: Callable[[List[T]], List[R]],
    batch_size: int = 100,
    max_workers: int = 4
) -> BatchResult[R]:
    """便捷函数：批处理
    
    Args:
        items: 要处理的项目列表
        processor: 处理函数
        batch_size: 批大小
        max_workers: 最大并发数
        
    Returns:
        批处理结果
    """
    batch_processor = ParallelBatchProcessor(processor, batch_size, max_workers)
    return await batch_processor.process(items)

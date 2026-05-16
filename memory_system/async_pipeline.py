"""
异步流水线处理器模块

本模块实现了生产者-消费者模式的多级流水线，支持背压机制、优雅关闭、
错误恢复等功能。专为Agent Memory System的嵌入生成、存储、索引三阶段处理设计。

核心特性:
- 多级流水线架构
- 背压(Backpressure)机制
- 优雅关闭和错误恢复
- 阶段间缓冲区管理
- 性能监控和指标收集
- 动态流水线配置

示例用法:
    >>> from async_pipeline import Pipeline, PipelineStage, PipelineConfig
    >>> 
    >>> # 定义处理阶段
    >>> async def embedding_stage(item):
    ...     item['embedding'] = await generate_embedding(item['text'])
    ...     return item
    >>> 
    >>> async def storage_stage(item):
    ...     await store_embedding(item)
    ...     return item
    >>> 
    >>> # 创建流水线
    >>> pipeline = Pipeline([
    ...     PipelineStage("embedding", embedding_stage, workers=4),
    ...     PipelineStage("storage", storage_stage, workers=2),
    ... ])
    >>> 
    >>> await pipeline.start()
    >>> await pipeline.submit({"text": "example"})
    >>> await pipeline.stop()
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Coroutine,
    Deque,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    Set,
    TypeVar,
    Union,
)
from contextlib import asynccontextmanager
import uuid


logger = logging.getLogger(__name__)


class PipelineStatus(Enum):
    """流水线状态枚举"""
    CREATED = auto()
    STARTING = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPING = auto()
    STOPPED = auto()
    ERROR = auto()


class BackpressureStrategy(Enum):
    """背压策略枚举"""
    BLOCK = auto()          # 阻塞生产者直到有空间
    DROP_OLDEST = auto()    # 丢弃最旧的数据
    DROP_NEWEST = auto()    # 丢弃最新的数据
    RAISE_EXCEPTION = auto() # 抛出异常


class PipelineError(Exception):
    """流水线错误基类"""
    pass


class StageError(PipelineError):
    """阶段处理错误"""
    pass


class BackpressureError(PipelineError):
    """背压错误"""
    pass


class PipelineFullError(PipelineError):
    """流水线已满错误"""
    pass


class QueryDegradationError(PipelineError):
    """查询性能退化错误"""
    def __init__(
        self,
        message: str,
        stage_name: Optional[str] = None,
        queue_size: int = 0,
        processing_time: float = 0.0
    ):
        self.stage_name = stage_name
        self.queue_size = queue_size
        self.processing_time = processing_time
        super().__init__(message)


class IndexBuildError(PipelineError):
    """索引构建错误"""
    def __init__(self, message: str, index_name: Optional[str] = None):
        self.index_name = index_name
        super().__init__(message)


@dataclass
class PipelineItem:
    """
    流水线数据项
    
    Attributes:
        data: 实际数据
        item_id: 唯一标识
        created_at: 创建时间戳
        stage_timestamps: 各阶段处理时间记录
        metadata: 元数据
        retry_count: 重试次数
        error: 错误信息
    """
    data: Any
    item_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=time.time)
    stage_timestamps: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    error: Optional[str] = None
    
    def record_stage_entry(self, stage_name: str):
        """记录进入阶段的时间"""
        self.stage_timestamps[f"{stage_name}_enter"] = time.time()
    
    def record_stage_exit(self, stage_name: str):
        """记录离开阶段的时间"""
        self.stage_timestamps[f"{stage_name}_exit"] = time.time()
    
    def get_stage_duration(self, stage_name: str) -> float:
        """获取阶段处理时长"""
        enter_key = f"{stage_name}_enter"
        exit_key = f"{stage_name}_exit"
        if enter_key in self.stage_timestamps and exit_key in self.stage_timestamps:
            return self.stage_timestamps[exit_key] - self.stage_timestamps[enter_key]
        return 0.0
    
    def get_total_duration(self) -> float:
        """获取总处理时长"""
        return time.time() - self.created_at


@dataclass
class StageMetrics:
    """
    阶段指标
    
    Attributes:
        items_processed: 处理的项目数
        items_failed: 失败的项目数
        total_processing_time: 总处理时间
        avg_processing_time: 平均处理时间
        max_queue_size: 最大队列大小
        current_queue_size: 当前队列大小
    """
    items_processed: int = 0
    items_failed: int = 0
    total_processing_time: float = 0.0
    avg_processing_time: float = 0.0
    max_queue_size: int = 0
    current_queue_size: int = 0
    
    def update_processing_time(self, duration: float):
        """更新处理时间统计"""
        self.total_processing_time += duration
        self.items_processed += 1
        self.avg_processing_time = (
            self.total_processing_time / self.items_processed
            if self.items_processed > 0 else 0.0
        )
    
    def record_failure(self):
        """记录失败"""
        self.items_failed += 1
    
    def update_queue_size(self, size: int):
        """更新队列大小"""
        self.current_queue_size = size
        self.max_queue_size = max(self.max_queue_size, size)


@dataclass
class PipelineConfig:
    """
    流水线配置
    
    Attributes:
        max_concurrent_items: 最大并发处理项目数
        global_timeout: 全局超时时间(秒)
        enable_metrics: 是否启用指标收集
        graceful_shutdown_timeout: 优雅关闭超时时间(秒)
        error_handler: 错误处理回调
    """
    max_concurrent_items: int = 1000
    global_timeout: float = 300.0
    enable_metrics: bool = True
    graceful_shutdown_timeout: float = 30.0
    error_handler: Optional[Callable[[PipelineItem, Exception], None]] = None


@dataclass
class StageConfig:
    """
    阶段配置
    
    Attributes:
        name: 阶段名称
        buffer_size: 缓冲区大小
        num_workers: 工作线程数
        backpressure_strategy: 背压策略
        max_retries: 最大重试次数
        retry_delay: 重试延迟(秒)
        timeout: 阶段超时时间(秒)
        batch_size: 批处理大小(0表示不批处理)
        batch_timeout: 批处理超时时间(秒)
        enable_indexing: 是否启用索引
        index_fields: 索引字段列表
        index_build_threshold: 触发索引构建的阈值
        query_timeout: 查询超时时间(秒)
        enable_query_cache: 是否启用查询缓存
        cache_size: 查询缓存大小
    """
    name: str
    buffer_size: int = 100
    num_workers: int = 1
    backpressure_strategy: BackpressureStrategy = BackpressureStrategy.BLOCK
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 60.0
    batch_size: int = 0
    batch_timeout: float = 0.1
    # 查询性能优化配置
    enable_indexing: bool = True
    index_fields: List[str] = field(default_factory=list)
    index_build_threshold: int = 100
    query_timeout: float = 30.0
    enable_query_cache: bool = True
    cache_size: int = 1000


StageHandler = Callable[[PipelineItem], Coroutine[Any, Any, PipelineItem]]
BatchStageHandler = Callable[[List[PipelineItem]], Coroutine[Any, Any, List[PipelineItem]]]


class PipelineStage:
    """
    流水线阶段
    
    表示流水线中的一个处理阶段，支持单条处理和批处理模式。
    """
    
    def __init__(
        self,
        name: str,
        handler: Union[StageHandler, BatchStageHandler],
        config: Optional[StageConfig] = None,
        is_batch_handler: bool = False,
    ):
        """
        初始化阶段
        
        Args:
            name: 阶段名称
            handler: 处理函数
            config: 阶段配置
            is_batch_handler: 是否为批处理handler
        """
        self.name = name
        self.handler = handler
        self.config = config or StageConfig(name=name)
        self.is_batch_handler = is_batch_handler
        
        # 队列
        self.input_queue: asyncio.Queue[PipelineItem] = asyncio.Queue(
            maxsize=self.config.buffer_size
        )
        self.output_queue: Optional[asyncio.Queue[PipelineItem]] = None
        
        # 工作线程
        self._workers: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        
        # 指标
        self.metrics = StageMetrics()
        
        # 批处理缓冲区
        self._batch_buffer: List[PipelineItem] = []
        self._batch_timer: Optional[asyncio.Task] = None
        
        # 查询性能优化 - 索引管理
        self._index: Dict[str, Dict[Any, Set[str]]] = {}  # field -> value -> item_ids
        self._item_store: Dict[str, PipelineItem] = {}  # item_id -> item
        self._index_lock = asyncio.Lock()
        self._index_build_pending = False
        self._last_index_build_time: float = 0.0
        
        # 查询缓存
        self._query_cache: Dict[str, Tuple[PipelineItem, float]] = {}
        self._cache_lock = asyncio.Lock()
        
        # 性能监控
        self._processing_times: Deque[float] = deque(maxlen=100)
        self._query_times: Deque[float] = deque(maxlen=100)
    
    async def start(self, output_queue: Optional[asyncio.Queue] = None):
        """启动阶段"""
        self.output_queue = output_queue
        self._shutdown_event.clear()
        
        # 启动工作线程
        for i in range(self.config.num_workers):
            worker = asyncio.create_task(
                self._worker_loop(worker_id=i),
                name=f"{self.name}_worker_{i}"
            )
            self._workers.append(worker)
        
        logger.info(f"Stage {self.name} started with {self.config.num_workers} workers")
    
    async def stop(self):
        """停止阶段 - P1修复: 确保所有资源被正确清理"""
        self._shutdown_event.set()
        
        # P1修复: 取消批处理定时器
        if self._batch_timer:
            self._batch_timer.cancel()
            try:
                await self._batch_timer
            except asyncio.CancelledError:
                pass
            self._batch_timer = None
        
        # P1修复: 等待所有工作线程完成
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()
        
        # P1修复: 清理输入队列
        while not self.input_queue.empty():
            try:
                self.input_queue.get_nowait()
                self.input_queue.task_done()
            except asyncio.QueueEmpty:
                break
        
        # P1修复: 清理输出队列引用
        self.output_queue = None
        
        logger.info(f"Stage {self.name} stopped and resources cleaned up")
    
    async def _worker_loop(self, worker_id: int):
        """工作线程主循环"""
        while not self._shutdown_event.is_set():
            try:
                if self.is_batch_handler and self.config.batch_size > 0:
                    await self._batch_process(worker_id)
                else:
                    await self._single_process(worker_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} in stage {self.name} error: {e}")
    
    async def _single_process(self, worker_id: int):
        """单条处理模式"""
        try:
            item = await asyncio.wait_for(
                self.input_queue.get(),
                timeout=0.5
            )
        except asyncio.TimeoutError:
            return
        
        item.record_stage_entry(self.name)
        start_time = time.time()
        
        try:
            # 重试逻辑
            for attempt in range(self.config.max_retries + 1):
                try:
                    if asyncio.iscoroutinefunction(self.handler):
                        result = await asyncio.wait_for(
                            self.handler(item),  # type: ignore
                            timeout=self.config.timeout
                        )
                    else:
                        result = self.handler(item)  # type: ignore
                    
                    if result is not None:
                        item = result
                    
                    break
                except Exception as e:
                    item.retry_count += 1
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
                    else:
                        raise
            
            # 更新指标
            duration = time.time() - start_time
            self.metrics.update_processing_time(duration)
            
            item.record_stage_exit(self.name)
            
            # 发送到下一个阶段
            if self.output_queue:
                await self.output_queue.put(item)
            
        except Exception as e:
            logger.error(f"Stage {self.name} failed to process item {item.item_id}: {e}")
            item.error = str(e)
            self.metrics.record_failure()
            
            # 错误处理：可以选择发送到死信队列或丢弃
            if self.output_queue:
                await self.output_queue.put(item)
        
        finally:
            self.input_queue.task_done()
    
    async def _batch_process(self, worker_id: int):
        """批处理模式"""
        # 收集批次
        batch: List[PipelineItem] = []
        deadline = time.time() + self.config.batch_timeout
        
        while len(batch) < self.config.batch_size and time.time() < deadline:
            try:
                timeout = max(0, deadline - time.time())
                item = await asyncio.wait_for(
                    self.input_queue.get(),
                    timeout=timeout
                )
                item.record_stage_entry(self.name)
                batch.append(item)
            except asyncio.TimeoutError:
                break
        
        if not batch:
            return
        
        # 处理批次
        start_time = time.time()
        
        try:
            for attempt in range(self.config.max_retries + 1):
                try:
                    results = await asyncio.wait_for(
                        self.handler(batch),  # type: ignore
                        timeout=self.config.timeout
                    )
                    
                    if results is None:
                        results = batch
                    
                    break
                except Exception as e:
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
                    else:
                        raise
            
            # 更新指标
            duration = time.time() - start_time
            for _ in results:
                self.metrics.update_processing_time(duration / len(results))
            
            # 发送到下一个阶段
            if self.output_queue:
                for item in results:
                    item.record_stage_exit(self.name)
                    await self.output_queue.put(item)
            
        except Exception as e:
            logger.error(f"Stage {self.name} failed to process batch: {e}")
            self.metrics.record_failure()
            
            # 将错误信息附加到每个项目
            if self.output_queue:
                for item in batch:
                    item.error = str(e)
                    await self.output_queue.put(item)
        
        finally:
            for _ in batch:
                self.input_queue.task_done()
    
    async def submit(self, item: PipelineItem, timeout: Optional[float] = None) -> bool:
        """
        提交项目到阶段
        
        Args:
            item: 要处理的项目
            timeout: 超时时间
            
        Returns:
            是否成功提交
        """
        timeout = timeout or 5.0
        
        try:
            # 根据背压策略处理
            if self.config.backpressure_strategy == BackpressureStrategy.BLOCK:
                await asyncio.wait_for(
                    self.input_queue.put(item),
                    timeout=timeout
                )
                return True
            
            elif self.config.backpressure_strategy == BackpressureStrategy.DROP_OLDEST:
                if self.input_queue.full():
                    try:
                        # 丢弃最旧的项目
                        dropped = self.input_queue.get_nowait()
                        self.input_queue.task_done()
                        logger.warning(f"Dropped oldest item {dropped.item_id} due to backpressure")
                    except asyncio.QueueEmpty:
                        pass
                await self.input_queue.put(item)
                return True
            
            elif self.config.backpressure_strategy == BackpressureStrategy.DROP_NEWEST:
                if self.input_queue.full():
                    logger.warning(f"Dropped newest item {item.item_id} due to backpressure")
                    return False
                await self.input_queue.put(item)
                return True
            
            elif self.config.backpressure_strategy == BackpressureStrategy.RAISE_EXCEPTION:
                self.input_queue.put_nowait(item)
                return True
            
        except asyncio.TimeoutError:
            raise BackpressureError(f"Timeout submitting to stage {self.name}")
        except asyncio.QueueFull:
            raise PipelineFullError(f"Stage {self.name} queue is full")
        
        return False
    
    def get_metrics(self) -> StageMetrics:
        """获取阶段指标"""
        self.metrics.update_queue_size(self.input_queue.qsize())
        return self.metrics
    
    # ========================================================================
    # 查询性能优化 - P0级别修复
    # ========================================================================
    
    async def build_index(self, force: bool = False) -> bool:
        """
        构建索引
        
        为已处理的数据构建索引以加速查询
        
        Args:
            force: 是否强制重建索引
            
        Returns:
            索引是否构建成功
        """
        if not self.config.enable_indexing:
            logger.debug(f"Indexing disabled for stage {self.name}")
            return False
        
        async with self._index_lock:
            # 检查是否需要构建索引
            if not force and len(self._item_store) < self.config.index_build_threshold:
                return True
            
            # 检查是否已有索引构建在进行
            if self._index_build_pending:
                logger.debug(f"Index build already pending for stage {self.name}")
                return False
            
            self._index_build_pending = True
            
        try:
            start_time = time.time()
            new_index: Dict[str, Dict[Any, Set[str]]] = {}
            
            # 为每个索引字段构建索引
            for field in self.config.index_fields:
                new_index[field] = {}
            
            # 遍历所有项目构建索引
            for item_id, item in self._item_store.items():
                if item.data is None:
                    continue
                
                if isinstance(item.data, dict):
                    for field in self.config.index_fields:
                        if field in item.data:
                            value = item.data[field]
                            if value is not None:
                                if value not in new_index[field]:
                                    new_index[field][value] = set()
                                new_index[field][value].add(item_id)
            
            # 更新索引
            async with self._index_lock:
                self._index = new_index
                self._last_index_build_time = time.time()
            
            build_time = time.time() - start_time
            logger.info(
                f"Index built for stage {self.name} in {build_time:.3f}s, "
                f"items: {len(self._item_store)}, fields: {self.config.index_fields}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to build index for stage {self.name}: {e}")
            raise IndexBuildError(str(e), index_name=self.name)
        finally:
            async with self._index_lock:
                self._index_build_pending = False
    
    async def add_to_index(self, item: PipelineItem) -> None:
        """
        将单个项目添加到索引
        
        Args:
            item: 要添加的项目
        """
        if not self.config.enable_indexing:
            return
        
        async with self._index_lock:
            # 存储项目
            self._item_store[item.item_id] = item
            
            # 添加到索引
            if isinstance(item.data, dict):
                for field in self.config.index_fields:
                    if field in item.data:
                        value = item.data[field]
                        if value is not None:
                            if field not in self._index:
                                self._index[field] = {}
                            if value not in self._index[field]:
                                self._index[field][value] = set()
                            self._index[field][value].add(item.item_id)
            
            # 检查是否需要重建索引
            if len(self._item_store) >= self.config.index_build_threshold:
                if not self._index_build_pending:
                    asyncio.create_task(self.build_index())
    
    async def query_by_index(
        self,
        field: str,
        value: Any,
        timeout: Optional[float] = None
    ) -> List[PipelineItem]:
        """
        通过索引查询项目
        
        Args:
            field: 查询字段
            value: 查询值
            timeout: 查询超时时间
            
        Returns:
            匹配的项目列表
            
        Raises:
            QueryDegradationError: 查询超时或性能退化时抛出
        """
        timeout = timeout or self.config.query_timeout
        start_time = time.time()
        
        # 检查查询缓存
        if self.config.enable_query_cache:
            cache_key = f"{field}:{value}"
            async with self._cache_lock:
                cached = self._query_cache.get(cache_key)
                if cached:
                    item, cache_time = cached
                    if time.time() - cache_time < 60:  # 缓存60秒
                        return [item]
        
        try:
            async with self._index_lock:
                # 检查索引是否存在
                if field not in self._index:
                    logger.warning(f"No index for field '{field}' in stage {self.name}")
                    # 回退到全表扫描
                    results = await self._full_scan_query(field, value, timeout)
                else:
                    # 使用索引查询
                    item_ids = self._index[field].get(value, set())
                    results = [
                        self._item_store[item_id]
                        for item_id in item_ids
                        if item_id in self._item_store
                    ]
            
            query_time = time.time() - start_time
            self._query_times.append(query_time)
            
            # 检查查询性能
            if query_time > timeout:
                raise QueryDegradationError(
                    f"Query timeout after {query_time:.3f}s for field '{field}'",
                    stage_name=self.name,
                    queue_size=self.input_queue.qsize(),
                    processing_time=query_time
                )
            
            # 更新缓存
            if self.config.enable_query_cache and len(results) == 1:
                cache_key = f"{field}:{value}"
                async with self._cache_lock:
                    if len(self._query_cache) >= self.config.cache_size:
                        # 移除最旧的缓存项
                        oldest_key = min(
                            self._query_cache.keys(),
                            key=lambda k: self._query_cache[k][1]
                        )
                        del self._query_cache[oldest_key]
                    self._query_cache[cache_key] = (results[0], time.time())
            
            return results
            
        except QueryDegradationError:
            raise
        except Exception as e:
            logger.error(f"Query failed for field '{field}' in stage {self.name}: {e}")
            raise QueryDegradationError(
                f"Query failed: {e}",
                stage_name=self.name,
                queue_size=self.input_queue.qsize()
            )
    
    async def _full_scan_query(
        self,
        field: str,
        value: Any,
        timeout: float
    ) -> List[PipelineItem]:
        """
        全表扫描查询（索引不可用时的回退方案）
        
        Args:
            field: 查询字段
            value: 查询值
            timeout: 超时时间
            
        Returns:
            匹配的项目列表
        """
        start_time = time.time()
        results = []
        
        for item_id, item in self._item_store.items():
            # 检查超时
            if time.time() - start_time > timeout:
                logger.warning(
                    f"Full scan query timeout after scanning {len(results)} items"
                )
                break
            
            if item.data is None:
                continue
            
            if isinstance(item.data, dict) and item.data.get(field) == value:
                results.append(item)
        
        return results
    
    async def query_by_conditions(
        self,
        conditions: Dict[str, Any],
        timeout: Optional[float] = None
    ) -> List[PipelineItem]:
        """
        多条件查询
        
        Args:
            conditions: 查询条件字典 {field: value}
            timeout: 查询超时时间
            
        Returns:
            匹配的项目列表
        """
        timeout = timeout or self.config.query_timeout
        start_time = time.time()
        
        if not conditions:
            return list(self._item_store.values())
        
        # 获取每个条件的匹配项
        candidate_sets: List[Set[str]] = []
        
        async with self._index_lock:
            for field, value in conditions.items():
                if field in self._index:
                    item_ids = self._index[field].get(value, set())
                    candidate_sets.append(item_ids)
                else:
                    # 如果某个字段没有索引，需要全表扫描
                    logger.debug(f"No index for field '{field}', using full scan")
                    matched_ids = set()
                    for item_id, item in self._item_store.items():
                        if isinstance(item.data, dict) and item.data.get(field) == value:
                            matched_ids.add(item_id)
                    candidate_sets.append(matched_ids)
        
        # 取交集
        if candidate_sets:
            result_ids = candidate_sets[0]
            for s in candidate_sets[1:]:
                result_ids = result_ids & s
        else:
            result_ids = set()
        
        # 获取结果项目
        results = [
            self._item_store[item_id]
            for item_id in result_ids
            if item_id in self._item_store
        ]
        
        query_time = time.time() - start_time
        self._query_times.append(query_time)
        
        if query_time > timeout:
            raise QueryDegradationError(
                f"Multi-condition query timeout after {query_time:.3f}s",
                stage_name=self.name,
                queue_size=self.input_queue.qsize(),
                processing_time=query_time
            )
        
        return results
    
    def get_index_stats(self) -> Dict[str, Any]:
        """
        获取索引统计信息
        
        Returns:
            索引统计信息
        """
        index_stats = {}
        for field, value_index in self._index.items():
            index_stats[field] = {
                "unique_values": len(value_index),
                "total_entries": sum(len(ids) for ids in value_index.values()),
            }
        
        avg_query_time = (
            sum(self._query_times) / len(self._query_times)
            if self._query_times else 0.0
        )
        
        return {
            "indexed_fields": list(self._index.keys()),
            "total_items": len(self._item_store),
            "index_stats": index_stats,
            "last_index_build_time": self._last_index_build_time,
            "cache_size": len(self._query_cache),
            "avg_query_time": avg_query_time,
        }
    
    async def clear_index(self) -> None:
        """清空索引"""
        async with self._index_lock:
            self._index.clear()
            self._item_store.clear()
            self._query_cache.clear()
            logger.info(f"Index cleared for stage {self.name}")


class Pipeline:
    """
    异步流水线
    
    管理多个处理阶段，实现数据流的自动流转。
    """
    
    def __init__(
        self,
        stages: List[PipelineStage],
        config: Optional[PipelineConfig] = None,
    ):
        """
        初始化流水线
        
        Args:
            stages: 流水线阶段列表
            config: 流水线配置
        """
        self.stages = stages
        self.config = config or PipelineConfig()
        
        self._status = PipelineStatus.CREATED
        self._status_lock = asyncio.Lock()
        
        # 输出队列(最后一个阶段的输出)
        self._output_queue: asyncio.Queue[PipelineItem] = asyncio.Queue()
        
        # 结果等待字典
        self._pending_results: Dict[str, asyncio.Future] = {}
        
        # 指标
        self._start_time: Optional[float] = None
        self._total_submitted = 0
        self._total_completed = 0
        
        # 监控任务
        self._monitor_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """启动流水线"""
        async with self._status_lock:
            if self._status != PipelineStatus.CREATED:
                raise PipelineError(f"Cannot start pipeline in status {self._status}")
            
            self._status = PipelineStatus.STARTING
        
        # 连接各阶段
        for i in range(len(self.stages) - 1):
            self.stages[i].output_queue = self.stages[i + 1].input_queue
        
        # 最后一个阶段的输出到结果队列
        if self.stages:
            self.stages[-1].output_queue = self._output_queue
        
        # 启动所有阶段
        for stage in self.stages:
            await stage.start()
        
        # 启动结果收集任务
        self._result_collector = asyncio.create_task(self._collect_results())
        
        # 启动监控任务
        if self.config.enable_metrics:
            self._monitor_task = asyncio.create_task(self._monitor_loop())
        
        self._start_time = time.time()
        
        async with self._status_lock:
            self._status = PipelineStatus.RUNNING
        
        logger.info(f"Pipeline started with {len(self.stages)} stages")
    
    async def stop(self, graceful: bool = True):
        """
        停止流水线 - P1修复: 确保所有资源被正确清理
        
        Args:
            graceful: 是否优雅关闭
        """
        async with self._status_lock:
            if self._status in (PipelineStatus.STOPPING, PipelineStatus.STOPPED):
                return
            self._status = PipelineStatus.STOPPING
        
        if graceful:
            # 等待所有输入队列处理完成
            try:
                await asyncio.wait_for(
                    self._wait_for_completion(),
                    timeout=self.config.graceful_shutdown_timeout
                )
            except asyncio.TimeoutError:
                logger.warning("Graceful shutdown timed out, forcing stop")
        
        # P1修复: 停止所有阶段，确保清理
        for stage in self.stages:
            try:
                await stage.stop()
                # P1修复: 清理阶段的索引和缓存
                await stage.clear_index()
            except Exception as e:
                logger.error(f"Error stopping stage {stage.name}: {e}")
        
        # P1修复: 停止收集任务
        if self._result_collector:
            self._result_collector.cancel()
            try:
                await self._result_collector
            except asyncio.CancelledError:
                pass
            self._result_collector = None
        
        # P1修复: 停止监控任务
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None
        
        # P1修复: 取消所有等待的结果
        for future in self._pending_results.values():
            if not future.done():
                future.cancel()
        self._pending_results.clear()
        
        # P1修复: 清理输出队列
        while not self._output_queue.empty():
            try:
                self._output_queue.get_nowait()
                self._output_queue.task_done()
            except asyncio.QueueEmpty:
                break
        
        async with self._status_lock:
            self._status = PipelineStatus.STOPPED
        
        logger.info("Pipeline stopped and all resources cleaned up")
    
    async def _wait_for_completion(self):
        """等待所有项目处理完成"""
        # 等待所有阶段输入队列为空
        for stage in self.stages:
            await stage.input_queue.join()
    
    async def _collect_results(self):
        """收集处理结果"""
        while True:
            try:
                item = await self._output_queue.get()
                self._total_completed += 1
                
                # 如果有等待的future，设置结果
                if item.item_id in self._pending_results:
                    future = self._pending_results.pop(item.item_id)
                    if not future.done():
                        if item.error:
                            future.set_exception(StageError(item.error))
                        else:
                            future.set_result(item)
                
                self._output_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error collecting results: {e}")
    
    async def _monitor_loop(self):
        """监控循环"""
        while True:
            try:
                await asyncio.sleep(5)
                await self._log_metrics()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitor error: {e}")
    
    async def _log_metrics(self):
        """记录指标"""
        metrics = self.get_metrics()
        logger.info(f"Pipeline metrics: {metrics}")
    
    async def submit(
        self,
        data: Any,
        wait_for_result: bool = True,
        timeout: Optional[float] = None,
    ) -> Optional[PipelineItem]:
        """
        提交数据到流水线
        
        Args:
            data: 要处理的数据
            wait_for_result: 是否等待结果
            timeout: 超时时间
            
        Returns:
            处理结果(如果wait_for_result为True)
        """
        if self._status != PipelineStatus.RUNNING:
            raise PipelineError(f"Pipeline is not running, current status: {self._status}")
        
        timeout = timeout or self.config.global_timeout
        
        item = PipelineItem(data=data)
        self._total_submitted += 1
        
        if wait_for_result:
            future: asyncio.Future = asyncio.get_event_loop().create_future()
            self._pending_results[item.item_id] = future
        
        # 提交到第一个阶段
        if self.stages:
            await self.stages[0].submit(item, timeout=timeout)
        
        if wait_for_result:
            try:
                result = await asyncio.wait_for(future, timeout=timeout)
                return result
            except asyncio.TimeoutError:
                self._pending_results.pop(item.item_id, None)
                raise PipelineError(f"Pipeline processing timeout after {timeout}s")
        
        return None
    
    async def submit_many(
        self,
        items: List[Any],
        timeout: Optional[float] = None,
    ) -> List[PipelineItem]:
        """
        批量提交数据
        
        Args:
            items: 数据列表
            timeout: 超时时间
            
        Returns:
            处理结果列表
        """
        timeout = timeout or self.config.global_timeout
        futures = []
        
        for data in items:
            item = PipelineItem(data=data)
            self._total_submitted += 1
            
            future: asyncio.Future = asyncio.get_event_loop().create_future()
            self._pending_results[item.item_id] = future
            futures.append((item, future))
            
            if self.stages:
                await self.stages[0].submit(item, timeout=timeout)
        
        # 等待所有结果
        results = []
        for item, future in futures:
            try:
                result = await asyncio.wait_for(future, timeout=timeout)
                results.append(result)
            except asyncio.TimeoutError:
                self._pending_results.pop(item.item_id, None)
                item.error = "Timeout"
                results.append(item)
            except Exception as e:
                self._pending_results.pop(item.item_id, None)
                item.error = str(e)
                results.append(item)
        
        return results
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取流水线指标"""
        stage_metrics = {}
        for stage in self.stages:
            stage_metrics[stage.name] = {
                "processed": stage.metrics.items_processed,
                "failed": stage.metrics.items_failed,
                "avg_time": stage.metrics.avg_processing_time,
                "queue_size": stage.input_queue.qsize(),
            }
        
        return {
            "status": self._status.name,
            "total_submitted": self._total_submitted,
            "total_completed": self._total_completed,
            "pending": len(self._pending_results),
            "uptime": time.time() - self._start_time if self._start_time else 0,
            "stages": stage_metrics,
        }
    
    async def pause(self):
        """暂停流水线(暂停接受新数据)"""
        async with self._status_lock:
            if self._status == PipelineStatus.RUNNING:
                self._status = PipelineStatus.PAUSED
                logger.info("Pipeline paused")
    
    async def resume(self):
        """恢复流水线"""
        async with self._status_lock:
            if self._status == PipelineStatus.PAUSED:
                self._status = PipelineStatus.RUNNING
                logger.info("Pipeline resumed")
    
    @property
    def status(self) -> PipelineStatus:
        """获取当前状态"""
        return self._status


# 预定义的内存处理流水线阶段

async def embedding_stage_handler(item: PipelineItem) -> PipelineItem:
    """
    嵌入生成阶段处理器
    
    为文本数据生成向量嵌入
    """
    data = item.data
    
    # 模拟嵌入生成
    if isinstance(data, dict) and "text" in data:
        text = data["text"]
        # 这里应该调用实际的嵌入生成服务
        # 模拟生成128维向量
        import random
        embedding = [random.random() for _ in range(128)]
        data["embedding"] = embedding
        data["embedding_model"] = "mock-embedding-v1"
        data["embedding_timestamp"] = time.time()
    
    item.data = data
    return item


async def storage_stage_handler(item: PipelineItem) -> PipelineItem:
    """
    存储阶段处理器
    
    将数据持久化存储
    """
    data = item.data
    
    # 模拟存储操作
    data["stored"] = True
    data["storage_timestamp"] = time.time()
    
    item.data = data
    return item


async def indexing_stage_handler(item: PipelineItem) -> PipelineItem:
    """
    索引阶段处理器
    
    为数据建立索引
    """
    data = item.data
    
    # 模拟索引操作
    data["indexed"] = True
    data["index_timestamp"] = time.time()
    
    item.data = data
    return item


def create_memory_pipeline(
    embedding_workers: int = 4,
    storage_workers: int = 2,
    indexing_workers: int = 2,
) -> Pipeline:
    """
    创建标准的内存处理流水线
    
    Args:
        embedding_workers: 嵌入阶段工作线程数
        storage_workers: 存储阶段工作线程数
        indexing_workers: 索引阶段工作线程数
        
    Returns:
        配置好的流水线
    """
    stages = [
        PipelineStage(
            name="embedding",
            handler=embedding_stage_handler,
            config=StageConfig(
                name="embedding",
                num_workers=embedding_workers,
                buffer_size=200,
                timeout=30.0,
            ),
        ),
        PipelineStage(
            name="storage",
            handler=storage_stage_handler,
            config=StageConfig(
                name="storage",
                num_workers=storage_workers,
                buffer_size=100,
                timeout=10.0,
            ),
        ),
        PipelineStage(
            name="indexing",
            handler=indexing_stage_handler,
            config=StageConfig(
                name="indexing",
                num_workers=indexing_workers,
                buffer_size=100,
                timeout=10.0,
            ),
        ),
    ]
    
    config = PipelineConfig(
        max_concurrent_items=1000,
        global_timeout=120.0,
        enable_metrics=True,
    )
    
    return Pipeline(stages=stages, config=config)


# 测试代码
if __name__ == "__main__":
    async def test_pipeline():
        """测试流水线"""
        logging.basicConfig(level=logging.INFO)
        
        # 创建流水线
        pipeline = create_memory_pipeline(
            embedding_workers=2,
            storage_workers=1,
            indexing_workers=1,
        )
        
        await pipeline.start()
        
        # 提交一些测试数据
        test_items = [
            {"text": f"This is test item {i}", "id": i}
            for i in range(10)
        ]
        
        print("Submitting items individually...")
        for item in test_items[:5]:
            result = await pipeline.submit(item, wait_for_result=True)
            print(f"Result: {result.data if result else None}")
        
        print("\nSubmitting items in batch...")
        results = await pipeline.submit_many(test_items[5:])
        for result in results:
            print(f"Batch result: {result.data}")
        
        # 获取指标
        metrics = pipeline.get_metrics()
        print(f"\nPipeline metrics: {metrics}")
        
        await pipeline.stop()
        print("\nPipeline test completed!")
    
    asyncio.run(test_pipeline())

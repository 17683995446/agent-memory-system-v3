"""
智能任务调度器模块

本模块实现了基于优先级的任务队列、负载感知的动态调度、任务依赖和DAG执行、
公平调度和资源隔离等功能。专为Agent Memory System的任务管理设计。

核心特性:
- 基于优先级的多级任务队列
- 负载感知的动态调度算法
- 任务依赖图(DAG)执行
- 公平调度策略(防止任务饥饿)
- 资源隔离和配额管理
- 任务取消和超时控制
- 执行历史和统计分析

示例用法:
    >>> from task_scheduler import TaskScheduler, Task, TaskPriority
    >>> 
    >>> scheduler = TaskScheduler(max_workers=4)
    >>> await scheduler.start()
    >>> 
    >>> # 提交简单任务
    >>> task = Task(
    ...     task_id="task1",
    ...     coroutine=some_async_function(),
    ...     priority=TaskPriority.HIGH
    ... )
    >>> await scheduler.submit(task)
    >>> 
    >>> # 提交带依赖的任务
    >>> task2 = Task(
    ...     task_id="task2",
    ...     coroutine=another_function(),
    ...     dependencies=["task1"]
    >>> )
    >>> await scheduler.submit(task2)
"""

import asyncio
import heapq
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)
import uuid
from contextlib import asynccontextmanager


logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    """任务优先级枚举"""
    CRITICAL = 0    # 关键任务，最高优先级
    HIGH = 1        # 高优先级
    NORMAL = 2      # 普通优先级
    LOW = 3         # 低优先级
    BACKGROUND = 4  # 后台任务，最低优先级


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = auto()        # 等待中
    SCHEDULED = auto()      # 已调度
    RUNNING = auto()        # 运行中
    COMPLETED = auto()      # 已完成
    FAILED = auto()         # 失败
    CANCELLED = auto()      # 已取消
    TIMEOUT = auto()        # 超时
    BLOCKED = auto()        # 被依赖阻塞


class SchedulingPolicy(Enum):
    """调度策略枚举"""
    PRIORITY = auto()       # 纯优先级调度
    FAIR = auto()           # 公平调度
    ROUND_ROBIN = auto()    # 轮询调度
    LOAD_AWARE = auto()     # 负载感知调度


class SchedulerError(Exception):
    """调度器错误基类"""
    pass


class TaskNotFoundError(SchedulerError):
    """任务未找到错误"""
    pass


class CircularDependencyError(SchedulerError):
    """循环依赖错误"""
    pass


class ResourceExhaustedError(SchedulerError):
    """资源耗尽错误"""
    pass


@dataclass(order=True)
class PrioritizedTask:
    """
    带优先级的任务包装器
    
    用于在优先队列中排序
    """
    priority: int
    sequence: int
    task: "Task" = field(compare=False)


@dataclass
class TaskResources:
    """
    任务资源需求
    
    Attributes:
        cpu_percent: CPU百分比需求
        memory_mb: 内存需求(MB)
        io_ops_per_sec: IO操作每秒
        network_mbps: 网络带宽需求(Mbps)
    """
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    io_ops_per_sec: float = 0.0
    network_mbps: float = 0.0


@dataclass
class TaskMetrics:
    """
    任务执行指标
    
    Attributes:
        submit_time: 提交时间
        start_time: 开始执行时间
        end_time: 结束时间
        wait_time: 等待时间
        execution_time: 执行时间
        retry_count: 重试次数
    """
    submit_time: float = field(default_factory=time.time)
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    wait_time: float = 0.0
    execution_time: float = 0.0
    retry_count: int = 0
    
    def mark_started(self):
        """标记任务开始"""
        self.start_time = time.time()
        self.wait_time = self.start_time - self.submit_time
    
    def mark_completed(self):
        """标记任务完成"""
        self.end_time = time.time()
        if self.start_time:
            self.execution_time = self.end_time - self.start_time


@dataclass
class Task:
    """
    任务定义
    
    Attributes:
        task_id: 任务唯一标识
        coroutine: 异步协程或可调用对象
        priority: 任务优先级
        dependencies: 依赖的任务ID列表
        timeout: 超时时间(秒)
        max_retries: 最大重试次数
        resources: 资源需求
        metadata: 元数据
        group_id: 任务组ID(用于公平调度)
    """
    task_id: str
    coroutine: Union[Coroutine[Any, Any, Any], Callable[[], Coroutine[Any, Any, Any]]]
    priority: TaskPriority = TaskPriority.NORMAL
    dependencies: List[str] = field(default_factory=list)
    timeout: Optional[float] = None
    max_retries: int = 0
    resources: TaskResources = field(default_factory=TaskResources)
    metadata: Dict[str, Any] = field(default_factory=dict)
    group_id: Optional[str] = None
    
    # 内部状态
    status: TaskStatus = field(default=TaskStatus.PENDING, init=False)
    result: Any = field(default=None, init=False)
    error: Optional[Exception] = field(default=None, init=False)
    metrics: TaskMetrics = field(default_factory=TaskMetrics, init=False)
    _future: Optional[asyncio.Future] = field(default=None, init=False)
    _cancelled: bool = field(default=False, init=False)
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.group_id:
            self.group_id = "default"
    
    def create_future(self) -> asyncio.Future:
        """创建Future对象用于等待结果"""
        self._future = asyncio.get_event_loop().create_future()
        return self._future
    
    def set_result(self, result: Any):
        """设置任务结果"""
        self.result = result
        self.status = TaskStatus.COMPLETED
        if self._future and not self._future.done():
            self._future.set_result(result)
    
    def set_error(self, error: Exception):
        """设置任务错误"""
        self.error = error
        self.status = TaskStatus.FAILED
        if self._future and not self._future.done():
            self._future.set_exception(error)
    
    def cancel(self):
        """取消任务"""
        self._cancelled = True
        self.status = TaskStatus.CANCELLED
        if self._future and not self._future.done():
            self._future.cancel()
    
    async def execute(self) -> Any:
        """执行任务"""
        if self._cancelled:
            raise asyncio.CancelledError(f"Task {self.task_id} was cancelled")
        
        if asyncio.iscoroutine(self.coroutine):
            return await self.coroutine
        elif callable(self.coroutine):
            coro = self.coroutine()
            if asyncio.iscoroutine(coro):
                return await coro
            return coro
        else:
            raise TypeError(f"Invalid coroutine type: {type(self.coroutine)}")


@dataclass
class SchedulerConfig:
    """
    调度器配置
    
    Attributes:
        max_workers: 最大工作线程数
        max_queue_size: 最大队列大小
        scheduling_policy: 调度策略
        enable_fair_scheduling: 是否启用公平调度
        fair_share_time_slice: 公平调度时间片(秒)
        task_timeout_default: 默认任务超时
        enable_resource_monitoring: 是否启用资源监控
        resource_check_interval: 资源检查间隔(秒)
    """
    max_workers: int = 4
    max_queue_size: int = 1000
    scheduling_policy: SchedulingPolicy = SchedulingPolicy.LOAD_AWARE
    enable_fair_scheduling: bool = True
    fair_share_time_slice: float = 1.0
    task_timeout_default: float = 300.0
    enable_resource_monitoring: bool = True
    resource_check_interval: float = 5.0


class TaskQueue:
    """
    多级优先级任务队列
    
    支持优先级调度和公平调度
    """
    
    def __init__(self, config: SchedulerConfig):
        self.config = config
        self._sequence = 0
        
        # 按优先级分级的队列
        self._queues: Dict[TaskPriority, deque] = {
            priority: deque() for priority in TaskPriority
        }
        
        # 按组统计的任务数(用于公平调度)
        self._group_task_count: Dict[str, int] = defaultdict(int)
        
        # 被阻塞的任务(等待依赖)
        self._blocked_tasks: Dict[str, Task] = {}
        
        # 任务索引
        self._tasks: Dict[str, Task] = {}
        
        self._lock = asyncio.Lock()
    
    async def put(self, task: Task) -> bool:
        """
        添加任务到队列
        
        Args:
            task: 要添加的任务
            
        Returns:
            是否成功添加
        """
        async with self._lock:
            if len(self._tasks) >= self.config.max_queue_size:
                return False
            
            self._tasks[task.task_id] = task
            
            # 检查是否有依赖
            if task.dependencies:
                # 检查依赖是否已完成
                blocked = False
                for dep_id in task.dependencies:
                    if dep_id in self._tasks:
                        dep_task = self._tasks[dep_id]
                        if dep_task.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                            blocked = True
                            break
                    else:
                        # 依赖不存在，标记为失败
                        task.set_error(TaskNotFoundError(f"Dependency {dep_id} not found"))
                        return False
                
                if blocked:
                    task.status = TaskStatus.BLOCKED
                    self._blocked_tasks[task.task_id] = task
                    return True
            
            # 添加到优先级队列
            self._queues[task.priority].append(task)
            self._group_task_count[task.group_id] += 1
            return True
    
    async def get(self) -> Optional[Task]:
        """
        获取下一个要执行的任务
        
        Returns:
            下一个任务，如果没有则返回None
        """
        async with self._lock:
            if self.config.enable_fair_scheduling:
                return self._get_fair()
            else:
                return self._get_priority()
    
    def _get_priority(self) -> Optional[Task]:
        """基于优先级的获取"""
        for priority in sorted(TaskPriority, key=lambda p: p.value):
            if self._queues[priority]:
                task = self._queues[priority].popleft()
                self._group_task_count[task.group_id] -= 1
                return task
        return None
    
    def _get_fair(self) -> Optional[Task]:
        """基于公平调度的获取"""
        # 找到任务数最少的组
        min_count = float('inf')
        selected_group = None
        
        for priority in sorted(TaskPriority, key=lambda p: p.value):
            for task in self._queues[priority]:
                count = self._group_task_count[task.group_id]
                if count < min_count:
                    min_count = count
                    selected_group = task.group_id
        
        if selected_group:
            for priority in sorted(TaskPriority, key=lambda p: p.value):
                for i, task in enumerate(self._queues[priority]):
                    if task.group_id == selected_group:
                        self._queues[priority].rotate(-i)
                        task = self._queues[priority].popleft()
                        self._group_task_count[task.group_id] -= 1
                        return task
        
        return None
    
    async def complete_dependency(self, task_id: str):
        """
        标记任务完成，检查并解锁依赖它的任务
        
        Args:
            task_id: 完成的任务ID
        """
        async with self._lock:
            # 找到所有依赖此任务的任务
            unblocked = []
            for blocked_id, task in list(self._blocked_tasks.items()):
                if task_id in task.dependencies:
                    # 检查是否所有依赖都已完成
                    all_deps_completed = all(
                        self._tasks.get(dep_id) and
                        self._tasks[dep_id].status == TaskStatus.COMPLETED
                        for dep_id in task.dependencies
                    )
                    
                    if all_deps_completed:
                        task.status = TaskStatus.PENDING
                        unblocked.append(task)
                        del self._blocked_tasks[blocked_id]
            
            # 将解锁的任务加入队列
            for task in unblocked:
                self._queues[task.priority].append(task)
    
    async def remove(self, task_id: str) -> Optional[Task]:
        """从队列中移除任务"""
        async with self._lock:
            if task_id in self._tasks:
                task = self._tasks.pop(task_id)
                
                # 从优先级队列中移除
                for queue in self._queues.values():
                    if task in queue:
                        queue.remove(task)
                        break
                
                # 从阻塞列表中移除
                if task_id in self._blocked_tasks:
                    del self._blocked_tasks[task_id]
                
                self._group_task_count[task.group_id] -= 1
                return task
            return None
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        return self._tasks.get(task_id)
    
    def __len__(self) -> int:
        """队列中的任务数"""
        return len(self._tasks)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取队列统计"""
        stats = {
            "total": len(self._tasks),
            "by_priority": {},
            "by_group": dict(self._group_task_count),
            "blocked": len(self._blocked_tasks),
        }
        
        for priority, queue in self._queues.items():
            stats["by_priority"][priority.name] = len(queue)
        
        return stats


class ResourceMonitor:
    """
    资源监控器
    
    监控系统资源使用情况
    """
    
    def __init__(self, check_interval: float = 5.0):
        self.check_interval = check_interval
        self._cpu_usage = 0.0
        self._memory_usage = 0.0
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """启动监控"""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
    
    async def stop(self):
        """停止监控"""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
    
    async def _monitor_loop(self):
        """监控循环"""
        while self._running:
            try:
                # 模拟资源监控
                # 实际实现中应该读取系统指标
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
    
    def get_load_factor(self) -> float:
        """
        获取系统负载因子
        
        Returns:
            0.0-1.0之间的负载因子
        """
        # 简化的负载计算
        return (self._cpu_usage + self._memory_usage) / 200.0
    
    def can_accept_task(self, resources: TaskResources) -> bool:
        """
        检查是否可以接受新任务
        
        Args:
            resources: 任务资源需求
            
        Returns:
            是否可以接受
        """
        # 简化的资源检查
        return self.get_load_factor() < 0.9


class TaskScheduler:
    """
    智能任务调度器
    
    管理任务的提交、调度、执行和监控
    """
    
    def __init__(self, config: Optional[SchedulerConfig] = None):
        """
        初始化调度器
        
        Args:
            config: 调度器配置
        """
        self.config = config or SchedulerConfig()
        self._task_queue = TaskQueue(self.config)
        self._resource_monitor = ResourceMonitor(self.config.resource_check_interval)
        
        # 工作线程
        self._workers: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # 调度任务
        self._scheduler_task: Optional[asyncio.Task] = None
        
        # 任务执行历史
        self._execution_history: deque = deque(maxlen=1000)
        
        # 统计
        self._stats = {
            "submitted": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
        }
        
        self._lock = asyncio.Lock()
    
    async def start(self):
        """启动调度器"""
        if self._running:
            return
        
        self._running = True
        self._shutdown_event.clear()
        
        # 启动资源监控
        if self.config.enable_resource_monitoring:
            await self._resource_monitor.start()
        
        # 启动调度器
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        
        # 启动工作线程
        for i in range(self.config.max_workers):
            worker = asyncio.create_task(
                self._worker_loop(worker_id=i),
                name=f"scheduler_worker_{i}"
            )
            self._workers.append(worker)
        
        logger.info(f"TaskScheduler started with {self.config.max_workers} workers")
    
    async def stop(self, wait_for_pending: bool = True, timeout: float = 30.0):
        """
        停止调度器
        
        Args:
            wait_for_pending: 是否等待待处理任务
            timeout: 超时时间
        """
        if not self._running:
            return
        
        self._running = False
        self._shutdown_event.set()
        
        if wait_for_pending:
            # 等待队列清空
            try:
                await asyncio.wait_for(
                    self._wait_for_empty(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for tasks to complete")
        
        # 停止调度器
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        # 停止工作线程
        for worker in self._workers:
            worker.cancel()
        
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        
        # 停止资源监控
        if self.config.enable_resource_monitoring:
            await self._resource_monitor.stop()
        
        logger.info("TaskScheduler stopped")
    
    async def _wait_for_empty(self):
        """等待队列为空"""
        while len(self._task_queue) > 0:
            await asyncio.sleep(0.1)
    
    async def _scheduler_loop(self):
        """调度器主循环"""
        while self._running:
            try:
                await asyncio.sleep(0.01)  # 10ms调度周期
            except asyncio.CancelledError:
                break
    
    async def _worker_loop(self, worker_id: int):
        """工作线程主循环"""
        while self._running:
            try:
                # 获取任务
                task = await self._get_next_task()
                
                if task is None:
                    await asyncio.sleep(0.01)
                    continue
                
                # 执行任务
                await self._execute_task(task)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
    
    async def _get_next_task(self) -> Optional[Task]:
        """获取下一个任务"""
        # 检查资源
        if self.config.enable_resource_monitoring:
            if self._resource_monitor.get_load_factor() > 0.9:
                return None
        
        return await self._task_queue.get()
    
    async def _execute_task(self, task: Task):
        """执行任务"""
        task.status = TaskStatus.RUNNING
        task.metrics.mark_started()
        
        timeout = task.timeout or self.config.task_timeout_default
        
        try:
            # 执行带超时
            result = await asyncio.wait_for(
                task.execute(),
                timeout=timeout
            )
            
            task.set_result(result)
            task.metrics.mark_completed()
            
            async with self._lock:
                self._stats["completed"] += 1
                self._execution_history.append({
                    "task_id": task.task_id,
                    "status": "completed",
                    "duration": task.metrics.execution_time,
                })
            
            # 通知依赖任务
            await self._task_queue.complete_dependency(task.task_id)
            
        except asyncio.TimeoutError:
            task.status = TaskStatus.TIMEOUT
            task.set_error(TimeoutError(f"Task {task.task_id} timed out after {timeout}s"))
            
            async with self._lock:
                self._stats["failed"] += 1
        
        except asyncio.CancelledError:
            task.cancel()
            async with self._lock:
                self._stats["cancelled"] += 1
        
        except Exception as e:
            task.set_error(e)
            
            async with self._lock:
                self._stats["failed"] += 1
                self._execution_history.append({
                    "task_id": task.task_id,
                    "status": "failed",
                    "error": str(e),
                })
            
            # 通知依赖任务(失败的依赖也会解锁等待的任务)
            await self._task_queue.complete_dependency(task.task_id)
    
    async def submit(self, task: Task, wait: bool = False, timeout: Optional[float] = None) -> Union[Task, Any]:
        """
        提交任务
        
        Args:
            task: 要提交的任务
            wait: 是否等待结果
            timeout: 等待超时
            
        Returns:
            如果wait为True返回结果，否则返回Task对象
        """
        if not self._running:
            raise SchedulerError("Scheduler is not running")
        
        # 检查循环依赖
        if task.dependencies:
            visited = set()
            stack = [task.task_id]
            
            while stack:
                current = stack.pop()
                if current in visited:
                    raise CircularDependencyError(f"Circular dependency detected for task {task.task_id}")
                visited.add(current)
                
                existing_task = self._task_queue.get_task(current)
                if existing_task:
                    stack.extend(existing_task.dependencies)
        
        # 添加到队列
        success = await self._task_queue.put(task)
        if not success:
            raise ResourceExhaustedError("Task queue is full")
        
        async with self._lock:
            self._stats["submitted"] += 1
        
        if wait:
            future = task.create_future()
            try:
                return await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(f"Wait for task {task.task_id} timed out")
        
        return task
    
    async def submit_batch(self, tasks: List[Task]) -> List[Task]:
        """
        批量提交任务
        
        Args:
            tasks: 任务列表
            
        Returns:
            提交的任务列表
        """
        results = []
        for task in tasks:
            try:
                result = await self.submit(task)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to submit task {task.task_id}: {e}")
                task.set_error(e)
                results.append(task)
        return results
    
    async def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功取消
        """
        task = await self._task_queue.remove(task_id)
        if task:
            task.cancel()
            async with self._lock:
                self._stats["cancelled"] += 1
            return True
        return False
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """获取任务状态"""
        task = self._task_queue.get_task(task_id)
        return task.status if task else None
    
    def get_stats(self) -> Dict[str, Any]:
        """获取调度器统计"""
        return {
            **self._stats,
            "queue_stats": self._task_queue.get_stats(),
            "running": self._running,
        }
    
    def get_execution_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取执行历史"""
        return list(self._execution_history)[-limit:]


# DAG任务构建器

class DAGBuilder:
    """
    DAG任务构建器
    
    用于构建和管理任务依赖图
    """
    
    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self._dependencies: Dict[str, Set[str]] = defaultdict(set)
    
    def add_task(
        self,
        task_id: str,
        coroutine: Union[Coroutine[Any, Any, Any], Callable[[], Coroutine[Any, Any, Any]]],
        priority: TaskPriority = TaskPriority.NORMAL,
        **kwargs
    ) -> "DAGBuilder":
        """
        添加任务
        
        Args:
            task_id: 任务ID
            coroutine: 协程
            priority: 优先级
            **kwargs: 其他任务参数
            
        Returns:
            self，支持链式调用
        """
        task = Task(
            task_id=task_id,
            coroutine=coroutine,
            priority=priority,
            **kwargs
        )
        self._tasks[task_id] = task
        return self
    
    def add_dependency(self, task_id: str, depends_on: str) -> "DAGBuilder":
        """
        添加依赖关系
        
        Args:
            task_id: 依赖任务ID
            depends_on: 被依赖任务ID
            
        Returns:
            self，支持链式调用
        """
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"Task {task_id} not found")
        if depends_on not in self._tasks:
            raise TaskNotFoundError(f"Task {depends_on} not found")
        
        self._dependencies[task_id].add(depends_on)
        return self
    
    def build(self) -> List[Task]:
        """
        构建任务列表
        
        Returns:
            配置好依赖关系的任务列表
        """
        # 拓扑排序检查循环依赖
        visited = set()
        temp_mark = set()
        
        def visit(task_id: str):
            if task_id in temp_mark:
                raise CircularDependencyError(f"Circular dependency detected")
            if task_id in visited:
                return
            
            temp_mark.add(task_id)
            for dep in self._dependencies[task_id]:
                visit(dep)
            temp_mark.remove(task_id)
            visited.add(task_id)
        
        for task_id in self._tasks:
            if task_id not in visited:
                visit(task_id)
        
        # 设置依赖关系
        for task_id, deps in self._dependencies.items():
            self._tasks[task_id].dependencies = list(deps)
        
        return list(self._tasks.values())
    
    async def submit_to_scheduler(self, scheduler: TaskScheduler) -> List[Task]:
        """
        提交所有任务到调度器
        
        Args:
            scheduler: 任务调度器
            
        Returns:
            提交的任务列表
        """
        tasks = self.build()
        return await scheduler.submit_batch(tasks)


# 测试代码
if __name__ == "__main__":
    async def test_scheduler():
        """测试调度器"""
        logging.basicConfig(level=logging.INFO)
        
        # 创建调度器
        config = SchedulerConfig(
            max_workers=4,
            enable_fair_scheduling=True,
        )
        scheduler = TaskScheduler(config)
        await scheduler.start()
        
        # 测试简单任务
        async def simple_task(name: str, duration: float):
            await asyncio.sleep(duration)
            return f"Task {name} completed"
        
        print("Submitting simple tasks...")
        tasks = []
        for i in range(5):
            task = Task(
                task_id=f"task_{i}",
                coroutine=simple_task(f"task_{i}", 0.5),
                priority=TaskPriority.NORMAL if i < 3 else TaskPriority.LOW,
            )
            result = await scheduler.submit(task)
            tasks.append(result)
        
        # 等待所有任务完成
        await asyncio.sleep(3)
        
        # 测试DAG
        print("\nTesting DAG...")
        dag = DAGBuilder()
        dag.add_task("A", simple_task("A", 0.5), priority=TaskPriority.HIGH)
        dag.add_task("B", simple_task("B", 0.3), priority=TaskPriority.HIGH)
        dag.add_task("C", simple_task("C", 0.4), priority=TaskPriority.NORMAL)
        dag.add_task("D", simple_task("D", 0.2), priority=TaskPriority.NORMAL)
        
        dag.add_dependency("C", "A")
        dag.add_dependency("C", "B")
        dag.add_dependency("D", "C")
        
        dag_tasks = await dag.submit_to_scheduler(scheduler)
        print(f"Submitted {len(dag_tasks)} DAG tasks")
        
        # 等待DAG任务完成
        await asyncio.sleep(3)
        
        # 获取统计
        stats = scheduler.get_stats()
        print(f"\nScheduler stats: {stats}")
        
        await scheduler.stop()
        print("\nScheduler test completed!")
    
    asyncio.run(test_scheduler())

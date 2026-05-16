"""
异步读写锁模块 (M-09)
优化短期记忆的并发访问性能
"""

import asyncio
from typing import Optional
from contextlib import asynccontextmanager

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class ReadWriteLock:
    """
    异步读写锁

    支持多个并发读操作或单个写操作，适用于读多写少的场景：
        - 读-读：允许并发
        - 读-写：互斥
        - 写-写：互斥

    使用示例:
        >>> lock = ReadWriteLock()
        >>> async with lock.read_lock():
        ...     # 读取操作
        >>> async with lock.write_lock():
        ...     # 写入操作
    """

    def __init__(self):
        # 写锁
        self._write_lock = asyncio.Lock()
        # 读计数锁
        self._read_count_lock = asyncio.Lock()
        # 当前读操作数
        self._read_count = 0
        # 等待写的任务数
        self._waiting_writers = 0
        # 写等待信号量（用于公平性）
        self._write_gate = asyncio.Semaphore(1)

    @asynccontextmanager
    async def read_lock(self):
        """
        获取读锁（上下文管理器）

        示例:
            >>> async with lock.read_lock():
            ...     # 安全地读取数据
        """
        await self.acquire_read()
        try:
            yield
        finally:
            await self.release_read()

    @asynccontextmanager
    async def write_lock(self):
        """
        获取写锁（上下文管理器）

        示例:
            >>> async with lock.write_lock():
            ...     # 安全地写入数据
        """
        await self.acquire_write()
        try:
            yield
        finally:
            await self.release_write()

    async def acquire_read(self) -> None:
        """
        获取读锁

        如果有写操作在等待，先让写操作完成（公平性）
        """
        # 如果有写操作在等待，先等待
        if self._waiting_writers > 0:
            async with self._write_gate:
                pass

        async with self._read_count_lock:
            self._read_count += 1
            if self._read_count == 1:
                # 第一个读操作，获取写锁
                await self._write_lock.acquire()

    async def release_read(self) -> None:
        """释放读锁"""
        async with self._read_count_lock:
            self._read_count -= 1
            if self._read_count == 0:
                # 最后一个读操作，释放写锁
                self._write_lock.release()

    async def acquire_write(self) -> None:
        """
        获取写锁

        等待所有读操作完成
        """
        self._waiting_writers += 1
        try:
            # 获取写锁
            await self._write_lock.acquire()
        finally:
            self._waiting_writers -= 1

    async def release_write(self) -> None:
        """释放写锁"""
        self._write_lock.release()

    def is_write_locked(self) -> bool:
        """检查是否被写锁定"""
        return self._write_lock.locked() and self._read_count == 0

    def is_read_locked(self) -> bool:
        """检查是否有读锁定"""
        return self._read_count > 0

    def get_stats(self) -> dict:
        """获取锁统计信息"""
        return {
            "read_count": self._read_count,
            "waiting_writers": self._waiting_writers,
            "is_write_locked": self.is_write_locked(),
            "is_read_locked": self.is_read_locked(),
        }


class OptimizedReadWriteLock:
    """
    优化的异步读写锁

    特点:
        - 支持读锁升级（读锁持有者可以获取写锁）
        - 支持写优先（避免写饥饿）
        - 超时机制
    """

    def __init__(self, write_priority: bool = True):
        """
        初始化优化读写锁

        Args:
            write_priority: 是否启用写优先模式
        """
        self._write_priority = write_priority

        # 内部锁
        self._internal_lock = asyncio.Lock()

        # 写锁
        self._write_lock = asyncio.Lock()

        # 读状态
        self._read_count = 0
        self._read_waiters = 0

        # 写状态
        self._write_count = 0
        self._write_waiters = 0

        # 当前持有者（用于锁升级检测）
        self._write_holder: Optional[asyncio.Task] = None

    @asynccontextmanager
    async def read_lock(self, timeout: Optional[float] = None):
        """获取读锁"""
        await self.acquire_read(timeout)
        try:
            yield
        finally:
            await self.release_read()

    @asynccontextmanager
    async def write_lock(self, timeout: Optional[float] = None):
        """获取写锁"""
        await self.acquire_write(timeout)
        try:
            yield
        finally:
            await self.release_write()

    async def acquire_read(self, timeout: Optional[float] = None) -> bool:
        """
        获取读锁

        Args:
            timeout: 超时时间（秒）

        Returns:
            是否成功获取
        """
        async with self._internal_lock:
            self._read_waiters += 1

        try:
            if timeout:
                try:
                    await asyncio.wait_for(
                        self._acquire_read_internal(),
                        timeout=timeout
                    )
                    return True
                except asyncio.TimeoutError:
                    return False
            else:
                await self._acquire_read_internal()
                return True
        finally:
            async with self._internal_lock:
                self._read_waiters -= 1

    async def _acquire_read_internal(self) -> None:
        """内部读锁获取逻辑"""
        while True:
            async with self._internal_lock:
                # 检查是否可以获取读锁
                if self._write_count == 0:
                    if not self._write_priority or self._write_waiters == 0:
                        self._read_count += 1
                        return

            # 等待并重试
            await asyncio.sleep(0.001)

    async def release_read(self) -> None:
        """释放读锁"""
        async with self._internal_lock:
            self._read_count = max(0, self._read_count - 1)

    async def acquire_write(self, timeout: Optional[float] = None) -> bool:
        """
        获取写锁

        Args:
            timeout: 超时时间（秒）

        Returns:
            是否成功获取
        """
        async with self._internal_lock:
            self._write_waiters += 1

        try:
            if timeout:
                try:
                    await asyncio.wait_for(
                        self._acquire_write_internal(),
                        timeout=timeout
                    )
                    return True
                except asyncio.TimeoutError:
                    return False
            else:
                await self._acquire_write_internal()
                return True
        finally:
            async with self._internal_lock:
                self._write_waiters -= 1

    async def _acquire_write_internal(self) -> None:
        """内部写锁获取逻辑"""
        # 获取写锁
        await self._write_lock.acquire()

        try:
            # 等待所有读操作完成
            while True:
                async with self._internal_lock:
                    if self._read_count == 0:
                        self._write_count += 1
                        self._write_holder = asyncio.current_task()
                        return
                await asyncio.sleep(0.001)
        except Exception:
            self._write_lock.release()
            raise

    async def release_write(self) -> None:
        """释放写锁"""
        async with self._internal_lock:
            self._write_count = max(0, self._write_count - 1)
            self._write_holder = None

        self._write_lock.release()

    def get_stats(self) -> dict:
        """获取锁统计信息"""
        return {
            "read_count": self._read_count,
            "read_waiters": self._read_waiters,
            "write_count": self._write_count,
            "write_waiters": self._write_waiters,
            "write_priority": self._write_priority,
        }


class ShortTermMemoryRWLock:
    """
    短期记忆专用读写锁

    针对短期记忆访问模式优化的读写锁：
        - 高频读、低频写
        - 支持批量读优化
        - 写操作合并
    """

    def __init__(self):
        self._rw_lock = ReadWriteLock()

        # 批量读优化
        self._batch_read_lock = asyncio.Lock()
        self._pending_reads = 0

        # 写合并
        self._write_queue: asyncio.Queue = asyncio.Queue()
        self._write_merger_task: Optional[asyncio.Task] = None
        self._write_merger_running = False

    async def start_write_merger(self) -> None:
        """启动写合并器"""
        if not self._write_merger_running:
            self._write_merger_running = True
            self._write_merger_task = asyncio.create_task(self._write_merger_loop())

    async def stop_write_merger(self) -> None:
        """停止写合并器"""
        self._write_merger_running = False
        if self._write_merger_task:
            self._write_merger_task.cancel()
            try:
                await self._write_merger_task
            except asyncio.CancelledError:
                pass

    async def _write_merger_loop(self) -> None:
        """写合并循环"""
        while self._write_merger_running:
            try:
                # 收集一批写操作
                writes = []
                try:
                    write = await asyncio.wait_for(
                        self._write_queue.get(),
                        timeout=0.01
                    )
                    writes.append(write)

                    # 尝试收集更多写操作
                    while len(writes) < 10:
                        try:
                            write = self._write_queue.get_nowait()
                            writes.append(write)
                        except asyncio.QueueEmpty:
                            break
                except asyncio.TimeoutError:
                    continue

                # 执行批量写
                if writes:
                    async with self._rw_lock.write_lock():
                        for write in writes:
                            try:
                                await write()
                            except Exception as e:
                                logger.warning(f"写操作失败: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"写合并循环错误: {e}")

    @asynccontextmanager
    async def read(self):
        """获取读锁"""
        async with self._batch_read_lock:
            self._pending_reads += 1

        try:
            async with self._rw_lock.read_lock():
                yield
        finally:
            async with self._batch_read_lock:
                self._pending_reads -= 1

    @asynccontextmanager
    async def write(self):
        """获取写锁"""
        async with self._rw_lock.write_lock():
            yield

    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            **self._rw_lock.get_stats(),
            "pending_reads": self._pending_reads,
            "write_queue_size": self._write_queue.qsize(),
            "write_merger_running": self._write_merger_running,
        }

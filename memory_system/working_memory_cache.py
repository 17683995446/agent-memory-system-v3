"""
短期记忆 (工作记忆) 实现
基于Miller定律，容量限制为7±2
支持可插拔后端：内存、Redis、SQLite
"""

from typing import List, Optional, Dict, Any
import asyncio
from cachetools import TTLCache

from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.core_abstractions.abstract_component_interfaces import ShortTermMemoryBackend


class InMemoryShortTermBackend(ShortTermMemoryBackend):
    """内存中的短期记忆后端（默认实现）"""

    MAX_COUNTER = 2**30  # 约10亿

    def __init__(self, capacity: int = 7, ttl_seconds: float = 300):
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self._cache: TTLCache[int, MemoryItem] = TTLCache(maxsize=capacity, ttl=ttl_seconds)
        self._counter: int = 0
        self._counter_lock = asyncio.Lock()  # 线程安全锁

    async def add(self, item: MemoryItem) -> str:
        """添加记忆"""
        async with self._counter_lock:
            self._counter += 1
            if self._counter > self.MAX_COUNTER:
                self._counter = 1
            counter_value = self._counter
        self._cache[counter_value] = item
        return item.memory_id

    async def get_recent(self, n: int) -> List[MemoryItem]:
        """获取最近n条记忆"""
        all_items = self._get_valid_items()
        all_items.sort(key=lambda x: x.timestamp, reverse=True)
        return all_items[:n]

    async def get_all(self) -> List[MemoryItem]:
        """获取所有记忆"""
        all_items = self._get_valid_items()
        all_items.sort(key=lambda x: x.timestamp)
        return all_items

    async def clear(self) -> None:
        """清空所有记忆"""
        self._cache.clear()

    async def is_full(self) -> bool:
        """检查是否已满"""
        # Trigger TTL expiration before checking
        self._cache.expire()
        return len(self._cache) >= self.capacity

    def _get_valid_items(self) -> List[MemoryItem]:
        """获取所有记忆项（TTLCache 自动处理过期）"""
        return list(self._cache.values())


class WorkingMemoryCache:
    """
    工作记忆缓存

    特点:
        - 容量有限 (默认7个组块，符合Miller定律)
        - 快速存取 O(1)
        - TTL自动过期
        - 临时存储当前上下文
        - 支持可插拔后端（内存、Redis、SQLite）
    """

    def __init__(
        self,
        capacity: int = 7,
        ttl_seconds: float = 300,
        backend: Optional[ShortTermMemoryBackend] = None
    ):
        """
        初始化

        Args:
            capacity: 容量限制
            ttl_seconds: TTL过期时间
            backend: 后端存储，默认为内存后端
        """
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self._backend = backend or InMemoryShortTermBackend(capacity, ttl_seconds)

    async def add(
        self,
        content: str,
        importance: ImportanceLevel = ImportanceLevel.MEDIUM,
        metadata: Optional[Dict[str, Any]] = None
    ) -> MemoryItem:
        """
        添加记忆

        Args:
            content: 内容
            importance: 重要性
            metadata: 元数据

        Returns:
            记忆项
        """
        item = MemoryItem(
            content=content,
            memory_type=MemoryType.SHORT_TERM,
            importance=importance,
            metadata=metadata or {}
        )

        await self._backend.add(item)
        return item

    async def get_recent(self, n: int = 5) -> List[MemoryItem]:
        """
        获取最近的n条记忆

        Args:
            n: 数量

        Returns:
            记忆列表
        """
        return await self._backend.get_recent(n)

    async def get_all(self) -> List[MemoryItem]:
        """获取所有短期记忆"""
        return await self._backend.get_all()

    async def clear(self):
        """清空短期记忆"""
        await self._backend.clear()

    async def is_full(self) -> bool:
        """检查是否已满"""
        return await self._backend.is_full()


# 保持向后兼容的别名
ShortTermMemory = WorkingMemoryCache

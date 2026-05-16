"""
Redis 短期记忆后端
支持分布式部署和TTL自动过期
"""

import json
from typing import Any, List, Optional

from memory_system.core_abstractions.abstract_component_interfaces import ShortTermMemoryBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem


try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None  # type: ignore


class RedisShortTermMemoryBackend(ShortTermMemoryBackend):
    """
    Redis 短期记忆后端

    特点:
        - 支持分布式部署
        - TTL自动过期
        - 可持久化到磁盘
    """

    def __init__(
        self,
        capacity: int = 7,
        ttl_seconds: float = 300,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: Optional[str] = None,
        key_prefix: str = "short_term_memory:"
    ):
        if not REDIS_AVAILABLE:
            raise ImportError(
                "Redis backend requires 'redis' package. "
                "Install it with: pip install redis"
            )

        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix
        self._redis: Optional[Any] = None
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_db = redis_db
        self._redis_password = redis_password

    async def _get_redis(self) -> Any:
        """获取Redis连接（懒连接）"""
        if self._redis is None:
            self._redis = redis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                db=self._redis_db,
                password=self._redis_password,
                decode_responses=False
            )
        return self._redis

    async def add(self, item: MemoryItem) -> str:
        """添加记忆到Redis"""
        r = await self._get_redis()
        key = f"{self.key_prefix}{item.memory_id}"

        # 序列化并存储
        data = json.dumps(item.to_storage_dict(), ensure_ascii=False, default=str).encode("utf-8")
        await r.setex(key, int(self.ttl_seconds), data)

        # 维护容量限制
        await self._enforce_capacity()

        return item.memory_id

    async def _enforce_capacity(self) -> None:
        """强制执行容量限制"""
        r = await self._get_redis()
        pattern = f"{self.key_prefix}*"
        keys = []
        ttls = []
        async for key in r.scan_iter(match=pattern):
            keys.append(key)
            ttl = await r.ttl(key)
            ttls.append(ttl)

        if len(keys) > self.capacity:
            # 按 TTL 排序，TTL 最小（最接近过期）的优先删除
            pairs = sorted(zip(keys, ttls), key=lambda x: x[1])
            keys_to_delete = [p[0] for p in pairs[:len(keys) - self.capacity]]
            for key in keys_to_delete:
                await r.delete(key)

    async def get_recent(self, n: int) -> List[MemoryItem]:
        """获取最近n条记忆"""
        all_items = await self.get_all()
        all_items.sort(key=lambda x: x.timestamp, reverse=True)
        return all_items[:n]

    async def get_all(self) -> List[MemoryItem]:
        """获取所有记忆"""
        r = await self._get_redis()
        pattern = f"{self.key_prefix}*"
        items = []

        async for key in r.scan_iter(match=pattern):
            data = await r.get(key)
            if data:
                item_dict = json.loads(data.decode("utf-8"))
                items.append(MemoryItem.from_storage_dict(item_dict))

        return items

    async def clear(self) -> None:
        """清空所有记忆"""
        r = await self._get_redis()
        pattern = f"{self.key_prefix}*"
        async for key in r.scan_iter(match=pattern):
            await r.delete(key)

    async def is_full(self) -> bool:
        """检查是否已满"""
        r = await self._get_redis()
        pattern = f"{self.key_prefix}*"
        count = 0
        async for _ in r.scan_iter(match=pattern):
            count += 1
            if count >= self.capacity:
                return True
        return False

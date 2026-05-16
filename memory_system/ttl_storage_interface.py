"""TTL存储抽象接口 - 解耦TTL管理器与具体存储

Agent Memory System V2.29.0 重构项目 - R-07
重构TTL管理器的存储依赖，通过抽象接口解耦
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import time

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class TTLEntry:
    """TTL条目数据类"""
    memory_id: str
    expiration_time: float
    created_at: float
    metadata: Optional[Dict[str, Any]] = None
    
    def is_expired(self, current_time: Optional[float] = None) -> bool:
        """检查是否已过期"""
        now = current_time or time.time()
        return now >= self.expiration_time
    
    def time_remaining(self, current_time: Optional[float] = None) -> float:
        """获取剩余时间（秒）"""
        now = current_time or time.time()
        return max(0, self.expiration_time - now)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "memory_id": self.memory_id,
            "expiration_time": self.expiration_time,
            "created_at": self.created_at,
            "metadata": self.metadata or {},
            "is_expired": self.is_expired(),
            "time_remaining": self.time_remaining()
        }


class TTLStorageInterface(ABC):
    """TTL存储抽象接口 - 解耦TTL管理器与具体存储
    
    定义TTL管理所需的所有存储操作，使TTL管理器
    可以独立于具体的存储后端实现。
    
    实现类可以是：
    - InMemoryTTLStorage: 内存存储（用于测试）
    - RedisTTLStorage: Redis存储
    - SQLTTLStorage: SQL数据库存储
    """
    
    @abstractmethod
    async def add_ttl_entry(
        self,
        memory_id: str,
        expiration: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """添加TTL条目
        
        Args:
            memory_id: 记忆ID
            expiration: 过期时间戳（秒）
            metadata: 可选元数据
            
        Returns:
            是否添加成功
        """
        pass
    
    @abstractmethod
    async def get_expired_entries(
        self,
        before_time: Optional[float] = None,
        limit: int = 100
    ) -> List[str]:
        """获取已过期条目
        
        Args:
            before_time: 过期时间阈值，None表示当前时间
            limit: 最大返回数量
            
        Returns:
            过期记忆ID列表
        """
        pass
    
    @abstractmethod
    async def remove_ttl_entry(self, memory_id: str) -> bool:
        """移除TTL条目
        
        Args:
            memory_id: 记忆ID
            
        Returns:
            是否移除成功
        """
        pass
    
    @abstractmethod
    async def extend_ttl(
        self,
        memory_id: str,
        new_expiration: float
    ) -> bool:
        """延长TTL
        
        Args:
            memory_id: 记忆ID
            new_expiration: 新的过期时间戳
            
        Returns:
            是否延长成功
        """
        pass
    
    @abstractmethod
    async def get_ttl_entry(self, memory_id: str) -> Optional[TTLEntry]:
        """获取TTL条目详情
        
        Args:
            memory_id: 记忆ID
            
        Returns:
            TTL条目，不存在则返回None
        """
        pass
    
    @abstractmethod
    async def list_all_entries(
        self,
        include_expired: bool = False
    ) -> List[TTLEntry]:
        """列出所有TTL条目
        
        Args:
            include_expired: 是否包含已过期条目
            
        Returns:
            TTL条目列表
        """
        pass
    
    @abstractmethod
    async def count_entries(self, include_expired: bool = False) -> int:
        """统计TTL条目数量
        
        Args:
            include_expired: 是否包含已过期条目
            
        Returns:
            条目数量
        """
        pass
    
    @abstractmethod
    async def batch_remove(self, memory_ids: List[str]) -> int:
        """批量移除TTL条目
        
        Args:
            memory_ids: 记忆ID列表
            
        Returns:
            成功移除的数量
        """
        pass
    
    @abstractmethod
    async def clear_all(self) -> int:
        """清除所有TTL条目
        
        Returns:
            清除的条目数量
        """
        pass


class InMemoryTTLStorage(TTLStorageInterface):
    """内存TTL存储实现
    
    基于内存字典的实现，适用于测试和单机部署。
    """
    
    def __init__(self):
        self._entries: Dict[str, TTLEntry] = {}
    
    async def add_ttl_entry(
        self,
        memory_id: str,
        expiration: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """添加TTL条目"""
        self._entries[memory_id] = TTLEntry(
            memory_id=memory_id,
            expiration_time=expiration,
            created_at=time.time(),
            metadata=metadata
        )
        return True
    
    async def get_expired_entries(
        self,
        before_time: Optional[float] = None,
        limit: int = 100
    ) -> List[str]:
        """获取已过期条目"""
        now = before_time or time.time()
        expired = [
            entry.memory_id
            for entry in self._entries.values()
            if entry.expiration_time <= now
        ]
        return expired[:limit]
    
    async def remove_ttl_entry(self, memory_id: str) -> bool:
        """移除TTL条目"""
        if memory_id in self._entries:
            del self._entries[memory_id]
            return True
        return False
    
    async def extend_ttl(
        self,
        memory_id: str,
        new_expiration: float
    ) -> bool:
        """延长TTL"""
        if memory_id not in self._entries:
            return False
        
        entry = self._entries[memory_id]
        self._entries[memory_id] = TTLEntry(
            memory_id=memory_id,
            expiration_time=new_expiration,
            created_at=entry.created_at,
            metadata=entry.metadata
        )
        return True
    
    async def get_ttl_entry(self, memory_id: str) -> Optional[TTLEntry]:
        """获取TTL条目详情"""
        return self._entries.get(memory_id)
    
    async def list_all_entries(
        self,
        include_expired: bool = False
    ) -> List[TTLEntry]:
        """列出所有TTL条目"""
        now = time.time()
        entries = list(self._entries.values())
        
        if not include_expired:
            entries = [e for e in entries if e.expiration_time > now]
        
        return sorted(entries, key=lambda x: x.expiration_time)
    
    async def count_entries(self, include_expired: bool = False) -> int:
        """统计TTL条目数量"""
        if include_expired:
            return len(self._entries)
        
        now = time.time()
        return sum(1 for e in self._entries.values() if e.expiration_time > now)
    
    async def batch_remove(self, memory_ids: List[str]) -> int:
        """批量移除TTL条目"""
        count = 0
        for mem_id in memory_ids:
            if await self.remove_ttl_entry(mem_id):
                count += 1
        return count
    
    async def clear_all(self) -> int:
        """清除所有TTL条目"""
        count = len(self._entries)
        self._entries.clear()
        return count


class RedisTTLStorage(TTLStorageInterface):
    """Redis TTL存储实现
    
    使用Redis Sorted Set存储TTL条目，
    分数为过期时间戳，便于范围查询。
    """
    
    def __init__(self, redis_client, key_prefix: str = "ttl"):
        self._redis = redis_client
        self._key = f"{key_prefix}:entries"
        self._meta_key = f"{key_prefix}:meta"
    
    async def add_ttl_entry(
        self,
        memory_id: str,
        expiration: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """添加TTL条目"""
        import json
        
        # 使用Sorted Set，分数为过期时间
        await self._redis.zadd(self._key, {memory_id: expiration})
        
        # 存储元数据
        if metadata:
            meta_data = {
                "created_at": time.time(),
                "metadata": metadata
            }
            await self._redis.hset(
                self._meta_key,
                memory_id,
                json.dumps(meta_data)
            )
        
        return True
    
    async def get_expired_entries(
        self,
        before_time: Optional[float] = None,
        limit: int = 100
    ) -> List[str]:
        """获取已过期条目"""
        now = before_time or time.time()
        # 获取分数 <= now 的所有成员
        expired = await self._redis.zrangebyscore(
            self._key,
            min="-inf",
            max=now,
            start=0,
            num=limit
        )
        return [e.decode() if isinstance(e, bytes) else e for e in expired]
    
    async def remove_ttl_entry(self, memory_id: str) -> bool:
        """移除TTL条目"""
        removed = await self._redis.zrem(self._key, memory_id)
        await self._redis.hdel(self._meta_key, memory_id)
        return removed > 0
    
    async def extend_ttl(
        self,
        memory_id: str,
        new_expiration: float
    ) -> bool:
        """延长TTL"""
        # 检查是否存在
        score = await self._redis.zscore(self._key, memory_id)
        if score is None:
            return False
        
        # 更新分数
        await self._redis.zadd(self._key, {memory_id: new_expiration})
        return True
    
    async def get_ttl_entry(self, memory_id: str) -> Optional[TTLEntry]:
        """获取TTL条目详情"""
        import json
        
        score = await self._redis.zscore(self._key, memory_id)
        if score is None:
            return None
        
        # 获取元数据
        meta_raw = await self._redis.hget(self._meta_key, memory_id)
        metadata = None
        created_at = time.time()
        
        if meta_raw:
            try:
                meta = json.loads(meta_raw.decode() if isinstance(meta_raw, bytes) else meta_raw)
                metadata = meta.get("metadata")
                created_at = meta.get("created_at", created_at)
            except json.JSONDecodeError:
                pass
        
        return TTLEntry(
            memory_id=memory_id,
            expiration_time=float(score),
            created_at=created_at,
            metadata=metadata
        )
    
    async def list_all_entries(
        self,
        include_expired: bool = False
    ) -> List[TTLEntry]:
        """列出所有TTL条目"""
        now = time.time()
        
        if include_expired:
            # 获取所有
            entries = await self._redis.zrange(self._key, 0, -1, withscores=True)
        else:
            # 获取未过期的
            entries = await self._redis.zrangebyscore(
                self._key,
                min=now,
                max="+inf",
                withscores=True
            )
        
        result = []
        for member, score in entries:
            mem_id = member.decode() if isinstance(member, bytes) else member
            entry = await self.get_ttl_entry(mem_id)
            if entry:
                result.append(entry)
        
        return result
    
    async def count_entries(self, include_expired: bool = False) -> int:
        """统计TTL条目数量"""
        if include_expired:
            return await self._redis.zcard(self._key)
        
        now = time.time()
        return await self._redis.zcount(self._key, now, "+inf")
    
    async def batch_remove(self, memory_ids: List[str]) -> int:
        """批量移除TTL条目"""
        if not memory_ids:
            return 0
        
        removed = await self._redis.zrem(self._key, *memory_ids)
        await self._redis.hdel(self._meta_key, *memory_ids)
        return removed
    
    async def clear_all(self) -> int:
        """清除所有TTL条目"""
        count = await self._redis.zcard(self._key)
        await self._redis.delete(self._key, self._meta_key)
        return count


class SQLTTLStorage(TTLStorageInterface):
    """SQL数据库TTL存储实现"""
    
    def __init__(self, db_connection, table_name: str = "ttl_entries"):
        self._db = db_connection
        self._table = table_name
    
    async def add_ttl_entry(
        self,
        memory_id: str,
        expiration: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """添加TTL条目"""
        import json
        
        query = f"""
            INSERT OR REPLACE INTO {self._table}
            (memory_id, expiration_time, created_at, metadata)
            VALUES (?, ?, ?, ?)
        """
        meta_str = json.dumps(metadata) if metadata else None
        await self._db.execute(query, (memory_id, expiration, time.time(), meta_str))
        await self._db.commit()
        return True
    
    async def get_expired_entries(
        self,
        before_time: Optional[float] = None,
        limit: int = 100
    ) -> List[str]:
        """获取已过期条目"""
        now = before_time or time.time()
        
        query = f"""
            SELECT memory_id FROM {self._table}
            WHERE expiration_time <= ?
            LIMIT ?
        """
        cursor = await self._db.execute(query, (now, limit))
        rows = await cursor.fetchall()
        return [row[0] for row in rows]
    
    async def remove_ttl_entry(self, memory_id: str) -> bool:
        """移除TTL条目"""
        query = f"DELETE FROM {self._table} WHERE memory_id = ?"
        cursor = await self._db.execute(query, (memory_id,))
        await self._db.commit()
        return cursor.rowcount > 0
    
    async def extend_ttl(
        self,
        memory_id: str,
        new_expiration: float
    ) -> bool:
        """延长TTL"""
        query = f"""
            UPDATE {self._table}
            SET expiration_time = ?
            WHERE memory_id = ?
        """
        cursor = await self._db.execute(query, (new_expiration, memory_id))
        await self._db.commit()
        return cursor.rowcount > 0
    
    async def get_ttl_entry(self, memory_id: str) -> Optional[TTLEntry]:
        """获取TTL条目详情"""
        import json
        
        query = f"SELECT * FROM {self._table} WHERE memory_id = ?"
        cursor = await self._db.execute(query, (memory_id,))
        row = await cursor.fetchone()
        
        if not row:
            return None
        
        metadata = json.loads(row[3]) if row[3] else None
        return TTLEntry(
            memory_id=row[0],
            expiration_time=row[1],
            created_at=row[2],
            metadata=metadata
        )
    
    async def list_all_entries(
        self,
        include_expired: bool = False
    ) -> List[TTLEntry]:
        """列出所有TTL条目"""
        import json
        
        if include_expired:
            query = f"SELECT * FROM {self._table} ORDER BY expiration_time"
            cursor = await self._db.execute(query)
        else:
            now = time.time()
            query = f"""
                SELECT * FROM {self._table}
                WHERE expiration_time > ?
                ORDER BY expiration_time
            """
            cursor = await self._db.execute(query, (now,))
        
        rows = await cursor.fetchall()
        return [
            TTLEntry(
                memory_id=row[0],
                expiration_time=row[1],
                created_at=row[2],
                metadata=json.loads(row[3]) if row[3] else None
            )
            for row in rows
        ]
    
    async def count_entries(self, include_expired: bool = False) -> int:
        """统计TTL条目数量"""
        if include_expired:
            query = f"SELECT COUNT(*) FROM {self._table}"
            cursor = await self._db.execute(query)
        else:
            now = time.time()
            query = f"SELECT COUNT(*) FROM {self._table} WHERE expiration_time > ?"
            cursor = await self._db.execute(query, (now,))
        
        row = await cursor.fetchone()
        return row[0] if row else 0
    
    async def batch_remove(self, memory_ids: List[str]) -> int:
        """批量移除TTL条目"""
        if not memory_ids:
            return 0
        
        placeholders = ','.join('?' * len(memory_ids))
        query = f"DELETE FROM {self._table} WHERE memory_id IN ({placeholders})"
        cursor = await self._db.execute(query, memory_ids)
        await self._db.commit()
        return cursor.rowcount
    
    async def clear_all(self) -> int:
        """清除所有TTL条目"""
        query = f"SELECT COUNT(*) FROM {self._table}"
        cursor = await self._db.execute(query)
        row = await cursor.fetchone()
        count = row[0] if row else 0
        
        await self._db.execute(f"DELETE FROM {self._table}")
        await self._db.commit()
        return count


class TTLStorageFactory:
    """TTL存储工厂"""
    
    @staticmethod
    def create_in_memory() -> InMemoryTTLStorage:
        """创建内存存储"""
        return InMemoryTTLStorage()
    
    @staticmethod
    def create_redis(redis_client, key_prefix: str = "ttl") -> RedisTTLStorage:
        """创建Redis存储"""
        return RedisTTLStorage(redis_client, key_prefix)
    
    @staticmethod
    def create_sql(db_connection, table_name: str = "ttl_entries") -> SQLTTLStorage:
        """创建SQL存储"""
        return SQLTTLStorage(db_connection, table_name)

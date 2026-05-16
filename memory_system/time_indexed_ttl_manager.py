"""
时间索引TTL管理模块 (M-05)
基于时间索引的增量过期清理，提升TTL清理性能
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend

from memory_system.memory_ttl_expiration_manager import (
    MemoryTTLExpirationManager,
    TTLConfig
)
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class TTLIndexEntry:
    """TTL索引条目"""
    memory_id: str
    expiration_time: float
    importance: int = 3


class TimeIndexedTTLManager(MemoryTTLExpirationManager):
    """
    时间索引TTL管理器

    在基础MemoryTTLExpirationManager之上增加了:
        - 时间索引：按过期时间组织记忆，实现O(1)过期检测
        - 增量清理：只检查可能过期的记忆
        - 批量删除：减少I/O操作
        - 内存高效：避免全表扫描

    使用示例:
        >>> manager = TimeIndexedTTLManager(storage)
        >>> await manager.add_with_ttl(item, ttl=3600)
        >>> count = await manager.cleanup_expired()
    """

    def __init__(
        self,
        storage: "StorageBackend",
        config: Optional[TTLConfig] = None,
        on_expire: Optional[Any] = None,
        index_bucket_size: float = 3600.0,  # 1小时一个桶
    ):
        """
        初始化时间索引TTL管理器

        Args:
            storage: 存储后端
            config: TTL配置
            on_expire: 过期回调
            index_bucket_size: 时间索引桶大小（秒）
        """
        super().__init__(storage, config, on_expire)

        self._index_bucket_size = index_bucket_size

        # 时间索引: bucket_time -> {memory_id: TTLIndexEntry}
        self._time_index: Dict[int, Dict[str, TTLIndexEntry]] = {}

        # 快速查找: memory_id -> bucket_time
        self._memory_to_bucket: Dict[str, int] = {}

        # 保护锁
        self._index_lock = asyncio.Lock()

        # 统计
        self._index_stats = {
            "index_size": 0,
            "buckets_count": 0,
            "last_scan_buckets": 0,
        }

    def _get_bucket_time(self, expiration_time: float) -> int:
        """获取时间桶"""
        return int(expiration_time // self._index_bucket_size) * int(self._index_bucket_size)

    async def add_with_ttl(self, item: Any, ttl: float) -> None:
        """
        添加带TTL的记忆

        Args:
            item: 记忆项
            ttl: TTL（秒）
        """
        # 调用父类方法
        # 注意：父类没有add_with_ttl方法，我们直接处理
        expiration = time.time() + ttl

        async with self._index_lock:
            # 计算时间桶
            bucket_time = self._get_bucket_time(expiration)

            # 创建索引条目
            entry = TTLIndexEntry(
                memory_id=item.memory_id,
                expiration_time=expiration,
                importance=getattr(item.importance, 'value', 3)
            )

            # 添加到索引
            if bucket_time not in self._time_index:
                self._time_index[bucket_time] = {}

            # 如果记忆已在其他桶中，先移除
            if item.memory_id in self._memory_to_bucket:
                old_bucket = self._memory_to_bucket[item.memory_id]
                if old_bucket in self._time_index:
                    self._time_index[old_bucket].pop(item.memory_id, None)

            # 添加到新桶
            self._time_index[bucket_time][item.memory_id] = entry
            self._memory_to_bucket[item.memory_id] = bucket_time

            # 更新统计
            self._index_stats["index_size"] = len(self._memory_to_bucket)
            self._index_stats["buckets_count"] = len(self._time_index)

    async def remove_from_index(self, memory_id: str) -> bool:
        """
        从索引中移除记忆

        Args:
            memory_id: 记忆ID

        Returns:
            是否成功移除
        """
        async with self._index_lock:
            if memory_id not in self._memory_to_bucket:
                return False

            bucket_time = self._memory_to_bucket[memory_id]

            if bucket_time in self._time_index:
                self._time_index[bucket_time].pop(memory_id, None)

                # 清理空桶
                if not self._time_index[bucket_time]:
                    del self._time_index[bucket_time]

            del self._memory_to_bucket[memory_id]

            # 更新统计
            self._index_stats["index_size"] = len(self._memory_to_bucket)
            self._index_stats["buckets_count"] = len(self._time_index)

            return True

    async def cleanup_expired(self) -> int:
        """
        清理过期记忆（增量式）

        只扫描时间索引中已过期的时间桶，避免全表扫描

        Returns:
            清理的记忆数量
        """
        if not self.config.enabled:
            return 0

        current_time = time.time()
        current_bucket = self._get_bucket_time(current_time)
        deleted_count = 0

        async with self._index_lock:
            # 找出所有已过期的时间桶
            expired_buckets = [
                bucket_time for bucket_time in self._time_index.keys()
                if bucket_time < current_bucket
            ]

            self._index_stats["last_scan_buckets"] = len(expired_buckets)

            # 处理每个过期桶
            for bucket_time in expired_buckets:
                bucket = self._time_index.get(bucket_time, {})
                expired_ids = []

                # 收集该桶中所有过期的记忆
                for memory_id, entry in list(bucket.items()):
                    if entry.expiration_time <= current_time:
                        expired_ids.append(memory_id)

                    # 检查是否达到单次最大删除数
                    if len(expired_ids) >= self.config.max_delete_per_run:
                        break

                # 批量删除
                for memory_id in expired_ids:
                    try:
                        if await self.storage.delete(memory_id):
                            deleted_count += 1
                            bucket.pop(memory_id, None)
                            self._memory_to_bucket.pop(memory_id, None)

                            # 调用过期回调
                            if self.on_expire:
                                try:
                                    if asyncio.iscoroutinefunction(self.on_expire):
                                        await self.on_expire(memory_id, {"timestamp": current_time})
                                    else:
                                        self.on_expire(memory_id, {"timestamp": current_time})
                                except Exception as e:
                                    logger.warning(f"过期回调执行失败 [{memory_id}]: {e}")

                    except Exception as e:
                        logger.warning(f"删除过期记忆失败 {memory_id}: {e}")

                # 清理空桶
                if not bucket:
                    del self._time_index[bucket_time]

                # 检查是否达到单次最大删除数
                if deleted_count >= self.config.max_delete_per_run:
                    break

        # 更新统计
        self._stats["total_cleaned"] += deleted_count
        self._stats["last_cleanup_time"] = current_time
        self._stats["last_cleanup_count"] = deleted_count

        # 更新索引统计
        self._index_stats["index_size"] = len(self._memory_to_bucket)
        self._index_stats["buckets_count"] = len(self._time_index)

        if deleted_count > 0:
            logger.info(f"TTL增量清理完成: 删除 {deleted_count} 条过期记忆, 扫描 {len(expired_buckets)} 个时间桶")

        return deleted_count

    async def get_expiration_info(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """
        获取记忆的过期信息

        Args:
            memory_id: 记忆ID

        Returns:
            过期信息字典
        """
        async with self._index_lock:
            if memory_id not in self._memory_to_bucket:
                # 回退到父类方法
                return await super().get_expiration_info(memory_id)

            bucket_time = self._memory_to_bucket[memory_id]
            bucket = self._time_index.get(bucket_time, {})
            entry = bucket.get(memory_id)

            if not entry:
                return None

            now = time.time()
            return {
                "memory_id": memory_id,
                "expiration_time": entry.expiration_time,
                "is_expired": now > entry.expiration_time,
                "seconds_remaining": max(0, entry.expiration_time - now),
                "importance": entry.importance,
            }

    async def preview_expired(self, limit: int = 100) -> List[str]:
        """
        预览即将过期的记忆

        Args:
            limit: 最大返回数量

        Returns:
            即将过期的记忆ID列表
        """
        current_time = time.time()
        current_bucket = self._get_bucket_time(current_time)
        expired = []

        async with self._index_lock:
            # 遍历当前和之前的桶
            for bucket_time in sorted(self._time_index.keys()):
                if bucket_time > current_bucket:
                    break

                bucket = self._time_index[bucket_time]
                for memory_id, entry in bucket.items():
                    if entry.expiration_time <= current_time:
                        expired.append(memory_id)
                        if len(expired) >= limit:
                            return expired

        return expired

    async def get_index_stats(self) -> Dict[str, Any]:
        """获取索引统计信息"""
        return {
            **self._index_stats,
            "bucket_size_seconds": self._index_bucket_size,
            "memory_efficiency": self._index_stats["index_size"] / max(self._index_stats["buckets_count"], 1),
        }

    async def rebuild_index(self) -> int:
        """
        重建时间索引

        从存储中加载所有记忆并重建索引

        Returns:
            索引的记忆数量
        """
        async with self._index_lock:
            # 清空现有索引
            self._time_index.clear()
            self._memory_to_bucket.clear()

            # 获取所有记忆
            all_memories = await self.storage.search_by_time_range(0, time.time() + 86400 * 365)

            indexed_count = 0
            for memory in all_memories:
                # 计算过期时间
                ttl_days = self._get_ttl_for_memory(memory)
                expiration = memory.timestamp + (ttl_days * 24 * 3600)

                # 添加到索引
                bucket_time = self._get_bucket_time(expiration)

                entry = TTLIndexEntry(
                    memory_id=memory.memory_id,
                    expiration_time=expiration,
                    importance=getattr(memory.importance, 'value', 3)
                )

                if bucket_time not in self._time_index:
                    self._time_index[bucket_time] = {}

                self._time_index[bucket_time][memory.memory_id] = entry
                self._memory_to_bucket[memory.memory_id] = bucket_time
                indexed_count += 1

            # 更新统计
            self._index_stats["index_size"] = len(self._memory_to_bucket)
            self._index_stats["buckets_count"] = len(self._time_index)

        logger.info(f"TTL时间索引重建完成: 索引了 {indexed_count} 条记忆")
        return indexed_count

    def _get_ttl_for_memory(self, memory) -> float:
        """获取记忆的TTL（天）"""
        # 优先使用元数据中的自定义TTL
        custom_ttl = memory.metadata.get("ttl_days")
        if custom_ttl is not None:
            return float(custom_ttl)

        # 按重要性获取TTL
        importance = memory.importance.value
        return self.config.ttl_by_importance.get(
            importance,
            self.config.default_ttl_days
        )

"""
智能缓存预热模块 (I-05)
基于访问模式在启动时预计算热门记忆的嵌入向量
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from collections import OrderedDict

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import (
        StorageBackend,
        EmbeddingProvider,
    )
    from memory_system.core_abstractions.memory_data_models import MemoryItem

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class CacheWarmer:
    """
    智能缓存预热器

    功能:
        - 跟踪记忆访问模式（哪些记忆被频繁访问）
        - 在启动时预计算Top-N访问记忆的嵌入向量
        - 使用后台任务异步预热缓存
        - 支持访问频率衰减，避免缓存过时数据

    使用示例:
        >>> warmer = CacheWarmer(storage, embedding, cache, top_n=100)
        >>> warmer.record_access("memory_id_123")
        >>> await warmer.warm_cache()  # 预热缓存
    """

    def __init__(
        self,
        storage: "StorageBackend",
        embedding: "EmbeddingProvider",
        cache: Dict[str, Any],
        top_n: int = 100,
        access_decay_factor: float = 0.9,
        min_access_count: int = 2,
    ):
        """
        初始化缓存预热器

        Args:
            storage: 存储后端
            embedding: 嵌入提供者
            cache: 缓存字典
            top_n: 预热时加载的热门记忆数量
            access_decay_factor: 访问计数衰减因子（用于定期衰减旧访问记录）
            min_access_count: 最小访问次数阈值，低于此值的记忆不会被预热
        """
        self.storage = storage
        self.embedding = embedding
        self.cache = cache
        self.top_n = top_n
        self.access_decay_factor = access_decay_factor
        self.min_access_count = min_access_count

        # 访问日志: memory_id -> 访问次数
        self._access_log: Dict[str, int] = {}
        # 访问时间戳: memory_id -> 最后访问时间
        self._access_timestamps: Dict[str, float] = {}
        # 锁保护
        self._lock = asyncio.Lock()

    def record_access(self, memory_id: str) -> None:
        """
        记录记忆访问

        Args:
            memory_id: 被访问的记忆ID
        """
        current_time = time.time()
        self._access_log[memory_id] = self._access_log.get(memory_id, 0) + 1
        self._access_timestamps[memory_id] = current_time

    def record_access_batch(self, memory_ids: List[str]) -> None:
        """
        批量记录记忆访问

        Args:
            memory_ids: 被访问的记忆ID列表
        """
        current_time = time.time()
        for memory_id in memory_ids:
            self._access_log[memory_id] = self._access_log.get(memory_id, 0) + 1
            self._access_timestamps[memory_id] = current_time

    async def warm_cache(self, progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """
        预热缓存 - 异步预计算热门记忆的嵌入向量

        Args:
            progress_callback: 进度回调函数，接收(current, total)参数

        Returns:
            预热统计信息
        """
        async with self._lock:
            # 衰减访问计数（避免过时数据占据缓存）
            await self._decay_access_counts()

            # 获取Top-N访问的记忆
            sorted_items = sorted(
                self._access_log.items(),
                key=lambda x: (x[1], self._access_timestamps.get(x[0], 0)),
                reverse=True
            )

            # 过滤掉访问次数低于阈值的记忆
            hot_memories = [
                (mid, count) for mid, count in sorted_items[:self.top_n]
                if count >= self.min_access_count
            ]

            if not hot_memories:
                logger.info("缓存预热: 没有足够热门的记忆需要预热")
                return {"warmed": 0, "skipped": 0, "errors": 0}

            logger.info(f"缓存预热: 开始预热 {len(hot_memories)} 条热门记忆")

            warmed = 0
            skipped = 0
            errors = 0

            for idx, (memory_id, access_count) in enumerate(hot_memories):
                try:
                    # 检查是否已在缓存中
                    memory = await self.storage.get(memory_id)
                    if not memory:
                        continue

                    cache_key = memory.content
                    if cache_key in self.cache:
                        skipped += 1
                        continue

                    # 预计算嵌入向量
                    embedding = await self.embedding.embed(memory.content)
                    self.cache[cache_key] = embedding
                    warmed += 1

                    if progress_callback:
                        progress_callback(idx + 1, len(hot_memories))

                    # 短暂休眠避免阻塞
                    if idx % 10 == 0:
                        await asyncio.sleep(0)

                except Exception as e:
                    logger.warning(f"缓存预热失败 [{memory_id}]: {e}")
                    errors += 1

            stats = {
                "warmed": warmed,
                "skipped": skipped,
                "errors": errors,
                "total_candidates": len(hot_memories),
            }
            logger.info(f"缓存预热完成: {warmed} 条已预热, {skipped} 条已存在, {errors} 条失败")
            return stats

    async def warm_cache_concurrent(
        self,
        max_concurrent: int = 5,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        并发预热缓存 - 使用限流并发提高预热速度

        Args:
            max_concurrent: 最大并发数
            progress_callback: 进度回调函数

        Returns:
            预热统计信息
        """
        async with self._lock:
            await self._decay_access_counts()

            sorted_items = sorted(
                self._access_log.items(),
                key=lambda x: (x[1], self._access_timestamps.get(x[0], 0)),
                reverse=True
            )

            hot_memories = [
                (mid, count) for mid, count in sorted_items[:self.top_n]
                if count >= self.min_access_count
            ]

            if not hot_memories:
                return {"warmed": 0, "skipped": 0, "errors": 0}

            semaphore = asyncio.Semaphore(max_concurrent)
            results = {"warmed": 0, "skipped": 0, "errors": 0}
            completed = 0

            async def warm_single(memory_id: str, access_count: int) -> None:
                nonlocal completed
                async with semaphore:
                    try:
                        memory = await self.storage.get(memory_id)
                        if not memory:
                            return

                        cache_key = memory.content
                        if cache_key in self.cache:
                            results["skipped"] += 1
                            return

                        embedding = await self.embedding.embed(memory.content)
                        self.cache[cache_key] = embedding
                        results["warmed"] += 1

                    except Exception as e:
                        logger.warning(f"并发缓存预热失败 [{memory_id}]: {e}")
                        results["errors"] += 1
                    finally:
                        completed += 1
                        if progress_callback:
                            progress_callback(completed, len(hot_memories))

            # 并发执行所有预热任务
            await asyncio.gather(*[
                warm_single(mid, count) for mid, count in hot_memories
            ])

            return results

    async def _decay_access_counts(self) -> None:
        """衰减访问计数，避免过时数据长期占据热门位置"""
        current_time = time.time()
        cutoff_time = current_time - 86400  # 24小时前的记录

        # 衰减所有访问计数
        for memory_id in list(self._access_log.keys()):
            last_access = self._access_timestamps.get(memory_id, 0)
            if last_access < cutoff_time:
                # 24小时未访问，衰减计数
                self._access_log[memory_id] *= self.access_decay_factor
                if self._access_log[memory_id] < 1:
                    del self._access_log[memory_id]
                    del self._access_timestamps[memory_id]

    def get_hot_memories(self, n: int = 10) -> List[tuple]:
        """
        获取最热门的记忆列表

        Args:
            n: 返回数量

        Returns:
            [(memory_id, access_count), ...] 列表
        """
        sorted_items = sorted(
            self._access_log.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return sorted_items[:n]

    def get_stats(self) -> Dict[str, Any]:
        """获取访问统计信息"""
        return {
            "total_tracked": len(self._access_log),
            "top_n_threshold": self.top_n,
            "min_access_count": self.min_access_count,
            "total_accesses": sum(self._access_log.values()),
            "hot_memories": self.get_hot_memories(5),
        }

    def clear_access_log(self) -> None:
        """清空访问日志"""
        self._access_log.clear()
        self._access_timestamps.clear()

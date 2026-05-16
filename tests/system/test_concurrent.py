"""
并发安全系统测试
"""
import pytest
import asyncio

from memory_system import MemorySystem
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from tests.conftest import MockEmbeddingProvider


def _create_memory_system():
    """创建测试用记忆系统"""
    embedding = MockEmbeddingProvider(dimension=64)
    storage = MemoryStorage(max_memories=10000)
    retrieval = HierarchicalRetrieval(strategy="adaptive")
    short_term = ShortTermMemory(capacity=100, ttl_seconds=300)
    return MemorySystem(
        storage=storage,
        embedding=embedding,
        retrieval=retrieval,
        short_term=short_term
    )


class TestConcurrentOperations:
    """并发操作测试"""

    @pytest.mark.asyncio
    async def test_concurrent_remember(self):
        """并发存储多条记忆"""
        memory = _create_memory_system()

        async def store_item(i):
            return await memory.remember(
                f"并发测试记忆{i}",
                memory_type="semantic",
                importance="medium"
            )

        # 并发存储50条
        results = await asyncio.gather(*[store_item(i) for i in range(50)])

        assert len(results) == 50
        assert all(r is not None for r in results)

        # 验证存储数量
        stats = await memory.get_stats()
        assert stats.total_added == 50

        await memory.close()

    @pytest.mark.asyncio
    async def test_concurrent_recall(self):
        """并发检索"""
        memory = _create_memory_system()

        # 先存储一些记忆
        for i in range(20):
            await memory.remember(
                f"并发检索测试{i}，包含关键词Python",
                memory_type="semantic",
                importance="high" if i % 5 == 0 else "medium"
            )

        # 并发检索
        async def search(query):
            return await memory.recall(query, max_results=5)

        queries = ["Python", "测试", "并发", "检索"]
        results = await asyncio.gather(*[search(q) for q in queries])

        assert len(results) == 4
        for result in results:
            assert "combined" in result

        await memory.close()

    @pytest.mark.asyncio
    async def test_mixed_concurrent_operations(self):
        """混合并发操作（存储+检索同时进行）"""
        memory = _create_memory_system()

        async def store_items():
            for i in range(10):
                await memory.remember(f"混合测试{i}", memory_type="semantic")

        async def search_items():
            for _ in range(5):
                await memory.recall("测试", max_results=3)

        # 同时执行存储和检索
        await asyncio.gather(store_items(), search_items())

        stats = await memory.get_stats()
        assert stats.total_added == 10

        await memory.close()

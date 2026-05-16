"""
测试 storage/memory_storage.py 中的 MemoryStorage
"""

import time
import pytest

from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel


class TestAddAndGet:
    """test_add_and_get: 添加后获取，验证内容一致"""

    @pytest.mark.asyncio
    async def test_add_and_get(self, memory_storage):
        item = MemoryItem(
            content="添加获取测试",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
        )
        memory_id = await memory_storage.add(item)
        assert memory_id == item.memory_id

        retrieved = await memory_storage.get(memory_id)
        assert retrieved is not None
        assert retrieved.content == "添加获取测试"
        assert retrieved.memory_type == MemoryType.EPISODIC
        assert retrieved.importance == ImportanceLevel.HIGH


class TestAddEpisodicAndSemantic:
    """test_add_episodic_and_semantic: 分别添加不同类型"""

    @pytest.mark.asyncio
    async def test_add_episodic_and_semantic(self, memory_storage):
        epi = MemoryItem(content="情景记忆", memory_type=MemoryType.EPISODIC)
        sem = MemoryItem(content="语义记忆", memory_type=MemoryType.SEMANTIC)

        epi_id = await memory_storage.add(epi)
        sem_id = await memory_storage.add(sem)

        epi_retrieved = await memory_storage.get(epi_id)
        sem_retrieved = await memory_storage.get(sem_id)

        assert epi_retrieved.memory_type == MemoryType.EPISODIC
        assert sem_retrieved.memory_type == MemoryType.SEMANTIC


class TestSearchByVector:
    """test_search_by_vector: 添加带 embedding 的记忆，搜索验证排序"""

    @pytest.mark.asyncio
    async def test_search_by_vector(self, memory_storage, mock_embedding):
        # 创建两条相似度不同的记忆
        vec_a = await mock_embedding.embed("Python编程语言")
        vec_b = await mock_embedding.embed("烹饪意大利面")

        item_a = MemoryItem(
            content="Python编程语言",
            memory_type=MemoryType.EPISODIC,
            embedding=vec_a,
        )
        item_b = MemoryItem(
            content="烹饪意大利面",
            memory_type=MemoryType.EPISODIC,
            embedding=vec_b,
        )
        await memory_storage.add(item_a)
        await memory_storage.add(item_b)

        # 搜索与 item_a 相似的
        query_vec = await mock_embedding.embed("Python开发")
        results = await memory_storage.search_by_vector(query_vec, top_k=2)

        assert len(results) >= 1
        # 第一条应该更接近 Python 相关
        assert results[0][0].content == "Python编程语言"
        assert results[0][1] > results[1][1]


class TestSearchByVectorZeroVector:
    """test_search_by_vector_zero_vector: 查询零向量，验证不崩溃"""

    @pytest.mark.asyncio
    async def test_search_by_vector_zero_vector(self, memory_storage, mock_embedding):
        vec = await mock_embedding.embed("测试内容")
        item = MemoryItem(content="测试内容", embedding=vec)
        await memory_storage.add(item)

        zero_vec = [0.0] * mock_embedding.dimension
        results = await memory_storage.search_by_vector(zero_vec, top_k=5)
        assert isinstance(results, list)


class TestSearchByVectorWithFilters:
    """test_search_by_vector_with_filters: 使用 memory_type 过滤"""

    @pytest.mark.asyncio
    async def test_search_by_vector_with_filters(self, memory_storage, mock_embedding):
        vec = await mock_embedding.embed("共享向量")

        epi = MemoryItem(
            content="情景记忆",
            memory_type=MemoryType.EPISODIC,
            embedding=vec,
        )
        sem = MemoryItem(
            content="语义记忆",
            memory_type=MemoryType.SEMANTIC,
            embedding=vec,
        )
        await memory_storage.add(epi)
        await memory_storage.add(sem)

        # 过滤只返回 episodic
        results = await memory_storage.search_by_vector(
            vec, top_k=10, filters={"memory_type": "episodic"}
        )
        assert len(results) == 1
        assert results[0][0].memory_type == MemoryType.EPISODIC


class TestSearchByTimeRange:
    """test_search_by_time_range: 按时间范围搜索"""

    @pytest.mark.asyncio
    async def test_search_by_time_range(self, memory_storage):
        now = time.time()

        old_item = MemoryItem(content="旧记忆", memory_type=MemoryType.EPISODIC)
        old_item.timestamp = now - 3600  # 1小时前
        await memory_storage.add(old_item)

        new_item = MemoryItem(content="新记忆", memory_type=MemoryType.EPISODIC)
        new_item.timestamp = now - 60  # 1分钟前
        await memory_storage.add(new_item)

        # 搜索最近10分钟内的
        results = await memory_storage.search_by_time_range(now - 600, now)
        assert len(results) == 1
        assert results[0].content == "新记忆"


class TestDelete:
    """test_delete: 删除后验证不存在"""

    @pytest.mark.asyncio
    async def test_delete(self, memory_storage):
        item = MemoryItem(content="待删除")
        mid = await memory_storage.add(item)
        assert await memory_storage.get(mid) is not None

        result = await memory_storage.delete(mid)
        assert result is True
        assert await memory_storage.get(mid) is None


class TestDeleteNonexistent:
    """test_delete_nonexistent: 删除不存在的 ID 返回 False"""

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, memory_storage):
        result = await memory_storage.delete("nonexistent_id_12345")
        assert result is False


class TestCount:
    """test_count: 统计总数和按类型统计"""

    @pytest.mark.asyncio
    async def test_count(self, memory_storage):
        await memory_storage.add(
            MemoryItem(content="e1", memory_type=MemoryType.EPISODIC)
        )
        await memory_storage.add(
            MemoryItem(content="e2", memory_type=MemoryType.EPISODIC)
        )
        await memory_storage.add(
            MemoryItem(content="s1", memory_type=MemoryType.SEMANTIC)
        )

        total = await memory_storage.count()
        assert total == 3

        epi_count = await memory_storage.count("episodic")
        assert epi_count == 2

        sem_count = await memory_storage.count("semantic")
        assert sem_count == 1


class TestCapacityEviction:
    """test_capacity_eviction: 超过 max_memories 时自动淘汰"""

    @pytest.mark.asyncio
    async def test_capacity_eviction(self):
        storage = __import__(
            "memory_system.storage_backends.in_memory_storage_backend", fromlist=["MemoryStorage"]
        ).MemoryStorage(max_memories=3)

        for i in range(5):
            item = MemoryItem(content=f"记忆{i}", memory_type=MemoryType.EPISODIC)
            await storage.add(item)

        total = await storage.count()
        assert total == 3

        await storage.close()


class TestClose:
    """test_close: 关闭后验证清空"""

    @pytest.mark.asyncio
    async def test_close(self, memory_storage):
        await memory_storage.add(MemoryItem(content="记忆1"))
        await memory_storage.add(MemoryItem(content="记忆2"))
        assert await memory_storage.count() == 2

        await memory_storage.close()
        assert await memory_storage.count() == 0

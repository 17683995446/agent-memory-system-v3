"""
V2.28.0 重构测试
测试 batch_delete、RememberResponse、remember_detailed、forget 方法、search_by_metadata、_search_by_importance 优化
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    RememberResponse,
)
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval


# ============================================================
# TestBatchDelete
# ============================================================
class TestBatchDelete:
    """测试 batch_delete 批量删除功能"""

    @pytest.mark.asyncio
    async def test_batch_delete_basic(self):
        """基本批量删除：删除存在的记忆"""
        storage = MemoryStorage()
        items = []
        for i in range(5):
            item = MemoryItem(
                content=f"记忆内容 {i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.MEDIUM,
            )
            await storage.add(item)
            items.append(item)

        ids = [item.memory_id for item in items]
        count = await storage.batch_delete(ids)
        assert count == 5
        # 验证全部已删除
        for mid in ids:
            assert await storage.get(mid) is None

    @pytest.mark.asyncio
    async def test_batch_delete_empty_list(self):
        """空列表批量删除"""
        storage = MemoryStorage()
        count = await storage.batch_delete([])
        assert count == 0

    @pytest.mark.asyncio
    async def test_batch_delete_partial_failure(self):
        """部分 ID 不存在时的批量删除"""
        storage = MemoryStorage()
        item = MemoryItem(
            content="存在的记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.MEDIUM,
        )
        await storage.add(item)

        ids = [item.memory_id, "nonexistent_id_1", "nonexistent_id_2"]
        count = await storage.batch_delete(ids)
        assert count == 1
        assert await storage.get(item.memory_id) is None

    @pytest.mark.asyncio
    async def test_batch_delete_all_nonexistent(self):
        """全部 ID 不存在"""
        storage = MemoryStorage()
        count = await storage.batch_delete(["id1", "id2", "id3"])
        assert count == 0

    @pytest.mark.asyncio
    async def test_batch_delete_single_lock_optimization(self):
        """验证优化版 batch_delete 使用单次锁（通过性能间接验证）"""
        storage = MemoryStorage()
        for i in range(20):
            item = MemoryItem(
                content=f"记忆 {i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.MEDIUM,
            )
            await storage.add(item)

        all_memories = await storage.list_memories(limit=100)
        ids = [m.memory_id for m in all_memories]
        count = await storage.batch_delete(ids)
        assert count == 20
        assert await storage.count() == 0


# ============================================================
# TestRememberResponse
# ============================================================
class TestRememberResponse:
    """测试 RememberResponse 数据模型"""

    def test_creation_with_required_fields(self):
        """仅必填字段创建"""
        resp = RememberResponse(
            memory_id="abc123",
            memory_type="semantic",
            importance="high",
        )
        assert resp.memory_id == "abc123"
        assert resp.memory_type == "semantic"
        assert resp.importance == "high"
        assert resp.importance_confidence is None
        assert resp.is_new is True
        assert resp.related_memories is None

    def test_creation_with_all_fields(self):
        """所有字段创建"""
        related = [{"memory_id": "r1", "content": "相关记忆", "score": 0.9}]
        resp = RememberResponse(
            memory_id="abc123",
            memory_type="episodic",
            importance="critical",
            importance_confidence=0.95,
            is_new=False,
            related_memories=related,
        )
        assert resp.importance_confidence == 0.95
        assert resp.is_new is False
        assert resp.related_memories == related

    def test_to_dict(self):
        """to_dict 转换"""
        resp = RememberResponse(
            memory_id="abc123",
            memory_type="semantic",
            importance="medium",
            importance_confidence=0.8,
            is_new=True,
            related_memories=None,
        )
        d = resp.to_dict()
        assert isinstance(d, dict)
        assert d["memory_id"] == "abc123"
        assert d["memory_type"] == "semantic"
        assert d["importance"] == "medium"
        assert d["importance_confidence"] == 0.8
        assert d["is_new"] is True
        assert d["related_memories"] is None

    def test_to_dict_with_related_memories(self):
        """to_dict 包含相关记忆"""
        related = [{"memory_id": "r1", "content": "test", "score": 0.5}]
        resp = RememberResponse(
            memory_id="x",
            memory_type="short_term",
            importance="low",
            related_memories=related,
        )
        d = resp.to_dict()
        assert d["related_memories"] == related

    def test_default_values(self):
        """默认值验证"""
        resp = RememberResponse(memory_id="id", memory_type="t", importance="m")
        assert resp.importance_confidence is None
        assert resp.is_new is True
        assert resp.related_memories is None


# ============================================================
# TestRememberDetailed
# ============================================================
class TestRememberDetailed:
    """测试 remember_detailed 方法"""

    def _create_mock_system(self, importance_scorer=None):
        """创建带 mock 的 MemorySystem 实例"""
        from memory_system.agent_memory_system_facade import MemorySystem
        from memory_system.working_memory_cache import ShortTermMemory

        storage = MemoryStorage()
        embedding = AsyncMock()
        embedding.dimension = 128
        embedding.embed = AsyncMock(return_value=[0.1] * 128)
        embedding.embed_batch = AsyncMock(return_value=[[0.1] * 128])
        embedding.similarity = AsyncMock(return_value=0.9)

        retrieval = HierarchicalRetrieval(strategy="fast")
        short_term = ShortTermMemory(capacity=100, ttl_seconds=300)

        system = MemorySystem(
            storage=storage,
            embedding=embedding,
            retrieval=retrieval,
            short_term=short_term,
            importance_scorer=importance_scorer,
        )
        return system

    @pytest.mark.asyncio
    async def test_remember_detailed_basic(self):
        """基本 remember_detailed 流程"""
        system = self._create_mock_system()
        resp = await system.remember_detailed("测试记忆内容", memory_type="episodic", importance="high")
        assert isinstance(resp, RememberResponse)
        assert resp.memory_type == "episodic"
        assert resp.importance == "high"
        assert resp.is_new is True
        assert resp.memory_id is not None

    @pytest.mark.asyncio
    async def test_remember_detailed_auto_type_selection(self):
        """auto 类型自动选择"""
        system = self._create_mock_system()
        # high importance -> semantic
        resp = await system.remember_detailed("重要内容", memory_type="auto", importance="high")
        assert resp.memory_type == "semantic"

        # medium importance -> short_term
        resp2 = await system.remember_detailed("普通内容", memory_type="auto", importance="medium")
        assert resp2.memory_type == "short_term"

    @pytest.mark.asyncio
    async def test_remember_detailed_with_scorer(self):
        """带 importance_scorer 的 remember_detailed"""
        scorer = AsyncMock()
        scorer.score = AsyncMock(return_value=(ImportanceLevel.CRITICAL, 0.92))
        system = self._create_mock_system(importance_scorer=scorer)

        resp = await system.remember_detailed("自动评估内容", importance="auto")
        assert resp.importance == "critical"
        assert resp.importance_confidence == 0.92
        scorer.score.assert_called_once()

    @pytest.mark.asyncio
    async def test_remember_detailed_empty_content_raises(self):
        """空内容抛出异常"""
        system = self._create_mock_system()
        with pytest.raises(ValueError, match="记忆内容不能为空"):
            await system.remember_detailed("")

    @pytest.mark.asyncio
    async def test_remember_detailed_invalid_importance_raises(self):
        """无效重要性级别抛出异常"""
        system = self._create_mock_system()
        with pytest.raises(ValueError, match="无效的重要性级别"):
            await system.remember_detailed("内容", importance="invalid_level")


# ============================================================
# TestForgetMethods
# ============================================================
class TestForgetMethods:
    """测试 forget 系列方法"""

    def _create_mock_system(self):
        """创建带 mock 的 MemorySystem 实例"""
        from memory_system.agent_memory_system_facade import MemorySystem
        from memory_system.working_memory_cache import ShortTermMemory

        storage = MemoryStorage()
        embedding = AsyncMock()
        embedding.dimension = 128
        embedding.embed = AsyncMock(return_value=[0.1] * 128)
        embedding.embed_batch = AsyncMock(return_value=[[0.1] * 128])
        embedding.similarity = AsyncMock(return_value=0.9)

        retrieval = HierarchicalRetrieval(strategy="fast")
        short_term = ShortTermMemory(capacity=100, ttl_seconds=300)

        system = MemorySystem(
            storage=storage,
            embedding=embedding,
            retrieval=retrieval,
            short_term=short_term,
        )
        return system

    @pytest.mark.asyncio
    async def test_forget_single(self):
        """遗忘单条记忆"""
        system = self._create_mock_system()
        memory_id = await system.remember("要遗忘的记忆", memory_type="episodic")
        assert await system.storage.get(memory_id) is not None

        result = await system.forget(memory_id)
        assert result is True
        assert await system.storage.get(memory_id) is None

    @pytest.mark.asyncio
    async def test_forget_nonexistent(self):
        """遗忘不存在的记忆"""
        system = self._create_mock_system()
        result = await system.forget("nonexistent_id")
        assert result is False

    @pytest.mark.asyncio
    async def test_forget_batch(self):
        """批量遗忘"""
        system = self._create_mock_system()
        ids = []
        for i in range(5):
            mid = await system.remember(f"记忆 {i}", memory_type="episodic")
            ids.append(mid)

        count = await system.forget_batch(ids)
        assert count == 5
        for mid in ids:
            assert await system.storage.get(mid) is None

    @pytest.mark.asyncio
    async def test_forget_batch_empty(self):
        """空列表批量遗忘"""
        system = self._create_mock_system()
        count = await system.forget_batch([])
        assert count == 0

    @pytest.mark.asyncio
    async def test_forget_by_query_no_match(self):
        """查询遗忘：无匹配结果"""
        system = self._create_mock_system()
        count = await system.forget_by_query("不存在的查询内容", min_similarity=0.99)
        assert count == 0

    @pytest.mark.asyncio
    async def test_forget_updates_stats(self):
        """遗忘后统计信息更新"""
        system = self._create_mock_system()
        await system.remember("记忆1", memory_type="episodic")
        await system.remember("记忆2", memory_type="episodic")

        stats_before = system._stats["total_added"]
        await system.forget_batch([])
        # 空删除不影响统计
        assert system._stats["total_added"] == stats_before


# ============================================================
# TestSearchByMetadata
# ============================================================
class TestSearchByMetadata:
    """测试 search_by_metadata 元数据搜索"""

    @pytest.mark.asyncio
    async def test_basic_search(self):
        """基本元数据搜索"""
        storage = MemoryStorage()
        item1 = MemoryItem(
            content="Python 相关",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.HIGH,
            metadata={"topic": "python", "level": "advanced"},
        )
        item2 = MemoryItem(
            content="Java 相关",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.MEDIUM,
            metadata={"topic": "java", "level": "beginner"},
        )
        await storage.add(item1)
        await storage.add(item2)

        results = await storage.search_by_metadata("topic", "python")
        assert len(results) == 1
        assert results[0].memory_id == item1.memory_id

    @pytest.mark.asyncio
    async def test_no_match(self):
        """无匹配结果"""
        storage = MemoryStorage()
        item = MemoryItem(
            content="测试",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.MEDIUM,
            metadata={"key": "value"},
        )
        await storage.add(item)

        results = await storage.search_by_metadata("key", "nonexistent")
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_with_type_filter(self):
        """带类型过滤的元数据搜索"""
        storage = MemoryStorage()
        item1 = MemoryItem(
            content="情景记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.MEDIUM,
            metadata={"category": "work"},
        )
        item2 = MemoryItem(
            content="语义记忆",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.MEDIUM,
            metadata={"category": "work"},
        )
        await storage.add(item1)
        await storage.add(item2)

        results = await storage.search_by_metadata("category", "work", memory_type="episodic")
        assert len(results) == 1
        assert results[0].memory_id == item1.memory_id

    @pytest.mark.asyncio
    async def test_top_k_limit(self):
        """top_k 限制"""
        storage = MemoryStorage()
        for i in range(10):
            item = MemoryItem(
                content=f"内容 {i}",
                memory_type=MemoryType.SEMANTIC,
                importance=ImportanceLevel.MEDIUM,
                metadata={"tag": "same_tag"},
            )
            await storage.add(item)

        results = await storage.search_by_metadata("tag", "same_tag", top_k=3)
        assert len(results) == 3


# ============================================================
# TestSearchByImportanceOptimized
# ============================================================
class TestSearchByImportanceOptimized:
    """测试 _search_by_importance 优化：验证使用 list_memories"""

    @pytest.mark.asyncio
    async def test_uses_list_memories(self):
        """验证 _search_by_importance 调用 list_memories 而非 search_by_time_range"""
        storage = MemoryStorage()

        # 添加不同重要性的记忆
        for importance in [ImportanceLevel.LOW, ImportanceLevel.MEDIUM, ImportanceLevel.HIGH, ImportanceLevel.CRITICAL]:
            item = MemoryItem(
                content=f"重要性 {importance.name} 的记忆",
                memory_type=MemoryType.EPISODIC,
                importance=importance,
            )
            await storage.add(item)

        retrieval = HierarchicalRetrieval(strategy="thorough")

        # Mock list_memories 验证调用参数
        original_list = storage.list_memories
        call_args = []

        async def tracked_list_memories(**kwargs):
            call_args.append(kwargs)
            return await original_list(**kwargs)

        storage.list_memories = tracked_list_memories

        results = await retrieval._search_by_importance(storage, max_results=2)

        # 验证调用了 list_memories 且参数正确
        assert len(call_args) == 1
        assert call_args[0]["order_by"] == "importance"
        assert call_args[0]["limit"] == 2

        # 验证结果按重要性排序
        assert len(results) == 2
        assert results[0].memory.importance.value >= results[1].memory.importance.value

    @pytest.mark.asyncio
    async def test_empty_storage(self):
        """空存储时返回空列表"""
        storage = MemoryStorage()
        retrieval = HierarchicalRetrieval(strategy="thorough")
        results = await retrieval._search_by_importance(storage, max_results=5)
        assert results == []

    @pytest.mark.asyncio
    async def test_returns_correct_count(self):
        """返回数量不超过 max_results"""
        storage = MemoryStorage()
        for i in range(10):
            item = MemoryItem(
                content=f"记忆 {i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel(i % 5 + 1),
            )
            await storage.add(item)

        retrieval = HierarchicalRetrieval(strategy="thorough")
        results = await retrieval._search_by_importance(storage, max_results=3)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_score_normalization(self):
        """验证分数归一化到 0-1"""
        storage = MemoryStorage()
        item = MemoryItem(
            content="关键记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.CRITICAL,
        )
        await storage.add(item)

        retrieval = HierarchicalRetrieval(strategy="thorough")
        results = await retrieval._search_by_importance(storage, max_results=1)
        assert len(results) == 1
        # CRITICAL = 5, score = 5/5 = 1.0
        assert results[0].score == 1.0
        assert results[0].layer == "importance"

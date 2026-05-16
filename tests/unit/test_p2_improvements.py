"""
P2 改进测试
覆盖: CachingEmbeddingProxy、RetrievalResponse、list_memories、recall strategy override
"""
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cachetools import TTLCache

from memory_system.core_abstractions.memory_data_models import (
    MemoryItem, MemoryType, ImportanceLevel, RetrievalResponse,
)
from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend, EmbeddingProvider,
)
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.agent_memory_system_facade import CachingEmbeddingProxy, MemorySystem


# ============================================================
# CachingEmbeddingProxy 测试
# ============================================================
class TestCachingEmbeddingProxy:
    """验证 CachingEmbeddingProxy 的缓存行为"""

    def _make_proxy(self):
        """创建带缓存的代理"""
        mock_embedding = AsyncMock(spec=EmbeddingProvider)
        mock_embedding.dimension = 64
        mock_embedding.embed = AsyncMock(return_value=[0.1] * 64)
        mock_embedding.embed_batch = AsyncMock(return_value=[[0.1] * 64, [0.2] * 64])
        mock_embedding.similarity = AsyncMock(return_value=0.95)
        cache = TTLCache(maxsize=128, ttl=3600)
        proxy = CachingEmbeddingProxy(mock_embedding, cache)
        return proxy, mock_embedding, cache

    @pytest.mark.asyncio
    async def test_cache_miss(self):
        """缓存未命中时应调用底层 embed"""
        proxy, mock_embedding, cache = self._make_proxy()
        result = await proxy.embed("hello world")
        assert result == [0.1] * 64
        mock_embedding.embed.assert_awaited_once_with("hello world")
        assert "hello world" in cache

    @pytest.mark.asyncio
    async def test_cache_hit(self):
        """缓存命中时不应调用底层 embed"""
        proxy, mock_embedding, cache = self._make_proxy()
        # 预热缓存
        cache["cached_text"] = [0.5] * 64
        result = await proxy.embed("cached_text")
        assert result == [0.5] * 64
        mock_embedding.embed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_embed_batch_partial_cache(self):
        """embed_batch 部分命中缓存时只对未命中的调用底层"""
        proxy, mock_embedding, cache = self._make_proxy()
        # 预热部分缓存
        cache["text_a"] = [0.9] * 64
        # text_b 不在缓存中

        mock_embedding.embed_batch = AsyncMock(return_value=[[0.3] * 64])

        results = await proxy.embed_batch(["text_a", "text_b"])
        assert len(results) == 2
        assert results[0] == [0.9] * 64  # 来自缓存
        assert results[1] == [0.3] * 64  # 来自底层调用

        mock_embedding.embed_batch.assert_awaited_once_with(["text_b"])

    @pytest.mark.asyncio
    async def test_embed_batch_all_cached(self):
        """embed_batch 全部命中缓存时不调用底层"""
        proxy, mock_embedding, cache = self._make_proxy()
        cache["a"] = [0.1] * 64
        cache["b"] = [0.2] * 64

        results = await proxy.embed_batch(["a", "b"])
        assert len(results) == 2
        mock_embedding.embed_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_embed_batch_none_cached(self):
        """embed_batch 全部未命中时传递全部文本给底层"""
        proxy, mock_embedding, cache = self._make_proxy()
        mock_embedding.embed_batch = AsyncMock(return_value=[[0.1] * 64, [0.2] * 64, [0.3] * 64])

        results = await proxy.embed_batch(["x", "y", "z"])
        assert len(results) == 3
        mock_embedding.embed_batch.assert_awaited_once_with(["x", "y", "z"])

    @pytest.mark.asyncio
    async def test_similarity_delegates(self):
        """similarity 应委托给底层"""
        proxy, mock_embedding, cache = self._make_proxy()
        result = await proxy.similarity([0.1] * 64, [0.2] * 64)
        assert result == 0.95
        mock_embedding.similarity.assert_awaited_once()

    def test_dimension_delegates(self):
        """dimension 应委托给底层"""
        proxy, mock_embedding, cache = self._make_proxy()
        assert proxy.dimension == 64


# ============================================================
# RetrievalResponse 测试
# ============================================================
class TestRetrievalResponse:
    """验证 RetrievalResponse 数据模型"""

    def test_creation_with_defaults(self):
        """使用默认值创建"""
        resp = RetrievalResponse(query="test", strategy="vector")
        assert resp.query == "test"
        assert resp.strategy == "vector"
        assert resp.layers == {}
        assert resp.combined == []
        assert resp.total_found == 0
        assert resp.retrieval_time == 0.0

    def test_creation_with_all_fields(self):
        """使用全部字段创建"""
        resp = RetrievalResponse(
            query="hello",
            strategy="hierarchical",
            layers={"episodic": [{"content": "a"}], "semantic": []},
            combined=[{"content": "a"}],
            total_found=5,
            retrieval_time=0.123,
        )
        assert resp.query == "hello"
        assert resp.strategy == "hierarchical"
        assert len(resp.layers) == 2
        assert resp.total_found == 5
        assert abs(resp.retrieval_time - 0.123) < 1e-9

    def test_to_dict(self):
        """to_dict 应返回完整字典"""
        resp = RetrievalResponse(
            query="q", strategy="s",
            layers={"l1": [1]}, combined=[1],
            total_found=3, retrieval_time=0.5,
        )
        d = resp.to_dict()
        assert isinstance(d, dict)
        assert d["query"] == "q"
        assert d["strategy"] == "s"
        assert d["layers"] == {"l1": [1]}
        assert d["combined"] == [1]
        assert d["total_found"] == 3
        assert d["retrieval_time"] == 0.5

    def test_from_dict(self):
        """from_dict 应正确创建对象"""
        data = {
            "query": "test_query",
            "strategy": "adaptive",
            "layers": {"episodic": [{"content": "x"}]},
            "combined": [{"content": "x"}],
            "total_found": 10,
            "retrieval_time": 0.42,
        }
        resp = RetrievalResponse.from_dict(data)
        assert resp.query == "test_query"
        assert resp.strategy == "adaptive"
        assert resp.total_found == 10
        assert abs(resp.retrieval_time - 0.42) < 1e-9

    def test_from_dict_missing_fields(self):
        """from_dict 缺少字段时使用默认值"""
        resp = RetrievalResponse.from_dict({"query": "q", "strategy": "s"})
        assert resp.query == "q"
        assert resp.strategy == "s"
        assert resp.layers == {}
        assert resp.combined == []
        assert resp.total_found == 0  # combined 为空，默认 0
        assert resp.retrieval_time == 0.0

    def test_from_dict_total_found_fallback(self):
        """from_dict 缺少 total_found 时应从 combined 长度推导"""
        data = {
            "query": "q",
            "strategy": "s",
            "combined": [{"a": 1}, {"b": 2}],
        }
        resp = RetrievalResponse.from_dict(data)
        assert resp.total_found == 2

    def test_round_trip(self):
        """to_dict -> from_dict 往返应保持数据一致"""
        original = RetrievalResponse(
            query="round_trip_test",
            strategy="vector_only",
            layers={"layer1": [{"score": 0.9}]},
            combined=[{"score": 0.9}],
            total_found=1,
            retrieval_time=0.01,
        )
        restored = RetrievalResponse.from_dict(original.to_dict())
        assert restored.query == original.query
        assert restored.strategy == original.strategy
        assert restored.layers == original.layers
        assert restored.combined == original.combined
        assert restored.total_found == original.total_found
        assert abs(restored.retrieval_time - original.retrieval_time) < 1e-9


# ============================================================
# list_memories 测试
# ============================================================
class TestListMemories:
    """验证 list_memories 分页、过滤、排序功能"""

    @pytest.fixture
    async def populated_storage(self):
        """创建包含多种记忆的 MemoryStorage"""
        storage = MemoryStorage(max_memories=100)
        now = time.time()
        items = [
            MemoryItem(
                content=f"episodic_{i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel(i % 5 + 1),
                timestamp=now - i * 100,
                access_count=i,
            )
            for i in range(5)
        ]
        items += [
            MemoryItem(
                content=f"semantic_{i}",
                memory_type=MemoryType.SEMANTIC,
                importance=ImportanceLevel((i + 2) % 5 + 1),
                timestamp=now - i * 50 - 500,
                access_count=i * 2,
            )
            for i in range(3)
        ]
        for item in items:
            await storage.add(item)
        return storage

    @pytest.mark.asyncio
    async def test_pagination_basic(self, populated_storage):
        """基本分页：limit 和 offset"""
        page1 = await populated_storage.list_memories(limit=3, offset=0)
        page2 = await populated_storage.list_memories(limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        # 确保没有重叠
        page1_ids = {m.memory_id for m in page1}
        page2_ids = {m.memory_id for m in page2}
        assert page1_ids.isdisjoint(page2_ids)

    @pytest.mark.asyncio
    async def test_pagination_beyond_end(self, populated_storage):
        """offset 超出范围应返回空列表"""
        result = await populated_storage.list_memories(limit=10, offset=999)
        assert result == []

    @pytest.mark.asyncio
    async def test_filter_by_episodic(self, populated_storage):
        """按 episodic 类型过滤"""
        result = await populated_storage.list_memories(memory_type="episodic")
        assert len(result) == 5
        assert all(m.memory_type == MemoryType.EPISODIC for m in result)

    @pytest.mark.asyncio
    async def test_filter_by_semantic(self, populated_storage):
        """按 semantic 类型过滤"""
        result = await populated_storage.list_memories(memory_type="semantic")
        assert len(result) == 3
        assert all(m.memory_type == MemoryType.SEMANTIC for m in result)

    @pytest.mark.asyncio
    async def test_filter_no_match(self, populated_storage):
        """过滤不存在的类型应返回空列表"""
        result = await populated_storage.list_memories(memory_type="nonexistent")
        assert result == []

    @pytest.mark.asyncio
    async def test_sort_by_created_at_descending(self, populated_storage):
        """按 created_at 降序（默认）"""
        result = await populated_storage.list_memories(order_by="created_at", ascending=False)
        for i in range(len(result) - 1):
            assert result[i].timestamp >= result[i + 1].timestamp

    @pytest.mark.asyncio
    async def test_sort_by_created_at_ascending(self, populated_storage):
        """按 created_at 升序"""
        result = await populated_storage.list_memories(order_by="created_at", ascending=True)
        for i in range(len(result) - 1):
            assert result[i].timestamp <= result[i + 1].timestamp

    @pytest.mark.asyncio
    async def test_sort_by_importance(self, populated_storage):
        """按 importance 降序"""
        result = await populated_storage.list_memories(order_by="importance", ascending=False)
        for i in range(len(result) - 1):
            assert result[i].importance.value >= result[i + 1].importance.value

    @pytest.mark.asyncio
    async def test_sort_by_access_count(self, populated_storage):
        """按 access_count 降序"""
        result = await populated_storage.list_memories(order_by="access_count", ascending=False)
        for i in range(len(result) - 1):
            assert result[i].access_count >= result[i + 1].access_count

    @pytest.mark.asyncio
    async def test_filter_and_sort_combined(self, populated_storage):
        """过滤 + 排序组合"""
        result = await populated_storage.list_memories(
            memory_type="episodic",
            order_by="importance",
            ascending=True,
        )
        assert all(m.memory_type == MemoryType.EPISODIC for m in result)
        for i in range(len(result) - 1):
            assert result[i].importance.value <= result[i + 1].importance.value

    @pytest.mark.asyncio
    async def test_empty_storage(self):
        """空存储应返回空列表"""
        storage = MemoryStorage(max_memories=10)
        result = await storage.list_memories()
        assert result == []
        await storage.close()


# ============================================================
# recall strategy override 测试
# ============================================================
class TestRecallStrategyOverride:
    """验证 recall() 的 strategy 参数确实改变检索行为"""

    @pytest.fixture
    def memory_system_with_mock_retrieval(self):
        """创建带有 mock retrieval 的 MemorySystem"""
        mock_storage = AsyncMock()
        mock_storage.add = AsyncMock(return_value="mem_001")
        mock_storage.count = AsyncMock(return_value=0)
        mock_storage.close = AsyncMock()
        mock_storage.search_by_time_range = AsyncMock(return_value=[])

        mock_embedding = AsyncMock()
        mock_embedding.embed = AsyncMock(return_value=[0.1] * 64)
        mock_embedding.dimension = 64

        mock_retrieval = AsyncMock()
        mock_retrieval.retrieve = AsyncMock(return_value={
            "query": "test",
            "strategy": "default",
            "combined": [],
        })
        mock_retrieval.strategy = "default"

        mock_short_term = AsyncMock()
        mock_short_term.add = AsyncMock(
            return_value=MemoryItem(content="test", memory_type=MemoryType.SHORT_TERM)
        )
        mock_short_term.get_all = AsyncMock(return_value=[])

        mock_metrics = MagicMock()
        mock_metrics.record_recall = MagicMock()

        ms = MemorySystem(
            storage=mock_storage,
            embedding=mock_embedding,
            retrieval=mock_retrieval,
            short_term=mock_short_term,
            metrics=mock_metrics,
        )
        return ms

    @pytest.mark.asyncio
    async def test_strategy_override_passed_to_context(self, memory_system_with_mock_retrieval):
        """strategy 参数应通过 context.strategy_override 传递给检索策略"""
        ms = memory_system_with_mock_retrieval
        await ms.recall("test query", strategy="fast")

        # 验证 retrieve 被调用，且 context 中包含 strategy_override
        ms.retrieval.retrieve.assert_awaited_once()
        call_kwargs = ms.retrieval.retrieve.call_args[1]
        assert "context" in call_kwargs
        assert call_kwargs["context"]["strategy_override"] == "fast"

    @pytest.mark.asyncio
    async def test_no_strategy_override_when_same(self, memory_system_with_mock_retrieval):
        """strategy 与当前策略相同时不应设置 override"""
        ms = memory_system_with_mock_retrieval
        await ms.recall("test query", strategy="default")

        call_kwargs = ms.retrieval.retrieve.call_args[1]
        assert "strategy_override" not in call_kwargs["context"]

    @pytest.mark.asyncio
    async def test_no_strategy_override_when_none(self, memory_system_with_mock_retrieval):
        """strategy 为 None 时不应设置 override"""
        ms = memory_system_with_mock_retrieval
        await ms.recall("test query")

        call_kwargs = ms.retrieval.retrieve.call_args[1]
        assert "strategy_override" not in call_kwargs["context"]

    @pytest.mark.asyncio
    async def test_recall_uses_caching_proxy(self, memory_system_with_mock_retrieval):
        """recall 应使用 CachingEmbeddingProxy 包装 embedding"""
        ms = memory_system_with_mock_retrieval
        await ms.recall("test query")

        call_kwargs = ms.retrieval.retrieve.call_args[1]
        assert isinstance(call_kwargs["embedding"], CachingEmbeddingProxy)

    @pytest.mark.asyncio
    async def test_recall_records_metrics(self, memory_system_with_mock_retrieval):
        """recall 应记录指标"""
        ms = memory_system_with_mock_retrieval
        await ms.recall("test query")

        ms.metrics.record_recall.assert_called_once()

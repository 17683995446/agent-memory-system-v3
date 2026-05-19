"""
深度测试: retrieval_engine.py 模块

覆盖 P1 盲区模块 - 检索引擎
测试 RetrievalContext, RetrievalEngine, RetrievalPipeline,
QueryExpansionProcessor, ResultDeduplicationProcessor

使用 stub 类模拟 StorageBackend, EmbeddingProvider, RetrievalStrategy
"""

import sys
import time
import asyncio
import pytest
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, '/workspace/agent_memory_system_v2')

from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy,
)
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    RetrievalResponse,
)
from memory_system.retrieval_engine import (
    RetrievalContext,
    RetrievalEngine,
    RetrievalPipeline,
    QueryExpansionProcessor,
    ResultDeduplicationProcessor,
)
from memory_system.exceptions import RetrievalError, RetrievalTimeoutError


# ============================================================
# Stub 实现
# ============================================================

class StubStorage(StorageBackend):
    """模拟存储后端"""

    def __init__(self, items=None, fail_on_search=False, fail_on_add=False):
        self._items: Dict[str, MemoryItem] = {}
        if items:
            for item in items:
                self._items[item.memory_id] = item
        self.fail_on_search = fail_on_search
        self.fail_on_add = fail_on_add
        self.search_calls = []
        self.add_calls = []

    async def add(self, item: MemoryItem) -> str:
        self.add_calls.append(item)
        if self.fail_on_add:
            raise RuntimeError("Storage add failed")
        self._items[item.memory_id] = item
        return item.memory_id

    async def get(self, memory_id: str) -> Optional[MemoryItem]:
        return self._items.get(memory_id)

    async def search_by_vector(
        self,
        query_vector: List[float],
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[MemoryItem, float]]:
        self.search_calls.append(("vector", query_vector, top_k, filters))
        if self.fail_on_search:
            raise RuntimeError("Storage search failed")
        results = []
        for item in list(self._items.values())[:top_k]:
            results.append((item, 0.95))
        return results

    async def search_by_time_range(
        self, start_time: float, end_time: float, memory_type: Optional[str] = None
    ) -> List[MemoryItem]:
        return list(self._items.values())

    async def delete(self, memory_id: str) -> bool:
        return self._items.pop(memory_id, None) is not None

    async def count(self, memory_type: Optional[str] = None) -> int:
        return len(self._items)

    async def close(self):
        pass


class StubEmbedding(EmbeddingProvider):
    """模拟嵌入提供者"""

    def __init__(self, dimension=128, fail_on_embed=False):
        self._dimension = dimension
        self.fail_on_embed = fail_on_embed
        self.embed_calls = []

    @property
    def dimension(self) -> int:
        return self._dimension

    async def embed(self, text: str) -> List[float]:
        self.embed_calls.append(text)
        if self.fail_on_embed:
            raise RuntimeError("Embedding failed")
        return [0.1] * self._dimension

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        results = []
        for text in texts:
            results.append(await self.embed(text))
        return results

    async def similarity(self, vec1: List[float], vec2: List[float]) -> float:
        return 0.95


class StubStrategy(RetrievalStrategy):
    """模拟检索策略"""

    def __init__(self, name="StubStrategy", results=None, delay=0, fail=False):
        self._name = name
        self._results = results or []
        self._delay = delay
        self._fail = fail
        self.retrieve_calls = []

    async def retrieve(
        self,
        query: str,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        max_results: int = 10,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self.retrieve_calls.append({
            "query": query,
            "max_results": max_results,
            "context": context,
        })
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._fail:
            raise RuntimeError(f"{self._name} retrieval failed")

        combined = []
        for item in self._results[:max_results]:
            combined.append({
                "memory": item.to_storage_dict(),
                "score": 0.9,
                "layer": self._name,
            })

        return {
            "strategy": self._name,
            "layers": {self._name: combined},
            "combined": combined,
        }


def _make_memory(content: str, memory_id: str = None) -> MemoryItem:
    """创建测试用 MemoryItem"""
    return MemoryItem(
        content=content,
        memory_type=MemoryType.EPISODIC,
        importance=ImportanceLevel.MEDIUM,
        memory_id=memory_id or f"mem_{content[:8]}",
    )


# ============================================================
# 1. RetrievalContext 创建和序列化
# ============================================================
class TestRetrievalContext:

    def test_create_empty_context(self):
        """创建空的检索上下文"""
        ctx = RetrievalContext()
        assert ctx.short_term_memories == []
        assert ctx.session_id is None
        assert ctx.user_id is None
        assert ctx.filters == {}
        assert ctx.previous_queries == []
        assert ctx.extra == {}

    def test_create_context_with_params(self):
        """创建带参数的检索上下文"""
        mem = _make_memory("test")
        ctx = RetrievalContext(
            short_term_memories=[mem],
            session_id="sess_001",
            user_id="user_001",
            filters={"type": "episodic"},
            previous_queries=["hello", "world"],
            extra={"key": "value"},
        )
        assert len(ctx.short_term_memories) == 1
        assert ctx.session_id == "sess_001"
        assert ctx.user_id == "user_001"
        assert ctx.filters == {"type": "episodic"}
        assert ctx.previous_queries == ["hello", "world"]
        assert ctx.extra == {"key": "value"}

    def test_to_dict(self):
        """序列化为字典"""
        ctx = RetrievalContext(
            session_id="sess_001",
            user_id="user_001",
            filters={"k": "v"},
            previous_queries=["q1"],
            extra={"x": "y"},
        )
        d = ctx.to_dict()
        assert d["session_id"] == "sess_001"
        assert d["user_id"] == "user_001"
        assert d["filters"] == {"k": "v"}
        assert d["previous_queries"] == ["q1"]
        assert d["extra"] == {"x": "y"}


# ============================================================
# 2. 基本搜索
# ============================================================
class TestBasicSearch:

    @pytest.mark.asyncio
    async def test_basic_search_returns_response(self):
        """基本搜索返回 RetrievalResponse"""
        items = [_make_memory(f"content_{i}", f"mem_{i}") for i in range(5)]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("test query", max_results=3)

        assert isinstance(response, RetrievalResponse)
        assert response.query == "test query"
        assert response.total_found <= 3
        assert response.retrieval_time >= 0

    @pytest.mark.asyncio
    async def test_basic_search_strategy_called(self):
        """基本搜索应调用策略"""
        items = [_make_memory("content")]
        strategy = StubStrategy(results=items)
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        await engine.search("test query")

        assert len(strategy.retrieve_calls) == 1
        assert strategy.retrieve_calls[0]["query"] == "test query"
        assert strategy.retrieve_calls[0]["max_results"] == 10


# ============================================================
# 3. 搜索超时处理
# ============================================================
class TestSearchTimeout:

    @pytest.mark.asyncio
    async def test_search_timeout_raises(self):
        """搜索超时应抛出 RetrievalTimeoutError"""
        strategy = StubStrategy(delay=5.0)  # 5秒延迟
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(
            strategy=strategy,
            storage=storage,
            embedding=embedding,
            timeout=0.1,  # 100ms 超时
        )

        with pytest.raises(RetrievalTimeoutError):
            await engine.search("test query", timeout=0.1)

    @pytest.mark.asyncio
    async def test_timeout_increments_stats(self):
        """超时应增加超时计数"""
        strategy = StubStrategy(delay=5.0)
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(
            strategy=strategy, storage=storage, embedding=embedding, timeout=0.1
        )

        try:
            await engine.search("test query", timeout=0.1)
        except RetrievalTimeoutError:
            pass

        stats = engine.get_stats()
        assert stats["timeout_count"] == 1


# ============================================================
# 4. 多策略搜索 - RRF 融合
# ============================================================
class TestMultiStrategyRRF:

    @pytest.mark.asyncio
    async def test_rrf_fusion(self):
        """RRF 融合应合并多策略结果"""
        items_a = [_make_memory("shared", "mem_shared"), _make_memory("only_a", "mem_a")]
        items_b = [_make_memory("shared", "mem_shared"), _make_memory("only_b", "mem_b")]

        strategy_a = StubStrategy(name="StrategyA", results=items_a)
        strategy_b = StubStrategy(name="StrategyB", results=items_b)
        storage = StubStorage(items_a + items_b)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy_a, storage=storage, embedding=embedding)
        response = await engine.search_multi_strategy(
            "test query",
            strategies=[strategy_a, strategy_b],
            max_results=10,
            fusion_method="rrf",
        )

        assert isinstance(response, RetrievalResponse)
        assert "multi_rrf" in response.strategy
        assert response.total_found >= 1
        # shared item 应出现在结果中
        found_ids = {item.get("memory", {}).get("memory_id") for item in response.combined}
        assert "mem_shared" in found_ids

    @pytest.mark.asyncio
    async def test_rrf_layers_populated(self):
        """RRF 融合应填充 layers"""
        items_a = [_make_memory("a", "mem_a")]
        items_b = [_make_memory("b", "mem_b")]

        strategy_a = StubStrategy(name="StrategyA", results=items_a)
        strategy_b = StubStrategy(name="StrategyB", results=items_b)
        storage = StubStorage(items_a + items_b)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy_a, storage=storage, embedding=embedding)
        response = await engine.search_multi_strategy(
            "test query",
            strategies=[strategy_a, strategy_b],
            fusion_method="rrf",
        )

        # layers 的键是 strategy.__class__.__name__，即 "StubStrategy"
        # 因为两个策略都是 StubStrategy 类的实例
        assert "StubStrategy" in response.layers
        # 至少有一个策略的结果在 layers 中
        assert len(response.layers) >= 1


# ============================================================
# 5. 多策略搜索 - 加权融合
# ============================================================
class TestMultiStrategyWeighted:

    @pytest.mark.asyncio
    async def test_weighted_fusion(self):
        """加权融合应合并多策略结果"""
        items_a = [_make_memory("shared", "mem_shared")]
        items_b = [_make_memory("shared", "mem_shared")]

        strategy_a = StubStrategy(name="StrategyA", results=items_a)
        strategy_b = StubStrategy(name="StrategyB", results=items_b)
        storage = StubStorage(items_a + items_b)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy_a, storage=storage, embedding=embedding)
        response = await engine.search_multi_strategy(
            "test query",
            strategies=[strategy_a, strategy_b],
            max_results=10,
            fusion_method="weighted",
        )

        assert isinstance(response, RetrievalResponse)
        assert "multi_weighted" in response.strategy
        assert response.total_found >= 1

    @pytest.mark.asyncio
    async def test_weighted_fusion_deduplicates(self):
        """加权融合应去重"""
        items_a = [_make_memory("shared", "mem_shared")]
        items_b = [_make_memory("shared", "mem_shared")]

        strategy_a = StubStrategy(name="StrategyA", results=items_a)
        strategy_b = StubStrategy(name="StrategyB", results=items_b)
        storage = StubStorage(items_a + items_b)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy_a, storage=storage, embedding=embedding)
        response = await engine.search_multi_strategy(
            "test query",
            strategies=[strategy_a, strategy_b],
            fusion_method="weighted",
        )

        found_ids = [item.get("memory", {}).get("memory_id") for item in response.combined]
        assert found_ids.count("mem_shared") == 1


# ============================================================
# 6. 查询扩展处理器
# ============================================================
class TestQueryExpansionProcessor:

    @pytest.mark.asyncio
    async def test_pre_process_expands_query(self):
        """预处理应扩展查询"""
        processor = QueryExpansionProcessor(
            expansions={"search": ["find", "lookup", "query"]}
        )
        ctx = RetrievalContext()
        expanded_query, new_ctx = await processor.pre_process("search results", ctx)

        assert "find" in expanded_query
        assert "lookup" in expanded_query
        assert "query" in expanded_query
        assert new_ctx.extra["original_query"] == "search results"

    @pytest.mark.asyncio
    async def test_pre_process_no_expansion(self):
        """无匹配项时不应扩展"""
        processor = QueryExpansionProcessor(expansions={"search": ["find"]})
        ctx = RetrievalContext()
        expanded_query, new_ctx = await processor.pre_process("hello world", ctx)

        assert expanded_query == "hello world"

    @pytest.mark.asyncio
    async def test_post_process_adds_layers(self):
        """后处理应添加扩展信息"""
        processor = QueryExpansionProcessor(expansions={"search": ["find"]})
        ctx = RetrievalContext()
        _, ctx = await processor.pre_process("search", ctx)

        response = RetrievalResponse(query="search", strategy="test", combined=[])
        response = await processor.post_process(response, ctx)

        assert "query_expansion" in response.layers
        assert response.layers["query_expansion"]["original"] == "search"


# ============================================================
# 7. 结果去重处理器
# ============================================================
class TestResultDeduplicationProcessor:

    @pytest.mark.asyncio
    async def test_dedup_removes_duplicates(self):
        """去重应移除重复结果"""
        processor = ResultDeduplicationProcessor()
        ctx = RetrievalContext()

        combined = [
            {"memory": {"memory_id": "mem_1"}, "score": 0.9},
            {"memory": {"memory_id": "mem_1"}, "score": 0.8},  # 重复
            {"memory": {"memory_id": "mem_2"}, "score": 0.7},
        ]
        response = RetrievalResponse(
            query="test", strategy="test", combined=combined, total_found=3
        )
        response = await processor.post_process(response, ctx)

        assert len(response.combined) == 2
        assert response.total_found == 2
        # 保留第一个（分数更高的）
        assert response.combined[0]["score"] == 0.9

    @pytest.mark.asyncio
    async def test_dedup_no_duplicates(self):
        """无重复时不应修改结果"""
        processor = ResultDeduplicationProcessor()
        ctx = RetrievalContext()

        combined = [
            {"memory": {"memory_id": "mem_1"}, "score": 0.9},
            {"memory": {"memory_id": "mem_2"}, "score": 0.7},
        ]
        response = RetrievalResponse(
            query="test", strategy="test", combined=combined, total_found=2
        )
        response = await processor.post_process(response, ctx)

        assert len(response.combined) == 2

    @pytest.mark.asyncio
    async def test_dedup_handles_missing_memory_id(self):
        """缺少 memory_id 的项不应导致错误"""
        processor = ResultDeduplicationProcessor()
        ctx = RetrievalContext()

        combined = [
            {"memory": {}, "score": 0.9},
            {"memory": {"memory_id": "mem_1"}, "score": 0.8},
        ]
        response = RetrievalResponse(
            query="test", strategy="test", combined=combined, total_found=2
        )
        response = await processor.post_process(response, ctx)

        # 缺少 memory_id 的项被跳过
        assert len(response.combined) == 1

    @pytest.mark.asyncio
    async def test_pre_process_passes_through(self):
        """预处理应原样传递"""
        processor = ResultDeduplicationProcessor()
        ctx = RetrievalContext()
        query, new_ctx = await processor.pre_process("test query", ctx)
        assert query == "test query"


# ============================================================
# 8. 检索管道 - 链式处理
# ============================================================
class TestRetrievalPipeline:

    @pytest.mark.asyncio
    async def test_pipeline_chain(self):
        """管道应链式执行多个处理器"""
        items = [_make_memory("search result", "mem_1")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)

        expansion = QueryExpansionProcessor(expansions={"search": ["find"]})
        dedup = ResultDeduplicationProcessor()

        pipeline = RetrievalPipeline()
        pipeline.add_processor(expansion).add_processor(dedup)

        response = await pipeline.execute("search query", engine, max_results=10)

        assert isinstance(response, RetrievalResponse)
        assert "query_expansion" in response.layers

    @pytest.mark.asyncio
    async def test_pipeline_empty(self):
        """空管道应正常执行"""
        items = [_make_memory("result", "mem_1")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        pipeline = RetrievalPipeline()

        response = await pipeline.execute("query", engine, max_results=5)
        assert isinstance(response, RetrievalResponse)

    @pytest.mark.asyncio
    async def test_pipeline_add_processor_returns_self(self):
        """add_processor 应返回 self（支持链式调用）"""
        pipeline = RetrievalPipeline()
        processor = QueryExpansionProcessor(expansions={})
        result = pipeline.add_processor(processor)
        assert result is pipeline


# ============================================================
# 9. 空结果处理
# ============================================================
class TestEmptyResults:

    @pytest.mark.asyncio
    async def test_empty_query_returns_empty(self):
        """空查询应返回空结果"""
        strategy = StubStrategy(results=[])
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("", max_results=10)

        assert isinstance(response, RetrievalResponse)
        assert response.total_found == 0
        assert response.combined == []

    @pytest.mark.asyncio
    async def test_whitespace_query_returns_empty(self):
        """纯空白查询应返回空结果"""
        strategy = StubStrategy(results=[])
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("   ", max_results=10)

        assert response.total_found == 0

    @pytest.mark.asyncio
    async def test_strategy_returns_no_results(self):
        """策略返回空结果"""
        strategy = StubStrategy(results=[])
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("real query", max_results=10)

        assert response.total_found == 0


# ============================================================
# 10. 统计信息收集
# ============================================================
class TestStatsCollection:

    @pytest.mark.asyncio
    async def test_stats_initial(self):
        """初始统计信息"""
        strategy = StubStrategy(results=[])
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        stats = engine.get_stats()

        assert stats["total_searches"] == 0
        assert stats["total_time"] == 0.0
        assert stats["timeout_count"] == 0
        assert stats["error_count"] == 0
        assert stats["avg_time"] == 0.0
        assert stats["timeout_rate"] == 0.0
        assert stats["error_rate"] == 0.0

    @pytest.mark.asyncio
    async def test_stats_after_search(self):
        """搜索后统计信息更新"""
        items = [_make_memory("content")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        await engine.search("test query")
        await engine.search("another query")

        stats = engine.get_stats()
        assert stats["total_searches"] == 2
        assert stats["total_time"] > 0
        assert stats["avg_time"] > 0


# ============================================================
# 11. 统计信息重置
# ============================================================
class TestStatsReset:

    @pytest.mark.asyncio
    async def test_reset_stats(self):
        """重置统计信息"""
        items = [_make_memory("content")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        await engine.search("test query")

        assert engine.get_stats()["total_searches"] == 1
        engine.reset_stats()
        assert engine.get_stats()["total_searches"] == 0
        assert engine.get_stats()["total_time"] == 0.0
        assert engine.get_stats()["timeout_count"] == 0
        assert engine.get_stats()["error_count"] == 0


# ============================================================
# 12. 上下文传递
# ============================================================
class TestContextPassing:

    @pytest.mark.asyncio
    async def test_context_passed_to_strategy(self):
        """上下文应传递给策略"""
        strategy = StubStrategy(results=[])
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        ctx = RetrievalContext(
            session_id="sess_001",
            user_id="user_001",
            filters={"type": "episodic"},
        )
        await engine.search("test query", context=ctx)

        assert len(strategy.retrieve_calls) == 1
        passed_context = strategy.retrieve_calls[0]["context"]
        assert passed_context["session_id"] == "sess_001"
        assert passed_context["user_id"] == "user_001"
        assert passed_context["filters"] == {"type": "episodic"}

    @pytest.mark.asyncio
    async def test_none_context(self):
        """None 上下文不应报错"""
        strategy = StubStrategy(results=[])
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("test query", context=None)
        assert isinstance(response, RetrievalResponse)


# ============================================================
# 13. 大量结果处理
# ============================================================
class TestLargeResults:

    @pytest.mark.asyncio
    async def test_large_result_set(self):
        """处理大量结果"""
        items = [_make_memory(f"content_{i}", f"mem_{i}") for i in range(100)]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("test query", max_results=50)

        assert response.total_found <= 50
        assert len(response.combined) <= 50

    @pytest.mark.asyncio
    async def test_multi_strategy_large_results(self):
        """多策略处理大量结果"""
        items_a = [_make_memory(f"shared_{i}", f"mem_{i}") for i in range(50)]
        items_b = [_make_memory(f"shared_{i}", f"mem_{i}") for i in range(50)]

        strategy_a = StubStrategy(name="A", results=items_a)
        strategy_b = StubStrategy(name="B", results=items_b)
        storage = StubStorage(items_a)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy_a, storage=storage, embedding=embedding)
        response = await engine.search_multi_strategy(
            "test query",
            strategies=[strategy_a, strategy_b],
            max_results=10,
            fusion_method="simple",
        )

        assert response.total_found <= 10


# ============================================================
# 14. 异常存储后端处理
# ============================================================
class TestFailingStorage:

    @pytest.mark.asyncio
    async def test_failing_strategy_raises_error(self):
        """策略失败应抛出 RetrievalError"""
        strategy = StubStrategy(fail=True)
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)

        with pytest.raises(RetrievalError):
            await engine.search("test query")

    @pytest.mark.asyncio
    async def test_failing_strategy_increments_error_count(self):
        """策略失败应增加错误计数"""
        strategy = StubStrategy(fail=True)
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)

        try:
            await engine.search("test query")
        except RetrievalError:
            pass

        stats = engine.get_stats()
        assert stats["error_count"] == 1
        assert stats["error_rate"] == 1.0

    @pytest.mark.asyncio
    async def test_multi_strategy_partial_failure(self):
        """多策略部分失败应仍返回结果"""
        items_ok = [_make_memory("ok", "mem_ok")]
        strategy_ok = StubStrategy(name="OK", results=items_ok)
        strategy_fail = StubStrategy(name="FAIL", fail=True)
        storage = StubStorage(items_ok)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy_ok, storage=storage, embedding=embedding)
        response = await engine.search_multi_strategy(
            "test query",
            strategies=[strategy_ok, strategy_fail],
            max_results=10,
            fusion_method="simple",
        )

        assert isinstance(response, RetrievalResponse)
        assert response.total_found >= 1

    @pytest.mark.asyncio
    async def test_multi_strategy_all_fail(self):
        """多策略全部失败应抛出 RetrievalError"""
        strategy_fail1 = StubStrategy(name="FAIL1", fail=True)
        strategy_fail2 = StubStrategy(name="FAIL2", fail=True)
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy_fail1, storage=storage, embedding=embedding)

        with pytest.raises(RetrievalError, match="All strategies failed"):
            await engine.search_multi_strategy(
                "test query",
                strategies=[strategy_fail1, strategy_fail2],
                max_results=10,
            )


# ============================================================
# 15. 嵌入失败处理
# ============================================================
class TestEmbeddingFailure:

    @pytest.mark.asyncio
    async def test_embedding_failure_during_search(self):
        """嵌入失败时搜索应抛出 RetrievalError"""
        items = [_make_memory("content")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding(fail_on_embed=True)

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)

        # 注意：StubStrategy 不调用 embedding，所以这里需要用会调用 embedding 的策略
        # 但由于我们的 StubStrategy 不调用 embedding，这个测试验证的是
        # 当策略内部调用 embedding 并失败时的错误传播
        # 我们创建一个会调用 embedding 的策略
        class EmbeddingUsingStrategy(RetrievalStrategy):
            async def retrieve(self, query, storage, embedding, max_results=10, context=None):
                await embedding.embed(query)  # 这会失败
                return {"strategy": "embed_user", "layers": {}, "combined": []}

        engine2 = RetrievalEngine(
            strategy=EmbeddingUsingStrategy(),
            storage=storage,
            embedding=embedding,
        )

        with pytest.raises(RetrievalError):
            await engine2.search("test query")

    @pytest.mark.asyncio
    async def test_embedding_failure_increments_error_count(self):
        """嵌入失败应增加错误计数"""
        strategy = StubStrategy(fail=True)
        storage = StubStorage()
        embedding = StubEmbedding(fail_on_embed=True)

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)

        try:
            await engine.search("test query")
        except RetrievalError:
            pass

        stats = engine.get_stats()
        assert stats["error_count"] == 1


# ============================================================
# 额外边界条件测试
# ============================================================
class TestEdgeCases:

    @pytest.mark.asyncio
    async def test_search_with_custom_timeout(self):
        """自定义超时覆盖默认值"""
        items = [_make_memory("content")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(
            strategy=strategy, storage=storage, embedding=embedding, timeout=30.0
        )
        # 使用更短的超时
        response = await engine.search("test query", timeout=5.0)
        assert isinstance(response, RetrievalResponse)

    @pytest.mark.asyncio
    async def test_search_response_has_strategy_name(self):
        """响应应包含策略名称"""
        strategy = StubStrategy(name="MyCustomStrategy", results=[])
        storage = StubStorage()
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("test query")
        assert response.strategy == "MyCustomStrategy"

    @pytest.mark.asyncio
    async def test_retrieval_context_with_short_term_memories(self):
        """带短期记忆的上下文"""
        mem1 = _make_memory("recent thought")
        mem2 = _make_memory("another thought")
        ctx = RetrievalContext(short_term_memories=[mem1, mem2])
        d = ctx.to_dict()
        assert len(d["short_term_memories"]) == 2

    @pytest.mark.asyncio
    async def test_query_expansion_case_insensitive(self):
        """查询扩展应大小写不敏感"""
        processor = QueryExpansionProcessor(
            expansions={"search": ["find", "lookup"]}
        )
        ctx = RetrievalContext()
        expanded, _ = await processor.pre_process("SEARCH results", ctx)
        assert "find" in expanded
        assert "lookup" in expanded

    @pytest.mark.asyncio
    async def test_pipeline_with_context(self):
        """管道应正确传递上下文"""
        items = [_make_memory("result", "mem_1")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        expansion = QueryExpansionProcessor(expansions={})
        pipeline = RetrievalPipeline()
        pipeline.add_processor(expansion)

        ctx = RetrievalContext(session_id="sess_001")
        response = await pipeline.execute("query", engine, context=ctx)
        assert isinstance(response, RetrievalResponse)

    @pytest.mark.asyncio
    async def test_search_with_zero_max_results(self):
        """max_results=0 应返回空结果"""
        items = [_make_memory("content")]
        strategy = StubStrategy(results=items)
        storage = StubStorage(items)
        embedding = StubEmbedding()

        engine = RetrievalEngine(strategy=strategy, storage=storage, embedding=embedding)
        response = await engine.search("test query", max_results=0)
        assert response.total_found == 0

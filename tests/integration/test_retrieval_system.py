"""
集成测试: 检索系统
"""

import pytest

from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from memory_system.retrieval_strategies.vector_only_retrieval_strategy import VectorOnlyRetrieval
from memory_system.working_memory_cache import ShortTermMemory


class TestHierarchicalRetrievalWithShortTerm:
    """test_hierarchical_retrieval_with_short_term: 有短期记忆时的分层检索"""

    @pytest.mark.asyncio
    async def test_hierarchical_retrieval_with_short_term(
        self, memory_storage, mock_embedding
    ):
        retrieval = HierarchicalRetrieval(strategy="adaptive")
        stm = ShortTermMemory(capacity=10, ttl_seconds=300)
        await stm.add("Python列表推导式")
        await stm.add("用户正在学习编程")

        context = {
            "short_term_memories": await stm.get_all(),
        }

        results = await retrieval.retrieve(
            query="Python编程",
            storage=memory_storage,
            embedding=mock_embedding,
            max_results=10,
            context=context,
        )

        assert results["strategy"] == "adaptive"
        assert "short_term" in results["layers"]
        # 应该有短期记忆结果
        assert len(results["layers"]["short_term"]) >= 1


class TestHierarchicalRetrievalVectorSearch:
    """test_hierarchical_retrieval_vector_search: 有长期记忆时的向量检索"""

    @pytest.mark.asyncio
    async def test_hierarchical_retrieval_vector_search(
        self, memory_storage, mock_embedding
    ):
        retrieval = HierarchicalRetrieval(strategy="adaptive")

        # 添加带 embedding 的长期记忆
        vec = await mock_embedding.embed("Python是一种通用编程语言")
        item = MemoryItem(
            content="Python是一种通用编程语言",
            memory_type=MemoryType.SEMANTIC,
            embedding=vec,
        )
        await memory_storage.add(item)

        context = {"short_term_memories": []}

        results = await retrieval.retrieve(
            query="Python编程",
            storage=memory_storage,
            embedding=mock_embedding,
            max_results=10,
            context=context,
        )

        assert "vector" in results["layers"]
        assert len(results["layers"]["vector"]) >= 1
        # L2 时间检索现在会自动执行，combined 结果可能来自 time_based 或 vector 层
        assert results["combined"][0]["layer"] in ("vector", "time_based")


class TestVectorOnlyRetrieval:
    """test_vector_only_retrieval: 纯向量检索策略"""

    @pytest.mark.asyncio
    async def test_vector_only_retrieval(self, memory_storage, mock_embedding):
        retrieval = VectorOnlyRetrieval()

        # 添加记忆
        for text in ["Python编程", "Java开发", "数据分析"]:
            vec = await mock_embedding.embed(text)
            item = MemoryItem(
                content=text,
                memory_type=MemoryType.SEMANTIC,
                embedding=vec,
            )
            await memory_storage.add(item)

        results = await retrieval.retrieve(
            query="Python",
            storage=memory_storage,
            embedding=mock_embedding,
            max_results=5,
        )

        assert results["strategy"] == "vector_only"
        assert "vector" in results["layers"]
        assert len(results["combined"]) >= 1
        # 所有结果的 layer 应该是 vector
        for r in results["combined"]:
            assert r["layer"] == "vector"


class TestRetrievalWithEmptyStorage:
    """test_retrieval_with_empty_storage: 空存储时检索返回空结果"""

    @pytest.mark.asyncio
    async def test_retrieval_with_empty_storage(self, memory_storage, mock_embedding):
        retrieval = HierarchicalRetrieval(strategy="adaptive")

        results = await retrieval.retrieve(
            query="测试查询",
            storage=memory_storage,
            embedding=mock_embedding,
            max_results=10,
            context={"short_term_memories": []},
        )

        assert results["query"] == "测试查询"
        assert results["combined"] == []

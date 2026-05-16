"""
混合检索策略单元测试

测试覆盖:
    - TestBM25Score: BM25 评分计算
    - TestReciprocalRankFusion: RRF 融合算法
    - TestHybridRetrievalStrategy: 混合检索策略集成
    - TestInvertedIndex: 倒排索引维护与关键词搜索
"""

import math
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from memory_system.core_abstractions.mathematical_utility_functions import (
    bm25_score,
    reciprocal_rank_fusion,
    tokenize,
)
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    RetrievalResult,
)
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.retrieval_strategies.hybrid_retrieval_strategy import HybridRetrieval
from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend


# ============================================================
# TestBM25Score
# ============================================================
class TestBM25Score:
    """BM25 评分函数测试"""

    def test_basic_scoring(self):
        """基本 BM25 评分应返回正值"""
        score = bm25_score(
            term_freq=3,
            doc_length=100,
            avg_doc_length=80.0,
            doc_count=1000,
            doc_freq=50,
        )
        assert score > 0.0

    def test_zero_doc_count(self):
        """doc_count=0 时应返回 0"""
        score = bm25_score(
            term_freq=1,
            doc_length=10,
            avg_doc_length=10.0,
            doc_count=0,
            doc_freq=1,
        )
        assert score == 0.0

    def test_zero_doc_freq(self):
        """doc_freq=0 时应返回 0"""
        score = bm25_score(
            term_freq=1,
            doc_length=10,
            avg_doc_length=10.0,
            doc_count=100,
            doc_freq=0,
        )
        assert score == 0.0

    def test_high_idf_rare_term(self):
        """稀有词项的 IDF 应更高，导致分数更高"""
        score_common = bm25_score(
            term_freq=1,
            doc_length=50,
            avg_doc_length=50.0,
            doc_count=100,
            doc_freq=50,  # 出现在一半文档中
        )
        score_rare = bm25_score(
            term_freq=1,
            doc_length=50,
            avg_doc_length=50.0,
            doc_count=100,
            doc_freq=2,  # 仅出现在 2 个文档中
        )
        assert score_rare > score_common

    def test_idf_calculation(self):
        """验证 IDF 计算公式正确性"""
        score = bm25_score(
            term_freq=1,
            doc_length=10,
            avg_doc_length=10.0,
            doc_count=100,
            doc_freq=10,
            k1=1.5,
            b=0.0,  # b=0 时无长度归一化
        )
        expected_idf = math.log((100 - 10 + 0.5) / (10 + 0.5) + 1.0)
        expected_tf_norm = (1 * (1.5 + 1)) / (1 + 1.5 * 1.0)  # b=0 => 1-b+b*1=1
        expected = expected_idf * expected_tf_norm
        assert abs(score - expected) < 1e-10

    def test_higher_term_freq_gives_higher_score(self):
        """更高词频应产生更高分数（在饱和之前）"""
        score_low = bm25_score(
            term_freq=1,
            doc_length=100,
            avg_doc_length=100.0,
            doc_count=1000,
            doc_freq=100,
        )
        score_high = bm25_score(
            term_freq=5,
            doc_length=100,
            avg_doc_length=100.0,
            doc_count=1000,
            doc_freq=100,
        )
        assert score_high > score_low

    def test_custom_k1_b_parameters(self):
        """自定义 k1 和 b 参数应正常工作"""
        score = bm25_score(
            term_freq=2,
            doc_length=200,
            avg_doc_length=100.0,
            doc_count=500,
            doc_freq=20,
            k1=2.0,
            b=0.5,
        )
        assert score > 0.0


# ============================================================
# TestReciprocalRankFusion
# ============================================================
class TestReciprocalRankFusion:
    """RRF 融合算法测试"""

    def test_single_list(self):
        """单路结果应保留排序"""
        results = [[("a", 0.9), ("b", 0.8), ("c", 0.7)]]
        fused = reciprocal_rank_fusion(results, k=60)
        ids = [item[0] for item in fused]
        assert ids == ["a", "b", "c"]

    def test_multiple_lists_fusion(self):
        """多路结果应正确融合"""
        list1 = [("a", 0.9), ("b", 0.8), ("c", 0.7)]
        list2 = [("b", 0.95), ("a", 0.85), ("d", 0.6)]
        fused = reciprocal_rank_fusion([list1, list2], k=60)
        fused_ids = [item[0] for item in fused]

        # "a" 在 list1 排名 1，list2 排名 2 => 1/(60+1) + 1/(60+2)
        # "b" 在 list1 排名 2，list2 排名 1 => 1/(60+2) + 1/(60+1)
        # "a" 和 "b" 分数应相同
        a_score = [s for mid, s in fused if mid == "a"][0]
        b_score = [s for mid, s in fused if mid == "b"][0]
        assert abs(a_score - b_score) < 1e-10

        # "c" 只在 list1 排名 3 => 1/(60+3)
        # "d" 只在 list2 排名 3 => 1/(60+3)
        # "c" 和 "d" 分数应相同
        c_score = [s for mid, s in fused if mid == "c"][0]
        d_score = [s for mid, s in fused if mid == "d"][0]
        assert abs(c_score - d_score) < 1e-10

        # "a"/"b" 应排在 "c"/"d" 前面
        assert fused_ids.index("a") < fused_ids.index("c")
        assert fused_ids.index("b") < fused_ids.index("d")

    def test_empty_lists(self):
        """空结果列表应返回空"""
        fused = reciprocal_rank_fusion([], k=60)
        assert fused == []

    def test_empty_inner_list(self):
        """包含空子列表应正常处理"""
        list1 = [("a", 0.9)]
        list2 = []
        fused = reciprocal_rank_fusion([list1, list2], k=60)
        assert len(fused) == 1
        assert fused[0][0] == "a"

    def test_k_parameter_affects_scoring(self):
        """不同 k 值应影响分数"""
        results = [[("a", 1.0), ("b", 0.5)]]
        fused_small_k = reciprocal_rank_fusion(results, k=1)
        fused_large_k = reciprocal_rank_fusion(results, k=100)

        # k 越小，排名差异越明显
        assert fused_small_k[0][1] > fused_large_k[0][1]

    def test_duplicate_items_across_lists(self):
        """同一 item 在多路中出现应累加分数"""
        list1 = [("a", 0.9)]
        list2 = [("a", 0.8)]
        list3 = [("a", 0.7)]
        fused = reciprocal_rank_fusion([list1, list2, list3], k=60)

        assert len(fused) == 1
        expected = 3 * (1.0 / (60 + 1))
        assert abs(fused[0][1] - expected) < 1e-10

    def test_results_sorted_descending(self):
        """结果应按分数降序排列"""
        list1 = [("z", 0.1), ("a", 0.9)]
        list2 = [("a", 0.5), ("z", 0.3)]
        fused = reciprocal_rank_fusion([list1, list2], k=60)
        scores = [s for _, s in fused]
        assert scores == sorted(scores, reverse=True)


# ============================================================
# TestInvertedIndex
# ============================================================
class TestInvertedIndex:
    """倒排索引维护与关键词搜索测试"""

    @pytest.fixture
    def storage(self):
        return MemoryStorage(max_memories=100)

    @pytest.mark.asyncio
    async def test_index_building_on_add(self, storage):
        """添加记忆时应构建倒排索引"""
        item = MemoryItem(
            content="Python programming language",
            memory_type=MemoryType.SEMANTIC,
        )
        await storage.add(item)

        assert len(storage._inverted_index) > 0
        assert item.memory_id in storage._doc_lengths
        assert storage._doc_lengths[item.memory_id] > 0

        # "python" 应在索引中
        assert "python" in storage._inverted_index
        assert item.memory_id in storage._inverted_index["python"]

    @pytest.mark.asyncio
    async def test_index_cleanup_on_delete(self, storage):
        """删除记忆时应清理倒排索引"""
        item = MemoryItem(
            content="unique keyword test xyz",
            memory_type=MemoryType.SEMANTIC,
        )
        await storage.add(item)
        memory_id = item.memory_id

        # 确认索引已建立
        assert memory_id in storage._doc_lengths

        # 删除
        await storage.delete(memory_id)

        # 确认索引已清理
        assert memory_id not in storage._doc_lengths
        for token_set in storage._inverted_index.values():
            assert memory_id not in token_set

    @pytest.mark.asyncio
    async def test_keyword_search_correctness(self, storage):
        """关键词搜索应返回匹配的记忆"""
        item1 = MemoryItem(
            content="Python is a programming language",
            memory_type=MemoryType.SEMANTIC,
        )
        item2 = MemoryItem(
            content="Java is also a programming language",
            memory_type=MemoryType.SEMANTIC,
        )
        item3 = MemoryItem(
            content="The weather is nice today",
            memory_type=MemoryType.EPISODIC,
        )
        await storage.add(item1)
        await storage.add(item2)
        await storage.add(item3)

        results = await storage.search_by_keyword(["programming"], top_k=5)
        contents = [m.content for m, s in results]

        assert item1.content in contents
        assert item2.content in contents
        assert item3.content not in contents

    @pytest.mark.asyncio
    async def test_search_with_no_matches(self, storage):
        """无匹配时应返回空列表"""
        item = MemoryItem(
            content="hello world",
            memory_type=MemoryType.SEMANTIC,
        )
        await storage.add(item)

        results = await storage.search_by_keyword(["xyznonexistent"], top_k=5)
        assert results == []

    @pytest.mark.asyncio
    async def test_search_with_memory_type_filter(self, storage):
        """按记忆类型过滤应正常工作"""
        item1 = MemoryItem(
            content="Python programming",
            memory_type=MemoryType.EPISODIC,
        )
        item2 = MemoryItem(
            content="Python programming",
            memory_type=MemoryType.SEMANTIC,
        )
        await storage.add(item1)
        await storage.add(item2)

        results_episodic = await storage.search_by_keyword(
            ["python"], top_k=5, memory_type="episodic"
        )
        results_semantic = await storage.search_by_keyword(
            ["python"], top_k=5, memory_type="semantic"
        )

        assert len(results_episodic) == 1
        assert results_episodic[0][0].memory_id == item1.memory_id
        assert len(results_semantic) == 1
        assert results_semantic[0][0].memory_id == item2.memory_id

    @pytest.mark.asyncio
    async def test_close_clears_index(self, storage):
        """close() 应清空倒排索引"""
        item = MemoryItem(
            content="test content",
            memory_type=MemoryType.SEMANTIC,
        )
        await storage.add(item)
        assert len(storage._inverted_index) > 0

        await storage.close()
        assert len(storage._inverted_index) == 0
        assert len(storage._doc_lengths) == 0

    @pytest.mark.asyncio
    async def test_top_k_limit(self, storage):
        """top_k 参数应限制返回数量"""
        for i in range(10):
            item = MemoryItem(
                content=f"python programming example number {i}",
                memory_type=MemoryType.SEMANTIC,
            )
            await storage.add(item)

        results = await storage.search_by_keyword(["python"], top_k=3)
        assert len(results) <= 3

    @pytest.mark.asyncio
    async def test_bm25_scores_are_positive(self, storage):
        """BM25 分数应为正值"""
        item = MemoryItem(
            content="python programming language tutorial",
            memory_type=MemoryType.SEMANTIC,
        )
        await storage.add(item)

        results = await storage.search_by_keyword(["python"], top_k=5)
        assert len(results) > 0
        for memory, score in results:
            assert score > 0.0


# ============================================================
# TestHybridRetrievalStrategy
# ============================================================
class TestHybridRetrievalStrategy:
    """混合检索策略集成测试"""

    @pytest.fixture
    def hybrid(self):
        return HybridRetrieval(
            rrf_k=60,
            vector_weight=0.6,
            keyword_weight=0.4,
            vector_top_k=20,
            keyword_top_k=20,
        )

    @pytest.fixture
    def storage(self):
        return MemoryStorage(max_memories=100)

    @pytest.fixture
    def mock_embedding(self):
        embedding = AsyncMock()
        embedding.embed = AsyncMock(return_value=[0.1] * 64)
        return embedding

    @pytest.mark.asyncio
    async def test_basic_retrieval_with_vector_and_keyword(self, hybrid, storage, mock_embedding):
        """基本混合检索应同时使用向量和关键词路径"""
        item = MemoryItem(
            content="Python programming language tutorial",
            memory_type=MemoryType.SEMANTIC,
            embedding=[0.1] * 64,
        )
        await storage.add(item)

        results = await hybrid.retrieve(
            query="Python programming",
            storage=storage,
            embedding=mock_embedding,
            max_results=5,
        )

        assert results["strategy"] == "hybrid"
        assert "vector" in results["layers"]
        assert "keyword" in results["layers"]
        assert results["query"] == "Python programming"

    @pytest.mark.asyncio
    async def test_retrieval_with_only_vector(self, hybrid):
        """当存储后端不支持关键词搜索时，应仅使用向量检索"""
        # 使用一个不支持 search_by_keyword 的 mock storage
        storage = AsyncMock(spec=StorageBackend)
        storage.search_by_vector = AsyncMock(return_value=[])
        storage.search_by_keyword = AsyncMock(
            side_effect=NotImplementedError("not implemented")
        )
        storage.get = AsyncMock(return_value=None)

        mock_embedding = AsyncMock()
        mock_embedding.embed = AsyncMock(return_value=[0.1] * 64)

        results = await hybrid.retrieve(
            query="test query",
            storage=storage,
            embedding=mock_embedding,
            max_results=5,
        )

        assert results["strategy"] == "hybrid"
        assert "vector" in results["layers"]
        assert "keyword" in results["layers"]
        # keyword layer should be empty since NotImplementedError
        assert results["layers"]["keyword"] == []

    @pytest.mark.asyncio
    async def test_short_term_integration(self, hybrid, storage, mock_embedding):
        """短期记忆应出现在检索结果中"""
        st_memory = MemoryItem(
            content="short term memory about Python",
            memory_type=MemoryType.SHORT_TERM,
        )

        results = await hybrid.retrieve(
            query="Python",
            storage=storage,
            embedding=mock_embedding,
            max_results=5,
            context={"short_term_memories": [st_memory]},
        )

        assert "short_term" in results["layers"]
        assert len(results["layers"]["short_term"]) > 0

    @pytest.mark.asyncio
    async def test_rrf_fusion_correctness(self, hybrid, storage, mock_embedding):
        """RRF 融合应正确合并向量和关键词结果"""
        # 添加多个记忆
        for i in range(5):
            item = MemoryItem(
                content=f"python programming example {i}",
                memory_type=MemoryType.SEMANTIC,
                embedding=[0.1] * 64,
            )
            await storage.add(item)

        results = await hybrid.retrieve(
            query="python programming",
            storage=storage,
            embedding=mock_embedding,
            max_results=10,
        )

        # 应有 combined 结果
        assert len(results["combined"]) > 0
        # 每个 combined 结果应有 memory, score, layer 字段
        for item in results["combined"]:
            assert "memory" in item
            assert "score" in item
            assert "layer" in item

    @pytest.mark.asyncio
    async def test_empty_query_handling(self, hybrid, storage, mock_embedding):
        """空查询应返回空结果"""
        results = await hybrid.retrieve(
            query="",
            storage=storage,
            embedding=mock_embedding,
            max_results=5,
        )
        assert results["combined"] == []

        results = await hybrid.retrieve(
            query="   ",
            storage=storage,
            embedding=mock_embedding,
            max_results=5,
        )
        assert results["combined"] == []

    @pytest.mark.asyncio
    async def test_no_duplicate_results(self, hybrid, storage, mock_embedding):
        """同一记忆不应在 combined 结果中重复出现"""
        item = MemoryItem(
            content="python programming language",
            memory_type=MemoryType.SEMANTIC,
            embedding=[0.1] * 64,
        )
        await storage.add(item)

        results = await hybrid.retrieve(
            query="python programming",
            storage=storage,
            embedding=mock_embedding,
            max_results=10,
        )

        combined_ids = [
            r["memory"]["memory_id"] for r in results["combined"]
        ]
        assert len(combined_ids) == len(set(combined_ids))

    @pytest.mark.asyncio
    async def test_max_results_respected(self, hybrid, storage, mock_embedding):
        """max_results 应限制最终返回数量"""
        for i in range(10):
            item = MemoryItem(
                content=f"python programming test {i}",
                memory_type=MemoryType.SEMANTIC,
                embedding=[0.1] * 64,
            )
            await storage.add(item)

        results = await hybrid.retrieve(
            query="python",
            storage=storage,
            embedding=mock_embedding,
            max_results=3,
        )
        assert len(results["combined"]) <= 3

    @pytest.mark.asyncio
    async def test_hybrid_rrf_layer_in_results(self, hybrid, storage, mock_embedding):
        """RRF 融合的结果应标记为 hybrid_rrf 层"""
        item = MemoryItem(
            content="python programming tutorial",
            memory_type=MemoryType.SEMANTIC,
            embedding=[0.1] * 64,
        )
        await storage.add(item)

        results = await hybrid.retrieve(
            query="python programming",
            storage=storage,
            embedding=mock_embedding,
            max_results=5,
        )

        # 至少应有一个 hybrid_rrf 层的结果（来自 RRF 融合）
        layers = [r["layer"] for r in results["combined"]]
        assert "hybrid_rrf" in layers

    def test_initialization_parameters(self):
        """初始化参数应正确存储"""
        hybrid = HybridRetrieval(
            rrf_k=50,
            vector_weight=0.7,
            keyword_weight=0.3,
            vector_top_k=15,
            keyword_top_k=25,
        )
        assert hybrid.rrf_k == 50
        assert hybrid.vector_weight == 0.7
        assert hybrid.keyword_weight == 0.3
        assert hybrid.vector_top_k == 15
        assert hybrid.keyword_top_k == 25

    def test_registration(self):
        """HybridRetrieval 应注册为 'hybrid' 策略"""
        from memory_system.core_abstractions.component_registry_manager import get_retrieval_registry
        registry = get_retrieval_registry()
        assert "hybrid" in registry
        assert registry["hybrid"] is HybridRetrieval

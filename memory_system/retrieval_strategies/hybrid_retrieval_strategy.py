"""
混合检索策略实现
结合稠密向量检索（语义相似度）和稀疏检索（BM25关键词匹配），
使用 Reciprocal Rank Fusion (RRF) 算法融合两路结果。

参考:
    - RRF: Cormack et al., "Reciprocal Rank Fusion outperforms Condorcet..."
    - Hybrid Search: ADR-007 Memory-First Architecture
"""

from typing import Any, Dict, List, Optional, Set, Tuple

from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy
)
from memory_system.core_abstractions.memory_data_models import MemoryItem, RetrievalResult, MemoryType
from memory_system.core_abstractions.component_registry_manager import register_retrieval
from memory_system.core_abstractions.mathematical_utility_functions import (
    tokenize,
    reciprocal_rank_fusion,
)
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@register_retrieval("hybrid")
class HybridRetrieval(RetrievalStrategy):
    """
    混合检索策略

    结合向量相似度检索（稠密）和 BM25 关键词检索（稀疏），
    使用 RRF 算法融合两路结果，兼顾语义理解和精确匹配。

    适用场景:
        - 包含专有名词、ID等精确匹配需求的查询
        - 需要同时考虑语义相似性和关键词匹配的场景
        - 对检索质量要求较高的生产环境
    """

    def __init__(
        self,
        rrf_k: int = 60,
        vector_weight: float = 0.6,
        keyword_weight: float = 0.4,
        vector_top_k: int = 20,
        keyword_top_k: int = 20,
    ):
        """
        Args:
            rrf_k: RRF 常数 (默认 60)
            vector_weight: 向量检索权重 (用于最终分数加权)
            keyword_weight: 关键词检索权重
            vector_top_k: 向量检索候选数
            keyword_top_k: 关键词检索候选数
        """
        self.rrf_k = rrf_k
        self.vector_weight = vector_weight
        self.keyword_weight = keyword_weight
        self.vector_top_k = vector_top_k
        self.keyword_top_k = keyword_top_k

    async def retrieve(
        self,
        query: str,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        max_results: int = 10,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        执行混合检索

        1. L1: 短期记忆检索（如果有上下文）
        2. L2: 向量相似度检索（稠密）
        3. L3: BM25 关键词检索（稀疏）
        4. RRF 融合 L2 + L3
        5. 合并 L1 + 融合结果
        """
        context = context or {}
        results = {
            "query": query,
            "strategy": "hybrid",
            "layers": {},
            "combined": []
        }

        if not query or not query.strip():
            return results

        safe_max = max(max_results, 1)
        seen_ids: Set[str] = set()
        all_results: List[RetrievalResult] = []

        # L1: 短期记忆
        if context.get("short_term_memories"):
            st_results = self._search_short_term(
                query, context["short_term_memories"], safe_max
            )
            results["layers"]["short_term"] = [
                {"memory": r.memory.model_dump(), "score": r.score}
                for r in st_results
            ]
            for r in st_results:
                if r.memory.memory_id not in seen_ids:
                    seen_ids.add(r.memory.memory_id)
                    all_results.append(r)

        # L2: 向量检索
        vector_results_raw = await self._search_by_vector(
            storage, embedding, query, self.vector_top_k
        )
        results["layers"]["vector"] = [
            {"memory": m.model_dump(), "score": s}
            for m, s in vector_results_raw
        ]

        # L3: 关键词检索
        keyword_results_raw = await self._search_by_keyword(
            storage, query, self.keyword_top_k
        )
        results["layers"]["keyword"] = [
            {"memory": m.model_dump(), "score": s}
            for m, s in keyword_results_raw
        ]

        # RRF 融合
        vector_list = [(m.memory_id, s) for m, s in vector_results_raw]
        keyword_list = [(m.memory_id, s) for m, s in keyword_results_raw]

        fused = reciprocal_rank_fusion([vector_list, keyword_list], k=self.rrf_k)

        # 将融合结果转换为 RetrievalResult
        for memory_id, rrf_score in fused:
            if memory_id in seen_ids:
                continue
            memory = await storage.get(memory_id)
            if memory:
                seen_ids.add(memory_id)
                all_results.append(RetrievalResult(
                    memory=memory,
                    score=rrf_score,
                    layer="hybrid_rrf"
                ))

        # 合并排序
        results["combined"] = self._merge_and_rank(all_results, max_results)
        return results

    def _search_short_term(
        self,
        query: str,
        short_term_memories: List[MemoryItem],
        max_results: int
    ) -> List[RetrievalResult]:
        """检索短期记忆"""
        query_terms = set(tokenize(query))
        current_time = __import__('time').time()
        scored_results = []

        for memory in short_term_memories:
            content_terms = set(tokenize(memory.content))
            overlap = len(query_terms & content_terms)
            keyword_score = overlap / max(len(query_terms), 1)
            relevance = memory.calculate_relevance_score(current_time)
            final_score = keyword_score * 0.5 + relevance * 0.5
            scored_results.append(RetrievalResult(
                memory=memory, score=final_score, layer="short_term"
            ))

        scored_results.sort(key=lambda x: x.score, reverse=True)
        return scored_results[:max_results]

    async def _search_by_vector(
        self,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        query: str,
        top_k: int
    ) -> List[Tuple[MemoryItem, float]]:
        """向量相似度检索"""
        query_vector = await embedding.embed(query)
        return await storage.search_by_vector(query_vector, top_k=top_k)

    async def _search_by_keyword(
        self,
        storage: StorageBackend,
        query: str,
        top_k: int
    ) -> List[Tuple[MemoryItem, float]]:
        """BM25 关键词检索"""
        query_terms = tokenize(query)
        try:
            return await storage.search_by_keyword(query_terms, top_k=top_k)
        except NotImplementedError:
            logger.debug("当前存储后端不支持关键词检索，跳过稀疏检索路径")
            return []

    def _merge_and_rank(
        self,
        results: List[RetrievalResult],
        max_results: int
    ) -> List[Dict[str, Any]]:
        """合并结果并排序"""
        results.sort(key=lambda x: x.score, reverse=True)
        return [
            {"memory": r.memory.model_dump(), "score": r.score, "layer": r.layer}
            for r in results[:max_results]
        ]

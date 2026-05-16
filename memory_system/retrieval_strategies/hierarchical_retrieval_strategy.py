"""
分层检索策略实现
L1: 短期记忆 (O(1))
L2: 时间检索 (O(log n))
L3: 向量检索 (O(n))
L4: 重要性检索 (O(n))
"""

from typing import Any, Dict, List, Optional, Set
import time

from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy
)
from memory_system.core_abstractions.memory_data_models import MemoryItem, RetrievalResult, MemoryType
from memory_system.core_abstractions.component_registry_manager import register_retrieval
from memory_system.core_abstractions.mathematical_utility_functions import tokenize
from memory_system.core_abstractions.system_constants_definitions import (
    DEFAULT_SHORT_TERM_RATIO,
    DEFAULT_TIME_RATIO,
    DEFAULT_VECTOR_RATIO,
    DEFAULT_IMPORTANCE_RATIO,
)


@register_retrieval("hierarchical")
class HierarchicalRetrieval(RetrievalStrategy):
    """
    分层检索策略
    
    从最快到最慢逐级检索，平衡速度与召回率
    
    策略模式:
        - fast: 仅L1（短期记忆）
        - adaptive: L1+L2+L3（平衡）
        - thorough: L1+L2+L3+L4（完整）
    """
    
    def __init__(
        self,
        strategy: str = "adaptive",
        short_term_ratio: float = DEFAULT_SHORT_TERM_RATIO,
        time_ratio: float = DEFAULT_TIME_RATIO,
        vector_ratio: float = DEFAULT_VECTOR_RATIO,
        importance_ratio: float = DEFAULT_IMPORTANCE_RATIO
    ):
        """
        初始化
        
        Args:
            strategy: 检索策略 (fast/adaptive/thorough)
            short_term_ratio: 短期记忆检索结果比例
            time_ratio: 时间检索结果比例
            vector_ratio: 向量检索结果比例
            importance_ratio: 重要性检索结果比例
        """
        self.strategy = strategy
        self.short_term_ratio = short_term_ratio
        self.time_ratio = time_ratio
        self.vector_ratio = vector_ratio
        self.importance_ratio = importance_ratio
    
    async def retrieve(
        self,
        query: str,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        max_results: int = 10,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        执行分层检索
        
        Args:
            query: 查询内容
            storage: 存储后端
            embedding: 嵌入提供者
            max_results: 最大结果数
            context: 上下文信息
                - short_term_memories: 短期记忆列表
                - time_range: (start, end) 时间范围
                
        Returns:
            检索结果字典
        """
        context = context or {}
        # 支持运行时策略覆盖
        effective_strategy = context.get("strategy_override", self.strategy)
        if not query or not query.strip():
            return {"query": query, "strategy": effective_strategy, "layers": {}, "combined": []}
        results = {
            "query": query,
            "strategy": effective_strategy,
            "layers": {},
            "combined": []
        }
        
        # 保护：确保 max_results 至少为 1
        safe_max = max(max_results, 1)
        
        seen_ids: Set[str] = set()
        all_results: List[RetrievalResult] = []
        
        # L1: 短期记忆检索 (最快)
        if context.get("short_term_memories"):
            st_results = self._search_short_term(
                query, 
                context["short_term_memories"],
                max(1, int(safe_max * self.short_term_ratio))
            )
            results["layers"]["short_term"] = [
                {"memory": r.memory.model_dump(), "score": r.score}
                for r in st_results
            ]
            for r in st_results:
                if r.memory.memory_id not in seen_ids:
                    seen_ids.add(r.memory.memory_id)
                    all_results.append(r)
            
            # fast模式：如果短期记忆足够，直接返回
            if effective_strategy == "fast" and len(st_results) >= safe_max // 2:
                results["combined"] = self._merge_and_rank(all_results, max_results)
                return results
        
        # L2: 时间范围检索
        if effective_strategy in ["adaptive", "thorough"]:
            # 自动填充时间范围：如果未提供，则搜索全部时间
            time_range = context.get("time_range")
            if time_range is None:
                time_range = (0.0, time.time())
            start, end = time_range
            time_results = await self._search_by_time(
                storage, start, end, max(1, int(safe_max * self.time_ratio))
            )
            results["layers"]["time_based"] = [
                {"memory": r.memory.model_dump(), "score": r.score}
                for r in time_results
            ]
            for r in time_results:
                if r.memory.memory_id not in seen_ids:
                    seen_ids.add(r.memory.memory_id)
                    all_results.append(r)
        
        # L3: 向量相似度检索
        if effective_strategy in ["adaptive", "thorough"]:
            vector_results = await self._search_by_vector(
                storage, embedding, query, max(1, int(safe_max * self.vector_ratio))
            )
            results["layers"]["vector"] = [
                {"memory": r.memory.model_dump(), "score": r.score}
                for r in vector_results
            ]
            for r in vector_results:
                if r.memory.memory_id not in seen_ids:
                    seen_ids.add(r.memory.memory_id)
                    all_results.append(r)
        
        # L4: 重要性检索 (thorough模式)
        if effective_strategy == "thorough":
            importance_results = await self._search_by_importance(
                storage, max(1, int(safe_max * self.importance_ratio))
            )
            results["layers"]["importance"] = [
                {"memory": r.memory.model_dump(), "score": r.score}
                for r in importance_results
            ]
            for r in importance_results:
                if r.memory.memory_id not in seen_ids:
                    seen_ids.add(r.memory.memory_id)
                    all_results.append(r)
        
        # 合并并排序结果
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
        current_time = time.time()
        scored_results = []
        
        for memory in short_term_memories:
            # 关键词匹配
            content_terms = set(tokenize(memory.content))
            overlap = len(query_terms & content_terms)
            keyword_score = overlap / max(len(query_terms), 1)
            
            # 相关性分数
            relevance = memory.calculate_relevance_score(current_time)
            
            # 综合分数
            final_score = keyword_score * 0.5 + relevance * 0.5
            
            scored_results.append(
                RetrievalResult(
                    memory=memory,
                    score=final_score,
                    layer="short_term"
                )
            )
        
        # 排序并返回
        scored_results.sort(key=lambda x: x.score, reverse=True)
        return scored_results[:max_results]
    
    async def _search_by_time(
        self,
        storage: StorageBackend,
        start_time: float,
        end_time: float,
        max_results: int
    ) -> List[RetrievalResult]:
        """按时间范围检索"""
        memories = await storage.search_by_time_range(start_time, end_time)
        
        current_time = time.time()
        results = []
        for memory in memories[:max_results]:
            score = memory.calculate_relevance_score(current_time)
            results.append(RetrievalResult(
                memory=memory,
                score=score,
                layer="time_based"
            ))
        
        return results
    
    async def _search_by_vector(
        self,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        query: str,
        max_results: int
    ) -> List[RetrievalResult]:
        """向量相似度检索"""
        query_vector = await embedding.embed(query)
        
        search_results = await storage.search_by_vector(
            query_vector, 
            top_k=max_results
        )
        
        results = []
        for memory, similarity in search_results:
            results.append(RetrievalResult(
                memory=memory,
                score=similarity,
                layer="vector"
            ))
        
        return results
    
    async def _search_by_importance(
        self,
        storage: StorageBackend,
        max_results: int
    ) -> List[RetrievalResult]:
        """按重要性搜索（优化版：使用 list_memories 按重要性排序）"""
        memories = await storage.list_memories(order_by="importance", limit=max_results)

        if not memories:
            return []

        results = []
        for memory in memories:
            score = memory.importance.value / 5.0  # 归一化到 0-1
            results.append(RetrievalResult(
                memory=memory,
                score=float(score),
                layer="importance"
            ))

        return results
    
    def _merge_and_rank(
        self,
        results: List[RetrievalResult],
        max_results: int
    ) -> List[Dict[str, Any]]:
        """合并结果并排序"""
        # 按分数排序
        results.sort(key=lambda x: x.score, reverse=True)

        # 转换为字典
        return [
            {
                "memory": r.memory.model_dump(),
                "score": r.score,
                "layer": r.layer
            }
            for r in results[:max_results]
        ]

    def _calculate_context_score(
        self,
        memory: MemoryItem,
        context: Dict[str, Any]
    ) -> float:
        """
        计算情境相关性分数 (N-08)

        基于会话上下文信息调整记忆的相关性分数

        Args:
            memory: 记忆项
            context: 上下文信息

        Returns:
            情境相关性分数 (0.0-1.0)
        """
        score = 0.0

        # 时间上下文
        if "current_time" in context:
            time_diff = abs(memory.timestamp - context["current_time"])
            # 1小时内的记忆获得最高分数
            time_score = max(0, 1 - time_diff / 3600)
            score += time_score * 0.3

        # 会话主题上下文
        if "session_topics" in context:
            memory_content_lower = memory.content.lower()
            topic_matches = 0
            for topic in context["session_topics"]:
                if topic.lower() in memory_content_lower:
                    topic_matches += 1
            if topic_matches > 0:
                score += min(0.4, topic_matches * 0.2)

        # 位置上下文
        if "location" in context and "location" in memory.metadata:
            if context["location"] == memory.metadata.get("location"):
                score += 0.2

        # 会话ID匹配
        if "session_id" in context and "session_id" in memory.metadata:
            if context["session_id"] == memory.metadata.get("session_id"):
                score += 0.1

        return min(1.0, score)

    async def retrieve_with_context(
        self,
        query: str,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        max_results: int = 10,
        context: Optional[Dict[str, Any]] = None,
        enable_context_scoring: bool = True
    ) -> Dict[str, Any]:
        """
        情境感知检索 (N-08)

        在基础检索之上增加情境相关性评分

        Args:
            query: 查询内容
            storage: 存储后端
            embedding: 嵌入提供者
            max_results: 最大结果数
            context: 上下文信息
                - current_time: 当前时间戳
                - session_topics: 会话主题列表
                - location: 位置信息
                - session_id: 会话ID
            enable_context_scoring: 是否启用情境评分

        Returns:
            检索结果
        """
        # 执行基础检索
        results = await self.retrieve(
            query=query,
            storage=storage,
            embedding=embedding,
            max_results=max_results * 2,  # 获取更多结果用于重排序
            context=context
        )

        if not enable_context_scoring or not context:
            return results

        # 应用情境评分
        combined = results.get("combined", [])
        for item in combined:
            memory_data = item.get("memory", {})
            base_score = item.get("score", 0)

            # 创建MemoryItem对象用于计算情境分数
            try:
                memory = MemoryItem.model_validate(memory_data)
                context_score = self._calculate_context_score(memory, context)

                # 综合分数 = 基础分数 * (1 + 情境分数 * 权重)
                final_score = base_score * (1 + context_score * 0.2)
                item["score"] = min(1.0, final_score)
                item["context_score"] = context_score
            except Exception:
                # 如果解析失败，保持原分数
                pass

        # 重新排序
        combined.sort(key=lambda x: x.get("score", 0), reverse=True)
        results["combined"] = combined[:max_results]

        return results

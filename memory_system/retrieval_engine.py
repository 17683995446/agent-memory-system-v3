"""检索引擎 - 解耦检索策略与存储后端

Agent Memory System V2.29.0 重构项目 - R-02
解耦检索策略与存储后端，通过抽象层实现统一的检索接口
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Tuple
import time
import asyncio

from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy
)
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    RetrievalResult,
    RetrievalResponse
)
from memory_system.exceptions import RetrievalError, RetrievalTimeoutError
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class RetrievalContext:
    """检索上下文 - 封装检索所需的所有上下文信息"""
    
    def __init__(
        self,
        short_term_memories: Optional[List[MemoryItem]] = None,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        previous_queries: Optional[List[str]] = None,
        extra: Optional[Dict[str, Any]] = None
    ):
        self.short_term_memories = short_term_memories or []
        self.session_id = session_id
        self.user_id = user_id
        self.filters = filters or {}
        self.previous_queries = previous_queries or []
        self.extra = extra or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "short_term_memories": self.short_term_memories,
            "session_id": self.session_id,
            "user_id": self.user_id,
            "filters": self.filters,
            "previous_queries": self.previous_queries,
            "extra": self.extra
        }


class RetrievalEngine:
    """检索引擎 - 解耦检索策略与存储后端
    
    提供统一的检索接口，将检索策略与具体存储后端解耦。
    支持策略组合、结果融合、超时控制等功能。
    
    Attributes:
        strategy: 检索策略实例
        storage: 存储后端实例
        embedding: 嵌入提供者实例
        timeout: 默认检索超时时间（秒）
    """
    
    def __init__(
        self,
        strategy: RetrievalStrategy,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        timeout: float = 30.0
    ):
        self.strategy = strategy
        self.storage = storage
        self.embedding = embedding
        self.timeout = timeout
        self._stats = {
            "total_searches": 0,
            "total_time": 0.0,
            "timeout_count": 0,
            "error_count": 0
        }
    
    async def search(
        self,
        query: str,
        max_results: int = 10,
        context: Optional[RetrievalContext] = None,
        timeout: Optional[float] = None
    ) -> RetrievalResponse:
        """执行统一检索
        
        Args:
            query: 查询内容
            max_results: 最大结果数
            context: 检索上下文
            timeout: 超时时间（覆盖默认值）
            
        Returns:
            结构化检索响应
            
        Raises:
            RetrievalTimeoutError: 检索超时
            RetrievalError: 检索过程中发生错误
        """
        start_time = time.time()
        self._stats["total_searches"] += 1
        
        if not query or not query.strip():
            return RetrievalResponse(
                query=query or "",
                strategy=self.strategy.__class__.__name__,
                combined=[],
                total_found=0,
                retrieval_time=0.0
            )
        
        context_dict = context.to_dict() if context else {}
        search_timeout = timeout or self.timeout
        
        try:
            # 使用 asyncio.wait_for 实现超时控制
            result = await asyncio.wait_for(
                self.strategy.retrieve(
                    query=query,
                    storage=self.storage,
                    embedding=self.embedding,
                    max_results=max_results,
                    context=context_dict
                ),
                timeout=search_timeout
            )
            
            retrieval_time = time.time() - start_time
            self._stats["total_time"] += retrieval_time
            
            # 转换为结构化响应
            if isinstance(result, dict):
                return RetrievalResponse(
                    query=query,
                    strategy=result.get("strategy", self.strategy.__class__.__name__),
                    layers=result.get("layers", {}),
                    combined=result.get("combined", []),
                    total_found=len(result.get("combined", [])),
                    retrieval_time=retrieval_time
                )
            elif isinstance(result, RetrievalResponse):
                result.retrieval_time = retrieval_time
                return result
            else:
                raise RetrievalError(
                    message=f"Unexpected result type: {type(result)}",
                    strategy=self.strategy.__class__.__name__
                )
                
        except asyncio.TimeoutError:
            self._stats["timeout_count"] += 1
            raise RetrievalTimeoutError(
                timeout=search_timeout,
                strategy=self.strategy.__class__.__name__
            )
        except Exception as e:
            self._stats["error_count"] += 1
            if isinstance(e, RetrievalError):
                raise
            raise RetrievalError(
                message=str(e),
                strategy=self.strategy.__class__.__name__,
                details={"exception_type": type(e).__name__}
            )
    
    async def search_multi_strategy(
        self,
        query: str,
        strategies: List[RetrievalStrategy],
        max_results: int = 10,
        context: Optional[RetrievalContext] = None,
        fusion_method: str = "rrf"
    ) -> RetrievalResponse:
        """多策略检索 - 并行执行多个检索策略并融合结果
        
        Args:
            query: 查询内容
            strategies: 检索策略列表
            max_results: 最大结果数
            context: 检索上下文
            fusion_method: 融合方法 ("rrf"=倒数秩融合, "weighted"=加权平均)
            
        Returns:
            融合后的检索响应
        """
        start_time = time.time()
        context_dict = context.to_dict() if context else {}
        
        # 并行执行所有策略
        tasks = []
        for strategy in strategies:
            engine = RetrievalEngine(
                strategy=strategy,
                storage=self.storage,
                embedding=self.embedding,
                timeout=self.timeout
            )
            task = engine.search(
                query=query,
                max_results=max_results * 2,  # 获取更多结果用于融合
                context=context
            )
            tasks.append(task)
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            raise RetrievalError(
                message=f"Multi-strategy search failed: {str(e)}",
                details={"strategies": [s.__class__.__name__ for s in strategies]}
            )
        
        # 收集有效结果
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, RetrievalResponse):
                valid_results.append((strategies[i].__class__.__name__, result))
            elif isinstance(result, Exception):
                logger.warning(f"Strategy {strategies[i].__class__.__name__} failed: {result}")
        
        if not valid_results:
            raise RetrievalError(
                message="All strategies failed",
                details={"strategies": [s.__class__.__name__ for s in strategies]}
            )
        
        # 融合结果
        if fusion_method == "rrf":
            combined = self._fuse_results_rrf(valid_results, max_results)
        elif fusion_method == "weighted":
            combined = self._fuse_results_weighted(valid_results, max_results)
        else:
            combined = self._fuse_results_simple(valid_results, max_results)
        
        retrieval_time = time.time() - start_time
        
        return RetrievalResponse(
            query=query,
            strategy=f"multi_{fusion_method}",
            layers={name: r.combined for name, r in valid_results},
            combined=combined,
            total_found=len(combined),
            retrieval_time=retrieval_time
        )
    
    def _fuse_results_rrf(
        self,
        results: List[Tuple[str, RetrievalResponse]],
        max_results: int,
        k: int = 60
    ) -> List[Dict[str, Any]]:
        """使用倒数秩融合(RRF)合并多策略结果
        
        RRF公式: score = sum(1 / (k + rank))
        """
        from memory_system.core_abstractions.mathematical_utility_functions import reciprocal_rank_fusion
        
        # 构建每策略的结果列表
        strategy_results = []
        for strategy_name, response in results:
            items = [(item.get("memory", {}).get("memory_id", str(i)), item.get("score", 0.0))
                    for i, item in enumerate(response.combined)]
            strategy_results.append(items)
        
        # 应用RRF
        fused = reciprocal_rank_fusion(strategy_results, k=k)
        
        # 构建最终输出
        # 需要重新获取完整的记忆信息
        seen_ids = set()
        combined = []
        for memory_id, rrf_score in fused[:max_results]:
            if memory_id in seen_ids:
                continue
            seen_ids.add(memory_id)
            
            # 从原始结果中查找完整信息
            for strategy_name, response in results:
                for item in response.combined:
                    if item.get("memory", {}).get("memory_id") == memory_id:
                        combined.append({
                            "memory": item.get("memory"),
                            "score": rrf_score,
                            "layer": f"fused_{strategy_name}"
                        })
                        break
                if memory_id in [c.get("memory", {}).get("memory_id") for c in combined]:
                    break
        
        return combined
    
    def _fuse_results_weighted(
        self,
        results: List[Tuple[str, RetrievalResponse]],
        max_results: int
    ) -> List[Dict[str, Any]]:
        """使用加权平均融合结果"""
        # 等权重
        weight = 1.0 / len(results)
        
        # 收集所有结果并计算加权分数
        all_items = {}  # memory_id -> {memory, scores: [], sources: []}
        
        for strategy_name, response in results:
            for item in response.combined:
                memory_id = item.get("memory", {}).get("memory_id")
                if not memory_id:
                    continue
                
                if memory_id not in all_items:
                    all_items[memory_id] = {
                        "memory": item.get("memory"),
                        "scores": [],
                        "sources": []
                    }
                all_items[memory_id]["scores"].append(item.get("score", 0.0) * weight)
                all_items[memory_id]["sources"].append(strategy_name)
        
        # 计算最终分数并排序
        scored_items = []
        for memory_id, data in all_items.items():
            final_score = sum(data["scores"])
            scored_items.append({
                "memory": data["memory"],
                "score": final_score,
                "layer": f"weighted_{'+'.join(set(data['sources']))}"
            })
        
        scored_items.sort(key=lambda x: x["score"], reverse=True)
        return scored_items[:max_results]
    
    def _fuse_results_simple(
        self,
        results: List[Tuple[str, RetrievalResponse]],
        max_results: int
    ) -> List[Dict[str, Any]]:
        """简单融合 - 去重后按分数排序"""
        seen_ids = set()
        all_items = []
        
        for strategy_name, response in results:
            for item in response.combined:
                memory_id = item.get("memory", {}).get("memory_id")
                if memory_id and memory_id not in seen_ids:
                    seen_ids.add(memory_id)
                    all_items.append({
                        "memory": item.get("memory"),
                        "score": item.get("score", 0.0),
                        "layer": strategy_name
                    })
        
        all_items.sort(key=lambda x: x["score"], reverse=True)
        return all_items[:max_results]
    
    def get_stats(self) -> Dict[str, Any]:
        """获取检索统计信息"""
        stats = self._stats.copy()
        if stats["total_searches"] > 0:
            stats["avg_time"] = stats["total_time"] / stats["total_searches"]
            stats["timeout_rate"] = stats["timeout_count"] / stats["total_searches"]
            stats["error_rate"] = stats["error_count"] / stats["total_searches"]
        else:
            stats["avg_time"] = 0.0
            stats["timeout_rate"] = 0.0
            stats["error_rate"] = 0.0
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self._stats = {
            "total_searches": 0,
            "total_time": 0.0,
            "timeout_count": 0,
            "error_count": 0
        }


class RetrievalPipeline:
    """检索管道 - 支持链式检索处理
    
    允许将多个检索处理器串联起来，每个处理器可以
    修改查询、过滤结果或添加元数据。
    """
    
    def __init__(self):
        self._processors: List['RetrievalProcessor'] = []
    
    def add_processor(self, processor: 'RetrievalProcessor') -> 'RetrievalPipeline':
        """添加处理器到管道"""
        self._processors.append(processor)
        return self
    
    async def execute(
        self,
        query: str,
        engine: RetrievalEngine,
        max_results: int = 10,
        context: Optional[RetrievalContext] = None
    ) -> RetrievalResponse:
        """执行管道处理"""
        current_query = query
        current_context = context or RetrievalContext()
        
        # 预处理阶段
        for processor in self._processors:
            if hasattr(processor, 'pre_process'):
                current_query, current_context = await processor.pre_process(
                    current_query, current_context
                )
        
        # 执行检索
        response = await engine.search(
            query=current_query,
            max_results=max_results,
            context=current_context
        )
        
        # 后处理阶段
        for processor in reversed(self._processors):
            if hasattr(processor, 'post_process'):
                response = await processor.post_process(response, current_context)
        
        return response


class RetrievalProcessor(ABC):
    """检索处理器抽象基类"""
    
    @abstractmethod
    async def pre_process(
        self,
        query: str,
        context: RetrievalContext
    ) -> Tuple[str, RetrievalContext]:
        """预处理查询和上下文"""
        pass
    
    @abstractmethod
    async def post_process(
        self,
        response: RetrievalResponse,
        context: RetrievalContext
    ) -> RetrievalResponse:
        """后处理检索结果"""
        pass


class QueryExpansionProcessor(RetrievalProcessor):
    """查询扩展处理器 - 使用同义词扩展查询"""
    
    def __init__(self, expansions: Dict[str, List[str]]):
        self.expansions = expansions
    
    async def pre_process(
        self,
        query: str,
        context: RetrievalContext
    ) -> Tuple[str, RetrievalContext]:
        """扩展查询词"""
        expanded_terms = [query]
        for term, synonyms in self.expansions.items():
            if term in query.lower():
                expanded_terms.extend(synonyms)
        
        expanded_query = " ".join(expanded_terms)
        context.extra["original_query"] = query
        context.extra["expanded_terms"] = expanded_terms
        
        return expanded_query, context
    
    async def post_process(
        self,
        response: RetrievalResponse,
        context: RetrievalContext
    ) -> RetrievalResponse:
        """添加扩展信息到响应"""
        response.layers["query_expansion"] = {
            "original": context.extra.get("original_query"),
            "expanded": context.extra.get("expanded_terms")
        }
        return response


class ResultDeduplicationProcessor(RetrievalProcessor):
    """结果去重处理器"""
    
    async def pre_process(
        self,
        query: str,
        context: RetrievalContext
    ) -> Tuple[str, RetrievalContext]:
        return query, context
    
    async def post_process(
        self,
        response: RetrievalResponse,
        context: RetrievalContext
    ) -> RetrievalResponse:
        """去重并保留最高分数的结果"""
        seen_ids = set()
        unique_results = []
        
        for item in response.combined:
            memory_id = item.get("memory", {}).get("memory_id")
            if memory_id and memory_id not in seen_ids:
                seen_ids.add(memory_id)
                unique_results.append(item)
        
        response.combined = unique_results
        response.total_found = len(unique_results)
        return response

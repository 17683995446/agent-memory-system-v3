"""
纯向量检索策略
简单高效，适合大规模向量库
"""

from typing import Any, Dict, List, Optional

from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy
)
from memory_system.core_abstractions.memory_data_models import RetrievalResult
from memory_system.core_abstractions.component_registry_manager import register_retrieval


@register_retrieval("vector_only")
class VectorOnlyRetrieval(RetrievalStrategy):
    """
    纯向量检索策略
    
    仅使用向量相似度搜索，最简单高效
    适合大规模向量库场景
    """
    
    async def retrieve(
        self,
        query: str,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        max_results: int = 10,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        执行向量检索
        
        Args:
            query: 查询内容
            storage: 存储后端
            embedding: 嵌入提供者
            max_results: 最大结果数
            context: 上下文（可包含filters）
            
        Returns:
            检索结果
        """
        context = context or {}
        
        # 生成查询向量
        query_vector = await embedding.embed(query)
        
        # 获取过滤条件
        filters = context.get("filters")
        
        # 执行向量搜索
        search_results = await storage.search_by_vector(
            query_vector,
            top_k=max_results,
            filters=filters
        )
        
        # 构建结果
        results = {
            "query": query,
            "strategy": "vector_only",
            "layers": {
                "vector": [
                    {"memory": memory.model_dump(), "score": similarity}
                    for memory, similarity in search_results
                ]
            },
            "combined": [
                {
                    "memory": memory.model_dump(),
                    "score": similarity,
                    "layer": "vector"
                }
                for memory, similarity in search_results
            ]
        }
        
        return results

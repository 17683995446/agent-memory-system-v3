"""
抽象接口定义
所有模块都基于这些接口实现，确保可替换性
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
import time

from memory_system.core_abstractions.memory_data_models import MemoryItem, RetrievalResult

if TYPE_CHECKING:
    from memory_system.core_abstractions.memory_data_models import MemoryLink


class StorageBackend(ABC):
    """
    存储后端抽象接口
    
    实现类:
        - ChromaStorage: Chroma向量数据库
        - QdrantStorage: Qdrant向量数据库
        - MemoryStorage: 内存存储 (用于测试)
    """
    
    @abstractmethod
    async def add(self, item: MemoryItem) -> str:
        """
        添加记忆
        
        Args:
            item: 记忆项
            
        Returns:
            记忆ID
        """
        pass
    
    @abstractmethod
    async def get(self, memory_id: str) -> Optional[MemoryItem]:
        """根据ID获取记忆"""
        pass
    
    @abstractmethod
    async def search_by_vector(
        self, 
        query_vector: List[float], 
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[MemoryItem, float]]:
        """
        向量相似度搜索
        
        Args:
            query_vector: 查询向量
            top_k: 返回结果数量
            filters: 过滤条件
            
        Returns:
            (记忆项, 相似度分数) 列表
        """
        pass
    
    @abstractmethod
    async def search_by_time_range(
        self,
        start_time: float,
        end_time: float,
        memory_type: Optional[str] = None
    ) -> List[MemoryItem]:
        """按时间范围搜索"""
        pass
    
    @abstractmethod
    async def delete(self, memory_id: str) -> bool:
        """删除记忆"""
        pass
    
    @abstractmethod
    async def count(self, memory_type: Optional[str] = None) -> int:
        """统计记忆数量"""
        pass
    
    async def update(self, memory_id: str, updates: Dict[str, Any]) -> bool:
        """
        更新记忆（部分更新）
        
        Args:
            memory_id: 记忆ID
            updates: 要更新的字段字典
            
        Returns:
            是否更新成功
        """
        item = await self.get(memory_id)
        if item is None:
            return False
        for key, value in updates.items():
            if hasattr(item, key):
                setattr(item, key, value)
        return True
    
    async def batch_add(self, items: List[MemoryItem]) -> List[str]:
        """
        批量添加记忆（默认实现：循环调用 add）
        
        子类可覆写此方法以利用存储后端的批量写入优化。
        
        Args:
            items: 记忆项列表
            
        Returns:
            记忆ID列表
        """
        ids = []
        for item in items:
            mem_id = await self.add(item)
            ids.append(mem_id)
        return ids
    
    async def batch_delete(self, memory_ids: List[str]) -> int:
        """
        批量删除记忆
        
        Args:
            memory_ids: 记忆ID列表
            
        Returns:
            成功删除的数量
        """
        count = 0
        for memory_id in memory_ids:
            if await self.delete(memory_id):
                count += 1
        return count
    
    async def exists(self, memory_id: str) -> bool:
        """
        检查记忆是否存在
        
        Args:
            memory_id: 记忆ID
            
        Returns:
            是否存在
        """
        return await self.get(memory_id) is not None
    
    async def list_memories(
        self,
        memory_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        ascending: bool = False
    ) -> List[MemoryItem]:
        """
        分页列出记忆（默认实现：基于 search_by_time_range）
        
        子类可覆写此方法以利用存储后端的原生分页能力。
        
        Args:
            memory_type: 记忆类型过滤 (episodic/semantic/None=全部)
            limit: 返回数量上限
            offset: 跳过数量
            order_by: 排序字段
            ascending: 是否升序
            
        Returns:
            记忆项列表
        """
        all_memories = await self.search_by_time_range(0, time.time())
        # 按类型过滤
        if memory_type:
            all_memories = [
                m for m in all_memories
                if m.memory_type.value == memory_type
            ]
        # 排序
        reverse = not ascending
        if order_by == "created_at":
            all_memories.sort(key=lambda m: m.timestamp, reverse=reverse)
        elif order_by == "importance":
            all_memories.sort(key=lambda m: m.importance.value, reverse=reverse)
        elif order_by == "access_count":
            all_memories.sort(key=lambda m: m.access_count, reverse=reverse)
        # 分页
        return all_memories[offset:offset + limit]
    
    async def search_by_metadata(
        self,
        key: str,
        value: Any,
        memory_type: Optional[str] = None,
        top_k: int = 10
    ) -> List[MemoryItem]:
        """
        按元数据搜索记忆
        
        Args:
            key: 元数据键
            value: 元数据值
            memory_type: 记忆类型过滤
            top_k: 最大返回数量
            
        Returns:
            匹配的记忆列表
        """
        results = []
        memories = await self.list_memories(memory_type=memory_type, limit=top_k * 10)
        for memory in memories:
            if memory.metadata.get(key) == value:
                results.append(memory)
                if len(results) >= top_k:
                    break
        return results

    async def search_by_keyword(
        self,
        query_terms: List[str],
        top_k: int = 10,
        memory_type: Optional[str] = None
    ) -> List[Tuple['MemoryItem', float]]:
        """
        关键词搜索（BM25 评分）
        
        Args:
            query_terms: 查询词项列表
            top_k: 返回数量
            memory_type: 记忆类型过滤
            
        Returns:
            (记忆项, BM25分数) 列表
        """
        raise NotImplementedError(f"{self.__class__.__name__} 未实现 search_by_keyword()")

    # ---- 图关联操作 ----
    
    async def add_link(self, link: 'MemoryLink') -> bool:
        """
        添加记忆关联链接
        
        Args:
            link: 记忆关联链接
            
        Returns:
            是否添加成功
        """
        raise NotImplementedError(f"{self.__class__.__name__} 未实现 add_link()")
    
    async def remove_link(self, source_id: str, target_id: str) -> bool:
        """
        移除记忆关联链接
        
        Args:
            source_id: 源记忆ID
            target_id: 目标记忆ID
            
        Returns:
            是否移除成功
        """
        raise NotImplementedError(f"{self.__class__.__name__} 未实现 remove_link()")
    
    async def get_linked_memories(
        self,
        memory_id: str,
        relation: Optional[str] = None,
        max_depth: int = 1
    ) -> List['MemoryItem']:
        """
        获取关联记忆（支持多跳遍历）
        
        Args:
            memory_id: 起始记忆ID
            relation: 关联类型过滤 (None=全部)
            max_depth: 最大遍历深度
            
        Returns:
            关联记忆列表
        """
        raise NotImplementedError(f"{self.__class__.__name__} 未实现 get_linked_memories()")
    
    async def get_links(
        self,
        memory_id: str,
        direction: str = "both"
    ) -> List['MemoryLink']:
        """
        获取指定记忆的所有关联链接
        
        Args:
            memory_id: 记忆ID
            direction: 方向 ("outgoing"/"incoming"/"both")
            
        Returns:
            关联链接列表
        """
        raise NotImplementedError(f"{self.__class__.__name__} 未实现 get_links()")

    @abstractmethod
    async def close(self):
        """关闭连接"""
        pass


class EmbeddingProvider(ABC):
    """
    嵌入模型提供者抽象接口
    
    实现类:
        - BGEEmbedding: BGE系列模型
        - SentenceTransformerEmbedding: Sentence-Transformers
        - OpenAIEmbedding: OpenAI API
    """
    
    @property
    @abstractmethod
    def dimension(self) -> int:
        """返回向量维度"""
        pass
    
    @abstractmethod
    async def embed(self, text: str) -> List[float]:
        """
        将文本转换为向量
        
        Args:
            text: 输入文本
            
        Returns:
            向量表示
        """
        pass
    
    @abstractmethod
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """批量嵌入"""
        pass
    
    @abstractmethod
    async def similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """计算两个向量的相似度"""
        pass


class RetrievalStrategy(ABC):
    """
    检索策略抽象接口
    
    实现类:
        - HierarchicalRetrieval: 分层检索
        - VectorOnlyRetrieval: 仅向量检索
        - KeywordRetrieval: 关键词检索
    """
    
    @abstractmethod
    async def retrieve(
        self,
        query: str,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        max_results: int = 10,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        执行检索
        
        Args:
            query: 查询内容
            storage: 存储后端
            embedding: 嵌入提供者
            max_results: 最大结果数
            context: 上下文信息
            
        Returns:
            检索结果字典
        """
        pass


class ConsolidationStrategy(ABC):
    """
    记忆整合策略抽象接口
    
    实现类:
        - TimeBasedConsolidation: 基于时间的整合
        - SimilarityConsolidation: 基于相似度的整合
    """
    
    @abstractmethod
    async def should_consolidate(
        self,
        memories: List[MemoryItem],
        stats: Dict[str, Any]
    ) -> bool:
        """判断是否需要整合"""
        pass
    
    @abstractmethod
    async def consolidate(
        self,
        memories: List[MemoryItem],
        embedding: EmbeddingProvider
    ) -> List[MemoryItem]:
        """
        执行整合
        
        Args:
            memories: 待整合的记忆列表
            embedding: 嵌入提供者
            
        Returns:
            整合后的记忆列表
        """
        pass


class ForgettingStrategy(ABC):
    """
    遗忘策略抽象接口
    
    实现类:
        - LRUForgotten: 基于LRU的遗忘
        - ScoreBasedForgetting: 基于分数的遗忘
    """
    
    @abstractmethod
    async def select_for_forgetting(
        self,
        memories: List[MemoryItem],
        max_keep: int
    ) -> List[str]:
        """
        选择需要遗忘的记忆ID
        
        Args:
            memories: 所有记忆
            max_keep: 最大保留数量
            
        Returns:
            需要删除的记忆ID列表
        """
        pass


class ShortTermMemoryBackend(ABC):
    """短期记忆后端抽象接口"""
    
    @abstractmethod
    async def add(self, item: MemoryItem) -> str:
        """添加记忆，返回ID"""
        pass
    
    @abstractmethod
    async def get_recent(self, n: int) -> List[MemoryItem]:
        """获取最近n条记忆"""
        pass
    
    @abstractmethod
    async def get_all(self) -> List[MemoryItem]:
        """获取所有记忆"""
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """清空所有记忆"""
        pass
    
    @abstractmethod
    async def is_full(self) -> bool:
        """检查是否已满"""
        pass


class ImportanceScorer(ABC):
    """
    记忆重要性自动评估器抽象接口
    
    实现类:
        - HeuristicImportanceScorer: 基于启发式规则的评估器
    """
    
    @abstractmethod
    async def score(self, content: str, context: Optional[Dict[str, Any]] = None) -> Tuple['ImportanceLevel', float]:
        """
        评估记忆内容的重要性
        
        Args:
            content: 记忆内容
            context: 上下文信息（如已有记忆、对话历史等）
            
        Returns:
            (重要性级别, 置信度 0.0-1.0)
        """
        pass

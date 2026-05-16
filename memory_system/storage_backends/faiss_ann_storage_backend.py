"""
FAISS ANN 向量存储实现
高性能近似最近邻搜索
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
import asyncio
import logging
import time

from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
from memory_system.core_abstractions.component_registry_manager import register_storage
from memory_system.core_abstractions.filter_utility_functions import matches_filters
from memory_system.core_abstractions.system_constants_definitions import DEFAULT_EMBEDDING_DIMENSION

if TYPE_CHECKING:
    from memory_system.configuration_settings.memory_system_configuration import StorageConfig

logger = logging.getLogger(__name__)


@register_storage("faiss")
class FAISSStorage(StorageBackend):
    """
    FAISS ANN 存储后端
    
    特点:
        - Facebook AI 相似度搜索库
        - O(log n) 近似最近邻搜索
        - 支持百万级向量高效检索
        - 纯 CPU 实现，无 GPU 依赖
    
    安装:
        pip install faiss-cpu
    """
    
    _faiss: Optional[Any] = None
    
    def __init__(self, dimension: int = DEFAULT_EMBEDDING_DIMENSION, max_memories: int = 100000):
        """
        初始化
        
        Args:
            dimension: 向量维度
            max_memories: 最大记忆数量
        """
        try:
            import faiss
            self._faiss: Optional[Any] = faiss
        except ImportError:
            raise ImportError(
                "faiss-cpu not installed. "
                "Please install with: pip install faiss-cpu"
            )
        
        self.dimension = dimension
        self.max_memories = max_memories
        self._memories: Dict[str, MemoryItem] = {}
        self._id_to_idx: Dict[str, int] = {}
        self._idx_to_id: Dict[int, str] = {}
        self._next_idx = 0
        self._lock = asyncio.Lock()
        
        # 创建 FAISS 索引 (Inner Product for cosine similarity with normalized vectors)
        # 使用 IndexIDMap 包装以支持删除操作
        base_index = faiss.IndexFlatIP(dimension)
        self._index = faiss.IndexIDMap(base_index)
        
        # 计数器
        self._counts = {
            MemoryType.SHORT_TERM.value: 0,
            MemoryType.EPISODIC.value: 0,
            MemoryType.SEMANTIC.value: 0,
        }
    
    async def add(self, item: MemoryItem) -> str:
        """添加记忆"""
        async with self._lock:
            return await self._add_internal(item)

    async def _add_internal(self, item: MemoryItem) -> str:
        """添加记忆（内部实现，不加锁）"""
        if item.embedding is None:
            # 无嵌入的记忆只存字典，不加入索引
            self._memories[item.memory_id] = item
            return item.memory_id

        import numpy as np

        # 添加到 FAISS 索引
        vector = np.array([item.embedding], dtype=np.float32)
        # 归一化以使用内积近似余弦相似度
        faiss = self._faiss
        faiss.normalize_L2(vector)

        idx = self._next_idx
        ids = np.array([idx], dtype=np.int64)
        self._index.add_with_ids(vector, ids)

        # 维护映射
        self._id_to_idx[item.memory_id] = idx
        self._idx_to_id[idx] = item.memory_id
        self._next_idx += 1

        # 存储记忆
        self._memories[item.memory_id] = item
        self._counts[item.memory_type.value] = self._counts.get(item.memory_type.value, 0) + 1

        # 检查容量
        if len(self._memories) > self.max_memories:
            await self._evict_oldest()

        return item.memory_id
    
    async def get(self, memory_id: str) -> Optional[MemoryItem]:
        """获取记忆"""
        async with self._lock:
            return self._memories.get(memory_id)
    
    async def search_by_vector(
        self,
        query_vector: List[float],
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[MemoryItem, float]]:
        """向量相似度搜索 (ANN)"""
        async with self._lock:
            return await self._search_by_vector_internal(query_vector, top_k, filters)

    async def _search_by_vector_internal(
        self,
        query_vector: List[float],
        top_k: int,
        filters: Optional[Dict[str, Any]]
    ) -> List[Tuple[MemoryItem, float]]:
        """向量相似度搜索内部实现"""
        if self._index.ntotal == 0:
            return []
        
        import numpy as np
        faiss = self._faiss
        
        # 准备查询向量
        query = np.array([query_vector], dtype=np.float32)
        faiss.normalize_L2(query)
        
        # 搜索
        actual_k = min(top_k * 3, self._index.ntotal)  # 多取一些用于过滤
        scores, indices = self._index.search(query, actual_k)
        
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx == -1:
                continue
            
            memory_id = self._idx_to_id.get(idx)
            if memory_id is None:
                continue
            
            memory = self._memories.get(memory_id)
            if memory is None:
                continue
            
            # 应用过滤条件
            if filters and not matches_filters(memory, filters):
                continue
            
            results.append((memory, float(score)))
            
            if len(results) >= top_k:
                break
        
        return results
    
    async def search_by_time_range(
        self,
        start_time: float,
        end_time: float,
        memory_type: Optional[str] = None
    ) -> List[MemoryItem]:
        """按时间范围搜索"""
        results = []
        for memory in self._memories.values():
            if start_time <= memory.timestamp <= end_time:
                if memory_type is None or memory.memory_type.value == memory_type:
                    results.append(memory)
        
        return sorted(results, key=lambda x: x.timestamp, reverse=True)
    
    async def delete(self, memory_id: str) -> bool:
        """删除记忆"""
        async with self._lock:
            return self._delete_internal(memory_id)

    def _delete_internal(self, memory_id: str) -> bool:
        """删除记忆（内部实现，不加锁）"""
        if memory_id not in self._memories:
            return False
        
        memory = self._memories[memory_id]
        del self._memories[memory_id]
        
        # 更新计数
        self._counts[memory.memory_type.value] = max(0, self._counts.get(memory.memory_type.value, 0) - 1)
        
        # 从 FAISS 索引中删除
        if memory_id in self._id_to_idx:
            idx = self._id_to_idx[memory_id]
            try:
                import numpy as np
                ids_to_remove = np.array([idx], dtype=np.int64)
                self._index.remove_ids(ids_to_remove)
            except Exception as e:
                logger.warning(f"FAISS 索引删除失败: {e}")
            del self._id_to_idx[memory_id]
            if idx in self._idx_to_id:
                del self._idx_to_id[idx]
        
        return True
    
    async def count(self, memory_type: Optional[str] = None) -> int:
        """统计记忆数量"""
        if memory_type is None:
            return len(self._memories)
        return self._counts.get(memory_type, 0)
    
    async def close(self):
        """关闭连接"""
        self._memories.clear()
        self._id_to_idx.clear()
        self._idx_to_id.clear()
        self._counts = {
            MemoryType.SHORT_TERM.value: 0,
            MemoryType.EPISODIC.value: 0,
            MemoryType.SEMANTIC.value: 0,
        }
        self._next_idx = 0
        base_index = self._faiss.IndexFlatIP(self.dimension)
        self._index = self._faiss.IndexIDMap(base_index)

    def save_index(self, filepath: str) -> None:
        """
        保存FAISS索引到文件
        
        将当前的FAISS索引和所有相关元数据持久化到磁盘。
        保存的内容包括:
            - FAISS索引文件 (filepath)
            - 元数据文件 (filepath + ".meta.json")，包含:
                - ID到索引的映射
                - 索引到ID的映射
                - 所有记忆数据的序列化形式
        
        Args:
            filepath: 索引文件保存路径。父目录会自动创建。
            
        Note:
            - 此方法是同步的，不涉及异步操作
            - 如果文件已存在会被覆盖
            - 需要确保有足够的磁盘空间
            
        Example:
            >>> storage.save_index("./data/faiss_index.bin")
            # 会创建两个文件:
            # - ./data/faiss_index.bin (FAISS索引)
            # - ./data/faiss_index.bin.meta.json (元数据)
        """
        import os
        import json

        dirpath = os.path.dirname(filepath)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)

        # 保存FAISS索引
        self._faiss.write_index(self._index, filepath)

        # 保存元数据（ID映射和记忆数据）
        meta_path = filepath + ".meta.json"
        meta_data = {
            "id_to_idx": self._id_to_idx,
            "idx_to_id": {str(k): v for k, v in self._idx_to_id.items()},
            "memories": {k: v.to_storage_dict() for k, v in self._memories.items()}
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_data, f, ensure_ascii=False, default=str)

    @classmethod
    def load_index(cls, filepath: str, dimension: int = 768) -> "FAISSStorage":
        """
        从文件加载FAISS索引
        
        从磁盘加载之前保存的FAISS索引和相关元数据，恢复存储状态。
        
        Args:
            filepath: 索引文件路径
            dimension: 向量维度，默认为768
            
        Returns:
            恢复的FAISSStorage实例
            
        Raises:
            FileNotFoundError: 如果索引文件或元数据文件不存在
            ValueError: 如果元数据文件格式无效
            
        Note:
            - 需要同时存在索引文件和元数据文件
            - 元数据文件路径为 filepath + ".meta.json"
            - 加载后的实例可以立即使用
            
        Example:
            >>> storage = FAISSStorage.load_index("./data/faiss_index.bin")
            >>> results = await storage.search_by_vector(query_vector, top_k=5)
        """
        import json
        from memory_system.core_abstractions.memory_data_models import MemoryItem

        # 加载FAISS索引
        import faiss
        index = faiss.read_index(filepath)

        # 加载元数据
        meta_path = filepath + ".meta.json"
        with open(meta_path, "r", encoding="utf-8") as f:
            meta_data = json.load(f)

        # 创建实例
        instance = cls(dimension=dimension)
        instance._index = index
        instance._id_to_idx = meta_data["id_to_idx"]
        instance._idx_to_id = {int(k): v for k, v in meta_data["idx_to_id"].items()}
        instance._memories = {
            k: MemoryItem.from_storage_dict(v)
            for k, v in meta_data["memories"].items()
        }

        # 更新 next_idx 以避免 ID 冲突
        if instance._idx_to_id:
            instance._next_idx = max(instance._idx_to_id.keys()) + 1

        # 重建 _counts 以与实际记忆数据一致
        instance._counts = {}
        for m in instance._memories.values():
            t = m.memory_type.value
            instance._counts[t] = instance._counts.get(t, 0) + 1

        return instance
    
    async def _evict_oldest(self):
        """淘汰最旧的记忆"""
        if not self._memories:
            return
        
        oldest_id = min(
            self._memories.keys(),
            key=lambda k: self._memories[k].timestamp
        )
        self._delete_internal(oldest_id)
    
    @classmethod
    def from_config(
        cls,
        config: "StorageConfig",
        embedding_dim: int = DEFAULT_EMBEDDING_DIMENSION
    ) -> "FAISSStorage":
        return cls(dimension=embedding_dim, max_memories=config.max_memories)

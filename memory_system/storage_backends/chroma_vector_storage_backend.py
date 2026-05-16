"""
Chroma向量数据库存储实现
轻量级、开发友好
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.core_abstractions.component_registry_manager import register_storage
from memory_system.core_abstractions.filter_utility_functions import separate_metadata, STORAGE_SYSTEM_KEYS
from memory_system.core_abstractions.system_constants_definitions import DEFAULT_EMBEDDING_DIMENSION

if TYPE_CHECKING:
    from memory_system.configuration_settings.memory_system_configuration import StorageConfig

# 条件注册：ChromaDB 不可用时不注册，避免运行时 ImportError
_CHROMADB_AVAILABLE = False
try:
    import chromadb
    from chromadb.config import Settings
    _CHROMADB_AVAILABLE = True
except ImportError:
    _CHROMADB_AVAILABLE = False


if _CHROMADB_AVAILABLE:
    class ChromaStorage(StorageBackend):
        """
        Chroma存储后端
        
        特点:
            - 轻量级，易于集成
            - 支持内存和持久化模式
            - 适合中小规模应用 (<100万向量)
            - Python原生支持好
        
        安装:
            pip install chromadb
        """
        
        def __init__(
            self,
            collection_name: str = "agent_memory",
            persist_path: Optional[str] = None,
            embedding_dimension: int = DEFAULT_EMBEDDING_DIMENSION
        ):
            """
            初始化Chroma存储
            
            Args:
                collection_name: 集合名称
                persist_path: 持久化路径，None则使用内存模式
                embedding_dimension: 向量维度
            """
            self.collection_name = collection_name
            self.embedding_dimension = embedding_dimension
            
            # 创建客户端
            if persist_path:
                self.client = chromadb.PersistentClient(
                    path=persist_path,
                    settings=Settings(anonymized_telemetry=False)
                )
            else:
                self.client = chromadb.Client(
                    settings=Settings(anonymized_telemetry=False)
                )
            
            # 获取或创建集合
            self.collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata={"hnsw:space": "cosine"}
            )
        
        async def add(self, item: MemoryItem) -> str:
            """添加记忆"""
            # 准备元数据
            metadata = {
                "memory_type": item.memory_type.value,
                "importance": item.importance.value,
                "timestamp": item.timestamp,
                "access_count": item.access_count,
                "last_access": item.last_access,
                **item.metadata
            }

            # 添加到集合
            await asyncio.to_thread(
                self.collection.add,
                ids=[item.memory_id],
                documents=[item.content],
                metadatas=[metadata],
                embeddings=[item.embedding] if item.embedding else None
            )

            return item.memory_id

        async def get(self, memory_id: str) -> Optional[MemoryItem]:
            """获取记忆"""
            try:
                result = await asyncio.to_thread(self.collection.get, ids=[memory_id])
                if not result["ids"]:
                    return None

                return self._convert_to_memory_item(
                    result["ids"][0],
                    result["documents"][0],
                    result["metadatas"][0],
                    result.get("embeddings", [None])[0]
                )
            except Exception as e:
                logger.debug("Failed to get memory %s: %s", memory_id, e)
                return None

        async def search_by_vector(
            self,
            query_vector: List[float],
            top_k: int = 5,
            filters: Optional[Dict[str, Any]] = None
        ) -> List[Tuple[MemoryItem, float]]:
            """向量相似度搜索"""
            # 转换过滤条件
            where_clause = self._convert_filters(filters) if filters else None

            # 执行查询
            results = await asyncio.to_thread(
                self.collection.query,
                query_embeddings=[query_vector],
                n_results=top_k,
                where=where_clause,
                include=["metadatas", "documents", "distances", "embeddings"]
            )

            # 转换为MemoryItem
            items = []
            if results["ids"] and results["ids"][0]:
                for i, memory_id in enumerate(results["ids"][0]):
                    distance = results["distances"][0][i]
                    # Chroma返回的是距离，转换为相似度 (余弦距离 -> 余弦相似度)
                    similarity = 1 - distance

                    item = self._convert_to_memory_item(
                        memory_id,
                        results["documents"][0][i],
                        results["metadatas"][0][i],
                        results.get("embeddings", [[None] * (top_k + 1)])[0][i] if results.get("embeddings") else None
                    )
                    items.append((item, similarity))

            return items
        
        async def search_by_time_range(
            self,
            start_time: float,
            end_time: float,
            memory_type: Optional[str] = None
        ) -> List[MemoryItem]:
            """按时间范围搜索"""
            # 构建过滤条件
            # Chroma where 子句不支持同一字段多个操作符，改用 $and 拆分
            conditions = []
            conditions.append({"timestamp": {"$gte": start_time}})
            conditions.append({"timestamp": {"$lte": end_time}})
            if memory_type:
                conditions.append({"memory_type": memory_type})

            if len(conditions) == 1:
                where_clause = conditions[0]
            else:
                where_clause = {"$and": conditions}
            
            # 查询所有匹配的记录
            results = await asyncio.to_thread(
                self.collection.get,
                where=where_clause,
                include=["metadatas", "documents", "embeddings"]
            )
            
            items = []
            if results["ids"]:
                for i, memory_id in enumerate(results["ids"]):
                    item = self._convert_to_memory_item(
                        memory_id,
                        results["documents"][i],
                        results["metadatas"][i],
                        results.get("embeddings", [None])[i] if results.get("embeddings") else None
                    )
                    items.append(item)
            
            # 按时间排序
            items.sort(key=lambda x: x.timestamp, reverse=True)
            return items
        
        async def delete(self, memory_id: str) -> bool:
            """删除记忆"""
            try:
                await asyncio.to_thread(self.collection.delete, ids=[memory_id])
                return True
            except Exception as e:
                logger.warning("删除记忆失败 [%s]: %s", memory_id, e)
                return False

        async def count(self, memory_type: Optional[str] = None) -> int:
            """统计记忆数量"""
            if memory_type:
                results = await asyncio.to_thread(
                    self.collection.get,
                    where={"memory_type": memory_type}
                )
                return len(results["ids"]) if results["ids"] else 0

            return await asyncio.to_thread(self.collection.count)
        
        async def close(self):
            """关闭连接"""
            # Chroma自动管理连接，无需显式关闭
            pass
        
        def _convert_to_memory_item(
            self,
            memory_id: str,
            content: str,
            metadata: Dict[str, Any],
            embedding: Optional[List[float]] = None
        ) -> MemoryItem:
            """将Chroma记录转换为MemoryItem"""
            # 提取元数据
            memory_type = MemoryType(metadata.get("memory_type", "semantic"))
            importance = ImportanceLevel(metadata.get("importance", 3))
            
            # 分离系统元数据和用户元数据
            system_data, user_metadata = separate_metadata(metadata, STORAGE_SYSTEM_KEYS)
            
            return MemoryItem(
                memory_id=memory_id,
                content=content,
                memory_type=memory_type,
                importance=importance,
                timestamp=metadata.get("timestamp", time.time()),
                access_count=metadata.get("access_count", 0),
                last_access=metadata.get("last_access", time.time()),
                embedding=embedding,
                metadata=user_metadata
            )
        
        def _convert_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
            """转换过滤条件为Chroma格式"""
            where_clause = {}
            
            for key, value in filters.items():
                if key == "memory_type":
                    where_clause["memory_type"] = value
                elif key == "importance":
                    where_clause["importance"] = value
                elif key.startswith("metadata."):
                    meta_key = key[9:]
                    where_clause[meta_key] = value
            
            return where_clause

        @classmethod
        def from_config(
            cls,
            config: "StorageConfig",
            embedding_dim: int = DEFAULT_EMBEDDING_DIMENSION
        ) -> "ChromaStorage":
            return cls(
                collection_name=config.chroma_collection,
                persist_path=config.chroma_path,
                embedding_dimension=embedding_dim
            )

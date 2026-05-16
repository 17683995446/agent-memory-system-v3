"""
Qdrant向量数据库存储实现
高性能、生产级
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
import asyncio
import time
import logging

from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.core_abstractions.component_registry_manager import register_storage
from memory_system.core_abstractions.filter_utility_functions import separate_metadata, STORAGE_SYSTEM_KEYS
from memory_system.core_abstractions.system_constants_definitions import DEFAULT_EMBEDDING_DIMENSION

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from memory_system.configuration_settings.memory_system_configuration import StorageConfig

# 重试机制支持
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# 条件注册：Qdrant 不可用时不注册
_QDRANT_AVAILABLE = False
try:
    from qdrant_client import QdrantClient
    _QDRANT_AVAILABLE = True
except ImportError:
    _QDRANT_AVAILABLE = False


def _with_retry(func):
    """为方法添加重试装饰器"""
    if not TENACITY_AVAILABLE:
        return func
    
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True
    )(func)

if _QDRANT_AVAILABLE:

    @register_storage("qdrant")
    class QdrantStorage(StorageBackend):
        """
        Qdrant存储后端
        
        特点:
            - Rust编写，高性能
            - 支持混合检索（稠密+稀疏向量）
            - 强大的payload过滤
            - 支持分布式部署
            - 生产级推荐
        
        安装:
            pip install qdrant-client
        """
        
        def __init__(
            self,
            collection_name: str = "agent_memory",
            host: str = "localhost",
            port: int = 6333,
            api_key: Optional[str] = None,
            embedding_dimension: int = DEFAULT_EMBEDDING_DIMENSION,
            url: Optional[str] = None
        ):
            """
            初始化Qdrant存储
            
            Args:
                collection_name: 集合名称
                host: Qdrant主机
                port: Qdrant端口
                api_key: API密钥（云端部署时使用）
                embedding_dimension: 向量维度
                url: 完整URL（替代host+port）
            """
            try:
                from qdrant_client import QdrantClient
                from qdrant_client.models import Distance, VectorParams
            except ImportError:
                raise ImportError(
                    "Qdrant client not installed. "
                    "Please install with: pip install qdrant-client"
                )
            
            self.collection_name = collection_name
            self.embedding_dimension = embedding_dimension
            
            # 创建客户端
            if url:
                self.client = QdrantClient(url=url, api_key=api_key)
            else:
                self.client = QdrantClient(host=host, port=port, api_key=api_key)
            
            # 确保集合存在
            self._ensure_collection()
        
        @_with_retry
        def _ensure_collection(self):
            """确保集合存在"""
            from qdrant_client.models import Distance, VectorParams
            
            try:
                self.client.get_collection(self.collection_name)
            except Exception as e:
                # 集合不存在，创建新集合
                logger.info("集合不存在，创建新集合 [%s]: %s", self.collection_name, e)
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.embedding_dimension,
                        distance=Distance.COSINE
                    )
                )
        
        async def add(self, item: MemoryItem) -> str:
            """添加记忆"""
            from qdrant_client.models import PointStruct
            
            # 准备payload
            payload = {
                "content": item.content,
                "memory_type": item.memory_type.value,
                "importance": item.importance.value,
                "timestamp": item.timestamp,
                "access_count": item.access_count,
                "last_access": item.last_access,
                **item.metadata
            }
            
            # 创建点
            point = PointStruct(
                id=item.memory_id,
                vector=item.embedding if item.embedding else None,
                payload=payload
            )
            
            # 上传到Qdrant
            await asyncio.to_thread(
                self.client.upsert,
                collection_name=self.collection_name,
                points=[point]
            )
            
            return item.memory_id
        
        async def get(self, memory_id: str) -> Optional[MemoryItem]:
            """获取记忆"""
            try:
                result = await asyncio.to_thread(
                    self.client.retrieve,
                    collection_name=self.collection_name,
                    ids=[memory_id],
                    with_payload=True,
                    with_vectors=True
                )
                
                if not result:
                    return None
                
                return self._convert_to_memory_item(result[0])
            except Exception as e:
                logger.warning("获取记忆失败 [%s]: %s", memory_id, e)
                return None
        
        async def search_by_vector(
            self,
            query_vector: List[float],
            top_k: int = 5,
            filters: Optional[Dict[str, Any]] = None
        ) -> List[Tuple[MemoryItem, float]]:
            """向量相似度搜索"""
            from qdrant_client.models import Filter, FieldCondition, MatchValue
            
            # 构建过滤条件
            query_filter = None
            if filters:
                conditions = []
                for key, value in filters.items():
                    if key == "memory_type":
                        conditions.append(
                            FieldCondition(key="memory_type", match=MatchValue(value=value))
                        )
                    elif key == "importance":
                        conditions.append(
                            FieldCondition(key="importance", match=MatchValue(value=value))
                        )
                    elif key.startswith("metadata."):
                        meta_key = key[9:]
                        conditions.append(
                            FieldCondition(key=meta_key, match=MatchValue(value=value))
                        )
                
                if conditions:
                    query_filter = Filter(must=conditions)
            
            # 执行搜索
            results = await asyncio.to_thread(
                self.client.search,
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=top_k,
                query_filter=query_filter,
                with_payload=True,
                with_vectors=True
            )
            
            # 转换为MemoryItem
            items = []
            for result in results:
                item = self._convert_to_memory_item(result)
                items.append((item, result.score))
            
            return items
        
        async def search_by_time_range(
            self,
            start_time: float,
            end_time: float,
            memory_type: Optional[str] = None
        ) -> List[MemoryItem]:
            """按时间范围搜索"""
            from qdrant_client.models import Filter, FieldCondition, Range, MatchValue
            
            # 构建时间范围过滤
            conditions = [
                FieldCondition(
                    key="timestamp",
                    range=Range(gte=start_time, lte=end_time)
                )
            ]
            
            if memory_type:
                conditions.append(
                    FieldCondition(key="memory_type", match=MatchValue(value=memory_type))
                )
            
            query_filter = Filter(must=conditions)
            
            # 执行滚动查询（获取所有匹配结果）
            # scroll() 返回元组 (results, offset)，需要正确解包
            # 使用循环确保获取所有结果（scroll 每次最多返回 limit 条）
            all_results = []
            offset = None
            while True:
                results, offset = await asyncio.to_thread(
                    self.client.scroll,
                    collection_name=self.collection_name,
                    scroll_filter=query_filter,
                    limit=1000,
                    offset=offset,
                    with_payload=True,
                    with_vectors=True
                )
                all_results.extend(results)
                if offset is None:
                    break
    
            # 转换为MemoryItem并排序
            items = [self._convert_to_memory_item(r) for r in all_results]
            items.sort(key=lambda x: x.timestamp, reverse=True)
            
            return items
        
        async def delete(self, memory_id: str) -> bool:
            """删除记忆"""
            try:
                await asyncio.to_thread(
                    self.client.delete,
                    collection_name=self.collection_name,
                    points_selector=[memory_id]
                )
                return True
            except Exception as e:
                logger.warning("删除记忆失败 [%s]: %s", memory_id, e)
                return False
        
        async def count(self, memory_type: Optional[str] = None) -> int:
            """统计记忆数量"""
            from qdrant_client.models import Filter, FieldCondition, MatchValue
            
            query_filter = None
            if memory_type:
                query_filter = Filter(
                    must=[FieldCondition(key="memory_type", match=MatchValue(value=memory_type))]
                )
            
            result = await asyncio.to_thread(
                self.client.count,
                collection_name=self.collection_name,
                count_filter=query_filter
            )
            
            return result.count
        
        async def close(self):
            """关闭连接"""
            await asyncio.to_thread(self.client.close)
        
        def _convert_to_memory_item(self, point) -> MemoryItem:
            """将Qdrant点转换为MemoryItem"""
            payload = point.payload or {}
            
            # 提取系统字段（使用get而非pop，避免破坏原始payload）
            content = payload.get("content", "")
            memory_type = MemoryType(payload.get("memory_type", "semantic"))
            importance = ImportanceLevel(payload.get("importance", 3))
            timestamp = payload.get("timestamp", time.time())
            access_count = payload.get("access_count", 0)
            last_access = payload.get("last_access", time.time())
            
            # 分离系统元数据和用户元数据
            system_data, user_metadata = separate_metadata(payload, STORAGE_SYSTEM_KEYS)
            
            return MemoryItem(
                memory_id=str(point.id),
                content=content,
                memory_type=memory_type,
                importance=importance,
                timestamp=timestamp,
                access_count=access_count,
                last_access=last_access,
                embedding=point.vector if hasattr(point, 'vector') else None,
                metadata=user_metadata
            )
    
        @classmethod
        def from_config(
            cls,
            config: "StorageConfig",
            embedding_dim: int = DEFAULT_EMBEDDING_DIMENSION
        ) -> "QdrantStorage":
            return cls(
                collection_name=config.qdrant_collection,
                host=config.qdrant_host,
                port=config.qdrant_port,
                api_key=config.qdrant_api_key,
                embedding_dimension=embedding_dim
            )

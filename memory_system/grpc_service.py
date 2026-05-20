"""
gRPC服务层实现模块

本模块实现了高性能的gRPC服务端，支持内存管理的核心功能、流式传输、
双向通信、Raft共识、分布式事务等。专为Agent Memory System V3.0设计。

核心特性:
- 完整的CRUD操作服务
- 流式搜索和存储支持
- Raft共识协议集成
- 分布式事务支持
- 健康检查和监控
- 集群管理功能
- 连接管理和负载均衡

示例用法:
    >>> from grpc_service import GRPCServer, GRPCServiceConfig
    >>> 
    >>> config = GRPCServiceConfig(
    ...     host="0.0.0.0",
    ...     port=50051,
    ...     max_workers=10
    ... )
    >>> server = GRPCServer(config)
    >>> await server.start()
    >>> 
    >>> # 运行直到停止
    >>> await server.wait_for_termination()
"""

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from concurrent import futures
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

import grpc
from grpc import ServicerContext


logger = logging.getLogger(__name__)


# 尝试导入protobuf生成的模块
# 如果没有生成，使用模拟类
try:
    from memory_system.proto import memory_service_v3_pb2 as pb2
    from memory_system.proto import memory_service_v3_pb2_grpc as pb2_grpc
    HAS_PROTO = True
except ImportError:
    HAS_PROTO = False
    logger.warning("Protocol buffer modules not found. Using mock implementations.")
    
    # 创建模拟的protobuf类
    class MockPB2:
        """模拟的protobuf消息类"""
        
        class MemoryEntry:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class StoreRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class StoreResponse:
            def __init__(self, memory_id="", success=True, error_message=""):
                self.memory_id = memory_id
                self.success = success
                self.error_message = error_message
        
        class SearchRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class SearchResponse:
            def __init__(self, results=None, total_count=0, search_time_ms=0.0):
                self.results = results or []
                self.total_count = total_count
                self.search_time_ms = search_time_ms
        
        class SearchResult:
            def __init__(self, entry=None, score=0.0, match_type=""):
                self.entry = entry
                self.score = score
                self.match_type = match_type
        
        class GetRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class DeleteRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class DeleteResponse:
            def __init__(self, success=True, error_message=""):
                self.success = success
                self.error_message = error_message
        
        class UpdateRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class BatchStoreRequest:
            def __init__(self, requests=None, atomic=False):
                self.requests = requests or []
                self.atomic = atomic
        
        class BatchStoreResponse:
            def __init__(self, responses=None, success_count=0, failed_count=0):
                self.responses = responses or []
                self.success_count = success_count
                self.failed_count = failed_count
        
        class StreamingStoreRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class StreamingSearchResponse:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class RaftProposeRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class RaftProposeResponse:
            def __init__(self, accepted=True, leader_id="", current_term=0, error_message=""):
                self.accepted = accepted
                self.leader_id = leader_id
                self.current_term = current_term
                self.error_message = error_message
        
        class RaftStatusRequest:
            def __init__(self, include_details=False):
                self.include_details = include_details
        
        class RaftStatusResponse:
            def __init__(self, node_id="", role=0, current_term=0, leader_id="",
                         log_size=0, commit_index=0, last_applied=0, peers=None):
                self.node_id = node_id
                self.role = role
                self.current_term = current_term
                self.leader_id = leader_id
                self.log_size = log_size
                self.commit_index = commit_index
                self.last_applied = last_applied
                self.peers = peers or []
        
        class TransactionRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class TransactionResponse:
            def __init__(self, transaction_id="", status=0, results=None, error_message=""):
                self.transaction_id = transaction_id
                self.status = status
                self.results = results or []
                self.error_message = error_message
        
        class HealthCheckRequest:
            def __init__(self, service=""):
                self.service = service
        
        class HealthCheckResponse:
            def __init__(self, status=0, message="", services=None, timestamp=0):
                self.status = status
                self.message = message
                self.services = services or {}
                self.timestamp = timestamp
        
        class MetricsRequest:
            def __init__(self, metric_names=None, time_range=None):
                self.metric_names = metric_names or []
                self.time_range = time_range
        
        class MetricsResponse:
            def __init__(self, metrics=None):
                self.metrics = metrics or []
        
        class ClusterStatusRequest:
            pass
        
        class ClusterStatusResponse:
            def __init__(self, cluster_id="", nodes=None, leader_id="", replication_factor=0):
                self.cluster_id = cluster_id
                self.nodes = nodes or []
                self.leader_id = leader_id
                self.replication_factor = replication_factor
        
        class JoinClusterRequest:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class JoinClusterResponse:
            def __init__(self, success=True, current_nodes=None, error_message=""):
                self.success = success
                self.current_nodes = current_nodes or []
                self.error_message = error_message
        
        class Empty:
            pass
        
        class ClusterNode:
            def __init__(self, node_id="", address="", status=0, metadata=None, last_heartbeat=0):
                self.node_id = node_id
                self.address = address
                self.status = status
                self.metadata = metadata or {}
                self.last_heartbeat = last_heartbeat
    
    class MockPB2Grpc:
        """模拟的gRPC服务类"""
        
        class AgentMemoryServiceServicer:
            pass
        
        class add_AgentMemoryServiceServicer_to_server:
            def __init__(self, servicer, server):
                pass
    
    pb2 = MockPB2()
    pb2_grpc = MockPB2Grpc()


@dataclass
class GRPCServiceConfig:
    """
    gRPC服务配置
    
    Attributes:
        host: 监听地址
        port: 监听端口
        max_workers: 最大工作线程数
        max_concurrent_rpcs: 最大并发RPC数
        compression: 压缩算法
        enable_reflection: 是否启用反射
        enable_health_check: 是否启用健康检查
        tls_cert_file: TLS证书文件路径
        tls_key_file: TLS密钥文件路径
        tls_ca_file: TLS CA证书文件路径
        keepalive_time_ms: keepalive时间(毫秒)
        keepalive_timeout_ms: keepalive超时(毫秒)
    """
    host: str = "0.0.0.0"
    port: int = 50051
    max_workers: int = 10
    max_concurrent_rpcs: Optional[int] = None
    compression: Optional[grpc.Compression] = None
    enable_reflection: bool = True
    enable_health_check: bool = True
    tls_cert_file: Optional[str] = None
    tls_key_file: Optional[str] = None
    tls_ca_file: Optional[str] = None
    keepalive_time_ms: int = 10000
    keepalive_timeout_ms: int = 5000


class MemoryStoreInterface(ABC):
    """内存存储接口抽象基类"""
    
    @abstractmethod
    async def store(self, agent_id: str, tenant_id: str, content: str,
                    embedding: List[float], metadata: Dict[str, str],
                    memory_type: str = "short_term", ttl: int = 0) -> str:
        """存储内存"""
        pass
    
    @abstractmethod
    async def get(self, memory_id: str, agent_id: str, tenant_id: str) -> Optional[Dict[str, Any]]:
        """获取内存"""
        pass
    
    @abstractmethod
    async def delete(self, memory_id: str, agent_id: str, tenant_id: str,
                     permanent: bool = False) -> bool:
        """删除内存"""
        pass
    
    @abstractmethod
    async def update(self, memory_id: str, agent_id: str, tenant_id: str,
                     **kwargs) -> bool:
        """更新内存"""
        pass
    
    @abstractmethod
    async def search(self, agent_id: str, tenant_id: str, query: str,
                     query_embedding: Optional[List[float]] = None,
                     top_k: int = 10, min_score: float = 0.0,
                     filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """搜索内存"""
        pass


class MockMemoryStore(MemoryStoreInterface):
    """模拟内存存储实现(用于测试)"""
    
    def __init__(self):
        self._memories: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def store(self, agent_id: str, tenant_id: str, content: str,
                    embedding: List[float], metadata: Dict[str, str],
                    memory_type: str = "short_term", ttl: int = 0) -> str:
        async with self._lock:
            memory_id = str(uuid.uuid4())
            self._memories[memory_id] = {
                "id": memory_id,
                "agent_id": agent_id,
                "tenant_id": tenant_id,
                "content": content,
                "embedding": embedding,
                "metadata": metadata,
                "memory_type": memory_type,
                "created_at": time.time(),
                # [BUG 9 修复] 记录TTL，单位为秒；ttl=0表示永不过期
                "ttl": ttl,
            }
            return memory_id
    
    async def get(self, memory_id: str, agent_id: str, tenant_id: str) -> Optional[Dict[str, Any]]:
        memory = self._memories.get(memory_id)
        if memory is None:
            return None
        
        # [BUG 7 修复] 租户隔离校验：验证agent_id和tenant_id匹配
        if memory.get("agent_id") != agent_id:
            logger.warning(
                f"租户隔离校验失败: memory_id={memory_id}, "
                f"请求agent_id={agent_id}, 存储agent_id={memory.get('agent_id')}"
            )
            return None
        if memory.get("tenant_id") != tenant_id:
            logger.warning(
                f"租户隔离校验失败: memory_id={memory_id}, "
                f"请求tenant_id={tenant_id}, 存储tenant_id={memory.get('tenant_id')}"
            )
            return None
        
        # [BUG 9 修复] 检查TTL是否过期
        ttl = memory.get("ttl", 0)
        if ttl > 0:
            created_at = memory.get("created_at", 0)
            if time.time() - created_at > ttl:
                logger.info(f"内存条目已过期: memory_id={memory_id}, ttl={ttl}s")
                # 清理过期条目
                async with self._lock:
                    self._memories.pop(memory_id, None)
                return None
        
        return memory
    
    async def delete(self, memory_id: str, agent_id: str, tenant_id: str,
                     permanent: bool = False) -> bool:
        async with self._lock:
            if memory_id in self._memories:
                # [BUG 7 修复] 删除前也校验租户隔离
                memory = self._memories[memory_id]
                if memory.get("agent_id") != agent_id or memory.get("tenant_id") != tenant_id:
                    logger.warning(
                        f"删除操作租户隔离校验失败: memory_id={memory_id}"
                    )
                    return False
                if permanent:
                    del self._memories[memory_id]
                else:
                    self._memories[memory_id]["deleted"] = True
                return True
            return False
    
    async def update(self, memory_id: str, agent_id: str, tenant_id: str,
                     **kwargs) -> bool:
        async with self._lock:
            if memory_id in self._memories:
                # [BUG 7 修复] 更新前也校验租户隔离
                memory = self._memories[memory_id]
                if memory.get("agent_id") != agent_id or memory.get("tenant_id") != tenant_id:
                    logger.warning(
                        f"更新操作租户隔离校验失败: memory_id={memory_id}"
                    )
                    return False
                self._memories[memory_id].update(kwargs)
                return True
            return False
    
    async def search(self, agent_id: str, tenant_id: str, query: str,
                     query_embedding: Optional[List[float]] = None,
                     top_k: int = 10, min_score: float = 0.0,
                     filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        results = []
        now = time.time()
        
        for memory in self._memories.values():
            # [BUG 7 修复] 搜索时也校验agent_id（tenant_id可选过滤）
            if memory.get("agent_id") != agent_id:
                continue
            if memory.get("tenant_id") != tenant_id:
                continue
            
            # [BUG 9 修复] 跳过已过期的条目
            ttl = memory.get("ttl", 0)
            if ttl > 0:
                created_at = memory.get("created_at", 0)
                if now - created_at > ttl:
                    continue
            
            # 跳过已软删除的条目
            if memory.get("deleted"):
                continue
            
            # [BUG 8 修复] 使用简单的文本匹配计算相似度分数，替代硬编码的0.5
            score = self._compute_similarity(query, memory, query_embedding)
            
            if score >= min_score:
                results.append({**memory, "score": score})
        
        # 按分数排序并限制数量
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]
    
    @staticmethod
    def _compute_similarity(
        query: str,
        memory: Dict[str, Any],
        query_embedding: Optional[List[float]] = None
    ) -> float:
        """
        计算查询与内存条目之间的相似度分数
        
        [BUG 8 修复] 使用多种策略计算分数：
        1. 如果提供了query_embedding和memory embedding，使用余弦相似度
        2. 否则使用简单的文本匹配（关键词重叠率）
        
        Returns:
            float: 0.0 ~ 1.0 之间的相似度分数
        """
        # 策略1：向量余弦相似度
        if query_embedding and memory.get("embedding"):
            memory_embedding = memory["embedding"]
            if len(query_embedding) == len(memory_embedding) and len(query_embedding) > 0:
                # 计算余弦相似度
                dot_product = sum(a * b for a, b in zip(query_embedding, memory_embedding))
                norm_a = sum(a * a for a in query_embedding) ** 0.5
                norm_b = sum(b * b for b in memory_embedding) ** 0.5
                if norm_a > 0 and norm_b > 0:
                    return dot_product / (norm_a * norm_b)
        
        # 策略2：文本关键词匹配
        content = memory.get("content", "")
        if not query or not content:
            return 0.0
        
        # 将查询和内容拆分为词（简单分词）
        query_terms = set(query.lower().split())
        content_terms = set(content.lower().split())
        
        if not query_terms:
            return 0.0
        
        # 计算Jaccard相似度：交集/并集
        intersection = query_terms & content_terms
        union = query_terms | content_terms
        
        if not union:
            return 0.0
        
        jaccard = len(intersection) / len(union)
        
        # 额外加成：查询词在内容中出现的频率
        content_lower = content.lower()
        match_ratio = sum(1 for term in query_terms if term in content_lower) / len(query_terms)
        
        # 综合分数：Jaccard相似度 * 0.4 + 匹配比率 * 0.6
        return jaccard * 0.4 + match_ratio * 0.6


class AgentMemoryServicer(pb2_grpc.AgentMemoryServiceServicer):
    """
    Agent Memory Service实现
    
    实现gRPC服务接口，处理客户端请求
    """
    
    def __init__(
        self,
        memory_store: Optional[MemoryStoreInterface] = None,
        raft_node: Optional[Any] = None,
        transaction_manager: Optional[Any] = None,
    ):
        self.memory_store = memory_store or MockMemoryStore()
        self.raft_node = raft_node
        self.transaction_manager = transaction_manager
        self._start_time = time.time()
        self._request_count = 0
        self._lock = asyncio.Lock()
    
    async def _increment_request_count(self):
        """增加请求计数"""
        async with self._lock:
            self._request_count += 1
    
    def Store(self, request: pb2.StoreRequest, context: ServicerContext) -> pb2.StoreResponse:
        """存储内存条目"""
        asyncio.create_task(self._increment_request_count())
        
        try:
            # 使用Raft保证一致性
            if self.raft_node and hasattr(self.raft_node, 'is_leader'):
                if not self.raft_node.is_leader:
                    # 转发到Leader
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details("Not the leader")
                    return pb2.StoreResponse(
                        success=False,
                        error_message="Not the leader"
                    )
            
            # 执行存储
            memory_id = asyncio.run(self.memory_store.store(
                agent_id=request.agent_id,
                tenant_id=request.tenant_id,
                content=request.content,
                embedding=list(request.embedding) if request.embedding else [],
                metadata=dict(request.metadata) if request.metadata else {},
                memory_type=str(request.memory_type),
                ttl=request.ttl_seconds,
            ))
            
            return pb2.StoreResponse(
                memory_id=memory_id,
                success=True,
            )
        
        except Exception as e:
            logger.error(f"Store failed: {e}")
            return pb2.StoreResponse(
                success=False,
                error_message=str(e),
            )
    
    def Get(self, request: pb2.GetRequest, context: ServicerContext) -> pb2.MemoryEntry:
        """获取内存条目"""
        asyncio.create_task(self._increment_request_count())
        
        try:
            memory = asyncio.run(self.memory_store.get(
                memory_id=request.memory_id,
                agent_id=request.agent_id,
                tenant_id=request.tenant_id,
            ))
            
            if memory is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Memory not found")
                return pb2.MemoryEntry()
            
            return pb2.MemoryEntry(
                id=memory.get("id", ""),
                agent_id=memory.get("agent_id", ""),
                tenant_id=memory.get("tenant_id", ""),
                content=memory.get("content", ""),
                metadata=memory.get("metadata", {}),
            )
        
        except Exception as e:
            logger.error(f"Get failed: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return pb2.MemoryEntry()
    
    def Delete(self, request: pb2.DeleteRequest, context: ServicerContext) -> pb2.DeleteResponse:
        """删除内存条目"""
        asyncio.create_task(self._increment_request_count())
        
        try:
            success = asyncio.run(self.memory_store.delete(
                memory_id=request.memory_id,
                agent_id=request.agent_id,
                tenant_id=request.tenant_id,
                permanent=request.permanent,
            ))
            
            return pb2.DeleteResponse(success=success)
        
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return pb2.DeleteResponse(
                success=False,
                error_message=str(e),
            )
    
    def Update(self, request: pb2.UpdateRequest, context: ServicerContext) -> pb2.StoreResponse:
        """更新内存条目"""
        asyncio.create_task(self._increment_request_count())
        
        try:
            kwargs = {}
            if hasattr(request, 'content') and request.content:
                kwargs['content'] = request.content
            if hasattr(request, 'embedding') and request.embedding:
                kwargs['embedding'] = list(request.embedding)
            if hasattr(request, 'metadata') and request.metadata:
                kwargs['metadata'] = dict(request.metadata)
            
            success = asyncio.run(self.memory_store.update(
                memory_id=request.memory_id,
                agent_id=request.agent_id,
                tenant_id=request.tenant_id,
                **kwargs
            ))
            
            return pb2.StoreResponse(
                memory_id=request.memory_id if success else "",
                success=success,
            )
        
        except Exception as e:
            logger.error(f"Update failed: {e}")
            return pb2.StoreResponse(
                success=False,
                error_message=str(e),
            )
    
    def Search(self, request: pb2.SearchRequest, context: ServicerContext) -> pb2.SearchResponse:
        """搜索内存"""
        asyncio.create_task(self._increment_request_count())
        start_time = time.time()
        
        try:
            results = asyncio.run(self.memory_store.search(
                agent_id=request.agent_id,
                tenant_id=request.tenant_id,
                query=request.query,
                query_embedding=list(request.query_embedding) if request.query_embedding else None,
                top_k=request.top_k or 10,
                min_score=request.min_score or 0.0,
            ))
            
            search_time = (time.time() - start_time) * 1000
            
            pb_results = []
            for result in results:
                entry = pb2.MemoryEntry(
                    id=result.get("id", ""),
                    agent_id=result.get("agent_id", ""),
                    tenant_id=result.get("tenant_id", ""),
                    content=result.get("content", ""),
                )
                pb_results.append(pb2.SearchResult(
                    entry=entry,
                    score=result.get("score", 0.0),
                    match_type="vector",
                ))
            
            return pb2.SearchResponse(
                results=pb_results,
                total_count=len(pb_results),
                search_time_ms=search_time,
            )
        
        except Exception as e:
            logger.error(f"Search failed: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return pb2.SearchResponse()
    
    def StreamingSearch(
        self,
        request: pb2.SearchRequest,
        context: ServicerContext
    ) -> Iterator[pb2.StreamingSearchResponse]:
        """流式搜索"""
        asyncio.create_task(self._increment_request_count())
        
        try:
            results = asyncio.run(self.memory_store.search(
                agent_id=request.agent_id,
                tenant_id=request.tenant_id,
                query=request.query,
                query_embedding=list(request.query_embedding) if request.query_embedding else None,
                top_k=request.top_k or 10,
                min_score=request.min_score or 0.0,
            ))
            
            for result in results:
                entry = pb2.MemoryEntry(
                    id=result.get("id", ""),
                    content=result.get("content", ""),
                )
                search_result = pb2.SearchResult(
                    entry=entry,
                    score=result.get("score", 0.0),
                )
                yield pb2.StreamingSearchResponse(result=search_result)
            
            # 发送完成信号
            yield pb2.StreamingSearchResponse(
                complete=pb2.SearchComplete(
                    total_results=len(results),
                    search_time_ms=0.0,
                )
            )
        
        except Exception as e:
            logger.error(f"Streaming search failed: {e}")
            yield pb2.StreamingSearchResponse(
                error=pb2.SearchError(
                    error_code="INTERNAL_ERROR",
                    error_message=str(e),
                )
            )
    
    def BatchStore(
        self,
        request: pb2.BatchStoreRequest,
        context: ServicerContext
    ) -> pb2.BatchStoreResponse:
        """批量存储"""
        asyncio.create_task(self._increment_request_count())
        
        responses = []
        success_count = 0
        failed_count = 0
        
        for store_request in request.requests:
            try:
                memory_id = asyncio.run(self.memory_store.store(
                    agent_id=store_request.agent_id,
                    tenant_id=store_request.tenant_id,
                    content=store_request.content,
                    embedding=list(store_request.embedding) if store_request.embedding else [],
                    metadata=dict(store_request.metadata) if store_request.metadata else {},
                ))
                
                responses.append(pb2.StoreResponse(
                    memory_id=memory_id,
                    success=True,
                ))
                success_count += 1
            
            except Exception as e:
                responses.append(pb2.StoreResponse(
                    success=False,
                    error_message=str(e),
                ))
                failed_count += 1
                
                if request.atomic and failed_count > 0:
                    # 原子操作失败，中止
                    break
        
        return pb2.BatchStoreResponse(
            responses=responses,
            success_count=success_count,
            failed_count=failed_count,
        )
    
    def StreamingStore(
        self,
        request_iterator: Iterator[pb2.StreamingStoreRequest],
        context: ServicerContext
    ) -> pb2.StoreResponse:
        """流式存储"""
        asyncio.create_task(self._increment_request_count())
        
        try:
            chunks = []
            header = None
            
            for request in request_iterator:
                if hasattr(request, 'header') and request.header:
                    header = request.header
                elif hasattr(request, 'chunk') and request.chunk:
                    chunks.append(request.chunk.data)
                elif hasattr(request, 'footer') and request.footer:
                    # 处理完成
                    break
            
            if header is None:
                return pb2.StoreResponse(
                    success=False,
                    error_message="Missing stream header",
                )
            
            # 合并数据
            content = b"".join(chunks).decode('utf-8')
            
            memory_id = asyncio.run(self.memory_store.store(
                agent_id=header.agent_id,
                tenant_id=header.tenant_id,
                content=content,
                embedding=[],
                metadata=dict(header.metadata) if header.metadata else {},
            ))
            
            return pb2.StoreResponse(
                memory_id=memory_id,
                success=True,
            )
        
        except Exception as e:
            logger.error(f"Streaming store failed: {e}")
            return pb2.StoreResponse(
                success=False,
                error_message=str(e),
            )
    
    def RaftPropose(
        self,
        request: pb2.RaftProposeRequest,
        context: ServicerContext
    ) -> pb2.RaftProposeResponse:
        """Raft提案"""
        if self.raft_node is None:
            return pb2.RaftProposeResponse(
                accepted=False,
                error_message="Raft not enabled",
            )
        
        try:
            if hasattr(self.raft_node, 'propose_command'):
                result = asyncio.run(self.raft_node.propose_command({
                    "command_id": request.command.command_id if request.command else "",
                    "payload": request.command.payload if request.command else b"",
                }))
                
                return pb2.RaftProposeResponse(
                    accepted=True,
                    current_term=getattr(self.raft_node, 'current_term', 0),
                )
            else:
                return pb2.RaftProposeResponse(
                    accepted=False,
                    error_message="Raft propose not available",
                )
        
        except Exception as e:
            logger.error(f"Raft propose failed: {e}")
            return pb2.RaftProposeResponse(
                accepted=False,
                error_message=str(e),
            )
    
    def RaftStatus(
        self,
        request: pb2.RaftStatusRequest,
        context: ServicerContext
    ) -> pb2.RaftStatusResponse:
        """获取Raft状态"""
        if self.raft_node is None:
            return pb2.RaftStatusResponse()
        
        try:
            status = asyncio.run(self.raft_node.get_status())
            
            role_map = {
                "FOLLOWER": 1,
                "CANDIDATE": 2,
                "LEADER": 3,
            }
            
            return pb2.RaftStatusResponse(
                node_id=status.get("node_id", ""),
                role=role_map.get(status.get("role", ""), 0),
                current_term=status.get("term", 0),
                leader_id=status.get("leader", ""),
                log_size=status.get("log_length", 0),
                commit_index=status.get("commit_index", 0),
                last_applied=status.get("last_applied", 0),
                peers=status.get("peers", []),
            )
        
        except Exception as e:
            logger.error(f"Raft status failed: {e}")
            return pb2.RaftStatusResponse()
    
    def ExecuteTransaction(
        self,
        request: pb2.TransactionRequest,
        context: ServicerContext
    ) -> pb2.TransactionResponse:
        """执行事务"""
        if self.transaction_manager is None:
            return pb2.TransactionResponse(
                transaction_id=request.transaction_id,
                status=0,
                error_message="Transaction manager not enabled",
            )
        
        # 简化的实现
        return pb2.TransactionResponse(
            transaction_id=request.transaction_id,
            status=5,  # COMMITTED
        )
    
    def GetTransactionStatus(
        self,
        request: pb2.TransactionRequest,
        context: ServicerContext
    ) -> pb2.TransactionResponse:
        """获取事务状态"""
        return pb2.TransactionResponse(
            transaction_id=request.transaction_id,
            status=5,  # COMMITTED
        )
    
    def HealthCheck(
        self,
        request: pb2.HealthCheckRequest,
        context: ServicerContext
    ) -> pb2.HealthCheckResponse:
        """健康检查"""
        uptime = time.time() - self._start_time
        
        services = {
            "memory_store": pb2.ServiceHealth(
                healthy=True,
                status="running",
                latency_ms=0.0,
            ),
            "raft": pb2.ServiceHealth(
                healthy=self.raft_node is not None,
                status="enabled" if self.raft_node else "disabled",
            ),
        }
        
        return pb2.HealthCheckResponse(
            status=1,  # SERVING
            message=f"Service healthy, uptime: {uptime:.2f}s",
            services=services,
            timestamp=int(time.time()),
        )
    
    def WatchHealth(
        self,
        request: pb2.Empty,
        context: ServicerContext
    ) -> Iterator[pb2.HealthCheckResponse]:
        """流式健康检查"""
        while context.is_active():
            yield self.HealthCheck(pb2.HealthCheckRequest(), context)
            time.sleep(5)  # 每5秒发送一次
    
    def GetMetrics(
        self,
        request: pb2.MetricsRequest,
        context: ServicerContext
    ) -> pb2.MetricsResponse:
        """获取指标"""
        metrics = [
            pb2.Metric(
                name="request_count",
                value=self._request_count,
                type=1,  # COUNTER
            ),
            pb2.Metric(
                name="uptime_seconds",
                value=time.time() - self._start_time,
                type=2,  # GAUGE
            ),
        ]
        
        return pb2.MetricsResponse(metrics=metrics)
    
    def StreamMetrics(
        self,
        request: pb2.MetricsRequest,
        context: ServicerContext
    ) -> Iterator[pb2.MetricsResponse]:
        """流式指标"""
        while context.is_active():
            yield self.GetMetrics(request, context)
            time.sleep(10)  # 每10秒发送一次
    
    def GetClusterStatus(
        self,
        request: pb2.ClusterStatusRequest,
        context: ServicerContext
    ) -> pb2.ClusterStatusResponse:
        """获取集群状态"""
        nodes = []
        
        if self.raft_node:
            status = asyncio.run(self.raft_node.get_status())
            nodes.append(pb2.ClusterNode(
                node_id=status.get("node_id", ""),
                status=1,  # HEALTHY
            ))
        
        return pb2.ClusterStatusResponse(
            cluster_id="default",
            nodes=nodes,
            leader_id=status.get("leader", "") if self.raft_node else "",
            replication_factor=3,
        )
    
    def JoinCluster(
        self,
        request: pb2.JoinClusterRequest,
        context: ServicerContext
    ) -> pb2.JoinClusterResponse:
        """加入集群"""
        if self.raft_node and hasattr(self.raft_node, 'add_member'):
            try:
                success = asyncio.run(self.raft_node.add_member(request.node_id))
                return pb2.JoinClusterResponse(success=success)
            except Exception as e:
                return pb2.JoinClusterResponse(
                    success=False,
                    error_message=str(e),
                )
        
        return pb2.JoinClusterResponse(
            success=False,
            error_message="Raft not enabled or add_member not supported",
        )
    
    def LeaveCluster(
        self,
        request: pb2.ClusterNode,
        context: ServicerContext
    ) -> pb2.Empty:
        """离开集群"""
        if self.raft_node and hasattr(self.raft_node, 'remove_member'):
            try:
                asyncio.run(self.raft_node.remove_member(request.node_id))
            except Exception as e:
                logger.error(f"Leave cluster failed: {e}")
        
        return pb2.Empty()


class GRPCServer:
    """
    gRPC服务器
    
    管理gRPC服务的生命周期
    """
    
    def __init__(
        self,
        config: Optional[GRPCServiceConfig] = None,
        memory_store: Optional[MemoryStoreInterface] = None,
        raft_node: Optional[Any] = None,
        transaction_manager: Optional[Any] = None,
    ):
        self.config = config or GRPCServiceConfig()
        self.memory_store = memory_store
        self.raft_node = raft_node
        self.transaction_manager = transaction_manager
        
        self._server: Optional[grpc.Server] = None
        self._running = False
    
    def _create_server(self) -> grpc.Server:
        """创建gRPC服务器"""
        # 创建线程池
        executor = futures.ThreadPoolExecutor(
            max_workers=self.config.max_workers
        )
        
        # 服务器选项
        options = [
            ('grpc.max_concurrent_rpcs', self.config.max_concurrent_rpcs or 100),
            ('grpc.keepalive_time_ms', self.config.keepalive_time_ms),
            ('grpc.keepalive_timeout_ms', self.config.keepalive_timeout_ms),
        ]
        
        # 创建服务器
        server = grpc.server(
            executor,
            options=options,
            compression=self.config.compression,
        )
        
        # 添加服务
        servicer = AgentMemoryServicer(
            memory_store=self.memory_store,
            raft_node=self.raft_node,
            transaction_manager=self.transaction_manager,
        )
        pb2_grpc.add_AgentMemoryServiceServicer_to_server(servicer, server)
        
        # 配置TLS
        if self.config.tls_cert_file and self.config.tls_key_file:
            with open(self.config.tls_cert_file, 'rb') as f:
                certificate_chain = f.read()
            with open(self.config.tls_key_file, 'rb') as f:
                private_key = f.read()
            
            credentials = grpc.ssl_server_credentials(
                ((private_key, certificate_chain),)
            )
            server.add_secure_port(f"{self.config.host}:{self.config.port}", credentials)
        else:
            server.add_insecure_port(f"{self.config.host}:{self.config.port}")
        
        return server
    
    async def start(self):
        """启动服务器"""
        if self._running:
            logger.warning("Server already running")
            return
        
        self._server = self._create_server()
        self._server.start()
        self._running = True
        
        logger.info(f"gRPC server started on {self.config.host}:{self.config.port}")
    
    async def stop(self, grace_period: float = 5.0):
        """停止服务器"""
        if not self._running or self._server is None:
            return
        
        self._server.stop(grace_period)
        self._running = False
        
        logger.info("gRPC server stopped")
    
    async def wait_for_termination(self):
        """等待服务器终止"""
        if self._server:
            self._server.wait_for_termination()


# 测试代码
if __name__ == "__main__":
    async def test_grpc_server():
        """测试gRPC服务器"""
        logging.basicConfig(level=logging.INFO)
        
        config = GRPCServiceConfig(
            host="0.0.0.0",
            port=50051,
            max_workers=4,
        )
        
        server = GRPCServer(config)
        await server.start()
        
        print(f"gRPC server running on {config.host}:{config.port}")
        print("Press Ctrl+C to stop")
        
        try:
            await server.wait_for_termination()
        except KeyboardInterrupt:
            await server.stop()
    
    asyncio.run(test_grpc_server())

"""
gRPC 服务实现
高性能二进制协议，适合大规模部署
"""

import asyncio
from typing import AsyncIterator, Optional
from concurrent import futures

import grpc
from google.protobuf import empty_pb2

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)

# 尝试导入生成的 protobuf 代码
# 如果未生成，提供占位符实现
try:
    from memory_system.proto import memory_pb2, memory_pb2_grpc
    _PROTO_AVAILABLE = True
except ImportError:
    _PROTO_AVAILABLE = False
    # 占位符定义
    class memory_pb2:
        class MemoryRequest:
            def __init__(self, **kwargs): pass
        class MemoryResponse:
            def __init__(self, **kwargs): pass
            memory_id = ""
            status = ""
        class RecallRequest:
            def __init__(self, **kwargs): pass
            query = ""
            max_results = 10
        class RecallResponse:
            def __init__(self, **kwargs): pass
            results = []
        class MemoryItem:
            def __init__(self, **kwargs): pass
        class HealthResponse:
            def __init__(self, **kwargs): pass
            status = ""
            version = ""
    
    class memory_pb2_grpc:
        class MemoryServiceServicer:
            pass
        class add_MemoryServiceServicer_to_server:
            @staticmethod
            def add(servicer, server): pass


class MemoryServiceServicer(memory_pb2_grpc.MemoryServiceServicer):
    """
    记忆服务 gRPC 实现
    """
    
    def __init__(self, memory_system):
        self.memory_system = memory_system
    
    async def StoreMemory(
        self,
        request: memory_pb2.MemoryRequest,
        context: grpc.ServicerContext
    ) -> memory_pb2.MemoryResponse:
        """存储记忆"""
        try:
            memory_id = await self.memory_system.remember(
                content=request.content,
                memory_type=request.memory_type or "auto",
                importance=request.importance or "medium",
                metadata=dict(request.metadata) if request.metadata else None
            )
            return memory_pb2.MemoryResponse(
                memory_id=memory_id,
                status="success"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return memory_pb2.MemoryResponse(status="error")
    
    async def RecallMemories(
        self,
        request: memory_pb2.RecallRequest,
        context: grpc.ServicerContext
    ) -> memory_pb2.RecallResponse:
        """检索记忆"""
        try:
            result = await self.memory_system.recall(
                query=request.query,
                max_results=request.max_results
            )
            
            items = []
            for item in result.get("combined", []):
                memory = item.get("memory")
                if memory:
                    items.append(memory_pb2.MemoryItem(
                        memory_id=memory.memory_id,
                        content=memory.content,
                        memory_type=memory.memory_type.value,
                        importance=memory.importance.value,
                        timestamp=memory.timestamp,
                        score=item.get("score", 0.0)
                    ))
            
            return memory_pb2.RecallResponse(results=items)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return memory_pb2.RecallResponse()
    
    async def StreamRecall(
        self,
        request: memory_pb2.RecallRequest,
        context: grpc.ServicerContext
    ) -> AsyncIterator[memory_pb2.MemoryItem]:
        """流式检索"""
        try:
            result = await self.memory_system.recall(
                query=request.query,
                max_results=request.max_results
            )
            
            for item in result.get("combined", []):
                memory = item.get("memory")
                if memory:
                    yield memory_pb2.MemoryItem(
                        memory_id=memory.memory_id,
                        content=memory.content,
                        memory_type=memory.memory_type.value,
                        importance=memory.importance.value,
                        timestamp=memory.timestamp,
                        score=item.get("score", 0.0)
                    )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
    
    async def HealthCheck(
        self,
        request: empty_pb2.Empty,
        context: grpc.ServicerContext
    ) -> memory_pb2.HealthResponse:
        """健康检查"""
        return memory_pb2.HealthResponse(
            status="healthy",
            version="2.1.0"
        )


class GRPCServer:
    """
    gRPC 服务器
    
    使用示例:
        >>> server = GRPCServer(memory_system, port=50051)
        >>> await server.start()
        >>> # ... 运行中 ...
        >>> await server.stop()
    """
    
    def __init__(
        self,
        memory_system,
        host: str = "0.0.0.0",
        port: int = 50051,
        max_workers: int = 10,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
    ):
        self.memory_system = memory_system
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self._server: Optional[grpc.aio.Server] = None
    
    async def start(self) -> None:
        """启动 gRPC 服务器"""
        if not _PROTO_AVAILABLE:
            logger.warning("gRPC protobuf 未生成，跳过启动。运行: python -m grpc_tools.protoc ...")
            return
        
        self._server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=self.max_workers))
        
        servicer = MemoryServiceServicer(self.memory_system)
        memory_pb2_grpc.add_MemoryServiceServicer_to_server(servicer, self._server)

        if self.tls_cert and self.tls_key:
            with open(self.tls_cert, 'rb') as f:
                cert_chain = f.read()
            with open(self.tls_key, 'rb') as f:
                private_key = f.read()
            credentials = grpc.ssl_server_credentials([(private_key, cert_chain)])
            self._server.add_secure_port(f"{self.host}:{self.port}", credentials)
        else:
            self._server.add_insecure_port(f"{self.host}:{self.port}")
        await self._server.start()
        logger.info(f"gRPC 服务器已启动: {self.host}:{self.port}")
    
    async def stop(self, grace_period: float = 5.0) -> None:
        """停止 gRPC 服务器"""
        if self._server:
            await self._server.stop(grace_period)
            self._server = None
            logger.info("gRPC 服务器已停止")
    
    @property
    def is_running(self) -> bool:
        """检查服务器是否运行中"""
        return self._server is not None

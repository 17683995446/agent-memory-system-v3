"""
gRPC 服务测试
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

# 测试占位符模式（protobuf 未生成时）
from memory_system.grpc_service_server import (
    MemoryServiceServicer,
    GRPCServer,
    _PROTO_AVAILABLE,
)


@pytest.fixture
def mock_memory_system():
    """创建模拟的记忆系统"""
    ms = MagicMock()
    ms.remember = AsyncMock(return_value="test-memory-id-123")
    ms.recall = AsyncMock(return_value={
        "combined": [
            {
                "memory": MagicMock(
                    memory_id="mem-1",
                    content="Test content",
                    memory_type=MagicMock(value="episodic"),
                    importance=MagicMock(value=3),
                    timestamp=1234567890.0,
                ),
                "score": 0.95,
            }
        ]
    })
    return ms


@pytest.fixture
def mock_context():
    """创建模拟的 gRPC 上下文"""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


class TestMemoryServiceServicer:
    """测试 MemoryServiceServicer"""

    @pytest.mark.asyncio
    async def test_store_memory_success(self, mock_memory_system, mock_context):
        """测试成功存储记忆"""
        servicer = MemoryServiceServicer(mock_memory_system)
        
        # 创建模拟请求
        request = MagicMock()
        request.content = "Test memory content"
        request.memory_type = "episodic"
        request.importance = "high"
        request.metadata = {"key": "value"}
        
        if _PROTO_AVAILABLE:
            response = await servicer.StoreMemory(request, mock_context)
            assert response.memory_id == "test-memory-id-123"
            assert response.status == "success"
        else:
            # 占位符模式下跳过实际测试
            pytest.skip("Protobuf 未生成，跳过测试")
    
    @pytest.mark.asyncio
    async def test_store_memory_error(self, mock_memory_system, mock_context):
        """测试存储记忆失败"""
        mock_memory_system.remember = AsyncMock(side_effect=Exception("Storage failed"))
        servicer = MemoryServiceServicer(mock_memory_system)
        
        request = MagicMock()
        request.content = "Test content"
        request.memory_type = None
        request.importance = None
        request.metadata = None
        
        if _PROTO_AVAILABLE:
            response = await servicer.StoreMemory(request, mock_context)
            assert response.status == "error"
            mock_context.set_code.assert_called_once()
        else:
            pytest.skip("Protobuf 未生成，跳过测试")
    
    @pytest.mark.asyncio
    async def test_recall_memories_success(self, mock_memory_system, mock_context):
        """测试成功检索记忆"""
        servicer = MemoryServiceServicer(mock_memory_system)
        
        request = MagicMock()
        request.query = "test query"
        request.max_results = 10
        
        if _PROTO_AVAILABLE:
            response = await servicer.RecallMemories(request, mock_context)
            assert len(response.results) == 1
        else:
            pytest.skip("Protobuf 未生成，跳过测试")
    
    @pytest.mark.asyncio
    async def test_recall_memories_error(self, mock_memory_system, mock_context):
        """测试检索记忆失败"""
        mock_memory_system.recall = AsyncMock(side_effect=Exception("Recall failed"))
        servicer = MemoryServiceServicer(mock_memory_system)
        
        request = MagicMock()
        request.query = "test"
        request.max_results = 10
        
        if _PROTO_AVAILABLE:
            response = await servicer.RecallMemories(request, mock_context)
            mock_context.set_code.assert_called_once()
        else:
            pytest.skip("Protobuf 未生成，跳过测试")
    
    @pytest.mark.asyncio
    async def test_stream_recall(self, mock_memory_system, mock_context):
        """测试流式检索"""
        servicer = MemoryServiceServicer(mock_memory_system)
        
        request = MagicMock()
        request.query = "test query"
        request.max_results = 10
        
        if _PROTO_AVAILABLE:
            results = []
            async for item in servicer.StreamRecall(request, mock_context):
                results.append(item)
            assert len(results) == 1
        else:
            pytest.skip("Protobuf 未生成，跳过测试")
    
    @pytest.mark.asyncio
    async def test_health_check(self, mock_memory_system, mock_context):
        """测试健康检查"""
        servicer = MemoryServiceServicer(mock_memory_system)
        
        from google.protobuf import empty_pb2
        request = empty_pb2.Empty()
        
        if _PROTO_AVAILABLE:
            response = await servicer.HealthCheck(request, mock_context)
            assert response.status == "healthy"
            assert response.version == "2.1.0"
        else:
            pytest.skip("Protobuf 未生成，跳过测试")


class TestGRPCServer:
    """测试 GRPCServer"""

    @pytest.mark.asyncio
    async def test_init(self, mock_memory_system):
        """测试服务器初始化"""
        server = GRPCServer(
            memory_system=mock_memory_system,
            host="127.0.0.1",
            port=50052,
            max_workers=5
        )
        
        assert server.memory_system == mock_memory_system
        assert server.host == "127.0.0.1"
        assert server.port == 50052
        assert server.max_workers == 5
        assert server._server is None
        assert not server.is_running
    
    @pytest.mark.asyncio
    async def test_start_stop_placeholder_mode(self, mock_memory_system):
        """测试占位符模式下的启动（protobuf 未生成）"""
        if _PROTO_AVAILABLE:
            pytest.skip("Protobuf 已生成，跳过占位符测试")
        
        server = GRPCServer(memory_system=mock_memory_system)
        
        # 占位符模式下不应抛出异常，但也不会真正启动
        await server.start()
        assert not server.is_running
        
        # 停止也不应抛出异常
        await server.stop()
    
    @pytest.mark.asyncio
    async def test_is_running_property(self, mock_memory_system):
        """测试 is_running 属性"""
        server = GRPCServer(memory_system=mock_memory_system)
        
        # 初始状态
        assert not server.is_running
        
        # 模拟服务器已启动
        server._server = MagicMock()
        assert server.is_running


class TestPlaceholderMode:
    """测试占位符模式（protobuf 未生成时）"""

    def test_placeholder_classes_exist(self):
        """测试占位符类存在"""
        if _PROTO_AVAILABLE:
            pytest.skip("Protobuf 已生成，跳过占位符测试")
        
        # 确保占位符定义存在
        from memory_system.grpc_service_server import memory_pb2, memory_pb2_grpc
        
        # 测试占位符类可以实例化
        req = memory_pb2.MemoryRequest()
        resp = memory_pb2.MemoryResponse()
        item = memory_pb2.MemoryItem()
        
        assert hasattr(memory_pb2, 'MemoryRequest')
        assert hasattr(memory_pb2, 'MemoryResponse')
        assert hasattr(memory_pb2, 'RecallRequest')
        assert hasattr(memory_pb2, 'RecallResponse')
        assert hasattr(memory_pb2, 'MemoryItem')
        assert hasattr(memory_pb2, 'HealthResponse')
        
        assert hasattr(memory_pb2_grpc, 'MemoryServiceServicer')
        assert hasattr(memory_pb2_grpc, 'add_MemoryServiceServicer_to_server')
    
    def test_proto_available_flag(self):
        """测试 _PROTO_AVAILABLE 标志"""
        # 该标志应该存在且为布尔值
        assert isinstance(_PROTO_AVAILABLE, bool)

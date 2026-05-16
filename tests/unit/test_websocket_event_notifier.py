"""
WebSocket 事件通知器测试
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from memory_system.websocket_event_notifier import (
    WebSocketEventNotifier,
    EventType,
    _FASTAPI_AVAILABLE,
)


@pytest.fixture
def mock_websocket():
    """创建模拟的 WebSocket 连接"""
    ws = MagicMock()
    ws.accept = AsyncMock()
    ws.send_json = AsyncMock()
    ws.close = AsyncMock()
    ws.receive_text = AsyncMock(return_value="{}")
    return ws


@pytest.fixture
def notifier():
    """创建通知器实例"""
    return WebSocketEventNotifier()


@pytest.mark.asyncio
class TestWebSocketEventNotifier:
    """测试 WebSocketEventNotifier"""

    async def test_connect(self, notifier, mock_websocket):
        """测试 WebSocket 连接"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        await notifier.connect(mock_websocket, tenant_id="test-tenant")
        
        mock_websocket.accept.assert_called_once()
        mock_websocket.send_json.assert_called_once()
        
        # 验证连接已记录
        assert notifier.get_connection_count() == 1
        assert notifier.get_connection_count("test-tenant") == 1
    
    async def test_connect_without_fastapi(self, notifier, mock_websocket):
        """测试无 FastAPI 时的连接行为"""
        with patch.object(notifier.__class__, '__module__', 'test_module'):
            # 模拟 FastAPI 不可用的情况
            with patch('memory_system.websocket_event_notifier._FASTAPI_AVAILABLE', False):
                notifier2 = WebSocketEventNotifier()
                await notifier2.connect(mock_websocket)
                # 不应抛出异常，但也不会真正连接
                assert notifier2.get_connection_count() == 0
    
    async def test_disconnect(self, notifier, mock_websocket):
        """测试 WebSocket 断开连接"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        await notifier.connect(mock_websocket)
        assert notifier.get_connection_count() == 1
        
        await notifier.disconnect(mock_websocket)
        assert notifier.get_connection_count() == 0
    
    async def test_broadcast(self, notifier, mock_websocket):
        """测试事件广播"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        await notifier.connect(mock_websocket)
        
        sent_count = await notifier.broadcast(
            EventType.MEMORY_ADDED,
            {"memory_id": "test-123"}
        )
        
        assert sent_count == 1
        mock_websocket.send_json.assert_called()
    
    async def test_broadcast_with_tenant(self, notifier):
        """测试按租户广播"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        ws1 = MagicMock()
        ws1.accept = AsyncMock()
        ws1.send_json = AsyncMock()
        
        ws2 = MagicMock()
        ws2.accept = AsyncMock()
        ws2.send_json = AsyncMock()
        
        await notifier.connect(ws1, tenant_id="tenant-a")
        await notifier.connect(ws2, tenant_id="tenant-b")
        
        # 重置 mock，清除欢迎消息的调用记录
        ws1.send_json.reset_mock()
        ws2.send_json.reset_mock()
        
        # 只广播到 tenant-a
        sent_count = await notifier.broadcast(
            EventType.MEMORY_ADDED,
            {"memory_id": "test"},
            tenant_id="tenant-a"
        )
        
        assert sent_count == 1
        ws1.send_json.assert_called_once()
        ws2.send_json.assert_not_called()
    
    async def test_broadcast_with_subscription(self, notifier):
        """测试按订阅类型广播"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock()
        
        # 只订阅 MEMORY_ADDED 事件
        await notifier.connect(
            ws,
            tenant_id="default",
            subscriptions=[EventType.MEMORY_ADDED]
        )
        
        # 发送订阅的事件
        sent_count = await notifier.broadcast(
            EventType.MEMORY_ADDED,
            {"memory_id": "test"}
        )
        assert sent_count == 1
        
        # 发送未订阅的事件
        ws.send_json.reset_mock()
        sent_count = await notifier.broadcast(
            EventType.MEMORY_DELETED,
            {"memory_id": "test"}
        )
        assert sent_count == 0
        ws.send_json.assert_not_called()
    
    async def test_send_to_tenant(self, notifier, mock_websocket):
        """测试发送到指定租户"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        await notifier.connect(mock_websocket, tenant_id="my-tenant")
        
        sent_count = await notifier.send_to_tenant(
            "my-tenant",
            EventType.MEMORY_RECALLED,
            {"query": "test"}
        )
        
        assert sent_count == 1
    
    async def test_broadcast_without_fastapi(self, notifier):
        """测试无 FastAPI 时的广播行为"""
        with patch('memory_system.websocket_event_notifier._FASTAPI_AVAILABLE', False):
            notifier2 = WebSocketEventNotifier()
            sent_count = await notifier2.broadcast(
                EventType.MEMORY_ADDED,
                {"memory_id": "test"}
            )
            assert sent_count == 0
    
    async def test_close_all(self, notifier, mock_websocket):
        """测试关闭所有连接"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        await notifier.connect(mock_websocket)
        assert notifier.get_connection_count() == 1
        
        await notifier.close_all()
        
        assert notifier.get_connection_count() == 0
        mock_websocket.close.assert_called_once()
    
    async def test_get_connection_count(self, notifier):
        """测试获取连接数量"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        # 初始状态
        assert notifier.get_connection_count() == 0
        assert notifier.get_connection_count("tenant-a") == 0
        
        ws1 = MagicMock()
        ws1.accept = AsyncMock()
        ws1.send_json = AsyncMock()
        
        ws2 = MagicMock()
        ws2.accept = AsyncMock()
        ws2.send_json = AsyncMock()
        
        await notifier.connect(ws1, tenant_id="tenant-a")
        await notifier.connect(ws2, tenant_id="tenant-b")
        
        assert notifier.get_connection_count() == 2
        assert notifier.get_connection_count("tenant-a") == 1
        assert notifier.get_connection_count("tenant-b") == 1
        assert notifier.get_connection_count("tenant-c") == 0


class TestEventType:
    """测试 EventType 常量"""

    def test_event_types(self):
        """测试事件类型常量值"""
        assert EventType.MEMORY_ADDED == "memory_added"
        assert EventType.MEMORY_RECALLED == "memory_recalled"
        assert EventType.MEMORY_DELETED == "memory_deleted"
        assert EventType.MEMORY_CONSOLIDATED == "memory_consolidated"
        assert EventType.SHORT_TERM_CLEARED == "short_term_cleared"


@pytest.mark.asyncio
class TestWebSocketIntegration:
    """测试 WebSocket 集成场景"""

    async def test_multiple_clients(self, notifier):
        """测试多客户端场景"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        clients = []
        for i in range(5):
            ws = MagicMock()
            ws.accept = AsyncMock()
            ws.send_json = AsyncMock()
            ws.close = AsyncMock()
            await notifier.connect(ws, tenant_id=f"tenant-{i % 2}")
            clients.append(ws)
        
        assert notifier.get_connection_count() == 5
        assert notifier.get_connection_count("tenant-0") == 3
        assert notifier.get_connection_count("tenant-1") == 2
        
        # 广播到所有客户端
        sent_count = await notifier.broadcast(
            EventType.MEMORY_ADDED,
            {"memory_id": "test"}
        )
        assert sent_count == 5
        
        # 关闭所有连接
        await notifier.close_all()
        assert notifier.get_connection_count() == 0
    
    async def test_client_disconnect_during_broadcast(self, notifier):
        """测试广播期间客户端断开"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock(side_effect=Exception("Connection closed"))
        ws.close = AsyncMock()
        
        await notifier.connect(ws)
        assert notifier.get_connection_count() == 1
        
        # 广播时客户端异常断开
        sent_count = await notifier.broadcast(
            EventType.MEMORY_ADDED,
            {"memory_id": "test"}
        )
        
        # 发送失败，客户端被清理
        assert sent_count == 0
        assert notifier.get_connection_count() == 0
    
    async def test_welcome_message_format(self, notifier, mock_websocket):
        """测试欢迎消息格式"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        await notifier.connect(
            mock_websocket,
            tenant_id="test-tenant",
            subscriptions=[EventType.MEMORY_ADDED, EventType.MEMORY_DELETED]
        )
        
        # 验证欢迎消息
        call_args = mock_websocket.send_json.call_args
        message = call_args[0][0]
        
        assert message["type"] == "connected"
        assert message["tenant_id"] == "test-tenant"
        assert "timestamp" in message
        assert set(message["subscriptions"]) == {EventType.MEMORY_ADDED, EventType.MEMORY_DELETED}
    
    async def test_broadcast_message_format(self, notifier, mock_websocket):
        """测试广播消息格式"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI 未安装，跳过测试")
        
        await notifier.connect(mock_websocket)
        
        await notifier.broadcast(
            EventType.MEMORY_ADDED,
            {"memory_id": "mem-123", "content": "Test"}
        )
        
        # 验证广播消息格式
        calls = mock_websocket.send_json.call_args_list
        # 第一个是欢迎消息，第二个是广播消息
        broadcast_call = calls[1]
        message = broadcast_call[0][0]
        
        assert message["type"] == EventType.MEMORY_ADDED
        assert message["data"] == {"memory_id": "mem-123", "content": "Test"}
        assert "timestamp" in message

"""
WebSocket 事件通知器
实时推送记忆变更事件
"""

import asyncio
import json
from typing import Dict, List, Set, Callable, Any
from datetime import datetime, timezone

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)

try:
    from fastapi import WebSocket, WebSocketDisconnect
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    class WebSocket:
        pass
    class WebSocketDisconnect(Exception):
        pass


class EventType:
    """事件类型常量"""
    MEMORY_ADDED = "memory_added"
    MEMORY_RECALLED = "memory_recalled"
    MEMORY_DELETED = "memory_deleted"
    MEMORY_CONSOLIDATED = "memory_consolidated"
    SHORT_TERM_CLEARED = "short_term_cleared"


class WebSocketEventNotifier:
    """
    WebSocket 事件通知器
    
    功能:
        - 管理 WebSocket 连接
        - 广播记忆变更事件
        - 支持按事件类型订阅
        - 支持按 tenant 隔离
    
    使用示例:
        >>> notifier = WebSocketEventNotifier()
        >>> 
        >>> # 在 FastAPI 路由中
        >>> @app.websocket("/ws/events")
        >>> async def websocket_endpoint(websocket: WebSocket):
        ...     await notifier.connect(websocket)
        ...     try:
        ...         while True:
        ...             data = await websocket.receive_text()
        ...     except WebSocketDisconnect:
        ...         await notifier.disconnect(websocket)
        >>> 
        >>> # 发送事件
        >>> await notifier.broadcast(EventType.MEMORY_ADDED, {"memory_id": "xxx"})
    """
    
    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._subscriptions: Dict[WebSocket, Set[str]] = {}  # conn -> event_types
        self._tenants: Dict[WebSocket, str] = {}  # conn -> tenant_id
        self._lock = asyncio.Lock()
    
    async def connect(
        self,
        websocket: WebSocket,
        tenant_id: str = "default",
        subscriptions: List[str] = None
    ) -> None:
        """接受 WebSocket 连接"""
        if not _FASTAPI_AVAILABLE:
            logger.warning("FastAPI 未安装，WebSocket 不可用")
            return
        
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
            self._subscriptions[websocket] = set(subscriptions or [])
            self._tenants[websocket] = tenant_id
        
        logger.debug(f"WebSocket 连接已建立: tenant={tenant_id}")
        
        # 发送欢迎消息
        await self._send_to(websocket, {
            "type": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tenant_id": tenant_id,
            "subscriptions": list(self._subscriptions[websocket])
        })
    
    async def disconnect(self, websocket: WebSocket) -> None:
        """断开 WebSocket 连接"""
        async with self._lock:
            self._connections.discard(websocket)
            self._subscriptions.pop(websocket, None)
            self._tenants.pop(websocket, None)
        
        logger.debug("WebSocket 连接已断开")
    
    async def broadcast(
        self,
        event_type: str,
        data: Dict[str, Any],
        tenant_id: str = None
    ) -> int:
        """
        广播事件到所有订阅的客户端
        
        Returns:
            成功发送的客户端数量
        """
        if not _FASTAPI_AVAILABLE:
            return 0
        
        message = {
            "type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        sent_count = 0
        disconnected = []
        
        async with self._lock:
            connections = list(self._connections)
            tenants = dict(self._tenants)
            subscriptions = {k: set(v) for k, v in self._subscriptions.items()}

        for conn in connections:
            # 检查租户隔离
            if tenant_id and tenants.get(conn) != tenant_id:
                continue

            # 检查订阅
            subs = subscriptions.get(conn, set())
            if subs and event_type not in subs:
                continue
            
            try:
                await conn.send_json(message)
                sent_count += 1
            except Exception:
                disconnected.append(conn)
        
        # 清理断开的连接
        for conn in disconnected:
            await self.disconnect(conn)
        
        return sent_count
    
    async def send_to_tenant(
        self,
        tenant_id: str,
        event_type: str,
        data: Dict[str, Any]
    ) -> int:
        """发送事件到指定租户的所有客户端"""
        return await self.broadcast(event_type, data, tenant_id=tenant_id)
    
    async def _send_to(self, websocket: WebSocket, message: Dict[str, Any]) -> None:
        """发送消息到单个客户端"""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.warning(f"发送 WebSocket 消息失败: {e}")
    
    def get_connection_count(self, tenant_id: str = None) -> int:
        """获取连接数量"""
        if tenant_id:
            return sum(1 for t in self._tenants.values() if t == tenant_id)
        return len(self._connections)
    
    async def close_all(self) -> None:
        """关闭所有连接"""
        async with self._lock:
            connections = list(self._connections)
            self._connections.clear()
            self._subscriptions.clear()
            self._tenants.clear()
        
        for conn in connections:
            try:
                await conn.close()
            except Exception:
                pass

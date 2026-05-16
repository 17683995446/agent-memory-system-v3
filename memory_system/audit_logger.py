"""安全审计日志"""

import json
import time
import hashlib
import threading
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict, field
from enum import Enum
from datetime import datetime
from pathlib import Path


class AuditEventType(Enum):
    """审计事件类型"""
    # 记忆操作
    MEMORY_CREATE = "memory_create"
    MEMORY_READ = "memory_read"
    MEMORY_UPDATE = "memory_update"
    MEMORY_DELETE = "memory_delete"
    MEMORY_BATCH_CREATE = "memory_batch_create"
    MEMORY_BATCH_DELETE = "memory_batch_delete"
    MEMORY_EXPORT = "memory_export"
    MEMORY_IMPORT = "memory_import"

    # 认证授权
    AUTH_LOGIN = "auth_login"
    AUTH_LOGOUT = "auth_logout"
    AUTH_FAILURE = "auth_failure"
    AUTH_TOKEN_REFRESH = "auth_token_refresh"
    PERMISSION_DENIED = "permission_denied"

    # 配置变更
    CONFIG_CHANGE = "config_change"
    CONFIG_RELOAD = "config_reload"

    # 系统操作
    SYSTEM_STARTUP = "system_startup"
    SYSTEM_SHUTDOWN = "system_shutdown"
    BACKUP_CREATE = "backup_create"
    BACKUP_RESTORE = "backup_restore"

    # 安全事件
    SECURITY_VIOLATION = "security_violation"
    RATE_LIMIT_HIT = "rate_limit_hit"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"


class AuditEventSeverity(Enum):
    """审计事件严重级别"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """审计事件"""
    event_type: AuditEventType
    user_id: str
    timestamp: float
    resource_type: str
    resource_id: Optional[str]
    action: str
    status: str  # success/failure
    details: Dict[str, Any] = field(default_factory=dict)
    severity: AuditEventSeverity = AuditEventSeverity.INFO
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    tenant_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        data['event_type'] = self.event_type.value
        data['severity'] = self.severity.value
        data['timestamp_iso'] = datetime.fromtimestamp(self.timestamp).isoformat()
        return data

    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)

    def get_hash(self) -> str:
        """计算事件哈希(用于防篡改验证)"""
        data = self.to_json()
        return hashlib.sha256(data.encode()).hexdigest()[:16]


class AuditLogger:
    """安全审计日志记录器"""

    def __init__(self, log_file: str = "audit.log", rotation_size: int = 100 * 1024 * 1024):
        """
        Args:
            log_file: 日志文件路径
            rotation_size: 日志轮转大小(字节)
        """
        self.log_file = Path(log_file)
        self.rotation_size = rotation_size
        self._lock = threading.RLock()
        self._ensure_directory()

    def _ensure_directory(self) -> None:
        """确保日志目录存在"""
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def _rotate_if_needed(self) -> None:
        """检查并执行日志轮转"""
        if not self.log_file.exists():
            return

        if self.log_file.stat().st_size >= self.rotation_size:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            rotated_file = self.log_file.parent / f"{self.log_file.stem}_{timestamp}{self.log_file.suffix}"
            self.log_file.rename(rotated_file)

    def log(self, event: AuditEvent) -> None:
        """记录审计事件

        Args:
            event: 审计事件
        """
        with self._lock:
            self._rotate_if_needed()

            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(event.to_json() + '\n')

    def log_memory_access(
        self,
        user_id: str,
        memory_id: Optional[str],
        action: str,
        status: str = "success",
        tenant_id: Optional[str] = None,
        **details
    ) -> None:
        """记录记忆访问

        Args:
            user_id: 用户ID
            memory_id: 记忆ID
            action: 操作类型 (create/read/update/delete)
            status: 操作状态
            tenant_id: 租户ID
            **details: 额外详情
        """
        event_type_map = {
            "create": AuditEventType.MEMORY_CREATE,
            "read": AuditEventType.MEMORY_READ,
            "update": AuditEventType.MEMORY_UPDATE,
            "delete": AuditEventType.MEMORY_DELETE,
        }

        event = AuditEvent(
            event_type=event_type_map.get(action, AuditEventType.MEMORY_READ),
            user_id=user_id,
            timestamp=time.time(),
            resource_type="memory",
            resource_id=memory_id,
            action=action,
            status=status,
            tenant_id=tenant_id,
            details=details
        )
        self.log(event)

    def log_auth(
        self,
        user_id: str,
        action: str,
        status: str,
        ip_address: Optional[str] = None,
        **details
    ) -> None:
        """记录认证事件

        Args:
            user_id: 用户ID
            action: 操作类型 (login/logout/failure)
            status: 操作状态
            ip_address: IP地址
            **details: 额外详情
        """
        event_type_map = {
            "login": AuditEventType.AUTH_LOGIN,
            "logout": AuditEventType.AUTH_LOGOUT,
            "failure": AuditEventType.AUTH_FAILURE,
            "token_refresh": AuditEventType.AUTH_TOKEN_REFRESH,
        }

        severity = AuditEventSeverity.WARNING if action == "failure" else AuditEventSeverity.INFO

        event = AuditEvent(
            event_type=event_type_map.get(action, AuditEventType.AUTH_FAILURE),
            user_id=user_id,
            timestamp=time.time(),
            resource_type="auth",
            resource_id=None,
            action=action,
            status=status,
            severity=severity,
            ip_address=ip_address,
            details=details
        )
        self.log(event)

    def log_config_change(
        self,
        user_id: str,
        config_path: str,
        old_value: Any,
        new_value: Any,
        **details
    ) -> None:
        """记录配置变更

        Args:
            user_id: 用户ID
            config_path: 配置路径
            old_value: 旧值
            new_value: 新值
            **details: 额外详情
        """
        event = AuditEvent(
            event_type=AuditEventType.CONFIG_CHANGE,
            user_id=user_id,
            timestamp=time.time(),
            resource_type="config",
            resource_id=config_path,
            action="change",
            status="success",
            details={
                "old_value": old_value,
                "new_value": new_value,
                **details
            }
        )
        self.log(event)

    def log_security_event(
        self,
        event_type: AuditEventType,
        user_id: str,
        description: str,
        severity: AuditEventSeverity = AuditEventSeverity.WARNING,
        **details
    ) -> None:
        """记录安全事件

        Args:
            event_type: 事件类型
            user_id: 用户ID
            description: 事件描述
            severity: 严重级别
            **details: 额外详情
        """
        event = AuditEvent(
            event_type=event_type,
            user_id=user_id,
            timestamp=time.time(),
            resource_type="security",
            resource_id=None,
            action="detect",
            status="detected",
            severity=severity,
            details={
                "description": description,
                **details
            }
        )
        self.log(event)

    def query(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        user_id: Optional[str] = None,
        event_type: Optional[AuditEventType] = None,
        resource_type: Optional[str] = None,
        limit: int = 100
    ) -> List[AuditEvent]:
        """查询审计日志

        Args:
            start_time: 开始时间戳
            end_time: 结束时间戳
            user_id: 用户ID过滤
            event_type: 事件类型过滤
            resource_type: 资源类型过滤
            limit: 返回数量限制

        Returns:
            List[AuditEvent]: 审计事件列表
        """
        events = []

        if not self.log_file.exists():
            return events

        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in reversed(f.readlines()):
                if len(events) >= limit:
                    break

                try:
                    data = json.loads(line.strip())
                    timestamp = data.get('timestamp', 0)

                    # 时间过滤
                    if start_time and timestamp < start_time:
                        continue
                    if end_time and timestamp > end_time:
                        continue

                    # 用户过滤
                    if user_id and data.get('user_id') != user_id:
                        continue

                    # 事件类型过滤
                    if event_type and data.get('event_type') != event_type.value:
                        continue

                    # 资源类型过滤
                    if resource_type and data.get('resource_type') != resource_type:
                        continue

                    events.append(self._dict_to_event(data))
                except json.JSONDecodeError:
                    continue

        return events

    def _dict_to_event(self, data: Dict[str, Any]) -> AuditEvent:
        """将字典转换为AuditEvent"""
        return AuditEvent(
            event_type=AuditEventType(data.get('event_type')),
            user_id=data.get('user_id', ''),
            timestamp=data.get('timestamp', 0),
            resource_type=data.get('resource_type', ''),
            resource_id=data.get('resource_id'),
            action=data.get('action', ''),
            status=data.get('status', ''),
            details=data.get('details', {}),
            severity=AuditEventSeverity(data.get('severity', 'info')),
            ip_address=data.get('ip_address'),
            user_agent=data.get('user_agent'),
            session_id=data.get('session_id'),
            request_id=data.get('request_id'),
            tenant_id=data.get('tenant_id'),
            metadata=data.get('metadata', {})
        )

    def get_statistics(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None
    ) -> Dict[str, Any]:
        """获取审计日志统计

        Args:
            start_time: 开始时间戳
            end_time: 结束时间戳

        Returns:
            Dict: 统计信息
        """
        events = self.query(
            start_time=start_time,
            end_time=end_time,
            limit=10000
        )

        stats = {
            "total_events": len(events),
            "by_type": {},
            "by_user": {},
            "by_status": {"success": 0, "failure": 0},
            "by_severity": {},
        }

        for event in events:
            # 按类型统计
            event_type = event.event_type.value
            stats["by_type"][event_type] = stats["by_type"].get(event_type, 0) + 1

            # 按用户统计
            stats["by_user"][event.user_id] = stats["by_user"].get(event.user_id, 0) + 1

            # 按状态统计
            if event.status in stats["by_status"]:
                stats["by_status"][event.status] += 1

            # 按严重级别统计
            severity = event.severity.value
            stats["by_severity"][severity] = stats["by_severity"].get(severity, 0) + 1

        return stats

    def export_to_json(self, output_file: str, **filters) -> int:
        """导出审计日志到JSON文件

        Args:
            output_file: 输出文件路径
            **filters: 过滤条件

        Returns:
            int: 导出的记录数
        """
        events = self.query(limit=100000, **filters)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([e.to_dict() for e in events], f, ensure_ascii=False, indent=2)

        return len(events)


class AsyncAuditLogger(AuditLogger):
    """异步审计日志记录器"""

    import asyncio

    async def log_async(self, event: AuditEvent) -> None:
        """异步记录审计事件"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.log, event)

    async def log_memory_access_async(self, **kwargs) -> None:
        """异步记录记忆访问"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.log_memory_access, **kwargs)

    async def log_auth_async(self, **kwargs) -> None:
        """异步记录认证事件"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.log_auth, **kwargs)


class AuditLoggerMiddleware:
    """审计日志中间件 - 用于Web框架集成"""

    def __init__(self, logger: AuditLogger):
        self.logger = logger

    def extract_client_info(self, request) -> Dict[str, Any]:
        """从请求中提取客户端信息"""
        return {
            "ip_address": getattr(request, 'client_ip', None),
            "user_agent": getattr(request, 'user_agent', None),
            "session_id": getattr(request, 'session_id', None),
            "request_id": getattr(request, 'request_id', None),
        }

    def log_request(self, request, response, user_id: str, **kwargs) -> None:
        """记录HTTP请求"""
        client_info = self.extract_client_info(request)

        event = AuditEvent(
            event_type=AuditEventType.MEMORY_READ,
            user_id=user_id,
            timestamp=time.time(),
            resource_type="api",
            resource_id=getattr(request, 'path', None),
            action=getattr(request, 'method', 'GET').lower(),
            status="success" if response.status_code < 400 else "failure",
            details={
                "status_code": getattr(response, 'status_code', 0),
                **kwargs
            },
            **client_info
        )
        self.logger.log(event)

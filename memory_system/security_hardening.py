#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
安全加固模块 (Security Hardening Module)

提供输入验证和过滤、SQL/XSS/CSRF防护、加密解密工具、审计日志、
访问控制(RBAC/ABAC)和漏洞扫描集成能力。

Author: Agent Memory System Team
Version: 3.0.0
"""

import hashlib
import hmac
import html
import json
import logging
import re
import secrets
import threading
import time
import uuid
from abc import ABC, abstractmethod
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from functools import wraps

# 配置日志
logger = logging.getLogger(__name__)


class Permission(Enum):
    """权限枚举"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    EXECUTE = "execute"
    ADMIN = "admin"


class Role(Enum):
    """角色枚举"""
    GUEST = "guest"
    USER = "user"
    MODERATOR = "moderator"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"


@dataclass
class User:
    """用户"""
    user_id: str
    username: str
    roles: List[Role] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    is_active: bool = True
    
    def has_role(self, role: Role) -> bool:
        """检查是否有指定角色"""
        return role in self.roles
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "roles": [r.value for r in self.roles],
            "attributes": self.attributes,
            "created_at": self.created_at.isoformat(),
            "last_login": self.last_login.isoformat() if self.last_login else None,
            "is_active": self.is_active,
        }


@dataclass
class AuditLogEntry:
    """审计日志条目"""
    entry_id: str
    timestamp: datetime
    user_id: Optional[str]
    action: str
    resource: str
    result: str  # success, failure, denied
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "action": self.action,
            "resource": self.resource,
            "result": self.result,
            "details": self.details,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
        }


class InputValidator:
    """输入验证器"""
    
    # 常见SQL注入模式
    SQL_INJECTION_PATTERNS = [
        r"(\%27)|(\')|(\-\-)|(\%23)|(#)",
        r"((\%3D)|(=))[^\n]*((\%27)|(\')|(\-\-)|(\%3B)|(;))",
        r"\w*((\%27)|(\'))((\%6F)|o|(\%4F))((\%72)|r|(\%52))",
        r"((\%27)|(\'))union",
        r"exec(\s|\+)+(s|x)p\w+",
        r"UNION\s+SELECT",
        r"INSERT\s+INTO",
        r"DELETE\s+FROM",
        r"DROP\s+TABLE",
    ]
    
    # XSS模式
    XSS_PATTERNS = [
        r"<script[^>]*>[\s\S]*?</script>",
        r"javascript:",
        r"on\w+\s*=",
        r"<iframe",
        r"<object",
        r"<embed",
        r"eval\s*\(",
        r"expression\s*\(",
    ]
    
    # 命令注入模式
    COMMAND_INJECTION_PATTERNS = [
        r"[;&|`]\s*\w+",
        r"\$\(",
        r"`[^`]+`",
    ]
    
    def __init__(self):
        self._sql_regex = [re.compile(p, re.IGNORECASE) for p in self.SQL_INJECTION_PATTERNS]
        self._xss_regex = [re.compile(p, re.IGNORECASE) for p in self.XSS_PATTERNS]
        self._cmd_regex = [re.compile(p, re.IGNORECASE) for p in self.COMMAND_INJECTION_PATTERNS]
    
    def sanitize_string(self, value: str, max_length: int = 1000) -> str:
        """清理字符串输入"""
        if not isinstance(value, str):
            value = str(value)
        
        # 截断超长字符串
        if len(value) > max_length:
            value = value[:max_length]
        
        # 移除控制字符
        value = ''.join(char for char in value if ord(char) >= 32 or char in '\n\r\t')
        
        return value
    
    def validate_email(self, email: str) -> bool:
        """验证邮箱格式"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def validate_url(self, url: str) -> bool:
        """验证URL格式"""
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, url, re.IGNORECASE))
    
    def validate_uuid(self, value: str) -> bool:
        """验证UUID格式"""
        try:
            uuid.UUID(value)
            return True
        except ValueError:
            return False
    
    def check_sql_injection(self, value: str) -> Tuple[bool, List[str]]:
        """检查SQL注入"""
        threats = []
        for regex in self._sql_regex:
            if regex.search(value):
                threats.append(regex.pattern[:50])
        return len(threats) > 0, threats
    
    def check_xss(self, value: str) -> Tuple[bool, List[str]]:
        """检查XSS攻击"""
        threats = []
        for regex in self._xss_regex:
            if regex.search(value):
                threats.append(regex.pattern[:50])
        return len(threats) > 0, threats
    
    def check_command_injection(self, value: str) -> Tuple[bool, List[str]]:
        """检查命令注入"""
        threats = []
        for regex in self._cmd_regex:
            if regex.search(value):
                threats.append(regex.pattern[:50])
        return len(threats) > 0, threats
    
    def escape_html(self, value: str) -> str:
        """转义HTML特殊字符"""
        return html.escape(value)
    
    def escape_sql(self, value: str) -> str:
        """转义SQL特殊字符"""
        # 基本转义，实际应使用参数化查询
        return value.replace("'", "''").replace("\\", "\\\\")
    
    def validate_json(self, value: str) -> Tuple[bool, Optional[Dict]]:
        """验证JSON格式"""
        try:
            data = json.loads(value)
            return True, data
        except json.JSONDecodeError as e:
            return False, None
    
    def validate_input(self, value: str, rules: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """根据规则验证输入"""
        errors = []
        
        # 类型检查
        if rules.get("type") == "email" and not self.validate_email(value):
            errors.append("Invalid email format")
        
        if rules.get("type") == "url" and not self.validate_url(value):
            errors.append("Invalid URL format")
        
        if rules.get("type") == "uuid" and not self.validate_uuid(value):
            errors.append("Invalid UUID format")
        
        # 长度检查
        min_len = rules.get("min_length")
        max_len = rules.get("max_length")
        if min_len and len(value) < min_len:
            errors.append(f"Minimum length is {min_len}")
        if max_len and len(value) > max_len:
            errors.append(f"Maximum length is {max_len}")
        
        # 模式检查
        if "pattern" in rules:
            if not re.match(rules["pattern"], value):
                errors.append("Value does not match required pattern")
        
        # 安全检查
        if rules.get("check_sql_injection", True):
            is_threat, threats = self.check_sql_injection(value)
            if is_threat:
                errors.append(f"Potential SQL injection detected")
        
        if rules.get("check_xss", True):
            is_threat, threats = self.check_xss(value)
            if is_threat:
                errors.append(f"Potential XSS detected")
        
        return len(errors) == 0, errors


class CryptoManager:
    """加密管理器"""
    
    def __init__(self, master_key: Optional[bytes] = None):
        self._master_key = master_key or secrets.token_bytes(32)
        self._lock = threading.Lock()
    
    def hash_password(self, password: str, salt: Optional[bytes] = None) -> Tuple[str, bytes]:
        """哈希密码 (PBKDF2)"""
        if salt is None:
            salt = secrets.token_bytes(32)
        
        hash_value = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt,
            100000  # 迭代次数
        )
        
        return b64encode(hash_value).decode('utf-8'), salt
    
    def verify_password(self, password: str, hash_value: str, salt: bytes) -> bool:
        """验证密码"""
        computed_hash, _ = self.hash_password(password, salt)
        return hmac.compare_digest(computed_hash, hash_value)
    
    def generate_token(self, length: int = 32) -> str:
        """生成安全令牌"""
        return secrets.token_urlsafe(length)
    
    def generate_api_key(self) -> str:
        """生成API密钥"""
        return f"ak_{secrets.token_urlsafe(32)}"
    
    def hash_data(self, data: str, algorithm: str = "sha256") -> str:
        """哈希数据"""
        if algorithm == "sha256":
            return hashlib.sha256(data.encode()).hexdigest()
        elif algorithm == "sha512":
            return hashlib.sha512(data.encode()).hexdigest()
        elif algorithm == "md5":
            return hashlib.md5(data.encode()).hexdigest()
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
    
    def simple_encrypt(self, data: str, key: Optional[bytes] = None) -> str:
        """简单加密 (XOR，仅用于演示，生产环境请使用专业加密库)"""
        key = key or self._master_key
        key_len = len(key)
        encrypted = bytearray()
        for i, char in enumerate(data.encode()):
            encrypted.append(char ^ key[i % key_len])
        return b64encode(bytes(encrypted)).decode()
    
    def simple_decrypt(self, encrypted_data: str, key: Optional[bytes] = None) -> str:
        """简单解密"""
        key = key or self._master_key
        encrypted = b64decode(encrypted_data)
        key_len = len(key)
        decrypted = bytearray()
        for i, byte in enumerate(encrypted):
            decrypted.append(byte ^ key[i % key_len])
        return bytes(decrypted).decode()


class CSRFProtector:
    """CSRF保护器"""
    
    def __init__(self, token_ttl: int = 3600):
        self.token_ttl = token_ttl
        self._tokens: Dict[str, Tuple[str, datetime]] = {}
        self._lock = threading.Lock()
    
    def generate_token(self, session_id: str) -> str:
        """生成CSRF令牌"""
        token = secrets.token_urlsafe(32)
        
        with self._lock:
            self._tokens[session_id] = (token, datetime.utcnow())
        
        return token
    
    def validate_token(self, session_id: str, token: str) -> bool:
        """验证CSRF令牌"""
        with self._lock:
            stored = self._tokens.get(session_id)
            if not stored:
                return False
            
            stored_token, created_at = stored
            
            # 检查是否过期
            if datetime.utcnow() - created_at > timedelta(seconds=self.token_ttl):
                del self._tokens[session_id]
                return False
            
            return hmac.compare_digest(stored_token, token)
    
    def remove_token(self, session_id: str) -> None:
        """移除令牌"""
        with self._lock:
            if session_id in self._tokens:
                del self._tokens[session_id]
    
    def cleanup_expired(self) -> int:
        """清理过期令牌"""
        now = datetime.utcnow()
        expired = []
        
        with self._lock:
            for session_id, (_, created_at) in self._tokens.items():
                if now - created_at > timedelta(seconds=self.token_ttl):
                    expired.append(session_id)
            
            for session_id in expired:
                del self._tokens[session_id]
        
        return len(expired)


class RBACManager:
    """基于角色的访问控制管理器"""
    
    # 角色权限映射
    DEFAULT_ROLE_PERMISSIONS = {
        Role.GUEST: [Permission.READ],
        Role.USER: [Permission.READ, Permission.WRITE],
        Role.MODERATOR: [Permission.READ, Permission.WRITE, Permission.DELETE],
        Role.ADMIN: [Permission.READ, Permission.WRITE, Permission.DELETE, Permission.EXECUTE],
        Role.SUPER_ADMIN: [Permission.READ, Permission.WRITE, Permission.DELETE, Permission.EXECUTE, Permission.ADMIN],
    }
    
    def __init__(self):
        self._users: Dict[str, User] = {}
        self._role_permissions: Dict[Role, List[Permission]] = self.DEFAULT_ROLE_PERMISSIONS.copy()
        self._resource_permissions: Dict[str, Dict[str, List[Permission]]] = {}  # resource -> {role -> permissions}
        self._lock = threading.Lock()
    
    def create_user(self, username: str, roles: Optional[List[Role]] = None) -> User:
        """创建用户"""
        user = User(
            user_id=str(uuid.uuid4()),
            username=username,
            roles=roles or [Role.USER],
        )
        
        with self._lock:
            self._users[user.user_id] = user
        
        return user
    
    def get_user(self, user_id: str) -> Optional[User]:
        """获取用户"""
        with self._lock:
            return self._users.get(user_id)
    
    def assign_role(self, user_id: str, role: Role) -> bool:
        """分配角色"""
        with self._lock:
            user = self._users.get(user_id)
            if not user:
                return False
            
            if role not in user.roles:
                user.roles.append(role)
            
            return True
    
    def revoke_role(self, user_id: str, role: Role) -> bool:
        """撤销角色"""
        with self._lock:
            user = self._users.get(user_id)
            if not user:
                return False
            
            if role in user.roles:
                user.roles.remove(role)
            
            return True
    
    def check_permission(self, user_id: str, permission: Permission, resource: Optional[str] = None) -> bool:
        """检查权限"""
        user = self.get_user(user_id)
        if not user:
            return False
        
        if not user.is_active:
            return False
        
        # 检查用户角色的权限
        for role in user.roles:
            # 检查资源特定权限
            if resource and resource in self._resource_permissions:
                resource_perms = self._resource_permissions[resource].get(role, [])
                if permission in resource_perms:
                    return True
            
            # 检查角色默认权限
            role_perms = self._role_permissions.get(role, [])
            if permission in role_perms:
                return True
        
        return False
    
    def set_resource_permission(self, resource: str, role: Role, permissions: List[Permission]) -> None:
        """设置资源权限"""
        with self._lock:
            if resource not in self._resource_permissions:
                self._resource_permissions[resource] = {}
            self._resource_permissions[resource][role] = permissions
    
    def get_user_permissions(self, user_id: str) -> List[Permission]:
        """获取用户的所有权限"""
        user = self.get_user(user_id)
        if not user:
            return []
        
        permissions = set()
        for role in user.roles:
            permissions.update(self._role_permissions.get(role, []))
        
        return list(permissions)


class ABACManager:
    """基于属性的访问控制管理器"""
    
    def __init__(self):
        self._policies: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
    
    def add_policy(
        self,
        name: str,
        subject_attrs: Dict[str, Any],
        resource_attrs: Dict[str, Any],
        action: str,
        condition: Optional[Callable] = None,
        effect: str = "allow"
    ) -> None:
        """添加策略"""
        policy = {
            "name": name,
            "subject_attrs": subject_attrs,
            "resource_attrs": resource_attrs,
            "action": action,
            "condition": condition,
            "effect": effect,
        }
        
        with self._lock:
            self._policies.append(policy)
    
    def evaluate(
        self,
        subject: Dict[str, Any],
        resource: Dict[str, Any],
        action: str,
        environment: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[str]]:
        """评估访问请求"""
        environment = environment or {}
        
        for policy in self._policies:
            # 检查动作匹配
            if policy["action"] != "*" and policy["action"] != action:
                continue
            
            # 检查主体属性匹配
            if not self._match_attrs(subject, policy["subject_attrs"]):
                continue
            
            # 检查资源属性匹配
            if not self._match_attrs(resource, policy["resource_attrs"]):
                continue
            
            # 检查条件
            if policy["condition"]:
                if not policy["condition"](subject, resource, environment):
                    continue
            
            # 返回策略效果
            effect = policy["effect"]
            return effect == "allow", policy["name"]
        
        # 默认拒绝
        return False, None
    
    def _match_attrs(self, attrs: Dict[str, Any], pattern: Dict[str, Any]) -> bool:
        """匹配属性"""
        for key, value in pattern.items():
            if key not in attrs:
                return False
            
            if isinstance(value, list):
                if attrs[key] not in value:
                    return False
            elif attrs[key] != value:
                return False
        
        return True


class AuditLogger:
    """审计日志记录器"""
    
    def __init__(self, max_entries: int = 100000):
        self._entries: List[AuditLogEntry] = []
        self._max_entries = max_entries
        self._lock = threading.Lock()
        self._handlers: List[Callable[[AuditLogEntry], None]] = []
    
    def add_handler(self, handler: Callable[[AuditLogEntry], None]) -> None:
        """添加日志处理器"""
        self._handlers.append(handler)
    
    def log(
        self,
        user_id: Optional[str],
        action: str,
        resource: str,
        result: str,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> AuditLogEntry:
        """记录审计日志"""
        entry = AuditLogEntry(
            entry_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            user_id=user_id,
            action=action,
            resource=resource,
            result=result,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
        )
        
        with self._lock:
            self._entries.append(entry)
            
            # 限制日志数量
            if len(self._entries) > self._max_entries:
                self._entries = self._entries[-self._max_entries:]
        
        # 通知处理器
        for handler in self._handlers:
            try:
                handler(entry)
            except Exception as e:
                logger.error(f"Audit log handler failed: {e}")
        
        return entry
    
    def query(
        self,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        resource: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[AuditLogEntry]:
        """查询审计日志"""
        with self._lock:
            entries = self._entries.copy()
        
        results = entries
        
        if user_id:
            results = [e for e in results if e.user_id == user_id]
        
        if action:
            results = [e for e in results if e.action == action]
        
        if resource:
            results = [e for e in results if e.resource == resource]
        
        if start_time:
            results = [e for e in results if e.timestamp >= start_time]
        
        if end_time:
            results = [e for e in results if e.timestamp <= end_time]
        
        return results[-limit:]
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            entries = self._entries.copy()
        
        action_counts = {}
        result_counts = {}
        
        for entry in entries:
            action_counts[entry.action] = action_counts.get(entry.action, 0) + 1
            result_counts[entry.result] = result_counts.get(entry.result, 0) + 1
        
        return {
            "total_entries": len(entries),
            "action_counts": action_counts,
            "result_counts": result_counts,
        }


class VulnerabilityScanner:
    """漏洞扫描器"""
    
    def __init__(self):
        self._checks: List[Dict[str, Any]] = []
        self._init_default_checks()
    
    def _init_default_checks(self) -> None:
        """初始化默认检查"""
        self._checks = [
            {
                "id": "weak_password",
                "name": "Weak Password Check",
                "severity": "high",
                "check": self._check_weak_password,
            },
            {
                "id": "missing_headers",
                "name": "Security Headers Check",
                "severity": "medium",
                "check": self._check_security_headers,
            },
            {
                "id": "debug_mode",
                "name": "Debug Mode Check",
                "severity": "high",
                "check": self._check_debug_mode,
            },
        ]
    
    def add_check(self, check_id: str, name: str, severity: str, check_func: Callable) -> None:
        """添加检查项"""
        self._checks.append({
            "id": check_id,
            "name": name,
            "severity": severity,
            "check": check_func,
        })
    
    def scan(self, target: Dict[str, Any]) -> List[Dict[str, Any]]:
        """执行扫描"""
        findings = []
        
        for check in self._checks:
            try:
                result = check["check"](target)
                if result.get("vulnerable"):
                    findings.append({
                        "check_id": check["id"],
                        "name": check["name"],
                        "severity": check["severity"],
                        "details": result.get("details", {}),
                        "recommendation": result.get("recommendation", ""),
                    })
            except Exception as e:
                logger.error(f"Scan check failed: {check['id']}: {e}")
        
        return findings
    
    def _check_weak_password(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """检查弱密码"""
        # 简化实现
        password_policy = target.get("password_policy", {})
        min_length = password_policy.get("min_length", 0)
        
        if min_length < 8:
            return {
                "vulnerable": True,
                "details": {"min_length": min_length},
                "recommendation": "Set minimum password length to at least 8 characters",
            }
        
        return {"vulnerable": False}
    
    def _check_security_headers(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """检查安全头"""
        headers = target.get("headers", {})
        missing = []
        
        required_headers = [
            "X-Content-Type-Options",
            "X-Frame-Options",
            "X-XSS-Protection",
            "Strict-Transport-Security",
        ]
        
        for header in required_headers:
            if header not in headers:
                missing.append(header)
        
        if missing:
            return {
                "vulnerable": True,
                "details": {"missing_headers": missing},
                "recommendation": f"Add security headers: {', '.join(missing)}",
            }
        
        return {"vulnerable": False}
    
    def _check_debug_mode(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """检查调试模式"""
        if target.get("debug_mode", False):
            return {
                "vulnerable": True,
                "details": {"debug_mode": True},
                "recommendation": "Disable debug mode in production",
            }
        
        return {"vulnerable": False}


class SecurityHardeningModule:
    """
    安全加固模块主类
    
    DEFECT-007 [P2] 新增使用示例:
    
    示例用法:
        >>> from security_hardening import SecurityHardeningModule, Permission
        >>> 
        >>> security = SecurityHardeningModule()
        >>> 
        >>> # 输入验证
        >>> is_valid = security.validator.validate_email("user@example.com")
        >>> 
        >>> # 加密解密
        >>> encrypted = security.crypto.encrypt("sensitive data")
        >>> decrypted = security.crypto.decrypt(encrypted)
        >>> 
        >>> # RBAC 权限检查
        >>> security.rbac.add_role("admin", [Permission.READ, Permission.WRITE, Permission.ADMIN])
        >>> security.rbac.assign_role("user_001", "admin")
        >>> allowed = security.rbac.check_permission("user_001", Permission.WRITE)
    """
    
    def __init__(self, master_key: Optional[bytes] = None):
        self.validator = InputValidator()
        self.crypto = CryptoManager(master_key)
        self.csrf = CSRFProtector()
        self.rbac = RBACManager()
        self.abac = ABACManager()
        self.audit = AuditLogger()
        self.scanner = VulnerabilityScanner()
    
    def secure_endpoint(self, required_permission: Optional[Permission] = None):
        """端点安全装饰器"""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                # 记录访问
                self.audit.log(
                    user_id=kwargs.get("user_id"),
                    action="access",
                    resource=func.__name__,
                    result="attempt",
                    details={"args": str(args), "kwargs": str(kwargs)}
                )
                
                # 权限检查
                if required_permission:
                    user_id = kwargs.get("user_id")
                    if not user_id or not self.rbac.check_permission(user_id, required_permission):
                        self.audit.log(
                            user_id=user_id,
                            action="access",
                            resource=func.__name__,
                            result="denied",
                            details={"reason": "insufficient_permissions"}
                        )
                        raise PermissionError("Insufficient permissions")
                
                # 执行函数
                try:
                    result = func(*args, **kwargs)
                    self.audit.log(
                        user_id=kwargs.get("user_id"),
                        action="access",
                        resource=func.__name__,
                        result="success",
                    )
                    return result
                except Exception as e:
                    self.audit.log(
                        user_id=kwargs.get("user_id"),
                        action="access",
                        resource=func.__name__,
                        result="error",
                        details={"error": str(e)}
                    )
                    raise
            
            return wrapper
        return decorator
    
    def get_security_report(self) -> Dict[str, Any]:
        """生成安全报告"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "audit_stats": self.audit.get_stats(),
            "user_count": len(self.rbac._users),
            "csrf_tokens_active": len(self.csrf._tokens),
            "abac_policies": len(self.abac._policies),
        }


# 全局安全模块实例
_security_module: Optional[SecurityHardeningModule] = None


def get_security_module() -> SecurityHardeningModule:
    """获取全局安全模块实例"""
    global _security_module
    if _security_module is None:
        _security_module = SecurityHardeningModule()
    return _security_module


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    print("=== Security Hardening Module Demo ===\n")
    
    # 创建安全模块
    security = get_security_module()
    
    # 示例1: 输入验证
    print("1. Input Validation:")
    
    # 验证邮箱
    email = "test@example.com"
    print(f"   Email '{email}' valid: {security.validator.validate_email(email)}")
    
    # 检查SQL注入
    malicious_input = "'; DROP TABLE users; --"
    is_threat, threats = security.validator.check_sql_injection(malicious_input)
    print(f"   SQL injection detected: {is_threat}")
    
    # HTML转义
    xss_input = "<script>alert('xss')</script>"
    escaped = security.validator.escape_html(xss_input)
    print(f"   Escaped HTML: {escaped}")
    
    # 示例2: 密码哈希
    print("\n2. Password Hashing:")
    
    password = "my_secure_password"
    hash_value, salt = security.crypto.hash_password(password)
    print(f"   Password hash: {hash_value[:30]}...")
    
    # 验证密码
    is_valid = security.crypto.verify_password(password, hash_value, salt)
    print(f"   Password valid: {is_valid}")
    
    # 示例3: RBAC
    print("\n3. Role-Based Access Control:")
    
    # 创建用户
    user = security.rbac.create_user("john_doe", [Role.USER])
    print(f"   Created user: {user.username} with roles: {[r.value for r in user.roles]}")
    
    # 检查权限
    can_write = security.rbac.check_permission(user.user_id, Permission.WRITE)
    can_delete = security.rbac.check_permission(user.user_id, Permission.DELETE)
    print(f"   Can write: {can_write}")
    print(f"   Can delete: {can_delete}")
    
    # 分配管理员角色
    security.rbac.assign_role(user.user_id, Role.ADMIN)
    can_delete = security.rbac.check_permission(user.user_id, Permission.DELETE)
    print(f"   Can delete (after admin role): {can_delete}")
    
    # 示例4: ABAC
    print("\n4. Attribute-Based Access Control:")
    
    # 添加策略
    security.abac.add_policy(
        name="business_hours_only",
        subject_attrs={"department": "finance"},
        resource_attrs={"type": "sensitive"},
        action="read",
        condition=lambda s, r, e: 9 <= datetime.utcnow().hour < 17,
        effect="allow"
    )
    
    # 评估访问
    subject = {"department": "finance", "user_id": "user123"}
    resource = {"type": "sensitive", "id": "doc456"}
    allowed, policy = security.abac.evaluate(subject, resource, "read")
    print(f"   Access allowed: {allowed} (policy: {policy})")
    
    # 示例5: 审计日志
    print("\n5. Audit Logging:")
    
    security.audit.log(
        user_id=user.user_id,
        action="login",
        resource="auth_system",
        result="success",
        details={"ip": "192.168.1.1"}
    )
    
    security.audit.log(
        user_id=user.user_id,
        action="data_access",
        resource="user_records",
        result="success",
        details={"records_count": 10}
    )
    
    stats = security.audit.get_stats()
    print(f"   Total audit entries: {stats['total_entries']}")
    print(f"   Action counts: {stats['action_counts']}")
    
    # 示例6: 漏洞扫描
    print("\n6. Vulnerability Scanning:")
    
    target = {
        "password_policy": {"min_length": 6},
        "headers": {},
        "debug_mode": True,
    }
    
    findings = security.scanner.scan(target)
    print(f"   Vulnerabilities found: {len(findings)}")
    for finding in findings:
        print(f"   - {finding['name']} ({finding['severity']}): {finding['recommendation']}")
    
    # 示例7: 安全报告
    print("\n7. Security Report:")
    
    report = security.get_security_report()
    print(f"   Timestamp: {report['timestamp']}")
    print(f"   User count: {report['user_count']}")
    print(f"   Active CSRF tokens: {report['csrf_tokens_active']}")
    
    print("\n=== Security Hardening Module Demo Completed ===")

"""
配置中心模块 - Config Center Module

该模块实现了云原生配置管理，包含：
- 集中式配置管理
- 动态配置热更新
- 配置版本控制
- 多环境配置隔离
- 配置加密和安全
- 变更通知机制

作者: Agent Memory System Team
版本: 3.0.0
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import time
import uuid
import yaml
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any, Callable, Coroutine, Dict, Generic, List, Optional, Set, TypeVar,
    Union, AsyncIterator, Protocol, runtime_checkable, Tuple
)
from contextlib import asynccontextmanager

# 配置日志
logger = logging.getLogger(__name__)


# ============================================================================
# 类型定义和枚举
# ============================================================================

class ConfigFormat(Enum):
    """配置格式"""
    JSON = "json"
    YAML = "yaml"
    PROPERTIES = "properties"
    ENV = "env"
    TOML = "toml"


class ConfigScope(Enum):
    """配置作用域"""
    GLOBAL = "global"
    APPLICATION = "application"
    SERVICE = "service"
    INSTANCE = "instance"


class ConfigStatus(Enum):
    """配置状态"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    ROLLED_BACK = "rolled_back"


class EncryptionType(Enum):
    """加密类型"""
    NONE = "none"
    AES = "aes"
    RSA = "rsa"
    VAULT = "vault"


T = TypeVar('T')


# ============================================================================
# 配置数据模型
# ============================================================================

@dataclass
class ConfigItem:
    """
    配置项
    
    单个配置键值对
    """
    key: str
    value: Any
    format: ConfigFormat = ConfigFormat.JSON
    scope: ConfigScope = ConfigScope.APPLICATION
    encrypted: bool = False
    description: str = ""
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value if not self.encrypted else "***encrypted***",
            "format": self.format.value,
            "scope": self.scope.value,
            "encrypted": self.encrypted,
            "description": self.description,
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "version": self.version
        }


@dataclass
class ConfigNamespace:
    """
    配置命名空间
    
    用于环境隔离（如dev, staging, prod）
    """
    name: str
    description: str = ""
    environment: str = "default"
    labels: Dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "environment": self.environment,
            "labels": self.labels,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class ConfigVersion:
    """
    配置版本
    
    配置的特定版本快照
    """
    version_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    namespace: str = ""
    config_data: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: str = ""
    comment: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "namespace": self.namespace,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "comment": self.comment,
            "labels": self.labels
        }


@dataclass
class ConfigChangeEvent:
    """
    配置变更事件
    
    当配置发生变化时触发
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    namespace: str = ""
    key: str = ""
    old_value: Any = None
    new_value: Any = None
    change_type: str = ""  # add, update, delete
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = ""  # 变更来源


# ============================================================================
# 配置存储抽象
# ============================================================================

class ConfigStorage(ABC):
    """
    配置存储抽象
    
    支持多种后端存储（文件、数据库、etcd等）
    """
    
    @abstractmethod
    async def get(self, namespace: str, key: str) -> Optional[ConfigItem]:
        """获取配置项"""
        pass
    
    @abstractmethod
    async def set(self, namespace: str, item: ConfigItem) -> bool:
        """设置配置项"""
        pass
    
    @abstractmethod
    async def delete(self, namespace: str, key: str) -> bool:
        """删除配置项"""
        pass
    
    @abstractmethod
    async def list_keys(self, namespace: str, prefix: str = "") -> List[str]:
        """列出配置键"""
        pass
    
    @abstractmethod
    async def get_all(self, namespace: str) -> Dict[str, Any]:
        """获取命名空间下所有配置"""
        pass
    
    @abstractmethod
    async def watch(
        self,
        namespace: str,
        callback: Callable[[ConfigChangeEvent], Coroutine[Any, Any, None]]
    ) -> None:
        """监视配置变化"""
        pass


class FileConfigStorage(ConfigStorage):
    """
    文件配置存储
    
    基于文件系统的配置存储
    """
    
    def __init__(self, base_path: str = "/etc/config"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._watchers: Dict[str, List[Callable]] = {}
        self._watch_task: Optional[asyncio.Task] = None
        self._file_mtimes: Dict[str, float] = {}
    
    def _get_namespace_path(self, namespace: str) -> Path:
        """获取命名空间路径"""
        path = self.base_path / namespace
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def _get_config_path(self, namespace: str, key: str) -> Path:
        """获取配置文件路径"""
        # 将点分隔的键转换为路径
        key_path = key.replace(".", "/")
        return self._get_namespace_path(namespace) / f"{key_path}.json"
    
    async def get(self, namespace: str, key: str) -> Optional[ConfigItem]:
        """获取配置项"""
        config_path = self._get_config_path(namespace, key)
        
        if not config_path.exists():
            return None
        
        try:
            with open(config_path, 'r') as f:
                data = json.load(f)
            
            return ConfigItem(
                key=key,
                value=data.get("value"),
                format=ConfigFormat(data.get("format", "json")),
                scope=ConfigScope(data.get("scope", "application")),
                encrypted=data.get("encrypted", False),
                description=data.get("description", ""),
                tags=data.get("tags", []),
                version=data.get("version", 1)
            )
        except Exception as e:
            logger.exception(f"Failed to read config: {e}")
            return None
    
    async def set(self, namespace: str, item: ConfigItem) -> bool:
        """设置配置项"""
        config_path = self._get_config_path(namespace, item.key)
        
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                "value": item.value,
                "format": item.format.value,
                "scope": item.scope.value,
                "encrypted": item.encrypted,
                "description": item.description,
                "tags": item.tags,
                "version": item.version,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            with open(config_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            return True
        except Exception as e:
            logger.exception(f"Failed to write config: {e}")
            return False
    
    async def delete(self, namespace: str, key: str) -> bool:
        """删除配置项"""
        config_path = self._get_config_path(namespace, key)
        
        try:
            if config_path.exists():
                config_path.unlink()
            return True
        except Exception as e:
            logger.exception(f"Failed to delete config: {e}")
            return False
    
    async def list_keys(self, namespace: str, prefix: str = "") -> List[str]:
        """列出配置键"""
        namespace_path = self._get_namespace_path(namespace)
        keys = []
        
        for config_file in namespace_path.rglob("*.json"):
            # 将路径转换回点分隔的键
            relative = config_file.relative_to(namespace_path)
            key = str(relative.with_suffix("")).replace("/", ".")
            
            if not prefix or key.startswith(prefix):
                keys.append(key)
        
        return keys
    
    async def get_all(self, namespace: str) -> Dict[str, Any]:
        """获取所有配置"""
        keys = await self.list_keys(namespace)
        config = {}
        
        for key in keys:
            item = await self.get(namespace, key)
            if item:
                config[key] = item.value
        
        return config
    
    async def watch(
        self,
        namespace: str,
        callback: Callable[[ConfigChangeEvent], Coroutine[Any, Any, None]]
    ) -> None:
        """监视配置变化"""
        if namespace not in self._watchers:
            self._watchers[namespace] = []
        
        self._watchers[namespace].append(callback)
        
        # 启动监视任务
        if self._watch_task is None:
            self._watch_task = asyncio.create_task(self._watch_loop())
    
    async def _watch_loop(self) -> None:
        """监视循环"""
        while True:
            try:
                await self._check_changes()
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Watch loop error: {e}")
    
    async def _check_changes(self) -> None:
        """检查配置变化"""
        for namespace in self._watchers:
            namespace_path = self._get_namespace_path(namespace)
            
            for config_file in namespace_path.rglob("*.json"):
                try:
                    mtime = config_file.stat().st_mtime
                    file_key = str(config_file)
                    
                    if file_key in self._file_mtimes:
                        if mtime > self._file_mtimes[file_key]:
                            # 文件已修改
                            await self._notify_change(namespace, config_file)
                    
                    self._file_mtimes[file_key] = mtime
                except Exception:
                    pass
    
    async def _notify_change(self, namespace: str, config_file: Path) -> None:
        """通知配置变化"""
        relative = config_file.relative_to(self._get_namespace_path(namespace))
        key = str(relative.with_suffix("")).replace("/", ".")
        
        event = ConfigChangeEvent(
            namespace=namespace,
            key=key,
            change_type="update"
        )
        
        for callback in self._watchers.get(namespace, []):
            try:
                await callback(event)
            except Exception as e:
                logger.exception(f"Watch callback error: {e}")


class EtcdConfigStorage(ConfigStorage):
    """
    Etcd配置存储
    
    基于Etcd的分布式配置存储
    """
    
    def __init__(self, endpoints: List[str] = None):
        self.endpoints = endpoints or ["localhost:2379"]
        self._client: Optional[Any] = None
    
    async def connect(self) -> None:
        """连接到Etcd"""
        try:
            import etcd3
            self._client = etcd3.client(host=self.endpoints[0].split(":")[0])
            logger.info("Connected to Etcd")
        except ImportError:
            logger.error("etcd3 not installed")
            raise
    
    async def get(self, namespace: str, key: str) -> Optional[ConfigItem]:
        """从Etcd获取配置"""
        if not self._client:
            return None
        
        full_key = f"/config/{namespace}/{key}"
        
        try:
            value, metadata = self._client.get(full_key)
            
            if value:
                data = json.loads(value.decode())
                return ConfigItem(
                    key=key,
                    value=data.get("value"),
                    format=ConfigFormat(data.get("format", "json")),
                    encrypted=data.get("encrypted", False)
                )
        except Exception as e:
            logger.exception(f"Etcd get error: {e}")
        
        return None
    
    async def set(self, namespace: str, item: ConfigItem) -> bool:
        """设置Etcd配置"""
        if not self._client:
            return False
        
        full_key = f"/config/{namespace}/{item.key}"
        
        try:
            data = {
                "value": item.value,
                "format": item.format.value,
                "encrypted": item.encrypted,
                "version": item.version
            }
            
            self._client.put(full_key, json.dumps(data, default=str).encode())
            return True
        except Exception as e:
            logger.exception(f"Etcd set error: {e}")
            return False
    
    async def delete(self, namespace: str, key: str) -> bool:
        """删除Etcd配置"""
        if not self._client:
            return False
        
        full_key = f"/config/{namespace}/{key}"
        
        try:
            self._client.delete(full_key)
            return True
        except Exception as e:
            logger.exception(f"Etcd delete error: {e}")
            return False
    
    async def list_keys(self, namespace: str, prefix: str = "") -> List[str]:
        """列出Etcd键"""
        if not self._client:
            return []
        
        full_prefix = f"/config/{namespace}/{prefix}"
        
        try:
            keys = []
            for value, metadata in self._client.get_prefix(full_prefix):
                key = metadata.key.decode().replace(f"/config/{namespace}/", "")
                keys.append(key)
            return keys
        except Exception as e:
            logger.exception(f"Etcd list error: {e}")
            return []
    
    async def get_all(self, namespace: str) -> Dict[str, Any]:
        """获取所有配置"""
        keys = await self.list_keys(namespace)
        config = {}
        
        for key in keys:
            item = await self.get(namespace, key)
            if item:
                config[key] = item.value
        
        return config
    
    async def watch(
        self,
        namespace: str,
        callback: Callable[[ConfigChangeEvent], Coroutine[Any, Any, None]]
    ) -> None:
        """监视Etcd变化"""
        if not self._client:
            return
        
        prefix = f"/config/{namespace}/"
        
        def on_event(event):
            for ev in event.events:
                key = ev.key.decode().replace(prefix, "")
                change_event = ConfigChangeEvent(
                    namespace=namespace,
                    key=key,
                    change_type="update" if hasattr(ev, 'value') else "delete"
                )
                asyncio.create_task(callback(change_event))
        
        self._client.add_watch_prefix_callback(prefix, on_event)


# ============================================================================
# 配置加密
# ============================================================================

class ConfigEncryptor(ABC):
    """配置加密器抽象"""
    
    @abstractmethod
    def encrypt(self, value: str) -> str:
        """加密值"""
        pass
    
    @abstractmethod
    def decrypt(self, encrypted_value: str) -> str:
        """解密值"""
        pass


class AESConfigEncryptor(ConfigEncryptor):
    """
    AES配置加密器
    
    使用AES加密敏感配置
    """
    
    def __init__(self, key: str):
        self.key = hashlib.sha256(key.encode()).digest()
    
    def encrypt(self, value: str) -> str:
        """加密值"""
        try:
            from cryptography.fernet import Fernet
            
            # 使用key生成Fernet密钥
            fernet_key = base64.urlsafe_b64encode(self.key[:32])
            fernet = Fernet(fernet_key)
            
            encrypted = fernet.encrypt(value.encode())
            return f"ENC:{encrypted.decode()}"
        except ImportError:
            logger.error("cryptography not installed")
            return value
    
    def decrypt(self, encrypted_value: str) -> str:
        """解密值"""
        if not encrypted_value.startswith("ENC:"):
            return encrypted_value
        
        try:
            from cryptography.fernet import Fernet
            
            fernet_key = base64.urlsafe_b64encode(self.key[:32])
            fernet = Fernet(fernet_key)
            
            encrypted = encrypted_value[4:].encode()
            decrypted = fernet.decrypt(encrypted)
            return decrypted.decode()
        except Exception as e:
            logger.exception(f"Decryption error: {e}")
            return encrypted_value


# ============================================================================
# 配置中心
# ============================================================================

class ConfigCenter:
    """
    配置中心
    
    统一的配置管理入口
    
    DEFECT-007 [P2] 新增使用示例:
    
    示例用法:
        >>> from config_center import ConfigCenter, FileConfigStorage, AESConfigEncryptor
        >>> 
        >>> storage = FileConfigStorage("/tmp/app_config")
        >>> encryptor = AESConfigEncryptor("my-secret-key")
        >>> center = ConfigCenter(storage, encryptor)
        >>> await center.initialize()
        >>> 
        >>> # 设置配置
        >>> await center.set("database.host", "db.example.com", namespace="production")
        >>> await center.set("database.port", 5432, namespace="production")
        >>> 
        >>> # 获取配置
        >>> host = await center.get("database.host", namespace="production")
        >>> 
        >>> # 监听配置变更
        >>> await center.watch("database.*", lambda event: print(f"Changed: {event.key}"))
        >>> 
        >>> # 创建版本快照
        >>> version = await center.create_version("production", comment="v1.0 release")
    """
    
    def __init__(
        self,
        storage: ConfigStorage,
        encryptor: Optional[ConfigEncryptor] = None
    ):
        self.storage = storage
        self.encryptor = encryptor
        self._namespaces: Dict[str, ConfigNamespace] = {}
        self._versions: Dict[str, List[ConfigVersion]] = {}
        self._listeners: Dict[str, List[Callable[[ConfigChangeEvent], None]]] = {}
        self._cache: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """初始化配置中心"""
        # 加载所有命名空间
        logger.info("Config center initialized")
    
    async def create_namespace(
        self,
        name: str,
        description: str = "",
        environment: str = "default",
        labels: Optional[Dict[str, str]] = None
    ) -> ConfigNamespace:
        """创建配置命名空间"""
        namespace = ConfigNamespace(
            name=name,
            description=description,
            environment=environment,
            labels=labels or {}
        )
        
        async with self._lock:
            self._namespaces[name] = namespace
        
        logger.info(f"Created namespace: {name}")
        return namespace
    
    async def get(
        self,
        key: str,
        namespace: str = "default",
        default: Any = None
    ) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键
            namespace: 命名空间
            default: 默认值
            
        Returns:
            配置值
        """
        # 检查缓存
        cache_key = f"{namespace}:{key}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # 从存储获取
        item = await self.storage.get(namespace, key)
        
        if not item:
            return default
        
        value = item.value
        
        # 解密
        if item.encrypted and self.encryptor:
            if isinstance(value, str):
                value = self.encryptor.decrypt(value)
        
        # 更新缓存
        self._cache[cache_key] = value
        
        return value
    
    async def set(
        self,
        key: str,
        value: Any,
        namespace: str = "default",
        encrypted: bool = False,
        description: str = "",
        tags: Optional[List[str]] = None
    ) -> bool:
        """
        设置配置值
        
        Args:
            key: 配置键
            value: 配置值
            namespace: 命名空间
            encrypted: 是否加密
            description: 描述
            tags: 标签
            
        Returns:
            是否成功
        """
        # 加密
        if encrypted and self.encryptor:
            if isinstance(value, str):
                value = self.encryptor.encrypt(value)
        
        # 获取旧值
        old_item = await self.storage.get(namespace, key)
        old_value = old_item.value if old_item else None
        
        # 创建配置项
        item = ConfigItem(
            key=key,
            value=value,
            encrypted=encrypted,
            description=description,
            tags=tags or [],
            version=(old_item.version + 1) if old_item else 1
        )
        
        # 保存
        success = await self.storage.set(namespace, item)
        
        if success:
            # 更新缓存
            cache_key = f"{namespace}:{key}"
            self._cache[cache_key] = value
            
            # 触发事件
            event = ConfigChangeEvent(
                namespace=namespace,
                key=key,
                old_value=old_value,
                new_value=value,
                change_type="update" if old_item else "add"
            )
            await self._notify_listeners(event)
            
            logger.info(f"Set config: {namespace}/{key}")
        
        return success
    
    async def delete(self, key: str, namespace: str = "default") -> bool:
        """删除配置"""
        # 获取旧值
        old_item = await self.storage.get(namespace, key)
        old_value = old_item.value if old_item else None
        
        success = await self.storage.delete(namespace, key)
        
        if success:
            # 清除缓存
            cache_key = f"{namespace}:{key}"
            self._cache.pop(cache_key, None)
            
            # 触发事件
            event = ConfigChangeEvent(
                namespace=namespace,
                key=key,
                old_value=old_value,
                change_type="delete"
            )
            await self._notify_listeners(event)
            
            logger.info(f"Deleted config: {namespace}/{key}")
        
        return success
    
    async def get_all(self, namespace: str = "default") -> Dict[str, Any]:
        """获取命名空间下所有配置"""
        return await self.storage.get_all(namespace)
    
    async def subscribe(
        self,
        key: str,
        callback: Callable[[ConfigChangeEvent], None],
        namespace: str = "default"
    ) -> None:
        """
        订阅配置变化
        
        Args:
            key: 配置键（支持通配符*）
            callback: 回调函数
            namespace: 命名空间
        """
        full_key = f"{namespace}:{key}"
        
        if full_key not in self._listeners:
            self._listeners[full_key] = []
        
        self._listeners[full_key].append(callback)
        
        # 启动监视
        await self.storage.watch(namespace, self._on_config_change)
    
    async def _on_config_change(self, event: ConfigChangeEvent) -> None:
        """配置变化回调"""
        # 清除缓存
        cache_key = f"{event.namespace}:{event.key}"
        self._cache.pop(cache_key, None)
        
        # 通知监听器
        await self._notify_listeners(event)
    
    async def _notify_listeners(self, event: ConfigChangeEvent) -> None:
        """通知监听器 - DEFECT-006: 支持 *:* 全局通配符匹配"""
        for pattern, callbacks in self._listeners.items():
            namespace, key_pattern = pattern.split(":", 1)
            
            # DEFECT-006: 支持 *:* 全局匹配（匹配所有命名空间和键）
            if namespace == "*" and key_pattern == "*":
                matched = True
            elif namespace == event.namespace:
                matched = (key_pattern == "*" or event.key == key_pattern)
            else:
                matched = False
            
            if matched:
                for callback in callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(event)
                        else:
                            callback(event)
                    except Exception as e:
                        logger.exception(f"Listener callback error: {e}")
    
    async def create_version(
        self,
        namespace: str,
        comment: str = "",
        created_by: str = ""
    ) -> ConfigVersion:
        """创建配置版本快照"""
        config_data = await self.get_all(namespace)
        
        version = ConfigVersion(
            namespace=namespace,
            config_data=config_data,
            comment=comment,
            created_by=created_by
        )
        
        async with self._lock:
            if namespace not in self._versions:
                self._versions[namespace] = []
            self._versions[namespace].append(version)
        
        logger.info(f"Created version: {version.version_id}")
        return version
    
    async def rollback(
        self,
        namespace: str,
        version_id: str
    ) -> bool:
        """回滚到指定版本"""
        versions = self._versions.get(namespace, [])
        target_version = None
        
        for v in versions:
            if v.version_id == version_id:
                target_version = v
                break
        
        if not target_version:
            return False
        
        # 恢复配置
        for key, value in target_version.config_data.items():
            await self.set(key, value, namespace)
        
        logger.info(f"Rolled back to version: {version_id}")
        return True
    
    def get_namespace(self, name: str) -> Optional[ConfigNamespace]:
        """获取命名空间"""
        return self._namespaces.get(name)
    
    def list_namespaces(self) -> List[ConfigNamespace]:
        """列出所有命名空间"""
        return list(self._namespaces.values())
    
    def get_versions(self, namespace: str) -> List[ConfigVersion]:
        """获取版本历史"""
        return self._versions.get(namespace, [])
    
    # ========================================================================
    # DEFECT-006 [P1] 修复: 添加配置变更监听接口
    # ========================================================================
    
    async def watch(
        self,
        key: str,
        callback: Union[
            Callable[[ConfigChangeEvent], None],
            Callable[[ConfigChangeEvent], Coroutine[Any, Any, None]]
        ],
        namespace: str = "default"
    ) -> None:
        """
        监听指定配置键的变更
        
        DEFECT-006 [P1] 新增: 当指定 key 的配置发生变化时自动触发回调。
        
        Args:
            key: 配置键（支持通配符 *）
            callback: 回调函数，接收 ConfigChangeEvent 参数。
                      支持同步和异步回调。
            namespace: 命名空间，默认为 "default"
        
        示例:
            >>> async def on_db_host_change(event: ConfigChangeEvent):
            ...     print(f"DB host changed: {event.old_value} -> {event.new_value}")
            >>> await config_center.watch("database.host", on_db_host_change, "production")
        """
        full_key = f"{namespace}:{key}"
        
        if full_key not in self._listeners:
            self._listeners[full_key] = []
        
        self._listeners[full_key].append(callback)
        
        # 启动底层存储的监视
        await self.storage.watch(namespace, self._on_config_change)
        
        logger.info(f"Registered watcher for {full_key}")
    
    async def on_config_change(
        self,
        callback: Union[
            Callable[[ConfigChangeEvent], None],
            Callable[[ConfigChangeEvent], Coroutine[Any, Any, None]]
        ]
    ) -> None:
        """
        全局配置变更监听
        
        DEFECT-006 [P1] 新增: 注册一个全局回调，当任意配置变更时都会被触发。
        
        Args:
            callback: 回调函数，接收 ConfigChangeEvent 参数。
                      支持同步和异步回调。
        
        示例:
            >>> async def on_any_change(event: ConfigChangeEvent):
            ...     print(f"Config changed: {event.namespace}/{event.key}")
            >>> await config_center.on_config_change(on_any_change)
        """
        # 使用通配符 "*" 注册到所有命名空间
        if "*:*" not in self._listeners:
            self._listeners["*:*"] = []
        
        self._listeners["*:*"].append(callback)
        
        logger.info("Registered global config change listener")


# ============================================================================
# 配置加载器
# ============================================================================

class ConfigLoader:
    """
    配置加载器
    
    从不同来源加载配置
    """
    
    @staticmethod
    def from_file(path: str, format: ConfigFormat = None) -> Dict[str, Any]:
        """从文件加载配置"""
        path = Path(path)
        
        if not path.exists():
            return {}
        
        # 自动检测格式
        if format is None:
            suffix = path.suffix.lower()
            if suffix == ".json":
                format = ConfigFormat.JSON
            elif suffix in [".yml", ".yaml"]:
                format = ConfigFormat.YAML
            elif suffix == ".properties":
                format = ConfigFormat.PROPERTIES
            elif suffix == ".toml":
                format = ConfigFormat.TOML
            else:
                format = ConfigFormat.JSON
        
        with open(path, 'r') as f:
            content = f.read()
        
        if format == ConfigFormat.JSON:
            return json.loads(content)
        elif format == ConfigFormat.YAML:
            return yaml.safe_load(content)
        elif format == ConfigFormat.PROPERTIES:
            return ConfigLoader._parse_properties(content)
        elif format == ConfigFormat.TOML:
            import toml
            return toml.loads(content)
        
        return {}
    
    @staticmethod
    def from_env(prefix: str = "") -> Dict[str, Any]:
        """从环境变量加载配置"""
        config = {}
        
        for key, value in os.environ.items():
            if prefix and not key.startswith(prefix):
                continue
            
            # 移除前缀并转换为小写
            config_key = key[len(prefix):].lower() if prefix else key.lower()
            config[config_key] = value
        
        return config
    
    @staticmethod
    def _parse_properties(content: str) -> Dict[str, Any]:
        """解析properties格式"""
        config = {}
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            
            if "=" in line:
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()
        
        return config


# ============================================================================
# 配置热更新
# ============================================================================

class ConfigHotReloader:
    """
    配置热重载器
    
    监视配置变化并自动重载
    """
    
    def __init__(self, config_center: ConfigCenter):
        self.config_center = config_center
        self._handlers: Dict[str, List[Callable[[str, Any], None]]] = {}
        self._running = False
    
    def on_change(
        self,
        key: str,
        handler: Callable[[str, Any], None]
    ) -> None:
        """
        注册配置变化处理器
        
        Args:
            key: 配置键（支持通配符*）
            handler: 变化处理器
        """
        if key not in self._handlers:
            self._handlers[key] = []
        
        self._handlers[key].append(handler)
    
    async def start(self, namespace: str = "default") -> None:
        """启动热重载监视"""
        self._running = True
        
        # 订阅配置变化
        await self.config_center.subscribe(
            "*",
            self._handle_change,
            namespace
        )
        
        logger.info("Config hot reloader started")
    
    async def stop(self) -> None:
        """停止热重载监视"""
        self._running = False
        logger.info("Config hot reloader stopped")
    
    async def _handle_change(self, event: ConfigChangeEvent) -> None:
        """处理配置变化"""
        for pattern, handlers in self._handlers.items():
            # 检查是否匹配
            if pattern == "*" or event.key == pattern:
                for handler in handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler(event.key, event.new_value)
                        else:
                            handler(event.key, event.new_value)
                    except Exception as e:
                        logger.exception(f"Hot reload handler error: {e}")


# ============================================================================
# 配置门面
# ============================================================================

class ConfigFacade:
    """
    配置门面
    
    提供简化的配置访问接口
    """
    
    def __init__(self, config_center: ConfigCenter):
        self.config_center = config_center
        self._namespace = "default"
    
    def with_namespace(self, namespace: str) -> ConfigFacade:
        """设置命名空间"""
        self._namespace = namespace
        return self
    
    async def get_string(self, key: str, default: str = "") -> str:
        """获取字符串配置"""
        value = await self.config_center.get(key, self._namespace, default)
        return str(value) if value is not None else default
    
    async def get_int(self, key: str, default: int = 0) -> int:
        """获取整数配置"""
        value = await self.config_center.get(key, self._namespace, default)
        return int(value) if value is not None else default
    
    async def get_float(self, key: str, default: float = 0.0) -> float:
        """获取浮点数配置"""
        value = await self.config_center.get(key, self._namespace, default)
        return float(value) if value is not None else default
    
    async def get_bool(self, key: str, default: bool = False) -> bool:
        """获取布尔配置"""
        value = await self.config_center.get(key, self._namespace, default)
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).lower() in ("true", "1", "yes", "on")
    
    async def get_list(self, key: str, default: List = None) -> List:
        """获取列表配置"""
        value = await self.config_center.get(key, self._namespace, default)
        if value is None:
            return default or []
        if isinstance(value, list):
            return value
        return str(value).split(",")
    
    async def get_dict(self, key: str, default: Dict = None) -> Dict:
        """获取字典配置"""
        value = await self.config_center.get(key, self._namespace, default)
        if value is None:
            return default or {}
        if isinstance(value, dict):
            return value
        return json.loads(str(value))
    
    async def set(self, key: str, value: Any) -> bool:
        """设置配置"""
        return await self.config_center.set(key, value, self._namespace)


# ============================================================================
# 使用示例
# ============================================================================

async def example_usage():
    """使用示例"""
    # 创建存储
    storage = FileConfigStorage("/tmp/config")
    
    # 创建加密器
    encryptor = AESConfigEncryptor("my-secret-key")
    
    # 创建配置中心
    config_center = ConfigCenter(storage, encryptor)
    await config_center.initialize()
    
    try:
        # 创建命名空间
        await config_center.create_namespace(
            "production",
            description="Production environment",
            environment="prod",
            labels={"region": "us-east-1", "tier": "backend"}
        )
        
        # 设置配置
        await config_center.set(
            "database.host",
            "db.example.com",
            namespace="production",
            description="Database host"
        )
        
        await config_center.set(
            "database.port",
            5432,
            namespace="production"
        )
        
        # 设置加密配置
        await config_center.set(
            "database.password",
            "secret-password-123",
            namespace="production",
            encrypted=True,
            description="Database password"
        )
        
        # 获取配置
        host = await config_center.get("database.host", "production")
        port = await config_center.get("database.port", "production")
        password = await config_center.get("database.password", "production")
        
        print(f"Database: {host}:{port}")
        print(f"Password: {password}")
        
        # 使用门面
        facade = ConfigFacade(config_center).with_namespace("production")
        
        db_host = await facade.get_string("database.host")
        db_port = await facade.get_int("database.port")
        
        print(f"Facade access: {db_host}:{db_port}")
        
        # 订阅配置变化
        def on_config_change(key: str, value: Any):
            print(f"Config changed: {key} = {value}")
        
        await config_center.subscribe("database.*", on_config_change, "production")
        
        # 创建版本
        version = await config_center.create_version(
            "production",
            comment="Initial production config",
            created_by="admin"
        )
        print(f"Created version: {version.version_id}")
        
        # 更新配置
        await config_center.set(
            "database.host",
            "new-db.example.com",
            namespace="production"
        )
        
        # 获取所有配置
        all_config = await config_center.get_all("production")
        print(f"All config: {json.dumps(all_config, indent=2)}")
        
        # 热重载示例
        reloader = ConfigHotReloader(config_center)
        
        def reload_handler(key: str, value: Any):
            print(f"Hot reload: {key} changed to {value}")
        
        reloader.on_change("database.*", reload_handler)
        await reloader.start("production")
        
        # 模拟配置变化
        await config_center.set(
            "database.port",
            5433,
            namespace="production"
        )
        
        await asyncio.sleep(2)
        
        await reloader.stop()
        
    finally:
        pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())

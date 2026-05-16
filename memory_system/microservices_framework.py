"""
微服务框架模块 - Microservices Framework Module

该模块实现了云原生微服务架构的核心组件，包含：
- 服务注册与发现（Consul/etcd）
- 负载均衡（轮询/加权/一致性哈希）
- 熔断器模式（Circuit Breaker）
- 限流器（Rate Limiter）
- 服务网关（API Gateway）
- 健康检查和优雅关闭

作者: Agent Memory System Team
版本: 3.0.0
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import signal
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import (
    Any, Callable, Coroutine, Dict, Generic, List, Optional, Set, TypeVar,
    Union, AsyncIterator, Protocol, runtime_checkable, Tuple
)
from contextlib import asynccontextmanager
from functools import wraps

# 配置日志
logger = logging.getLogger(__name__)


# ============================================================================
# 类型定义和枚举
# ============================================================================

class ServiceStatus(Enum):
    """服务状态"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    STOPPING = "stopping"
    UNKNOWN = "unknown"


class LoadBalanceStrategy(Enum):
    """负载均衡策略"""
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"
    CONSISTENT_HASH = "consistent_hash"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"


class CircuitState(Enum):
    """熔断器状态"""
    CLOSED = "closed"      # 正常
    OPEN = "open"          # 熔断
    HALF_OPEN = "half_open"  # 半开


class RateLimitStrategy(Enum):
    """限流策略"""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


T = TypeVar('T')
ServiceId = str


# ============================================================================
# 服务实例模型
# ============================================================================

@dataclass
class ServiceInstance:
    """服务实例"""
    id: ServiceId
    name: str
    host: str
    port: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    weight: int = 1
    status: ServiceStatus = ServiceStatus.STARTING
    health_check_url: Optional[str] = None
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tags: List[str] = field(default_factory=list)
    version: str = "1.0.0"
    region: str = "default"
    
    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"
    
    @property
    def is_healthy(self) -> bool:
        return self.status == ServiceStatus.HEALTHY
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "metadata": self.metadata,
            "weight": self.weight,
            "status": self.status.value,
            "health_check_url": self.health_check_url,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "tags": self.tags,
            "version": self.version,
            "region": self.region
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ServiceInstance:
        return cls(
            id=data["id"],
            name=data["name"],
            host=data["host"],
            port=data["port"],
            metadata=data.get("metadata", {}),
            weight=data.get("weight", 1),
            status=ServiceStatus(data.get("status", "unknown")),
            health_check_url=data.get("health_check_url"),
            last_heartbeat=datetime.fromisoformat(data.get("last_heartbeat", datetime.now(timezone.utc).isoformat())),
            tags=data.get("tags", []),
            version=data.get("version", "1.0.0"),
            region=data.get("region", "default")
        )


# ============================================================================
# 服务注册与发现
# ============================================================================

class ServiceRegistry(ABC):
    """
    服务注册表抽象
    
    管理服务实例的注册、发现和心跳
    
    DEFECT-007 [P2] 新增使用示例:
    
    示例用法:
        >>> from microservices_framework import InMemoryServiceRegistry, ServiceInstance
        >>> 
        >>> registry = InMemoryServiceRegistry()
        >>> 
        >>> # 注册服务
        >>> instance = ServiceInstance(
        ...     service_name="user-service",
        ...     host="10.0.0.1", port=8080,
        ...     metadata={"version": "1.0.0"}
        ... )
        >>> await registry.register(instance)
        >>> 
        >>> # 发现服务
        >>> instances = await registry.discover("user-service")
        >>> print(instances[0].host)  # "10.0.0.1"
    """
    
    @abstractmethod
    async def register(self, instance: ServiceInstance) -> bool:
        """注册服务实例"""
        pass
    
    @abstractmethod
    async def deregister(self, service_id: ServiceId) -> bool:
        """注销服务实例"""
        pass
    
    @abstractmethod
    async def discover(self, service_name: str) -> List[ServiceInstance]:
        """发现服务实例"""
        pass
    
    @abstractmethod
    async def heartbeat(self, service_id: ServiceId) -> bool:
        """发送心跳"""
        pass
    
    @abstractmethod
    async def watch(self, service_name: str, callback: Callable[[List[ServiceInstance]], None]) -> None:
        """监视服务变化"""
        pass


class InMemoryServiceRegistry(ServiceRegistry):
    """内存服务注册表实现"""
    
    def __init__(self, heartbeat_timeout: int = 30):
        self._services: Dict[ServiceId, ServiceInstance] = {}
        self._service_index: Dict[str, Set[ServiceId]] = {}
        self._heartbeat_timeout = heartbeat_timeout
        self._watchers: Dict[str, List[Callable]] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """启动清理任务"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop(self) -> None:
        """停止清理任务"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def register(self, instance: ServiceInstance) -> bool:
        """注册服务"""
        async with self._lock:
            self._services[instance.id] = instance
            
            if instance.name not in self._service_index:
                self._service_index[instance.name] = set()
            self._service_index[instance.name].add(instance.id)
        
        logger.info(f"Registered service: {instance.name} ({instance.id})")
        
        # 通知观察者
        await self._notify_watchers(instance.name)
        return True
    
    async def deregister(self, service_id: ServiceId) -> bool:
        """注销服务"""
        async with self._lock:
            instance = self._services.pop(service_id, None)
            if instance:
                self._service_index[instance.name].discard(service_id)
        
        if instance:
            logger.info(f"Deregistered service: {instance.name} ({service_id})")
            await self._notify_watchers(instance.name)
            return True
        return False
    
    async def discover(self, service_name: str) -> List[ServiceInstance]:
        """发现服务"""
        async with self._lock:
            service_ids = self._service_index.get(service_name, set())
            instances = [
                self._services[sid] for sid in service_ids
                if sid in self._services and self._services[sid].is_healthy
            ]
        return instances
    
    async def heartbeat(self, service_id: ServiceId) -> bool:
        """更新心跳"""
        async with self._lock:
            instance = self._services.get(service_id)
            if instance:
                instance.last_heartbeat = datetime.now(timezone.utc)
                if instance.status != ServiceStatus.HEALTHY:
                    instance.status = ServiceStatus.HEALTHY
                return True
        return False
    
    async def watch(self, service_name: str, callback: Callable[[List[ServiceInstance]], None]) -> None:
        """添加观察者"""
        if service_name not in self._watchers:
            self._watchers[service_name] = []
        self._watchers[service_name].append(callback)
    
    async def _notify_watchers(self, service_name: str) -> None:
        """通知观察者"""
        instances = await self.discover(service_name)
        for callback in self._watchers.get(service_name, []):
            try:
                callback(instances)
            except Exception as e:
                logger.exception(f"Watcher callback error: {e}")
    
    async def _cleanup_loop(self) -> None:
        """清理过期服务的循环"""
        while True:
            try:
                await asyncio.sleep(10)
                await self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Cleanup error: {e}")
    
    async def _cleanup_expired(self) -> None:
        """清理过期服务"""
        now = datetime.now(timezone.utc)
        expired = []
        
        async with self._lock:
            for sid, instance in self._services.items():
                if (now - instance.last_heartbeat).seconds > self._heartbeat_timeout:
                    expired.append(sid)
            
            for sid in expired:
                instance = self._services.pop(sid, None)
                if instance:
                    self._service_index[instance.name].discard(sid)
                    logger.warning(f"Service expired: {instance.name} ({sid})")


class ConsulServiceRegistry(ServiceRegistry):
    """
    Consul服务注册表实现
    
    生产级服务发现方案
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8500,
        token: Optional[str] = None,
        datacenter: str = "dc1"
    ):
        self.host = host
        self.port = port
        self.token = token
        self.datacenter = datacenter
        self._base_url = f"http://{host}:{port}/v1"
        self._session: Optional[Any] = None
    
    async def connect(self) -> None:
        """连接Consul"""
        import aiohttp
        self._session = aiohttp.ClientSession()
        logger.info(f"Connected to Consul at {self.host}:{self.port}")
    
    async def disconnect(self) -> None:
        """断开Consul"""
        if self._session:
            await self._session.close()
    
    async def register(self, instance: ServiceInstance) -> bool:
        """注册到Consul"""
        import aiohttp
        
        url = f"{self._base_url}/agent/service/register"
        
        payload = {
            "ID": instance.id,
            "Name": instance.name,
            "Tags": instance.tags,
            "Address": instance.host,
            "Port": instance.port,
            "Meta": instance.metadata,
            "Check": {
                "HTTP": instance.health_check_url or f"http://{instance.address}/health",
                "Interval": "10s",
                "Timeout": "5s"
            }
        }
        
        headers = {}
        if self.token:
            headers["X-Consul-Token"] = self.token
        
        try:
            async with self._session.put(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    logger.info(f"Registered service in Consul: {instance.name}")
                    return True
                else:
                    logger.error(f"Consul registration failed: {resp.status}")
                    return False
        except Exception as e:
            logger.exception(f"Consul registration error: {e}")
            return False
    
    async def deregister(self, service_id: ServiceId) -> bool:
        """从Consul注销"""
        url = f"{self._base_url}/agent/service/deregister/{service_id}"
        
        headers = {}
        if self.token:
            headers["X-Consul-Token"] = self.token
        
        try:
            async with self._session.put(url, headers=headers) as resp:
                return resp.status == 200
        except Exception as e:
            logger.exception(f"Consul deregistration error: {e}")
            return False
    
    async def discover(self, service_name: str) -> List[ServiceInstance]:
        """从Consul发现服务"""
        url = f"{self._base_url}/health/service/{service_name}"
        
        headers = {}
        if self.token:
            headers["X-Consul-Token"] = self.token
        
        try:
            async with self._session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    instances = []
                    for item in data:
                        service = item["Service"]
                        checks = item.get("Checks", [])
                        
                        # 检查健康状态
                        healthy = all(c["Status"] == "passing" for c in checks)
                        
                        instance = ServiceInstance(
                            id=service["ID"],
                            name=service["Service"],
                            host=service["Address"],
                            port=service["Port"],
                            metadata=service.get("Meta", {}),
                            tags=service.get("Tags", []),
                            status=ServiceStatus.HEALTHY if healthy else ServiceStatus.UNHEALTHY
                        )
                        instances.append(instance)
                    return instances
                return []
        except Exception as e:
            logger.exception(f"Consul discovery error: {e}")
            return []
    
    async def heartbeat(self, service_id: ServiceId) -> bool:
        """Consul使用TTL检查，不需要显式心跳"""
        return True
    
    async def watch(self, service_name: str, callback: Callable[[List[ServiceInstance]], None]) -> None:
        """监视服务变化（使用长轮询）"""
        # 实现长轮询逻辑
        pass


# ============================================================================
# 负载均衡器
# ============================================================================

class LoadBalancer:
    """
    负载均衡器
    
    支持多种负载均衡策略
    """
    
    def __init__(
        self,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        registry: Optional[ServiceRegistry] = None,
        failover_config: Optional[FailoverConfig] = None,
    ):
        self.strategy = strategy
        self.registry = registry
        self.failover_config = failover_config or FailoverConfig()
        self._round_robin_counters: Dict[str, int] = {}
        self._connection_counts: Dict[ServiceId, int] = defaultdict(int)
        self._virtual_nodes = 150  # 一致性哈希虚拟节点数
        
        # 故障转移状态跟踪
        self._unhealthy_instances: Dict[str, Set[ServiceId]] = defaultdict(set)
        self._last_health_check: Dict[str, float] = {}
        self._instance_retry_count: Dict[ServiceId, int] = defaultdict(int)
        self._lock = asyncio.Lock()
    
    async def select(
        self,
        service_name: str,
        context: Optional[Dict[str, Any]] = None,
        exclude_instances: Optional[Set[ServiceId]] = None,
    ) -> Optional[ServiceInstance]:
        """
        选择服务实例
        
        Args:
            service_name: 服务名称
            context: 上下文信息（用于一致性哈希）
            exclude_instances: 要排除的实例ID集合
            
        Returns:
            选中的服务实例
        """
        if not self.registry:
            return None
        
        instances = await self.registry.discover(service_name)
        
        if not instances:
            return None
        
        # 过滤掉不健康的实例和排除的实例
        exclude_instances = exclude_instances or set()
        healthy_instances = [
            inst for inst in instances
            if inst.id not in self._unhealthy_instances.get(service_name, set())
            and inst.id not in exclude_instances
            and inst.is_healthy
        ]
        
        # 如果没有健康实例，尝试重置不健康状态并重试
        if not healthy_instances:
            logger.warning(f"No healthy instances for {service_name}, attempting recovery")
            await self._recover_unhealthy_instances(service_name, instances)
            healthy_instances = [
                inst for inst in instances
                if inst.id not in self._unhealthy_instances.get(service_name, set())
                and inst.id not in exclude_instances
                and inst.is_healthy
            ]
        
        if not healthy_instances:
            logger.warning(f"No available instances for {service_name} after recovery attempt")
            return None
        
        if self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
            return self._round_robin(service_name, healthy_instances)
        elif self.strategy == LoadBalanceStrategy.WEIGHTED:
            return self._weighted_random(healthy_instances)
        elif self.strategy == LoadBalanceStrategy.CONSISTENT_HASH:
            return self._consistent_hash(healthy_instances, context)
        elif self.strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy_instances)
        elif self.strategy == LoadBalanceStrategy.RANDOM:
            return random.choice(healthy_instances)
        
        return healthy_instances[0]
    
    async def select_with_failover(
        self,
        service_name: str,
        context: Optional[Dict[str, Any]] = None,
        max_attempts: Optional[int] = None,
    ) -> Tuple[Optional[ServiceInstance], List[ServiceId]]:
        """
        带故障转移的实例选择
        
        Args:
            service_name: 服务名称
            context: 上下文信息
            max_attempts: 最大尝试次数
            
        Returns:
            (选中的实例, 尝试过的实例ID列表)
        """
        max_attempts = max_attempts or self.failover_config.max_retries
        tried_instances: List[ServiceId] = []
        exclude_set: Set[ServiceId] = set()
        
        for attempt in range(max_attempts):
            instance = await self.select(
                service_name,
                context=context,
                exclude_instances=exclude_set
            )
            
            if instance is None:
                break
            
            tried_instances.append(instance.id)
            exclude_set.add(instance.id)
            
            # 验证实例是否有效
            is_valid = await self._validate_instance(instance)
            if is_valid:
                return instance, tried_instances
            
            # 标记为不健康
            await self.mark_instance_unhealthy(service_name, instance.id)
        
        return None, tried_instances
    
    async def _validate_instance(self, instance: ServiceInstance) -> bool:
        """
        验证实例是否有效
        
        Args:
            instance: 服务实例
            
        Returns:
            实例是否有效
        """
        # 基本健康检查
        if not instance.is_healthy:
            return False
        
        # 检查心跳是否过期
        now = datetime.now(timezone.utc)
        heartbeat_age = (now - instance.last_heartbeat).total_seconds()
        if heartbeat_age > self.failover_config.health_check_interval * 3:
            logger.warning(
                f"Instance {instance.id} heartbeat expired: {heartbeat_age:.1f}s"
            )
            return False
        
        return True
    
    async def _recover_unhealthy_instances(
        self,
        service_name: str,
        instances: List[ServiceInstance]
    ) -> None:
        """
        尝试恢复不健康的实例
        
        Args:
            service_name: 服务名称
            instances: 所有实例列表
        """
        now = time.time()
        
        # 检查是否需要进行恢复检查
        last_check = self._last_health_check.get(service_name, 0)
        if now - last_check < self.failover_config.health_check_interval:
            return
        
        self._last_health_check[service_name] = now
        
        unhealthy_set = self._unhealthy_instances.get(service_name, set())
        if not unhealthy_set:
            return
        
        logger.info(f"Attempting to recover {len(unhealthy_set)} unhealthy instances for {service_name}")
        
        # 检查每个不健康实例是否已恢复
        recovered = set()
        for instance in instances:
            if instance.id in unhealthy_set:
                if instance.is_healthy:
                    # 重置重试计数
                    self._instance_retry_count[instance.id] = 0
                    recovered.add(instance.id)
                    logger.info(f"Instance {instance.id} has recovered")
        
        # 更新不健康实例集合
        self._unhealthy_instances[service_name] = unhealthy_set - recovered
    
    async def mark_instance_unhealthy(
        self,
        service_name: str,
        instance_id: ServiceId
    ) -> None:
        """
        标记实例为不健康
        
        Args:
            service_name: 服务名称
            instance_id: 实例ID
        """
        async with self._lock:
            self._unhealthy_instances[service_name].add(instance_id)
            self._instance_retry_count[instance_id] += 1
            
            logger.warning(
                f"Marked instance {instance_id} as unhealthy for service {service_name} "
                f"(retry count: {self._instance_retry_count[instance_id]})"
            )
    
    async def mark_instance_healthy(
        self,
        service_name: str,
        instance_id: ServiceId
    ) -> None:
        """
        标记实例为健康
        
        Args:
            service_name: 服务名称
            instance_id: 实例ID
        """
        async with self._lock:
            self._unhealthy_instances[service_name].discard(instance_id)
            self._instance_retry_count[instance_id] = 0
            logger.info(f"Marked instance {instance_id} as healthy for service {service_name}")
    
    def get_instance_status(self, service_name: str) -> Dict[str, Any]:
        """
        获取服务实例状态
        
        Args:
            service_name: 服务名称
            
        Returns:
            实例状态信息
        """
        return {
            "unhealthy_instances": list(self._unhealthy_instances.get(service_name, set())),
            "retry_counts": {
                inst_id: self._instance_retry_count[inst_id]
                for inst_id in self._unhealthy_instances.get(service_name, set())
            },
            "last_health_check": self._last_health_check.get(service_name, 0),
        }
    
    def _round_robin(
        self,
        service_name: str,
        instances: List[ServiceInstance]
    ) -> ServiceInstance:
        """轮询策略"""
        counter = self._round_robin_counters.get(service_name, 0)
        instance = instances[counter % len(instances)]
        self._round_robin_counters[service_name] = counter + 1
        return instance
    
    def _weighted_random(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """加权随机策略"""
        total_weight = sum(i.weight for i in instances)
        r = random.uniform(0, total_weight)
        
        current = 0
        for instance in instances:
            current += instance.weight
            if r <= current:
                return instance
        
        return instances[-1]
    
    def _consistent_hash(
        self,
        instances: List[ServiceInstance],
        context: Optional[Dict[str, Any]]
    ) -> ServiceInstance:
        """一致性哈希策略"""
        if not context:
            return instances[0]
        
        # 构建哈希环
        hash_ring = []
        for instance in instances:
            for i in range(self._virtual_nodes):
                key = f"{instance.id}:{i}"
                hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
                hash_ring.append((hash_val, instance))
        
        hash_ring.sort(key=lambda x: x[0])
        
        # 计算请求哈希
        context_str = json.dumps(context, sort_keys=True)
        request_hash = int(hashlib.md5(context_str.encode()).hexdigest(), 16)
        
        # 找到第一个大于等于请求哈希的节点
        for hash_val, instance in hash_ring:
            if hash_val >= request_hash:
                return instance
        
        # 回绕到第一个节点
        return hash_ring[0][1]
    
    def _least_connections(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """最少连接策略"""
        min_connections = float('inf')
        selected = instances[0]
        
        for instance in instances:
            connections = self._connection_counts[instance.id]
            if connections < min_connections:
                min_connections = connections
                selected = instance
        
        return selected
    
    def record_connection_start(self, instance: ServiceInstance) -> None:
        """记录连接开始"""
        self._connection_counts[instance.id] += 1
    
    def record_connection_end(self, instance: ServiceInstance) -> None:
        """记录连接结束"""
        self._connection_counts[instance.id] = max(
            0, self._connection_counts[instance.id] - 1
        )


# ============================================================================
# 熔断器
# ============================================================================

@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    failure_threshold: int = 5           # 失败阈值
    success_threshold: int = 3           # 成功阈值（半开状态）
    timeout: float = 60.0                # 熔断超时时间（秒）
    half_open_max_calls: int = 3         # 半开状态最大调用数
    error_codes: Set[int] = field(default_factory=lambda: {500, 502, 503, 504})


class CircuitBreaker:
    """
    熔断器实现
    
    实现熔断器模式，防止级联故障
    """
    
    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig = None
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._half_open_calls = 0
        self._lock = asyncio.Lock()
    
    @property
    def state(self) -> CircuitState:
        return self._state
    
    async def call(self, func: Callable[..., Coroutine[Any, Any, T]], *args, **kwargs) -> T:
        """
        执行受熔断器保护的调用
        
        Args:
            func: 要执行的异步函数
            *args, **kwargs: 函数参数
            
        Returns:
            函数执行结果
            
        Raises:
            CircuitBreakerOpen: 熔断器打开时抛出
        """
        async with self._lock:
            await self._update_state()
            
            if self._state == CircuitState.OPEN:
                raise CircuitBreakerOpen(f"Circuit breaker '{self.name}' is OPEN")
            
            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    raise CircuitBreakerOpen(f"Circuit breaker '{self.name}' half-open limit reached")
                self._half_open_calls += 1
        
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure(e)
            raise
    
    async def _update_state(self) -> None:
        """更新熔断器状态"""
        if self._state == CircuitState.OPEN:
            # 检查是否应该进入半开状态
            if self._last_failure_time:
                elapsed = (datetime.now(timezone.utc) - self._last_failure_time).total_seconds()
                if elapsed >= self.config.timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._success_count = 0
                    logger.info(f"Circuit breaker '{self.name}' moved to HALF_OPEN")
    
    async def _on_success(self) -> None:
        """处理成功调用"""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._half_open_calls = 0
                    logger.info(f"Circuit breaker '{self.name}' moved to CLOSED")
            else:
                self._failure_count = 0
    
    async def _on_failure(self, error: Exception) -> None:
        """处理失败调用"""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = datetime.now(timezone.utc)
            
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                logger.warning(f"Circuit breaker '{self.name}' moved to OPEN (half-open failure)")
            elif self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN
                logger.warning(f"Circuit breaker '{self.name}' moved to OPEN")
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取熔断器指标"""
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time.isoformat() if self._last_failure_time else None
        }


class CircuitBreakerOpen(Exception):
    """熔断器打开异常"""
    pass


class CircuitBreakerRegistry:
    """熔断器注册表"""
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()
    
    async def get_or_create(
        self,
        name: str,
        config: CircuitBreakerConfig = None
    ) -> CircuitBreaker:
        """获取或创建熔断器"""
        async with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, config)
            return self._breakers[name]
    
    def get(self, name: str) -> Optional[CircuitBreaker]:
        """获取熔断器"""
        return self._breakers.get(name)
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """获取所有熔断器指标"""
        return {name: breaker.get_metrics() for name, breaker in self._breakers.items()}


# ============================================================================
# 限流器
# ============================================================================

class RateLimiter(ABC):
    """限流器抽象基类"""
    
    @abstractmethod
    async def acquire(self, key: str = "default", tokens: int = 1) -> bool:
        """尝试获取令牌"""
        pass
    
    @abstractmethod
    def get_metrics(self, key: str = "default") -> Dict[str, Any]:
        """获取限流器指标"""
        pass


class TokenBucketRateLimiter(RateLimiter):
    """
    令牌桶限流器
    
    实现平滑限流，允许突发流量
    """
    
    def __init__(
        self,
        rate: float = 10.0,      # 每秒生成的令牌数
        burst: int = 100         # 桶容量
    ):
        self.rate = rate
        self.burst = burst
        self._buckets: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def acquire(self, key: str = "default", tokens: int = 1) -> bool:
        """获取令牌"""
        async with self._lock:
            now = time.time()
            
            if key not in self._buckets:
                self._buckets[key] = {
                    "tokens": self.burst,
                    "last_update": now
                }
            
            bucket = self._buckets[key]
            
            # 计算新增的令牌
            elapsed = now - bucket["last_update"]
            new_tokens = elapsed * self.rate
            bucket["tokens"] = min(self.burst, bucket["tokens"] + new_tokens)
            bucket["last_update"] = now
            
            # 尝试获取令牌
            if bucket["tokens"] >= tokens:
                bucket["tokens"] -= tokens
                return True
            
            return False
    
    def get_metrics(self, key: str = "default") -> Dict[str, Any]:
        """获取指标"""
        bucket = self._buckets.get(key, {"tokens": self.burst})
        return {
            "available_tokens": bucket.get("tokens", 0),
            "rate": self.rate,
            "burst": self.burst
        }


class SlidingWindowRateLimiter(RateLimiter):
    """
    滑动窗口限流器
    
    精确控制时间窗口内的请求数
    """
    
    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._windows: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
    
    async def acquire(self, key: str = "default", tokens: int = 1) -> bool:
        """获取许可"""
        async with self._lock:
            now = time.time()
            
            if key not in self._windows:
                self._windows[key] = deque()
            
            window = self._windows[key]
            
            # 移除过期的时间戳
            cutoff = now - self.window_seconds
            while window and window[0] < cutoff:
                window.popleft()
            
            # 检查是否超过限制
            if len(window) + tokens > self.max_requests:
                return False
            
            # 添加新的时间戳
            for _ in range(tokens):
                window.append(now)
            
            return True
    
    def get_metrics(self, key: str = "default") -> Dict[str, Any]:
        """获取指标"""
        window = self._windows.get(key, deque())
        now = time.time()
        cutoff = now - self.window_seconds
        
        # 统计有效请求
        valid_count = sum(1 for ts in window if ts >= cutoff)
        
        return {
            "current_requests": valid_count,
            "max_requests": self.max_requests,
            "window_seconds": self.window_seconds,
            "remaining": self.max_requests - valid_count
        }


# ============================================================================
# 服务客户端
# ============================================================================

class ServiceClient:
    """
    服务客户端
    
    集成负载均衡、熔断和限流的服务调用客户端
    """
    
    def __init__(
        self,
        registry: ServiceRegistry,
        load_balancer: LoadBalancer = None,
        circuit_breaker_registry: CircuitBreakerRegistry = None,
        rate_limiter: RateLimiter = None,
        failover_config: FailoverConfig = None,
    ):
        self.registry = registry
        self.load_balancer = load_balancer or LoadBalancer(
            registry=registry,
            failover_config=failover_config
        )
        self.circuit_breakers = circuit_breaker_registry or CircuitBreakerRegistry()
        self.rate_limiter = rate_limiter
        self.failover_config = failover_config or FailoverConfig()
        self._session: Optional[Any] = None
    
    async def connect(self) -> None:
        """初始化HTTP会话"""
        import aiohttp
        self._session = aiohttp.ClientSession()
    
    async def disconnect(self) -> None:
        """关闭HTTP会话"""
        if self._session:
            await self._session.close()
    
    async def call(
        self,
        service_name: str,
        method: str,
        path: str,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        调用服务
        
        Args:
            service_name: 服务名称
            method: HTTP方法
            path: 请求路径
            context: 负载均衡上下文
            **kwargs: 其他请求参数
            
        Returns:
            响应数据
        """
        # 限流检查
        if self.rate_limiter:
            if not await self.rate_limiter.acquire(service_name):
                raise RateLimitExceeded(f"Rate limit exceeded for service: {service_name}")
        
        # 选择服务实例
        instance = await self.load_balancer.select(service_name, context)
        if not instance:
            raise ServiceNotFound(f"No available instances for service: {service_name}")
        
        # 获取熔断器
        breaker = await self.circuit_breakers.get_or_create(
            f"{service_name}:{instance.id}"
        )
        
        # 记录连接
        self.load_balancer.record_connection_start(instance)
        
        try:
            # 执行调用
            result = await breaker.call(
                self._do_request,
                instance,
                method,
                path,
                **kwargs
            )
            return result
        finally:
            self.load_balancer.record_connection_end(instance)
    
    async def call_with_failover(
        self,
        service_name: str,
        method: str,
        path: str,
        context: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        带故障转移的服务调用
        
        当服务调用失败时，自动尝试其他实例
        
        Args:
            service_name: 服务名称
            method: HTTP方法
            path: 请求路径
            context: 负载均衡上下文
            timeout: 总超时时间
            **kwargs: 其他请求参数
            
        Returns:
            响应数据
            
        Raises:
            ServiceFailoverError: 所有重试都失败时抛出
            NoHealthyInstanceError: 没有健康实例时抛出
        """
        timeout = timeout or self.failover_config.failover_timeout
        start_time = time.time()
        
        # 限流检查
        if self.rate_limiter:
            if not await self.rate_limiter.acquire(service_name):
                raise RateLimitExceeded(f"Rate limit exceeded for service: {service_name}")
        
        tried_instances: List[ServiceId] = []
        last_error: Optional[Exception] = None
        
        # 使用指数退避重试
        for retry in range(self.failover_config.max_retries):
            # 检查总超时
            if time.time() - start_time > timeout:
                raise ServiceFailoverError(
                    f"Failover timeout after {timeout}s for service {service_name}",
                    service_name=service_name,
                    retry_count=retry,
                    tried_instances=tried_instances,
                    last_error=last_error
                )
            
            # 计算重试延迟
            if retry > 0:
                delay = self._calculate_retry_delay(retry - 1)
                await asyncio.sleep(delay)
            
            try:
                # 使用带故障转移的实例选择
                instance, tried = await self.load_balancer.select_with_failover(
                    service_name,
                    context=context,
                    max_attempts=self.failover_config.max_retries - retry
                )
                tried_instances.extend(tried)
                
                if instance is None:
                    raise NoHealthyInstanceError(
                        service_name,
                        f"All instances exhausted after {len(tried_instances)} attempts"
                    )
                
                # 获取熔断器
                breaker = await self.circuit_breakers.get_or_create(
                    f"{service_name}:{instance.id}"
                )
                
                # 记录连接
                self.load_balancer.record_connection_start(instance)
                
                try:
                    # 执行调用
                    result = await breaker.call(
                        self._do_request,
                        instance,
                        method,
                        path,
                        **kwargs
                    )
                    
                    # 成功后标记实例为健康
                    await self.load_balancer.mark_instance_healthy(service_name, instance.id)
                    
                    return result
                    
                finally:
                    self.load_balancer.record_connection_end(instance)
                    
            except (CircuitBreakerOpen, NoHealthyInstanceError) as e:
                last_error = e
                logger.warning(
                    f"Service call attempt {retry + 1} failed for {service_name}: {e}"
                )
                continue
                
            except Exception as e:
                last_error = e
                logger.error(
                    f"Unexpected error in service call to {service_name}: {e}"
                )
                
                # 标记当前实例为不健康
                if instance:
                    await self.load_balancer.mark_instance_unhealthy(service_name, instance.id)
                
                continue
        
        # 所有重试都失败
        raise ServiceFailoverError(
            f"All failover attempts failed for service {service_name}",
            service_name=service_name,
            retry_count=self.failover_config.max_retries,
            tried_instances=tried_instances,
            last_error=last_error
        )
    
    def _calculate_retry_delay(self, retry_count: int) -> float:
        """
        计算指数退避重试延迟
        
        Args:
            retry_count: 当前重试次数
            
        Returns:
            重试延迟时间(秒)
        """
        delay = self.failover_config.initial_retry_delay * (
            self.failover_config.retry_multiplier ** retry_count
        )
        return min(delay, self.failover_config.max_retry_delay)
    
    async def _do_request(
        self,
        instance: ServiceInstance,
        method: str,
        path: str,
        **kwargs
    ) -> Dict[str, Any]:
        """执行HTTP请求"""
        url = f"http://{instance.address}{path}"
        
        try:
            async with self._session.request(method, url, **kwargs) as resp:
                if resp.status >= 500:
                    resp.raise_for_status()
                
                return {
                    "status": resp.status,
                    "data": await resp.json() if resp.content_type == 'application/json' else await resp.text(),
                    "headers": dict(resp.headers)
                }
        except Exception as e:
            logger.exception(f"Request failed: {e}")
            raise
    
    async def get(self, service_name: str, path: str, **kwargs) -> Dict[str, Any]:
        """GET请求"""
        return await self.call(service_name, "GET", path, **kwargs)
    
    async def post(self, service_name: str, path: str, **kwargs) -> Dict[str, Any]:
        """POST请求"""
        return await self.call(service_name, "POST", path, **kwargs)
    
    async def put(self, service_name: str, path: str, **kwargs) -> Dict[str, Any]:
        """PUT请求"""
        return await self.call(service_name, "PUT", path, **kwargs)
    
    async def delete(self, service_name: str, path: str, **kwargs) -> Dict[str, Any]:
        """DELETE请求"""
        return await self.call(service_name, "DELETE", path, **kwargs)


class ServiceNotFound(Exception):
    """服务未找到异常"""
    pass


class RateLimitExceeded(Exception):
    """限流异常"""
    pass


class FailoverConfig:
    """
    故障转移配置
    
    用于配置服务调用时的故障转移行为
    """
    def __init__(
        self,
        max_retries: int = 3,
        initial_retry_delay: float = 0.1,
        max_retry_delay: float = 5.0,
        retry_multiplier: float = 2.0,
        failover_timeout: float = 30.0,
        candidate_selection_timeout: float = 1.0,
        health_check_interval: float = 5.0,
        enable_circuit_breaker: bool = True,
    ):
        self.max_retries = max_retries
        self.initial_retry_delay = initial_retry_delay
        self.max_retry_delay = max_retry_delay
        self.retry_multiplier = retry_multiplier
        self.failover_timeout = failover_timeout
        self.candidate_selection_timeout = candidate_selection_timeout
        self.health_check_interval = health_check_interval
        self.enable_circuit_breaker = enable_circuit_breaker


class ServiceFailoverError(Exception):
    """服务故障转移错误"""
    def __init__(
        self,
        message: str,
        service_name: str,
        retry_count: int = 0,
        tried_instances: Optional[List[str]] = None,
        last_error: Optional[Exception] = None
    ):
        self.service_name = service_name
        self.retry_count = retry_count
        self.tried_instances = tried_instances or []
        self.last_error = last_error
        super().__init__(message)


class NoHealthyInstanceError(Exception):
    """没有健康实例可用错误"""
    def __init__(self, service_name: str, reason: str = ""):
        self.service_name = service_name
        self.reason = reason
        super().__init__(f"No healthy instances available for service '{service_name}': {reason}")


# ============================================================================
# 健康检查
# ============================================================================

@dataclass
class HealthStatus:
    """健康状态"""
    status: ServiceStatus
    checks: Dict[str, bool]
    details: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class HealthChecker:
    """
    健康检查器
    
    执行各种健康检查并报告状态
    """
    
    def __init__(self):
        self._checks: Dict[str, Callable[[], Coroutine[Any, Any, Tuple[bool, Any]]]] = {}
    
    def register_check(
        self,
        name: str,
        check_func: Callable[[], Coroutine[Any, Any, Tuple[bool, Any]]]
    ) -> None:
        """注册健康检查"""
        self._checks[name] = check_func
    
    async def check(self) -> HealthStatus:
        """执行所有健康检查"""
        checks = {}
        details = {}
        
        for name, check_func in self._checks.items():
            try:
                healthy, detail = await check_func()
                checks[name] = healthy
                details[name] = detail
            except Exception as e:
                checks[name] = False
                details[name] = str(e)
        
        overall_status = ServiceStatus.HEALTHY if all(checks.values()) else ServiceStatus.UNHEALTHY
        
        return HealthStatus(
            status=overall_status,
            checks=checks,
            details=details
        )


# ============================================================================
# 优雅关闭管理
# ============================================================================

class GracefulShutdownManager:
    """
    优雅关闭管理器
    
    处理SIGTERM信号，确保服务优雅关闭
    """
    
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._handlers: List[Callable[[], Coroutine[Any, Any, None]]] = []
        self._shutdown_event = asyncio.Event()
        self._is_shutting_down = False
    
    def register_handler(self, handler: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """注册关闭处理器"""
        self._handlers.append(handler)
    
    def setup_signal_handlers(self) -> None:
        """设置信号处理器"""
        try:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError:
            # Windows不支持add_signal_handler
            pass
    
    async def shutdown(self) -> None:
        """执行关闭流程"""
        if self._is_shutting_down:
            return
        
        self._is_shutting_down = True
        logger.info("Starting graceful shutdown...")
        
        # 设置超时
        try:
            await asyncio.wait_for(self._do_shutdown(), timeout=self.timeout)
        except asyncio.TimeoutError:
            logger.warning("Graceful shutdown timed out")
        
        self._shutdown_event.set()
        logger.info("Graceful shutdown completed")
    
    async def _do_shutdown(self) -> None:
        """执行关闭处理器"""
        for handler in self._handlers:
            try:
                await handler()
            except Exception as e:
                logger.exception(f"Shutdown handler error: {e}")
    
    async def wait_for_shutdown(self) -> None:
        """等待关闭信号"""
        await self._shutdown_event.wait()
    
    @property
    def is_shutting_down(self) -> bool:
        return self._is_shutting_down


# ============================================================================
# 微服务应用
# ============================================================================

class MicroserviceApp:
    """
    微服务应用
    
    整合所有微服务组件，提供完整的应用生命周期管理
    """
    
    def __init__(
        self,
        name: str,
        version: str = "1.0.0",
        host: str = "0.0.0.0",
        port: int = 8000,
        registry: Optional[ServiceRegistry] = None
    ):
        self.name = name
        self.version = version
        self.host = host
        self.port = port
        self.registry = registry
        self.instance_id = str(uuid.uuid4())
        self.health_checker = HealthChecker()
        self.shutdown_manager = GracefulShutdownManager()
        self._server: Optional[Any] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """启动服务"""
        logger.info(f"Starting service: {self.name} v{self.version}")
        
        # 设置信号处理器
        self.shutdown_manager.setup_signal_handlers()
        
        # 注册到服务注册表
        if self.registry:
            instance = ServiceInstance(
                id=self.instance_id,
                name=self.name,
                host=self.host,
                port=self.port,
                health_check_url=f"http://{self.host}:{self.port}/health",
                version=self.version,
                status=ServiceStatus.HEALTHY
            )
            await self.registry.register(instance)
            
            # 启动心跳
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        logger.info(f"Service {self.name} started at {self.host}:{self.port}")
    
    async def stop(self) -> None:
        """停止服务"""
        logger.info(f"Stopping service: {self.name}")
        
        # 取消心跳任务
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # 从注册表注销
        if self.registry:
            await self.registry.deregister(self.instance_id)
        
        logger.info(f"Service {self.name} stopped")
    
    async def _heartbeat_loop(self) -> None:
        """心跳循环"""
        while True:
            try:
                await asyncio.sleep(10)
                if self.registry:
                    await self.registry.heartbeat(self.instance_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Heartbeat error: {e}")
    
    def register_health_check(
        self,
        name: str,
        check_func: Callable[[], Coroutine[Any, Any, Tuple[bool, Any]]]
    ) -> None:
        """注册健康检查"""
        self.health_checker.register_check(name, check_func)
    
    def register_shutdown_handler(
        self,
        handler: Callable[[], Coroutine[Any, Any, None]]
    ) -> None:
        """注册关闭处理器"""
        self.shutdown_manager.register_handler(handler)
    
    async def get_health(self) -> HealthStatus:
        """获取健康状态"""
        return await self.health_checker.check()


# ============================================================================
# 使用示例
# ============================================================================

async def example_usage():
    """使用示例"""
    # 创建服务注册表
    registry = InMemoryServiceRegistry()
    await registry.start()
    
    try:
        # 创建负载均衡器
        load_balancer = LoadBalancer(
            strategy=LoadBalanceStrategy.ROUND_ROBIN,
            registry=registry
        )
        
        # 创建熔断器注册表
        circuit_breakers = CircuitBreakerRegistry()
        
        # 创建限流器
        rate_limiter = TokenBucketRateLimiter(rate=100, burst=200)
        
        # 创建服务客户端
        client = ServiceClient(
            registry=registry,
            load_balancer=load_balancer,
            circuit_breaker_registry=circuit_breakers,
            rate_limiter=rate_limiter
        )
        await client.connect()
        
        # 注册一些服务实例
        for i in range(3):
            instance = ServiceInstance(
                id=f"user-service-{i}",
                name="user-service",
                host="localhost",
                port=8001 + i,
                weight=random.randint(1, 5)
            )
            await registry.register(instance)
        
        # 模拟服务调用
        for i in range(10):
            instance = await load_balancer.select("user-service", {"user_id": f"user-{i}"})
            print(f"Selected instance: {instance.id if instance else 'None'}")
        
        # 熔断器测试
        breaker = await circuit_breakers.get_or_create("test-service")
        
        async def failing_operation():
            raise Exception("Simulated failure")
        
        # 触发熔断
        for i in range(10):
            try:
                await breaker.call(failing_operation)
            except CircuitBreakerOpen:
                print(f"Circuit breaker opened at attempt {i}")
                break
            except Exception:
                pass
        
        print(f"Circuit breaker state: {breaker.state.value}")
        print(f"Circuit breaker metrics: {breaker.get_metrics()}")
        
        # 限流器测试
        for i in range(105):
            allowed = await rate_limiter.acquire("api", tokens=1)
            if i >= 100:
                print(f"Request {i}: {'allowed' if allowed else 'denied'}")
        
        print(f"Rate limiter metrics: {rate_limiter.get_metrics('api')}")
        
        # 创建微服务应用
        app = MicroserviceApp(
            name="order-service",
            version="1.0.0",
            port=8080,
            registry=registry
        )
        
        # 注册健康检查
        async def check_database():
            return True, {"connections": 10, "max_connections": 100}
        
        async def check_cache():
            return True, {"hit_rate": 0.95}
        
        app.register_health_check("database", check_database)
        app.register_health_check("cache", check_cache)
        
        # 启动应用
        await app.start()
        
        # 获取健康状态
        health = await app.get_health()
        print(f"Health status: {health.status.value}")
        print(f"Health checks: {health.checks}")
        
        # 模拟运行
        await asyncio.sleep(2)
        
        # 停止应用
        await app.stop()
        
    finally:
        await registry.stop()
        await client.disconnect()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())

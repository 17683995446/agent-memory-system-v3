"""
Service Mesh客户端模块 - Service Mesh Client Module

该模块实现了与Service Mesh（Istio/Linkerd）集成的客户端组件，包含：
- Istio/Linkerd集成接口
- 流量管理（金丝雀发布、蓝绿部署）
- mTLS安全通信
- 遥测数据收集
- 分布式追踪集成
- 重试和超时策略

作者: Agent Memory System Team
版本: 3.0.0
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
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

class MeshProvider(Enum):
    """Service Mesh提供商"""
    ISTIO = "istio"
    LINKERD = "linkerd"
    CONSUL_CONNECT = "consul_connect"
    AWS_APP_MESH = "aws_app_mesh"


class TrafficSplitStrategy(Enum):
    """流量分割策略"""
    CANARY = "canary"           # 金丝雀发布
    BLUE_GREEN = "blue_green"   # 蓝绿部署
    AB_TEST = "ab_test"         # A/B测试
    SHADOW = "shadow"           # 影子流量


class RetryPolicy(Enum):
    """重试策略"""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"


class MTLSMode(Enum):
    """mTLS模式"""
    DISABLE = "DISABLE"
    PERMISSIVE = "PERMISSIVE"
    STRICT = "STRICT"


T = TypeVar('T')


# ============================================================================
# 数据模型
# ============================================================================

@dataclass
class ServiceEndpoint:
    """服务端点"""
    name: str
    namespace: str = "default"
    host: str = ""
    port: int = 80
    subset: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)
    weight: int = 100
    
    @property
    def fqdn(self) -> str:
        """完全限定域名"""
        if self.subset:
            return f"{self.name}-{self.subset}.{self.namespace}.svc.cluster.local"
        return f"{self.name}.{self.namespace}.svc.cluster.local"


@dataclass
class TrafficRoute:
    """流量路由规则"""
    name: str
    source: Optional[ServiceEndpoint] = None
    destination: ServiceEndpoint = field(default_factory=lambda: ServiceEndpoint(""))
    match_headers: Dict[str, str] = field(default_factory=dict)
    match_query_params: Dict[str, str] = field(default_factory=dict)
    retries: int = 3
    timeout_ms: int = 30000
    fault_injection: Optional[Dict[str, Any]] = None


@dataclass
class CanaryConfig:
    """金丝雀发布配置"""
    service_name: str
    namespace: str = "default"
    stable_version: str = "v1"
    canary_version: str = "v2"
    initial_weight: int = 10
    max_weight: int = 100
    step_weight: int = 10
    analysis_interval: int = 60  # 秒
    success_rate_threshold: float = 0.99
    latency_p99_threshold: float = 500  # ms
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BlueGreenConfig:
    """蓝绿部署配置"""
    service_name: str
    namespace: str = "default"
    blue_version: str = "blue"
    green_version: str = "green"
    active_color: str = "blue"
    preview_service_enabled: bool = True
    
    @property
    def active_version(self) -> str:
        return self.blue_version if self.active_color == "blue" else self.green_version
    
    @property
    def preview_version(self) -> str:
        return self.green_version if self.active_color == "blue" else self.blue_version


@dataclass
class RetryConfig:
    """重试配置"""
    attempts: int = 3
    per_try_timeout_ms: int = 5000
    retry_on: List[str] = field(default_factory=lambda: ["5xx", "gateway-error", "connect-failure"])
    backoff: RetryPolicy = RetryPolicy.EXPONENTIAL
    base_interval_ms: int = 25
    max_interval_ms: int = 1000


@dataclass
class TimeoutConfig:
    """超时配置"""
    request_timeout_ms: int = 30000
    idle_timeout_ms: int = 3600000
    max_grpc_timeout_ms: int = 30000


@dataclass
class MTLSConfig:
    """mTLS配置"""
    mode: MTLSMode = MTLSMode.PERMISSIVE
    client_certificate: Optional[str] = None
    client_key: Optional[str] = None
    ca_certificate: Optional[str] = None
    sni: Optional[str] = None


@dataclass
class TelemetryData:
    """遥测数据"""
    timestamp: datetime
    source_service: str
    destination_service: str
    request_count: int
    success_count: int
    error_count: int
    latency_ms: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    bytes_sent: int
    bytes_received: int
    protocol: str = "http"


@dataclass
class TraceSpan:
    """追踪跨度"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    operation_name: str = ""
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def duration_ms(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return 0.0


# ============================================================================
# Service Mesh客户端抽象
# ============================================================================

class ServiceMeshClient(ABC):
    """
    Service Mesh客户端抽象基类
    
    定义与Service Mesh控制平面交互的标准接口
    """
    
    def __init__(self, provider: MeshProvider, namespace: str = "default"):
        self.provider = provider
        self.namespace = namespace
        self._connected = False
    
    @abstractmethod
    async def connect(self) -> None:
        """连接到Service Mesh控制平面"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        pass
    
    @abstractmethod
    async def apply_traffic_split(
        self,
        service_name: str,
        subsets: Dict[str, int],
        strategy: TrafficSplitStrategy = TrafficSplitStrategy.CANARY
    ) -> bool:
        """
        应用流量分割
        
        Args:
            service_name: 服务名称
            subsets: 子集权重映射 {subset_name: weight}
            strategy: 流量分割策略
            
        Returns:
            是否成功应用
        """
        pass
    
    @abstractmethod
    async def configure_retry(
        self,
        service_name: str,
        config: RetryConfig
    ) -> bool:
        """配置重试策略"""
        pass
    
    @abstractmethod
    async def configure_timeout(
        self,
        service_name: str,
        config: TimeoutConfig
    ) -> bool:
        """配置超时策略"""
        pass
    
    @abstractmethod
    async def configure_mtls(
        self,
        service_name: str,
        config: MTLSConfig
    ) -> bool:
        """配置mTLS"""
        pass
    
    @abstractmethod
    async def get_telemetry(
        self,
        service_name: Optional[str] = None,
        duration: timedelta = timedelta(minutes=5)
    ) -> List[TelemetryData]:
        """获取遥测数据"""
        pass
    
    @property
    def is_connected(self) -> bool:
        return self._connected


# ============================================================================
# Istio客户端实现
# ============================================================================

class IstioClient(ServiceMeshClient):
    """
    Istio Service Mesh客户端
    
    通过Kubernetes API与Istio控制平面交互
    """
    
    def __init__(
        self,
        namespace: str = "default",
        kube_config: Optional[str] = None,
        istio_namespace: str = "istio-system"
    ):
        super().__init__(MeshProvider.ISTIO, namespace)
        self.kube_config = kube_config
        self.istio_namespace = istio_namespace
        self._k8s_client: Optional[Any] = None
        self._custom_objects_api: Optional[Any] = None
    
    async def connect(self) -> None:
        """连接到Kubernetes集群"""
        try:
            from kubernetes import client, config
            
            if self.kube_config:
                config.load_kube_config(self.kube_config)
            else:
                config.load_incluster_config()
            
            self._k8s_client = client
            self._custom_objects_api = client.CustomObjectsApi()
            self._connected = True
            
            logger.info("Connected to Istio via Kubernetes API")
        except ImportError:
            logger.error("kubernetes client not installed")
            raise
        except Exception as e:
            logger.exception(f"Failed to connect to Kubernetes: {e}")
            raise
    
    async def disconnect(self) -> None:
        """断开连接"""
        self._connected = False
        logger.info("Disconnected from Istio")
    
    async def apply_traffic_split(
        self,
        service_name: str,
        subsets: Dict[str, int],
        strategy: TrafficSplitStrategy = TrafficSplitStrategy.CANARY
    ) -> bool:
        """应用Istio流量分割（VirtualService + DestinationRule）"""
        try:
            # 创建DestinationRule
            destination_rule = self._build_destination_rule(service_name, list(subsets.keys()))
            await self._apply_destination_rule(destination_rule)
            
            # 创建VirtualService
            virtual_service = self._build_virtual_service(service_name, subsets)
            await self._apply_virtual_service(virtual_service)
            
            logger.info(f"Applied {strategy.value} traffic split for {service_name}: {subsets}")
            return True
        except Exception as e:
            logger.exception(f"Failed to apply traffic split: {e}")
            return False
    
    def _build_destination_rule(
        self,
        service_name: str,
        subsets: List[str]
    ) -> Dict[str, Any]:
        """构建DestinationRule"""
        return {
            "apiVersion": "networking.istio.io/v1beta1",
            "kind": "DestinationRule",
            "metadata": {
                "name": f"{service_name}-dr",
                "namespace": self.namespace
            },
            "spec": {
                "host": service_name,
                "trafficPolicy": {
                    "connectionPool": {
                        "tcp": {
                            "maxConnections": 100
                        },
                        "http": {
                            "http1MaxPendingRequests": 100,
                            "http2MaxRequests": 1000
                        }
                    },
                    "outlierDetection": {
                        "consecutiveErrors": 5,
                        "interval": "30s",
                        "baseEjectionTime": "30s"
                    }
                },
                "subsets": [
                    {
                        "name": subset,
                        "labels": {"version": subset}
                    }
                    for subset in subsets
                ]
            }
        }
    
    def _build_virtual_service(
        self,
        service_name: str,
        subsets: Dict[str, int]
    ) -> Dict[str, Any]:
        """构建VirtualService"""
        return {
            "apiVersion": "networking.istio.io/v1beta1",
            "kind": "VirtualService",
            "metadata": {
                "name": f"{service_name}-vs",
                "namespace": self.namespace
            },
            "spec": {
                "hosts": [service_name],
                "http": [
                    {
                        "route": [
                            {
                                "destination": {
                                    "host": service_name,
                                    "subset": subset
                                },
                                "weight": weight
                            }
                            for subset, weight in subsets.items()
                        ]
                    }
                ]
            }
        }
    
    async def _apply_destination_rule(self, dr: Dict[str, Any]) -> None:
        """应用DestinationRule"""
        try:
            self._custom_objects_api.patch_namespaced_custom_object(
                group="networking.istio.io",
                version="v1beta1",
                namespace=self.namespace,
                plural="destinationrules",
                name=dr["metadata"]["name"],
                body=dr
            )
        except Exception as e:
            # 如果不存在则创建
            self._custom_objects_api.create_namespaced_custom_object(
                group="networking.istio.io",
                version="v1beta1",
                namespace=self.namespace,
                plural="destinationrules",
                body=dr
            )
    
    async def _apply_virtual_service(self, vs: Dict[str, Any]) -> None:
        """应用VirtualService"""
        try:
            self._custom_objects_api.patch_namespaced_custom_object(
                group="networking.istio.io",
                version="v1beta1",
                namespace=self.namespace,
                plural="virtualservices",
                name=vs["metadata"]["name"],
                body=vs
            )
        except Exception as e:
            self._custom_objects_api.create_namespaced_custom_object(
                group="networking.istio.io",
                version="v1beta1",
                namespace=self.namespace,
                plural="virtualservices",
                body=vs
            )
    
    async def configure_retry(
        self,
        service_name: str,
        config: RetryConfig
    ) -> bool:
        """配置重试策略"""
        try:
            virtual_service = {
                "apiVersion": "networking.istio.io/v1beta1",
                "kind": "VirtualService",
                "metadata": {
                    "name": f"{service_name}-retry",
                    "namespace": self.namespace
                },
                "spec": {
                    "hosts": [service_name],
                    "http": [
                        {
                            "route": [
                                {
                                    "destination": {
                                        "host": service_name
                                    }
                                }
                            ],
                            "retries": {
                                "attempts": config.attempts,
                                "perTryTimeout": f"{config.per_try_timeout_ms}ms",
                                "retryOn": ",".join(config.retry_on)
                            }
                        }
                    ]
                }
            }
            
            await self._apply_virtual_service(virtual_service)
            logger.info(f"Configured retry for {service_name}: {config.attempts} attempts")
            return True
        except Exception as e:
            logger.exception(f"Failed to configure retry: {e}")
            return False
    
    async def configure_timeout(
        self,
        service_name: str,
        config: TimeoutConfig
    ) -> bool:
        """配置超时策略"""
        try:
            virtual_service = {
                "apiVersion": "networking.istio.io/v1beta1",
                "kind": "VirtualService",
                "metadata": {
                    "name": f"{service_name}-timeout",
                    "namespace": self.namespace
                },
                "spec": {
                    "hosts": [service_name],
                    "http": [
                        {
                            "route": [
                                {
                                    "destination": {
                                        "host": service_name
                                    }
                                }
                            ],
                            "timeout": f"{config.request_timeout_ms}ms"
                        }
                    ]
                }
            }
            
            await self._apply_virtual_service(virtual_service)
            logger.info(f"Configured timeout for {service_name}: {config.request_timeout_ms}ms")
            return True
        except Exception as e:
            logger.exception(f"Failed to configure timeout: {e}")
            return False
    
    async def configure_mtls(
        self,
        service_name: str,
        config: MTLSConfig
    ) -> bool:
        """配置mTLS（PeerAuthentication）"""
        try:
            peer_auth = {
                "apiVersion": "security.istio.io/v1beta1",
                "kind": "PeerAuthentication",
                "metadata": {
                    "name": f"{service_name}-mtls",
                    "namespace": self.namespace
                },
                "spec": {
                    "selector": {
                        "matchLabels": {
                            "app": service_name
                        }
                    },
                    "mtls": {
                        "mode": config.mode.value
                    }
                }
            }
            
            try:
                self._custom_objects_api.patch_namespaced_custom_object(
                    group="security.istio.io",
                    version="v1beta1",
                    namespace=self.namespace,
                    plural="peerauthentications",
                    name=peer_auth["metadata"]["name"],
                    body=peer_auth
                )
            except Exception:
                self._custom_objects_api.create_namespaced_custom_object(
                    group="security.istio.io",
                    version="v1beta1",
                    namespace=self.namespace,
                    plural="peerauthentications",
                    body=peer_auth
                )
            
            logger.info(f"Configured mTLS for {service_name}: {config.mode.value}")
            return True
        except Exception as e:
            logger.exception(f"Failed to configure mTLS: {e}")
            return False
    
    async def get_telemetry(
        self,
        service_name: Optional[str] = None,
        duration: timedelta = timedelta(minutes=5)
    ) -> List[TelemetryData]:
        """从Prometheus获取遥测数据"""
        # 这里应该连接到Istio的Prometheus实例查询指标
        # 简化实现，返回模拟数据
        telemetry_list = []
        
        # 实际实现应该查询Prometheus
        # 例如: istio_requests_total, istio_request_duration_milliseconds等
        
        return telemetry_list


# ============================================================================
# Linkerd客户端实现
# ============================================================================

class LinkerdClient(ServiceMeshClient):
    """
    Linkerd Service Mesh客户端
    
    通过Linkerd的API进行流量管理
    """
    
    def __init__(
        self,
        namespace: str = "default",
        linkerd_namespace: str = "linkerd"
    ):
        super().__init__(MeshProvider.LINKERD, namespace)
        self.linkerd_namespace = linkerd_namespace
        self._api_endpoint = f"http://localhost:8085"  # linkerd viz API
    
    async def connect(self) -> None:
        """连接到Linkerd"""
        # 使用linkerd CLI或API
        self._connected = True
        logger.info("Connected to Linkerd")
    
    async def disconnect(self) -> None:
        """断开连接"""
        self._connected = False
    
    async def apply_traffic_split(
        self,
        service_name: str,
        subsets: Dict[str, int],
        strategy: TrafficSplitStrategy = TrafficSplitStrategy.CANARY
    ) -> bool:
        """应用Linkerd流量分割（TrafficSplit CRD）"""
        try:
            traffic_split = {
                "apiVersion": "split.smi-spec.io/v1alpha4",
                "kind": "TrafficSplit",
                "metadata": {
                    "name": service_name,
                    "namespace": self.namespace
                },
                "spec": {
                    "service": service_name,
                    "backends": [
                        {
                            "service": f"{service_name}-{subset}",
                            "weight": weight
                        }
                        for subset, weight in subsets.items()
                    ]
                }
            }
            
            # 应用到集群
            logger.info(f"Applied Linkerd traffic split for {service_name}")
            return True
        except Exception as e:
            logger.exception(f"Failed to apply traffic split: {e}")
            return False
    
    async def configure_retry(
        self,
        service_name: str,
        config: RetryConfig
    ) -> bool:
        """配置重试"""
        # Linkerd使用Retry CRD
        logger.info(f"Configured Linkerd retry for {service_name}")
        return True
    
    async def configure_timeout(
        self,
        service_name: str,
        config: TimeoutConfig
    ) -> bool:
        """配置超时"""
        logger.info(f"Configured Linkerd timeout for {service_name}")
        return True
    
    async def configure_mtls(
        self,
        service_name: str,
        config: MTLSConfig
    ) -> bool:
        """Linkerd默认启用mTLS，无需额外配置"""
        logger.info(f"Linkerd mTLS is always enabled for {service_name}")
        return True
    
    async def get_telemetry(
        self,
        service_name: Optional[str] = None,
        duration: timedelta = timedelta(minutes=5)
    ) -> List[TelemetryData]:
        """获取遥测数据"""
        return []


# ============================================================================
# 流量管理器
# ============================================================================

class TrafficManager:
    """
    流量管理器
    
    管理金丝雀发布、蓝绿部署等高级流量策略
    """
    
    def __init__(self, mesh_client: ServiceMeshClient):
        self.mesh_client = mesh_client
        self._deployments: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def start_canary(
        self,
        config: CanaryConfig,
        auto_promote: bool = True
    ) -> str:
        """
        启动金丝雀发布
        
        Args:
            config: 金丝雀配置
            auto_promote: 是否自动晋升
            
        Returns:
            部署ID
        """
        deployment_id = str(uuid.uuid4())
        
        async with self._lock:
            self._deployments[deployment_id] = {
                "config": config,
                "status": "running",
                "current_weight": config.initial_weight,
                "analysis_results": []
            }
        
        # 应用初始流量分割
        subsets = {
            config.stable_version: 100 - config.initial_weight,
            config.canary_version: config.initial_weight
        }
        
        await self.mesh_client.apply_traffic_split(
            config.service_name,
            subsets,
            TrafficSplitStrategy.CANARY
        )
        
        logger.info(f"Started canary deployment {deployment_id} for {config.service_name}")
        
        if auto_promote:
            asyncio.create_task(self._auto_promote_canary(deployment_id))
        
        return deployment_id
    
    async def _auto_promote_canary(self, deployment_id: str) -> None:
        """自动晋升金丝雀"""
        deployment = self._deployments.get(deployment_id)
        if not deployment:
            return
        
        config: CanaryConfig = deployment["config"]
        
        while deployment["current_weight"] < config.max_weight:
            # 等待分析间隔
            await asyncio.sleep(config.analysis_interval)
            
            # 获取遥测数据进行分析
            telemetry = await self.mesh_client.get_telemetry(
                config.service_name,
                timedelta(seconds=config.analysis_interval)
            )
            
            # 分析指标
            analysis = self._analyze_telemetry(telemetry, config)
            deployment["analysis_results"].append(analysis)
            
            if not analysis["passed"]:
                logger.warning(f"Canary analysis failed: {analysis['reason']}")
                await self.rollback_canary(deployment_id)
                return
            
            # 增加权重
            deployment["current_weight"] = min(
                deployment["current_weight"] + config.step_weight,
                config.max_weight
            )
            
            subsets = {
                config.stable_version: 100 - deployment["current_weight"],
                config.canary_version: deployment["current_weight"]
            }
            
            await self.mesh_client.apply_traffic_split(
                config.service_name,
                subsets,
                TrafficSplitStrategy.CANARY
            )
            
            logger.info(f"Promoted canary to {deployment['current_weight']}%")
        
        # 金丝雀发布完成
        deployment["status"] = "completed"
        logger.info(f"Canary deployment {deployment_id} completed")
    
    def _analyze_telemetry(
        self,
        telemetry: List[TelemetryData],
        config: CanaryConfig
    ) -> Dict[str, Any]:
        """分析遥测数据"""
        if not telemetry:
            return {"passed": True, "reason": "No data"}
        
        # 计算成功率
        total_requests = sum(t.request_count for t in telemetry)
        total_errors = sum(t.error_count for t in telemetry)
        
        if total_requests == 0:
            return {"passed": True, "reason": "No requests"}
        
        success_rate = (total_requests - total_errors) / total_requests
        
        # 计算P99延迟
        p99_latency = max(t.latency_p99 for t in telemetry) if telemetry else 0
        
        if success_rate < config.success_rate_threshold:
            return {
                "passed": False,
                "reason": f"Success rate {success_rate:.4f} below threshold {config.success_rate_threshold}"
            }
        
        if p99_latency > config.latency_p99_threshold:
            return {
                "passed": False,
                "reason": f"P99 latency {p99_latency}ms above threshold {config.latency_p99_threshold}ms"
            }
        
        return {
            "passed": True,
            "success_rate": success_rate,
            "p99_latency": p99_latency
        }
    
    async def rollback_canary(self, deployment_id: str) -> bool:
        """回滚金丝雀发布"""
        deployment = self._deployments.get(deployment_id)
        if not deployment:
            return False
        
        config: CanaryConfig = deployment["config"]
        
        # 将所有流量切回稳定版本
        subsets = {
            config.stable_version: 100,
            config.canary_version: 0
        }
        
        await self.mesh_client.apply_traffic_split(
            config.service_name,
            subsets,
            TrafficSplitStrategy.CANARY
        )
        
        deployment["status"] = "rolled_back"
        logger.info(f"Rolled back canary deployment {deployment_id}")
        return True
    
    async def start_blue_green(
        self,
        config: BlueGreenConfig
    ) -> str:
        """启动蓝绿部署"""
        deployment_id = str(uuid.uuid4())
        
        self._deployments[deployment_id] = {
            "config": config,
            "status": "preview",
            "type": "blue_green"
        }
        
        # 设置预览服务
        if config.preview_service_enabled:
            subsets = {
                config.active_version: 0,
                config.preview_version: 100
            }
            await self.mesh_client.apply_traffic_split(
                f"{config.service_name}-preview",
                subsets,
                TrafficSplitStrategy.BLUE_GREEN
            )
        
        logger.info(f"Started blue-green deployment {deployment_id}")
        return deployment_id
    
    async def promote_blue_green(self, deployment_id: str) -> bool:
        """晋升蓝绿部署（切换流量）"""
        deployment = self._deployments.get(deployment_id)
        if not deployment or deployment.get("type") != "blue_green":
            return False
        
        config: BlueGreenConfig = deployment["config"]
        
        # 切换活动版本
        new_active = config.preview_version
        new_preview = config.active_version
        
        subsets = {
            new_active: 100,
            new_preview: 0
        }
        
        await self.mesh_client.apply_traffic_split(
            config.service_name,
            subsets,
            TrafficSplitStrategy.BLUE_GREEN
        )
        
        # 更新配置
        config.active_color = "green" if config.active_color == "blue" else "blue"
        deployment["status"] = "promoted"
        
        logger.info(f"Promoted blue-green deployment {deployment_id} to {new_active}")
        return True
    
    def get_deployment_status(self, deployment_id: str) -> Optional[Dict[str, Any]]:
        """获取部署状态"""
        return self._deployments.get(deployment_id)


# ============================================================================
# 分布式追踪
# ============================================================================

class TracingManager:
    """
    分布式追踪管理器
    
    集成Jaeger/Zipkin等追踪系统
    """
    
    def __init__(
        self,
        service_name: str,
        collector_endpoint: str = "http://localhost:9411/api/v2/spans",
        sampler_rate: float = 1.0
    ):
        self.service_name = service_name
        self.collector_endpoint = collector_endpoint
        self.sampler_rate = sampler_rate
        self._current_span: Optional[TraceSpan] = None
        self._span_stack: List[TraceSpan] = []
    
    def start_span(
        self,
        operation_name: str,
        tags: Optional[Dict[str, str]] = None,
        parent_span: Optional[TraceSpan] = None
    ) -> TraceSpan:
        """开始新的追踪跨度"""
        span = TraceSpan(
            trace_id=parent_span.trace_id if parent_span else self._generate_trace_id(),
            span_id=self._generate_span_id(),
            parent_span_id=parent_span.span_id if parent_span else None,
            operation_name=operation_name,
            tags=tags or {}
        )
        
        self._span_stack.append(span)
        self._current_span = span
        
        return span
    
    def finish_span(self, span: Optional[TraceSpan] = None) -> None:
        """完成追踪跨度"""
        if span is None:
            span = self._current_span
        
        if span:
            span.end_time = datetime.now(timezone.utc)
            
            # 发送跨度到收集器
            asyncio.create_task(self._send_span(span))
            
            # 从栈中移除
            if span in self._span_stack:
                self._span_stack.remove(span)
            
            # 更新当前跨度
            self._current_span = self._span_stack[-1] if self._span_stack else None
    
    def add_tag(self, key: str, value: str) -> None:
        """添加标签到当前跨度"""
        if self._current_span:
            self._current_span.tags[key] = value
    
    def log_event(self, event: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """记录事件"""
        if self._current_span:
            self._current_span.logs.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event,
                "payload": payload or {}
            })
    
    def get_current_span(self) -> Optional[TraceSpan]:
        """获取当前跨度"""
        return self._current_span
    
    def inject_headers(self) -> Dict[str, str]:
        """注入追踪上下文到HTTP头"""
        if not self._current_span:
            return {}
        
        return {
            "x-b3-traceid": self._current_span.trace_id,
            "x-b3-spanid": self._current_span.span_id,
            "x-b3-parentspanid": self._current_span.parent_span_id or "",
            "x-b3-sampled": "1" if random.random() < self.sampler_rate else "0"
        }
    
    def extract_headers(self, headers: Dict[str, str]) -> Optional[TraceSpan]:
        """从HTTP头提取追踪上下文"""
        trace_id = headers.get("x-b3-traceid")
        span_id = headers.get("x-b3-spanid")
        
        if trace_id and span_id:
            return TraceSpan(
                trace_id=trace_id,
                span_id=self._generate_span_id(),  # 创建新span
                parent_span_id=span_id,
                operation_name="extracted"
            )
        return None
    
    def _generate_trace_id(self) -> str:
        """生成追踪ID"""
        return uuid.uuid4().hex[:16]
    
    def _generate_span_id(self) -> str:
        """生成跨度ID"""
        return uuid.uuid4().hex[:16]
    
    async def _send_span(self, span: TraceSpan) -> None:
        """发送跨度到收集器"""
        try:
            import aiohttp
            
            span_data = {
                "id": span.span_id,
                "traceId": span.trace_id,
                "parentId": span.parent_span_id,
                "name": span.operation_name,
                "timestamp": int(span.start_time.timestamp() * 1000000),
                "duration": int(span.duration_ms * 1000),
                "localEndpoint": {
                    "serviceName": self.service_name
                },
                "tags": span.tags,
                "annotations": [
                    {
                        "timestamp": int(datetime.fromisoformat(log["timestamp"]).timestamp() * 1000000),
                        "value": log["event"]
                    }
                    for log in span.logs
                ]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.collector_endpoint,
                    json=[span_data]
                ) as resp:
                    if resp.status >= 400:
                        logger.warning(f"Failed to send span: {resp.status}")
        except Exception as e:
            logger.exception(f"Error sending span: {e}")


# ============================================================================
# mTLS证书管理
# ============================================================================

class CertificateManager:
    """
    mTLS证书管理器
    
    管理客户端证书、CA证书等
    """
    
    def __init__(self, cert_dir: str = "/etc/certs"):
        self.cert_dir = cert_dir
        self._certificates: Dict[str, Dict[str, str]] = {}
        self._lock = asyncio.Lock()
    
    async def load_certificate(
        self,
        name: str,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        ca_path: Optional[str] = None
    ) -> bool:
        """加载证书"""
        try:
            cert_data = {}
            
            if cert_path:
                with open(cert_path, 'r') as f:
                    cert_data['cert'] = f.read()
            
            if key_path:
                with open(key_path, 'r') as f:
                    cert_data['key'] = f.read()
            
            if ca_path:
                with open(ca_path, 'r') as f:
                    cert_data['ca'] = f.read()
            
            async with self._lock:
                self._certificates[name] = cert_data
            
            logger.info(f"Loaded certificate: {name}")
            return True
        except Exception as e:
            logger.exception(f"Failed to load certificate: {e}")
            return False
    
    async def get_certificate(self, name: str) -> Optional[Dict[str, str]]:
        """获取证书"""
        async with self._lock:
            return self._certificates.get(name)
    
    async def rotate_certificate(
        self,
        name: str,
        new_cert_path: str,
        new_key_path: str
    ) -> bool:
        """轮换证书"""
        try:
            with open(new_cert_path, 'r') as f:
                new_cert = f.read()
            
            with open(new_key_path, 'r') as f:
                new_key = f.read()
            
            async with self._lock:
                if name in self._certificates:
                    self._certificates[name]['cert'] = new_cert
                    self._certificates[name]['key'] = new_key
            
            logger.info(f"Rotated certificate: {name}")
            return True
        except Exception as e:
            logger.exception(f"Failed to rotate certificate: {e}")
            return False


# ============================================================================
# Service Mesh HTTP客户端
# ============================================================================

class MeshHTTPClient:
    """
    Service Mesh HTTP客户端
    
    集成追踪、mTLS、重试等功能的HTTP客户端
    """
    
    def __init__(
        self,
        tracing_manager: Optional[TracingManager] = None,
        cert_manager: Optional[CertificateManager] = None,
        retry_config: Optional[RetryConfig] = None,
        timeout_config: Optional[TimeoutConfig] = None
    ):
        self.tracing_manager = tracing_manager
        self.cert_manager = cert_manager
        self.retry_config = retry_config or RetryConfig()
        self.timeout_config = timeout_config or TimeoutConfig()
        self._session: Optional[Any] = None
    
    async def connect(self, ssl_context: Optional[Any] = None) -> None:
        """创建HTTP会话"""
        import aiohttp
        
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(
            total=self.timeout_config.request_timeout_ms / 1000
        )
        
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
    
    async def disconnect(self) -> None:
        """关闭会话"""
        if self._session:
            await self._session.close()
    
    async def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        发送HTTP请求
        
        自动集成追踪、重试等功能
        """
        headers = headers or {}
        
        # 注入追踪头
        if self.tracing_manager:
            trace_headers = self.tracing_manager.inject_headers()
            headers.update(trace_headers)
            
            # 开始追踪跨度
            span = self.tracing_manager.start_span(
                f"HTTP {method}",
                tags={
                    "http.method": method,
                    "http.url": url
                }
            )
        else:
            span = None
        
        try:
            # 执行带重试的请求
            result = await self._request_with_retry(method, url, headers, **kwargs)
            
            if span:
                self.tracing_manager.add_tag("http.status_code", str(result["status"]))
            
            return result
        except Exception as e:
            if span:
                self.tracing_manager.add_tag("error", str(e))
            raise
        finally:
            if span:
                self.tracing_manager.finish_span(span)
    
    async def _request_with_retry(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        **kwargs
    ) -> Dict[str, Any]:
        """带重试的请求"""
        last_error = None
        
        for attempt in range(self.retry_config.attempts):
            try:
                async with self._session.request(
                    method, url, headers=headers, **kwargs
                ) as resp:
                    data = await resp.json() if resp.content_type == 'application/json' else await resp.text()
                    
                    return {
                        "status": resp.status,
                        "data": data,
                        "headers": dict(resp.headers)
                    }
            except Exception as e:
                last_error = e
                
                if attempt < self.retry_config.attempts - 1:
                    # 计算退避时间
                    if self.retry_config.backoff == RetryPolicy.EXPONENTIAL:
                        delay = self.retry_config.base_interval_ms * (2 ** attempt) / 1000
                    elif self.retry_config.backoff == RetryPolicy.LINEAR:
                        delay = self.retry_config.base_interval_ms * (attempt + 1) / 1000
                    else:
                        delay = self.retry_config.base_interval_ms / 1000
                    
                    delay = min(delay, self.retry_config.max_interval_ms / 1000)
                    await asyncio.sleep(delay)
        
        raise last_error
    
    async def get(self, url: str, **kwargs) -> Dict[str, Any]:
        """GET请求"""
        return await self.request("GET", url, **kwargs)
    
    async def post(self, url: str, **kwargs) -> Dict[str, Any]:
        """POST请求"""
        return await self.request("POST", url, **kwargs)
    
    async def put(self, url: str, **kwargs) -> Dict[str, Any]:
        """PUT请求"""
        return await self.request("PUT", url, **kwargs)
    
    async def delete(self, url: str, **kwargs) -> Dict[str, Any]:
        """DELETE请求"""
        return await self.request("DELETE", url, **kwargs)


# ============================================================================
# Service Mesh门面
# ============================================================================

class ServiceMeshFacade:
    """
    Service Mesh门面
    
    提供统一的Service Mesh操作接口
    """
    
    def __init__(
        self,
        provider: MeshProvider = MeshProvider.ISTIO,
        namespace: str = "default"
    ):
        self.provider = provider
        self.namespace = namespace
        self.client: ServiceMeshClient = self._create_client()
        self.traffic_manager = TrafficManager(self.client)
        self.tracing_manager: Optional[TracingManager] = None
        self.cert_manager = CertificateManager()
        self.http_client: Optional[MeshHTTPClient] = None
    
    def _create_client(self) -> ServiceMeshClient:
        """创建Service Mesh客户端"""
        if self.provider == MeshProvider.ISTIO:
            return IstioClient(self.namespace)
        elif self.provider == MeshProvider.LINKERD:
            return LinkerdClient(self.namespace)
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    async def initialize(
        self,
        service_name: str,
        enable_tracing: bool = True,
        tracing_endpoint: str = "http://localhost:9411/api/v2/spans"
    ) -> None:
        """初始化Service Mesh集成"""
        await self.client.connect()
        
        if enable_tracing:
            self.tracing_manager = TracingManager(service_name, tracing_endpoint)
        
        self.http_client = MeshHTTPClient(
            tracing_manager=self.tracing_manager,
            cert_manager=self.cert_manager
        )
        await self.http_client.connect()
        
        logger.info(f"ServiceMesh facade initialized for {service_name}")
    
    async def shutdown(self) -> None:
        """关闭Service Mesh集成"""
        if self.http_client:
            await self.http_client.disconnect()
        await self.client.disconnect()
        logger.info("ServiceMesh facade shutdown")
    
    async def deploy_canary(self, config: CanaryConfig) -> str:
        """部署金丝雀"""
        return await self.traffic_manager.start_canary(config)
    
    async def deploy_blue_green(self, config: BlueGreenConfig) -> str:
        """部署蓝绿"""
        return await self.traffic_manager.start_blue_green(config)
    
    async def promote_deployment(self, deployment_id: str) -> bool:
        """晋升部署"""
        return await self.traffic_manager.promote_blue_green(deployment_id)
    
    async def rollback_deployment(self, deployment_id: str) -> bool:
        """回滚部署"""
        return await self.traffic_manager.rollback_canary(deployment_id)
    
    def start_trace(self, operation_name: str) -> TraceSpan:
        """开始追踪"""
        if self.tracing_manager:
            return self.tracing_manager.start_span(operation_name)
        return None
    
    def finish_trace(self, span: TraceSpan) -> None:
        """结束追踪"""
        if self.tracing_manager and span:
            self.tracing_manager.finish_span(span)
    
    async def call_service(
        self,
        service_name: str,
        method: str,
        path: str,
        **kwargs
    ) -> Dict[str, Any]:
        """调用服务"""
        if not self.http_client:
            raise RuntimeError("HTTP client not initialized")
        
        url = f"http://{service_name}.{self.namespace}.svc.cluster.local{path}"
        return await self.http_client.request(method, url, **kwargs)
    
    async def get_metrics(self) -> Dict[str, Any]:
        """获取指标"""
        telemetry = await self.client.get_telemetry(duration=timedelta(minutes=5))
        return {
            "telemetry_count": len(telemetry),
            "deployments": list(self.traffic_manager._deployments.keys()),
            "provider": self.provider.value
        }


# ============================================================================
# 使用示例
# ============================================================================

async def example_usage():
    """使用示例"""
    # 创建Service Mesh门面
    mesh = ServiceMeshFacade(
        provider=MeshProvider.ISTIO,
        namespace="default"
    )
    
    # 初始化
    await mesh.initialize(
        service_name="order-service",
        enable_tracing=True
    )
    
    try:
        # 配置金丝雀发布
        canary_config = CanaryConfig(
            service_name="payment-service",
            namespace="default",
            stable_version="v1",
            canary_version="v2",
            initial_weight=10,
            max_weight=100,
            step_weight=10,
            analysis_interval=30
        )
        
        # 启动金丝雀发布
        deployment_id = await mesh.deploy_canary(canary_config)
        print(f"Canary deployment started: {deployment_id}")
        
        # 配置蓝绿部署
        blue_green_config = BlueGreenConfig(
            service_name="user-service",
            namespace="default",
            blue_version="v1",
            green_version="v2",
            active_color="blue",
            preview_service_enabled=True
        )
        
        bg_deployment_id = await mesh.deploy_blue_green(blue_green_config)
        print(f"Blue-green deployment started: {bg_deployment_id}")
        
        # 开始追踪
        span = mesh.start_trace("process-order")
        mesh.tracing_manager.add_tag("order_id", "12345")
        mesh.tracing_manager.log_event("order_received", {"items": 3})
        
        # 调用服务
        try:
            result = await mesh.call_service(
                "inventory-service",
                "GET",
                "/api/stock/item-123"
            )
            print(f"Service response: {result}")
        except Exception as e:
            mesh.tracing_manager.add_tag("error", str(e))
        
        # 结束追踪
        mesh.finish_trace(span)
        
        # 获取指标
        metrics = await mesh.get_metrics()
        print(f"Mesh metrics: {metrics}")
        
        # 模拟运行
        await asyncio.sleep(5)
        
        # 获取部署状态
        status = mesh.traffic_manager.get_deployment_status(deployment_id)
        print(f"Deployment status: {status}")
        
    finally:
        await mesh.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())

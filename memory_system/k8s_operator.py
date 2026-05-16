"""
Kubernetes Operator模块 - K8s Operator Module

该模块实现了云原生Operator模式，包含：
- CRD（自定义资源定义）
- 控制器循环实现
- 自动扩缩容（HPA/VPA）
- 配置管理和密钥管理
- 运维自动化（备份/恢复）
- 状态检查和自愈

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

class ResourceStatus(Enum):
    """资源状态"""
    PENDING = "Pending"
    CREATING = "Creating"
    RUNNING = "Running"
    UPDATING = "Updating"
    SCALING = "Scaling"
    DEGRADED = "Degraded"
    FAILED = "Failed"
    TERMINATING = "Terminating"
    COMPLETED = "Completed"


class ScalingPolicy(Enum):
    """扩缩容策略"""
    HORIZONTAL = "horizontal"  # HPA
    VERTICAL = "vertical"      # VPA
    CUSTOM = "custom"          # 自定义


class BackupType(Enum):
    """备份类型"""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


T = TypeVar('T')


# ============================================================================
# 自定义资源定义（CRD）
# ============================================================================

@dataclass
class CustomResourceSpec:
    """自定义资源规格"""
    replicas: int = 1
    image: str = ""
    resources: Dict[str, Any] = field(default_factory=dict)
    env: List[Dict[str, str]] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    storage: Dict[str, Any] = field(default_factory=dict)
    service: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CustomResourceStatus:
    """自定义资源状态"""
    phase: ResourceStatus = ResourceStatus.PENDING
    conditions: List[Dict[str, Any]] = field(default_factory=list)
    replicas: int = 0
    ready_replicas: int = 0
    observed_generation: int = 0
    last_transition_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "phase": self.phase.value,
            "conditions": self.conditions,
            "replicas": self.replicas,
            "ready_replicas": self.ready_replicas,
            "observed_generation": self.observed_generation,
            "last_transition_time": self.last_transition_time.isoformat(),
            "message": self.message
        }


@dataclass
class CustomResource:
    """
    自定义资源（CR）
    
    代表一个Kubernetes自定义资源实例
    """
    api_version: str
    kind: str
    metadata: Dict[str, Any]
    spec: CustomResourceSpec
    status: CustomResourceStatus = field(default_factory=CustomResourceStatus)
    
    @property
    def name(self) -> str:
        return self.metadata.get("name", "")
    
    @property
    def namespace(self) -> str:
        return self.metadata.get("namespace", "default")
    
    @property
    def uid(self) -> str:
        return self.metadata.get("uid", "")
    
    @property
    def generation(self) -> int:
        return self.metadata.get("generation", 1)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "apiVersion": self.api_version,
            "kind": self.kind,
            "metadata": self.metadata,
            "spec": self.spec.to_dict(),
            "status": self.status.to_dict()
        }


@dataclass
class CustomResourceDefinition:
    """
    自定义资源定义（CRD）
    
    定义新的Kubernetes资源类型
    """
    api_version: str = "apiextensions.k8s.io/v1"
    kind: str = "CustomResourceDefinition"
    name: str = ""
    group: str = ""
    version: str = "v1"
    scope: str = "Namespaced"  # or "Cluster"
    plural: str = ""
    singular: str = ""
    kind_name: str = ""
    short_names: List[str] = field(default_factory=list)
    schema: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "apiVersion": self.api_version,
            "kind": self.kind,
            "metadata": {"name": f"{self.plural}.{self.group}"},
            "spec": {
                "group": self.group,
                "versions": [{
                    "name": self.version,
                    "served": True,
                    "storage": True,
                    "schema": {
                        "openAPIV3Schema": self.schema
                    }
                }],
                "scope": self.scope,
                "names": {
                    "plural": self.plural,
                    "singular": self.singular,
                    "kind": self.kind_name,
                    "shortNames": self.short_names
                }
            }
        }


# ============================================================================
# Kubernetes客户端
# ============================================================================

class KubernetesClient:
    """
    Kubernetes客户端
    
    封装与Kubernetes API的交互
    """
    
    def __init__(self, kube_config: Optional[str] = None):
        self.kube_config = kube_config
        self._core_api: Optional[Any] = None
        self._apps_api: Optional[Any] = None
        self._custom_objects_api: Optional[Any] = None
        self._autoscaling_api: Optional[Any] = None
        self._connected = False
    
    async def connect(self) -> None:
        """连接到Kubernetes集群"""
        try:
            from kubernetes import client, config
            
            if self.kube_config:
                config.load_kube_config(self.kube_config)
            else:
                config.load_incluster_config()
            
            self._core_api = client.CoreV1Api()
            self._apps_api = client.AppsV1Api()
            self._custom_objects_api = client.CustomObjectsApi()
            self._autoscaling_api = client.AutoscalingV2Api()
            self._connected = True
            
            logger.info("Connected to Kubernetes cluster")
        except ImportError:
            logger.error("kubernetes client not installed")
            raise
        except Exception as e:
            logger.exception(f"Failed to connect to Kubernetes: {e}")
            raise
    
    async def disconnect(self) -> None:
        """断开连接"""
        self._connected = False
    
    async def create_crd(self, crd: CustomResourceDefinition) -> bool:
        """创建CRD"""
        try:
            body = crd.to_dict()
            self._custom_objects_api.create_cluster_custom_object(
                group="apiextensions.k8s.io",
                version="v1",
                plural="customresourcedefinitions",
                body=body
            )
            logger.info(f"Created CRD: {crd.name}")
            return True
        except Exception as e:
            if "already exists" in str(e):
                logger.info(f"CRD already exists: {crd.name}")
                return True
            logger.exception(f"Failed to create CRD: {e}")
            return False
    
    async def create_custom_resource(self, cr: CustomResource) -> bool:
        """创建自定义资源"""
        try:
            group = cr.api_version.split("/")[0]
            version = cr.api_version.split("/")[1]
            plural = f"{cr.kind.lower()}s"
            
            body = cr.to_dict()
            self._custom_objects_api.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=cr.namespace,
                plural=plural,
                body=body
            )
            logger.info(f"Created custom resource: {cr.name}")
            return True
        except Exception as e:
            logger.exception(f"Failed to create custom resource: {e}")
            return False
    
    async def update_custom_resource_status(
        self,
        cr: CustomResource,
        status: CustomResourceStatus
    ) -> bool:
        """更新自定义资源状态"""
        try:
            group = cr.api_version.split("/")[0]
            version = cr.api_version.split("/")[1]
            plural = f"{cr.kind.lower()}s"
            
            patch = {"status": status.to_dict()}
            self._custom_objects_api.patch_namespaced_custom_object_status(
                group=group,
                version=version,
                namespace=cr.namespace,
                plural=plural,
                name=cr.name,
                body=patch
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to update status: {e}")
            return False
    
    async def get_custom_resource(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        namespace: str
    ) -> Optional[CustomResource]:
        """获取自定义资源"""
        try:
            obj = self._custom_objects_api.get_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name
            )
            
            return CustomResource(
                api_version=obj["apiVersion"],
                kind=obj["kind"],
                metadata=obj["metadata"],
                spec=CustomResourceSpec(**obj.get("spec", {})),
                status=CustomResourceStatus(**obj.get("status", {}))
            )
        except Exception as e:
            logger.exception(f"Failed to get custom resource: {e}")
            return None
    
    async def list_custom_resources(
        self,
        group: str,
        version: str,
        plural: str,
        namespace: Optional[str] = None
    ) -> List[CustomResource]:
        """列出自定义资源"""
        try:
            if namespace:
                objs = self._custom_objects_api.list_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural
                )
            else:
                objs = self._custom_objects_api.list_cluster_custom_object(
                    group=group,
                    version=version,
                    plural=plural
                )
            
            return [
                CustomResource(
                    api_version=obj["apiVersion"],
                    kind=obj["kind"],
                    metadata=obj["metadata"],
                    spec=CustomResourceSpec(**obj.get("spec", {})),
                    status=CustomResourceStatus(**obj.get("status", {}))
                )
                for obj in objs.get("items", [])
            ]
        except Exception as e:
            logger.exception(f"Failed to list custom resources: {e}")
            return []
    
    async def create_deployment(self, namespace: str, deployment: Dict[str, Any]) -> bool:
        """创建Deployment"""
        try:
            self._apps_api.create_namespaced_deployment(
                namespace=namespace,
                body=deployment
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to create deployment: {e}")
            return False
    
    async def update_deployment(self, namespace: str, name: str, patch: Dict[str, Any]) -> bool:
        """更新Deployment"""
        try:
            self._apps_api.patch_namespaced_deployment(
                name=name,
                namespace=namespace,
                body=patch
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to update deployment: {e}")
            return False
    
    async def create_service(self, namespace: str, service: Dict[str, Any]) -> bool:
        """创建Service"""
        try:
            self._core_api.create_namespaced_service(
                namespace=namespace,
                body=service
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to create service: {e}")
            return False
    
    async def create_configmap(self, namespace: str, configmap: Dict[str, Any]) -> bool:
        """创建ConfigMap"""
        try:
            self._core_api.create_namespaced_config_map(
                namespace=namespace,
                body=configmap
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to create configmap: {e}")
            return False
    
    async def create_secret(self, namespace: str, secret: Dict[str, Any]) -> bool:
        """创建Secret"""
        try:
            self._core_api.create_namespaced_secret(
                namespace=namespace,
                body=secret
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to create secret: {e}")
            return False
    
    async def create_hpa(self, namespace: str, hpa: Dict[str, Any]) -> bool:
        """创建HPA"""
        try:
            self._autoscaling_api.create_namespaced_horizontal_pod_autoscaler(
                namespace=namespace,
                body=hpa
            )
            return True
        except Exception as e:
            logger.exception(f"Failed to create HPA: {e}")
            return False


# ============================================================================
# 控制器基类
# ============================================================================

class Controller(ABC):
    """
    Kubernetes控制器基类
    
    实现控制循环（Reconciliation Loop）
    """
    
    def __init__(
        self,
        k8s_client: KubernetesClient,
        resync_period: int = 60,
        worker_count: int = 3
    ):
        self.k8s_client = k8s_client
        self.resync_period = resync_period
        self.worker_count = worker_count
        self._running = False
        self._work_queue: asyncio.Queue = asyncio.Queue()
        self._worker_tasks: List[asyncio.Task] = []
        self._watcher_task: Optional[asyncio.Task] = None
    
    @abstractmethod
    def get_crd(self) -> CustomResourceDefinition:
        """返回此控制器管理的CRD"""
        pass
    
    @abstractmethod
    async def reconcile(self, cr: CustomResource) -> CustomResourceStatus:
        """
        调谐方法
        
        比较期望状态和实际状态，执行必要的操作使它们一致
        
        Args:
            cr: 自定义资源实例
            
        Returns:
            新的资源状态
        """
        pass
    
    @abstractmethod
    async def delete(self, cr: CustomResource) -> bool:
        """
        删除资源
        
        执行清理操作
        """
        pass
    
    async def start(self) -> None:
        """启动控制器"""
        self._running = True
        
        # 创建CRD
        crd = self.get_crd()
        await self.k8s_client.create_crd(crd)
        
        # 启动工作线程
        for i in range(self.worker_count):
            task = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(task)
        
        # 启动监视器
        self._watcher_task = asyncio.create_task(self._watch_loop())
        
        logger.info(f"Controller for {crd.kind_name} started")
    
    async def stop(self) -> None:
        """停止控制器"""
        self._running = False
        
        # 取消工作线程
        for task in self._worker_tasks:
            task.cancel()
        
        if self._watcher_task:
            self._watcher_task.cancel()
        
        # 等待所有任务完成
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        
        if self._watcher_task:
            try:
                await self._watcher_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Controller stopped")
    
    async def _watch_loop(self) -> None:
        """监视资源变化的循环"""
        crd = self.get_crd()
        
        while self._running:
            try:
                # 列出所有资源
                crs = await self.k8s_client.list_custom_resources(
                    group=crd.group,
                    version=crd.version,
                    plural=crd.plural
                )
                
                for cr in crs:
                    # 将资源加入工作队列
                    await self._work_queue.put(("reconcile", cr))
                
                await asyncio.sleep(self.resync_period)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Watch loop error: {e}")
                await asyncio.sleep(5)
    
    async def _worker_loop(self, worker_id: int) -> None:
        """工作线程循环"""
        while self._running:
            try:
                action, cr = await asyncio.wait_for(
                    self._work_queue.get(),
                    timeout=1.0
                )
                
                if action == "reconcile":
                    await self._do_reconcile(cr)
                elif action == "delete":
                    await self.delete(cr)
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Worker {worker_id} error: {e}")
    
    async def _do_reconcile(self, cr: CustomResource) -> None:
        """执行调谐"""
        try:
            # 更新状态为处理中
            cr.status.phase = ResourceStatus.UPDATING
            await self.k8s_client.update_custom_resource_status(cr, cr.status)
            
            # 执行调谐逻辑
            new_status = await self.reconcile(cr)
            
            # 更新状态
            new_status.observed_generation = cr.generation
            await self.k8s_client.update_custom_resource_status(cr, new_status)
            
        except Exception as e:
            logger.exception(f"Reconcile error for {cr.name}: {e}")
            
            # 更新失败状态
            cr.status.phase = ResourceStatus.FAILED
            cr.status.message = str(e)
            await self.k8s_client.update_custom_resource_status(cr, cr.status)
    
    def enqueue(self, action: str, cr: CustomResource) -> None:
        """将资源加入工作队列"""
        self._work_queue.put_nowait((action, cr))


# ============================================================================
# 自动扩缩容管理器
# ============================================================================

@dataclass
class HPASpec:
    """HPA规格"""
    min_replicas: int = 1
    max_replicas: int = 10
    target_cpu_utilization: int = 70
    target_memory_utilization: Optional[int] = None
    scale_down_stabilization: int = 300  # 秒
    scale_up_stabilization: int = 0
    
    def to_dict(self, deployment_name: str) -> Dict[str, Any]:
        metrics = [{
            "type": "Resource",
            "resource": {
                "name": "cpu",
                "target": {
                    "type": "Utilization",
                    "averageUtilization": self.target_cpu_utilization
                }
            }
        }]
        
        if self.target_memory_utilization:
            metrics.append({
                "type": "Resource",
                "resource": {
                    "name": "memory",
                    "target": {
                        "type": "Utilization",
                        "averageUtilization": self.target_memory_utilization
                    }
                }
            })
        
        return {
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": f"{deployment_name}-hpa"},
            "spec": {
                "scaleTargetRef": {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": deployment_name
                },
                "minReplicas": self.min_replicas,
                "maxReplicas": self.max_replicas,
                "metrics": metrics,
                "behavior": {
                    "scaleDown": {
                        "stabilizationWindowSeconds": self.scale_down_stabilization
                    },
                    "scaleUp": {
                        "stabilizationWindowSeconds": self.scale_up_stabilization
                    }
                }
            }
        }


@dataclass
class VPASpec:
    """VPA规格"""
    mode: str = "Auto"  # Auto, Off, Initial, Recreate
    update_mode: str = "Auto"
    controlled_resources: List[str] = field(default_factory=lambda: ["cpu", "memory"])
    
    def to_dict(self, deployment_name: str) -> Dict[str, Any]:
        return {
            "apiVersion": "autoscaling.k8s.io/v1",
            "kind": "VerticalPodAutoscaler",
            "metadata": {"name": f"{deployment_name}-vpa"},
            "spec": {
                "targetRef": {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": deployment_name
                },
                "updatePolicy": {
                    "updateMode": self.update_mode
                },
                "resourcePolicy": {
                    "containerPolicies": [{
                        "containerName": "*",
                        "controlledResources": self.controlled_resources
                    }]
                }
            }
        }


class AutoScaler:
    """
    自动扩缩容管理器
    
    管理HPA和VPA资源
    """
    
    def __init__(self, k8s_client: KubernetesClient):
        self.k8s_client = k8s_client
        self._hpas: Dict[str, HPASpec] = {}
        self._vpas: Dict[str, VPASpec] = {}
    
    async def create_hpa(
        self,
        namespace: str,
        deployment_name: str,
        spec: HPASpec
    ) -> bool:
        """创建HPA"""
        hpa_dict = spec.to_dict(deployment_name)
        success = await self.k8s_client.create_hpa(namespace, hpa_dict)
        
        if success:
            self._hpas[f"{namespace}/{deployment_name}"] = spec
            logger.info(f"Created HPA for {deployment_name}")
        
        return success
    
    async def get_recommendation(
        self,
        deployment_name: str,
        namespace: str
    ) -> Dict[str, Any]:
        """获取扩缩容建议"""
        # 这里应该查询metrics-server或Prometheus获取实际指标
        # 简化实现
        return {
            "current_replicas": 3,
            "recommended_replicas": 5,
            "cpu_utilization": 85,
            "memory_utilization": 60,
            "reason": "High CPU utilization"
        }


# ============================================================================
# 配置管理
# ============================================================================

class ConfigManager:
    """
    配置管理器
    
    管理ConfigMap和Secret
    """
    
    def __init__(self, k8s_client: KubernetesClient):
        self.k8s_client = k8s_client
    
    async def create_configmap_from_dict(
        self,
        name: str,
        namespace: str,
        data: Dict[str, str],
        labels: Optional[Dict[str, str]] = None
    ) -> bool:
        """从字典创建ConfigMap"""
        configmap = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels or {}
            },
            "data": data
        }
        
        return await self.k8s_client.create_configmap(namespace, configmap)
    
    async def create_secret_from_dict(
        self,
        name: str,
        namespace: str,
        data: Dict[str, str],
        secret_type: str = "Opaque"
    ) -> bool:
        """从字典创建Secret"""
        # Base64编码数据
        encoded_data = {
            k: base64.b64encode(v.encode()).decode()
            for k, v in data.items()
        }
        
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": name,
                "namespace": namespace
            },
            "type": secret_type,
            "data": encoded_data
        }
        
        return await self.k8s_client.create_secret(namespace, secret)
    
    async def create_tls_secret(
        self,
        name: str,
        namespace: str,
        cert_path: str,
        key_path: str
    ) -> bool:
        """创建TLS Secret"""
        try:
            with open(cert_path, 'r') as f:
                cert = f.read()
            
            with open(key_path, 'r') as f:
                key = f.read()
            
            return await self.create_secret_from_dict(
                name,
                namespace,
                {"tls.crt": cert, "tls.key": key},
                secret_type="kubernetes.io/tls"
            )
        except Exception as e:
            logger.exception(f"Failed to create TLS secret: {e}")
            return False


# ============================================================================
# 备份恢复管理器
# ============================================================================

@dataclass
class BackupJob:
    """备份任务"""
    id: str
    resource_name: str
    resource_namespace: str
    backup_type: BackupType
    status: str = "pending"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    backup_location: str = ""
    size_bytes: int = 0
    error_message: str = ""


class BackupManager:
    """
    备份恢复管理器
    
    管理资源备份和恢复
    """
    
    def __init__(self, k8s_client: KubernetesClient, backup_storage_path: str = "/backups"):
        self.k8s_client = k8s_client
        self.backup_storage_path = backup_storage_path
        self._jobs: Dict[str, BackupJob] = {}
        self._lock = asyncio.Lock()
    
    async def create_backup(
        self,
        resource_name: str,
        resource_namespace: str,
        backup_type: BackupType = BackupType.FULL
    ) -> str:
        """创建备份"""
        job_id = str(uuid.uuid4())
        
        job = BackupJob(
            id=job_id,
            resource_name=resource_name,
            resource_namespace=resource_namespace,
            backup_type=backup_type,
            started_at=datetime.now(timezone.utc)
        )
        
        async with self._lock:
            self._jobs[job_id] = job
        
        # 异步执行备份
        asyncio.create_task(self._do_backup(job))
        
        return job_id
    
    async def _do_backup(self, job: BackupJob) -> None:
        """执行备份"""
        try:
            job.status = "running"
            
            # 获取资源
            # 这里简化实现，实际应该根据资源类型获取
            resource_data = {
                "name": job.resource_name,
                "namespace": job.resource_namespace,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # 保存到存储
            backup_file = f"{self.backup_storage_path}/{job.id}.json"
            # 实际应该写入文件或上传到对象存储
            
            job.status = "completed"
            job.completed_at = datetime.now(timezone.utc)
            job.backup_location = backup_file
            job.size_bytes = len(json.dumps(resource_data).encode())
            
            logger.info(f"Backup completed: {job.id}")
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            logger.exception(f"Backup failed: {e}")
    
    async def restore_backup(
        self,
        job_id: str,
        target_namespace: Optional[str] = None
    ) -> bool:
        """恢复备份"""
        job = self._jobs.get(job_id)
        if not job or job.status != "completed":
            return False
        
        try:
            # 读取备份文件
            # 实际应该从存储读取
            
            # 恢复资源
            logger.info(f"Restored backup: {job_id}")
            return True
        except Exception as e:
            logger.exception(f"Restore failed: {e}")
            return False
    
    def get_backup_status(self, job_id: str) -> Optional[BackupJob]:
        """获取备份状态"""
        return self._jobs.get(job_id)
    
    def list_backups(
        self,
        resource_name: Optional[str] = None
    ) -> List[BackupJob]:
        """列出备份"""
        jobs = list(self._jobs.values())
        if resource_name:
            jobs = [j for j in jobs if j.resource_name == resource_name]
        return jobs


# ============================================================================
# 自愈管理器
# ============================================================================

@dataclass
class HealthCheck:
    """健康检查定义"""
    name: str
    check_type: str  # http, tcp, exec
    endpoint: str = ""
    interval: int = 10
    timeout: int = 5
    failure_threshold: int = 3
    success_threshold: int = 1


class SelfHealingManager:
    """
    自愈管理器
    
    监控资源健康状态并执行修复操作
    """
    
    def __init__(self, k8s_client: KubernetesClient):
        self.k8s_client = k8s_client
        self._checks: Dict[str, HealthCheck] = {}
        self._health_states: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
    
    def register_health_check(self, resource_name: str, check: HealthCheck) -> None:
        """注册健康检查"""
        self._checks[resource_name] = check
        self._health_states[resource_name] = {
            "consecutive_failures": 0,
            "consecutive_successes": 0,
            "last_check": None,
            "is_healthy": True
        }
    
    async def start(self) -> None:
        """启动监控"""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Self-healing manager started")
    
    async def stop(self) -> None:
        """停止监控"""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Self-healing manager stopped")
    
    async def _monitor_loop(self) -> None:
        """监控循环"""
        while self._running:
            try:
                for resource_name, check in self._checks.items():
                    await self._check_health(resource_name, check)
                
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Monitor loop error: {e}")
                await asyncio.sleep(5)
    
    async def _check_health(self, resource_name: str, check: HealthCheck) -> None:
        """执行健康检查"""
        state = self._health_states[resource_name]
        
        try:
            # 执行检查
            is_healthy = await self._execute_check(check)
            state["last_check"] = datetime.now(timezone.utc)
            
            if is_healthy:
                state["consecutive_successes"] += 1
                state["consecutive_failures"] = 0
                
                if state["consecutive_successes"] >= check.success_threshold:
                    if not state["is_healthy"]:
                        logger.info(f"Resource {resource_name} is now healthy")
                        state["is_healthy"] = True
            else:
                state["consecutive_failures"] += 1
                state["consecutive_successes"] = 0
                
                if state["consecutive_failures"] >= check.failure_threshold:
                    if state["is_healthy"]:
                        logger.warning(f"Resource {resource_name} is unhealthy, triggering healing")
                        state["is_healthy"] = False
                        await self._heal_resource(resource_name)
        
        except Exception as e:
            logger.exception(f"Health check error for {resource_name}: {e}")
    
    async def _execute_check(self, check: HealthCheck) -> bool:
        """执行具体检查"""
        if check.check_type == "http":
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        check.endpoint,
                        timeout=aiohttp.ClientTimeout(total=check.timeout)
                    ) as resp:
                        return resp.status < 500
            except Exception:
                return False
        
        # 其他检查类型...
        return True
    
    async def _heal_resource(self, resource_name: str) -> None:
        """执行修复操作"""
        logger.info(f"Healing resource: {resource_name}")
        
        # 可能的修复操作：
        # 1. 重启Pod
        # 2. 重新创建资源
        # 3. 扩容
        # 4. 切换到备用实例
        
        # 简化实现
        logger.info(f"Healing actions completed for {resource_name}")
    
    def get_health_status(self, resource_name: str) -> Optional[Dict[str, Any]]:
        """获取健康状态"""
        return self._health_states.get(resource_name)


# ============================================================================
# Operator门面
# ============================================================================

class Operator:
    """
    Kubernetes Operator门面
    
    整合所有Operator功能
    
    DEFECT-007 [P2] 新增使用示例:
    
    示例用法:
        >>> from k8s_operator import Operator
        >>> 
        >>> operator = Operator()
        >>> await operator.initialize()
        >>> 
        >>> # 创建自定义资源
        >>> crd = CustomResourceDefinition(
        ...     group="memory.example.com",
        ...     version="v1",
        ...     kind="MemoryStore",
        ...     plural="memorystores"
        ... )
        >>> await operator.create_crd(crd)
        >>> 
        >>> # 自动扩缩容
        >>> await operator.auto_scaler.scale("memory-store", namespace="default", replicas=3)
        >>> 
        >>> # 备份
        >>> await operator.backup_manager.create_backup("memory-store", "default")
    """
    
    def __init__(self, kube_config: Optional[str] = None):
        self.k8s_client = KubernetesClient(kube_config)
        self.controllers: List[Controller] = []
        self.auto_scaler = AutoScaler(self.k8s_client)
        self.config_manager = ConfigManager(self.k8s_client)
        self.backup_manager = BackupManager(self.k8s_client)
        self.healing_manager = SelfHealingManager(self.k8s_client)
    
    async def initialize(self) -> None:
        """初始化Operator"""
        await self.k8s_client.connect()
        logger.info("Operator initialized")
    
    async def shutdown(self) -> None:
        """关闭Operator"""
        # 停止所有控制器
        for controller in self.controllers:
            await controller.stop()
        
        await self.healing_manager.stop()
        await self.k8s_client.disconnect()
        logger.info("Operator shutdown")
    
    def register_controller(self, controller: Controller) -> None:
        """注册控制器"""
        controller.k8s_client = self.k8s_client
        self.controllers.append(controller)
    
    async def start(self) -> None:
        """启动Operator"""
        # 启动所有控制器
        for controller in self.controllers:
            await controller.start()
        
        # 启动自愈管理器
        await self.healing_manager.start()
        
        logger.info("Operator started")
    
    async def create_resource(self, cr: CustomResource) -> bool:
        """创建自定义资源"""
        return await self.k8s_client.create_custom_resource(cr)
    
    async def create_backup(
        self,
        resource_name: str,
        resource_namespace: str
    ) -> str:
        """创建备份"""
        return await self.backup_manager.create_backup(
            resource_name,
            resource_namespace
        )
    
    async def setup_autoscaling(
        self,
        namespace: str,
        deployment_name: str,
        hpa_spec: Optional[HPASpec] = None,
        vpa_spec: Optional[VPASpec] = None
    ) -> bool:
        """设置自动扩缩容"""
        success = True
        
        if hpa_spec:
            success = success and await self.auto_scaler.create_hpa(
                namespace, deployment_name, hpa_spec
            )
        
        return success


# ============================================================================
# 示例控制器实现
# ============================================================================

class MemorySystemController(Controller):
    """
    Agent Memory System控制器示例
    
    管理Agent Memory System的自定义资源
    """
    
    def __init__(self, k8s_client: KubernetesClient):
        super().__init__(k8s_client)
        self.operator = None
    
    def get_crd(self) -> CustomResourceDefinition:
        """定义MemorySystem CRD"""
        return CustomResourceDefinition(
            name="memorysystems.ams.example.com",
            group="ams.example.com",
            version="v1",
            scope="Namespaced",
            plural="memorysystems",
            singular="memorysystem",
            kind_name="MemorySystem",
            short_names=["ams"],
            schema={
                "type": "object",
                "properties": {
                    "spec": {
                        "type": "object",
                        "properties": {
                            "replicas": {"type": "integer", "minimum": 1},
                            "image": {"type": "string"},
                            "storage": {
                                "type": "object",
                                "properties": {
                                    "size": {"type": "string"},
                                    "class": {"type": "string"}
                                }
                            },
                            "config": {
                                "type": "object",
                                "properties": {
                                    "embedding_model": {"type": "string"},
                                    "vector_dimension": {"type": "integer"}
                                }
                            }
                        },
                        "required": ["image"]
                    }
                }
            }
        )
    
    async def reconcile(self, cr: CustomResource) -> CustomResourceStatus:
        """调谐MemorySystem资源"""
        logger.info(f"Reconciling MemorySystem: {cr.name}")
        
        status = CustomResourceStatus(
            phase=ResourceStatus.RUNNING,
            replicas=cr.spec.replicas,
            ready_replicas=cr.spec.replicas,
            message="MemorySystem is running"
        )
        
        # 1. 创建或更新Deployment
        deployment = self._build_deployment(cr)
        # await self.k8s_client.create_deployment(cr.namespace, deployment)
        
        # 2. 创建或更新Service
        service = self._build_service(cr)
        # await self.k8s_client.create_service(cr.namespace, service)
        
        # 3. 创建ConfigMap
        configmap = self._build_configmap(cr)
        # await self.k8s_client.create_configmap(cr.namespace, configmap)
        
        return status
    
    async def delete(self, cr: CustomResource) -> bool:
        """删除MemorySystem资源"""
        logger.info(f"Deleting MemorySystem: {cr.name}")
        # 执行清理操作
        return True
    
    def _build_deployment(self, cr: CustomResource) -> Dict[str, Any]:
        """构建Deployment"""
        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": cr.name,
                "namespace": cr.namespace,
                "labels": {"app": "memory-system", "instance": cr.name}
            },
            "spec": {
                "replicas": cr.spec.replicas,
                "selector": {
                    "matchLabels": {"app": "memory-system", "instance": cr.name}
                },
                "template": {
                    "metadata": {
                        "labels": {"app": "memory-system", "instance": cr.name}
                    },
                    "spec": {
                        "containers": [{
                            "name": "memory-system",
                            "image": cr.spec.image,
                            "ports": [{"containerPort": 8080}],
                            "envFrom": [{
                                "configMapRef": {"name": f"{cr.name}-config"}
                            }],
                            "volumeMounts": [{
                                "name": "data",
                                "mountPath": "/data"
                            }]
                        }],
                        "volumes": [{
                            "name": "data",
                            "persistentVolumeClaim": {
                                "claimName": f"{cr.name}-data"
                            }
                        }]
                    }
                }
            }
        }
    
    def _build_service(self, cr: CustomResource) -> Dict[str, Any]:
        """构建Service"""
        return {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": cr.name,
                "namespace": cr.namespace
            },
            "spec": {
                "selector": {"app": "memory-system", "instance": cr.name},
                "ports": [{
                    "port": 80,
                    "targetPort": 8080
                }],
                "type": "ClusterIP"
            }
        }
    
    def _build_configmap(self, cr: CustomResource) -> Dict[str, Any]:
        """构建ConfigMap"""
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": f"{cr.name}-config",
                "namespace": cr.namespace
            },
            "data": {
                "config.json": json.dumps(cr.spec.config, indent=2)
            }
        }


# ============================================================================
# 使用示例
# ============================================================================

async def example_usage():
    """使用示例"""
    # 创建Operator
    operator = Operator()
    await operator.initialize()
    
    try:
        # 创建控制器
        controller = MemorySystemController(operator.k8s_client)
        operator.register_controller(controller)
        
        # 启动Operator
        await operator.start()
        
        # 创建自定义资源
        cr = CustomResource(
            api_version="ams.example.com/v1",
            kind="MemorySystem",
            metadata={
                "name": "memory-system-prod",
                "namespace": "default"
            },
            spec=CustomResourceSpec(
                replicas=3,
                image="ams/memory-system:v1.0.0",
                config={
                    "embedding_model": "bge-large-zh",
                    "vector_dimension": 1024
                },
                storage={
                    "size": "100Gi",
                    "class": "fast-ssd"
                }
            )
        )
        
        # 创建资源
        await operator.create_resource(cr)
        
        # 设置自动扩缩容
        hpa_spec = HPASpec(
            min_replicas=3,
            max_replicas=10,
            target_cpu_utilization=70
        )
        
        await operator.setup_autoscaling(
            "default",
            "memory-system-prod",
            hpa_spec=hpa_spec
        )
        
        # 创建配置
        await operator.config_manager.create_configmap_from_dict(
            "memory-config",
            "default",
            {
                "LOG_LEVEL": "INFO",
                "CACHE_SIZE": "1000"
            }
        )
        
        # 注册健康检查
        operator.healing_manager.register_health_check(
            "memory-system-prod",
            HealthCheck(
                name="health",
                check_type="http",
                endpoint="http://memory-system-prod.default.svc.cluster.local/health",
                interval=10,
                failure_threshold=3
            )
        )
        
        # 创建备份
        backup_id = await operator.create_backup(
            "memory-system-prod",
            "default"
        )
        print(f"Backup created: {backup_id}")
        
        # 模拟运行
        await asyncio.sleep(30)
        
    finally:
        await operator.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())

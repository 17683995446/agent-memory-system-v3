"""
增强多租户资源隔离模块 (I-08)
提供更细粒度的资源使用跟踪和配额管理
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
    from memory_system.core_abstractions.memory_data_models import MemoryItem

from memory_system.tenant_isolation_manager import (
    TenantIsolationManager,
    TenantContext,
    TenantAwareStorageWrapper,
    QuotaExceededError
)
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class ResourceType(str, Enum):
    """资源类型"""
    MEMORY_COUNT = "memory_count"
    STORAGE_BYTES = "storage_bytes"
    QUERY_COUNT = "query_count"
    EMBEDDING_CALLS = "embedding_calls"
    BANDWIDTH_BYTES = "bandwidth_bytes"


@dataclass
class TenantResourceUsage:
    """
    租户资源使用情况

    Attributes:
        memory_count: 记忆数量
        storage_bytes: 存储字节数
        query_count: 查询次数
        embedding_calls: 嵌入调用次数
        bandwidth_bytes: 带宽使用量
        last_updated: 最后更新时间
    """
    memory_count: int = 0
    storage_bytes: int = 0
    query_count: int = 0
    embedding_calls: int = 0
    bandwidth_bytes: int = 0
    last_updated: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "memory_count": self.memory_count,
            "storage_bytes": self.storage_bytes,
            "query_count": self.query_count,
            "embedding_calls": self.embedding_calls,
            "bandwidth_bytes": self.bandwidth_bytes,
            "last_updated": self.last_updated,
        }


@dataclass
class TenantQuota:
    """
    租户资源配额

    Attributes:
        max_memories: 最大记忆数量
        max_storage_bytes: 最大存储字节数
        max_queries_per_minute: 每分钟最大查询次数
        max_embedding_calls_per_minute: 每分钟最大嵌入调用次数
        max_bandwidth_bytes_per_day: 每天最大带宽使用量
    """
    max_memories: int = 10000
    max_storage_bytes: int = 100 * 1024 * 1024  # 100MB
    max_queries_per_minute: int = 1000
    max_embedding_calls_per_minute: int = 500
    max_bandwidth_bytes_per_day: int = 1024 * 1024 * 1024  # 1GB

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "max_memories": self.max_memories,
            "max_storage_bytes": self.max_storage_bytes,
            "max_queries_per_minute": self.max_queries_per_minute,
            "max_embedding_calls_per_minute": self.max_embedding_calls_per_minute,
            "max_bandwidth_bytes_per_day": self.max_bandwidth_bytes_per_day,
        }


class EnhancedTenantIsolationManager(TenantIsolationManager):
    """
    增强型多租户隔离管理器

    在基础TenantIsolationManager之上增加了:
        - 细粒度的资源使用跟踪
        - 多维度配额管理
        - 资源使用历史记录
        - 实时资源监控

    使用示例:
        >>> manager = EnhancedTenantIsolationManager()
        >>> await manager.record_memory_add("tenant_001", content_size=1024)
        >>> await manager.record_query("tenant_001")
        >>> is_allowed = await manager.check_resource_quota("tenant_001", ResourceType.MEMORY_COUNT)
    """

    def __init__(
        self,
        default_tenant: str = "default",
        enable_quota: bool = True,
        max_memories_per_tenant: int = 10000,
        max_short_term_per_tenant: int = 100,
        enable_rate_limiting: bool = True,
        rate_limit_window_seconds: float = 60.0,
    ):
        """
        初始化增强型租户隔离管理器

        Args:
            default_tenant: 默认租户ID
            enable_quota: 是否启用配额限制
            max_memories_per_tenant: 每租户最大记忆数
            max_short_term_per_tenant: 每租户最大短期记忆数
            enable_rate_limiting: 是否启用速率限制
            rate_limit_window_seconds: 速率限制窗口（秒）
        """
        super().__init__(
            default_tenant=default_tenant,
            enable_quota=enable_quota,
            max_memories_per_tenant=max_memories_per_tenant,
            max_short_term_per_tenant=max_short_term_per_tenant,
        )

        # 资源使用跟踪
        self._resource_usage: Dict[str, TenantResourceUsage] = {}
        # 租户配额
        self._tenant_quotas: Dict[str, TenantQuota] = {}
        # 速率限制窗口
        self._rate_limit_window = rate_limit_window_seconds
        self._enable_rate_limiting = enable_rate_limiting

        # 查询历史（用于速率限制）
        self._query_history: Dict[str, List[float]] = {}
        self._embedding_history: Dict[str, List[float]] = {}

        # 资源使用历史（用于趋势分析）
        self._usage_history: Dict[str, List[Dict[str, Any]]] = {}
        self._max_history_size = 1000

        # 锁保护
        self._resource_lock = asyncio.Lock()

    async def record_memory_add(self, tenant_id: str, content_size: int) -> None:
        """
        记录记忆添加

        Args:
            tenant_id: 租户ID
            content_size: 内容大小（字节）
        """
        async with self._resource_lock:
            usage = self._resource_usage.setdefault(tenant_id, TenantResourceUsage())
            usage.memory_count += 1
            usage.storage_bytes += content_size
            usage.last_updated = time.time()
            await self._add_to_history(tenant_id, "memory_add", {"size": content_size})

    async def record_memory_delete(self, tenant_id: str, content_size: int) -> None:
        """
        记录记忆删除

        Args:
            tenant_id: 租户ID
            content_size: 内容大小（字节）
        """
        async with self._resource_lock:
            usage = self._resource_usage.setdefault(tenant_id, TenantResourceUsage())
            usage.memory_count = max(0, usage.memory_count - 1)
            usage.storage_bytes = max(0, usage.storage_bytes - content_size)
            usage.last_updated = time.time()

    async def record_query(self, tenant_id: str) -> None:
        """
        记录查询操作

        Args:
            tenant_id: 租户ID
        """
        async with self._resource_lock:
            usage = self._resource_usage.setdefault(tenant_id, TenantResourceUsage())
            usage.query_count += 1
            usage.last_updated = time.time()

            # 添加到查询历史
            if tenant_id not in self._query_history:
                self._query_history[tenant_id] = []
            self._query_history[tenant_id].append(time.time())

            # 清理过期历史
            await self._cleanup_rate_history(tenant_id, self._query_history)

    async def record_embedding_call(self, tenant_id: str, input_bytes: int = 0) -> None:
        """
        记录嵌入调用

        Args:
            tenant_id: 租户ID
            input_bytes: 输入数据大小
        """
        async with self._resource_lock:
            usage = self._resource_usage.setdefault(tenant_id, TenantResourceUsage())
            usage.embedding_calls += 1
            usage.bandwidth_bytes += input_bytes
            usage.last_updated = time.time()

            # 添加到嵌入调用历史
            if tenant_id not in self._embedding_history:
                self._embedding_history[tenant_id] = []
            self._embedding_history[tenant_id].append(time.time())

            # 清理过期历史
            await self._cleanup_rate_history(tenant_id, self._embedding_history)

    async def check_resource_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType
    ) -> bool:
        """
        检查资源配额

        Args:
            tenant_id: 租户ID
            resource_type: 资源类型

        Returns:
            True 如果在配额内，False 如果超出配额
        """
        if not self.enable_quota:
            return True

        async with self._resource_lock:
            usage = self._resource_usage.get(tenant_id, TenantResourceUsage())
            quota = self._tenant_quotas.get(tenant_id, TenantQuota())

            if resource_type == ResourceType.MEMORY_COUNT:
                return usage.memory_count < quota.max_memories
            elif resource_type == ResourceType.STORAGE_BYTES:
                return usage.storage_bytes < quota.max_storage_bytes
            elif resource_type == ResourceType.QUERY_COUNT:
                if not self._enable_rate_limiting:
                    return True
                return await self._check_rate_limit(
                    tenant_id, self._query_history, quota.max_queries_per_minute
                )
            elif resource_type == ResourceType.EMBEDDING_CALLS:
                if not self._enable_rate_limiting:
                    return True
                return await self._check_rate_limit(
                    tenant_id, self._embedding_history, quota.max_embedding_calls_per_minute
                )
            elif resource_type == ResourceType.BANDWIDTH_BYTES:
                # 简化为检查总带宽
                return usage.bandwidth_bytes < quota.max_bandwidth_bytes_per_day

            return True

    async def check_all_quotas(self, tenant_id: str) -> Dict[str, bool]:
        """
        检查所有资源配额

        Args:
            tenant_id: 租户ID

        Returns:
            {资源类型: 是否通过} 字典
        """
        results = {}
        for resource_type in ResourceType:
            results[resource_type.value] = await self.check_resource_quota(tenant_id, resource_type)
        return results

    async def _check_rate_limit(
        self,
        tenant_id: str,
        history: Dict[str, List[float]],
        limit: int
    ) -> bool:
        """检查速率限制"""
        tenant_history = history.get(tenant_id, [])
        now = time.time()
        window_start = now - self._rate_limit_window

        # 计算窗口内的请求数
        recent_count = sum(1 for t in tenant_history if t > window_start)
        return recent_count < limit

    async def _cleanup_rate_history(self, tenant_id: str, history: Dict[str, List[float]]) -> None:
        """清理过期的速率历史记录"""
        if tenant_id not in history:
            return

        now = time.time()
        cutoff = now - self._rate_limit_window
        history[tenant_id] = [t for t in history[tenant_id] if t > cutoff]

    async def _add_to_history(self, tenant_id: str, event_type: str, data: Dict[str, Any]) -> None:
        """添加事件到历史记录"""
        if tenant_id not in self._usage_history:
            self._usage_history[tenant_id] = []

        self._usage_history[tenant_id].append({
            "timestamp": time.time(),
            "type": event_type,
            **data
        })

        # 限制历史记录大小
        if len(self._usage_history[tenant_id]) > self._max_history_size:
            self._usage_history[tenant_id] = self._usage_history[tenant_id][-self._max_history_size:]

    def set_tenant_quota(self, tenant_id: str, quota: TenantQuota) -> None:
        """
        设置租户配额

        Args:
            tenant_id: 租户ID
            quota: 配额配置
        """
        self._tenant_quotas[tenant_id] = quota
        logger.info(f"已设置租户 {tenant_id} 的配额")

    def get_tenant_quota(self, tenant_id: str) -> TenantQuota:
        """
        获取租户配额

        Args:
            tenant_id: 租户ID

        Returns:
            配额配置
        """
        return self._tenant_quotas.get(tenant_id, TenantQuota())

    def get_tenant_resource_usage(self, tenant_id: str) -> TenantResourceUsage:
        """
        获取租户资源使用

        Args:
            tenant_id: 租户ID

        Returns:
            资源使用情况
        """
        return self._resource_usage.get(tenant_id, TenantResourceUsage())

    def get_enhanced_tenant_stats(self, tenant_id: str) -> Dict[str, Any]:
        """
        获取增强的租户统计信息

        Args:
            tenant_id: 租户ID

        Returns:
            详细的统计信息
        """
        usage = self._resource_usage.get(tenant_id, TenantResourceUsage())
        quota = self._tenant_quotas.get(tenant_id, TenantQuota())
        basic_stats = self.get_tenant_stats(tenant_id)

        # 计算使用率
        memory_usage_rate = usage.memory_count / max(quota.max_memories, 1)
        storage_usage_rate = usage.storage_bytes / max(quota.max_storage_bytes, 1)

        return {
            **basic_stats,
            "resource_usage": usage.to_dict(),
            "quota": quota.to_dict(),
            "usage_rates": {
                "memory": memory_usage_rate,
                "storage": storage_usage_rate,
            },
            "query_rate_current": len(self._query_history.get(tenant_id, [])),
            "embedding_rate_current": len(self._embedding_history.get(tenant_id, [])),
        }

    def get_all_tenant_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有租户的统计信息

        Returns:
            {tenant_id: stats} 字典
        """
        all_tenants = set(self._resource_usage.keys()) | set(self._tenant_counts.keys())
        return {
            tenant_id: self.get_enhanced_tenant_stats(tenant_id)
            for tenant_id in all_tenants
        }

    async def reset_tenant_usage(self, tenant_id: str) -> None:
        """
        重置租户资源使用统计

        Args:
            tenant_id: 租户ID
        """
        async with self._resource_lock:
            if tenant_id in self._resource_usage:
                del self._resource_usage[tenant_id]
            if tenant_id in self._query_history:
                del self._query_history[tenant_id]
            if tenant_id in self._embedding_history:
                del self._embedding_history[tenant_id]
            if tenant_id in self._usage_history:
                del self._usage_history[tenant_id]

    async def cleanup_inactive_tenants(self, inactive_threshold_hours: float = 24.0) -> List[str]:
        """
        清理不活跃的租户数据

        Args:
            inactive_threshold_hours: 不活跃阈值（小时）

        Returns:
            被清理的租户ID列表
        """
        now = time.time()
        threshold_seconds = inactive_threshold_hours * 3600
        cleaned = []

        async with self._resource_lock:
            for tenant_id, usage in list(self._resource_usage.items()):
                if now - usage.last_updated > threshold_seconds:
                    await self.reset_tenant_usage(tenant_id)
                    cleaned.append(tenant_id)

        if cleaned:
            logger.info(f"已清理 {len(cleaned)} 个不活跃租户的数据")

        return cleaned

"""
多租户隔离管理器
支持按租户隔离记忆数据，确保数据安全
"""

import asyncio
import hashlib
import re
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
    from memory_system.core_abstractions.memory_data_models import MemoryItem

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class TenantContext:
    """
    租户上下文
    用于在操作中传递租户信息
    """
    
    def __init__(
        self,
        tenant_id: str,
        tenant_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.tenant_id = tenant_id
        self.tenant_name = tenant_name or tenant_id
        self.metadata = metadata or {}
    
    def __repr__(self) -> str:
        return f"TenantContext({self.tenant_id})"
    
    def get_namespace_key(self) -> str:
        """获取命名空间键（用于存储前缀）"""
        return f"tenant:{self.tenant_id}:"


class TenantIsolationManager:
    """
    多租户隔离管理器
    
    功能:
        - 为每个租户创建独立的命名空间
        - 存储隔离：租户A无法访问租户B的数据
        - 配额管理：限制每个租户的资源使用
        - 审计日志：记录租户操作
    
    使用示例:
        >>> manager = TenantIsolationManager()
        >>> context = TenantContext("tenant_001", "客户A")
        >>> 
        >>> # 存储时带上租户上下文
        >>> memory_id = await storage.add(item, tenant_context=context)
        >>> 
        >>> # 检索时只返回该租户的数据
        >>> results = await storage.search_by_vector(vec, tenant_context=context)
    """
    
    def __init__(
        self,
        default_tenant: str = "default",
        enable_quota: bool = True,
        max_memories_per_tenant: int = 10000,
        max_short_term_per_tenant: int = 100
    ):
        self.default_tenant = default_tenant
        self.enable_quota = enable_quota
        self.max_memories_per_tenant = max_memories_per_tenant
        self.max_short_term_per_tenant = max_short_term_per_tenant
        
        # 租户配额跟踪
        self._tenant_counts: Dict[str, int] = {}
        self._tenant_short_term_counts: Dict[str, int] = {}
        self._lock = asyncio.Lock()
    
    def create_context(
        self,
        tenant_id: Optional[str] = None,
        tenant_name: Optional[str] = None,
        **metadata
    ) -> TenantContext:
        """创建租户上下文"""
        return TenantContext(
            tenant_id=tenant_id or self.default_tenant,
            tenant_name=tenant_name,
            metadata=metadata
        )
    
    def get_tenant_prefix(self, tenant_id: str) -> str:
        """获取租户的存储前缀"""
        return f"tenant:{tenant_id}:"
    
    def add_tenant_prefix(self, memory_id: str, tenant_id: str) -> str:
        """为记忆ID添加租户前缀"""
        prefix = self.get_tenant_prefix(tenant_id)
        if memory_id.startswith(prefix):
            return memory_id
        return f"{prefix}{memory_id}"
    
    def remove_tenant_prefix(self, memory_id: str, tenant_id: str) -> str:
        """从记忆ID移除租户前缀"""
        prefix = self.get_tenant_prefix(tenant_id)
        if memory_id.startswith(prefix):
            return memory_id[len(prefix):]
        return memory_id
    
    def check_quota(
        self,
        tenant_id: str,
        is_short_term: bool = False
    ) -> bool:
        """检查租户是否超出配额"""
        if not self.enable_quota:
            return True
        
        if is_short_term:
            count = self._tenant_short_term_counts.get(tenant_id, 0)
            return count < self.max_short_term_per_tenant
        else:
            count = self._tenant_counts.get(tenant_id, 0)
            return count < self.max_memories_per_tenant
    
    async def check_and_increment_quota(
        self,
        tenant_id: str,
        is_short_term: bool = False
    ) -> bool:
        """
        原子操作：检查配额并在未超限时递增计数
        
        解决 TOCTOU (Time-of-check to time-of-use) 竞态条件问题
        
        Args:
            tenant_id: 租户ID
            is_short_term: 是否为短期记忆
            
        Returns:
            True 如果配额检查通过并已递增，False 如果超出配额
        """
        async with self._lock:
            if not self.enable_quota:
                # 即使禁用配额，也要递增计数用于统计
                if is_short_term:
                    self._tenant_short_term_counts[tenant_id] = \
                        self._tenant_short_term_counts.get(tenant_id, 0) + 1
                else:
                    self._tenant_counts[tenant_id] = \
                        self._tenant_counts.get(tenant_id, 0) + 1
                return True
            
            if is_short_term:
                count = self._tenant_short_term_counts.get(tenant_id, 0)
                if count >= self.max_short_term_per_tenant:
                    return False
                self._tenant_short_term_counts[tenant_id] = count + 1
            else:
                count = self._tenant_counts.get(tenant_id, 0)
                if count >= self.max_memories_per_tenant:
                    return False
                self._tenant_counts[tenant_id] = count + 1
            
            return True
    
    async def increment_count(self, tenant_id: str, is_short_term: bool = False) -> None:
        """增加租户记忆计数"""
        async with self._lock:
            if is_short_term:
                self._tenant_short_term_counts[tenant_id] = \
                    self._tenant_short_term_counts.get(tenant_id, 0) + 1
            else:
                self._tenant_counts[tenant_id] = \
                    self._tenant_counts.get(tenant_id, 0) + 1

    async def decrement_count(self, tenant_id: str, is_short_term: bool = False) -> None:
        """减少租户记忆计数"""
        async with self._lock:
            if is_short_term:
                self._tenant_short_term_counts[tenant_id] = max(
                    0, self._tenant_short_term_counts.get(tenant_id, 0) - 1
                )
            else:
                self._tenant_counts[tenant_id] = max(
                    0, self._tenant_counts.get(tenant_id, 0) - 1
                )
    
    def get_tenant_stats(self, tenant_id: str) -> Dict[str, int]:
        """获取租户统计信息"""
        return {
            "long_term_count": self._tenant_counts.get(tenant_id, 0),
            "short_term_count": self._tenant_short_term_counts.get(tenant_id, 0),
            "max_long_term": self.max_memories_per_tenant,
            "max_short_term": self.max_short_term_per_tenant,
        }
    
    def list_tenants(self) -> List[str]:
        """列出所有租户ID"""
        tenants = set(self._tenant_counts.keys())
        tenants.update(self._tenant_short_term_counts.keys())
        return sorted(list(tenants))
    
    def validate_tenant_id(self, tenant_id: str) -> bool:
        """
        验证租户ID格式
        
        验证规则:
            - 不能为空
            - 不能包含冒号 (用于命名空间分隔)
            - 长度不能超过64字符
            - 只允许字母、数字、下划线和连字符
        
        Args:
            tenant_id: 要验证的租户ID
            
        Returns:
            True 如果租户ID有效，False 否则
        """
        if not tenant_id:
            return False
        if ":" in tenant_id:
            return False
        if len(tenant_id) > 64:
            return False
        # 特殊字符验证：只允许字母、数字、下划线和连字符
        if not re.match(r'^[a-zA-Z0-9_-]+$', tenant_id):
            return False
        return True


class TenantAwareStorageWrapper:
    """
    租户感知的存储包装器
    
    包装任意 StorageBackend，添加租户隔离功能。
    自动为记忆ID添加租户前缀，确保不同租户的数据完全隔离。
    
    Attributes:
        storage: 底层存储后端
        tenant_manager: 租户隔离管理器
        tenant_context: 当前租户上下文
    
    使用示例:
        >>> wrapper = TenantAwareStorageWrapper(storage, tenant_manager, context)
        >>> memory_id = await wrapper.add(memory_item)
        >>> results = await wrapper.search_by_vector(query_vector)
    """
    
    def __init__(
        self,
        storage: "StorageBackend",
        tenant_manager: TenantIsolationManager,
        tenant_context: Optional[TenantContext] = None
    ):
        self.storage = storage
        self.tenant_manager = tenant_manager
        self.tenant_context = tenant_context
    
    def _get_tenant_id(self) -> str:
        """获取当前租户ID"""
        if self.tenant_context:
            return self.tenant_context.tenant_id
        return self.tenant_manager.default_tenant
    
    def _prefix_id(self, memory_id: str) -> str:
        """为ID添加租户前缀"""
        return self.tenant_manager.add_tenant_prefix(
            memory_id, self._get_tenant_id()
        )
    
    def _unprefix_id(self, memory_id: str) -> str:
        """从ID移除租户前缀"""
        return self.tenant_manager.remove_tenant_prefix(
            memory_id, self._get_tenant_id()
        )
    
    async def add(self, item: "MemoryItem") -> str:
        """
        添加记忆（带租户前缀）
        
        Args:
            item: 要添加的记忆项
            
        Returns:
            不带租户前缀的记忆ID
            
        Raises:
            QuotaExceededError: 如果租户超出配额限制
        """
        tenant_id = self._get_tenant_id()
        
        # 使用原子操作检查配额并递增计数（解决 TOCTOU 问题）
        if not await self.tenant_manager.check_and_increment_quota(tenant_id):
            raise QuotaExceededError(
                f"租户 {tenant_id} 超出配额限制"
            )
        
        # 添加租户前缀到ID
        prefixed_id = self._prefix_id(item.memory_id)
        item.memory_id = prefixed_id
        
        result = await self.storage.add(item)
        
        return self._unprefix_id(result)
    
    async def get(self, memory_id: str) -> Optional["MemoryItem"]:
        """
        获取记忆（带租户前缀）
        
        Args:
            memory_id: 不带租户前缀的记忆ID
            
        Returns:
            记忆项，如果不存在则返回 None
        """
        prefixed_id = self._prefix_id(memory_id)
        return await self.storage.get(prefixed_id)
    
    async def delete(self, memory_id: str) -> bool:
        """
        删除记忆（带租户前缀）
        
        Args:
            memory_id: 不带租户前缀的记忆ID
            
        Returns:
            True 如果删除成功，False 如果记忆不存在
        """
        prefixed_id = self._prefix_id(memory_id)
        result = await self.storage.delete(prefixed_id)
        if result:
            await self.tenant_manager.decrement_count(self._get_tenant_id())
        return result
    
    async def search_by_vector(
        self,
        query_vector: List[float],
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List:
        """
        向量搜索（带租户过滤）
        
        Args:
            query_vector: 查询向量
            top_k: 返回结果数量上限
            filters: 额外的过滤条件
            
        Returns:
            匹配的记忆项列表
        """
        # 添加租户过滤
        tenant_filter = {"tenant_id": self._get_tenant_id()}
        if filters:
            filters.update(tenant_filter)
        else:
            filters = tenant_filter

        return await self.storage.search_by_vector(query_vector, top_k, filters)

    async def search_by_time_range(
        self,
        start_time: float,
        end_time: float,
        memory_type: Optional[str] = None
    ) -> List["MemoryItem"]:
        """
        按时间范围搜索（带租户过滤）
        
        Args:
            start_time: 开始时间戳
            end_time: 结束时间戳
            memory_type: 可选的记忆类型过滤
            
        Returns:
            匹配的记忆项列表
        """
        results = await self.storage.search_by_time_range(start_time, end_time, memory_type)
        # 过滤只返回当前租户的记忆
        prefix = self.tenant_manager.get_tenant_prefix(self._get_tenant_id())
        return [m for m in results if m.memory_id.startswith(prefix)]

    async def count(self, memory_type: Optional[str] = None) -> int:
        """
        获取当前租户的记忆数量
        
        Args:
            memory_type: 可选的记忆类型过滤
            
        Returns:
            记忆数量
        """
        return self.tenant_manager.get_tenant_stats(self._get_tenant_id())["long_term_count"]

    async def close(self) -> None:
        """关闭底层存储"""
        return await self.storage.close()


class QuotaExceededError(Exception):
    """配额超出错误"""
    pass

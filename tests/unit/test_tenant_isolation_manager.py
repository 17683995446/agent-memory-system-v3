"""
租户隔离管理器测试
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from memory_system.tenant_isolation_manager import (
    TenantContext,
    TenantIsolationManager,
    TenantAwareStorageWrapper,
    QuotaExceededError,
)


class TestTenantContext:
    """测试租户上下文"""
    
    def test_create_context(self):
        """test_create_context - 创建租户上下文"""
        context = TenantContext("tenant_001", "客户A")
        assert context.tenant_id == "tenant_001"
        assert context.tenant_name == "客户A"
        assert context.metadata == {}
    
    def test_create_context_with_metadata(self):
        """创建带元数据的租户上下文"""
        context = TenantContext(
            "tenant_002",
            "客户B",
            metadata={"region": "cn", "plan": "premium"}
        )
        assert context.tenant_id == "tenant_002"
        assert context.metadata == {"region": "cn", "plan": "premium"}
    
    def test_create_context_default_name(self):
        """创建租户上下文使用默认名称"""
        context = TenantContext("tenant_003")
        assert context.tenant_name == "tenant_003"
    
    def test_get_namespace_key(self):
        """获取命名空间键"""
        context = TenantContext("tenant_001")
        assert context.get_namespace_key() == "tenant:tenant_001:"
    
    def test_repr(self):
        """测试字符串表示"""
        context = TenantContext("tenant_001")
        assert repr(context) == "TenantContext(tenant_001)"


class TestTenantIsolationManager:
    """测试租户隔离管理器"""
    
    @pytest.fixture
    def manager(self):
        return TenantIsolationManager()
    
    def test_create_context(self, manager):
        """test_create_context - 创建租户上下文"""
        context = manager.create_context("tenant_001", "客户A")
        assert isinstance(context, TenantContext)
        assert context.tenant_id == "tenant_001"
        assert context.tenant_name == "客户A"
    
    def test_create_context_default(self, manager):
        """使用默认租户创建上下文"""
        context = manager.create_context()
        assert context.tenant_id == "default"
    
    def test_create_context_with_metadata(self, manager):
        """创建带元数据的上下文"""
        context = manager.create_context(
            "tenant_001",
            metadata={"region": "cn"}
        )
        assert context.metadata == {"metadata": {"region": "cn"}}
    
    def test_tenant_prefix(self, manager):
        """test_tenant_prefix - 租户前缀添加/移除"""
        # 测试获取前缀
        prefix = manager.get_tenant_prefix("tenant_001")
        assert prefix == "tenant:tenant_001:"
        
        # 测试添加前缀
        memory_id = "mem_123"
        prefixed = manager.add_tenant_prefix(memory_id, "tenant_001")
        assert prefixed == "tenant:tenant_001:mem_123"
        
        # 测试重复添加前缀（不应重复）
        reprefixed = manager.add_tenant_prefix(prefixed, "tenant_001")
        assert reprefixed == "tenant:tenant_001:mem_123"
        
        # 测试移除前缀
        unprefixed = manager.remove_tenant_prefix(prefixed, "tenant_001")
        assert unprefixed == "mem_123"
        
        # 测试移除不存在的前缀
        result = manager.remove_tenant_prefix("mem_123", "tenant_001")
        assert result == "mem_123"
    
    @pytest.mark.asyncio
    async def test_quota_check(self, manager):
        """test_quota_check - 配额检查"""
        # 初始状态应在配额内
        assert manager.check_quota("tenant_001") is True
        assert manager.check_quota("tenant_001", is_short_term=True) is True
        
        # 增加计数后仍应在配额内
        await manager.increment_count("tenant_001")
        assert manager.check_quota("tenant_001") is True
        
        # 增加短期记忆计数
        await manager.increment_count("tenant_001", is_short_term=True)
        assert manager.check_quota("tenant_001", is_short_term=True) is True
    
    @pytest.mark.asyncio
    async def test_quota_exceeded(self, manager):
        """test_quota_exceeded - 超出配额抛出异常"""
        # 设置很小的配额
        manager.max_memories_per_tenant = 3
        
        # 添加2个记忆，应成功
        await manager.increment_count("tenant_001")
        await manager.increment_count("tenant_001")
        assert manager.check_quota("tenant_001") is True
        
        # 添加第3个，仍在配额内
        await manager.increment_count("tenant_001")
        assert manager.check_quota("tenant_001") is False
        
        # 测试短期记忆配额
        manager.max_short_term_per_tenant = 1
        await manager.increment_count("tenant_001", is_short_term=True)
        assert manager.check_quota("tenant_001", is_short_term=True) is False
    
    @pytest.mark.asyncio
    async def test_decrement_count(self, manager):
        """减少计数"""
        await manager.increment_count("tenant_001")
        await manager.increment_count("tenant_001")
        assert manager._tenant_counts.get("tenant_001") == 2
        
        await manager.decrement_count("tenant_001")
        assert manager._tenant_counts.get("tenant_001") == 1
        
        # 测试不会减到负数
        await manager.decrement_count("tenant_001")
        await manager.decrement_count("tenant_001")
        await manager.decrement_count("tenant_001")
        assert manager._tenant_counts.get("tenant_001") == 0
    
    @pytest.mark.asyncio
    async def test_tenant_stats(self, manager):
        """test_tenant_stats - 租户统计"""
        # 添加一些记忆
        await manager.increment_count("tenant_001")
        await manager.increment_count("tenant_001")
        await manager.increment_count("tenant_001", is_short_term=True)
        
        stats = manager.get_tenant_stats("tenant_001")
        assert stats["long_term_count"] == 2
        assert stats["short_term_count"] == 1
        assert stats["max_long_term"] == 10000
        assert stats["max_short_term"] == 100
    
    def test_tenant_stats_empty(self, manager):
        """获取空租户统计"""
        stats = manager.get_tenant_stats("non_existent")
        assert stats["long_term_count"] == 0
        assert stats["short_term_count"] == 0
    
    @pytest.mark.asyncio
    async def test_list_tenants(self, manager):
        """test_list_tenants - 列出租户"""
        # 添加不同租户的记忆
        await manager.increment_count("tenant_a")
        await manager.increment_count("tenant_b")
        await manager.increment_count("tenant_c", is_short_term=True)
        
        tenants = manager.list_tenants()
        assert tenants == ["tenant_a", "tenant_b", "tenant_c"]
    
    def test_list_tenants_empty(self, manager):
        """列出空租户列表"""
        assert manager.list_tenants() == []
    
    def test_validate_tenant_id(self, manager):
        """test_validate_tenant_id - 验证租户ID格式"""
        # 有效ID
        assert manager.validate_tenant_id("tenant_001") is True
        assert manager.validate_tenant_id("abc123") is True
        assert manager.validate_tenant_id("a") is True
        
        # 无效ID - 空字符串
        assert manager.validate_tenant_id("") is False
        
        # 无效ID - 包含冒号
        assert manager.validate_tenant_id("tenant:001") is False
        
        # 无效ID - 超过64字符
        assert manager.validate_tenant_id("a" * 65) is False
        
        # 边界值 - 正好64字符
        assert manager.validate_tenant_id("a" * 64) is True
    
    def test_quota_disabled(self):
        """禁用配额检查"""
        manager = TenantIsolationManager(enable_quota=False)
        # 即使超出配额也应返回True
        manager._tenant_counts["tenant_001"] = 999999
        assert manager.check_quota("tenant_001") is True


class TestTenantAwareStorageWrapper:
    """测试租户感知存储包装器"""
    
    @pytest.fixture
    def mock_storage(self):
        storage = MagicMock()
        storage.add = AsyncMock(return_value="prefixed_mem_123")
        storage.get = AsyncMock(return_value=None)
        storage.delete = AsyncMock(return_value=True)
        storage.search_by_vector = AsyncMock(return_value=[])
        return storage
    
    @pytest.fixture
    def tenant_manager(self):
        return TenantIsolationManager()
    
    @pytest.fixture
    def tenant_context(self):
        return TenantContext("tenant_001", "客户A")
    
    @pytest.mark.asyncio
    async def test_wrapper_add_with_prefix(self, mock_storage, tenant_manager, tenant_context):
        """test_wrapper_add_with_prefix - 包装器添加带前缀"""
        # 修改 mock 返回带前缀的ID
        mock_storage.add = AsyncMock(return_value="tenant:tenant_001:mem_123")
        
        wrapper = TenantAwareStorageWrapper(
            mock_storage, tenant_manager, tenant_context
        )
        
        # 创建模拟记忆项
        item = MagicMock()
        item.memory_id = "mem_123"
        
        result = await wrapper.add(item)
        
        # 验证ID被添加了前缀
        assert item.memory_id == "tenant:tenant_001:mem_123"
        
        # 验证返回结果移除了前缀
        assert result == "mem_123"
        
        # 验证调用了底层存储
        mock_storage.add.assert_called_once_with(item)
        
        # 验证计数增加
        assert tenant_manager._tenant_counts.get("tenant_001") == 1
    
    @pytest.mark.asyncio
    async def test_wrapper_add_quota_exceeded(self, mock_storage, tenant_manager, tenant_context):
        """test_quota_exceeded - 超出配额抛出异常"""
        tenant_manager.max_memories_per_tenant = 1
        await tenant_manager.increment_count("tenant_001")  # 已达到配额
        
        wrapper = TenantAwareStorageWrapper(
            mock_storage, tenant_manager, tenant_context
        )
        
        item = MagicMock()
        item.memory_id = "mem_123"
        
        with pytest.raises(QuotaExceededError) as exc_info:
            await wrapper.add(item)
        
        assert "tenant_001" in str(exc_info.value)
        assert "超出配额" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_wrapper_get_with_prefix(self, mock_storage, tenant_manager, tenant_context):
        """测试包装器获取带前缀"""
        wrapper = TenantAwareStorageWrapper(
            mock_storage, tenant_manager, tenant_context
        )
        
        await wrapper.get("mem_123")
        
        # 验证调用了底层存储，且ID带前缀
        mock_storage.get.assert_called_once_with("tenant:tenant_001:mem_123")
    
    @pytest.mark.asyncio
    async def test_wrapper_delete_with_prefix(self, mock_storage, tenant_manager, tenant_context):
        """测试包装器删除带前缀"""
        await tenant_manager.increment_count("tenant_001")
        
        wrapper = TenantAwareStorageWrapper(
            mock_storage, tenant_manager, tenant_context
        )
        
        result = await wrapper.delete("mem_123")
        
        # 验证调用了底层存储，且ID带前缀
        mock_storage.delete.assert_called_once_with("tenant:tenant_001:mem_123")
        assert result is True
        
        # 验证计数减少
        assert tenant_manager._tenant_counts.get("tenant_001") == 0
    
    @pytest.mark.asyncio
    async def test_wrapper_search_with_filter(self, mock_storage, tenant_manager, tenant_context):
        """test_wrapper_search_with_filter - 包装器搜索带过滤"""
        wrapper = TenantAwareStorageWrapper(
            mock_storage, tenant_manager, tenant_context
        )
        
        query_vector = [0.1, 0.2, 0.3]
        await wrapper.search_by_vector(query_vector, top_k=5)
        
        # 验证调用了底层存储，且添加了租户过滤
        mock_storage.search_by_vector.assert_called_once()
        call_args = mock_storage.search_by_vector.call_args
        # call_args[0] 是位置参数，call_args[1] 是关键字参数
        assert call_args[0][0] == query_vector
        assert call_args[0][1] == 5  # top_k
        assert call_args[0][2] == {"tenant_id": "tenant_001"}  # filters
    
    @pytest.mark.asyncio
    async def test_wrapper_search_with_existing_filters(self, mock_storage, tenant_manager, tenant_context):
        """测试包装器搜索合并过滤条件"""
        wrapper = TenantAwareStorageWrapper(
            mock_storage, tenant_manager, tenant_context
        )
        
        query_vector = [0.1, 0.2, 0.3]
        existing_filters = {"type": "conversation", "score": 0.8}
        await wrapper.search_by_vector(query_vector, top_k=10, filters=existing_filters)
        
        # 验证过滤器被合并
        call_args = mock_storage.search_by_vector.call_args
        expected_filters = {"type": "conversation", "score": 0.8, "tenant_id": "tenant_001"}
        # call_args[0] 是位置参数 (query_vector, top_k, filters)
        assert call_args[0][2] == expected_filters
    
    def test_wrapper_default_tenant(self, mock_storage, tenant_manager):
        """测试包装器使用默认租户"""
        wrapper = TenantAwareStorageWrapper(mock_storage, tenant_manager)
        
        # 未提供tenant_context时应使用默认租户
        assert wrapper._get_tenant_id() == "default"
    
    def test_wrapper_prefix_unprefix(self, mock_storage, tenant_manager, tenant_context):
        """测试包装器前缀添加和移除"""
        wrapper = TenantAwareStorageWrapper(
            mock_storage, tenant_manager, tenant_context
        )
        
        # 测试添加前缀
        prefixed = wrapper._prefix_id("mem_123")
        assert prefixed == "tenant:tenant_001:mem_123"
        
        # 测试移除前缀
        unprefixed = wrapper._unprefix_id("tenant:tenant_001:mem_123")
        assert unprefixed == "mem_123"
        
        # 测试移除不存在的前缀
        result = wrapper._unprefix_id("mem_123")
        assert result == "mem_123"


class TestQuotaExceededError:
    """测试配额超出错误"""
    
    def test_quota_exceeded_error(self):
        """测试配额超出错误异常"""
        error = QuotaExceededError("租户 tenant_001 超出配额限制")
        assert str(error) == "租户 tenant_001 超出配额限制"
        assert isinstance(error, Exception)

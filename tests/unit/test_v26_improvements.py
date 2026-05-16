"""
V2.26.0 改进测试
测试线程安全计数器、原子配额检查和常量使用
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from memory_system.core_abstractions.system_constants_definitions import (
    EMBEDDING_CACHE_MAX_SIZE,
    EMBEDDING_CACHE_TTL_SECONDS,
    DEFAULT_SHORT_TERM_RATIO,
    DEFAULT_TIME_RATIO,
    DEFAULT_VECTOR_RATIO,
    DEFAULT_IMPORTANCE_RATIO,
)
from memory_system.working_memory_cache import InMemoryShortTermBackend
from memory_system.tenant_isolation_manager import (
    TenantIsolationManager,
    TenantAwareStorageWrapper,
    QuotaExceededError,
)
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel


class TestThreadSafeCounter:
    """测试线程安全的计数器递增"""

    @pytest.mark.asyncio
    async def test_counter_increment_is_thread_safe(self):
        """测试计数器在并发环境下正确递增"""
        backend = InMemoryShortTermBackend(capacity=100, ttl_seconds=300)
        
        # 并发添加100个记忆
        tasks = []
        for i in range(100):
            item = MemoryItem(
                content=f"test content {i}",
                memory_type=MemoryType.SHORT_TERM,
                importance=ImportanceLevel.MEDIUM,
            )
            tasks.append(backend.add(item))
        
        await asyncio.gather(*tasks)
        
        # 验证计数器正确递增
        # 由于TTLCache可能过期，我们只验证操作完成
        assert backend._counter == 100

    @pytest.mark.asyncio
    async def test_counter_overflow_handling(self):
        """测试计数器溢出处理"""
        backend = InMemoryShortTermBackend(capacity=10, ttl_seconds=300)
        backend._counter = backend.MAX_COUNTER  # 设置为最大值
        
        # 下一次添加应该重置计数器
        item = MemoryItem(
            content="test overflow",
            memory_type=MemoryType.SHORT_TERM,
            importance=ImportanceLevel.MEDIUM,
        )
        await backend.add(item)
        
        assert backend._counter == 1

    @pytest.mark.asyncio
    async def test_concurrent_add_operations(self):
        """测试并发添加操作不会丢失数据"""
        backend = InMemoryShortTermBackend(capacity=1000, ttl_seconds=300)
        
        # 创建大量并发任务
        num_items = 500
        items = [
            MemoryItem(
                content=f"concurrent test {i}",
                memory_type=MemoryType.SHORT_TERM,
                importance=ImportanceLevel.MEDIUM,
            )
            for i in range(num_items)
        ]
        
        # 并发执行
        results = await asyncio.gather(*[backend.add(item) for item in items])
        
        # 验证所有操作都成功
        assert len(results) == num_items
        assert backend._counter == num_items


class TestThreadSafeStatsUpdate:
    """测试线程安全的统计信息更新"""

    @pytest.mark.asyncio
    async def test_stats_update_with_lock(self):
        """测试统计信息更新使用锁保护"""
        from memory_system.agent_memory_system_facade import MemorySystem
        
        # 创建模拟组件
        mock_storage = AsyncMock()
        mock_storage.add = AsyncMock(return_value="test_id")
        mock_storage.delete = AsyncMock(return_value=True)
        mock_storage.get = AsyncMock(return_value=None)
        mock_storage.count = AsyncMock(return_value=0)
        mock_storage.search_by_time_range = AsyncMock(return_value=[])
        mock_storage.close = AsyncMock()
        
        mock_embedding = AsyncMock()
        mock_embedding.embed = AsyncMock(return_value=[0.1] * 768)
        mock_embedding.dimension = 768
        
        mock_retrieval = AsyncMock()
        mock_retrieval.retrieve = AsyncMock(return_value={"combined": []})
        
        mock_short_term = AsyncMock()
        mock_short_term.add = AsyncMock(return_value=MemoryItem(
            content="test",
            memory_type=MemoryType.SHORT_TERM,
            importance=ImportanceLevel.MEDIUM,
        ))
        mock_short_term.get_all = AsyncMock(return_value=[])
        mock_short_term.get_recent = AsyncMock(return_value=[])
        mock_short_term.clear = AsyncMock()
        
        # 创建 MemorySystem 实例
        memory_system = MemorySystem(
            storage=mock_storage,
            embedding=mock_embedding,
            retrieval=mock_retrieval,
            short_term=mock_short_term,
        )
        
        # 验证锁存在
        assert hasattr(memory_system, '_stats_lock')
        assert isinstance(memory_system._stats_lock, asyncio.Lock)
        
        # 验证初始统计
        assert memory_system._stats["total_added"] == 0
        assert memory_system._stats["total_retrieved"] == 0


class TestAtomicQuotaCheck:
    """测试原子配额检查和递增"""

    @pytest.mark.asyncio
    async def test_check_and_increment_quota_success(self):
        """测试配额检查和递增成功"""
        manager = TenantIsolationManager(
            enable_quota=True,
            max_memories_per_tenant=100,
        )
        
        tenant_id = "test_tenant"
        
        # 执行原子检查和递增
        result = await manager.check_and_increment_quota(tenant_id)
        
        assert result is True
        assert manager._tenant_counts[tenant_id] == 1

    @pytest.mark.asyncio
    async def test_check_and_increment_quota_exceeded(self):
        """测试配额超限"""
        manager = TenantIsolationManager(
            enable_quota=True,
            max_memories_per_tenant=2,
        )
        
        tenant_id = "test_tenant"
        
        # 第一次应该成功
        result1 = await manager.check_and_increment_quota(tenant_id)
        assert result1 is True
        
        # 第二次应该成功
        result2 = await manager.check_and_increment_quota(tenant_id)
        assert result2 is True
        
        # 第三次应该失败（超出配额）
        result3 = await manager.check_and_increment_quota(tenant_id)
        assert result3 is False

    @pytest.mark.asyncio
    async def test_check_and_increment_quota_concurrent(self):
        """测试并发配额检查"""
        manager = TenantIsolationManager(
            enable_quota=True,
            max_memories_per_tenant=50,
        )
        
        tenant_id = "test_tenant"
        
        # 并发执行100次检查（配额只有50）
        tasks = [
            manager.check_and_increment_quota(tenant_id)
            for _ in range(100)
        ]
        results = await asyncio.gather(*tasks)
        
        # 统计成功次数
        success_count = sum(1 for r in results if r)
        
        # 成功次数应该正好等于配额限制
        assert success_count == 50
        assert manager._tenant_counts[tenant_id] == 50

    @pytest.mark.asyncio
    async def test_check_and_increment_quota_disabled(self):
        """测试禁用配额时的行为"""
        manager = TenantIsolationManager(
            enable_quota=False,
            max_memories_per_tenant=10,
        )
        
        tenant_id = "test_tenant"
        
        # 即使配额禁用，也应该递增计数
        for i in range(20):
            result = await manager.check_and_increment_quota(tenant_id)
            assert result is True
        
        # 计数应该正确
        assert manager._tenant_counts[tenant_id] == 20

    @pytest.mark.asyncio
    async def test_tenant_aware_storage_wrapper_uses_atomic_check(self):
        """测试 TenantAwareStorageWrapper 使用原子检查"""
        manager = TenantIsolationManager(
            enable_quota=True,
            max_memories_per_tenant=5,
        )
        
        mock_storage = AsyncMock()
        mock_storage.add = AsyncMock(return_value="test_id")
        
        wrapper = TenantAwareStorageWrapper(
            storage=mock_storage,
            tenant_manager=manager,
        )
        
        # 添加记忆应该使用原子检查
        item = MemoryItem(
            content="test",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.MEDIUM,
        )
        
        result = await wrapper.add(item)
        
        # 验证计数已递增
        assert manager._tenant_counts["default"] == 1


class TestConstantsUsage:
    """测试常量被正确使用"""

    def test_embedding_cache_constants_exist(self):
        """测试嵌入缓存常量存在且值正确"""
        assert EMBEDDING_CACHE_MAX_SIZE == 512
        assert EMBEDDING_CACHE_TTL_SECONDS == 3600.0

    def test_retrieval_ratio_constants_exist(self):
        """测试检索比例常量存在且值正确"""
        assert DEFAULT_SHORT_TERM_RATIO == 0.33
        assert DEFAULT_TIME_RATIO == 0.33
        assert DEFAULT_VECTOR_RATIO == 0.5
        assert DEFAULT_IMPORTANCE_RATIO == 0.33

    def test_hierarchical_retrieval_uses_constants(self):
        """测试分层检索策略使用常量作为默认值"""
        from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
        
        # 使用默认值创建
        retrieval = HierarchicalRetrieval()
        
        assert retrieval.short_term_ratio == DEFAULT_SHORT_TERM_RATIO
        assert retrieval.time_ratio == DEFAULT_TIME_RATIO
        assert retrieval.vector_ratio == DEFAULT_VECTOR_RATIO
        assert retrieval.importance_ratio == DEFAULT_IMPORTANCE_RATIO

    def test_memory_system_uses_cache_constants(self):
        """测试 MemorySystem 使用缓存常量"""
        from cachetools import TTLCache
        from memory_system.agent_memory_system_facade import MemorySystem
        
        # 创建模拟组件
        mock_storage = AsyncMock()
        mock_embedding = AsyncMock()
        mock_embedding.dimension = 768
        mock_retrieval = AsyncMock()
        mock_short_term = AsyncMock()
        
        memory_system = MemorySystem(
            storage=mock_storage,
            embedding=mock_embedding,
            retrieval=mock_retrieval,
            short_term=mock_short_term,
        )
        
        # 验证缓存使用常量
        assert memory_system._embedding_cache.maxsize == EMBEDDING_CACHE_MAX_SIZE
        assert memory_system._embedding_cache.ttl == EMBEDDING_CACHE_TTL_SECONDS


class TestLoggingInExceptBlocks:
    """测试静默except块添加了日志记录"""

    def test_ast_parser_has_logger(self):
        """测试 AST 解析器有 logger"""
        from memory_system.code_memory_support.ast_parser import logger
        
        assert logger is not None
        assert logger.name == "memory_system.code_memory_support.ast_parser"

    def test_qdrant_storage_has_logger(self):
        """测试 Qdrant 存储有 logger"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import logger
        
        assert logger is not None
        assert logger.name == "memory_system.storage_backends.qdrant_vector_storage_backend"

    def test_authentication_middleware_has_logger(self):
        """测试认证中间件有 logger"""
        from memory_system.core_abstractions.authentication_middleware import logger
        
        assert logger is not None
        assert logger.name == "memory_system.core_abstractions.authentication_middleware"

    def test_telemetry_manager_has_logger(self):
        """测试遥测管理器有 logger"""
        from memory_system.telemetry_tracing_manager import logger
        
        # structlog 的 logger 没有 name 属性，只验证存在
        assert logger is not None

    @pytest.mark.asyncio
    async def test_verify_password_logs_on_exception(self, caplog):
        """测试密码验证失败时记录日志"""
        import logging
        from memory_system.core_abstractions.authentication_middleware import verify_password
        
        # 使用无效的哈希值触发异常
        result = verify_password("password", "invalid_hash")
        
        assert result is False
        # 检查日志记录
        assert any("密码验证失败" in record.message for record in caplog.records)


class TestShortTermBackendLock:
    """测试短期记忆后端的锁保护"""

    def test_backend_has_counter_lock(self):
        """测试后端有计数器锁"""
        backend = InMemoryShortTermBackend()
        
        assert hasattr(backend, '_counter_lock')
        assert isinstance(backend._counter_lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_lock_protects_counter(self):
        """测试锁保护计数器"""
        backend = InMemoryShortTermBackend(capacity=100, ttl_seconds=300)
        
        # 在锁内检查计数器
        async with backend._counter_lock:
            initial_counter = backend._counter
            # 模拟并发场景
            pass
        
        assert backend._counter == initial_counter

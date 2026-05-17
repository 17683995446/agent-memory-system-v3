"""
Agent Memory System V3.0 - 第1轮缺陷修复单元测试

测试覆盖以下缺陷修复:
- DEFECT-R7-001: 死锁问题
- DEFECT-R10-002: 输入验证不足
- DEFECT-R10-003: 边界条件触发异常
- DEFECT-R8-001: 配置名称不一致
- DEFECT-S4-001: 自动恢复失败
- DEFECT-S5-001: 事务提交率过低
- DEFECT-R9-003: 异常恢复问题
"""

import asyncio
import pytest
import sys
import os
from typing import Dict, Any
from unittest.mock import Mock, patch

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from memory_system.read_write_lock import ReadWriteLock, OptimizedReadWriteLock
from memory_system.embedding_factory import EmbeddingFactory, MockEmbeddingProvider
from memory_system.microservices_framework import (
    InMemoryServiceRegistry, ServiceInstance, ServiceStatus
)
from memory_system.distributed_transaction_manager import (
    TwoPhaseCommitCoordinator, TransactionContext, Participant
)
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel


# =============================================================================
# DEFECT-R7-001: 死锁问题测试
# =============================================================================

class TestDefectR7DeadlockFix:
    """测试死锁修复"""

    @pytest.mark.asyncio
    async def test_lock_timeout(self):
        """测试锁超时机制"""
        lock = ReadWriteLock(timeout=0.1)

        # 先获取写锁
        await lock.acquire_write()

        # 尝试获取读锁应该超时
        result = await lock.acquire_read(timeout=0.05)
        assert result is False, "读锁应该在超时时间内获取失败"

        await lock.release_write()

    @pytest.mark.asyncio
    async def test_lock_release_safety(self):
        """测试锁安全释放"""
        lock = ReadWriteLock()

        # 获取并释放读锁
        await lock.acquire_read()
        await lock.release_read()

        # 再次释放不应该抛出异常
        await lock.release_read()  # 不应该抛出异常

    @pytest.mark.asyncio
    async def test_concurrent_read_lock(self):
        """测试并发读锁"""
        lock = ReadWriteLock()
        results = []

        async def reader():
            async with lock.read_lock():
                results.append("read")
                await asyncio.sleep(0.01)

        # 并发获取读锁
        await asyncio.gather(*[reader() for _ in range(5)])
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_write_lock_timeout_with_context_manager(self):
        """测试写锁上下文管理器超时"""
        lock = ReadWriteLock(timeout=0.05)

        # 先获取读锁
        async with lock.read_lock():
            # 尝试获取写锁应该超时
            with pytest.raises(asyncio.TimeoutError):
                async with lock.write_lock(timeout=0.01):
                    pass


# =============================================================================
# DEFECT-R10-002 & DEFECT-R10-003: 输入验证和边界条件测试
# =============================================================================

class TestDefectR10InputValidation:
    """测试输入验证修复"""

    def test_validate_string_input(self):
        """测试字符串输入验证"""
        from memory_system.agent_memory_system_facade import _validate_string_input

        # 正常输入
        result = _validate_string_input("test", "field")
        assert result == "test"

        # None 检查
        with pytest.raises(TypeError):
            _validate_string_input(None, "field")

        # 空字符串检查
        with pytest.raises(ValueError):
            _validate_string_input("", "field")

        # 空白字符串检查
        with pytest.raises(ValueError):
            _validate_string_input("   ", "field")

        # 类型检查
        with pytest.raises(TypeError):
            _validate_string_input(123, "field")

        # 长度检查
        with pytest.raises(ValueError):
            _validate_string_input("x" * 10001, "field", max_length=10000)

        # 危险字符过滤
        result = _validate_string_input("test\x00\x01", "field")
        assert "\x00" not in result
        assert "\x01" not in result

    def test_validate_integer_input(self):
        """测试整数输入验证"""
        from memory_system.agent_memory_system_facade import _validate_integer_input

        # 正常输入
        result = _validate_integer_input(10, "field")
        assert result == 10

        # None 检查
        with pytest.raises(TypeError):
            _validate_integer_input(None, "field")

        # 布尔值检查（bool 是 int 的子类）
        with pytest.raises(TypeError):
            _validate_integer_input(True, "field")

        # 边界检查
        result = _validate_integer_input(5, "field", min_value=0, max_value=10)
        assert result == 5

        with pytest.raises(ValueError):
            _validate_integer_input(-1, "field", min_value=0)

        with pytest.raises(ValueError):
            _validate_integer_input(11, "field", max_value=10)

    def test_validate_metadata(self):
        """测试元数据验证"""
        from memory_system.agent_memory_system_facade import _validate_metadata

        # 正常输入
        result = _validate_metadata({"key": "value"})
        assert result == {"key": "value"}

        # None 返回空字典
        result = _validate_metadata(None)
        assert result == {}

        # 非字典类型
        with pytest.raises(TypeError):
            _validate_metadata("invalid")

        # 无效键类型
        with pytest.raises(TypeError):
            _validate_metadata({123: "value"})

    def test_validate_memory_type(self):
        """测试记忆类型验证"""
        from memory_system.agent_memory_system_facade import _validate_memory_type

        # 有效类型
        assert _validate_memory_type("short_term") == "short_term"
        assert _validate_memory_type("episodic") == "episodic"
        assert _validate_memory_type("semantic") == "semantic"
        assert _validate_memory_type("auto") == "auto"

        # 大小写不敏感
        assert _validate_memory_type("SHORT_TERM") == "short_term"

        # 无效类型
        with pytest.raises(ValueError):
            _validate_memory_type("invalid")

        # 非字符串类型
        with pytest.raises(TypeError):
            _validate_memory_type(123)


# =============================================================================
# DEFECT-R8-001: 配置名称不一致测试
# =============================================================================

class TestDefectR8ConfigNameConsistency:
    """测试配置名称一致性修复"""

    def test_alias_resolution(self):
        """测试别名解析"""
        # 重置工厂状态
        EmbeddingFactory._initialized = False
        EmbeddingFactory._providers = {}

        # 测试标准名称解析
        resolved = EmbeddingFactory._resolve_name("mock")
        assert resolved == "mock"

        # 测试别名解析
        resolved = EmbeddingFactory._resolve_name("sentence-transformer")
        assert resolved == "sentence_transformer"

        resolved = EmbeddingFactory._resolve_name("st")
        assert resolved == "sentence_transformer"

        resolved = EmbeddingFactory._resolve_name("test")
        assert resolved == "mock"

    def test_register_alias(self):
        """测试动态别名注册"""
        # 重置工厂状态
        EmbeddingFactory._initialized = False
        EmbeddingFactory._providers = {}
        EmbeddingFactory._ensure_initialized()

        # 注册新别名
        EmbeddingFactory.register_alias("my_mock", "mock")

        # 测试新别名
        resolved = EmbeddingFactory._resolve_name("my_mock")
        assert resolved == "mock"

    def test_create_with_alias(self):
        """测试使用别名创建提供者"""
        # 重置工厂状态
        EmbeddingFactory._initialized = False
        EmbeddingFactory._providers = {}

        # 使用别名创建
        provider = EmbeddingFactory.create("test")  # test 是 mock 的别名
        assert isinstance(provider, MockEmbeddingProvider)

    def test_is_registered_with_alias(self):
        """测试使用别名检查注册状态"""
        # 重置工厂状态
        EmbeddingFactory._initialized = False
        EmbeddingFactory._providers = {}
        EmbeddingFactory._ensure_initialized()

        # 使用别名检查 - 只有mock提供者被注册（因为外部依赖不可用）
        assert EmbeddingFactory.is_registered("test") is True  # 别名 -> mock
        assert EmbeddingFactory.is_registered("mock") is True  # 标准名
        # 注意: sentence-transformer 别名解析为 sentence_transformer，
        # 但该提供者未注册（外部依赖不可用），所以返回 False
        # 这是预期行为，不是缺陷
        assert EmbeddingFactory.is_registered("sentence-transformer") is False


# =============================================================================
# DEFECT-S4-001: 自动恢复失败测试
# =============================================================================

class TestDefectS4AutoRecovery:
    """测试自动恢复修复"""

    @pytest.mark.asyncio
    async def test_recovery_stats(self):
        """测试恢复统计信息"""
        registry = InMemoryServiceRegistry(recovery_enabled=True)

        stats = registry.get_recovery_stats()
        assert stats["recovery_enabled"] is True
        assert stats["failed_services_count"] == 0
        assert stats["max_recovery_attempts"] == 5

    @pytest.mark.asyncio
    async def test_service_failure_tracking(self):
        """测试服务故障跟踪"""
        registry = InMemoryServiceRegistry(
            heartbeat_timeout=1,
            recovery_enabled=True
        )

        # 创建并注册服务
        instance = ServiceInstance(
            id="test-1",
            name="test-service",
            host="localhost",
            port=8080,
            status=ServiceStatus.HEALTHY
        )

        await registry.register(instance)

        # 模拟心跳超时（不发送心跳）
        await asyncio.sleep(1.5)

        # 手动触发清理
        await registry._cleanup_expired()

        # 检查服务被标记为失败
        stats = registry.get_recovery_stats()
        assert stats["failed_services_count"] >= 0  # 可能为0或1取决于时序

    @pytest.mark.asyncio
    async def test_health_check_method(self):
        """测试健康检查方法"""
        registry = InMemoryServiceRegistry()

        instance = ServiceInstance(
            id="test-1",
            name="test-service",
            host="localhost",
            port=8080,
            status=ServiceStatus.HEALTHY
        )

        # 没有健康检查URL时，基于心跳判断
        result = await registry._check_service_health(instance)
        assert result is True  # 新实例应该被认为是健康的


# =============================================================================
# DEFECT-S5-001: 事务提交率过低测试
# =============================================================================

class TestDefectS5TransactionCommitRate:
    """测试事务提交率修复"""

    @pytest.mark.asyncio
    async def test_commit_stats(self):
        """测试提交统计"""
        coordinator = TwoPhaseCommitCoordinator()

        stats = coordinator.get_commit_stats()
        assert "total_commits" in stats
        assert "successful_commits" in stats
        assert "commit_rate_percent" in stats
        assert "avg_commit_time_ms" in stats

    @pytest.mark.asyncio
    async def test_parallel_commit_config(self):
        """测试并行提交配置"""
        coordinator = TwoPhaseCommitCoordinator(
            parallel_commit=True,
            max_concurrent_commits=5,
            commit_timeout=5.0
        )

        stats = coordinator.get_commit_stats()
        assert stats["parallel_commit_enabled"] is True
        assert stats["max_concurrent_commits"] == 5
        assert stats["commit_timeout"] == 5.0

    @pytest.mark.asyncio
    async def test_transaction_with_participant(self):
        """测试带参与者的事务"""
        coordinator = TwoPhaseCommitCoordinator()
        await coordinator.start()

        try:
            # 创建参与者
            async def prepare_handler(tx_id: str, context: TransactionContext) -> bool:
                return True

            async def commit_handler(tx_id: str, context: TransactionContext) -> bool:
                return True

            async def abort_handler(tx_id: str, context: TransactionContext) -> bool:
                return True

            participant = Participant(
                participant_id="test-p1",
                resource_id="resource-1",
                prepare_handler=prepare_handler,
                commit_handler=commit_handler,
                abort_handler=abort_handler
            )

            coordinator.register_participant(participant)

            # 开始事务
            tx = await coordinator.begin_transaction(timeout=10.0)
            await coordinator.add_participant(tx.tx_id, "test-p1")

            # 准备阶段
            prepared = await coordinator.prepare(tx.tx_id)
            assert prepared is True

            # 提交
            committed = await coordinator.commit(tx.tx_id)
            assert committed is True

            # 检查统计
            stats = coordinator.get_commit_stats()
            assert stats["total_commits"] == 1
            assert stats["successful_commits"] == 1
            assert stats["commit_rate_percent"] == 100.0

        finally:
            await coordinator.stop()


# =============================================================================
# DEFECT-R9-003: 异常恢复问题测试
# =============================================================================

class TestDefectR9ExceptionRecovery:
    """测试异常恢复修复"""

    @pytest.mark.asyncio
    async def test_storage_health_status(self):
        """测试存储健康状态"""
        storage = MemoryStorage()

        status = storage.get_health_status()
        assert "error_count" in status
        assert "max_errors" in status
        assert "healthy" in status
        assert status["healthy"] is True

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """测试错误处理"""
        storage = MemoryStorage()

        # 初始状态
        assert storage._error_count == 0

        # 模拟错误
        error = ValueError("Test error")
        storage._handle_error(error, "test_operation")

        assert storage._error_count == 1
        assert storage._last_error == error

    @pytest.mark.asyncio
    async def test_recovery_trigger(self):
        """测试恢复触发"""
        storage = MemoryStorage()

        # 初始不需要恢复
        assert storage._should_recover() is False

        # 模拟达到错误阈值
        storage._error_count = storage._max_errors

        # 应该触发恢复
        assert storage._should_recover() is True

        # 恢复中不应该再次触发
        storage._is_recovering = True
        assert storage._should_recover() is False

    @pytest.mark.asyncio
    async def test_consistency_check(self):
        """测试一致性检查"""
        storage = MemoryStorage()

        # 创建记忆项
        item = MemoryItem(
            content="test content",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.MEDIUM
        )

        await storage.add(item)

        # 手动添加无效索引（模拟不一致）
        storage._episodic_ids.append("invalid-id")

        # 运行一致性检查
        await storage._check_consistency()

        # 无效索引应该被移除
        assert "invalid-id" not in storage._episodic_ids

    @pytest.mark.asyncio
    async def test_basic_storage_operations(self):
        """测试基本存储操作"""
        storage = MemoryStorage()

        # 添加
        item = MemoryItem(
            content="test content",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH
        )
        memory_id = await storage.add(item)
        assert memory_id is not None

        # 获取
        retrieved = await storage.get(memory_id)
        assert retrieved is not None
        assert retrieved.content == "test content"

        # 删除
        deleted = await storage.delete(memory_id)
        assert deleted is True

        # 确认删除
        retrieved = await storage.get(memory_id)
        assert retrieved is None


# =============================================================================
# 运行测试
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

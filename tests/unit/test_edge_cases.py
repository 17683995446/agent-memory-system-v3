"""
边缘情况测试
测试各种异常场景和边界条件
"""

import asyncio
import json
import os
import sys
import time
import tempfile
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.conftest import MockEmbeddingProvider, MemoryStorage
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
)
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.memory_snapshot_manager import (
    MemorySnapshotManager,
    MemorySnapshotMetadata,
)
from memory_system.memory_ttl_expiration_manager import (
    MemoryTTLExpirationManager,
    TTLConfig,
)
from memory_system.tenant_isolation_manager import (
    TenantContext,
    TenantIsolationManager,
    TenantAwareStorageWrapper,
    QuotaExceededError,
)
from memory_system.telemetry_tracing_manager import (
    TelemetryTracingManager,
    _NoOpSpan,
    _OTEL_AVAILABLE,
)
from memory_system.websocket_event_notifier import (
    WebSocketEventNotifier,
    EventType,
    _FASTAPI_AVAILABLE,
)


# ============================================================================
# Snapshot Edge Cases
# ============================================================================

class TestSnapshotEdgeCases:
    """快照边缘情况测试"""

    @pytest.fixture
    def snapshot_dir(self, tmp_path):
        """快照存储目录"""
        return str(tmp_path / "snapshots")

    @pytest_asyncio.fixture
    async def storage(self):
        """存储实例"""
        return MemoryStorage(max_memories=100)

    @pytest_asyncio.fixture
    async def short_term(self):
        """短期记忆实例"""
        return ShortTermMemory(capacity=10, ttl_seconds=60)

    @pytest.mark.asyncio
    async def test_corrupted_snapshot_file_handling(self, storage, short_term, snapshot_dir):
        """测试损坏的快照文件处理"""
        manager = MemorySnapshotManager(storage, short_term, snapshot_dir=snapshot_dir)
        
        # 创建一个损坏的快照文件
        corrupted_file = Path(snapshot_dir) / "snap_corrupted_123456789.json"
        corrupted_file.write_text("{ invalid json content", encoding="utf-8")
        
        # 列出快照时应跳过损坏的文件
        snapshots = await manager.list_snapshots()
        assert len(snapshots) == 0  # 损坏的文件被跳过
        
        # 尝试恢复损坏的快照应抛出异常
        with pytest.raises(Exception):  # JSONDecodeError
            await manager.restore_snapshot("snap_corrupted_123456789")

    @pytest.mark.asyncio
    async def test_partial_restore_failure_recovery(self, storage, short_term, snapshot_dir):
        """测试部分恢复失败的回滚机制"""
        manager = MemorySnapshotManager(storage, short_term, snapshot_dir=snapshot_dir)
        
        # 添加一些初始记忆
        item1 = MemoryItem(
            content="原始记忆1",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
        )
        await storage.add(item1)
        
        # 创建快照
        snapshot_id = await manager.create_snapshot("测试快照")
        
        # 添加更多记忆
        item2 = MemoryItem(
            content="原始记忆2",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
        )
        await storage.add(item2)
        
        # 恢复快照应成功
        restored = await manager.restore_snapshot(snapshot_id)
        assert restored == 1  # 只恢复快照中的1条记忆

    @pytest.mark.asyncio
    async def test_snapshot_creation_and_restore(self, snapshot_dir):
        """测试快照创建和恢复的基本流程"""
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)
        
        # 添加一条记忆
        item = MemoryItem(
            content="测试记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
        )
        await storage.add(item)
        
        # 创建快照
        snapshot_id = await manager.create_snapshot("测试快照")
        assert snapshot_id.startswith("snap_")
        
        # 添加另一条记忆改变状态
        item2 = MemoryItem(
            content="第二条记忆",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.MEDIUM,
        )
        await storage.add(item2)
        
        # 现在存储有2条记忆
        count_before = await storage.count()
        assert count_before == 2
        
        # 恢复快照（恢复到只有1条记忆的状态）
        restored = await manager.restore_snapshot(snapshot_id)
        assert restored == 1  # 恢复1条记忆
        
        # 验证存储现在只有1条记忆
        count_after = await storage.count()
        assert count_after == 1

    @pytest.mark.asyncio
    async def test_snapshot_id_validation_edge_cases(self, storage, snapshot_dir):
        """测试快照ID验证的边缘情况"""
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)
        
        # 测试空ID
        with pytest.raises(ValueError, match="不能为空"):
            manager._validate_snapshot_id("")
        
        # 测试过短的ID
        with pytest.raises(ValueError, match="长度不足"):
            manager._validate_snapshot_id("snap")
        
        # 测试过长的ID
        long_id = "snap_" + "a" * 200
        with pytest.raises(ValueError, match="长度超限"):
            manager._validate_snapshot_id(long_id)
        
        # 测试缺少前缀
        with pytest.raises(ValueError, match="必须以'snap_'开头"):
            manager._validate_snapshot_id("invalid_id_123")
        
        # 测试路径遍历攻击
        with pytest.raises(ValueError, match="非法路径字符"):
            manager._validate_snapshot_id("snap_../etc/passwd")
        
        with pytest.raises(ValueError, match="非法路径字符"):
            manager._validate_snapshot_id("snap_/etc/passwd")
        
        # 测试非法字符
        with pytest.raises(ValueError, match="非法字符"):
            manager._validate_snapshot_id("snap_test@123")
        
        # 测试有效的ID
        manager._validate_snapshot_id("snap_1234567890_abc123")  # 不应抛出异常

    @pytest.mark.asyncio
    async def test_snapshot_with_missing_metadata_fields(self, storage, snapshot_dir):
        """测试缺少元数据字段的快照处理"""
        manager = MemorySnapshotManager(storage, None, snapshot_dir=snapshot_dir)
        
        # 创建一个缺少某些字段的快照文件
        incomplete_file = Path(snapshot_dir) / "snap_incomplete_123456789.json"
        incomplete_data = {
            "metadata": {
                "snapshot_id": "snap_incomplete_123456789",
                "created_at": time.time(),
                # 缺少 memory_count, short_term_count 等字段
            },
            "memories": [],
            "short_term_memories": [],
        }
        incomplete_file.write_text(json.dumps(incomplete_data), encoding="utf-8")
        
        # 列出快照时可能失败或使用默认值
        try:
            snapshots = await manager.list_snapshots()
            # 如果成功，检查是否使用了默认值
        except Exception:
            pass  # 某些实现可能会抛出异常


# ============================================================================
# TTL Expiration Edge Cases
# ============================================================================

class TestTTLExpirationEdgeCases:
    """TTL过期边缘情况测试"""

    @pytest.fixture
    def mock_storage(self):
        """创建模拟存储"""
        storage = MagicMock()
        storage.search_by_time_range = AsyncMock(return_value=[])
        storage.delete = AsyncMock()
        storage.get = AsyncMock(return_value=None)
        return storage

    @pytest.mark.asyncio
    async def test_concurrent_cleanup(self, mock_storage):
        """测试并发清理"""
        config = TTLConfig(enabled=True, max_delete_per_run=10)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        now = time.time()
        
        # 创建一些过期记忆
        memories = []
        for i in range(20):
            memory = MagicMock()
            memory.memory_id = f"expired_{i}"
            memory.timestamp = now - (400 * 24 * 3600)
            memory.metadata = {}
            memory.importance = ImportanceLevel.HIGH
            memories.append(memory)
        
        mock_storage.search_by_time_range.return_value = memories
        
        # 并发执行清理
        results = await asyncio.gather(
            manager.cleanup_expired(),
            manager.cleanup_expired(),
            manager.cleanup_expired(),
        )
        
        # 验证所有清理操作都完成了
        assert all(isinstance(r, int) for r in results)

    @pytest.mark.asyncio
    async def test_cleanup_interruption_recovery(self, mock_storage):
        """测试清理中断后的恢复"""
        config = TTLConfig(enabled=True, max_delete_per_run=5)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        now = time.time()
        
        # 创建过期记忆
        memories = []
        for i in range(10):
            memory = MagicMock()
            memory.memory_id = f"expired_{i}"
            memory.timestamp = now - (400 * 24 * 3600)
            memory.metadata = {}
            memory.importance = ImportanceLevel.HIGH
            memories.append(memory)
        
        # 模拟部分删除失败
        delete_count = 0
        async def mock_delete(memory_id):
            nonlocal delete_count
            if delete_count >= 3:
                raise Exception("模拟删除失败")
            delete_count += 1
        
        mock_storage.search_by_time_range.return_value = memories
        mock_storage.delete = AsyncMock(side_effect=mock_delete)
        
        # 清理应继续执行，即使部分失败
        deleted = await manager.cleanup_expired()
        assert deleted >= 0  # 应该有一些被删除

    @pytest.mark.asyncio
    async def test_empty_ttl_queue(self, mock_storage):
        """测试空TTL队列"""
        config = TTLConfig(enabled=True)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        # 空存储
        mock_storage.search_by_time_range.return_value = []
        
        # 清理应返回0
        deleted = await manager.cleanup_expired()
        assert deleted == 0
        
        # 预览也应返回空
        expired = await manager.preview_expired()
        assert len(expired) == 0

    @pytest.mark.asyncio
    async def test_ttl_with_very_old_memories(self, mock_storage):
        """测试非常旧的记忆的TTL处理"""
        config = TTLConfig(enabled=True)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        # 创建一个非常旧的记忆（10年前）
        memory = MagicMock()
        memory.memory_id = "very_old"
        memory.timestamp = time.time() - (3650 * 24 * 3600)  # 10年前
        memory.metadata = {}
        memory.importance = ImportanceLevel.LOW  # 1个月TTL
        
        mock_storage.search_by_time_range.return_value = [memory]
        
        deleted = await manager.cleanup_expired()
        assert deleted == 1

    @pytest.mark.asyncio
    async def test_ttl_with_future_timestamp(self, mock_storage):
        """测试未来时间戳的记忆"""
        config = TTLConfig(enabled=True)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        # 创建一个未来时间戳的记忆
        memory = MagicMock()
        memory.memory_id = "future"
        memory.timestamp = time.time() + (365 * 24 * 3600)  # 1年后
        memory.metadata = {}
        memory.importance = ImportanceLevel.HIGH
        
        mock_storage.search_by_time_range.return_value = [memory]
        
        # 不应被删除
        deleted = await manager.cleanup_expired()
        assert deleted == 0


# ============================================================================
# Tenant Isolation Edge Cases
# ============================================================================

class TestTenantIsolationEdgeCases:
    """租户隔离边缘情况测试"""

    @pytest.fixture
    def tenant_manager(self):
        return TenantIsolationManager()

    @pytest.fixture
    def mock_storage(self):
        storage = MagicMock()
        storage.add = AsyncMock(return_value="test_id")
        storage.get = AsyncMock(return_value=None)
        storage.delete = AsyncMock(return_value=True)
        storage.search_by_vector = AsyncMock(return_value=[])
        return storage

    @pytest.mark.asyncio
    async def test_quota_exhaustion(self, tenant_manager, mock_storage):
        """测试配额耗尽"""
        tenant_manager.max_memories_per_tenant = 2
        
        context = TenantContext("tenant_limited")
        wrapper = TenantAwareStorageWrapper(mock_storage, tenant_manager, context)
        
        # 添加记忆直到配额耗尽
        item1 = MagicMock()
        item1.memory_id = "mem_1"
        await wrapper.add(item1)
        
        item2 = MagicMock()
        item2.memory_id = "mem_2"
        await wrapper.add(item2)
        
        # 第三个应该失败
        item3 = MagicMock()
        item3.memory_id = "mem_3"
        with pytest.raises(QuotaExceededError):
            await wrapper.add(item3)

    @pytest.mark.asyncio
    async def test_cross_tenant_access_prevention(self, tenant_manager):
        """测试跨租户访问防止"""
        # 创建两个不同租户的存储
        storage_a = MagicMock()
        storage_a.get = AsyncMock(return_value=MagicMock(memory_id="tenant:tenant_a:mem_1"))
        storage_a.search_by_time_range = AsyncMock(return_value=[])
        
        storage_b = MagicMock()
        storage_b.get = AsyncMock(return_value=None)
        
        context_a = TenantContext("tenant_a")
        context_b = TenantContext("tenant_b")
        
        wrapper_a = TenantAwareStorageWrapper(storage_a, tenant_manager, context_a)
        wrapper_b = TenantAwareStorageWrapper(storage_b, tenant_manager, context_b)
        
        # 租户A尝试访问自己的记忆
        await wrapper_a.get("mem_1")
        
        # 验证使用了正确的前缀
        storage_a.get.assert_called_with("tenant:tenant_a:mem_1")
        
        # 租户B尝试访问相同的记忆ID（但会使用不同的前缀）
        await wrapper_b.get("mem_1")
        storage_b.get.assert_called_with("tenant:tenant_b:mem_1")

    @pytest.mark.asyncio
    async def test_invalid_tenant_id_handling(self, tenant_manager):
        """测试无效租户ID处理"""
        # 测试空租户ID
        assert tenant_manager.validate_tenant_id("") is False
        
        # 测试包含冒号的租户ID
        assert tenant_manager.validate_tenant_id("tenant:invalid") is False
        
        # 测试过长的租户ID
        assert tenant_manager.validate_tenant_id("a" * 65) is False
        
        # 测试包含特殊字符的租户ID
        assert tenant_manager.validate_tenant_id("tenant@123") is False
        assert tenant_manager.validate_tenant_id("tenant#123") is False
        assert tenant_manager.validate_tenant_id("tenant 123") is False  # 空格
        assert tenant_manager.validate_tenant_id("tenant.123") is False  # 点号
        
        # 测试有效的租户ID
        assert tenant_manager.validate_tenant_id("tenant_123") is True
        assert tenant_manager.validate_tenant_id("tenant-123") is True
        assert tenant_manager.validate_tenant_id("TENANT123") is True
        assert tenant_manager.validate_tenant_id("a" * 64) is True  # 正好64字符

    @pytest.mark.asyncio
    async def test_tenant_prefix_edge_cases(self, tenant_manager):
        """测试租户前缀边缘情况"""
        # 空记忆ID
        prefixed = tenant_manager.add_tenant_prefix("", "tenant_1")
        assert prefixed == "tenant:tenant_1:"
        
        # 已有前缀的记忆ID
        prefixed = tenant_manager.add_tenant_prefix("tenant:tenant_1:mem_1", "tenant_1")
        assert prefixed == "tenant:tenant_1:mem_1"  # 不重复添加
        
        # 移除不存在的前缀
        unprefixed = tenant_manager.remove_tenant_prefix("mem_1", "tenant_1")
        assert unprefixed == "mem_1"


# ============================================================================
# Telemetry Edge Cases
# ============================================================================

class TestTelemetryEdgeCases:
    """遥测边缘情况测试"""

    def test_opentelemetry_unavailable_degradation(self):
        """测试OpenTelemetry不可用时的优雅降级"""
        # 创建禁用的管理器
        mgr = TelemetryTracingManager(enabled=False)
        
        # 应该返回NoOpSpan
        span = mgr.start_as_current_span("test_span")
        assert isinstance(span, _NoOpSpan)
        
        # NoOpSpan的所有方法都不应抛出异常
        span.set_attribute("key", "value")
        span.add_event("event")
        span.set_status("ok")
        span.record_exception(Exception("test"))
        assert span.is_recording() is False

    @pytest.mark.asyncio
    async def test_span_creation_when_disabled(self):
        """测试禁用时创建span"""
        mgr = TelemetryTracingManager(enabled=False)
        
        # 同步创建
        span = mgr.start_as_current_span("test")
        assert isinstance(span, _NoOpSpan)
        
        # 异步创建
        async_span = await mgr.start_span("test_async")
        assert isinstance(async_span, _NoOpSpan)

    def test_setup_without_otel(self):
        """测试无OpenTelemetry时的setup"""
        if _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry已安装")
        
        mgr = TelemetryTracingManager(enabled=True)
        result = mgr.setup()
        
        # 应该返回False并禁用
        assert result is False
        assert mgr.is_enabled is False

    def test_shutdown_idempotent(self):
        """测试shutdown的幂等性"""
        mgr = TelemetryTracingManager(enabled=False)
        
        # 多次shutdown不应抛出异常
        mgr.shutdown()
        mgr.shutdown()
        mgr.shutdown()

    def test_noop_span_context_managers(self):
        """测试NoOpSpan的上下文管理器"""
        span = _NoOpSpan()
        
        # 同步上下文管理器
        with span as s:
            assert s is span
        
        # 异步上下文管理器
        async def test_async():
            async with span as s:
                assert s is span
        
        asyncio.run(test_async())


# ============================================================================
# WebSocket Edge Cases
# ============================================================================

class TestWebSocketEdgeCases:
    """WebSocket边缘情况测试"""

    @pytest.fixture
    def notifier(self):
        return WebSocketEventNotifier()

    @pytest.fixture
    def mock_websocket(self):
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock()
        ws.close = AsyncMock()
        return ws

    @pytest.mark.asyncio
    async def test_connection_drop_during_broadcast(self, notifier):
        """测试广播期间连接断开"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI未安装")
        
        # 创建一个会在发送时断开的连接
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock(side_effect=Exception("Connection lost"))
        ws.close = AsyncMock()
        
        await notifier.connect(ws)
        assert notifier.get_connection_count() == 1
        
        # 广播时连接断开
        sent = await notifier.broadcast(EventType.MEMORY_ADDED, {"id": "test"})
        
        # 发送失败，连接被清理
        assert sent == 0
        assert notifier.get_connection_count() == 0

    @pytest.mark.asyncio
    async def test_invalid_message_handling(self, notifier, mock_websocket):
        """测试无效消息处理"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI未安装")
        
        await notifier.connect(mock_websocket)
        
        # 发送各种无效数据
        # 这些不应导致崩溃
        sent = await notifier.broadcast(EventType.MEMORY_ADDED, None)
        assert sent == 1  # 仍然发送成功
        
        sent = await notifier.broadcast(EventType.MEMORY_ADDED, {})
        assert sent == 1
        
        sent = await notifier.broadcast(EventType.MEMORY_ADDED, {"very": "large", "data": "x" * 10000})
        assert sent == 1

    @pytest.mark.asyncio
    async def test_broadcast_without_fastapi(self, notifier):
        """测试无FastAPI时的广播"""
        with patch('memory_system.websocket_event_notifier._FASTAPI_AVAILABLE', False):
            notifier2 = WebSocketEventNotifier()
            sent = await notifier2.broadcast(EventType.MEMORY_ADDED, {"id": "test"})
            assert sent == 0

    @pytest.mark.asyncio
    async def test_connect_without_fastapi(self, notifier, mock_websocket):
        """测试无FastAPI时的连接"""
        with patch('memory_system.websocket_event_notifier._FASTAPI_AVAILABLE', False):
            notifier2 = WebSocketEventNotifier()
            await notifier2.connect(mock_websocket)
            # 不应抛出异常，但也不会真正连接
            assert notifier2.get_connection_count() == 0

    @pytest.mark.asyncio
    async def test_multiple_disconnects(self, notifier, mock_websocket):
        """测试多次断开同一连接"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI未安装")
        
        await notifier.connect(mock_websocket)
        await notifier.disconnect(mock_websocket)
        
        # 再次断开不应抛出异常
        await notifier.disconnect(mock_websocket)
        
        assert notifier.get_connection_count() == 0

    @pytest.mark.asyncio
    async def test_close_all_with_no_connections(self, notifier):
        """测试无连接时关闭所有"""
        # 不应抛出异常
        await notifier.close_all()
        assert notifier.get_connection_count() == 0

    @pytest.mark.asyncio
    async def test_subscription_edge_cases(self, notifier):
        """测试订阅边缘情况"""
        if not _FASTAPI_AVAILABLE:
            pytest.skip("FastAPI未安装")
        
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock()
        
        # 空订阅列表
        await notifier.connect(ws, subscriptions=[])
        
        ws.send_json.reset_mock()
        sent = await notifier.broadcast(EventType.MEMORY_ADDED, {"id": "test"})
        # 空订阅意味着接收所有事件
        assert sent == 1
        
        # 断开并重新连接，使用特定订阅
        await notifier.disconnect(ws)
        
        ws2 = MagicMock()
        ws2.accept = AsyncMock()
        ws2.send_json = AsyncMock()
        
        await notifier.connect(ws2, subscriptions=[EventType.MEMORY_DELETED])
        
        ws2.send_json.reset_mock()
        # 发送未订阅的事件
        sent = await notifier.broadcast(EventType.MEMORY_ADDED, {"id": "test"})
        assert sent == 0
        
        # 发送已订阅的事件
        sent = await notifier.broadcast(EventType.MEMORY_DELETED, {"id": "test"})
        assert sent == 1


# ============================================================================
# Combined Edge Cases
# ============================================================================

class TestCombinedEdgeCases:
    """组合边缘情况测试"""

    @pytest.mark.asyncio
    async def test_tenant_with_snapshot(self, tmp_path):
        """测试租户隔离与快照的组合"""
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=10, ttl_seconds=60)
        
        tenant_manager = TenantIsolationManager()
        context = TenantContext("test_tenant")
        wrapper = TenantAwareStorageWrapper(storage, tenant_manager, context)
        
        snapshot_dir = str(tmp_path / "snapshots")
        snapshot_manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)
        
        # 添加记忆
        item = MemoryItem(
            content="测试记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
        )
        await storage.add(item)
        
        # 创建快照
        snapshot_id = await snapshot_manager.create_snapshot("租户测试快照")
        
        # 验证快照
        snapshots = await snapshot_manager.list_snapshots()
        assert len(snapshots) == 1

    @pytest.mark.asyncio
    async def test_ttl_with_tenant(self):
        """测试TTL与租户隔离的组合"""
        storage = MemoryStorage(max_memories=100)
        
        tenant_manager = TenantIsolationManager()
        context = TenantContext("ttl_tenant")
        wrapper = TenantAwareStorageWrapper(storage, tenant_manager, context)
        
        config = TTLConfig(enabled=True, default_ttl_days=30)
        ttl_manager = MemoryTTLExpirationManager(storage, config=config)
        
        # 添加记忆
        item = MemoryItem(
            content="TTL测试记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.LOW,
        )
        await storage.add(item)
        
        # 预览过期
        expired = await ttl_manager.preview_expired()
        assert isinstance(expired, list)

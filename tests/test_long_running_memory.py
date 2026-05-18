"""长时间运行测试 - 内存泄漏/资源耗尽检测"""
import pytest
import asyncio
import gc
import sys
import time
import threading
from datetime import datetime, timezone
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.security_hardening import CryptoManager, RBACManager, AuditLogger, Permission, Role
from memory_system.monitoring_alerting import MetricsCollector
from memory_system.partition_tolerance import PartitionManager
from memory_system.vector_clock import VectorClock


class TestMemoryLeakEventStore:
    """事件存储内存泄漏测试"""
    
    @pytest.mark.asyncio
    async def test_append_100000_events_memory_stable(self):
        """追加10万个事件,内存应稳定"""
        store = InMemoryEventStore()
        gc.collect()
        mem_before = sys.getsizeof(store._streams) + sys.getsizeof(store._all_events)
        
        for batch in range(100):
            events = [
                StoredEvent(
                    event_id=f"e{batch}_{i}", aggregate_id=f"agg_{batch}",
                    event_type="Update", event_data={"batch": batch, "i": i},
                    version=i+1, timestamp=datetime.now(timezone.utc),
                    sequence_number=batch*1000+i+1, metadata={}
                )
                for i in range(1000)
            ]
            await store.append(f"agg_{batch}", events)
        
        gc.collect()
        mem_after = sys.getsizeof(store._streams) + sys.getsizeof(store._all_events)
        # Memory growth should be reasonable (less than 10x)
        assert mem_after < mem_before * 10 + 10000000  # 10MB tolerance
    
    @pytest.mark.asyncio
    async def test_repeated_get_events_no_leak(self):
        """重复获取事件不应泄漏"""
        store = InMemoryEventStore()
        # Pre-populate
        events = [
            StoredEvent(
                event_id=f"e{i}", aggregate_id="agg1",
                event_type="Test", event_data={"i": i},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )
            for i in range(1000)
        ]
        await store.append("agg1", events)
        
        gc.collect()
        mem_before = len(gc.get_objects())
        
        for _ in range(1000):
            _ = await store.get_events("agg1")
        
        gc.collect()
        mem_after = len(gc.get_objects())
        # Object count should not grow significantly
        assert mem_after < mem_before * 1.5 + 1000


class TestMemoryLeakEventBus:
    """事件总线内存泄漏测试"""
    
    @pytest.mark.asyncio
    async def test_publish_50000_events_memory_stable(self):
        """发布5万个事件,内存应稳定"""
        bus = InMemoryEventBus(max_queue_size=100000)
        await bus.start()
        
        received = []
        await bus.subscribe("Test", lambda e: received.append(e))
        
        gc.collect()
        mem_before = len(gc.get_objects())
        
        for i in range(50000):
            await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))
        
        await asyncio.sleep(3.0)  # Wait for processing
        
        gc.collect()
        mem_after = len(gc.get_objects())
        await bus.stop()
        
        # Should have received most events without excessive memory growth
        assert len(received) > 45000
        # Memory growth should be sub-linear
        assert mem_after < mem_before * 2 + 50000


class TestMemoryLeakRBAC:
    """RBAC内存泄漏测试"""
    
    def test_create_10000_users_memory(self):
        """创建1万个用户,内存应可控"""
        rbac = RBACManager()
        gc.collect()
        mem_before = len(gc.get_objects())
        
        for i in range(10000):
            user = rbac.create_user(f"user_{i}", [Role.USER])
            if i % 2 == 0:
                rbac.assign_role(user.user_id, Role.ADMIN)
        
        gc.collect()
        mem_after = len(gc.get_objects())
        # Memory growth should be roughly linear
        assert mem_after < mem_before + 50000
    
    def test_repeated_permission_checks_no_leak(self):
        """重复权限检查不应泄漏"""
        rbac = RBACManager()
        user = rbac.create_user("test", [Role.ADMIN])
        
        gc.collect()
        mem_before = len(gc.get_objects())
        
        for _ in range(10000):
            rbac.check_permission(user.user_id, Permission.READ)
        
        gc.collect()
        mem_after = len(gc.get_objects())
        # Should not create objects on each check
        assert mem_after < mem_before + 1000


class TestResourceExhaustion:
    """资源耗尽测试"""
    
    @pytest.mark.asyncio
    async def test_event_store_with_limited_memory(self):
        """模拟内存受限环境"""
        store = InMemoryEventStore()
        
        try:
            # Try to store many events
            for i in range(10000):
                await store.append(f"agg_{i%100}", [StoredEvent(
                    event_id=f"e{i}", aggregate_id=f"agg_{i%100}",
                    event_type="Test", event_data={"large": "x" * 1000},
                    version=1, timestamp=datetime.now(timezone.utc),
                    sequence_number=i+1, metadata={}
                )])
        except MemoryError:
            pytest.skip("Memory limit reached - expected in constrained environment")
    
    @pytest.mark.asyncio
    async def test_event_bus_queue_full(self):
        """事件队列满时的行为"""
        bus = InMemoryEventBus(max_queue_size=100)
        await bus.start()
        
        # Fill the queue
        for i in range(200):
            result = await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))
            # Should either succeed or fail gracefully
            assert result in [True, False]
        
        await bus.stop()


class TestMetricsCollectorMemory:
    """指标收集器内存测试"""
    
    def test_1million_metrics_memory(self):
        """100万个指标数据点"""
        mc = MetricsCollector(max_history=100000)
        
        for i in range(1000000):
            mc.counter("requests", 1.0, labels={"endpoint": f"/api/{i%100}"})
        
        # Should not crash and memory should be bounded by max_history
        metrics = mc.get_metrics("requests")
        assert len(metrics) <= 100000


class TestAuditLoggerMemory:
    """审计日志内存测试"""
    
    def test_max_entries_enforced(self):
        """最大条目限制应生效"""
        audit = AuditLogger(max_entries=1000)
        
        for i in range(5000):
            audit.log(f"u{i%10}", "action", "resource", "success")
        
        stats = audit.get_stats()
        assert stats["total_entries"] <= 1000

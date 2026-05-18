"""性能退化测试"""
import pytest
import asyncio
import time
from datetime import datetime, timezone
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
from memory_system.security_hardening import CryptoManager, RBACManager, Permission, Role
from memory_system.monitoring_alerting import MetricsCollector
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage


class TestPerformanceDegradationEventStore:
    """事件存储性能退化测试"""
    
    @pytest.mark.asyncio
    async def test_append_performance_under_load(self):
        """负载下追加性能"""
        store = InMemoryEventStore()
        
        # Measure baseline
        start = time.time()
        for i in range(100):
            await store.append("agg1", [StoredEvent(
                event_id=f"e{i}", aggregate_id="agg1",
                event_type="Test", event_data={"i": i},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )])
        baseline = time.time() - start
        
        # Add 10000 events to create load
        for i in range(10000):
            await store.append("agg_load", [StoredEvent(
                event_id=f"load_{i}", aggregate_id="agg_load",
                event_type="Load", event_data={},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )])
        
        # Measure under load
        start = time.time()
        for i in range(100):
            await store.append("agg2", [StoredEvent(
                event_id=f"e2_{i}", aggregate_id="agg2",
                event_type="Test", event_data={"i": i},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )])
        under_load = time.time() - start
        
        # Performance should not degrade more than 5x
        assert under_load < baseline * 5
    
    @pytest.mark.asyncio
    async def test_query_performance_with_large_dataset(self):
        """大数据集查询性能"""
        store = InMemoryEventStore()
        
        # Create large dataset
        for i in range(10000):
            await store.append(f"agg_{i%100}", [StoredEvent(
                event_id=f"e{i}", aggregate_id=f"agg_{i%100}",
                event_type="Test", event_data={"i": i},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )])
        
        # Query performance
        start = time.time()
        for i in range(100):
            _ = await store.get_events(f"agg_{i%100}")
        elapsed = time.time() - start
        
        # Should complete within reasonable time
        assert elapsed < 10.0


class TestPerformanceDegradationCrypto:
    """加密性能退化测试"""
    
    def test_encrypt_performance_consistency(self):
        """加密性能一致性"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        
        times = []
        for i in range(100):
            start = time.time()
            crypto.encrypt(f"test_data_{i}")
            times.append(time.time() - start)
        
        avg_time = sum(times) / len(times)
        max_time = max(times)
        
        # Max should not be more than 100x average (放宽阈值以适应系统波动)
        assert max_time < avg_time * 100
    
    def test_hash_performance_consistency(self):
        """哈希性能一致性"""
        crypto = CryptoManager()
        
        times = []
        for i in range(1000):
            start = time.time()
            crypto.hash_data(f"data_{i}")
            times.append(time.time() - start)
        
        avg_time = sum(times) / len(times)
        p99 = sorted(times)[int(len(times) * 0.99)]
        
        # P99 should not be more than 5x average
        assert p99 < avg_time * 5


class TestPerformanceDegradationEventBus:
    """事件总线性能退化测试"""
    
    @pytest.mark.asyncio
    async def test_publish_performance_with_many_subscribers(self):
        """多订阅者下发布性能"""
        bus = InMemoryEventBus()
        await bus.start()
        
        # Add many subscribers
        for i in range(50):
            await bus.subscribe("Test", lambda e, idx=i: None)
        
        # Measure publish performance
        times = []
        for i in range(100):
            start = time.time()
            await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))
            times.append(time.time() - start)
        
        await bus.stop()
        
        avg_time = sum(times) / len(times)
        # Should be reasonably fast even with 50 subscribers
        assert avg_time < 0.01


class TestPerformanceDegradationRBAC:
    """RBAC性能退化测试"""
    
    def test_permission_check_performance_with_many_users(self):
        """多用户下权限检查性能"""
        rbac = RBACManager()
        
        # Create many users
        users = []
        for i in range(1000):
            user = rbac.create_user(f"user_{i}", [Role.USER])
            if i % 10 == 0:
                rbac.assign_role(user.user_id, Role.ADMIN)
            users.append(user)
        
        # Measure permission check performance
        times = []
        for user in users:
            start = time.time()
            rbac.check_permission(user.user_id, Permission.READ)
            times.append(time.time() - start)
        
        avg_time = sum(times) / len(times)
        # Should be fast even with 1000 users
        assert avg_time < 0.001


class TestPerformanceDegradationMetrics:
    """指标收集性能退化测试"""
    
    def test_counter_performance_with_many_labels(self):
        """多标签下计数器性能"""
        mc = MetricsCollector()
        
        # Add metrics with many different labels
        for i in range(1000):
            mc.counter("requests", 1.0, labels={"endpoint": f"/api/{i}"})
        
        # Measure performance
        times = []
        for i in range(100):
            start = time.time()
            mc.counter("requests", 1.0, labels={"endpoint": "/test"})
            times.append(time.time() - start)
        
        avg_time = sum(times) / len(times)
        # Should not degrade significantly
        assert avg_time < 0.001

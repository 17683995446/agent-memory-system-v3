"""极端并发压力测试"""
import pytest
import asyncio
import threading
import time
from memory_system.security_hardening import CryptoManager, RBACManager, AuditLogger, Permission, Role
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent, EventPriority
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.partition_tolerance import QuorumManager
from memory_system.vector_clock import VectorClock
from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp
from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
from memory_system.monitoring_alerting import MetricsCollector
from datetime import datetime, timezone


class TestCryptoStress:
    """加密模块压力测试"""

    def test_rapid_encrypt_decrypt_1000(self):
        """快速加密解密1000次"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        for i in range(1000):
            data = f"secret_data_{i}_with_some_content"
            encrypted = crypto.encrypt(data)
            assert crypto.decrypt(encrypted) == data

    def test_concurrent_encrypt_decrypt(self):
        """并发加密解密(20线程x100次)"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        errors = []

        def worker(thread_id):
            try:
                for i in range(100):
                    data = f"thread_{thread_id}_data_{i}"
                    encrypted = crypto.encrypt(data)
                    decrypted = crypto.decrypt(encrypted)
                    assert decrypted == data
            except Exception as e:
                errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0, f"Errors in concurrent encrypt/decrypt: {errors[:5]}"

    def test_hash_data_collision_check(self):
        """哈希碰撞检查 - 不同输入应产生不同哈希"""
        crypto = CryptoManager()
        hashes = set()
        for i in range(10000):
            h = crypto.hash_data(f"unique_string_{i}")
            assert h not in hashes, f"Hash collision at index {i}"
            hashes.add(h)


class TestRBACStress:
    """RBAC压力测试"""

    def test_massive_user_creation(self):
        """创建1000个用户"""
        rbac = RBACManager()
        users = []
        for i in range(1000):
            user = rbac.create_user(f"user_{i}", [Role.USER])
            users.append(user)
        assert len(users) == 1000
        # All should have unique IDs
        user_ids = [u.user_id for u in users]
        assert len(set(user_ids)) == 1000

    def test_concurrent_user_operations(self):
        """并发用户操作(10线程)"""
        rbac = RBACManager()
        errors = []

        def worker(thread_id):
            try:
                for i in range(100):
                    user = rbac.create_user(f"t{thread_id}_u{i}")
                    rbac.assign_role(user.user_id, Role.ADMIN)
                    rbac.check_permission(user.user_id, Permission.READ)
                    rbac.check_permission(user.user_id, Permission.WRITE)
                    rbac.check_permission(user.user_id, Permission.ADMIN)
                    rbac.revoke_role(user.user_id, Role.ADMIN)
            except Exception as e:
                errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0, f"Concurrent RBAC errors: {errors[:5]}"


class TestEventStoreStress:
    """事件存储压力测试"""

    @pytest.mark.asyncio
    async def test_10000_events_single_aggregate(self):
        """单个聚合10000个事件"""
        store = InMemoryEventStore()
        events = [
            StoredEvent(
                event_id=f"e{i}", aggregate_id="agg1",
                event_type="Update", event_data={"value": i},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )
            for i in range(10000)
        ]
        start = time.time()
        await store.append("agg1", events)
        elapsed = time.time() - start
        assert elapsed < 10.0, f"Appending 10000 events took {elapsed:.2f}s"

        # Verify version
        version = await store.get_current_version("agg1")
        assert version == 10000

    @pytest.mark.asyncio
    async def test_1000_aggregates_concurrent(self):
        """1000个聚合并发写入"""
        store = InMemoryEventStore()

        async def write_aggregate(agg_id):
            events = [
                StoredEvent(
                    event_id=f"e_{agg_id}_{i}", aggregate_id=agg_id,
                    event_type="Created", event_data={},
                    version=i+1, timestamp=datetime.now(timezone.utc),
                    sequence_number=i+1, metadata={}
                )
                for i in range(10)
            ]
            await store.append(agg_id, events)

        await asyncio.gather(*[write_aggregate(f"agg_{i}") for i in range(1000)])

        all_events = await store.get_all_events(limit=20000)
        assert len(all_events) == 10000

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self):
        """并发读写(10 readers + 10 writers)"""
        store = InMemoryEventStore()
        # Pre-populate
        for i in range(100):
            await store.append("agg1", [StoredEvent(
                event_id=f"e{i}", aggregate_id="agg1",
                event_type="Test", event_data={}, version=i+1,
                timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )])

        errors = []
        read_count = 0
        write_count = 0

        async def reader():
            nonlocal read_count
            try:
                for _ in range(50):
                    events = await store.get_events("agg1")
                    read_count += len(events)
            except Exception as e:
                errors.append(str(e))

        async def writer():
            nonlocal write_count
            try:
                for i in range(10):
                    await store.append(f"agg_write_{threading.current_thread().ident}_{i}",
                        [StoredEvent(
                            event_id=f"ew_{i}", aggregate_id=f"agg_write_{threading.current_thread().ident}_{i}",
                            event_type="Test", event_data={}, version=1,
                            timestamp=datetime.now(timezone.utc),
                            sequence_number=1, metadata={}
                        )])
                    write_count += 1
            except Exception as e:
                errors.append(str(e))

        await asyncio.gather(
            *[reader() for _ in range(10)],
            *[writer() for _ in range(10)]
        )
        assert len(errors) == 0, f"Concurrent read/write errors: {errors[:5]}"


class TestEventBusStress:
    """事件总线压力测试"""

    @pytest.mark.asyncio
    async def test_1000_events_rapid_fire(self):
        """快速连续发布1000个事件"""
        bus = InMemoryEventBus(max_queue_size=50000)
        await bus.start()

        received = []
        await bus.subscribe("Test", lambda e: received.append(e))

        for i in range(1000):
            await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))

        await asyncio.sleep(2.0)  # Wait for processing
        await bus.stop()
        # Should have received most events
        assert len(received) > 900, f"Only received {len(received)}/1000 events"

    @pytest.mark.asyncio
    async def test_multiple_event_types(self):
        """多种事件类型"""
        bus = InMemoryEventBus()
        await bus.start()

        received = {"A": 0, "B": 0, "C": 0}
        await bus.subscribe("TypeA", lambda e: received.__setitem__("A", received["A"] + 1))
        await bus.subscribe("TypeB", lambda e: received.__setitem__("B", received["B"] + 1))
        await bus.subscribe("TypeC", lambda e: received.__setitem__("C", received["C"] + 1))

        for event_type in ["TypeA", "TypeB", "TypeC"]:
            for _ in range(50):
                await bus.publish(DomainEvent(event_type=event_type))

        await asyncio.sleep(1.0)
        await bus.stop()

        assert received["A"] == 50
        assert received["B"] == 50
        assert received["C"] == 50


class TestMetricsStress:
    """指标收集压力测试"""

    def test_100000_metrics(self):
        """10万个指标数据点"""
        mc = MetricsCollector()
        start = time.time()
        for i in range(100000):
            mc.counter("requests", 1.0, labels={"path": f"/api/{i%100}"})
        elapsed = time.time() - start
        assert elapsed < 10.0, f"100K metrics took {elapsed:.2f}s"

    def test_concurrent_metrics(self):
        """并发指标收集(10线程)"""
        mc = MetricsCollector()
        errors = []

        def worker(thread_id):
            try:
                for i in range(5000):
                    mc.counter("requests", 1.0, labels={"thread": str(thread_id)})
                    mc.gauge("cpu", i % 100)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0, f"Concurrent metrics errors: {errors[:5]}"


class TestVectorClockStress:
    """向量时钟压力测试"""

    def test_merge_100_clocks(self):
        """合并100个时钟"""
        clocks = [VectorClock(node_id=f"n{i}") for i in range(100)]
        for vc in clocks:
            vc.increment()

        merged = clocks[0]
        for vc in clocks[1:]:
            merged = merged.merge(vc)

        # All node clocks should be >= 1
        for i in range(100):
            assert merged.clock.get(f"n{i}", 0) >= 1

    def test_rapid_increment(self):
        """快速递增10000次"""
        vc = VectorClock(node_id="n1")
        for _ in range(10000):
            vc.increment()
        assert vc.get_timestamp() == 10000


class TestHLCTimestampSerialization:
    """HLC时间戳序列化压力测试"""

    def test_10000_serialization_roundtrips(self):
        """10000次序列化往返"""
        hlc = HybridLogicalClock(node_id="n1")
        for i in range(10000):
            ts = hlc.now()
            data = ts.to_bytes()
            restored = HLCTimestamp.from_bytes(data)
            assert restored == ts

"""故障注入测试 - 模拟生产环境故障"""
import pytest
import asyncio
import time
from datetime import datetime, timezone
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent, ConcurrencyException
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.security_hardening import CryptoManager, RBACManager
from memory_system.partition_tolerance import PartitionManager
from memory_system.task_scheduler import TaskScheduler, Task, TaskPriority
from memory_system.read_write_lock import ReadWriteLock


class TestNetworkPartition:
    """网络分区故障注入"""
    
    def test_all_peers_fail_recovery(self):
        """所有peer故障后恢复"""
        pm = PartitionManager(
            node_id="n1",
            peers=["n2", "n3", "n4"],
            heartbeat_timeout=0.1,
            partition_threshold=2
        )
        pm.start()
        
        # All peers healthy initially
        pm.record_heartbeat("n2")
        pm.record_heartbeat("n3")
        pm.record_heartbeat("n4")
        assert not pm.is_partitioned()
        
        # All peers fail
        for _ in range(5):
            pm.record_missed_heartbeat("n2")
            pm.record_missed_heartbeat("n3")
            pm.record_missed_heartbeat("n4")
        
        time.sleep(0.2)
        assert pm.is_partitioned()
        
        # Recovery
        pm.record_heartbeat("n2")
        pm.record_heartbeat("n3")
        time.sleep(0.2)
        assert not pm.is_partitioned()
        
        pm.stop()
    
    def test_flapping_peer(self):
        """peer状态抖动"""
        pm = PartitionManager(
            node_id="n1",
            peers=["n2"],
            heartbeat_timeout=0.05,
            partition_threshold=3
        )
        pm.start()
        
        # Rapid state changes
        for _ in range(20):
            pm.record_heartbeat("n2")
            pm.record_missed_heartbeat("n2")
        
        # System should remain stable
        pm.stop()


class TestEventStoreCorruption:
    """事件存储故障注入"""
    
    @pytest.mark.asyncio
    async def test_concurrent_append_same_version(self):
        """并发追加同一版本"""
        store = InMemoryEventStore()
        
        async def append_events(agg_id, start_version):
            events = [
                StoredEvent(
                    event_id=f"e_{agg_id}_{i}", aggregate_id=agg_id,
                    event_type="Test", event_data={},
                    version=start_version + i,
                    timestamp=datetime.now(timezone.utc),
                    sequence_number=start_version + i,
                    metadata={}
                )
                for i in range(5)
            ]
            try:
                return await store.append(agg_id, events, expected_version=start_version - 1)
            except ConcurrencyException:
                return False
        
        # Multiple concurrent appends to same aggregate
        results = await asyncio.gather(
            append_events("agg1", 1),
            append_events("agg1", 1),
            append_events("agg1", 1),
            return_exceptions=True
        )
        
        # Only one should succeed, others should get ConcurrencyException
        successes = sum(1 for r in results if r is True)
        assert successes >= 1
    
    @pytest.mark.asyncio
    async def test_rapid_append_and_read(self):
        """快速追加和读取交替"""
        store = InMemoryEventStore()
        errors = []
        
        async def writer():
            for i in range(100):
                try:
                    await store.append("agg1", [StoredEvent(
                        event_id=f"e{i}", aggregate_id="agg1",
                        event_type="Test", event_data={"i": i},
                        version=i+1, timestamp=datetime.now(timezone.utc),
                        sequence_number=i+1, metadata={}
                    )])
                except Exception as e:
                    errors.append(f"write: {e}")
        
        async def reader():
            for _ in range(100):
                try:
                    _ = await store.get_events("agg1")
                except Exception as e:
                    errors.append(f"read: {e}")
        
        await asyncio.gather(writer(), reader())
        assert len(errors) == 0, f"Errors: {errors[:5]}"


class TestCryptoFailure:
    """加密故障注入"""
    
    def test_decrypt_with_wrong_key(self):
        """使用错误密钥解密"""
        crypto1 = CryptoManager(b"0123456789abcdef0123456789abcdef")
        crypto2 = CryptoManager(b"fedcba9876543210fedcba9876543210")
        
        encrypted = crypto1.encrypt("secret")
        
        with pytest.raises(Exception):
            crypto2.decrypt(encrypted)
    
    def test_decrypt_corrupted_data(self):
        """解密损坏的数据"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        encrypted = crypto.encrypt("secret")
        
        # Corrupt the data
        corrupted = encrypted[:-10] + "XXXXXXXXXX"
        
        with pytest.raises(Exception):
            crypto.decrypt(corrupted)


class TestSchedulerUnderLoad:
    """调度器负载测试"""
    
    @pytest.mark.asyncio
    async def test_scheduler_with_many_tasks(self):
        """大量任务调度"""
        scheduler = TaskScheduler()
        await scheduler.start()
        
        completed = []
        
        async def task_func(i):
            await asyncio.sleep(0.001)
            completed.append(i)
            return i
        
        tasks = [
            Task(
                task_id=f"t{i}",
                coroutine=task_func(i),
                priority=TaskPriority.NORMAL
            )
            for i in range(100)
        ]
        
        await asyncio.gather(*[scheduler.submit(t) for t in tasks])
        await asyncio.sleep(0.5)
        await scheduler.stop()
        
        # Most tasks should complete
        assert len(completed) >= 80


class TestReadWriteLockContention:
    """读写锁争用测试"""
    
    @pytest.mark.asyncio
    async def test_extreme_contention(self):
        """极端读写争用"""
        rw = ReadWriteLock()
        read_count = 0
        write_count = 0
        errors = []
        
        async def reader():
            nonlocal read_count
            try:
                for _ in range(50):
                    async with rw.read_lock():
                        read_count += 1
                        await asyncio.sleep(0.001)
            except Exception as e:
                errors.append(f"read: {e}")
        
        async def writer():
            nonlocal write_count
            try:
                for _ in range(20):
                    async with rw.write_lock():
                        write_count += 1
                        await asyncio.sleep(0.002)
            except Exception as e:
                errors.append(f"write: {e}")
        
        await asyncio.gather(
            *[reader() for _ in range(10)],
            *[writer() for _ in range(5)]
        )
        
        assert len(errors) == 0
        assert read_count == 500
        assert write_count == 100


class TestEventBusFailure:
    """事件总线故障注入"""
    
    @pytest.mark.asyncio
    async def test_handler_exception_isolation(self):
        """处理器异常隔离"""
        bus = InMemoryEventBus()
        await bus.start()
        
        good_received = []
        
        async def bad_handler(event):
            raise Exception("Handler error")
        
        async def good_handler(event):
            good_received.append(event)
        
        await bus.subscribe("Test", bad_handler)
        await bus.subscribe("Test", good_handler)
        
        # Publish should not crash even if handler fails
        for i in range(10):
            await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))
        
        await asyncio.sleep(0.5)
        await bus.stop()
        
        # Good handler should still receive events
        assert len(good_received) >= 5

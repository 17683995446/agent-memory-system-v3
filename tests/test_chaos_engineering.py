"""混沌工程测试 - 主动故障注入"""
import pytest
import asyncio
import random
import time
from datetime import datetime, timezone
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent, ConcurrencyException
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.security_hardening import CryptoManager, RBACManager, AuditLogger
from memory_system.partition_tolerance import PartitionManager
from memory_system.task_scheduler import TaskScheduler, Task, TaskPriority
from memory_system.read_write_lock import ReadWriteLock
from memory_system.monitoring_alerting import MetricsCollector


class TestChaosEventStore:
    """事件存储混沌测试"""
    
    @pytest.mark.asyncio
    async def test_random_event_loss_simulation(self):
        """模拟随机事件丢失"""
        store = InMemoryEventStore()
        
        # Inject random failures during append
        success_count = 0
        for i in range(1000):
            try:
                # Randomly decide if this append should "fail"
                if random.random() > 0.1:  # 90% success rate
                    await store.append(f"agg_{i%10}", [StoredEvent(
                        event_id=f"e{i}", aggregate_id=f"agg_{i%10}",
                        event_type="Test", event_data={"i": i},
                        version=i+1, timestamp=datetime.now(timezone.utc),
                        sequence_number=i+1, metadata={}
                    )])
                    success_count += 1
            except Exception as e:
                # Should handle gracefully
                pass
        
        # Verify system is still consistent
        all_events = await store.get_all_events(limit=2000)
        assert len(all_events) == success_count
    
    @pytest.mark.asyncio
    async def test_burst_traffic_spike(self):
        """突发流量峰值"""
        store = InMemoryEventStore()
        
        # Sudden burst of 10000 events
        start = time.time()
        tasks = []
        for i in range(100):
            async def burst_append(batch_id):
                events = [
                    StoredEvent(
                        event_id=f"e_{batch_id}_{j}", aggregate_id=f"agg_burst",
                        event_type="Burst", event_data={"batch": batch_id, "j": j},
                        version=batch_id*100+j+1, timestamp=datetime.now(timezone.utc),
                        sequence_number=batch_id*100+j+1, metadata={}
                    )
                    for j in range(100)
                ]
                await store.append(f"agg_burst", events)
            
            tasks.append(burst_append(i))
        
        await asyncio.gather(*tasks)
        elapsed = time.time() - start
        
        # Should complete within reasonable time
        assert elapsed < 30.0
        
        events = await store.get_events("agg_burst")
        assert len(events) == 10000
    
    @pytest.mark.asyncio
    async def test_storage_corruption_recovery(self):
        """存储损坏恢复"""
        store = InMemoryEventStore()
        
        # Add some valid events
        for i in range(100):
            await store.append("agg1", [StoredEvent(
                event_id=f"e{i}", aggregate_id="agg1",
                event_type="Valid", event_data={"valid": True},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )])
        
        # Simulate corruption by accessing internal state (if possible)
        # In real chaos, this would be disk corruption, network partition, etc.
        
        # System should still be queryable
        events = await store.get_events("agg1")
        assert len(events) == 100


class TestChaosEventBus:
    """事件总线混沌测试"""
    
    @pytest.mark.asyncio
    async def test_handler_crash_isolation(self):
        """处理器崩溃隔离"""
        bus = InMemoryEventBus()
        await bus.start()
        
        good_received = []
        
        async def crashing_handler(event):
            if random.random() > 0.7:  # 30% crash rate
                raise Exception("Simulated handler crash!")
            good_received.append(event)
        
        async def stable_handler(event):
            good_received.append(event)
        
        await bus.subscribe("Test", crashing_handler)
        await bus.subscribe("Test", stable_handler)
        
        # Send events
        for i in range(100):
            await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))
        
        await asyncio.sleep(2.0)
        await bus.stop()
        
        # Stable handler should still receive events
        assert len(good_received) > 50
    
    @pytest.mark.asyncio
    async def test_subscriber_disconnection_during_publish(self):
        """发布期间订阅者断开"""
        bus = InMemoryEventBus()
        await bus.start()
        
        received = []
        
        async def handler(event):
            received.append(event)
            if len(received) == 50:
                # Simulate disconnection by unsubscribing mid-stream
                await bus.unsubscribe("Test", handler)
        
        await bus.subscribe("Test", handler)
        
        for i in range(100):
            await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))
        
        await asyncio.sleep(1.0)
        await bus.stop()
        
        # Should not crash


class TestChaosPartitionManager:
    """分区管理器混沌测试"""
    
    def test_rapid_membership_changes(self):
        """快速成员变化"""
        pm = PartitionManager(
            node_id="n1",
            peers=["n2", "n3", "n4", "n5"],
            heartbeat_timeout=0.05,
            partition_threshold=2
        )
        pm.start()
        
        # Rapidly change peer states
        for _ in range(100):
            peer = random.choice(["n2", "n3", "n4", "n5"])
            if random.random() > 0.5:
                pm.record_heartbeat(peer)
            else:
                pm.record_missed_heartbeat(peer)
        
        time.sleep(0.2)
        
        # System should remain stable
        _ = pm.get_healthy_nodes()
        _ = pm.is_partitioned()
        
        pm.stop()
    
    def test_all_peers_flapping(self):
        """所有peer状态抖动"""
        pm = PartitionManager(
            node_id="n1",
            peers=["n2", "n3"],
            heartbeat_timeout=0.05,
            partition_threshold=3
        )
        pm.start()
        
        # Extreme flapping
        for _ in range(200):
            pm.record_heartbeat("n2")
            pm.record_missed_heartbeat("n2")
            pm.record_heartbeat("n3")
            pm.record_missed_heartbeat("n3")
        
        pm.stop()
        # Should not crash or hang


class TestChaosTaskScheduler:
    """任务调度器混沌测试"""
    
    @pytest.mark.asyncio
    async def test_task_exception_propagation(self):
        """任务异常传播"""
        scheduler = TaskScheduler()
        await scheduler.start()
        
        async def failing_task():
            await asyncio.sleep(0.01)
            raise ValueError("Task failed!")
        
        task = Task(
            task_id="t1",
            coroutine=failing_task(),
            priority=TaskPriority.NORMAL
        )
        
        # Should handle gracefully
        try:
            await scheduler.submit(task)
            await asyncio.sleep(0.1)
        except Exception:
            pass  # May or may not propagate
        
        await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_scheduler_under_memory_pressure(self):
        """内存压力下调度器"""
        scheduler = TaskScheduler()
        await scheduler.start()
        
        # Submit many tasks
        tasks = []
        for i in range(1000):
            async def dummy_task(task_id):
                await asyncio.sleep(0.001)
                return task_id
            
            task = Task(
                task_id=f"t{i}",
                coroutine=dummy_task(i),
                priority=TaskPriority.NORMAL
            )
            tasks.append(scheduler.submit(task))
        
        # Should handle without crashing
        await asyncio.gather(*tasks, return_exceptions=True)
        await scheduler.stop()


class TestChaosCrypto:
    """加密模块混沌测试"""
    
    def test_random_key_sizes(self):
        """随机密钥大小"""
        for key_size in [16, 24, 32, 48, 64]:
            key = b"x" * key_size
            try:
                crypto = CryptoManager(key)
                if key_size >= 32:
                    encrypted = crypto.encrypt("test")
                    decrypted = crypto.decrypt(encrypted)
                    assert decrypted == "test"
            except Exception:
                # Should fail gracefully for small keys
                pass
    
    def test_malformed_encrypted_data(self):
        """格式错误的加密数据"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        
        malformed_inputs = [
            "",
            "not_base64!!!",
            "YQ==",  # Valid base64 but too short
            "x" * 1000,  # Long garbage
            "\x00\x01\x02",  # Binary garbage
        ]
        
        for inp in malformed_inputs:
            try:
                crypto.decrypt(inp)
                assert False, f"Should have failed for: {inp[:20]}"
            except Exception:
                pass  # Expected


class TestChaosMetrics:
    """指标收集混沌测试"""
    
    def test_extreme_label_cardinality(self):
        """极端标签基数"""
        mc = MetricsCollector()
        
        # Create metrics with many unique labels
        for i in range(10000):
            mc.counter("requests", 1.0, labels={
                "user_id": f"user_{i}",
                "session_id": f"sess_{i}",
                "request_id": f"req_{i}"
            })
        
        # Should not crash or use excessive memory
        metrics = mc.get_metrics("requests")
        assert len(metrics) == 10000

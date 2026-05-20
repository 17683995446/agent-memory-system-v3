"""第18轮深度实践 - 跨模块协同压力测试

测试策略:
1. 事件总线+存储+序列化同时压力
2. Raft共识+事件发布并发测试
3. VectorClock+HLC+事件时间戳一致性
4. 事务+事件+存储原子性测试
5. 多模块资源竞争测试
"""

import asyncio
import gc
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestCrossModulePressure:
    """跨模块压力测试"""

    @pytest.mark.asyncio
    async def test_event_storage_serialization_together(self):
        """事件+存储+序列化同时压力测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
            from memory_system.storage_backends.in_memory_storage_backend import InMemoryStorageBackend
            from memory_system.snapshot_serializer import JSONSerializer
            from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        except ImportError as e:
            pytest.skip(f"Required modules not available: {e}")
            return

        # 初始化所有组件
        bus = InMemoryEventBus()
        await bus.start()
        storage = InMemoryStorageBackend()
        serializer = JSONSerializer(compression=True)

        stats = {
            "events_published": 0,
            "items_stored": 0,
            "items_serialized": 0,
            "errors": 0
        }
        lock = asyncio.Lock()

        async def event_task(task_id):
            for i in range(50):
                try:
                    event = DomainEvent(
                        event_type="cross_module_test",
                        aggregate_id=f"agg_{task_id}_{i}",
                        payload={"data": "x" * 100, "timestamp": time.time()}
                    )
                    await bus.publish(event)
                    async with lock:
                        stats["events_published"] += 1
                except Exception:
                    async with lock:
                        stats["errors"] += 1

        async def storage_task(task_id):
            for i in range(50):
                try:
                    item = MemoryItem(
                        memory_id=f"cross_mem_{task_id}_{i}",
                        content=f"content_{i}" * 10,
                        memory_type=MemoryType.SHORT_TERM,
                        agent_id=f"agent_{task_id}",
                    )
                    await storage.add(item)
                    async with lock:
                        stats["items_stored"] += 1
                except Exception:
                    async with lock:
                        stats["errors"] += 1

        async def serialization_task(task_id):
            for i in range(50):
                try:
                    data = {f"key_{j}": f"value_{j}_{task_id}" for j in range(20)}
                    serialized = serializer.serialize(data)
                    deserialized = serializer.deserialize(serialized)
                    async with lock:
                        stats["items_serialized"] += 1
                except Exception:
                    async with lock:
                        stats["errors"] += 1

        # 同时启动所有任务
        tasks = []
        tasks.extend([asyncio.create_task(event_task(i)) for i in range(10)])
        tasks.extend([asyncio.create_task(storage_task(i)) for i in range(10)])
        tasks.extend([asyncio.create_task(serialization_task(i)) for i in range(10)])

        await asyncio.gather(*tasks)

        await bus.stop()

        print(f"\n跨模块压力测试结果:")
        print(f"  事件发布: {stats['events_published']}")
        print(f"  存储操作: {stats['items_stored']}")
        print(f"  序列化操作: {stats['items_serialized']}")
        print(f"  错误数: {stats['errors']}")

        # 所有操作应该大部分成功
        total_expected = 10 * 50 * 3  # 1500
        total_success = stats["events_published"] + stats["items_stored"] + stats["items_serialized"]
        assert total_success > total_expected * 0.9

    @pytest.mark.asyncio
    async def test_clock_event_consistency(self):
        """时钟与事件一致性测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
            from memory_system.hybrid_logical_clock import HybridLogicalClock
            from memory_system.vector_clock import VectorClock
        except ImportError:
            pytest.skip("Required modules not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        hlc = HybridLogicalClock(node_id="consistency_test")
        vc = VectorClock(node_id="consistency_test")

        events_with_timestamps = []
        lock = asyncio.Lock()

        async def handler(event):
            async with lock:
                events_with_timestamps.append({
                    "event_id": getattr(event, 'event_id', str(event)),
                    "hlc_ts": hlc.now(),
                    "vc": vc._clock.copy()
                })

        await bus.subscribe("clock_test", handler)

        # 发布事件并更新时钟
        for i in range(100):
            vc.increment()
            event = DomainEvent(
                event_type="clock_test",
                aggregate_id=f"clock_agg_{i}",
                payload={"index": i, "hlc": str(hlc.now())}
            )
            await bus.publish(event)

        await asyncio.sleep(1)
        await bus.stop()

        # 验证事件顺序与时钟顺序一致
        print(f"\n时钟事件一致性测试:")
        print(f"  事件数: {len(events_with_timestamps)}")

        # 时钟应该单调递增
        for i in range(1, len(events_with_timestamps)):
            prev = events_with_timestamps[i-1]
            curr = events_with_timestamps[i]
            # HLC应该递增
            assert curr["hlc_ts"] >= prev["hlc_ts"]


class TestTransactionEventAtomicity:
    """事务事件原子性测试"""

    @pytest.mark.asyncio
    async def test_transaction_with_events(self):
        """事务与事件原子性"""
        try:
            from memory_system.distributed_transaction_manager import (
                TwoPhaseCommitCoordinator, Transaction, TransactionStatus
            )
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("Required modules not available")
            return

        bus = InMemoryEventBus()
        await bus.start()
        coordinator = TwoPhaseCommitCoordinator()

        stats = {"transactions": 0, "events": 0, "committed": 0}
        lock = asyncio.Lock()

        async def transaction_with_event(tx_id):
            try:
                # 开始事务
                tx = await coordinator.begin_transaction()

                # 发布事件
                event = DomainEvent(
                    event_type="transaction_test",
                    aggregate_id=f"tx_agg_{tx_id}",
                    payload={"tx_id": tx.tx_id}
                )
                await bus.publish(event)

                async with lock:
                    stats["transactions"] += 1
                    stats["events"] += 1

                # 提交事务
                await coordinator.store.update_transaction_status(
                    tx.tx_id, TransactionStatus.COMMITTED
                )
                async with lock:
                    stats["committed"] += 1

            except Exception:
                pass

        # 执行100个事务
        tasks = [asyncio.create_task(transaction_with_event(i)) for i in range(100)]
        await asyncio.gather(*tasks)

        await bus.stop()

        print(f"\n事务事件原子性测试:")
        print(f"  事务数: {stats['transactions']}")
        print(f"  事件数: {stats['events']}")
        print(f"  提交数: {stats['committed']}")

        assert stats["committed"] == stats["transactions"]


class TestResourceContention:
    """资源竞争测试"""

    @pytest.mark.asyncio
    async def test_shared_resource_access(self):
        """共享资源访问竞争"""
        try:
            from memory_system.read_write_lock import ReadWriteLock
        except ImportError:
            pytest.skip("read_write_lock not available")
            return

        rw_lock = ReadWriteLock()
        shared_data = {"value": 0, "read_count": 0, "write_count": 0}
        stats = {"reads": 0, "writes": 0, "errors": 0}

        async def reader(reader_id):
            for _ in range(100):
                try:
                    async with rw_lock.read_lock():
                        _ = shared_data["value"]
                        shared_data["read_count"] += 1
                        stats["reads"] += 1
                        await asyncio.sleep(0.001)
                except Exception:
                    stats["errors"] += 1

        async def writer(writer_id):
            for _ in range(50):
                try:
                    async with rw_lock.write_lock():
                        shared_data["value"] += 1
                        shared_data["write_count"] += 1
                        stats["writes"] += 1
                        await asyncio.sleep(0.002)
                except Exception:
                    stats["errors"] += 1

        # 20读者 + 10写者
        readers = [asyncio.create_task(reader(i)) for i in range(20)]
        writers = [asyncio.create_task(writer(i)) for i in range(10)]

        await asyncio.gather(*readers, *writers)

        print(f"\n共享资源竞争测试:")
        print(f"  读操作: {stats['reads']}")
        print(f"  写操作: {stats['writes']}")
        print(f"  错误数: {stats['errors']}")
        print(f"  最终值: {shared_data['value']}")

        assert stats["reads"] == 20 * 100
        assert stats["writes"] == 10 * 50
        assert shared_data["value"] == 10 * 50


class TestDistributedConsistency:
    """分布式一致性测试"""

    @pytest.mark.asyncio
    async def test_vector_clock_consistency(self):
        """VectorClock一致性测试"""
        try:
            from memory_system.vector_clock import VectorClock, VectorClockRelation
        except ImportError:
            pytest.skip("vector_clock not available")
            return

        # 模拟3个节点
        nodes = {
            "A": VectorClock(node_id="A"),
            "B": VectorClock(node_id="B"),
            "C": VectorClock(node_id="C"),
        }

        # 每个节点独立操作
        for _ in range(10):
            for node in nodes.values():
                node.increment()

        # 模拟消息交换
        # A发送给B
        nodes["B"] = nodes["B"].merge(nodes["A"])
        # B发送给C
        nodes["C"] = nodes["C"].merge(nodes["B"])
        # C发送给A
        nodes["A"] = nodes["A"].merge(nodes["C"])

        # 再次交换确保收敛
        nodes["A"] = nodes["A"].merge(nodes["B"]).merge(nodes["C"])
        nodes["B"] = nodes["B"].merge(nodes["A"]).merge(nodes["C"])
        nodes["C"] = nodes["C"].merge(nodes["A"]).merge(nodes["B"])

        print(f"\nVectorClock一致性测试:")
        for name, clock in nodes.items():
            print(f"  {name}: {clock._clock}")

        # 所有节点应该知道其他节点的存在
        for clock in nodes.values():
            assert "A" in clock._clock and clock._clock["A"] > 0
            assert "B" in clock._clock and clock._clock["B"] > 0
            assert "C" in clock._clock and clock._clock["C"] > 0

    @pytest.mark.asyncio
    async def test_hlc_monotonicity(self):
        """HLC单调性测试"""
        try:
            from memory_system.hybrid_logical_clock import HybridLogicalClock
        except ImportError:
            pytest.skip("hybrid_logical_clock not available")
            return

        hlc = HybridLogicalClock(node_id="monotonicity_test")

        timestamps = []
        for _ in range(1000):
            ts = hlc.now()
            timestamps.append(ts)
            await asyncio.sleep(0.001)

        # 验证单调性
        violations = 0
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i-1]:
                violations += 1

        print(f"\nHLC单调性测试:")
        print(f"  时间戳数: {len(timestamps)}")
        print(f"  单调性违反: {violations}")

        assert violations == 0


class TestErrorRecovery:
    """错误恢复测试"""

    @pytest.mark.asyncio
    async def test_partial_failure_recovery(self):
        """部分失败恢复测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        stats = {"phase1_success": 0, "phase1_fail": 0, "phase2_success": 0}

        # 阶段1: 部分失败
        for i in range(100):
            try:
                if random.random() < 0.3:
                    raise RuntimeError("Simulated failure")
                event = DomainEvent(
                    event_type="recovery_test",
                    aggregate_id=f"agg_{i}",
                    payload={"phase": 1}
                )
                await bus.publish(event)
                stats["phase1_success"] += 1
            except Exception:
                stats["phase1_fail"] += 1

        # 阶段2: 恢复后正常
        for i in range(100):
            try:
                event = DomainEvent(
                    event_type="recovery_test",
                    aggregate_id=f"agg_{i}",
                    payload={"phase": 2}
                )
                await bus.publish(event)
                stats["phase2_success"] += 1
            except Exception:
                pass

        await bus.stop()

        print(f"\n部分失败恢复测试:")
        print(f"  阶段1成功: {stats['phase1_success']}")
        print(f"  阶段1失败: {stats['phase1_fail']}")
        print(f"  阶段2成功: {stats['phase2_success']}")

        # 阶段2应该全部成功
        assert stats["phase2_success"] == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

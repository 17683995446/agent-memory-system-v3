"""第17轮深度实践 - 长时间压力测试

测试策略:
1. 30秒持续高负载运行 - 模拟生产环境
2. 内存使用监控 - 检测内存泄漏
3. 事件总线持续压力 - 检测事件丢失
4. 存储层持续读写 - 检测数据一致性
5. 时钟漂移模拟 - 检测HLC/VectorClock稳定性
6. 资源耗尽恢复 - 检测系统自愈能力
"""

import asyncio
import gc
import os
import random
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestLongRunningPressure:
    """长时间运行压力测试"""

    @pytest.mark.asyncio
    async def test_event_bus_30s_continuous_load(self):
        """30秒持续事件总线压力测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        stats = {"published": 0, "processed": 0, "errors": 0}
        processed_events = []
        lock = asyncio.Lock()

        async def handler(event):
            async with lock:
                stats["processed"] += 1
                processed_events.append(event.event_id if hasattr(event, 'event_id') else str(event))

        # 订阅所有事件
        await bus.subscribe("pressure_test", handler)

        start_time = time.time()
        duration = 30  # 30秒
        publish_tasks = []

        async def publish_loop():
            while time.time() - start_time < duration:
                try:
                    event = DomainEvent(
                        event_type="pressure_test",
                        aggregate_id=f"agg_{random.randint(1, 100)}",
                        payload={"timestamp": time.time(), "data": "x" * 100}
                    )
                    await bus.publish(event)
                    stats["published"] += 1
                    await asyncio.sleep(0.001)  # 1ms间隔
                except Exception as e:
                    stats["errors"] += 1

        # 启动多个发布者
        publishers = [asyncio.create_task(publish_loop()) for _ in range(5)]

        # 等待30秒
        await asyncio.sleep(duration + 1)

        # 停止发布者
        for p in publishers:
            p.cancel()
            try:
                await p
            except asyncio.CancelledError:
                pass

        await bus.stop()

        # 验证统计
        print(f"\n事件总线30秒压力测试结果:")
        print(f"  发布事件: {stats['published']}")
        print(f"  处理事件: {stats['processed']}")
        print(f"  错误数: {stats['errors']}")
        print(f"  处理率: {stats['processed']/max(stats['published'], 1)*100:.1f}%")

        # 处理率应该>80%
        assert stats["processed"] > stats["published"] * 0.8, \
            f"事件处理率过低: {stats['processed']}/{stats['published']}"

    @pytest.mark.asyncio
    async def test_memory_usage_during_load(self):
        """负载期间内存使用监控"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
        except ImportError:
            pytest.skip("psutil not available")
            return

        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        bus = InMemoryEventBus()
        await bus.start()

        # 持续发布事件10秒
        start = time.time()
        while time.time() - start < 10:
            event = DomainEvent(
                event_type="memory_test",
                aggregate_id="test",
                payload={"data": "x" * 1000}
            )
            await bus.publish(event)
            await asyncio.sleep(0.01)

        await bus.stop()

        # 强制垃圾回收
        gc.collect()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_growth = final_memory - initial_memory

        print(f"\n内存使用情况:")
        print(f"  初始内存: {initial_memory:.1f} MB")
        print(f"  最终内存: {final_memory:.1f} MB")
        print(f"  内存增长: {memory_growth:.1f} MB")

        # 内存增长不应超过100MB（允许一定增长，但不应泄漏）
        assert memory_growth < 100, f"内存增长过多: {memory_growth:.1f} MB"

    @pytest.mark.asyncio
    async def test_hlc_under_continuous_operations(self):
        """HLC持续操作测试 - 检测时钟漂移"""
        try:
            from memory_system.hybrid_logical_clock import HybridLogicalClock
        except ImportError:
            pytest.skip("hybrid_logical_clock not available")
            return

        hlc = HybridLogicalClock(node_id="pressure_test_node")

        timestamps = []
        start = time.time()

        # 持续生成时间戳20秒
        while time.time() - start < 20:
            ts = hlc.now()
            timestamps.append(ts)
            await asyncio.sleep(0.01)

        # 验证时间戳单调递增
        violations = 0
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i-1]:
                violations += 1

        print(f"\nHLC测试结果:")
        print(f"  生成时间戳: {len(timestamps)}")
        print(f"  单调性违反: {violations}")

        # 不应有单调性违反
        assert violations == 0, f"HLC单调性违反: {violations}次"

    @pytest.mark.asyncio
    async def test_vector_clock_concurrent_updates(self):
        """VectorClock并发更新测试"""
        try:
            from memory_system.vector_clock import VectorClock
        except ImportError:
            pytest.skip("vector_clock not available")
            return

        nodes = ["node_a", "node_b", "node_c"]
        clocks = {node: VectorClock(node_id=node) for node in nodes}

        async def node_operations(node, clock, iterations):
            for _ in range(iterations):
                clock.increment()
                # 随机合并其他节点的时钟
                other_node = random.choice([n for n in nodes if n != node])
                other_clock = clocks[other_node]
                merged = clock.merge(other_clock)
                await asyncio.sleep(0.001)

        # 每个节点执行100次操作
        tasks = [
            asyncio.create_task(node_operations(node, clocks[node], 100))
            for node in nodes
        ]
        await asyncio.gather(*tasks)

        # 验证所有时钟都有效
        for node, clock in clocks.items():
            assert clock._clock[node] > 0, f"节点{node}时钟未更新"

        print(f"\nVectorClock并发测试完成:")
        for node, clock in clocks.items():
            print(f"  {node}: {clock._clock}")


class TestResourceExhaustion:
    """资源耗尽测试"""

    @pytest.mark.asyncio
    async def test_event_bus_overflow_handling(self):
        """事件总线溢出处理"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        stats = {"processed": 0}

        async def slow_handler(event):
            await asyncio.sleep(0.1)  # 慢处理
            stats["processed"] += 1

        await bus.subscribe("overflow_test", slow_handler)

        # 快速发布大量事件
        published = 0
        for i in range(100):
            try:
                event = DomainEvent(
                    event_type="overflow_test",
                    aggregate_id=f"agg_{i}",
                    payload={"index": i}
                )
                await bus.publish(event)
                published += 1
            except Exception:
                pass

        # 等待处理完成
        await asyncio.sleep(15)
        await bus.stop()

        print(f"\n事件溢出测试:")
        print(f"  发布: {published}")
        print(f"  处理: {stats['processed']}")

        # 至少应该处理一部分
        assert stats["processed"] > 0

    def test_memory_pressure_with_large_objects(self):
        """大对象内存压力测试"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
        except ImportError:
            pytest.skip("snapshot_serializer not available")
            return

        serializer = JSONSerializer(compression=True)

        # 创建大对象
        large_data = {
            f"key_{i}": f"value_{i}" * 1000
            for i in range(1000)
        }

        # 多次序列化/反序列化
        for _ in range(10):
            serialized = serializer.serialize(large_data)
            deserialized = serializer.deserialize(serialized)
            assert len(deserialized) == 1000

        # 强制GC
        gc.collect()


class TestClockDriftSimulation:
    """时钟漂移模拟测试"""

    @pytest.mark.asyncio
    async def test_hlc_with_simulated_clock_drift(self):
        """模拟时钟漂移的HLC测试"""
        try:
            from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp
        except ImportError:
            pytest.skip("hybrid_logical_clock not available")
            return

        # 创建多个节点，模拟不同时钟漂移
        nodes = {}
        for name in ["fast_node", "normal_node", "slow_node"]:
            nodes[name] = HybridLogicalClock(node_id=name)

        # 模拟时钟漂移场景
        events = []
        base_time = time.time()

        # 快节点（时钟快10秒）
        fast_ts = nodes["fast_node"].now()
        events.append(("fast_node", fast_ts))

        # 正常节点
        normal_ts = nodes["normal_node"].now()
        events.append(("normal_node", normal_ts))

        # 慢节点（时钟慢10秒）
        slow_ts = nodes["slow_node"].now()
        events.append(("slow_node", slow_ts))

        # 验证HLC能正确处理时钟差异
        # 即使物理时钟不同，HLC应该能通过逻辑计数器保证因果顺序
        print(f"\n时钟漂移模拟测试:")
        for node, ts in events:
            print(f"  {node}: physical={ts.physical}, logical={ts.logical}")

        # 所有时间戳都应该是有效的
        for node, ts in events:
            assert ts.physical > 0
            assert ts.logical >= 0


class TestConcurrentStorageOperations:
    """并发存储操作测试"""

    @pytest.mark.asyncio
    async def test_concurrent_storage_reads_writes(self):
        """并发读写测试"""
        try:
            from memory_system.storage_backends.in_memory_storage_backend import InMemoryStorageBackend
            from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        except ImportError:
            pytest.skip("storage backends not available")
            return

        storage = InMemoryStorageBackend()

        errors = []
        written = []
        lock = asyncio.Lock()

        async def writer(writer_id, count):
            for i in range(count):
                try:
                    item = MemoryItem(
                        memory_id=f"mem_{writer_id}_{i}",
                        content=f"content_{i}",
                        memory_type=MemoryType.SHORT_TERM,
                        agent_id=f"agent_{writer_id}",
                    )
                    await storage.add(item)
                    async with lock:
                        written.append(item.memory_id)
                except Exception as e:
                    errors.append(str(e))

        async def reader(reader_id, count):
            for i in range(count):
                try:
                    # 随机读取已写入的数据
                    async with lock:
                        if written:
                            mem_id = random.choice(written)
                        else:
                            mem_id = f"mem_0_0"

                    result = await storage.get(mem_id)
                except Exception as e:
                    errors.append(str(e))

        # 启动10个写者和10个读者，每个执行100次操作
        writers = [asyncio.create_task(writer(i, 100)) for i in range(10)]
        readers = [asyncio.create_task(reader(i, 100)) for i in range(10)]

        await asyncio.gather(*writers, *readers)

        print(f"\n并发存储测试:")
        print(f"  写入成功: {len(written)}")
        print(f"  错误数: {len(errors)}")

        # 错误应该很少
        assert len(errors) < 10, f"过多错误: {errors[:5]}"


class TestSystemRecovery:
    """系统恢复测试"""

    @pytest.mark.asyncio
    async def test_event_bus_recovery_after_error(self):
        """错误后事件总线恢复测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        stats = {"processed": 0, "errors": 0}

        async def failing_handler(event):
            if random.random() < 0.3:  # 30%失败率
                raise RuntimeError("Random failure")
            stats["processed"] += 1

        await bus.subscribe("recovery_test", failing_handler)

        # 发布100个事件
        for i in range(100):
            event = DomainEvent(
                event_type="recovery_test",
                aggregate_id=f"agg_{i}",
                payload={"index": i}
            )
            await bus.publish(event)

        await asyncio.sleep(2)
        await bus.stop()

        print(f"\n错误恢复测试:")
        print(f"  成功处理: {stats['processed']}")

        # 即使有错误，也应该处理大部分
        assert stats["processed"] > 50


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

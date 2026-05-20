"""第19轮深度实践 - 120秒极限压力测试

测试策略:
1. 120秒持续极限压力 - 双倍时长
2. 内存泄漏深度检测 - 线性回归分析内存趋势
3. 事件处理吞吐量统计 - 持续监控
4. GC压力测试 - 频繁触发垃圾回收
5. 资源使用峰值检测 - CPU/内存/IO峰值
"""

import asyncio
import gc
import os
import random
import sys
import time
from collections import deque
from statistics import linear_regression

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestUltimateLongRunning:
    """极限长时间运行测试"""

    @pytest.mark.asyncio
    async def test_120s_ultimate_pressure(self):
        """120秒极限压力测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        stats = {
            "published": 0,
            "processed": 0,
            "errors": 0,
            "latencies": deque(maxlen=2000)
        }
        lock = asyncio.Lock()

        async def handler(event):
            async with lock:
                stats["processed"] += 1
                if hasattr(event, 'payload') and 'timestamp' in event.payload:
                    latency = time.time() - event.payload['timestamp']
                    stats["latencies"].append(latency)

        await bus.subscribe("ultimate_test", handler)

        start_time = time.time()
        duration = 120  # 120秒

        async def publisher(pub_id):
            while time.time() - start_time < duration:
                try:
                    event = DomainEvent(
                        event_type="ultimate_test",
                        aggregate_id=f"agg_{pub_id}_{random.randint(1, 1000)}",
                        payload={
                            "timestamp": time.time(),
                            "publisher": pub_id,
                            "data": "x" * random.randint(10, 1000)
                        }
                    )
                    await bus.publish(event)
                    async with lock:
                        stats["published"] += 1
                    await asyncio.sleep(random.uniform(0.0005, 0.005))
                except Exception:
                    async with lock:
                        stats["errors"] += 1

        # 启动20个发布者
        publishers = [asyncio.create_task(publisher(i)) for i in range(20)]

        # 等待120秒
        await asyncio.sleep(duration + 2)

        for p in publishers:
            p.cancel()
            try:
                await p
            except asyncio.CancelledError:
                pass

        await bus.stop()

        latencies = list(stats["latencies"])
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        throughput = stats["processed"] / duration

        print(f"\n120秒极限压力测试结果:")
        print(f"  发布事件: {stats['published']}")
        print(f"  处理事件: {stats['processed']}")
        print(f"  错误数: {stats['errors']}")
        print(f"  处理率: {stats['processed']/max(stats['published'], 1)*100:.1f}%")
        print(f"  吞吐量: {throughput:.1f} events/s")
        print(f"  平均延迟: {avg_latency*1000:.2f}ms")

        assert stats["processed"] > stats["published"] * 0.7

    @pytest.mark.asyncio
    async def test_memory_leak_detection(self):
        """内存泄漏深度检测"""
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

        bus = InMemoryEventBus()
        await bus.start()

        memory_samples = []
        timestamps = []

        start = time.time()
        for i in range(60):  # 60秒采样
            for _ in range(200):
                event = DomainEvent(
                    event_type="leak_test",
                    aggregate_id=f"agg_{i}",
                    payload={"data": "x" * 500}
                )
                await bus.publish(event)

            mem = process.memory_info().rss / 1024 / 1024
            memory_samples.append(mem)
            timestamps.append(i)

            if i % 10 == 0:
                gc.collect()

            await asyncio.sleep(1)

        await bus.stop()
        gc.collect()

        # 线性回归分析内存趋势
        try:
            slope = linear_regression(timestamps, memory_samples)[0]
        except:
            # 如果无法计算回归，使用简单差分
            slope = (memory_samples[-1] - memory_samples[0]) / len(memory_samples)

        print(f"\n内存泄漏检测:")
        print(f"  初始内存: {memory_samples[0]:.1f}MB")
        print(f"  最终内存: {memory_samples[-1]:.1f}MB")
        print(f"  趋势斜率: {slope:.4f} MB/s")

        # 斜率应该接近0（<0.5MB/s）
        assert abs(slope) < 0.5, f"检测到内存泄漏趋势: {slope:.4f} MB/s"


class TestGCPressure:
    """GC压力测试"""

    @pytest.mark.asyncio
    async def test_frequent_gc_under_load(self):
        """负载下频繁GC测试"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
        except ImportError:
            pytest.skip("snapshot_serializer not available")
            return

        serializer = JSONSerializer()
        stats = {"success": 0, "gc_count": 0}

        for i in range(500):
            try:
                data = {f"key_{j}": "x" * 100 for j in range(100)}
                serialized = serializer.serialize(data)
                deserialized = serializer.deserialize(serialized)

                # 每10次操作触发GC
                if i % 10 == 0:
                    gc.collect()
                    stats["gc_count"] += 1

                stats["success"] += 1
            except Exception:
                pass

        print(f"\n频繁GC测试:")
        print(f"  成功操作: {stats['success']}")
        print(f"  GC次数: {stats['gc_count']}")

        assert stats["success"] == 500


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

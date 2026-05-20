"""第18轮深度实践 - 60秒极端压力测试

测试策略:
1. 60秒持续高负载运行 - 更长时间的压力
2. 内存使用持续监控 - 检测内存泄漏趋势
3. 事件处理率统计 - 验证系统吞吐量
4. 多生产者多消费者模式 - 模拟真实场景
5. 资源使用趋势分析 - 检测资源耗尽
"""

import asyncio
import gc
import os
import random
import sys
import time
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestExtremeLongRunning:
    """极端长时间运行测试"""

    @pytest.mark.asyncio
    async def test_60s_continuous_event_processing(self):
        """60秒持续事件处理测试"""
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
            "latencies": deque(maxlen=1000)  # 记录最近1000个延迟
        }
        lock = asyncio.Lock()

        async def handler(event):
            async with lock:
                stats["processed"] += 1
                # 计算延迟
                if hasattr(event, 'payload') and 'timestamp' in event.payload:
                    latency = time.time() - event.payload['timestamp']
                    stats["latencies"].append(latency)

        await bus.subscribe("extreme_test", handler)

        start_time = time.time()
        duration = 60  # 60秒

        async def publisher(pub_id):
            while time.time() - start_time < duration:
                try:
                    event = DomainEvent(
                        event_type="extreme_test",
                        aggregate_id=f"agg_{pub_id}_{random.randint(1, 1000)}",
                        payload={
                            "timestamp": time.time(),
                            "publisher": pub_id,
                            "data": "x" * random.randint(10, 500)
                        }
                    )
                    await bus.publish(event)
                    async with lock:
                        stats["published"] += 1
                    await asyncio.sleep(random.uniform(0.001, 0.01))
                except Exception:
                    async with lock:
                        stats["errors"] += 1

        # 启动10个发布者
        publishers = [asyncio.create_task(publisher(i)) for i in range(10)]

        # 等待60秒
        await asyncio.sleep(duration + 2)

        # 停止发布者
        for p in publishers:
            p.cancel()
            try:
                await p
            except asyncio.CancelledError:
                pass

        await bus.stop()

        # 计算统计信息
        latencies = list(stats["latencies"])
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        max_latency = max(latencies) if latencies else 0
        min_latency = min(latencies) if latencies else 0
        throughput = stats["processed"] / duration

        print(f"\n60秒极端压力测试结果:")
        print(f"  发布事件: {stats['published']}")
        print(f"  处理事件: {stats['processed']}")
        print(f"  错误数: {stats['errors']}")
        print(f"  处理率: {stats['processed']/max(stats['published'], 1)*100:.1f}%")
        print(f"  吞吐量: {throughput:.1f} events/s")
        print(f"  平均延迟: {avg_latency*1000:.2f}ms")
        print(f"  最大延迟: {max_latency*1000:.2f}ms")
        print(f"  最小延迟: {min_latency*1000:.2f}ms")

        # 验证处理率应该>70%
        assert stats["processed"] > stats["published"] * 0.7, \
            f"事件处理率过低: {stats['processed']}/{stats['published']}"

    @pytest.mark.asyncio
    async def test_memory_trend_over_time(self):
        """内存使用趋势分析"""
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

        # 记录内存使用趋势
        memory_samples = []

        bus = InMemoryEventBus()
        await bus.start()

        # 运行30秒，每秒采样内存
        start = time.time()
        for i in range(30):
            # 发布一些事件
            for _ in range(100):
                event = DomainEvent(
                    event_type="memory_trend_test",
                    aggregate_id=f"agg_{i}",
                    payload={"data": "x" * 100}
                )
                await bus.publish(event)

            # 采样内存
            mem = process.memory_info().rss / 1024 / 1024
            memory_samples.append(mem)
            await asyncio.sleep(1)

        await bus.stop()
        gc.collect()

        # 分析内存趋势
        final_mem = process.memory_info().rss / 1024 / 1024
        initial_mem = memory_samples[0]
        max_mem = max(memory_samples)
        min_mem = min(memory_samples)

        # 计算线性趋势
        n = len(memory_samples)
        x_mean = (n - 1) / 2
        y_mean = sum(memory_samples) / n
        numerator = sum((i - x_mean) * (memory_samples[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0

        print(f"\n内存趋势分析:")
        print(f"  初始内存: {initial_mem:.1f}MB")
        print(f"  最大内存: {max_mem:.1f}MB")
        print(f"  最小内存: {min_mem:.1f}MB")
        print(f"  最终内存: {final_mem:.1f}MB")
        print(f"  内存增长: {final_mem - initial_mem:.1f}MB")
        print(f"  趋势斜率: {slope:.3f} MB/s")

        # 内存增长不应过快（斜率<1MB/s）
        assert slope < 1.0, f"内存增长过快: {slope:.3f} MB/s"

    @pytest.mark.asyncio
    async def test_concurrent_producers_consumers(self):
        """多生产者多消费者测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        stats = {"produced": 0, "consumed": 0}
        lock = asyncio.Lock()
        queue = asyncio.Queue(maxsize=10000)

        # 消费者
        async def consumer(consumer_id):
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    async with lock:
                        stats["consumed"] += 1
                except asyncio.TimeoutError:
                    break

        # 生产者
        async def producer(producer_id):
            for i in range(500):
                try:
                    event = DomainEvent(
                        event_type="producer_consumer_test",
                        aggregate_id=f"agg_{producer_id}_{i}",
                        payload={"producer": producer_id, "index": i}
                    )
                    await bus.publish(event)
                    await queue.put(event)
                    async with lock:
                        stats["produced"] += 1
                except Exception:
                    pass

        # 启动10个生产者和5个消费者
        consumers = [asyncio.create_task(consumer(i)) for i in range(5)]
        producers = [asyncio.create_task(producer(i)) for i in range(10)]

        await asyncio.gather(*producers)
        await asyncio.gather(*consumers, return_exceptions=True)

        await bus.stop()

        print(f"\n多生产者多消费者测试:")
        print(f"  生产: {stats['produced']}")
        print(f"  消费: {stats['consumed']}")

        assert stats["produced"] == 10 * 500
        assert stats["consumed"] > 0


class TestResourceExhaustionRecovery:
    """资源耗尽恢复测试"""

    @pytest.mark.asyncio
    async def test_memory_pressure_with_recovery(self):
        """内存压力测试与恢复"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
        except ImportError:
            pytest.skip("snapshot_serializer not available")
            return

        serializer = JSONSerializer(compression=True)

        # 阶段1: 逐渐增加内存压力
        sizes = [100, 500, 1000, 2000, 5000]
        results = []

        for size in sizes:
            data = {f"key_{i}": "x" * 100 for i in range(size)}
            start = time.time()
            serialized = serializer.serialize(data)
            serialize_time = time.time() - start

            start = time.time()
            deserialized = serializer.deserialize(serialized)
            deserialize_time = time.time() - start

            results.append({
                "size": size,
                "serialized_size": len(serialized),
                "serialize_time": serialize_time,
                "deserialize_time": deserialize_time
            })

            # 强制GC
            gc.collect()

        print(f"\n内存压力测试结果:")
        for r in results:
            print(f"  大小{r['size']}: 序列化{r['serialize_time']*1000:.1f}ms, "
                  f"反序列化{r['deserialize_time']*1000:.1f}ms, "
                  f"压缩后{r['serialized_size']/1024:.1f}KB")

        # 验证所有操作成功
        assert len(results) == len(sizes)

    @pytest.mark.asyncio
    async def test_connection_pool_exhaustion(self):
        """连接池耗尽模拟"""
        # 使用信号量模拟连接池
        pool_size = 3
        semaphore = asyncio.Semaphore(pool_size)

        stats = {"acquired": 0, "total_wait": 0}

        async def use_connection(conn_id, hold_time):
            wait_start = time.time()
            async with semaphore:
                wait_time = time.time() - wait_start
                stats["total_wait"] += wait_time
                stats["acquired"] += 1
                await asyncio.sleep(hold_time)

        # 启动超过连接池大小的任务
        tasks = [
            asyncio.create_task(use_connection(i, random.uniform(0.05, 0.1)))
            for i in range(20)
        ]

        await asyncio.gather(*tasks)

        print(f"\n连接池耗尽测试:")
        print(f"  池大小: {pool_size}")
        print(f"  任务数: 20")
        print(f"  成功获取: {stats['acquired']}")
        if stats['acquired'] > 0:
            print(f"  平均等待: {stats['total_wait']/stats['acquired']*1000:.1f}ms")

        # 所有任务应该都完成
        assert stats["acquired"] == 20


class TestThroughputBenchmark:
    """吞吐量基准测试"""

    @pytest.mark.asyncio
    async def test_event_throughput_benchmark(self):
        """事件吞吐量基准"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        processed = 0
        lock = asyncio.Lock()

        async def handler(event):
            nonlocal processed
            async with lock:
                processed += 1

        await bus.subscribe("throughput_test", handler)

        # 测试不同批量大小
        batch_sizes = [10, 100, 1000]
        results = []

        for batch_size in batch_sizes:
            processed = 0
            start = time.time()

            for _ in range(batch_size):
                event = DomainEvent(
                    event_type="throughput_test",
                    aggregate_id="benchmark",
                    payload={"data": "x" * 100}
                )
                await bus.publish(event)

            # 等待处理完成
            await asyncio.sleep(1)

            elapsed = time.time() - start
            throughput = processed / elapsed

            results.append({
                "batch_size": batch_size,
                "processed": processed,
                "elapsed": elapsed,
                "throughput": throughput
            })

        await bus.stop()

        print(f"\n事件吞吐量基准:")
        for r in results:
            print(f"  批量{r['batch_size']}: {r['throughput']:.0f} events/s")

    @pytest.mark.asyncio
    async def test_serialization_throughput(self):
        """序列化吞吐量基准"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer, PickleSerializer
        except ImportError:
            pytest.skip("snapshot_serializer not available")
            return

        data = {f"key_{i}": f"value_{i}" * 10 for i in range(100)}

        results = []

        for name, serializer in [("JSON", JSONSerializer()), ("Pickle", PickleSerializer())]:
            # 序列化吞吐量
            start = time.time()
            for _ in range(1000):
                serialized = serializer.serialize(data)
            serialize_time = time.time() - start

            # 反序列化吞吐量
            start = time.time()
            for _ in range(1000):
                deserialized = serializer.deserialize(serialized)
            deserialize_time = time.time() - start

            results.append({
                "name": name,
                "serialize_ops": 1000 / serialize_time,
                "deserialize_ops": 1000 / deserialize_time
            })

        print(f"\n序列化吞吐量基准:")
        for r in results:
            print(f"  {r['name']}: 序列化{r['serialize_ops']:.0f} ops/s, "
                  f"反序列化{r['deserialize_ops']:.0f} ops/s")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

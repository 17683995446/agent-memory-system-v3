"""第17轮深度实践 - 资源竞争极端场景测试

测试策略:
1. 内存压力测试 - 接近OOM边缘
2. 文件描述符压力 - 大量文件操作
3. CPU过载测试 - 密集计算
4. IO竞争 - 同时读写
5. 连接池耗尽 - 模拟连接泄漏
6. 任务队列溢出 - 检测背压处理
"""

import asyncio
import gc
import os
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestMemoryPressure:
    """内存压力测试"""

    @pytest.mark.asyncio
    async def test_large_data_handling(self):
        """大数据处理测试"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
        except ImportError:
            pytest.skip("snapshot_serializer not available")
            return

        serializer = JSONSerializer(compression=True)

        # 创建大数据（约10MB）
        large_data = {
            f"key_{i}": {
                "data": "x" * 1000,
                "nested": {"value": i, "text": f"text_{i}" * 10}
            }
            for i in range(5000)
        }

        # 序列化
        start = time.time()
        serialized = serializer.serialize(large_data)
        serialize_time = time.time() - start

        # 反序列化
        start = time.time()
        deserialized = serializer.deserialize(serialized)
        deserialize_time = time.time() - start

        print(f"\n大数据处理测试:")
        print(f"  原始大小: ~{len(str(large_data))/1024/1024:.1f}MB")
        print(f"  序列化后: {len(serialized)/1024:.1f}KB")
        print(f"  序列化时间: {serialize_time:.2f}s")
        print(f"  反序列化时间: {deserialize_time:.2f}s")

        assert len(deserialized) == 5000

    @pytest.mark.asyncio
    async def test_memory_growth_monitoring(self):
        """内存增长监控"""
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

        initial_mem = process.memory_info().rss / 1024 / 1024

        bus = InMemoryEventBus()
        await bus.start()

        # 执行大量操作
        for i in range(1000):
            event = DomainEvent(
                event_type="memory_test",
                aggregate_id=f"agg_{i % 100}",
                payload={"data": "x" * 100}
            )
            await bus.publish(event)

        await bus.stop()
        gc.collect()

        final_mem = process.memory_info().rss / 1024 / 1024
        growth = final_mem - initial_mem

        print(f"\n内存增长监控:")
        print(f"  初始: {initial_mem:.1f}MB")
        print(f"  最终: {final_mem:.1f}MB")
        print(f"  增长: {growth:.1f}MB")

        # 增长应该合理
        assert growth < 200  # 不超过200MB增长


class TestCPUOverload:
    """CPU过载测试"""

    def test_intensive_computation(self):
        """密集计算测试"""
        try:
            from memory_system.core_abstractions.mathematical_utility_functions import cosine_similarity
        except ImportError:
            # 使用简单的余弦相似度计算
            def cosine_similarity(a, b):
                dot = sum(x * y for x, y in zip(a, b))
                norm_a = sum(x * x for x in a) ** 0.5
                norm_b = sum(x * x for x in b) ** 0.5
                return dot / (norm_a * norm_b) if norm_a and norm_b else 0

        # 生成大量向量
        vectors = [[random.random() for _ in range(100)] for _ in range(1000)]

        start = time.time()
        similarities = []
        for i in range(100):
            for j in range(i + 1, min(i + 50, len(vectors))):
                sim = cosine_similarity(vectors[i], vectors[j])
                similarities.append(sim)

        elapsed = time.time() - start

        print(f"\n密集计算测试:")
        print(f"  向量数: {len(vectors)}")
        print(f"  计算数: {len(similarities)}")
        print(f"  耗时: {elapsed:.2f}s")

        assert len(similarities) > 0

    @pytest.mark.asyncio
    async def test_concurrent_computation(self):
        """并发计算测试"""
        async def compute_chunk(chunk_id, size):
            # 模拟计算密集任务
            result = 0
            for i in range(size):
                result += (i ** 2) % 1000
            return chunk_id, result

        # 启动多个计算任务
        tasks = [asyncio.create_task(compute_chunk(i, 10000)) for i in range(20)]
        results = await asyncio.gather(*tasks)

        print(f"\n并发计算测试:")
        print(f"  任务数: {len(results)}")
        print(f"  总结果: {sum(r for _, r in results)}")

        assert len(results) == 20


class TestIOContention:
    """IO竞争测试"""

    @pytest.mark.asyncio
    async def test_concurrent_serialization(self):
        """并发序列化测试"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
        except ImportError:
            pytest.skip("snapshot_serializer not available")
            return

        serializer = JSONSerializer()
        errors = []
        results = []

        async def serialize_task(task_id):
            try:
                data = {f"key_{i}": f"value_{i}_{task_id}" for i in range(100)}
                serialized = serializer.serialize(data)
                deserialized = serializer.deserialize(serialized)
                results.append((task_id, len(deserialized)))
            except Exception as e:
                errors.append(str(e))

        # 50个并发序列化任务
        tasks = [asyncio.create_task(serialize_task(i)) for i in range(50)]
        await asyncio.gather(*tasks)

        print(f"\n并发序列化测试:")
        print(f"  成功: {len(results)}")
        print(f"  错误: {len(errors)}")

        assert len(errors) == 0
        assert len(results) == 50

    @pytest.mark.asyncio
    async def test_mixed_read_write_operations(self):
        """混合读写操作测试"""
        try:
            from memory_system.storage_backends.in_memory_storage_backend import InMemoryStorageBackend
            from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        except ImportError:
            pytest.skip("storage backends not available")
            return

        storage = InMemoryStorageBackend()
        lock = asyncio.Lock()
        written = []
        errors = []

        async def writer(w_id):
            for i in range(20):
                try:
                    item = MemoryItem(
                        memory_id=f"mem_{w_id}_{i}",
                        content=f"content_{i}",
                        memory_type=MemoryType.SHORT_TERM,
                        agent_id=f"agent_{w_id}",
                    )
                    await storage.add(item)
                    async with lock:
                        written.append(item.memory_id)
                except Exception as e:
                    errors.append(str(e))

        async def reader(r_id):
            for _ in range(20):
                try:
                    async with lock:
                        if written:
                            mem_id = random.choice(written)
                        else:
                            continue
                    await storage.get(mem_id)
                except Exception as e:
                    errors.append(str(e))

        async def deleter(d_id):
            for _ in range(10):
                try:
                    async with lock:
                        if written:
                            mem_id = random.choice(written)
                            written.remove(mem_id)
                        else:
                            continue
                    await storage.remove(mem_id)
                except Exception:
                    pass  # 删除可能失败

        # 混合执行
        tasks = []
        tasks.extend([asyncio.create_task(writer(i)) for i in range(10)])
        tasks.extend([asyncio.create_task(reader(i)) for i in range(10)])
        tasks.extend([asyncio.create_task(deleter(i)) for i in range(5)])

        await asyncio.gather(*tasks, return_exceptions=True)

        print(f"\n混合读写测试:")
        print(f"  写入数: {len(written)}")
        print(f"  错误数: {len(errors)}")


class TestTaskQueueOverflow:
    """任务队列溢出测试"""

    @pytest.mark.asyncio
    async def test_backpressure_handling(self):
        """背压处理测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        stats = {"published": 0, "processed": 0}

        async def slow_handler(event):
            await asyncio.sleep(0.05)  # 慢处理
            stats["processed"] += 1

        await bus.subscribe("backpressure_test", slow_handler)

        # 快速发布大量事件
        for i in range(200):
            event = DomainEvent(
                event_type="backpressure_test",
                aggregate_id=f"agg_{i}",
                payload={"index": i}
            )
            await bus.publish(event)
            stats["published"] += 1

        # 等待处理
        await asyncio.sleep(15)
        await bus.stop()

        print(f"\n背压处理测试:")
        print(f"  发布: {stats['published']}")
        print(f"  处理: {stats['processed']}")
        print(f"  处理率: {stats['processed']/stats['published']*100:.1f}%")

        # 应该处理一部分
        assert stats["processed"] > 0


class TestConnectionPoolExhaustion:
    """连接池耗尽测试"""

    @pytest.mark.asyncio
    async def test_semaphore_contention(self):
        """信号量竞争测试"""
        # 模拟连接池（使用信号量）
        pool_size = 5
        semaphore = asyncio.Semaphore(pool_size)

        stats = {"acquired": 0, "released": 0, "waited": 0}

        async def use_connection(conn_id):
            wait_start = time.time()
            async with semaphore:
                wait_time = time.time() - wait_start
                if wait_time > 0.01:
                    stats["waited"] += 1
                stats["acquired"] += 1
                # 模拟使用连接
                await asyncio.sleep(random.uniform(0.01, 0.1))
                stats["released"] += 1

        # 启动超过连接池大小的任务
        tasks = [asyncio.create_task(use_connection(i)) for i in range(50)]
        await asyncio.gather(*tasks)

        print(f"\n连接池测试:")
        print(f"  池大小: {pool_size}")
        print(f"  任务数: 50")
        print(f"  获取次数: {stats['acquired']}")
        print(f"  等待次数: {stats['waited']}")

        assert stats["acquired"] == 50
        assert stats["released"] == 50


class TestThreadContention:
    """线程竞争测试"""

    def test_shared_resource_contention(self):
        """共享资源竞争测试"""
        from threading import RLock

        shared_value = 0
        lock = RLock()
        errors = []

        def increment(thread_id, count):
            nonlocal shared_value
            for _ in range(count):
                try:
                    with lock:
                        temp = shared_value
                        # 模拟一些处理
                        time.sleep(0.0001)
                        shared_value = temp + 1
                except Exception as e:
                    errors.append(str(e))

        # 多线程竞争
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(increment, i, 100) for i in range(10)]
            for f in futures:
                f.result()

        print(f"\n共享资源竞争测试:")
        print(f"  预期值: 1000")
        print(f"  实际值: {shared_value}")
        print(f"  错误数: {len(errors)}")

        # 使用RLock应该保证正确性
        assert len(errors) == 0
        assert shared_value == 1000


class TestGarbageCollectionInteraction:
    """垃圾回收交互测试"""

    @pytest.mark.asyncio
    async def test_gc_during_operations(self):
        """操作期间的GC测试"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
        except ImportError:
            pytest.skip("snapshot_serializer not available")
            return

        serializer = JSONSerializer()
        errors = []

        async def operation(op_id):
            try:
                # 创建临时大对象
                data = {f"key_{i}": "x" * 100 for i in range(100)}
                serialized = serializer.serialize(data)
                deserialized = serializer.deserialize(serialized)

                # 随机触发GC
                if random.random() < 0.1:
                    gc.collect()

                return True
            except Exception as e:
                errors.append(str(e))
                return False

        # 并发执行，期间GC可能随时发生
        tasks = [asyncio.create_task(operation(i)) for i in range(100)]
        results = await asyncio.gather(*tasks)

        print(f"\nGC交互测试:")
        print(f"  成功: {sum(results)}")
        print(f"  错误: {len(errors)}")

        assert len(errors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

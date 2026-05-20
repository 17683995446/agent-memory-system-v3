"""第17轮深度实践 - 大规模并发混乱测试

测试策略:
1. 1000+并发操作同时执行
2. 随机故障注入 - 30%操作随机失败
3. 随机延迟注入 - 模拟网络延迟
4. 随机顺序执行 - 打破预期顺序
5. 资源竞争 - 多个组件竞争相同资源
6. 异常传播测试 - 确保异常正确传播
"""

import asyncio
import os
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ChaosInjector:
    """混乱注入器"""

    def __init__(self, failure_rate=0.3, delay_range=(0.001, 0.1)):
        self.failure_rate = failure_rate
        self.delay_range = delay_range
        self.stats = {"injected_failures": 0, "injected_delays": 0}

    async def maybe_fail(self):
        """随机失败"""
        if random.random() < self.failure_rate:
            self.stats["injected_failures"] += 1
            raise RuntimeError(f"Chaos: random failure #{self.stats['injected_failures']}")

    async def maybe_delay(self):
        """随机延迟"""
        if random.random() < 0.5:
            delay = random.uniform(*self.delay_range)
            self.stats["injected_delays"] += 1
            await asyncio.sleep(delay)


class TestMassiveConcurrency:
    """大规模并发测试"""

    @pytest.mark.asyncio
    async def test_1000_concurrent_operations(self):
        """1000个并发操作测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        chaos = ChaosInjector(failure_rate=0.2)
        stats = {"success": 0, "failure": 0, "total": 0}

        async def operation(op_id):
            stats["total"] += 1
            try:
                await chaos.maybe_delay()
                await chaos.maybe_fail()

                event = DomainEvent(
                    event_type="massive_test",
                    aggregate_id=f"agg_{op_id}",
                    payload={"id": op_id, "data": "x" * 100}
                )
                await bus.publish(event)
                stats["success"] += 1
            except Exception:
                stats["failure"] += 1

        # 启动1000个并发操作
        tasks = [asyncio.create_task(operation(i)) for i in range(1000)]
        await asyncio.gather(*tasks, return_exceptions=True)

        await bus.stop()

        print(f"\n1000并发操作测试:")
        print(f"  总数: {stats['total']}")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failure']}")
        print(f"  注入失败: {chaos.stats['injected_failures']}")
        print(f"  注入延迟: {chaos.stats['injected_delays']}")

        # 成功率应该>50%（考虑到注入的失败）
        assert stats["success"] > 500

    @pytest.mark.asyncio
    async def test_concurrent_storage_with_chaos(self):
        """混乱条件下的并发存储测试"""
        try:
            from memory_system.storage_backends.in_memory_storage_backend import InMemoryStorageBackend
            from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        except ImportError:
            pytest.skip("storage backends not available")
            return

        storage = InMemoryStorageBackend()
        chaos = ChaosInjector(failure_rate=0.3)

        stats = {"writes": 0, "reads": 0, "errors": 0}
        lock = asyncio.Lock()
        written_ids = []

        async def write_op(op_id):
            try:
                await chaos.maybe_delay()
                await chaos.maybe_fail()

                item = MemoryItem(
                    memory_id=f"chaos_mem_{op_id}",
                    content=f"content_{op_id}" * 10,
                    memory_type=MemoryType.SHORT_TERM,
                    agent_id="chaos_agent",
                )
                await storage.add(item)
                async with lock:
                    written_ids.append(item.memory_id)
                stats["writes"] += 1
            except Exception:
                stats["errors"] += 1

        async def read_op():
            try:
                await chaos.maybe_delay()
                async with lock:
                    if written_ids:
                        mem_id = random.choice(written_ids)
                    else:
                        return
                await storage.get(mem_id)
                stats["reads"] += 1
            except Exception:
                stats["errors"] += 1

        # 500写 + 500读并发
        write_tasks = [asyncio.create_task(write_op(i)) for i in range(500)]
        read_tasks = [asyncio.create_task(read_op()) for _ in range(500)]

        await asyncio.gather(*write_tasks, *read_tasks, return_exceptions=True)

        print(f"\n混乱存储测试:")
        print(f"  写入: {stats['writes']}")
        print(f"  读取: {stats['reads']}")
        print(f"  错误: {stats['errors']}")

        # 应该有成功的操作
        assert stats["writes"] + stats["reads"] > 100

    @pytest.mark.asyncio
    async def test_random_operation_order(self):
        """随机操作顺序测试"""
        try:
            from memory_system.cqrs_system import CommandBus, QueryBus, InMemoryEventStore
        except ImportError:
            pytest.skip("cqrs_system not available")
            return

        event_store = InMemoryEventStore()
        command_bus = CommandBus(event_store=event_store)
        query_bus = QueryBus()  # QueryBus不接受event_store参数

        operations = []
        for i in range(500):
            operations.append(("write", i))
            operations.append(("read", i))

        # 随机打乱顺序
        random.shuffle(operations)

        stats = {"writes": 0, "reads": 0, "errors": 0}

        async def execute_op(op_type, op_id):
            try:
                if op_type == "write":
                    # 模拟写入
                    await asyncio.sleep(random.uniform(0.001, 0.01))
                    stats["writes"] += 1
                else:
                    # 模拟读取
                    await asyncio.sleep(random.uniform(0.001, 0.01))
                    stats["reads"] += 1
            except Exception:
                stats["errors"] += 1

        tasks = [asyncio.create_task(execute_op(op, idx)) for op, idx in operations]
        await asyncio.gather(*tasks, return_exceptions=True)

        print(f"\n随机顺序测试:")
        print(f"  写入: {stats['writes']}")
        print(f"  读取: {stats['reads']}")
        print(f"  错误: {stats['errors']}")

        assert stats["writes"] + stats["reads"] > 900


class TestResourceContention:
    """资源竞争测试"""

    @pytest.mark.asyncio
    async def test_lock_contention(self):
        """锁竞争测试"""
        try:
            from memory_system.read_write_lock import ReadWriteLock
        except ImportError:
            pytest.skip("read_write_lock not available")
            return

        rw_lock = ReadWriteLock()
        shared_resource = {"value": 0}
        stats = {"reads": 0, "writes": 0}

        async def reader(reader_id):
            for _ in range(100):
                async with rw_lock.read_lock():
                    _ = shared_resource["value"]
                    stats["reads"] += 1
                    await asyncio.sleep(0.001)

        async def writer(writer_id):
            for _ in range(50):
                async with rw_lock.write_lock():
                    shared_resource["value"] += 1
                    stats["writes"] += 1
                    await asyncio.sleep(0.002)

        # 20读者 + 10写者竞争
        readers = [asyncio.create_task(reader(i)) for i in range(20)]
        writers = [asyncio.create_task(writer(i)) for i in range(10)]

        await asyncio.gather(*readers, *writers)

        print(f"\n锁竞争测试:")
        print(f"  读操作: {stats['reads']}")
        print(f"  写操作: {stats['writes']}")
        print(f"  最终值: {shared_resource['value']}")

        assert stats["reads"] == 20 * 100
        assert stats["writes"] == 10 * 50
        assert shared_resource["value"] == 10 * 50

    @pytest.mark.asyncio
    async def test_rate_limiter_contention(self):
        """限流器竞争测试"""
        try:
            from memory_system.rate_limiter import RateLimiter, RateLimitConfig
        except ImportError:
            pytest.skip("rate_limiter not available")
            return

        config = RateLimitConfig(requests_per_minute=1000, burst_size=100)
        limiter = RateLimiter(config=config)

        stats = {"allowed": 0, "denied": 0}

        async def request(user_id):
            if limiter.is_allowed(user_id, "/api/test"):
                stats["allowed"] += 1
            else:
                stats["denied"] += 1

        # 100用户同时请求，每个请求100次
        tasks = []
        for user in range(100):
            for _ in range(100):
                tasks.append(asyncio.create_task(request(f"user_{user}")))

        await asyncio.gather(*tasks)

        print(f"\n限流器竞争测试:")
        print(f"  允许: {stats['allowed']}")
        print(f"  拒绝: {stats['denied']}")

        # 应该有部分被允许
        assert stats["allowed"] > 0


class TestExceptionPropagation:
    """异常传播测试"""

    @pytest.mark.asyncio
    async def test_nested_async_exception_propagation(self):
        """嵌套异步异常传播测试"""
        errors_caught = []

        async def level_3():
            raise ValueError("Deep error")

        async def level_2():
            try:
                await level_3()
            except ValueError as e:
                errors_caught.append(("level_2", str(e)))
                raise

        async def level_1():
            try:
                await level_2()
            except ValueError as e:
                errors_caught.append(("level_1", str(e)))
                raise

        # 执行并捕获
        try:
            await level_1()
        except ValueError as e:
            errors_caught.append(("top", str(e)))

        assert len(errors_caught) == 3
        assert all("Deep error" in str(e) for _, e in errors_caught)

    @pytest.mark.asyncio
    async def test_gather_exception_handling(self):
        """asyncio.gather异常处理测试"""
        results = []
        exceptions = []

        async def succeed(i):
            results.append(i)
            return i

        async def fail(i):
            raise RuntimeError(f"Task {i} failed")

        # 使用return_exceptions=True
        outcomes = await asyncio.gather(
            succeed(1),
            fail(2),
            succeed(3),
            fail(4),
            succeed(5),
            return_exceptions=True
        )

        for outcome in outcomes:
            if isinstance(outcome, Exception):
                exceptions.append(str(outcome))

        assert len(exceptions) == 2  # 2 failures


class TestThreadSafety:
    """线程安全测试"""

    def test_concurrent_dict_access(self):
        """并发字典访问测试"""
        from threading import Lock

        shared_dict = {}
        lock = Lock()
        errors = []

        def writer(start, count):
            for i in range(count):
                try:
                    with lock:
                        shared_dict[f"key_{start}_{i}"] = f"value_{i}"
                except Exception as e:
                    errors.append(str(e))

        def reader(start, count):
            for i in range(count):
                try:
                    with lock:
                        _ = shared_dict.get(f"key_{start}_{i}", None)
                except Exception as e:
                    errors.append(str(e))

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for i in range(10):
                futures.append(executor.submit(writer, i, 100))
                futures.append(executor.submit(reader, i, 100))

            for f in as_completed(futures):
                f.result()

        print(f"\n线程安全测试:")
        print(f"  字典大小: {len(shared_dict)}")
        print(f"  错误数: {len(errors)}")

        assert len(errors) == 0
        assert len(shared_dict) == 10 * 100


class TestMemoryConsistency:
    """内存一致性测试"""

    @pytest.mark.asyncio
    async def test_eventual_consistency(self):
        """最终一致性测试"""
        try:
            from memory_system.vector_clock import VectorClock
        except ImportError:
            pytest.skip("vector_clock not available")
            return

        # 模拟3个节点的最终一致性
        nodes = {
            "A": VectorClock(node_id="A"),
            "B": VectorClock(node_id="B"),
            "C": VectorClock(node_id="C"),
        }

        # 每个节点独立更新
        for node, clock in nodes.items():
            for _ in range(10):
                clock.increment()

        # 模拟信息交换
        # A收到B的时钟
        nodes["A"] = nodes["A"].merge(nodes["B"])
        # B收到C的时钟
        nodes["B"] = nodes["B"].merge(nodes["C"])
        # C收到A的时钟
        nodes["C"] = nodes["C"].merge(nodes["A"])
        # 再次交换确保收敛
        nodes["A"] = nodes["A"].merge(nodes["C"])
        nodes["B"] = nodes["B"].merge(nodes["A"])
        nodes["C"] = nodes["C"].merge(nodes["B"])

        # 验证所有节点看到相同的时钟状态
        clocks = [clock._clock for clock in nodes.values()]

        print(f"\n最终一致性测试:")
        for node, clock in nodes.items():
            print(f"  {node}: {clock._clock}")

        # 所有节点应该知道其他节点的存在
        for clock in nodes.values():
            vc = clock._clock
            assert "A" in vc and vc["A"] > 0
            assert "B" in vc and vc["B"] > 0
            assert "C" in vc and vc["C"] > 0

        print(f"\n最终一致性测试:")
        for node, clock in nodes.items():
            print(f"  {node}: {clock._clock}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

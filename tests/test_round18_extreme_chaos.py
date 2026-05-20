"""第18轮深度实践 - 5000+并发混乱测试

测试策略:
1. 5000+并发操作 - 极端并发压力
2. 50%故障注入率 - 高故障环境
3. 随机延迟注入 - 模拟网络延迟
4. 资源耗尽模拟 - 内存/连接池耗尽
5. 随机操作顺序 - 打破所有假设
6. 多级异常传播 - 验证异常处理链
"""

import asyncio
import gc
import os
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ExtremeChaosInjector:
    """极端混乱注入器"""

    def __init__(self, failure_rate=0.5, delay_range=(0.001, 0.5)):
        self.failure_rate = failure_rate
        self.delay_range = delay_range
        self.stats = {
            "injected_failures": 0,
            "injected_delays": 0,
            "total_delay_time": 0
        }

    async def maybe_fail(self, context=""):
        """随机失败"""
        if random.random() < self.failure_rate:
            self.stats["injected_failures"] += 1
            error_types = [
                RuntimeError(f"Chaos failure #{self.stats['injected_failures']} in {context}"),
                ValueError(f"Invalid value from chaos in {context}"),
                TypeError(f"Type error from chaos in {context}"),
                ConnectionError(f"Connection lost in {context}"),
                TimeoutError(f"Timeout in {context}"),
            ]
            raise random.choice(error_types)

    async def maybe_delay(self):
        """随机延迟"""
        if random.random() < 0.7:  # 70%概率延迟
            delay = random.uniform(*self.delay_range)
            self.stats["injected_delays"] += 1
            self.stats["total_delay_time"] += delay
            await asyncio.sleep(delay)

    def get_stats(self):
        return {
            **self.stats,
            "avg_delay": self.stats["total_delay_time"] / max(self.stats["injected_delays"], 1)
        }


class TestExtremeConcurrency:
    """极端并发测试"""

    @pytest.mark.asyncio
    async def test_5000_concurrent_operations(self):
        """5000个并发操作测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        chaos = ExtremeChaosInjector(failure_rate=0.5)
        stats = {"success": 0, "failure": 0, "total": 0}
        lock = asyncio.Lock()

        async def operation(op_id):
            async with lock:
                stats["total"] += 1

            try:
                await chaos.maybe_delay()
                await chaos.maybe_fail(f"operation_{op_id}")

                event = DomainEvent(
                    event_type="extreme_concurrent_test",
                    aggregate_id=f"agg_{op_id % 100}",
                    payload={"id": op_id, "data": "x" * random.randint(10, 1000)}
                )
                await bus.publish(event)

                async with lock:
                    stats["success"] += 1
            except Exception:
                async with lock:
                    stats["failure"] += 1

        # 启动5000个并发操作
        tasks = [asyncio.create_task(operation(i)) for i in range(5000)]
        await asyncio.gather(*tasks, return_exceptions=True)

        await bus.stop()

        chaos_stats = chaos.get_stats()
        print(f"\n5000并发操作测试:")
        print(f"  总数: {stats['total']}")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failure']}")
        print(f"  注入失败: {chaos_stats['injected_failures']}")
        print(f"  注入延迟: {chaos_stats['injected_delays']}")
        print(f"  平均延迟: {chaos_stats['avg_delay']*1000:.1f}ms")

        # 成功率应该>30%（考虑到50%故障注入）
        assert stats["success"] > 1500

    @pytest.mark.asyncio
    async def test_concurrent_with_resource_exhaustion(self):
        """资源耗尽下的并发测试"""
        try:
            from memory_system.storage_backends.in_memory_storage_backend import InMemoryStorageBackend
            from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        except ImportError:
            pytest.skip("storage backends not available")
            return

        storage = InMemoryStorageBackend()
        chaos = ExtremeChaosInjector(failure_rate=0.3)

        stats = {"writes": 0, "reads": 0, "errors": 0, "resource_exhausted": 0}
        lock = asyncio.Lock()
        written_ids = []

        # 模拟资源限制
        resource_semaphore = asyncio.Semaphore(100)  # 最多100个并发资源

        async def write_op(op_id):
            try:
                async with resource_semaphore:
                    await chaos.maybe_delay()
                    await chaos.maybe_fail(f"write_{op_id}")

                    item = MemoryItem(
                        memory_id=f"extreme_mem_{op_id}",
                        content=f"content_{op_id}" * random.randint(1, 10),
                        memory_type=MemoryType.SHORT_TERM,
                        agent_id="extreme_agent",
                    )
                    await storage.add(item)
                    async with lock:
                        written_ids.append(item.memory_id)
                        stats["writes"] += 1
            except asyncio.TimeoutError:
                async with lock:
                    stats["resource_exhausted"] += 1
            except Exception:
                async with lock:
                    stats["errors"] += 1

        async def read_op():
            try:
                async with resource_semaphore:
                    await chaos.maybe_delay()
                    async with lock:
                        if written_ids:
                            mem_id = random.choice(written_ids)
                        else:
                            return
                    await storage.get(mem_id)
                    async with lock:
                        stats["reads"] += 1
            except Exception:
                async with lock:
                    stats["errors"] += 1

        # 2000写 + 2000读并发
        write_tasks = [asyncio.create_task(write_op(i)) for i in range(2000)]
        read_tasks = [asyncio.create_task(read_op()) for _ in range(2000)]

        await asyncio.gather(*write_tasks, *read_tasks, return_exceptions=True)

        print(f"\n资源耗尽并发测试:")
        print(f"  写入: {stats['writes']}")
        print(f"  读取: {stats['reads']}")
        print(f"  错误: {stats['errors']}")
        print(f"  资源耗尽: {stats['resource_exhausted']}")

        assert stats["writes"] + stats["reads"] > 500


class TestRandomOperationOrder:
    """随机操作顺序测试"""

    @pytest.mark.asyncio
    async def test_completely_random_operations(self):
        """完全随机操作顺序"""
        try:
            from memory_system.vector_clock import VectorClock
        except ImportError:
            pytest.skip("vector_clock not available")
            return

        # 创建多个节点的时钟
        nodes = {f"node_{i}": VectorClock(node_id=f"node_{i}") for i in range(10)}

        operations = []
        # 生成各种操作
        for i in range(1000):
            op_type = random.choice(["increment", "merge", "compare"])
            node = random.choice(list(nodes.keys()))
            operations.append((op_type, node, i))

        # 随机打乱
        random.shuffle(operations)

        stats = {"increment": 0, "merge": 0, "compare": 0, "errors": 0}

        for op_type, node, idx in operations:
            try:
                if op_type == "increment":
                    nodes[node].increment()
                    stats["increment"] += 1
                elif op_type == "merge":
                    other = random.choice([n for n in nodes.keys() if n != node])
                    nodes[node] = nodes[node].merge(nodes[other])
                    stats["merge"] += 1
                else:  # compare
                    other = random.choice([n for n in nodes.keys() if n != node])
                    nodes[node].compare(nodes[other])
                    stats["compare"] += 1
            except Exception:
                stats["errors"] += 1

        print(f"\n随机操作顺序测试:")
        print(f"  递增: {stats['increment']}")
        print(f"  合并: {stats['merge']}")
        print(f"  比较: {stats['compare']}")
        print(f"  错误: {stats['errors']}")

        assert stats["errors"] < 10


class TestMultiLevelExceptionPropagation:
    """多级异常传播测试"""

    @pytest.mark.asyncio
    async def test_deep_exception_chain(self):
        """深层异常链测试"""
        exception_chain = []

        async def level_5():
            exception_chain.append("level_5")
            raise RuntimeError("Deep error at level 5")

        async def level_4():
            try:
                await level_5()
            except RuntimeError as e:
                exception_chain.append("level_4")
                raise ValueError(f"Wrapped at 4: {e}") from e

        async def level_3():
            try:
                await level_4()
            except ValueError as e:
                exception_chain.append("level_3")
                raise TypeError(f"Wrapped at 3: {e}") from e

        async def level_2():
            try:
                await level_3()
            except TypeError as e:
                exception_chain.append("level_2")
                raise ConnectionError(f"Wrapped at 2: {e}") from e

        async def level_1():
            try:
                await level_2()
            except ConnectionError as e:
                exception_chain.append("level_1")
                raise

        # 执行并捕获
        try:
            await level_1()
        except Exception as e:
            exception_chain.append(f"caught: {type(e).__name__}")

        print(f"\n深层异常链测试:")
        print(f"  异常链: {' -> '.join(exception_chain)}")

        assert len(exception_chain) == 6
        assert "level_5" in exception_chain
        assert "caught: ConnectionError" in exception_chain

    @pytest.mark.asyncio
    async def test_concurrent_exception_handling(self):
        """并发异常处理测试"""
        results = {"success": 0, "failed": 0}
        exceptions_seen = set()

        async def task_that_may_fail(task_id):
            try:
                if random.random() < 0.5:
                    raise RuntimeError(f"Task {task_id} failed")
                await asyncio.sleep(random.uniform(0.01, 0.1))
                return task_id
            except Exception as e:
                exceptions_seen.add(type(e).__name__)
                raise

        # 启动100个任务
        outcomes = await asyncio.gather(
            *[task_that_may_fail(i) for i in range(100)],
            return_exceptions=True
        )

        for outcome in outcomes:
            if isinstance(outcome, Exception):
                results["failed"] += 1
            else:
                results["success"] += 1

        print(f"\n并发异常处理测试:")
        print(f"  成功: {results['success']}")
        print(f"  失败: {results['failed']}")
        print(f"  异常类型: {exceptions_seen}")

        assert results["success"] + results["failed"] == 100


class TestChaosWithRecovery:
    """混乱环境下的恢复测试"""

    @pytest.mark.asyncio
    async def test_system_recovery_after_chaos(self):
        """混乱后系统恢复测试"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        chaos = ExtremeChaosInjector(failure_rate=0.7)  # 70%失败率
        stats = {"phase1_success": 0, "phase1_fail": 0, "phase2_success": 0}

        # 阶段1: 混乱环境
        for i in range(100):
            try:
                await chaos.maybe_fail("phase1")
                event = DomainEvent(
                    event_type="chaos_test",
                    aggregate_id=f"agg_{i}",
                    payload={"phase": 1, "data": "x" * 100}
                )
                await bus.publish(event)
                stats["phase1_success"] += 1
            except Exception:
                stats["phase1_fail"] += 1

        # 阶段2: 正常环境（恢复后）
        for i in range(100):
            try:
                event = DomainEvent(
                    event_type="recovery_test",
                    aggregate_id=f"agg_{i}",
                    payload={"phase": 2, "data": "x" * 100}
                )
                await bus.publish(event)
                stats["phase2_success"] += 1
            except Exception:
                pass

        await bus.stop()

        print(f"\n混乱后恢复测试:")
        print(f"  阶段1成功: {stats['phase1_success']}")
        print(f"  阶段1失败: {stats['phase1_fail']}")
        print(f"  阶段2成功: {stats['phase2_success']}")

        # 阶段2应该全部成功
        assert stats["phase2_success"] == 100


class TestThreadChaos:
    """线程混乱测试"""

    def test_thread_pool_with_chaos(self):
        """混乱线程池测试"""
        from threading import Lock

        shared_data = {"value": 0, "errors": []}
        lock = Lock()

        def chaotic_task(task_id):
            try:
                # 随机延迟
                time.sleep(random.uniform(0.001, 0.05))

                # 随机失败
                if random.random() < 0.3:
                    raise RuntimeError(f"Thread {task_id} chaos failure")

                with lock:
                    old = shared_data["value"]
                    time.sleep(0.001)  # 模拟处理
                    shared_data["value"] = old + 1

            except Exception as e:
                with lock:
                    shared_data["errors"].append(str(e))

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(chaotic_task, i) for i in range(500)]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception:
                    pass

        print(f"\n混乱线程池测试:")
        print(f"  成功更新: {shared_data['value']}")
        print(f"  错误数: {len(shared_data['errors'])}")

        # 应该有部分成功
        assert shared_data["value"] > 100


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

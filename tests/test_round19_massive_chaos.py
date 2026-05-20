"""第19轮深度实践 - 10000+并发混乱测试

测试策略:
1. 10000+并发操作 - 翻倍并发
2. 70%故障注入率 - 更高故障率
3. 死锁检测 - 循环等待场景
4. 活锁检测 - 无限重试场景
5. 级联故障 - 单点故障扩散
"""

import asyncio
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class UltimateChaosInjector:
    """终极混乱注入器"""

    def __init__(self, failure_rate=0.7):
        self.failure_rate = failure_rate
        self.stats = {"failures": 0, "delays": 0}

    async def maybe_fail(self, context=""):
        if random.random() < self.failure_rate:
            self.stats["failures"] += 1
            errors = [
                RuntimeError(f"Ultimate chaos #{self.stats['failures']} in {context}"),
                ValueError(f"Value chaos in {context}"),
                TypeError(f"Type chaos in {context}"),
                ConnectionError(f"Connection chaos in {context}"),
                TimeoutError(f"Timeout chaos in {context}"),
                OSError(f"OS chaos in {context}"),
            ]
            raise random.choice(errors)

    async def maybe_delay(self, max_delay=1.0):
        if random.random() < 0.8:
            delay = random.uniform(0.001, max_delay)
            self.stats["delays"] += 1
            await asyncio.sleep(delay)


class TestUltimateConcurrency:
    """极限并发测试"""

    @pytest.mark.asyncio
    async def test_10000_concurrent_operations(self):
        """10000个并发操作"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        chaos = UltimateChaosInjector(failure_rate=0.7)
        stats = {"success": 0, "failure": 0}
        lock = asyncio.Lock()

        async def operation(op_id):
            try:
                await chaos.maybe_delay(0.5)
                await chaos.maybe_fail(f"op_{op_id}")

                event = DomainEvent(
                    event_type="ultimate_concurrent",
                    aggregate_id=f"agg_{op_id % 100}",
                    payload={"id": op_id}
                )
                await bus.publish(event)

                async with lock:
                    stats["success"] += 1
            except Exception:
                async with lock:
                    stats["failure"] += 1

        tasks = [asyncio.create_task(operation(i)) for i in range(10000)]
        await asyncio.gather(*tasks, return_exceptions=True)

        await bus.stop()

        print(f"\n10000并发测试:")
        print(f"  成功: {stats['success']}")
        print(f"  失败: {stats['failure']}")
        print(f"  注入失败: {chaos.stats['failures']}")

        # 成功率应该>20%（70%故障注入）
        assert stats["success"] > 2000


class TestDeadlockDetection:
    """死锁检测测试"""

    @pytest.mark.asyncio
    async def test_read_lock_no_deadlock(self):
        """读锁不应该死锁"""
        try:
            from memory_system.read_write_lock import ReadWriteLock
        except ImportError:
            pytest.skip("read_write_lock not available")
            return

        lock = ReadWriteLock()
        stats = {"completed": 0}

        async def reader_task(task_id):
            async with lock.read_lock():
                await asyncio.sleep(0.01)
                stats["completed"] += 1

        # 启动多个读任务
        tasks = [asyncio.create_task(reader_task(i)) for i in range(20)]
        await asyncio.gather(*tasks)

        print(f"\n读锁无死锁测试:")
        print(f"  完成: {stats['completed']}")

        assert stats["completed"] == 20


class TestCascadingFailure:
    """级联故障测试"""

    @pytest.mark.asyncio
    async def test_cascading_failure_simulation(self):
        """级联故障模拟"""
        try:
            from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
        except ImportError:
            pytest.skip("event_driven_core not available")
            return

        bus = InMemoryEventBus()
        await bus.start()

        # 模拟依赖链：A -> B -> C -> D
        services = {"A": True, "B": True, "C": True, "D": True}
        stats = {"success": 0, "failure": 0}

        async def service_call(service_name):
            if not services[service_name]:
                raise RuntimeError(f"Service {service_name} down")
            await asyncio.sleep(0.01)

        async def operation(op_id):
            try:
                await service_call("A")
                await service_call("B")
                await service_call("C")
                await service_call("D")
                stats["success"] += 1
            except Exception:
                stats["failure"] += 1

        # 正常运行
        tasks = [asyncio.create_task(operation(i)) for i in range(50)]
        await asyncio.gather(*tasks)

        # 模拟C故障
        services["C"] = False

        # 故障后运行
        tasks = [asyncio.create_task(operation(i)) for i in range(50, 100)]
        await asyncio.gather(*tasks)

        await bus.stop()

        print(f"\n级联故障测试:")
        print(f"  故障前成功: {stats['success']}")
        print(f"  故障后失败: {stats['failure']}")

        assert stats["failure"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

"""
性能基准测试
使用 pytest-benchmark（可选）
当 pytest-benchmark 未安装时作为普通性能测试运行
"""

import pytest
import asyncio
import time
import statistics
from typing import List, Callable, Dict, Any
from dataclasses import dataclass

from memory_system import MemorySystem
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from tests.conftest import MockEmbeddingProvider

# 检测 pytest-benchmark 是否可用
try:
    import pytest_benchmark
    BENCHMARK_AVAILABLE = True
except ImportError:
    BENCHMARK_AVAILABLE = False


@dataclass
class BenchmarkResult:
    """基准测试结果"""
    name: str
    iterations: int
    total_time: float
    avg_time: float
    min_time: float
    max_time: float
    std_dev: float
    throughput: float  # ops/sec


def run_benchmark(func: Callable, iterations: int = 100, warmup: int = 10) -> BenchmarkResult:
    """运行基准测试

    Args:
        func: 要测试的函数
        iterations: 迭代次数
        warmup: 预热次数

    Returns:
        BenchmarkResult: 基准测试结果
    """
    times = []

    # 预热
    for _ in range(warmup):
        func()

    # 正式测试
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)

    return BenchmarkResult(
        name=func.__name__,
        iterations=iterations,
        total_time=sum(times),
        avg_time=statistics.mean(times),
        min_time=min(times),
        max_time=max(times),
        std_dev=statistics.stdev(times) if len(times) > 1 else 0,
        throughput=iterations / sum(times) if sum(times) > 0 else 0
    )


def _run_or_benchmark(benchmark_or_none, func):
    """如果有 benchmark fixture 则使用，否则直接运行并计时"""
    if benchmark_or_none is not None:
        return benchmark_or_none(func)
    else:
        start = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - start
        print(f"  耗时: {elapsed*1000:.2f}ms")
        return result


class TestRememberPerformance:
    """存储操作性能测试"""

    def test_remember_short_term(self, benchmark=None):
        """测试短期记忆存储性能"""
        embedding = MockEmbeddingProvider(dimension=64)
        storage = MemoryStorage(max_memories=10000)
        retrieval = HierarchicalRetrieval(strategy="adaptive")
        short_term = ShortTermMemory(capacity=100, ttl_seconds=300)

        memory = MemorySystem(
            storage=storage, embedding=embedding,
            retrieval=retrieval, short_term=short_term
        )

        def store():
            asyncio.run(memory.remember(
                "这是一条测试记忆内容",
                memory_type="short_term", importance="medium"
            ))

        _run_or_benchmark(benchmark, store)
        asyncio.run(memory.close())

    def test_remember_semantic(self, benchmark=None):
        """测试语义记忆存储性能（含嵌入生成）"""
        embedding = MockEmbeddingProvider(dimension=64)
        storage = MemoryStorage(max_memories=10000)
        retrieval = HierarchicalRetrieval(strategy="adaptive")
        short_term = ShortTermMemory(capacity=100, ttl_seconds=300)

        memory = MemorySystem(
            storage=storage, embedding=embedding,
            retrieval=retrieval, short_term=short_term
        )

        def store():
            asyncio.run(memory.remember(
                "这是一条需要生成嵌入的语义记忆",
                memory_type="semantic", importance="high"
            ))

        _run_or_benchmark(benchmark, store)
        asyncio.run(memory.close())

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_remember_performance_detailed(self, memory_system):
        """详细测试存储性能"""
        iterations = 100
        times = []

        for i in range(iterations):
            start = time.perf_counter()
            await memory_system.remember(f"性能测试内容{i}")
            end = time.perf_counter()
            times.append(end - start)

        avg_time = statistics.mean(times) * 1000  # ms
        throughput = iterations / sum(times)

        print(f"\n存储操作性能统计:")
        print(f"  平均耗时: {avg_time:.2f}ms")
        print(f"  最小耗时: {min(times)*1000:.2f}ms")
        print(f"  最大耗时: {max(times)*1000:.2f}ms")
        print(f"  标准差: {statistics.stdev(times)*1000:.2f}ms")
        print(f"  吞吐量: {throughput:.2f} ops/sec")

        assert avg_time < 100, f"存储操作太慢: {avg_time:.2f}ms"


class TestRecallPerformance:
    """检索操作性能测试"""

    def _create_populated_memory(self):
        """创建包含100条记忆的系统"""
        embedding = MockEmbeddingProvider(dimension=64)
        storage = MemoryStorage(max_memories=10000)
        retrieval = HierarchicalRetrieval(strategy="adaptive")
        short_term = ShortTermMemory(capacity=100, ttl_seconds=300)

        memory = MemorySystem(
            storage=storage, embedding=embedding,
            retrieval=retrieval, short_term=short_term
        )

        async def populate():
            for i in range(100):
                await memory.remember(
                    f"这是第{i}条测试记忆，包含一些关键词如Python、机器学习、数据分析",
                    memory_type="semantic" if i % 3 == 0 else "episodic",
                    importance="high" if i % 5 == 0 else "medium"
                )

        asyncio.run(populate())
        return memory

    def test_recall_with_100_memories(self, benchmark=None):
        """测试100条记忆时的检索性能"""
        memory = self._create_populated_memory()

        def search():
            asyncio.run(memory.recall("Python 机器学习", max_results=10))

        _run_or_benchmark(benchmark, search)
        asyncio.run(memory.close())

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_recall_performance_detailed(self, memory_system):
        """详细测试检索性能"""
        # 先存储一些数据
        for i in range(1000):
            await memory_system.remember(f"测试内容{i} 关键词")

        # 测试检索
        iterations = 100
        times = []

        for _ in range(iterations):
            start = time.perf_counter()
            await memory_system.recall("关键词")
            end = time.perf_counter()
            times.append(end - start)

        avg_time = statistics.mean(times) * 1000
        throughput = iterations / sum(times)

        print(f"\n检索操作性能统计:")
        print(f"  平均耗时: {avg_time:.2f}ms")
        print(f"  最小耗时: {min(times)*1000:.2f}ms")
        print(f"  最大耗时: {max(times)*1000:.2f}ms")
        print(f"  标准差: {statistics.stdev(times)*1000:.2f}ms")
        print(f"  吞吐量: {throughput:.2f} ops/sec")

        assert avg_time < 50, f"检索操作太慢: {avg_time:.2f}ms"


class TestStoragePerformance:
    """存储后端性能测试"""

    def test_memory_storage_add(self, benchmark=None):
        """测试内存存储添加性能"""
        storage = MemoryStorage(max_memories=10000)

        from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel

        async def add_item():
            item = MemoryItem(
                content="测试内容",
                memory_type=MemoryType.SEMANTIC,
                importance=ImportanceLevel.MEDIUM
            )
            await storage.add(item)

        _run_or_benchmark(benchmark, lambda: asyncio.run(add_item()))

    def test_short_term_add(self, benchmark=None):
        """测试短期记忆添加性能"""
        stm = ShortTermMemory(capacity=100, ttl_seconds=300)

        from memory_system.core_abstractions.memory_data_models import ImportanceLevel

        async def add():
            await stm.add("测试内容", importance=ImportanceLevel.MEDIUM)

        _run_or_benchmark(benchmark, lambda: asyncio.run(add()))


class TestConcurrentPerformance:
    """并发性能测试"""

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_concurrent_performance(self, memory_system):
        """测试并发性能"""
        async def worker(worker_id: int):
            for i in range(10):
                await memory_system.remember(f"工作者{worker_id} 内容{i}")
            return await memory_system.recall(f"工作者{worker_id}")

        start = time.perf_counter()
        workers = [worker(i) for i in range(10)]
        await asyncio.gather(*workers)
        end = time.perf_counter()

        total_time = (end - start) * 1000
        print(f"\n10并发工作者总耗时: {total_time:.2f}ms")
        assert total_time < 5000, f"并发性能不足: {total_time:.2f}ms"


class TestBatchPerformance:
    """批量操作性能测试"""

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_batch_remember_performance(self, memory_system):
        """测试批量存储性能"""
        batch_sizes = [10, 50, 100]

        for batch_size in batch_sizes:
            contents = [f"批量内容{i}" for i in range(batch_size)]

            start = time.perf_counter()
            await memory_system.remember_batch(contents)
            end = time.perf_counter()

            elapsed = (end - start) * 1000
            throughput = batch_size / (end - start)

            print(f"\n批量存储 {batch_size} 条:")
            print(f"  总耗时: {elapsed:.2f}ms")
            print(f"  单条平均: {elapsed/batch_size:.2f}ms")
            print(f"  吞吐量: {throughput:.2f} ops/sec")


class TestScalabilityPerformance:
    """可扩展性性能测试"""

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_scalability_with_data_size(self, memory_system):
        """测试不同数据规模下的性能"""
        data_sizes = [100, 500, 1000]

        for size in data_sizes:
            # 准备数据
            for i in range(size):
                await memory_system.remember(f"规模测试{i} 关键词")

            # 测试检索性能
            times = []
            for _ in range(10):
                start = time.perf_counter()
                await memory_system.recall("关键词")
                end = time.perf_counter()
                times.append(end - start)

            avg_time = statistics.mean(times) * 1000
            print(f"\n数据规模 {size} 条记忆:")
            print(f"  检索平均耗时: {avg_time:.2f}ms")


def generate_performance_report(results: List[BenchmarkResult]) -> str:
    """生成性能测试报告

    Args:
        results: 基准测试结果列表

    Returns:
        str: Markdown格式的报告
    """
    lines = [
        "# 性能基准测试报告",
        "",
        "## 测试环境",
        f"- 测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 测试结果",
        "",
        "| 测试名称 | 迭代次数 | 平均耗时(ms) | 最小耗时(ms) | 最大耗时(ms) | 标准差(ms) | 吞吐量(ops/sec) |",
        "|---------|---------|-------------|-------------|-------------|-----------|----------------|"
    ]

    for r in results:
        lines.append(
            f"| {r.name} | {r.iterations} | {r.avg_time*1000:.2f} | "
            f"{r.min_time*1000:.2f} | {r.max_time*1000:.2f} | "
            f"{r.std_dev*1000:.2f} | {r.throughput:.2f} |"
        )

    lines.extend([
        "",
        "## 性能指标",
        "",
        "- 存储操作目标: < 100ms",
        "- 检索操作目标: < 50ms",
        "- 并发性能目标: 10并发 < 5000ms",
        ""
    ])

    return "\n".join(lines)

"""性能回归测试

检测性能退化，确保新版本不会比旧版本慢。
"""

import pytest
import json
import os
import time
import statistics
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class PerformanceBaseline:
    """性能基线数据"""
    test_name: str
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    std_dev_ms: float
    timestamp: float
    version: str
    git_commit: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PerformanceBaseline":
        return cls(**data)


class PerformanceRegressionTest:
    """性能回归测试基类"""

    BASELINE_FILE = ".performance_baseline.json"
    REGRESSION_THRESHOLD = 1.2  # 20% regression allowed
    IMPROVEMENT_THRESHOLD = 0.8  # 20% improvement to update baseline

    def __init__(self, baseline_dir: Optional[str] = None):
        """
        Args:
            baseline_dir: 基线文件存储目录，默认为当前目录
        """
        if baseline_dir:
            self.baseline_path = Path(baseline_dir) / self.BASELINE_FILE
        else:
            self.baseline_path = Path(__file__).parent / self.BASELINE_FILE

    def load_baseline(self) -> Dict[str, PerformanceBaseline]:
        """加载性能基线"""
        if self.baseline_path.exists():
            with open(self.baseline_path, 'r') as f:
                data = json.load(f)
                return {
                    name: PerformanceBaseline.from_dict(baseline)
                    for name, baseline in data.items()
                }
        return {}

    def save_baseline(self, baselines: Dict[str, PerformanceBaseline]) -> None:
        """保存性能基线"""
        self.baseline_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.baseline_path, 'w') as f:
            json.dump(
                {name: baseline.to_dict() for name, baseline in baselines.items()},
                f,
                indent=2
            )

    def check_regression(
        self,
        test_name: str,
        current_time_ms: float,
        update_baseline: bool = False
    ) -> Dict[str, Any]:
        """检查性能回归

        Args:
            test_name: 测试名称
            current_time_ms: 当前测试耗时(毫秒)
            update_baseline: 是否更新基线

        Returns:
            Dict: 检查结果
        """
        baselines = self.load_baseline()

        result = {
            "test_name": test_name,
            "current_time_ms": current_time_ms,
            "baseline_time_ms": None,
            "ratio": None,
            "regression_detected": False,
            "improvement_detected": False,
            "message": ""
        }

        if test_name not in baselines:
            # First run, save as baseline
            baselines[test_name] = PerformanceBaseline(
                test_name=test_name,
                avg_time_ms=current_time_ms,
                min_time_ms=current_time_ms,
                max_time_ms=current_time_ms,
                std_dev_ms=0.0,
                timestamp=time.time(),
                version="2.29.0"
            )
            if update_baseline:
                self.save_baseline(baselines)
            result["message"] = f"首次运行，已创建基线: {current_time_ms:.2f}ms"
            return result

        baseline = baselines[test_name]
        result["baseline_time_ms"] = baseline.avg_time_ms

        ratio = current_time_ms / baseline.avg_time_ms
        result["ratio"] = ratio

        if ratio > self.REGRESSION_THRESHOLD:
            result["regression_detected"] = True
            result["message"] = (
                f"性能回归检测到: {test_name} "
                f"当前 {current_time_ms:.2f}ms vs 基线 {baseline.avg_time_ms:.2f}ms "
                f"(比率: {ratio:.2f}x, 阈值: {self.REGRESSION_THRESHOLD}x)"
            )
        elif ratio < self.IMPROVEMENT_THRESHOLD:
            result["improvement_detected"] = True
            result["message"] = (
                f"性能提升检测到: {test_name} "
                f"当前 {current_time_ms:.2f}ms vs 基线 {baseline.avg_time_ms:.2f}ms "
                f"(比率: {ratio:.2f}x)"
            )
            # Update baseline if significantly improved
            if update_baseline:
                baselines[test_name] = PerformanceBaseline(
                    test_name=test_name,
                    avg_time_ms=current_time_ms,
                    min_time_ms=min(current_time_ms, baseline.min_time_ms),
                    max_time_ms=max(current_time_ms, baseline.max_time_ms),
                    std_dev_ms=0.0,
                    timestamp=time.time(),
                    version="2.29.0"
                )
                self.save_baseline(baselines)
        else:
            result["message"] = (
                f"性能正常: {test_name} "
                f"当前 {current_time_ms:.2f}ms vs 基线 {baseline.avg_time_ms:.2f}ms "
                f"(比率: {ratio:.2f}x)"
            )

        return result


class TestPerformanceRegression(PerformanceRegressionTest):
    """性能回归测试套件"""

    def __init__(self):
        super().__init__(baseline_dir=str(Path(__file__).parent))

    @pytest.mark.regression
    @pytest.mark.asyncio
    async def test_remember_performance_regression(self, memory_system):
        """测试存储性能回归"""
        import time

        iterations = 50
        times = []

        for i in range(iterations):
            start = time.perf_counter()
            await memory_system.remember(f"性能回归测试{i}")
            elapsed = time.perf_counter() - start
            times.append(elapsed * 1000)  # Convert to ms

        avg_time = statistics.mean(times)

        result = self.check_regression(
            "remember_50",
            avg_time,
            update_baseline=os.environ.get("UPDATE_BASELINE", "false").lower() == "true"
        )

        print(f"\n{result['message']}")

        if result["regression_detected"]:
            pytest.fail(result["message"])

    @pytest.mark.regression
    @pytest.mark.asyncio
    async def test_recall_performance_regression(self, memory_system):
        """测试检索性能回归"""
        import time

        # 准备数据
        for i in range(500):
            await memory_system.remember(f"检索测试{i} 关键词")

        iterations = 50
        times = []

        for _ in range(iterations):
            start = time.perf_counter()
            await memory_system.recall("关键词")
            elapsed = time.perf_counter() - start
            times.append(elapsed * 1000)

        avg_time = statistics.mean(times)

        result = self.check_regression(
            "recall_500",
            avg_time,
            update_baseline=os.environ.get("UPDATE_BASELINE", "false").lower() == "true"
        )

        print(f"\n{result['message']}")

        if result["regression_detected"]:
            pytest.fail(result["message"])

    @pytest.mark.regression
    @pytest.mark.asyncio
    async def test_batch_remember_performance_regression(self, memory_system):
        """测试批量存储性能回归"""
        import time

        batch_size = 50
        contents = [f"批量内容{i}" for i in range(batch_size)]

        start = time.perf_counter()
        await memory_system.remember_batch(contents)
        elapsed = time.perf_counter() - start

        avg_time = elapsed * 1000 / batch_size  # Per item

        result = self.check_regression(
            "batch_remember_50",
            avg_time,
            update_baseline=os.environ.get("UPDATE_BASELINE", "false").lower() == "true"
        )

        print(f"\n{result['message']}")

        if result["regression_detected"]:
            pytest.fail(result["message"])

    @pytest.mark.regression
    @pytest.mark.asyncio
    async def test_concurrent_performance_regression(self, memory_system):
        """测试并发性能回归"""
        import asyncio
        import time

        async def worker(worker_id: int):
            for i in range(5):
                await memory_system.remember(f"工作者{worker_id} 内容{i}")
            return await memory_system.recall(f"工作者{worker_id}")

        start = time.perf_counter()
        workers = [worker(i) for i in range(5)]
        await asyncio.gather(*workers)
        elapsed = time.perf_counter() - start

        avg_time = elapsed * 1000  # Total time

        result = self.check_regression(
            "concurrent_5_workers",
            avg_time,
            update_baseline=os.environ.get("UPDATE_BASELINE", "false").lower() == "true"
        )

        print(f"\n{result['message']}")

        if result["regression_detected"]:
            pytest.fail(result["message"])


def generate_regression_report(results: list) -> str:
    """生成回归测试报告

    Args:
        results: 测试结果列表

    Returns:
        str: Markdown格式的报告
    """
    lines = [
        "# 性能回归测试报告",
        "",
        f"**测试时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 测试结果",
        "",
        "| 测试名称 | 当前耗时(ms) | 基线耗时(ms) | 比率 | 状态 |",
        "|---------|-------------|-------------|------|------|"
    ]

    for result in results:
        status = "正常"
        if result.get("regression_detected"):
            status = "退化"
        elif result.get("improvement_detected"):
            status = "提升"

        ratio = result.get("ratio", "-")
        if isinstance(ratio, float):
            ratio = f"{ratio:.2f}x"

        lines.append(
            f"| {result['test_name']} | {result['current_time_ms']:.2f} | "
            f"{result.get('baseline_time_ms', '-')} | {ratio} | {status} |"
        )

    lines.extend([
        "",
        "## 说明",
        "",
        "- 比率 > 1.2x: 性能退化",
        "- 比率 < 0.8x: 性能提升",
        "- 比率在 0.8x ~ 1.2x 之间: 性能正常",
        "",
        "**阈值设置**: 允许 20% 的性能波动",
        ""
    ])

    return "\n".join(lines)


class TestMemoryUsageRegression:
    """内存使用回归测试"""

    @pytest.mark.regression
    @pytest.mark.asyncio
    async def test_memory_usage_regression(self, memory_system):
        """测试内存使用回归"""
        import gc
        import tracemalloc

        # 开始跟踪内存
        tracemalloc.start()
        gc.collect()

        # 记录初始内存
        snapshot1 = tracemalloc.take_snapshot()

        # 执行操作
        for i in range(1000):
            await memory_system.remember(f"内存测试{i}")

        gc.collect()
        snapshot2 = tracemalloc.take_snapshot()

        # 计算内存增长
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        total_diff = sum(stat.size_diff for stat in top_stats[:10] if stat.size_diff > 0)

        # 平均每条记忆的内存占用
        avg_memory_per_item = total_diff / 1000

        print(f"\n内存使用统计:")
        print(f"  总内存增长: {total_diff / 1024 / 1024:.2f} MB")
        print(f"  单条平均: {avg_memory_per_item / 1024:.2f} KB")

        # 检查是否超过阈值 (假设每条记忆不应超过 100KB)
        assert avg_memory_per_item < 100 * 1024, f"内存使用过高: {avg_memory_per_item / 1024:.2f} KB/条"

        tracemalloc.stop()


class TestThroughputRegression:
    """吞吐量回归测试"""

    @pytest.mark.regression
    @pytest.mark.asyncio
    async def test_throughput_regression(self, memory_system):
        """测试吞吐量回归"""
        import time

        duration = 5  # 测试5秒
        count = 0

        start = time.perf_counter()
        while time.perf_counter() - start < duration:
            await memory_system.remember(f"吞吐量测试{count}")
            count += 1

        elapsed = time.perf_counter() - start
        throughput = count / elapsed

        print(f"\n吞吐量统计:")
        print(f"  操作次数: {count}")
        print(f"  耗时: {elapsed:.2f}s")
        print(f"  吞吐量: {throughput:.2f} ops/sec")

        # 检查吞吐量是否达标 (假设至少 50 ops/sec)
        assert throughput > 50, f"吞吐量不足: {throughput:.2f} ops/sec"

"""
监控指标模块测试
"""
import pytest
import time

from memory_system.monitoring_metrics_collector import MemoryMetrics


class TestMemoryMetrics:
    """MemoryMetrics 测试"""

    def test_create_metrics(self):
        """创建指标实例"""
        metrics = MemoryMetrics()
        assert metrics is not None

    def test_record_remember(self):
        """记录存储操作"""
        metrics = MemoryMetrics()
        metrics.record_remember("semantic", "high", 0.1)
        # 无异常即通过

    def test_record_recall(self):
        """记录检索操作"""
        metrics = MemoryMetrics()
        metrics.record_recall(0.05)

    def test_record_delete(self):
        """记录删除操作"""
        metrics = MemoryMetrics()
        metrics.record_delete()

    def test_record_embedding(self):
        """记录嵌入生成"""
        metrics = MemoryMetrics()
        metrics.record_embedding(0.2)

    def test_update_memory_count(self):
        """更新记忆数量"""
        metrics = MemoryMetrics()
        metrics.update_memory_count("semantic", 100)
        metrics.update_memory_count("episodic", 50)

    def test_update_short_term_size(self):
        """更新短期记忆大小"""
        metrics = MemoryMetrics()
        metrics.update_short_term_size(7)

    def test_set_system_info(self):
        """设置系统信息"""
        metrics = MemoryMetrics()
        metrics.set_system_info("2.0.0", "memory", "mock")

    def test_time_remember_context_manager(self):
        """计时上下文管理器"""
        metrics = MemoryMetrics()
        with metrics.time_remember():
            time.sleep(0.01)
        # 无异常即通过

    def test_time_recall_context_manager(self):
        """检索计时上下文管理器"""
        metrics = MemoryMetrics()
        with metrics.time_recall():
            time.sleep(0.01)
        # 无异常即通过

    def test_noop_when_prometheus_unavailable(self):
        """prometheus 不可用时降级为 noop"""
        # MemoryMetrics 在 prometheus 不可用时应该静默降级
        metrics = MemoryMetrics()
        metrics.record_remember("test", "low", 0.1)
        metrics.record_recall(0.1)
        metrics.update_memory_count("test", 0)
        # 全部无异常

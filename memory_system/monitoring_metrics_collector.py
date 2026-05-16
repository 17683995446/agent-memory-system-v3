"""
监控指标模块
基于 prometheus-client 的可观测性
"""

from typing import Optional
from contextlib import contextmanager
import time

try:
    from prometheus_client import Counter, Histogram, Gauge, Info, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


class MemoryMetrics:
    """记忆系统监控指标"""

    def __init__(self, registry: Optional["CollectorRegistry"] = None):
        if not PROMETHEUS_AVAILABLE:
            self._noop = True
            return

        self._noop = False
        self.registry = registry or CollectorRegistry()

        # 计数器
        self.memories_added = Counter(
            "memory_added_total",
            "Total number of memories added",
            ["memory_type", "importance"],
            registry=self.registry
        )
        self.memories_retrieved = Counter(
            "memory_retrieved_total",
            "Total number of memory retrievals",
            registry=self.registry
        )
        self.memories_deleted = Counter(
            "memory_deleted_total",
            "Total number of memories deleted",
            registry=self.registry
        )

        # 直方图
        self.remember_duration = Histogram(
            "memory_remember_duration_seconds",
            "Time spent storing memories",
            registry=self.registry
        )
        self.recall_duration = Histogram(
            "memory_recall_duration_seconds",
            "Time spent retrieving memories",
            registry=self.registry
        )
        self.embedding_duration = Histogram(
            "memory_embedding_duration_seconds",
            "Time spent generating embeddings",
            registry=self.registry
        )

        # 仪表盘
        self.memory_count = Gauge(
            "memory_count",
            "Current number of memories",
            ["memory_type"],
            registry=self.registry
        )
        self.short_term_size = Gauge(
            "memory_short_term_size",
            "Current short term memory size",
            registry=self.registry
        )

        # 信息
        self.system_info = Info(
            "memory_system",
            "Memory system information",
            registry=self.registry
        )

    def record_remember(self, memory_type: str, importance: str, duration: float):
        """记录存储操作"""
        if self._noop:
            return
        self.memories_added.labels(memory_type=memory_type, importance=importance).inc()
        self.remember_duration.observe(duration)

    def record_recall(self, duration: float):
        """记录检索操作"""
        if self._noop:
            return
        self.memories_retrieved.inc()
        self.recall_duration.observe(duration)

    def record_delete(self):
        """记录删除操作"""
        if self._noop:
            return
        self.memories_deleted.inc()

    def record_embedding(self, duration: float):
        """记录嵌入生成时间"""
        if self._noop:
            return
        self.embedding_duration.observe(duration)

    def update_memory_count(self, memory_type: str, count: int):
        """更新记忆数量"""
        if self._noop:
            return
        self.memory_count.labels(memory_type=memory_type).set(count)

    def update_short_term_size(self, size: int):
        """更新短期记忆大小"""
        if self._noop:
            return
        self.short_term_size.set(size)

    def set_system_info(self, version: str, backend: str, embedding: str):
        """设置系统信息"""
        if self._noop:
            return
        self.system_info.info({
            "version": version,
            "backend": backend,
            "embedding": embedding
        })

    @contextmanager
    def time_remember(self):
        """计时上下文管理器 - remember"""
        start = time.time()
        try:
            yield
        finally:
            if not self._noop:
                self.remember_duration.observe(time.time() - start)

    @contextmanager
    def time_recall(self):
        """计时上下文管理器 - recall"""
        start = time.time()
        try:
            yield
        finally:
            if not self._noop:
                self.recall_duration.observe(time.time() - start)

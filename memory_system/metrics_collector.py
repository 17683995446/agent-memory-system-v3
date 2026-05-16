"""集中式指标收集器

Agent Memory System V2.29.0 重构项目 - R-03
重构统计信息收集，提供统一的指标收集和管理
"""

from typing import Dict, Any, Callable, List, Optional, Union
from dataclasses import dataclass, field
from collections import deque
import time
import threading
import asyncio
from contextlib import asynccontextmanager, contextmanager

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class MetricRecord:
    """指标记录"""
    name: str
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp,
            "labels": self.labels
        }


@dataclass
class HistogramBucket:
    """直方图桶"""
    upper_bound: float
    count: int = 0
    
    def add(self, value: float) -> bool:
        """添加值到桶，如果值在范围内返回True"""
        if value <= self.upper_bound:
            self.count += 1
            return True
        return False


class Histogram:
    """直方图指标 - 用于记录分布"""
    
    DEFAULT_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
    
    def __init__(self, name: str, buckets: Optional[List[float]] = None, description: str = ""):
        self.name = name
        self.description = description
        self.buckets = [HistogramBucket(b) for b in (buckets or self.DEFAULT_BUCKETS)]
        self.buckets.append(HistogramBucket(float('inf')))  # +Inf bucket
        self.sum = 0.0
        self.count = 0
        self._lock = threading.Lock()
    
    def observe(self, value: float):
        """记录观测值"""
        with self._lock:
            self.sum += value
            self.count += 1
            for bucket in self.buckets:
                bucket.add(value)
    
    def get_snapshot(self) -> Dict[str, Any]:
        """获取当前快照"""
        with self._lock:
            return {
                "name": self.name,
                "count": self.count,
                "sum": self.sum,
                "avg": self.sum / self.count if self.count > 0 else 0,
                "buckets": [
                    {"le": b.upper_bound, "count": b.count}
                    for b in self.buckets
                ]
            }


class MetricsCollector:
    """集中式指标收集器
    
    提供统一的指标收集、存储和导出功能。
    支持计数器、仪表盘、直方图和摘要等多种指标类型。
    
    Attributes:
        max_history: 每个指标保留的最大历史记录数
        enable_histograms: 是否启用直方图自动收集
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, max_history: int = 10000, enable_histograms: bool = True):
        if self._initialized:
            return
        
        self._initialized = True
        self.max_history = max_history
        self.enable_histograms = enable_histograms
        
        # 指标存储
        self._metrics: Dict[str, deque] = {}
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Histogram] = {}
        
        # 回调函数
        self._exporters: List[Callable[[Dict[str, Any]], None]] = []
        
        # 线程安全
        self._lock = threading.Lock()
        
        logger.info("MetricsCollector initialized")
    
    def record(self, name: str, value: float, **labels):
        """记录指标值
        
        Args:
            name: 指标名称
            value: 指标值
            **labels: 标签键值对
        """
        with self._lock:
            if name not in self._metrics:
                self._metrics[name] = deque(maxlen=self.max_history)
            
            record = MetricRecord(name, value, labels=labels)
            self._metrics[name].append(record)
            
            # 自动更新直方图
            if self.enable_histograms and name in self._histograms:
                self._histograms[name].observe(value)
    
    def increment(self, name: str, delta: int = 1, **labels):
        """增加计数器
        
        Args:
            name: 计数器名称
            delta: 增量（默认为1）
            **labels: 标签键值对
        """
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + delta
        
        # 同时记录为时间序列
        self.record(f"{name}_total", self._counters[name], **labels)
    
    def decrement(self, name: str, delta: int = 1, **labels):
        """减少计数器"""
        self.increment(name, -delta, **labels)
    
    def gauge(self, name: str, value: float, **labels):
        """设置仪表盘值
        
        Args:
            name: 仪表盘名称
            value: 当前值
            **labels: 标签键值对
        """
        with self._lock:
            self._gauges[name] = value
        
        self.record(name, value, **labels)
    
    def histogram(self, name: str, value: float, buckets: Optional[List[float]] = None):
        """记录直方图观测值
        
        Args:
            name: 直方图名称
            value: 观测值
            buckets: 自定义桶边界（可选）
        """
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, buckets)
        
        self._histograms[name].observe(value)
    
    @asynccontextmanager
    async def timeit_async(self, name: str, **labels):
        """异步上下文管理器用于计时
        
        Example:
            async with collector.timeit_async("operation_duration"):
                await do_something()
        """
        start = time.time()
        try:
            yield
        finally:
            duration = time.time() - start
            self.record(name, duration, **labels)
            self.histogram(f"{name}_histogram", duration)
    
    @contextmanager
    def timeit(self, name: str, **labels):
        """同步上下文管理器用于计时
        
        Example:
            with collector.timeit("operation_duration"):
                do_something()
        """
        start = time.time()
        try:
            yield
        finally:
            duration = time.time() - start
            self.record(name, duration, **labels)
            self.histogram(f"{name}_histogram", duration)
    
    def timer(self, name: str, **labels):
        """获取计时器上下文管理器（自动选择同步/异步）
        
        这是一个工厂方法，返回适合当前环境的计时器。
        """
        try:
            asyncio.get_running_loop()
            return self.timeit_async(name, **labels)
        except RuntimeError:
            return self.timeit(name, **labels)
    
    def get_counter(self, name: str) -> int:
        """获取计数器当前值"""
        with self._lock:
            return self._counters.get(name, 0)
    
    def get_gauge(self, name: str) -> float:
        """获取仪表盘当前值"""
        with self._lock:
            return self._gauges.get(name, 0.0)
    
    def get_histogram(self, name: str) -> Optional[Dict[str, Any]]:
        """获取直方图快照"""
        with self._lock:
            if name in self._histograms:
                return self._histograms[name].get_snapshot()
            return None
    
    def get_metrics(self, name: str, limit: Optional[int] = None) -> List[MetricRecord]:
        """获取指定指标的历史记录"""
        with self._lock:
            if name not in self._metrics:
                return []
            records = list(self._metrics[name])
            if limit:
                records = records[-limit:]
            return records
    
    def get_all_metrics(self) -> Dict[str, List[Dict[str, Any]]]:
        """获取所有指标的历史记录"""
        with self._lock:
            return {
                name: [r.to_dict() for r in records]
                for name, records in self._metrics.items()
            }
    
    def get_summary(self) -> Dict[str, Any]:
        """获取指标摘要"""
        with self._lock:
            summary = {
                "counters": self._counters.copy(),
                "gauges": self._gauges.copy(),
                "histograms": {
                    name: hist.get_snapshot()
                    for name, hist in self._histograms.items()
                },
                "metric_names": list(self._metrics.keys()),
                "total_records": sum(len(records) for records in self._metrics.values())
            }
        return summary
    
    def aggregate(self, name: str, func: str = "avg") -> Optional[float]:
        """聚合指标值
        
        Args:
            name: 指标名称
            func: 聚合函数 ("avg", "sum", "min", "max", "count")
            
        Returns:
            聚合结果
        """
        records = self.get_metrics(name)
        if not records:
            return None
        
        values = [r.value for r in records]
        
        if func == "avg":
            return sum(values) / len(values)
        elif func == "sum":
            return sum(values)
        elif func == "min":
            return min(values)
        elif func == "max":
            return max(values)
        elif func == "count":
            return len(values)
        else:
            raise ValueError(f"Unknown aggregation function: {func}")
    
    def clear(self, name: Optional[str] = None):
        """清除指标数据
        
        Args:
            name: 要清除的指标名称，为None则清除所有
        """
        with self._lock:
            if name:
                self._metrics.pop(name, None)
                self._counters.pop(name, None)
                self._gauges.pop(name, None)
                self._histograms.pop(name, None)
            else:
                self._metrics.clear()
                self._counters.clear()
                self._gauges.clear()
                self._histograms.clear()
    
    def register_exporter(self, exporter: Callable[[Dict[str, Any]], None]):
        """注册指标导出器
        
        Args:
            exporter: 导出器回调函数，接收指标摘要字典
        """
        self._exporters.append(exporter)
    
    def export(self):
        """导出指标到所有注册的导出器"""
        summary = self.get_summary()
        for exporter in self._exporters:
            try:
                exporter(summary)
            except Exception as e:
                logger.error(f"Exporter failed: {e}")
    
    def create_report(self, time_range_seconds: Optional[float] = None) -> Dict[str, Any]:
        """创建指标报告
        
        Args:
            time_range_seconds: 时间范围（秒），None表示全部
            
        Returns:
            报告字典
        """
        now = time.time()
        cutoff = now - time_range_seconds if time_range_seconds else 0
        
        report = {
            "generated_at": now,
            "time_range_seconds": time_range_seconds,
            "metrics": {}
        }
        
        with self._lock:
            for name, records in self._metrics.items():
                filtered = [r for r in records if r.timestamp >= cutoff]
                if filtered:
                    values = [r.value for r in filtered]
                    report["metrics"][name] = {
                        "count": len(values),
                        "avg": sum(values) / len(values),
                        "min": min(values),
                        "max": max(values),
                        "sum": sum(values)
                    }
        
        return report


class MetricDecorator:
    """指标装饰器 - 用于自动收集函数指标"""
    
    def __init__(self, collector: Optional[MetricsCollector] = None):
        self.collector = collector or MetricsCollector()
    
    def count_calls(self, name: Optional[str] = None):
        """装饰器：统计调用次数"""
        def decorator(func):
            metric_name = name or f"{func.__module__}.{func.__name__}_calls"
            
            if asyncio.iscoroutinefunction(func):
                async def wrapper(*args, **kwargs):
                    self.collector.increment(metric_name)
                    return await func(*args, **kwargs)
            else:
                def wrapper(*args, **kwargs):
                    self.collector.increment(metric_name)
                    return func(*args, **kwargs)
            
            wrapper.__name__ = func.__name__
            wrapper.__doc__ = func.__doc__
            return wrapper
        return decorator
    
    def time_execution(self, name: Optional[str] = None):
        """装饰器：统计执行时间"""
        def decorator(func):
            metric_name = name or f"{func.__module__}.{func.__name__}_duration"
            
            if asyncio.iscoroutinefunction(func):
                async def wrapper(*args, **kwargs):
                    start = time.time()
                    try:
                        return await func(*args, **kwargs)
                    finally:
                        duration = time.time() - start
                        self.collector.record(metric_name, duration)
            else:
                def wrapper(*args, **kwargs):
                    start = time.time()
                    try:
                        return func(*args, **kwargs)
                    finally:
                        duration = time.time() - start
                        self.collector.record(metric_name, duration)
            
            wrapper.__name__ = func.__name__
            wrapper.__doc__ = func.__doc__
            return wrapper
        return decorator
    
    def track_errors(self, name: Optional[str] = None):
        """装饰器：统计错误次数"""
        def decorator(func):
            metric_name = name or f"{func.__module__}.{func.__name__}_errors"
            
            if asyncio.iscoroutinefunction(func):
                async def wrapper(*args, **kwargs):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        self.collector.increment(metric_name, error_type=type(e).__name__)
                        raise
            else:
                def wrapper(*args, **kwargs):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        self.collector.increment(metric_name, error_type=type(e).__name__)
                        raise
            
            wrapper.__name__ = func.__name__
            wrapper.__doc__ = func.__doc__
            return wrapper
        return decorator


# 全局实例和便捷函数
def get_metrics_collector() -> MetricsCollector:
    """获取全局指标收集器实例"""
    return MetricsCollector()


def record_metric(name: str, value: float, **labels):
    """便捷函数：记录指标"""
    get_metrics_collector().record(name, value, **labels)


def increment_counter(name: str, delta: int = 1, **labels):
    """便捷函数：增加计数器"""
    get_metrics_collector().increment(name, delta, **labels)


def set_gauge(name: str, value: float, **labels):
    """便捷函数：设置仪表盘"""
    get_metrics_collector().gauge(name, value, **labels)


def observe_histogram(name: str, value: float):
    """便捷函数：记录直方图"""
    get_metrics_collector().histogram(name, value)

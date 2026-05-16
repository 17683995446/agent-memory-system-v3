#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全链路追踪系统 (Distributed Tracing System)

基于OpenTelemetry的分布式追踪实现，支持追踪上下文传播、多种采样策略、
追踪数据存储和查询，以及与Jaeger/Zipkin等后端的集成。

Author: Agent Memory System Team
Version: 3.0.0
"""

import asyncio
import hashlib
import json
import logging
import random
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)
from functools import wraps

# 配置日志
logger = logging.getLogger(__name__)


class SpanKind(Enum):
    """Span类型"""
    INTERNAL = "INTERNAL"
    SERVER = "SERVER"
    CLIENT = "CLIENT"
    PRODUCER = "PRODUCER"
    CONSUMER = "CONSUMER"


class SpanStatus(Enum):
    """Span状态"""
    UNSET = "UNSET"
    OK = "OK"
    ERROR = "ERROR"


class SamplingStrategy(Enum):
    """采样策略"""
    ALWAYS_ON = "always_on"          # 总是采样
    ALWAYS_OFF = "always_off"        # 总是跳过
    PROBABILISTIC = "probabilistic"  # 概率采样
    RATE_LIMITING = "rate_limiting"  # 速率限制采样
    PARENT_BASED = "parent_based"    # 基于父Span的采样


@dataclass
class TraceContext:
    """追踪上下文"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    sampled: bool = True
    baggage: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "sampled": self.sampled,
            "baggage": self.baggage,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TraceContext":
        """从字典创建"""
        return cls(
            trace_id=data["trace_id"],
            span_id=data["span_id"],
            parent_span_id=data.get("parent_span_id"),
            sampled=data.get("sampled", True),
            baggage=data.get("baggage", {}),
        )


@dataclass
class SpanEvent:
    """Span事件"""
    name: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SpanLink:
    """Span链接"""
    trace_id: str
    span_id: str
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Span:
    """追踪Span"""
    span_id: str
    trace_id: str
    parent_id: Optional[str] = None
    name: str = ""
    kind: SpanKind = SpanKind.INTERNAL
    status: SpanStatus = SpanStatus.UNSET
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[SpanEvent] = field(default_factory=list)
    links: List[SpanLink] = field(default_factory=list)
    
    def duration_ms(self) -> float:
        """获取Span持续时间(毫秒)"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return (datetime.utcnow() - self.start_time).total_seconds() * 1000
    
    def set_attribute(self, key: str, value: Any) -> None:
        """设置属性"""
        self.attributes[key] = value
    
    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """添加事件"""
        self.events.append(SpanEvent(
            name=name,
            attributes=attributes or {}
        ))
    
    def add_link(self, trace_id: str, span_id: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """添加链接"""
        self.links.append(SpanLink(
            trace_id=trace_id,
            span_id=span_id,
            attributes=attributes or {}
        ))
    
    def set_status(self, status: SpanStatus, description: str = "") -> None:
        """设置状态"""
        self.status = status
        if description:
            self.set_attribute("status.description", description)
    
    def end(self, end_time: Optional[datetime] = None) -> None:
        """结束Span"""
        self.end_time = end_time or datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "span_id": self.span_id,
            "trace_id": self.trace_id,
            "parent_id": self.parent_id,
            "name": self.name,
            "kind": self.kind.value,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms(),
            "attributes": self.attributes,
            "events": [
                {
                    "name": e.name,
                    "timestamp": e.timestamp.isoformat(),
                    "attributes": e.attributes,
                }
                for e in self.events
            ],
            "links": [
                {
                    "trace_id": l.trace_id,
                    "span_id": l.span_id,
                    "attributes": l.attributes,
                }
                for l in self.links
            ],
        }


@dataclass
class Trace:
    """完整追踪"""
    trace_id: str
    spans: List[Span] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    
    def add_span(self, span: Span) -> None:
        """添加Span"""
        self.spans.append(span)
    
    def get_root_span(self) -> Optional[Span]:
        """获取根Span"""
        for span in self.spans:
            if span.parent_id is None:
                return span
        return self.spans[0] if self.spans else None
    
    def duration_ms(self) -> float:
        """获取追踪持续时间"""
        if self.spans:
            durations = [s.duration_ms() for s in self.spans if s.end_time]
            return max(durations) if durations else 0
        return 0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "trace_id": self.trace_id,
            "span_count": len(self.spans),
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms(),
            "spans": [s.to_dict() for s in self.spans],
        }


class Sampler(ABC):
    """采样器抽象基类"""
    
    @abstractmethod
    def should_sample(
        self,
        trace_id: str,
        parent_context: Optional[TraceContext],
        name: str,
        kind: SpanKind
    ) -> bool:
        """决定是否采样"""
        pass


class AlwaysOnSampler(Sampler):
    """总是采样"""
    
    def should_sample(
        self,
        trace_id: str,
        parent_context: Optional[TraceContext],
        name: str,
        kind: SpanKind
    ) -> bool:
        return True


class AlwaysOffSampler(Sampler):
    """总是跳过"""
    
    def should_sample(
        self,
        trace_id: str,
        parent_context: Optional[TraceContext],
        name: str,
        kind: SpanKind
    ) -> bool:
        return False


class ProbabilisticSampler(Sampler):
    """概率采样器"""
    
    def __init__(self, probability: float = 0.1):
        self.probability = max(0.0, min(1.0, probability))
    
    def should_sample(
        self,
        trace_id: str,
        parent_context: Optional[TraceContext],
        name: str,
        kind: SpanKind
    ) -> bool:
        # 使用trace_id进行确定性采样
        hash_value = int(hashlib.md5(trace_id.encode()).hexdigest(), 16)
        return (hash_value % 10000) < (self.probability * 10000)


class RateLimitingSampler(Sampler):
    """速率限制采样器"""
    
    def __init__(self, max_traces_per_second: float = 10.0):
        self.max_traces_per_second = max_traces_per_second
        self._tokens = max_traces_per_second
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def should_sample(
        self,
        trace_id: str,
        parent_context: Optional[TraceContext],
        name: str,
        kind: SpanKind
    ) -> bool:
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(
                self.max_traces_per_second,
                self._tokens + elapsed * self.max_traces_per_second
            )
            self._last_update = now
            
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class ParentBasedSampler(Sampler):
    """基于父Span的采样器"""
    
    def __init__(
        self,
        root_sampler: Optional[Sampler] = None,
        remote_parent_sampled: Optional[Sampler] = None,
        remote_parent_not_sampled: Optional[Sampler] = None,
        local_parent_sampled: Optional[Sampler] = None,
        local_parent_not_sampled: Optional[Sampler] = None,
    ):
        self.root_sampler = root_sampler or ProbabilisticSampler(0.1)
        self.remote_parent_sampled = remote_parent_sampled or AlwaysOnSampler()
        self.remote_parent_not_sampled = remote_parent_not_sampled or AlwaysOffSampler()
        self.local_parent_sampled = local_parent_sampled or AlwaysOnSampler()
        self.local_parent_not_sampled = local_parent_not_sampled or AlwaysOffSampler()
    
    def should_sample(
        self,
        trace_id: str,
        parent_context: Optional[TraceContext],
        name: str,
        kind: SpanKind
    ) -> bool:
        if parent_context is None:
            return self.root_sampler.should_sample(trace_id, parent_context, name, kind)
        
        # 简化处理，实际应区分remote/local
        if parent_context.sampled:
            return self.local_parent_sampled.should_sample(trace_id, parent_context, name, kind)
        else:
            return self.local_parent_not_sampled.should_sample(trace_id, parent_context, name, kind)


class TraceExporter(ABC):
    """追踪导出器抽象基类"""
    
    @abstractmethod
    def export(self, spans: List[Span]) -> bool:
        """导出Span列表"""
        pass
    
    @abstractmethod
    def shutdown(self) -> None:
        """关闭导出器"""
        pass


class ConsoleExporter(TraceExporter):
    """控制台导出器"""
    
    def __init__(self, pretty_print: bool = True):
        self.pretty_print = pretty_print
    
    def export(self, spans: List[Span]) -> bool:
        """导出到控制台"""
        for span in spans:
            if self.pretty_print:
                print(json.dumps(span.to_dict(), indent=2))
            else:
                print(json.dumps(span.to_dict()))
        return True
    
    def shutdown(self) -> None:
        pass


class InMemoryExporter(TraceExporter):
    """内存导出器(用于测试)"""
    
    def __init__(self, max_spans: int = 10000):
        self.spans: deque = deque(maxlen=max_spans)
    
    def export(self, spans: List[Span]) -> bool:
        """导出到内存"""
        self.spans.extend(spans)
        return True
    
    def get_spans(self) -> List[Span]:
        """获取所有Span"""
        return list(self.spans)
    
    def clear(self) -> None:
        """清空Span"""
        self.spans.clear()
    
    def shutdown(self) -> None:
        pass


class JaegerExporter(TraceExporter):
    """Jaeger导出器"""
    
    def __init__(
        self,
        agent_host: str = "localhost",
        agent_port: int = 6831,
        service_name: str = "agent-memory-system"
    ):
        self.agent_host = agent_host
        self.agent_port = agent_port
        self.service_name = service_name
        self._buffer: List[Span] = []
        self._lock = threading.Lock()
    
    def export(self, spans: List[Span]) -> bool:
        """导出到Jaeger"""
        try:
            # 这里简化处理，实际应使用Jaeger Thrift协议
            with self._lock:
                self._buffer.extend(spans)
                
                # 批量发送
                if len(self._buffer) >= 100:
                    self._flush()
            
            return True
        except Exception as e:
            logger.error(f"Failed to export to Jaeger: {e}")
            return False
    
    def _flush(self) -> None:
        """刷新缓冲区"""
        # 实际实现应使用UDP发送Thrift格式数据到Jaeger Agent
        logger.debug(f"Flushing {len(self._buffer)} spans to Jaeger")
        self._buffer.clear()
    
    def shutdown(self) -> None:
        """关闭导出器"""
        with self._lock:
            if self._buffer:
                self._flush()


class ZipkinExporter(TraceExporter):
    """Zipkin导出器"""
    
    def __init__(
        self,
        endpoint: str = "http://localhost:9411/api/v2/spans",
        service_name: str = "agent-memory-system"
    ):
        self.endpoint = endpoint
        self.service_name = service_name
        self._buffer: List[Span] = []
        self._lock = threading.Lock()
    
    def export(self, spans: List[Span]) -> bool:
        """导出到Zipkin"""
        try:
            with self._lock:
                self._buffer.extend(spans)
                
                if len(self._buffer) >= 100:
                    self._flush()
            
            return True
        except Exception as e:
            logger.error(f"Failed to export to Zipkin: {e}")
            return False
    
    def _flush(self) -> None:
        """刷新缓冲区"""
        # 实际实现应发送HTTP POST请求到Zipkin API
        logger.debug(f"Flushing {len(self._buffer)} spans to Zipkin")
        self._buffer.clear()
    
    def shutdown(self) -> None:
        """关闭导出器"""
        with self._lock:
            if self._buffer:
                self._flush()


# 当前追踪上下文
_current_context: ContextVar[Optional[TraceContext]] = ContextVar('trace_context', default=None)


class Tracer:
    """追踪器"""
    
    def __init__(
        self,
        service_name: str = "agent-memory-system",
        sampler: Optional[Sampler] = None,
        exporter: Optional[TraceExporter] = None,
        batch_size: int = 100,
        export_interval_ms: float = 5000,
    ):
        self.service_name = service_name
        self.sampler = sampler or ProbabilisticSampler(0.1)
        self.exporter = exporter or ConsoleExporter()
        self.batch_size = batch_size
        self.export_interval_ms = export_interval_ms
        
        self._span_buffer: List[Span] = []
        self._active_spans: Dict[str, Span] = {}
        self._lock = threading.Lock()
        self._shutdown = False
        
        # 启动导出线程
        self._export_thread = threading.Thread(target=self._export_loop, daemon=True)
        self._export_thread.start()
    
    def _export_loop(self) -> None:
        """导出循环"""
        while not self._shutdown:
            time.sleep(self.export_interval_ms / 1000)
            self._flush_spans()
    
    def _flush_spans(self) -> None:
        """刷新Span缓冲区"""
        with self._lock:
            if self._span_buffer:
                spans_to_export = self._span_buffer[:]
                self._span_buffer.clear()
                
                try:
                    self.exporter.export(spans_to_export)
                except Exception as e:
                    logger.error(f"Failed to export spans: {e}")
    
    def start_span(
        self,
        name: str,
        kind: SpanKind = SpanKind.INTERNAL,
        parent_context: Optional[TraceContext] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Span, TraceContext]:
        """开始一个新的Span"""
        # 获取或创建父上下文
        if parent_context is None:
            parent_context = _current_context.get()
        
        # 创建trace_id和span_id
        if parent_context:
            trace_id = parent_context.trace_id
            parent_span_id = parent_context.span_id
            sampled = parent_context.sampled
        else:
            trace_id = format(uuid.uuid4().int, '032x')
            parent_span_id = None
            sampled = self.sampler.should_sample(trace_id, None, name, kind)
        
        span_id = format(uuid.uuid4().int, '016x')
        
        # 创建Span
        span = Span(
            span_id=span_id,
            trace_id=trace_id,
            parent_id=parent_span_id,
            name=name,
            kind=kind,
        )
        
        # 设置属性
        span.set_attribute("service.name", self.service_name)
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        
        # 创建新的上下文
        new_context = TraceContext(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            sampled=sampled,
        )
        
        # 保存活动Span
        with self._lock:
            self._active_spans[span_id] = span
        
        # 设置当前上下文
        _current_context.set(new_context)
        
        return span, new_context
    
    def end_span(self, span: Span, status: Optional[SpanStatus] = None) -> None:
        """结束Span"""
        span.end()
        
        if status:
            span.set_status(status)
        
        with self._lock:
            # 从活动Span中移除
            if span.span_id in self._active_spans:
                del self._active_spans[span.span_id]
            
            # 添加到缓冲区
            if span.status != SpanStatus.ERROR or span.attributes.get("error.record", True):
                self._span_buffer.append(span)
                
                # 如果缓冲区满了，立即刷新
                if len(self._span_buffer) >= self.batch_size:
                    self._flush_spans()
    
    def get_current_context(self) -> Optional[TraceContext]:
        """获取当前追踪上下文"""
        return _current_context.get()
    
    def set_current_context(self, context: Optional[TraceContext]) -> None:
        """设置当前追踪上下文"""
        _current_context.set(context)
    
    def create_child_context(self, parent_context: TraceContext) -> TraceContext:
        """创建子上下文"""
        return TraceContext(
            trace_id=parent_context.trace_id,
            span_id=format(uuid.uuid4().int, '016x'),
            parent_span_id=parent_context.span_id,
            sampled=parent_context.sampled,
            baggage=parent_context.baggage.copy(),
        )
    
    def inject_context(self, carrier: Dict[str, str], context: Optional[TraceContext] = None) -> None:
        """将上下文注入到carrier(用于传播)"""
        if context is None:
            context = _current_context.get()
        
        if context:
            carrier["traceparent"] = f"00-{context.trace_id}-{context.span_id}-{'01' if context.sampled else '00'}"
            for key, value in context.baggage.items():
                carrier[f"baggage-{key}"] = value
    
    def extract_context(self, carrier: Dict[str, str]) -> Optional[TraceContext]:
        """从carrier提取上下文"""
        traceparent = carrier.get("traceparent", "")
        parts = traceparent.split("-")
        
        if len(parts) >= 4:
            trace_id = parts[1]
            span_id = parts[2]
            sampled = parts[3] == "01"
            
            # 提取baggage
            baggage = {}
            for key, value in carrier.items():
                if key.startswith("baggage-"):
                    baggage[key[8:]] = value
            
            return TraceContext(
                trace_id=trace_id,
                span_id=span_id,
                sampled=sampled,
                baggage=baggage,
            )
        
        return None
    
    def shutdown(self) -> None:
        """关闭追踪器"""
        self._shutdown = True
        self._flush_spans()
        self.exporter.shutdown()


class TraceAnalyzer:
    """追踪分析器"""
    
    def __init__(self, tracer: Tracer):
        self.tracer = tracer
    
    def find_bottlenecks(self, trace_id: str, spans: List[Span]) -> List[Dict[str, Any]]:
        """查找性能瓶颈"""
        # 按持续时间排序
        sorted_spans = sorted(spans, key=lambda s: s.duration_ms(), reverse=True)
        
        bottlenecks = []
        for span in sorted_spans[:5]:  # 前5个最慢的Span
            bottlenecks.append({
                "span_id": span.span_id,
                "name": span.name,
                "duration_ms": span.duration_ms(),
                "attributes": span.attributes,
            })
        
        return bottlenecks
    
    def calculate_critical_path(self, spans: List[Span]) -> List[Span]:
        """计算关键路径"""
        # 构建Span树
        span_map = {s.span_id: s for s in spans}
        children: Dict[str, List[Span]] = defaultdict(list)
        
        root = None
        for span in spans:
            if span.parent_id:
                children[span.parent_id].append(span)
            else:
                root = span
        
        # 找到持续时间最长的路径
        critical_path = []
        current = root
        
        while current:
            critical_path.append(current)
            
            # 找到子Span中持续时间最长的
            child_spans = children.get(current.span_id, [])
            if child_spans:
                current = max(child_spans, key=lambda s: s.duration_ms())
            else:
                current = None
        
        return critical_path
    
    def analyze_error_rate(self, spans: List[Span]) -> Dict[str, Any]:
        """分析错误率"""
        total = len(spans)
        errors = sum(1 for s in spans if s.status == SpanStatus.ERROR)
        
        error_spans = [s for s in spans if s.status == SpanStatus.ERROR]
        error_breakdown: Dict[str, int] = defaultdict(int)
        
        for span in error_spans:
            error_type = span.attributes.get("error.type", "unknown")
            error_breakdown[error_type] += 1
        
        return {
            "total_spans": total,
            "error_count": errors,
            "error_rate": errors / total if total > 0 else 0,
            "error_breakdown": dict(error_breakdown),
        }
    
    def generate_service_graph(self, spans: List[Span]) -> Dict[str, Any]:
        """生成服务调用图"""
        nodes: Set[str] = set()
        edges: List[Dict[str, str]] = []
        
        span_map = {s.span_id: s for s in spans}
        
        for span in spans:
            source = span.attributes.get("service.name", "unknown")
            nodes.add(source)
            
            if span.parent_id and span.parent_id in span_map:
                parent = span_map[span.parent_id]
                target = parent.attributes.get("service.name", "unknown")
                nodes.add(target)
                edges.append({
                    "source": source,
                    "target": target,
                    "operation": span.name,
                })
        
        return {
            "nodes": list(nodes),
            "edges": edges,
        }


class DistributedTracingSystem:
    """分布式追踪系统主类"""
    
    def __init__(self):
        self._tracers: Dict[str, Tracer] = {}
        self._lock = threading.RLock()
        self._analyzers: Dict[str, TraceAnalyzer] = {}
    
    def create_tracer(
        self,
        service_name: str,
        sampler: Optional[Sampler] = None,
        exporter: Optional[TraceExporter] = None,
        **kwargs
    ) -> Tracer:
        """创建追踪器"""
        with self._lock:
            tracer = Tracer(
                service_name=service_name,
                sampler=sampler,
                exporter=exporter,
                **kwargs
            )
            self._tracers[service_name] = tracer
            self._analyzers[service_name] = TraceAnalyzer(tracer)
            return tracer
    
    def get_tracer(self, service_name: str) -> Optional[Tracer]:
        """获取追踪器"""
        with self._lock:
            return self._tracers.get(service_name)
    
    def shutdown_all(self) -> None:
        """关闭所有追踪器"""
        with self._lock:
            for tracer in self._tracers.values():
                tracer.shutdown()
            self._tracers.clear()
            self._analyzers.clear()


# 全局追踪系统实例
_tracing_system: Optional[DistributedTracingSystem] = None


def get_tracing_system() -> DistributedTracingSystem:
    """获取全局追踪系统实例"""
    global _tracing_system
    if _tracing_system is None:
        _tracing_system = DistributedTracingSystem()
    return _tracing_system


# 装饰器
def traced(
    name: Optional[str] = None,
    kind: SpanKind = SpanKind.INTERNAL,
    attributes: Optional[Dict[str, Any]] = None
):
    """追踪装饰器"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracing_system().get_tracer("default")
            if tracer is None:
                return func(*args, **kwargs)
            
            span_name = name or func.__name__
            span, context = tracer.start_span(
                name=span_name,
                kind=kind,
                attributes=attributes
            )
            
            try:
                # 添加函数参数作为属性
                for i, arg in enumerate(args):
                    span.set_attribute(f"arg.{i}", str(arg))
                for key, value in kwargs.items():
                    span.set_attribute(f"kwarg.{key}", str(value))
                
                result = func(*args, **kwargs)
                span.set_status(SpanStatus.OK)
                return result
            except Exception as e:
                span.set_status(SpanStatus.ERROR, str(e))
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                raise
            finally:
                tracer.end_span(span)
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            tracer = get_tracing_system().get_tracer("default")
            if tracer is None:
                return await func(*args, **kwargs)
            
            span_name = name or func.__name__
            span, context = tracer.start_span(
                name=span_name,
                kind=kind,
                attributes=attributes
            )
            
            try:
                for i, arg in enumerate(args):
                    span.set_attribute(f"arg.{i}", str(arg))
                for key, value in kwargs.items():
                    span.set_attribute(f"kwarg.{key}", str(value))
                
                result = await func(*args, **kwargs)
                span.set_status(SpanStatus.OK)
                return result
            except Exception as e:
                span.set_status(SpanStatus.ERROR, str(e))
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                raise
            finally:
                tracer.end_span(span)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator


@contextmanager
def start_span_context(
    name: str,
    kind: SpanKind = SpanKind.INTERNAL,
    tracer_name: str = "default",
    attributes: Optional[Dict[str, Any]] = None
) -> Iterator[Span]:
    """Span上下文管理器"""
    tracer = get_tracing_system().get_tracer(tracer_name)
    if tracer is None:
        # 创建一个虚拟Span
        span = Span(span_id="", trace_id="", name=name)
        yield span
        return
    
    span, context = tracer.start_span(name=name, kind=kind, attributes=attributes)
    
    try:
        yield span
        span.set_status(SpanStatus.OK)
    except Exception as e:
        span.set_status(SpanStatus.ERROR, str(e))
        span.set_attribute("error.type", type(e).__name__)
        raise
    finally:
        tracer.end_span(span)


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    # 创建追踪系统
    tracing_system = get_tracing_system()
    
    # 创建内存导出器用于演示
    exporter = InMemoryExporter()
    
    # 创建追踪器
    tracer = tracing_system.create_tracer(
        service_name="demo-service",
        sampler=AlwaysOnSampler(),
        exporter=exporter,
        batch_size=10
    )
    
    print("=== Distributed Tracing Demo ===\n")
    
    # 示例1: 基本Span创建
    print("1. Basic Span Creation:")
    span1, ctx1 = tracer.start_span(
        name="process-request",
        kind=SpanKind.SERVER,
        attributes={"http.method": "GET", "http.url": "/api/data"}
    )
    
    time.sleep(0.01)  # 模拟工作
    span1.add_event("processing_started")
    time.sleep(0.02)
    span1.add_event("processing_completed")
    tracer.end_span(span1)
    
    print(f"   Created span: {span1.name}, duration: {span1.duration_ms():.2f}ms")
    
    # 示例2: 嵌套Span
    print("\n2. Nested Spans:")
    parent_span, parent_ctx = tracer.start_span(
        name="parent-operation",
        kind=SpanKind.INTERNAL
    )
    
    # 子Span
    child_span, child_ctx = tracer.start_span(
        name="child-operation",
        kind=SpanKind.INTERNAL,
        parent_context=parent_ctx
    )
    time.sleep(0.01)
    tracer.end_span(child_span)
    
    tracer.end_span(parent_span)
    
    print(f"   Parent: {parent_span.name}, duration: {parent_span.duration_ms():.2f}ms")
    print(f"   Child: {child_span.name}, duration: {child_span.duration_ms():.2f}ms")
    
    # 示例3: 使用装饰器
    print("\n3. Using Decorator:")
    
    @traced(name="decorated-function", attributes={"custom.attr": "value"})
    def example_function(x: int, y: int) -> int:
        time.sleep(0.01)
        return x + y
    
    result = example_function(10, 20)
    print(f"   Function result: {result}")
    
    # 示例4: 上下文传播
    print("\n4. Context Propagation:")
    carrier: Dict[str, str] = {}
    tracer.inject_context(carrier)
    print(f"   Injected context: {carrier}")
    
    extracted = tracer.extract_context(carrier)
    if extracted:
        print(f"   Extracted trace_id: {extracted.trace_id}")
    
    # 示例5: 分析
    print("\n5. Trace Analysis:")
    analyzer = TraceAnalyzer(tracer)
    spans = exporter.get_spans()
    
    print(f"   Total spans collected: {len(spans)}")
    
    if spans:
        error_analysis = analyzer.analyze_error_rate(spans)
        print(f"   Error rate: {error_analysis['error_rate']:.2%}")
        
        service_graph = analyzer.generate_service_graph(spans)
        print(f"   Services: {service_graph['nodes']}")
    
    # 清理
    tracer.shutdown()
    tracing_system.shutdown_all()
    
    print("\n=== Distributed Tracing Demo Completed ===")

"""
深度测试: distributed_tracing.py 模块

覆盖 P1 盲区模块 - 分布式追踪系统
测试 Span, Trace, TraceContext, Sampler, TraceExporter, Tracer, TraceAnalyzer,
traced 装饰器, start_span_context 上下文管理器
"""

import sys
import time
import json
import pytest
from datetime import datetime, timedelta

sys.path.insert(0, '/workspace/agent_memory_system_v2')

from memory_system.distributed_tracing import (
    SpanKind,
    SpanStatus,
    SamplingStrategy,
    TraceContext,
    Span,
    SpanEvent,
    SpanLink,
    Trace,
    Sampler,
    AlwaysOnSampler,
    AlwaysOffSampler,
    ProbabilisticSampler,
    RateLimitingSampler,
    ParentBasedSampler,
    TraceExporter,
    InMemoryExporter,
    ConsoleExporter,
    Tracer,
    TraceAnalyzer,
    traced,
    start_span_context,
    get_tracing_system,
    DistributedTracingSystem,
)


# ============================================================
# 1. Span 创建和结束
# ============================================================
class TestSpanCreation:

    def test_create_span_basic(self):
        """基本 Span 创建"""
        span = Span(
            span_id="span_001",
            trace_id="trace_001",
            name="test_operation",
        )
        assert span.span_id == "span_001"
        assert span.trace_id == "trace_001"
        assert span.name == "test_operation"
        assert span.parent_id is None
        assert span.kind == SpanKind.INTERNAL
        assert span.status == SpanStatus.UNSET
        assert span.end_time is None
        assert len(span.attributes) == 0
        assert len(span.events) == 0
        assert len(span.links) == 0

    def test_create_span_with_parent(self):
        """创建带有父 Span 的 Span"""
        span = Span(
            span_id="child_001",
            trace_id="trace_001",
            parent_id="parent_001",
            name="child_operation",
        )
        assert span.parent_id == "parent_001"

    def test_create_span_with_kind(self):
        """创建不同类型的 Span"""
        for kind in SpanKind:
            span = Span(span_id="s1", trace_id="t1", kind=kind)
            assert span.kind == kind

    def test_end_span(self):
        """结束 Span"""
        span = Span(span_id="s1", trace_id="t1")
        assert span.end_time is None
        span.end()
        assert span.end_time is not None
        assert isinstance(span.end_time, datetime)

    def test_end_span_with_custom_time(self):
        """使用自定义时间结束 Span"""
        span = Span(span_id="s1", trace_id="t1")
        custom_time = datetime(2025, 1, 1, 12, 0, 0)
        span.end(end_time=custom_time)
        assert span.end_time == custom_time


# ============================================================
# 2. Span 持续时间计算
# ============================================================
class TestSpanDuration:

    def test_duration_before_end(self):
        """未结束的 Span 持续时间应为正数"""
        span = Span(span_id="s1", trace_id="t1")
        time.sleep(0.01)
        duration = span.duration_ms()
        assert duration >= 10.0  # 至少 10ms

    def test_duration_after_end(self):
        """已结束的 Span 持续时间应固定"""
        start = datetime(2025, 1, 1, 12, 0, 0)
        end = datetime(2025, 1, 1, 12, 0, 0, 500000)  # 500ms 后
        span = Span(span_id="s1", trace_id="t1", start_time=start)
        span.end(end_time=end)
        assert span.duration_ms() == pytest.approx(500.0, abs=1.0)

    def test_duration_zero_for_instant_span(self):
        """瞬时 Span 持续时间应接近 0"""
        now = datetime.utcnow()
        span = Span(span_id="s1", trace_id="t1", start_time=now)
        span.end(end_time=now)
        assert span.duration_ms() == pytest.approx(0.0, abs=1.0)


# ============================================================
# 3. Span 属性设置
# ============================================================
class TestSpanAttributes:

    def test_set_attribute_string(self):
        """设置字符串属性"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_attribute("http.method", "GET")
        assert span.attributes["http.method"] == "GET"

    def test_set_attribute_numeric(self):
        """设置数值属性"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_attribute("http.status_code", 200)
        assert span.attributes["http.status_code"] == 200

    def test_set_attribute_overwrite(self):
        """覆盖已有属性"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_attribute("key", "value1")
        span.set_attribute("key", "value2")
        assert span.attributes["key"] == "value2"

    def test_set_multiple_attributes(self):
        """设置多个属性"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_attribute("a", 1)
        span.set_attribute("b", 2)
        span.set_attribute("c", 3)
        assert len(span.attributes) == 3


# ============================================================
# 4. Span 事件添加
# ============================================================
class TestSpanEvents:

    def test_add_event_basic(self):
        """添加基本事件"""
        span = Span(span_id="s1", trace_id="t1")
        span.add_event("cache_miss")
        assert len(span.events) == 1
        assert span.events[0].name == "cache_miss"

    def test_add_event_with_attributes(self):
        """添加带属性的事件"""
        span = Span(span_id="s1", trace_id="t1")
        span.add_event("db_query", {"sql": "SELECT * FROM users"})
        assert len(span.events) == 1
        assert span.events[0].attributes["sql"] == "SELECT * FROM users"

    def test_add_multiple_events(self):
        """添加多个事件"""
        span = Span(span_id="s1", trace_id="t1")
        span.add_event("event_1")
        span.add_event("event_2")
        span.add_event("event_3")
        assert len(span.events) == 3

    def test_event_has_timestamp(self):
        """事件应有时间戳"""
        span = Span(span_id="s1", trace_id="t1")
        before = datetime.utcnow()
        span.add_event("test_event")
        after = datetime.utcnow()
        assert before <= span.events[0].timestamp <= after


# ============================================================
# 5. Span 链接添加
# ============================================================
class TestSpanLinks:

    def test_add_link_basic(self):
        """添加基本链接"""
        span = Span(span_id="s1", trace_id="t1")
        span.add_link("other_trace_id", "other_span_id")
        assert len(span.links) == 1
        assert span.links[0].trace_id == "other_trace_id"
        assert span.links[0].span_id == "other_span_id"

    def test_add_link_with_attributes(self):
        """添加带属性的链接"""
        span = Span(span_id="s1", trace_id="t1")
        span.add_link("t2", "s2", {"link_type": "causal"})
        assert span.links[0].attributes["link_type"] == "causal"

    def test_add_multiple_links(self):
        """添加多个链接"""
        span = Span(span_id="s1", trace_id="t1")
        span.add_link("t1", "s1")
        span.add_link("t2", "s2")
        assert len(span.links) == 2


# ============================================================
# 6. Span 状态设置
# ============================================================
class TestSpanStatus:

    def test_set_status_ok(self):
        """设置 OK 状态"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_status(SpanStatus.OK)
        assert span.status == SpanStatus.OK

    def test_set_status_error(self):
        """设置 ERROR 状态"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_status(SpanStatus.ERROR, "connection timeout")
        assert span.status == SpanStatus.ERROR
        assert span.attributes.get("status.description") == "connection timeout"

    def test_set_status_without_description(self):
        """设置状态不带描述"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_status(SpanStatus.ERROR)
        assert "status.description" not in span.attributes

    def test_set_status_unset(self):
        """设置 UNSET 状态"""
        span = Span(span_id="s1", trace_id="t1")
        span.set_status(SpanStatus.UNSET)
        assert span.status == SpanStatus.UNSET


# ============================================================
# 7. TraceContext 序列化/反序列化
# ============================================================
class TestTraceContext:

    def test_to_dict_basic(self):
        """基本序列化"""
        ctx = TraceContext(trace_id="t1", span_id="s1")
        d = ctx.to_dict()
        assert d["trace_id"] == "t1"
        assert d["span_id"] == "s1"
        assert d["parent_span_id"] is None
        assert d["sampled"] is True
        assert d["baggage"] == {}

    def test_to_dict_full(self):
        """完整序列化"""
        ctx = TraceContext(
            trace_id="t1",
            span_id="s1",
            parent_span_id="p1",
            sampled=False,
            baggage={"user_id": "123", "tenant": "acme"},
        )
        d = ctx.to_dict()
        assert d["trace_id"] == "t1"
        assert d["span_id"] == "s1"
        assert d["parent_span_id"] == "p1"
        assert d["sampled"] is False
        assert d["baggage"]["user_id"] == "123"
        assert d["baggage"]["tenant"] == "acme"

    def test_from_dict_basic(self):
        """基本反序列化"""
        data = {
            "trace_id": "t1",
            "span_id": "s1",
            "parent_span_id": "p1",
            "sampled": True,
            "baggage": {"key": "value"},
        }
        ctx = TraceContext.from_dict(data)
        assert ctx.trace_id == "t1"
        assert ctx.span_id == "s1"
        assert ctx.parent_span_id == "p1"
        assert ctx.sampled is True
        assert ctx.baggage == {"key": "value"}

    def test_from_dict_defaults(self):
        """反序列化默认值"""
        data = {"trace_id": "t1", "span_id": "s1"}
        ctx = TraceContext.from_dict(data)
        assert ctx.parent_span_id is None
        assert ctx.sampled is True
        assert ctx.baggage == {}

    def test_roundtrip_serialization(self):
        """序列化-反序列化往返一致性"""
        original = TraceContext(
            trace_id="t_abc",
            span_id="s_xyz",
            parent_span_id="p_parent",
            sampled=False,
            baggage={"env": "prod"},
        )
        restored = TraceContext.from_dict(original.to_dict())
        assert restored.trace_id == original.trace_id
        assert restored.span_id == original.span_id
        assert restored.parent_span_id == original.parent_span_id
        assert restored.sampled == original.sampled
        assert restored.baggage == original.baggage


# ============================================================
# 8. Trace 根 Span 获取
# ============================================================
class TestTrace:

    def test_add_span(self):
        """添加 Span 到 Trace"""
        trace = Trace(trace_id="t1")
        span = Span(span_id="s1", trace_id="t1", name="op1")
        trace.add_span(span)
        assert len(trace.spans) == 1
        assert trace.spans[0].name == "op1"

    def test_get_root_span_no_parent(self):
        """获取根 Span（无父 Span）"""
        trace = Trace(trace_id="t1")
        root = Span(span_id="s1", trace_id="t1", name="root", parent_id=None)
        child = Span(span_id="s2", trace_id="t1", name="child", parent_id="s1")
        trace.add_span(root)
        trace.add_span(child)
        assert trace.get_root_span().span_id == "s1"

    def test_get_root_span_empty_trace(self):
        """空 Trace 获取根 Span 返回 None"""
        trace = Trace(trace_id="t1")
        assert trace.get_root_span() is None

    def test_get_root_span_first_if_all_have_parents(self):
        """所有 Span 都有父 Span 时返回第一个"""
        trace = Trace(trace_id="t1")
        s1 = Span(span_id="s1", trace_id="t1", parent_id="external")
        trace.add_span(s1)
        assert trace.get_root_span().span_id == "s1"

    def test_trace_duration_ms(self):
        """Trace 持续时间应为最大 Span 持续时间"""
        trace = Trace(trace_id="t1")
        s1 = Span(span_id="s1", trace_id="t1", start_time=datetime(2025, 1, 1))
        s1.end(end_time=datetime(2025, 1, 1, 0, 0, 0, 100000))  # 100ms
        s2 = Span(span_id="s2", trace_id="t1", start_time=datetime(2025, 1, 1))
        s2.end(end_time=datetime(2025, 1, 1, 0, 0, 0, 500000))  # 500ms
        trace.add_span(s1)
        trace.add_span(s2)
        assert trace.duration_ms() == pytest.approx(500.0, abs=1.0)

    def test_trace_duration_empty(self):
        """空 Trace 持续时间为 0"""
        trace = Trace(trace_id="t1")
        assert trace.duration_ms() == 0.0

    def test_trace_to_dict(self):
        """Trace 序列化"""
        trace = Trace(trace_id="t1")
        span = Span(span_id="s1", trace_id="t1", name="op1")
        span.end()
        trace.add_span(span)
        d = trace.to_dict()
        assert d["trace_id"] == "t1"
        assert d["span_count"] == 1
        assert "spans" in d
        assert "duration_ms" in d


# ============================================================
# 9. AlwaysOnSampler
# ============================================================
class TestAlwaysOnSampler:

    def test_always_on(self):
        """AlwaysOnSampler 应始终返回 True"""
        sampler = AlwaysOnSampler()
        assert sampler.should_sample("trace_1", None, "op", SpanKind.INTERNAL) is True
        assert sampler.should_sample("trace_2", None, "op", SpanKind.SERVER) is True

    def test_always_on_with_parent(self):
        """AlwaysOnSampler 有父上下文时也应返回 True"""
        sampler = AlwaysOnSampler()
        parent = TraceContext(trace_id="t1", span_id="s1", sampled=False)
        assert sampler.should_sample("trace_1", parent, "op", SpanKind.INTERNAL) is True


# ============================================================
# 10. AlwaysOffSampler
# ============================================================
class TestAlwaysOffSampler:

    def test_always_off(self):
        """AlwaysOffSampler 应始终返回 False"""
        sampler = AlwaysOffSampler()
        assert sampler.should_sample("trace_1", None, "op", SpanKind.INTERNAL) is False

    def test_always_off_with_parent(self):
        """AlwaysOffSampler 有父上下文时也应返回 False"""
        sampler = AlwaysOffSampler()
        parent = TraceContext(trace_id="t1", span_id="s1", sampled=True)
        assert sampler.should_sample("trace_1", parent, "op", SpanKind.INTERNAL) is False


# ============================================================
# 11. ProbabilisticSampler 概率
# ============================================================
class TestProbabilisticSampler:

    def test_probability_1_always_samples(self):
        """概率 1.0 应始终采样"""
        sampler = ProbabilisticSampler(1.0)
        for i in range(100):
            assert sampler.should_sample(f"trace_{i}", None, "op", SpanKind.INTERNAL) is True

    def test_probability_0_never_samples(self):
        """概率 0.0 应从不采样"""
        sampler = ProbabilisticSampler(0.0)
        for i in range(100):
            assert sampler.should_sample(f"trace_{i}", None, "op", SpanKind.INTERNAL) is False

    def test_probability_clamped(self):
        """概率值应被限制在 [0.0, 1.0]"""
        sampler_high = ProbabilisticSampler(2.0)
        assert sampler_high.probability == 1.0
        sampler_low = ProbabilisticSampler(-1.0)
        assert sampler_low.probability == 0.0

    def test_deterministic_sampling(self):
        """相同 trace_id 应产生相同采样结果"""
        sampler = ProbabilisticSampler(0.5)
        result1 = sampler.should_sample("fixed_trace_id", None, "op", SpanKind.INTERNAL)
        result2 = sampler.should_sample("fixed_trace_id", None, "op", SpanKind.INTERNAL)
        assert result1 == result2


# ============================================================
# 12. RateLimitingSampler 速率限制
# ============================================================
class TestRateLimitingSampler:

    def test_rate_limiting_allows_initial(self):
        """初始应允许采样"""
        sampler = RateLimitingSampler(max_traces_per_second=100.0)
        assert sampler.should_sample("t1", None, "op", SpanKind.INTERNAL) is True

    def test_rate_limiting_exhausts_tokens(self):
        """超出速率限制后应拒绝"""
        sampler = RateLimitingSampler(max_traces_per_second=2.0)
        # 快速消耗令牌
        results = [sampler.should_sample(f"t{i}", None, "op", SpanKind.INTERNAL) for i in range(10)]
        # 至少有一些被拒绝
        assert False in results

    def test_rate_limiting_refills(self):
        """令牌应随时间补充"""
        sampler = RateLimitingSampler(max_traces_per_second=1000.0)
        # 消耗令牌
        for i in range(50):
            sampler.should_sample(f"t{i}", None, "op", SpanKind.INTERNAL)
        time.sleep(0.05)  # 等待补充
        assert sampler.should_sample("t_new", None, "op", SpanKind.INTERNAL) is True


# ============================================================
# 13. ParentBasedSampler
# ============================================================
class TestParentBasedSampler:

    def test_no_parent_uses_root_sampler(self):
        """无父上下文时使用根采样器"""
        sampler = ParentBasedSampler(root_sampler=AlwaysOnSampler())
        assert sampler.should_sample("t1", None, "op", SpanKind.INTERNAL) is True

    def test_parent_sampled_uses_local_parent_sampled(self):
        """父上下文已采样时使用 local_parent_sampled"""
        sampler = ParentBasedSampler(
            local_parent_sampled=AlwaysOnSampler(),
            local_parent_not_sampled=AlwaysOffSampler(),
        )
        parent = TraceContext(trace_id="t1", span_id="s1", sampled=True)
        assert sampler.should_sample("t1", parent, "op", SpanKind.INTERNAL) is True

    def test_parent_not_sampled_uses_local_parent_not_sampled(self):
        """父上下文未采样时使用 local_parent_not_sampled"""
        sampler = ParentBasedSampler(
            local_parent_sampled=AlwaysOnSampler(),
            local_parent_not_sampled=AlwaysOffSampler(),
        )
        parent = TraceContext(trace_id="t1", span_id="s1", sampled=False)
        assert sampler.should_sample("t1", parent, "op", SpanKind.INTERNAL) is False


# ============================================================
# 14. InMemoryExporter
# ============================================================
class TestInMemoryExporter:

    def test_export_and_get_spans(self):
        """导出并获取 Span"""
        exporter = InMemoryExporter()
        span1 = Span(span_id="s1", trace_id="t1", name="op1")
        span2 = Span(span_id="s2", trace_id="t1", name="op2")
        exporter.export([span1, span2])
        spans = exporter.get_spans()
        assert len(spans) == 2
        assert spans[0].span_id == "s1"
        assert spans[1].span_id == "s2"

    def test_clear_spans(self):
        """清空 Span"""
        exporter = InMemoryExporter()
        exporter.export([Span(span_id="s1", trace_id="t1")])
        exporter.clear()
        assert len(exporter.get_spans()) == 0

    def test_max_spans_limit(self):
        """超过最大 Span 数量时应丢弃旧的"""
        exporter = InMemoryExporter(max_spans=3)
        for i in range(5):
            exporter.export([Span(span_id=f"s{i}", trace_id="t1")])
        spans = exporter.get_spans()
        assert len(spans) == 3
        # 应保留最新的
        assert spans[-1].span_id == "s4"

    def test_export_returns_true(self):
        """export 应返回 True"""
        exporter = InMemoryExporter()
        assert exporter.export([Span(span_id="s1", trace_id="t1")]) is True

    def test_shutdown(self):
        """shutdown 不应抛出异常"""
        exporter = InMemoryExporter()
        exporter.shutdown()  # 不应抛出


# ============================================================
# 15. Tracer 上下文传播
# ============================================================
class TestTracerContextPropagation:

    def test_start_span_sets_current_context(self):
        """start_span 应设置当前上下文"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        span, ctx = tracer.start_span("test_op")
        current = tracer.get_current_context()
        assert current is not None
        assert current.trace_id == ctx.trace_id
        assert current.span_id == ctx.span_id
        tracer.shutdown()

    def test_nested_spans_share_trace_id(self):
        """嵌套 Span 应共享 trace_id"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        parent_span, parent_ctx = tracer.start_span("parent")
        child_span, child_ctx = tracer.start_span("child", parent_context=parent_ctx)
        assert parent_span.trace_id == child_span.trace_id
        assert child_span.parent_id == parent_span.span_id
        tracer.shutdown()

    def test_set_current_context(self):
        """手动设置当前上下文"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        ctx = TraceContext(trace_id="manual_trace", span_id="manual_span")
        tracer.set_current_context(ctx)
        current = tracer.get_current_context()
        assert current.trace_id == "manual_trace"
        tracer.shutdown()

    def test_set_current_context_none(self):
        """设置上下文为 None"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        tracer.start_span("op1")
        tracer.set_current_context(None)
        assert tracer.get_current_context() is None
        tracer.shutdown()


# ============================================================
# 16. Tracer 子上下文创建
# ============================================================
class TestTracerChildContext:

    def test_create_child_context(self):
        """创建子上下文"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        parent_ctx = TraceContext(
            trace_id="t1",
            span_id="s1",
            sampled=True,
            baggage={"key": "value"},
        )
        child_ctx = tracer.create_child_context(parent_ctx)
        assert child_ctx.trace_id == parent_ctx.trace_id
        assert child_ctx.parent_span_id == parent_ctx.span_id
        assert child_ctx.sampled == parent_ctx.sampled
        assert child_ctx.baggage == parent_ctx.baggage
        assert child_ctx.span_id != parent_ctx.span_id
        tracer.shutdown()

    def test_child_context_independent_baggage(self):
        """子上下文的 baggage 应是独立副本"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        parent_ctx = TraceContext(trace_id="t1", span_id="s1", baggage={"k": "v"})
        child_ctx = tracer.create_child_context(parent_ctx)
        child_ctx.baggage["new_key"] = "new_value"
        assert "new_key" not in parent_ctx.baggage
        tracer.shutdown()


# ============================================================
# 17. Tracer 注入/提取上下文
# ============================================================
class TestTracerInjectExtract:

    def test_inject_context(self):
        """注入上下文到 carrier"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        span, ctx = tracer.start_span("test_op")
        carrier = {}
        tracer.inject_context(carrier)
        assert "traceparent" in carrier
        assert carrier["traceparent"].startswith("00-")
        tracer.shutdown()

    def test_inject_with_baggage(self):
        """注入带 baggage 的上下文"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        ctx = TraceContext(
            trace_id="t1",
            span_id="s1",
            baggage={"user_id": "123"},
        )
        tracer.set_current_context(ctx)
        carrier = {}
        tracer.inject_context(carrier)
        assert "baggage-user_id" in carrier
        assert carrier["baggage-user_id"] == "123"
        tracer.shutdown()

    def test_extract_context(self):
        """从 carrier 提取上下文"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        carrier = {
            "traceparent": "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01",
            "baggage-env": "production",
        }
        ctx = tracer.extract_context(carrier)
        assert ctx is not None
        assert ctx.trace_id == "abcdef1234567890abcdef1234567890"
        assert ctx.span_id == "1234567890abcdef"
        assert ctx.sampled is True
        assert ctx.baggage["env"] == "production"
        tracer.shutdown()

    def test_extract_invalid_carrier(self):
        """提取无效 carrier 返回 None"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        ctx = tracer.extract_context({})
        assert ctx is None
        tracer.shutdown()

    def test_extract_not_sampled(self):
        """提取未采样的上下文"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        carrier = {
            "traceparent": "00-abcdef1234567890abcdef1234567890-1234567890abcdef-00",
        }
        ctx = tracer.extract_context(carrier)
        assert ctx is not None
        assert ctx.sampled is False
        tracer.shutdown()

    def test_inject_extract_roundtrip(self):
        """注入-提取往返一致性"""
        tracer = Tracer(service_name="test", sampler=AlwaysOnSampler(), exporter=InMemoryExporter())
        span, ctx = tracer.start_span("test_op")
        carrier = {}
        tracer.inject_context(carrier)
        extracted = tracer.extract_context(carrier)
        assert extracted.trace_id == ctx.trace_id
        assert extracted.span_id == ctx.span_id
        assert extracted.sampled == ctx.sampled
        tracer.shutdown()


# ============================================================
# 18. TraceAnalyzer 瓶颈检测
# ============================================================
class TestTraceAnalyzer:

    def setup_method(self):
        """每个测试前重置全局上下文"""
        import memory_system.distributed_tracing as dt_module
        dt_module._current_context.set(None)

    def _make_tracer_and_spans(self):
        """创建 Tracer 和测试 Span"""
        exporter = InMemoryExporter()
        tracer = Tracer(
            service_name="test-svc",
            sampler=AlwaysOnSampler(),
            exporter=exporter,
            export_interval_ms=60000,  # 长间隔避免自动刷新干扰
        )
        return tracer, exporter

    def _flush(self, tracer):
        """手动刷新 Tracer 的 span 缓冲区"""
        tracer._flush_spans()

    def test_find_bottlenecks(self):
        """查找性能瓶颈 - 应返回最慢的 Span"""
        tracer, exporter = self._make_tracer_and_spans()

        # 创建不同持续时间的 Span
        s1, _ = tracer.start_span("fast_op")
        time.sleep(0.005)
        tracer.end_span(s1)

        s2, _ = tracer.start_span("slow_op")
        time.sleep(0.02)
        tracer.end_span(s2)

        s3, _ = tracer.start_span("medium_op")
        time.sleep(0.01)
        tracer.end_span(s3)

        self._flush(tracer)

        analyzer = TraceAnalyzer(tracer)
        spans = exporter.get_spans()
        bottlenecks = analyzer.find_bottlenecks("t1", spans)

        assert len(bottlenecks) <= 5
        # 最慢的应是 slow_op
        assert bottlenecks[0]["name"] == "slow_op"
        tracer.shutdown()

    def test_find_bottlenecks_empty(self):
        """空 Span 列表不应报错"""
        tracer, exporter = self._make_tracer_and_spans()
        analyzer = TraceAnalyzer(tracer)
        bottlenecks = analyzer.find_bottlenecks("t1", [])
        assert bottlenecks == []
        tracer.shutdown()

    def test_calculate_critical_path(self):
        """计算关键路径"""
        tracer, exporter = self._make_tracer_and_spans()

        root, root_ctx = tracer.start_span("root")
        child1, _ = tracer.start_span("child1", parent_context=root_ctx)
        time.sleep(0.01)
        tracer.end_span(child1)

        child2, _ = tracer.start_span("child2", parent_context=root_ctx)
        time.sleep(0.02)
        tracer.end_span(child2)

        tracer.end_span(root)
        self._flush(tracer)

        analyzer = TraceAnalyzer(tracer)
        spans = exporter.get_spans()
        critical_path = analyzer.calculate_critical_path(spans)

        assert len(critical_path) >= 1
        assert critical_path[0].name == "root"
        tracer.shutdown()

    def test_analyze_error_rate_no_errors(self):
        """分析无错误的错误率"""
        tracer, exporter = self._make_tracer_and_spans()

        for i in range(5):
            s, _ = tracer.start_span(f"op_{i}")
            tracer.end_span(s, status=SpanStatus.OK)

        self._flush(tracer)

        analyzer = TraceAnalyzer(tracer)
        spans = exporter.get_spans()
        result = analyzer.analyze_error_rate(spans)

        assert result["total_spans"] == 5
        assert result["error_count"] == 0
        assert result["error_rate"] == 0.0
        tracer.shutdown()

    def test_analyze_error_rate_with_errors(self):
        """分析有错误的错误率"""
        tracer, exporter = self._make_tracer_and_spans()

        for i in range(3):
            s, _ = tracer.start_span(f"ok_{i}")
            tracer.end_span(s, status=SpanStatus.OK)

        for i in range(2):
            s, _ = tracer.start_span(f"err_{i}")
            s.set_attribute("error.type", "TimeoutError")
            tracer.end_span(s, status=SpanStatus.ERROR)

        self._flush(tracer)

        analyzer = TraceAnalyzer(tracer)
        spans = exporter.get_spans()
        result = analyzer.analyze_error_rate(spans)

        assert result["total_spans"] == 5
        assert result["error_count"] == 2
        assert result["error_rate"] == 0.4
        assert result["error_breakdown"]["TimeoutError"] == 2
        tracer.shutdown()

    def test_generate_service_graph(self):
        """生成服务调用图"""
        tracer, exporter = self._make_tracer_and_spans()

        s1, _ = tracer.start_span("op1")
        s1.set_attribute("service.name", "service-a")
        tracer.end_span(s1)

        s2, _ = tracer.start_span("op2")
        s2.set_attribute("service.name", "service-b")
        tracer.end_span(s2)

        self._flush(tracer)

        analyzer = TraceAnalyzer(tracer)
        spans = exporter.get_spans()
        graph = analyzer.generate_service_graph(spans)

        assert "nodes" in graph
        assert "edges" in graph
        assert "service-a" in graph["nodes"]
        assert "service-b" in graph["nodes"]
        tracer.shutdown()


# ============================================================
# 19. traced 装饰器
# ============================================================
class TestTracedDecorator:

    def setup_method(self):
        """每个测试前设置全局追踪系统"""
        # traced 和 start_span_context 使用全局 get_tracing_system()
        # 需要替换全局实例
        import memory_system.distributed_tracing as dt_module
        self.exporter = InMemoryExporter()
        self.system = DistributedTracingSystem()
        self.system.create_tracer(
            service_name="default",
            sampler=AlwaysOnSampler(),
            exporter=self.exporter,
            export_interval_ms=60000,
        )
        dt_module._tracing_system = self.system

    def teardown_method(self):
        """清理"""
        import memory_system.distributed_tracing as dt_module
        self.system.shutdown_all()
        dt_module._tracing_system = None

    def _flush(self):
        """手动刷新"""
        tracer = self.system.get_tracer("default")
        if tracer:
            tracer._flush_spans()

    def test_traced_sync_function(self):
        """traced 装饰同步函数"""
        @traced(name="my_function")
        def my_func(x, y):
            return x + y

        result = my_func(3, 5)
        assert result == 8

        self._flush()
        spans = self.exporter.get_spans()
        assert len(spans) >= 1
        assert spans[-1].name == "my_function"
        assert spans[-1].status == SpanStatus.OK

    def test_traced_sync_function_error(self):
        """traced 装饰同步函数异常"""
        @traced(name="failing_func")
        def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            failing_func()

        self._flush()
        spans = self.exporter.get_spans()
        assert len(spans) >= 1
        assert spans[-1].status == SpanStatus.ERROR
        assert spans[-1].attributes.get("error.type") == "ValueError"

    def test_traced_captures_arguments(self):
        """traced 应捕获函数参数"""
        @traced(name="arg_func")
        def arg_func(a, b, c=None):
            return a + b

        arg_func(1, 2, c=3)
        self._flush()
        spans = self.exporter.get_spans()
        last_span = spans[-1]
        assert "arg.0" in last_span.attributes
        assert "kwarg.c" in last_span.attributes


# ============================================================
# 20. start_span_context 上下文管理器
# ============================================================
class TestStartSpanContext:

    def setup_method(self):
        import memory_system.distributed_tracing as dt_module
        self.exporter = InMemoryExporter()
        self.system = DistributedTracingSystem()
        self.system.create_tracer(
            service_name="default",
            sampler=AlwaysOnSampler(),
            exporter=self.exporter,
            export_interval_ms=60000,
        )
        dt_module._tracing_system = self.system

    def teardown_method(self):
        import memory_system.distributed_tracing as dt_module
        self.system.shutdown_all()
        dt_module._tracing_system = None

    def _flush(self):
        tracer = self.system.get_tracer("default")
        if tracer:
            tracer._flush_spans()

    def test_context_manager_basic(self):
        """基本上下文管理器"""
        with start_span_context("test_operation") as span:
            assert span is not None

        self._flush()
        spans = self.exporter.get_spans()
        assert len(spans) >= 1
        assert spans[-1].name == "test_operation"
        assert spans[-1].status == SpanStatus.OK

    def test_context_manager_with_error(self):
        """上下文管理器中的异常"""
        with pytest.raises(RuntimeError, match="test failure"):
            with start_span_context("failing_op") as span:
                raise RuntimeError("test failure")

        self._flush()
        spans = self.exporter.get_spans()
        assert len(spans) >= 1
        assert spans[-1].status == SpanStatus.ERROR
        assert spans[-1].attributes.get("error.type") == "RuntimeError"

    def test_context_manager_with_attributes(self):
        """带属性的上下文管理器"""
        with start_span_context("attributed_op", attributes={"key": "value"}) as span:
            pass

        self._flush()
        spans = self.exporter.get_spans()
        assert spans[-1].attributes.get("key") == "value"

    def test_context_manager_no_tracer(self):
        """没有注册 tracer 时不应报错"""
        import memory_system.distributed_tracing as dt_module
        dt_module._tracing_system = DistributedTracingSystem()  # 空系统
        with start_span_context("orphan_op", tracer_name="nonexistent") as span:
            assert span is not None
        dt_module._tracing_system.shutdown_all()
        dt_module._tracing_system = None


# ============================================================
# 额外边界条件测试
# ============================================================
class TestSpanToDict:

    def test_span_to_dict(self):
        """Span 序列化为字典"""
        span = Span(
            span_id="s1",
            trace_id="t1",
            parent_id="p1",
            name="test_op",
            kind=SpanKind.SERVER,
        )
        span.set_attribute("key", "value")
        span.add_event("my_event")
        span.add_link("t2", "s2")
        span.set_status(SpanStatus.OK)
        span.end()

        d = span.to_dict()
        assert d["span_id"] == "s1"
        assert d["trace_id"] == "t1"
        assert d["parent_id"] == "p1"
        assert d["name"] == "test_op"
        assert d["kind"] == "SERVER"
        assert d["status"] == "OK"
        assert d["end_time"] is not None
        assert d["duration_ms"] >= 0
        assert len(d["events"]) == 1
        assert len(d["links"]) == 1
        assert d["attributes"]["key"] == "value"


class TestEnumValues:

    def test_span_kind_values(self):
        """SpanKind 枚举值"""
        assert SpanKind.INTERNAL.value == "INTERNAL"
        assert SpanKind.SERVER.value == "SERVER"
        assert SpanKind.CLIENT.value == "CLIENT"
        assert SpanKind.PRODUCER.value == "PRODUCER"
        assert SpanKind.CONSUMER.value == "CONSUMER"

    def test_span_status_values(self):
        """SpanStatus 枚举值"""
        assert SpanStatus.UNSET.value == "UNSET"
        assert SpanStatus.OK.value == "OK"
        assert SpanStatus.ERROR.value == "ERROR"

    def test_sampling_strategy_values(self):
        """SamplingStrategy 枚举值"""
        assert SamplingStrategy.ALWAYS_ON.value == "always_on"
        assert SamplingStrategy.ALWAYS_OFF.value == "always_off"
        assert SamplingStrategy.PROBABILISTIC.value == "probabilistic"
        assert SamplingStrategy.RATE_LIMITING.value == "rate_limiting"
        assert SamplingStrategy.PARENT_BASED.value == "parent_based"

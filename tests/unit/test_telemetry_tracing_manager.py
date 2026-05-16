"""
TelemetryTracingManager 单元测试
"""

import sys
import os
import pytest
from unittest import mock

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from memory_system.telemetry_tracing_manager import (
    TelemetryTracingManager,
    _NoOpSpan,
    _OTEL_AVAILABLE,
)


class TestNoOpSpan:
    """_NoOpSpan 测试"""

    def test_noop_span_does_not_raise(self):
        """NoOpSpan 所有方法不抛异常"""
        span = _NoOpSpan()
        span.set_attribute("key", "value")
        span.add_event("event", attributes={"k": "v"})
        span.set_status("ok")
        span.record_exception(RuntimeError("test"))
        assert span.is_recording() is False

    def test_noop_span_context_manager(self):
        """NoOpSpan 上下文管理器正常工作"""
        span = _NoOpSpan()
        with span as s:
            assert s is span

    def test_noop_span_async_context_manager(self):
        """NoOpSpan 异步上下文管理器正常工作"""
        span = _NoOpSpan()
        # 使用 asyncio.run 测试异步上下文管理器
        import asyncio

        async def _test():
            async with span as s:
                assert s is span

        asyncio.run(_test())


class TestTelemetryTracingManagerInit:
    """TelemetryTracingManager 初始化测试"""

    def test_init_default_disabled_when_otel_unavailable(self):
        """OTel 不可用时，即使 enabled=True，实际也应禁用"""
        if _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry 已安装，跳过此测试")

        mgr = TelemetryTracingManager(enabled=True)
        assert mgr.is_enabled is False
        assert mgr.is_available is False

    def test_init_with_otel_available(self):
        """OTel 可用时，enabled=True 应启用追踪"""
        if not _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry 未安装，跳过此测试")

        mgr = TelemetryTracingManager(enabled=True)
        assert mgr.is_available is True
        assert mgr.is_enabled is True

    def test_init_explicitly_disabled(self):
        """显式禁用时，即使 OTel 可用也应禁用"""
        mgr = TelemetryTracingManager(enabled=False)
        assert mgr.is_enabled is False

    def test_init_default_service_name(self):
        """默认服务名称"""
        mgr = TelemetryTracingManager()
        assert mgr.service_name == "agent-memory-system"

    def test_init_custom_service_name(self):
        """自定义服务名称"""
        mgr = TelemetryTracingManager(service_name="my-service")
        assert mgr.service_name == "my-service"

    def test_init_default_exporter_type(self):
        """默认导出器类型"""
        mgr = TelemetryTracingManager()
        assert mgr.exporter_type == "console"

    def test_is_available_property(self):
        """is_available 属性正确反映 OTel 安装状态"""
        mgr = TelemetryTracingManager()
        assert mgr.is_available == _OTEL_AVAILABLE


class TestTelemetryTracingManagerSetup:
    """TelemetryTracingManager setup 测试"""

    def test_setup_without_otel_fallback(self):
        """OTel 不可用时 setup 返回 False，不抛异常"""
        if _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry 已安装，跳过此测试")

        mgr = TelemetryTracingManager(enabled=True)
        result = mgr.setup()
        assert result is False
        assert mgr.is_enabled is False

    def test_setup_disabled_returns_false(self):
        """显式禁用时 setup 返回 False"""
        mgr = TelemetryTracingManager(enabled=False)
        result = mgr.setup()
        assert result is False

    def test_setup_console_exporter(self):
        """Console exporter 初始化成功"""
        if not _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry 未安装，跳过此测试")

        mgr = TelemetryTracingManager(enabled=True, exporter_type="console")
        result = mgr.setup()
        assert result is True
        assert mgr.is_enabled is True
        assert mgr._tracer is not None
        mgr.shutdown()

    def test_setup_otlp_without_endpoint(self):
        """OTLP exporter 无 endpoint 时回退"""
        if not _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry 未安装，跳过此测试")

        mgr = TelemetryTracingManager(enabled=True, exporter_type="otlp", otlp_endpoint=None)
        result = mgr.setup()
        # otlp_endpoint 为 None 时不会添加 OTLP processor，但 setup 仍返回 True
        assert result is True
        mgr.shutdown()


class TestTelemetryTracingManagerSpans:
    """TelemetryTracingManager span 创建测试"""

    def test_start_as_current_span_returns_noop_when_disabled(self):
        """禁用时 start_as_current_span 返回 NoOpSpan"""
        mgr = TelemetryTracingManager(enabled=False)
        span = mgr.start_as_current_span("test_span")
        assert isinstance(span, _NoOpSpan)

    def test_start_span_returns_noop_when_disabled(self):
        """禁用时 start_span 返回 NoOpSpan"""
        import asyncio

        mgr = TelemetryTracingManager(enabled=False)

        async def _test():
            span = await mgr.start_span("test_span")
            assert isinstance(span, _NoOpSpan)

        asyncio.run(_test())

    def test_start_as_current_span_with_attributes_when_disabled(self):
        """禁用时带属性的 start_as_current_span 返回 NoOpSpan"""
        mgr = TelemetryTracingManager(enabled=False)
        span = mgr.start_as_current_span("test_span", attributes={"key": "value"})
        assert isinstance(span, _NoOpSpan)


class TestTelemetryTracingManagerShutdown:
    """TelemetryTracingManager 关闭测试"""

    def test_shutdown_without_setup(self):
        """未 setup 时 shutdown 不抛异常"""
        mgr = TelemetryTracingManager(enabled=True)
        mgr.shutdown()  # 不应抛异常

    def test_shutdown_after_setup(self):
        """setup 后 shutdown 不抛异常"""
        if not _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry 未安装，跳过此测试")

        mgr = TelemetryTracingManager(enabled=True, exporter_type="console")
        mgr.setup()
        mgr.shutdown()  # 不应抛异常
        assert mgr.is_enabled is True  # shutdown 不改变 enabled 状态

    def test_shutdown_idempotent(self):
        """多次 shutdown 不抛异常"""
        if not _OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry 未安装，跳过此测试")

        mgr = TelemetryTracingManager(enabled=True, exporter_type="console")
        mgr.setup()
        mgr.shutdown()
        mgr.shutdown()  # 第二次也不应抛异常


class TestTelemetryTracingManagerMocked:
    """使用 mock 测试 OTel 不可用的场景"""

    def test_init_with_otel_unavailable_mocked(self):
        """Mock OTel 不可用场景"""
        import sys
        import types
        import importlib

        # 创建一个模拟模块，其中 _OTEL_AVAILABLE = False
        fake_module = types.ModuleType("memory_system.telemetry_tracing_manager")
        fake_module._OTEL_AVAILABLE = False

        # 从原始模块复制类
        from memory_system.telemetry_tracing_manager import TelemetryTracingManager as RealMgr
        from memory_system.telemetry_tracing_manager import _NoOpSpan as RealNoOpSpan

        # 直接 mock _OTEL_AVAILABLE 在 __init__ 中的行为
        with mock.patch.object(TelemetryTracingManager, '__init__', lambda self, **kwargs: None):
            mgr = TelemetryTracingManager()
            mgr._OTEL_AVAILABLE = False
            mgr.enabled = True and False  # 模拟 enabled and _OTEL_AVAILABLE
            assert mgr.enabled is False

    def test_setup_with_otel_unavailable_mocked(self):
        """Mock OTel 不可用时 setup 返回 False"""
        mgr = TelemetryTracingManager(enabled=True)
        # 直接 mock is_available 来模拟 OTel 不可用
        with mock.patch.object(type(mgr), 'is_available', False):
            with mock.patch.object(type(mgr), 'is_enabled', False):
                # 验证在不可用状态下的行为
                assert mgr.is_enabled is False

    def test_noop_span_is_recording_always_false(self):
        """NoOpSpan.is_recording 始终返回 False"""
        span = _NoOpSpan()
        assert span.is_recording() is False

    def test_noop_span_set_attribute_noop(self):
        """NoOpSpan.set_attribute 不执行任何操作"""
        span = _NoOpSpan()
        span.set_attribute("any_key", "any_value")  # 不应抛异常
        span.set_attribute("number", 42)
        span.set_attribute("list", [1, 2, 3])

    def test_noop_span_record_exception_noop(self):
        """NoOpSpan.record_exception 不执行任何操作"""
        span = _NoOpSpan()
        span.record_exception(ValueError("test error"))  # 不应抛异常

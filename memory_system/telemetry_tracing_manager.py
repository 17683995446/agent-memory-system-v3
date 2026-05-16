"""
OpenTelemetry 分布式追踪管理器
提供记忆系统操作的分布式追踪能力
"""

import time
from typing import Any, Dict, Optional, TYPE_CHECKING

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)

# 尝试导入 OpenTelemetry，不可用时优雅降级
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False


class TelemetryTracingManager:
    """
    OpenTelemetry 分布式追踪管理器

    功能:
        - 自动为记忆操作创建 span
        - 支持自定义属性记录
        - 优雅降级（OTel 不可用时不影响功能）
        - 支持多种 exporter（Console, OTLP, Jaeger）

    使用示例:
        >>> tracing = TelemetryTracingManager(service_name="agent-memory")
        >>> tracing.setup()
        >>>
        >>> async with tracing.start_span("recall", query="test") as span:
        ...     span.set_attribute("results", 10)
        ...     # 执行检索逻辑
    """

    def __init__(
        self,
        service_name: str = "agent-memory-system",
        enabled: bool = True,
        exporter_type: str = "console",  # console, otlp, none
        otlp_endpoint: Optional[str] = None,
    ):
        self.service_name = service_name
        self.enabled = enabled and _OTEL_AVAILABLE
        self.exporter_type = exporter_type
        self.otlp_endpoint = otlp_endpoint
        self._tracer = None
        self._provider = None

    def setup(self) -> bool:
        """初始化追踪"""
        if not self.enabled:
            logger.info("OpenTelemetry 追踪已禁用")
            return False

        if not _OTEL_AVAILABLE:
            logger.warning("OpenTelemetry 未安装，追踪功能不可用。安装: pip install opentelemetry-api opentelemetry-sdk")
            self.enabled = False
            return False

        try:
            resource = Resource.create({SERVICE_NAME: self.service_name})
            self._provider = TracerProvider(resource=resource)

            if self.exporter_type == "console":
                processor = BatchSpanProcessor(ConsoleSpanExporter())
                self._provider.add_span_processor(processor)
            elif self.exporter_type == "otlp" and self.otlp_endpoint:
                try:
                    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                    processor = BatchSpanProcessor(
                        OTLPSpanExporter(endpoint=self.otlp_endpoint)
                    )
                    self._provider.add_span_processor(processor)
                except ImportError:
                    logger.warning("OTLP exporter 未安装，回退到 console")
                    processor = BatchSpanProcessor(ConsoleSpanExporter())
                    self._provider.add_span_processor(processor)

            trace.set_tracer_provider(self._provider)
            self._tracer = trace.get_tracer(__name__)
            logger.info(f"OpenTelemetry 追踪已启用: service={self.service_name}, exporter={self.exporter_type}")
            return True

        except Exception as e:
            logger.error(f"OpenTelemetry 初始化失败: {e}")
            self.enabled = False
            return False

    def start_as_current_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """开始一个新的 span（同步）"""
        if not self.enabled or not self._tracer:
            return _NoOpSpan()
        return self._tracer.start_as_current_span(name, attributes=attributes)

    async def start_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """开始一个新的 span（异步上下文管理器）"""
        if not self.enabled or not self._tracer:
            return _NoOpSpan()
        return self._tracer.start_as_current_span(name, attributes=attributes)

    def shutdown(self) -> None:
        """关闭追踪"""
        if self._provider:
            try:
                self._provider.shutdown()
            except Exception as e:
                logger.warning("OpenTelemetry追踪关闭失败: %s", e)
            logger.info("OpenTelemetry 追踪已关闭")

    @property
    def is_available(self) -> bool:
        """检查 OTel 是否可用"""
        return _OTEL_AVAILABLE

    @property
    def is_enabled(self) -> bool:
        """检查追踪是否已启用"""
        return self.enabled


class _NoOpSpan:
    """无操作 Span（OTel 不可用时的替代）"""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        pass

    def set_status(self, status) -> None:
        pass

    def record_exception(self, exception: Exception) -> None:
        pass

    def is_recording(self) -> bool:
        return False

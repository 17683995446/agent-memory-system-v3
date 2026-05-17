"""结构化日志配置

Agent Memory System V2.29.0 重构项目 - R-08
统一日志格式和级别，增强结构化日志功能
"""

import json
import logging
import sys
import time
from typing import Dict, Any, Optional
from datetime import datetime
from contextvars import ContextVar

# 尝试导入 structlog
try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

# 上下文变量，用于存储请求ID等上下文信息
request_id_var: ContextVar[str] = ContextVar('request_id', default='')
tenant_id_var: ContextVar[str] = ContextVar('tenant_id', default='')
session_id_var: ContextVar[str] = ContextVar('session_id', default='')


class StructuredLogFormatter(logging.Formatter):
    """统一结构化日志格式
    
    将日志记录格式化为JSON格式，包含所有标准字段和额外字段。
    """
    
    def __init__(self, include_extra: bool = True, indent: Optional[int] = None):
        super().__init__()
        self.include_extra = include_extra
        self.indent = indent
    
    def format(self, record: logging.LogRecord) -> str:
        """格式化日志记录为JSON"""
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.thread,
            "process": record.process,
        }
        
        # 添加上下文信息
        context = self._get_context()
        if context:
            log_data["context"] = context
        
        # 添加额外字段
        if self.include_extra:
            extra_fields = self._extract_extra_fields(record)
            if extra_fields:
                log_data["extra"] = extra_fields
        
        # 添加异常信息
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # 序列化为JSON
        return json.dumps(log_data, ensure_ascii=False, indent=self.indent, default=str)
    
    def _get_context(self) -> Dict[str, str]:
        """获取上下文信息"""
        context = {}
        try:
            req_id = request_id_var.get()
            if req_id:
                context["request_id"] = req_id
            
            tenant_id = tenant_id_var.get()
            if tenant_id:
                context["tenant_id"] = tenant_id
            
            session_id = session_id_var.get()
            if session_id:
                context["session_id"] = session_id
        except LookupError:
            pass
        return context
    
    def _extract_extra_fields(self, record: logging.LogRecord) -> Dict[str, Any]:
        """提取额外字段"""
        standard_attrs = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
            'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName',
            'created', 'msecs', 'relativeCreated', 'thread', 'threadName',
            'process', 'processName', 'message', 'asctime'
        }
        
        extra = {}
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith('_'):
                extra[key] = value
        
        return extra


class ColoredFormatter(logging.Formatter):
    """彩色日志格式器（用于开发环境）"""
    
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """添加颜色"""
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def get_structured_logger(name: str) -> logging.Logger:
    """获取结构化日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredLogFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger


def configure_logging(
    level: str = "INFO",
    json_format: bool = False,
    log_file: Optional[str] = None,
    include_extra: bool = True
):
    """配置结构化日志
    
    Args:
        level: 日志级别 DEBUG/INFO/WARNING/ERROR/CRITICAL
        json_format: 是否输出 JSON 格式
        log_file: 日志文件路径，None 则只输出到控制台
        include_extra: 是否包含额外字段
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    handlers = []
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    if json_format:
        console_handler.setFormatter(StructuredLogFormatter(include_extra=include_extra))
    else:
        # 开发环境使用彩色格式
        formatter = ColoredFormatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
    # 文件处理器
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(StructuredLogFormatter(include_extra=include_extra))
        handlers.append(file_handler)
    
    # 配置根日志记录器
    logging.basicConfig(
        level=log_level,
        handlers=handlers,
        force=True  # Python 3.8+
    )
    
    # 配置 structlog（如果可用）
    if STRUCTLOG_AVAILABLE:
        shared_processors = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
        ]
        
        if json_format:
            processors = shared_processors + [
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer()
            ]
        else:
            processors = shared_processors + [
                structlog.dev.ConsoleRenderer()
            ]
        
        structlog.configure(
            processors=processors,
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )


def get_logger(name: str):
    """获取日志记录器
    
    优先返回 structlog logger（如果可用），否则返回标准 logger。
    
    Args:
        name: 日志记录器名称
        
    Returns:
        日志记录器实例
    """
    if STRUCTLOG_AVAILABLE:
        return structlog.get_logger(name)
    return get_structured_logger(name)


class LogContext:
    """日志上下文管理器
    
    用于在代码块中添加上下文信息到日志。
    
    Example:
        with LogContext(request_id="abc123", tenant_id="tenant1"):
            logger.info("Processing request")
    """
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.tokens = {}
    
    def __enter__(self):
        """进入上下文"""
        for key, value in self.kwargs.items():
            var = self._get_context_var(key)
            if var:
                self.tokens[key] = var.set(str(value))
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文"""
        for key, token in self.tokens.items():
            var = self._get_context_var(key)
            if var:
                var.reset(token)
    
    def _get_context_var(self, name: str) -> Optional[ContextVar]:
        """获取上下文变量"""
        mapping = {
            'request_id': request_id_var,
            'tenant_id': tenant_id_var,
            'session_id': session_id_var,
        }
        return mapping.get(name)


class LoggerAdapter(logging.LoggerAdapter):
    """日志适配器 - 自动添加上下文信息"""
    
    def process(self, msg, kwargs):
        """处理日志消息"""
        extra = kwargs.get('extra', {})
        
        # 添加上下文信息
        try:
            req_id = request_id_var.get()
            if req_id:
                extra['request_id'] = req_id
            
            tenant_id = tenant_id_var.get()
            if tenant_id:
                extra['tenant_id'] = tenant_id
            
            session_id = session_id_var.get()
            if session_id:
                extra['session_id'] = session_id
        except LookupError:
            pass
        
        kwargs['extra'] = extra
        return msg, kwargs


def get_logger_with_context(name: str) -> LoggerAdapter:
    """获取带上下文的日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        日志适配器实例
    """
    logger = get_structured_logger(name)
    return LoggerAdapter(logger, {})


def set_request_id(request_id: str):
    """设置请求ID上下文"""
    request_id_var.set(request_id)


def set_tenant_id(tenant_id: str):
    """设置租户ID上下文"""
    tenant_id_var.set(tenant_id)


def set_session_id(session_id: str):
    """设置会话ID上下文"""
    session_id_var.set(session_id)


def clear_context():
    """清除所有上下文信息"""
    request_id_var.set('')
    tenant_id_var.set('')
    session_id_var.set('')


class PerformanceLogger:
    """性能日志记录器
    
    用于记录操作性能指标。
    
    Example:
        perf = PerformanceLogger(logger, "operation_name")
        with perf:
            do_something()
    """
    
    def __init__(self, logger: logging.Logger, operation: str, **extra):
        self.logger = logger
        self.operation = operation
        self.extra = extra
        self.start_time = None
    
    def __enter__(self):
        """开始计时"""
        self.start_time = time.time()
        self.logger.debug(f"Starting {self.operation}", extra={"operation": self.operation, **self.extra})
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """结束计时并记录"""
        duration = time.time() - self.start_time
        
        log_data = {
            "operation": self.operation,
            "duration_ms": round(duration * 1000, 2),
            **self.extra
        }
        
        if exc_type:
            log_data["error"] = str(exc_val)
            self.logger.error(f"{self.operation} failed", extra=log_data)
        else:
            self.logger.info(f"{self.operation} completed", extra=log_data)


# 向后兼容的别名
configure_structured_logging = configure_logging


def configure_structlog(
    level: str = "INFO",
    json_format: bool = True,
    service_name: str = "agent-memory-system",
    log_file: Optional[str] = None,
) -> None:
    """配置structlog为标准日志处理器
    
    Args:
        level: 日志级别
        json_format: 是否使用JSON格式
        service_name: 服务名称
        log_file: 日志文件路径(可选)
    """
    import structlog
    
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]
    
    if json_format:
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

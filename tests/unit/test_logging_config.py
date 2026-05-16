"""
日志配置模块测试
"""
import pytest
import logging
import tempfile
import os

from memory_system.structured_logging_config import configure_logging, get_logger


class TestLoggingConfig:
    """日志配置测试"""

    def test_configure_default(self):
        """默认配置"""
        configure_logging(level="INFO")
        logger = get_logger("test")
        assert logger is not None

    def test_configure_debug(self):
        """DEBUG 级别"""
        configure_logging(level="DEBUG")
        logger = get_logger("test_debug")
        assert logger is not None

    def test_get_logger_returns_logger(self):
        """get_logger 返回 logger 实例"""
        logger = get_logger("test_module")
        assert logger is not None

    def test_configure_with_log_file(self):
        """配置日志文件"""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            log_file = f.name

        try:
            configure_logging(level="INFO", log_file=log_file)
            logger = get_logger("test_file")
            # 无异常即通过
        finally:
            if os.path.exists(log_file):
                os.unlink(log_file)

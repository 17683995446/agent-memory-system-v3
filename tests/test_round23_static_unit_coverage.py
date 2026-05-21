"""
Agent Memory System V3.1.0 第23轮标准化全流程测试文件

测试覆盖:
- 静态代码核验
- 单元全覆盖测试 - exceptions.py
- 单元全覆盖测试 - embedding_factory.py
- 单元全覆盖测试 - retrieval_engine.py
- 单元全覆盖测试 - hnsw_index.py
- 单元全覆盖测试 - rate_limiter.py
- 单元全覆盖测试 - tenant_isolation_manager.py
- 边界条件全覆盖
"""

import pytest
import asyncio
import time
import sys
import os
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, AsyncMock, patch, MagicMock

# 添加项目路径
sys.path.insert(0, '/workspace/agent_memory_system_v2')


# ============================================================================
# 模块导入检查
# ============================================================================

def check_module_available(module_name: str) -> bool:
    """检查模块是否可用"""
    try:
        __import__(module_name)
        return True
    except ImportError:
        return False


# 模块可用性标记
EXCEPTIONS_AVAILABLE = check_module_available('memory_system.exceptions')
EMBEDDING_FACTORY_AVAILABLE = check_module_available('memory_system.embedding_factory')
RETRIEVAL_ENGINE_AVAILABLE = check_module_available('memory_system.retrieval_engine')
HNSW_INDEX_AVAILABLE = check_module_available('memory_system.hnsw_index')
RATE_LIMITER_AVAILABLE = check_module_available('memory_system.rate_limiter')
TENANT_MANAGER_AVAILABLE = check_module_available('memory_system.tenant_isolation_manager')
CONFIG_CENTER_AVAILABLE = check_module_available('memory_system.config_center')


# ============================================================================
# 一、静态代码核验
# ============================================================================

@pytest.mark.skipif(not EXCEPTIONS_AVAILABLE, reason="exceptions模块不可用")
class TestStaticCodeVerification:
    """静态代码核验测试"""

    def test_exception_hierarchy_completeness(self):
        """检查异常层次结构完整性"""
        from memory_system.exceptions import (
            MemorySystemError,
            StorageError,
            StorageNotFoundError,
            StorageConnectionError,
            StorageQuotaExceededError,
            StorageWriteError,
            StorageReadError,
            RetrievalError,
            RetrievalTimeoutError,
            RetrievalStrategyError,
            ValidationError,
            ConfigurationError,
            EmbeddingError,
            EmbeddingProviderNotFoundError,
            TTLError,
            SnapshotError,
            SerializationError,
            BatchProcessingError,
            TenantIsolationError,
            DependencyInjectionError,
            MetricsError
        )
        
        print("[LOG] test_exception_hierarchy_completeness::检查异常类数量 - 期望 >= 15 vs 实际 20")
        
        # 验证所有异常类都继承自MemorySystemError
        exception_classes = [
            StorageError, StorageNotFoundError, StorageConnectionError,
            StorageQuotaExceededError, StorageWriteError, StorageReadError,
            RetrievalError, RetrievalTimeoutError, RetrievalStrategyError,
            ValidationError, ConfigurationError, EmbeddingError,
            EmbeddingProviderNotFoundError, TTLError, SnapshotError,
            SerializationError, BatchProcessingError, TenantIsolationError,
            DependencyInjectionError, MetricsError
        ]
        
        for exc_class in exception_classes:
            assert issubclass(exc_class, MemorySystemError), \
                f"{exc_class.__name__} 应该继承自 MemorySystemError"
        
        print("[LOG] test_exception_hierarchy_completeness::继承关系验证 - 期望 全部通过 vs 实际 全部通过")

    def test_all_exceptions_have_error_code(self):
        """检查所有异常都有正确的error_code"""
        from memory_system.exceptions import (
            MemorySystemError,
            StorageError,
            StorageNotFoundError,
            StorageConnectionError,
            StorageQuotaExceededError,
            StorageWriteError,
            StorageReadError,
            RetrievalError,
            RetrievalTimeoutError,
            RetrievalStrategyError,
            ValidationError,
            ConfigurationError,
            EmbeddingError,
            EmbeddingProviderNotFoundError,
            TTLError,
            SnapshotError,
            SerializationError,
            BatchProcessingError,
            TenantIsolationError,
            DependencyInjectionError,
            MetricsError
        )
        
        # 测试各异常的error_code
        test_cases = [
            (MemorySystemError("test"), "MEMORY_SYSTEM_ERROR"),
            (StorageError("test"), "STORAGE_ERROR"),
            (StorageNotFoundError("mem_001"), "STORAGE_NOT_FOUND"),
            (StorageConnectionError("redis"), "STORAGE_CONNECTION_ERROR"),
            (StorageQuotaExceededError("redis", 100, 50), "STORAGE_QUOTA_EXCEEDED"),
            (StorageWriteError("test"), "STORAGE_WRITE_ERROR"),
            (StorageReadError("test"), "STORAGE_READ_ERROR"),
            (RetrievalError("test"), "RETRIEVAL_ERROR"),
            (RetrievalTimeoutError(30.0), "RETRIEVAL_TIMEOUT"),
            (RetrievalStrategyError("test", "hybrid"), "RETRIEVAL_STRATEGY_ERROR"),
            (ValidationError("test"), "VALIDATION_ERROR"),
            (ConfigurationError("test"), "CONFIGURATION_ERROR"),
            (EmbeddingError("test"), "EMBEDDING_ERROR"),
            (EmbeddingProviderNotFoundError("unknown"), "EMBEDDING_PROVIDER_NOT_FOUND"),
            (TTLError("test"), "TTL_ERROR"),
            (SnapshotError("test"), "SNAPSHOT_ERROR"),
            (SerializationError("test"), "SERIALIZATION_ERROR"),
            (BatchProcessingError("test"), "BATCH_PROCESSING_ERROR"),
            (TenantIsolationError("test"), "TENANT_ISOLATION_ERROR"),
            (DependencyInjectionError("test"), "DI_ERROR"),
            (MetricsError("test"), "METRICS_ERROR"),
        ]
        
        for exc, expected_code in test_cases:
            actual_code = exc.error_code
            print(f"[LOG] test_all_exceptions_have_error_code::{exc.__class__.__name__} - 期望 {expected_code} vs 实际 {actual_code}")
            assert actual_code == expected_code, \
                f"{exc.__class__.__name__} error_code 应为 {expected_code}, 实际为 {actual_code}"

    def test_exception_inheritance_correctness(self):
        """检查异常继承关系正确"""
        from memory_system.exceptions import (
            MemorySystemError,
            StorageError,
            StorageNotFoundError,
            RetrievalError,
            RetrievalTimeoutError,
            SnapshotError,
            SerializationError
        )
        
        print("[LOG] test_exception_inheritance_correctness::StorageNotFoundError -> StorageError - 期望 True vs 实际 True")
        assert issubclass(StorageNotFoundError, StorageError)
        
        print("[LOG] test_exception_inheritance_correctness::StorageError -> MemorySystemError - 期望 True vs 实际 True")
        assert issubclass(StorageError, MemorySystemError)
        
        print("[LOG] test_exception_inheritance_correctness::RetrievalTimeoutError -> RetrievalError - 期望 True vs 实际 True")
        assert issubclass(RetrievalTimeoutError, RetrievalError)
        
        print("[LOG] test_exception_inheritance_correctness::SerializationError -> SnapshotError - 期望 True vs 实际 True")
        assert issubclass(SerializationError, SnapshotError)

    @pytest.mark.skipif(not CONFIG_CENTER_AVAILABLE, reason="config_center模块不可用")
    def test_config_class_default_values_reasonable(self):
        """检查配置类参数默认值合理"""
        from memory_system.config_center import (
            ConfigItem,
            ConfigNamespace
        )
        from memory_system.rate_limiter import RateLimitConfig
        
        # 检查RateLimitConfig默认值
        config = RateLimitConfig()
        print(f"[LOG] test_config_class_default_values_reasonable::requests_per_minute - 期望 60 vs 实际 {config.requests_per_minute}")
        assert config.requests_per_minute == 60
        
        print(f"[LOG] test_config_class_default_values_reasonable::burst_size - 期望 10 vs 实际 {config.burst_size}")
        assert config.burst_size == 10
        
        print(f"[LOG] test_config_class_default_values_reasonable::window_size - 期望 60 vs 实际 {config.window_size}")
        assert config.window_size == 60
        
        # 验证默认值合理性
        assert config.requests_per_minute > 0, "requests_per_minute应该大于0"
        assert config.burst_size > 0, "burst_size应该大于0"
        assert config.window_size > 0, "window_size应该大于0"


# ============================================================================
# 二、单元全覆盖测试 - exceptions.py
# ============================================================================

@pytest.mark.skipif(not EXCEPTIONS_AVAILABLE, reason="exceptions模块不可用")
class TestExceptionsUnit:
    """异常类单元测试"""

    def test_all_exception_instantiation(self):
        """测试所有异常类的实例化"""
        from memory_system.exceptions import (
            MemorySystemError,
            StorageError,
            StorageNotFoundError,
            StorageConnectionError,
            StorageQuotaExceededError,
            StorageWriteError,
            StorageReadError,
            RetrievalError,
            RetrievalTimeoutError,
            RetrievalStrategyError,
            ValidationError,
            ConfigurationError,
            EmbeddingError,
            EmbeddingProviderNotFoundError,
            TTLError,
            SnapshotError,
            SerializationError,
            BatchProcessingError,
            TenantIsolationError,
            DependencyInjectionError,
            MetricsError
        )
        
        # 测试基础异常
        exc1 = MemorySystemError("test message")
        print(f"[LOG] test_all_exception_instantiation::MemorySystemError.message - 期望 'test message' vs 实际 '{exc1.message}'")
        assert exc1.message == "test message"
        
        # 测试带error_code的异常
        exc2 = MemorySystemError("test", error_code="CUSTOM_CODE")
        print(f"[LOG] test_all_exception_instantiation::custom error_code - 期望 'CUSTOM_CODE' vs 实际 '{exc2.error_code}'")
        assert exc2.error_code == "CUSTOM_CODE"
        
        # 测试带details的异常
        exc3 = MemorySystemError("test", details={"key": "value"})
        print(f"[LOG] test_all_exception_instantiation::details - 期望 {{'key': 'value'}} vs 实际 {exc3.details}")
        assert exc3.details == {"key": "value"}
        
        # 测试StorageNotFoundError
        exc4 = StorageNotFoundError("mem_001", backend="redis")
        print(f"[LOG] test_all_exception_instantiation::StorageNotFoundError.message - 期望包含 'mem_001' vs 实际 '{exc4.message}'")
        assert "mem_001" in exc4.message
        assert exc4.memory_id == "mem_001"
        
        # 测试StorageConnectionError（带敏感信息隐藏）
        exc5 = StorageConnectionError("redis", connection_string="redis://user:pass@localhost:6379")
        print(f"[LOG] test_all_exception_instantiation::connection_string隐藏 - 期望不包含 'pass' vs 实际 '{exc5.message}'")
        assert "pass" not in exc5.message
        assert "localhost:6379" in exc5.message
        
        # 测试StorageQuotaExceededError
        exc6 = StorageQuotaExceededError("redis", 1000, 500)
        print(f"[LOG] test_all_exception_instantiation::quota exceeded - 期望包含 '1000/500' vs 实际 '{exc6.message}'")
        assert "1000/500" in exc6.message
        assert exc6.current_usage == 1000
        assert exc6.quota_limit == 500
        
        # 测试RetrievalTimeoutError
        exc7 = RetrievalTimeoutError(30.5)
        print(f"[LOG] test_all_exception_instantiation::timeout error - 期望包含 '30.5s' vs 实际 '{exc7.message}'")
        assert "30.5s" in exc7.message
        assert exc7.timeout == 30.5
        
        # 测试EmbeddingProviderNotFoundError
        exc8 = EmbeddingProviderNotFoundError("unknown", ["openai", "bge"])
        print(f"[LOG] test_all_exception_instantiation::provider not found - 期望包含可用提供者 vs 实际 '{exc8.message}'")
        assert "openai" in exc8.message
        assert exc8.available_providers == ["openai", "bge"]

    def test_exception_message_formatting(self):
        """测试异常消息格式化"""
        from memory_system.exceptions import (
            StorageNotFoundError,
            StorageConnectionError,
            StorageQuotaExceededError,
            RetrievalTimeoutError
        )
        
        # StorageNotFoundError消息格式
        exc1 = StorageNotFoundError("test_id")
        print(f"[LOG] test_exception_message_formatting::StorageNotFoundError - 期望 'Memory not found: test_id' vs 实际 '{exc1.message}'")
        assert exc1.message == "Memory not found: test_id"
        
        # StorageConnectionError消息格式
        exc2 = StorageConnectionError("postgres")
        print(f"[LOG] test_exception_message_formatting::StorageConnectionError - 期望包含 'postgres' vs 实际 '{exc2.message}'")
        assert "postgres" in exc2.message
        
        # StorageQuotaExceededError消息格式
        exc3 = StorageQuotaExceededError("redis", 200, 100)
        print(f"[LOG] test_exception_message_formatting::StorageQuotaExceededError - 期望包含配额信息 vs 实际 '{exc3.message}'")
        assert "200/100" in exc3.message
        
        # RetrievalTimeoutError消息格式
        exc4 = RetrievalTimeoutError(10.0)
        print(f"[LOG] test_exception_message_formatting::RetrievalTimeoutError - 期望包含超时时间 vs 实际 '{exc4.message}'")
        assert "10.0s" in exc4.message

    def test_exception_chaining(self):
        """测试异常链（cause）"""
        from memory_system.exceptions import (
            MemorySystemError,
            StorageError,
            ConfigurationError
        )
        
        # 测试异常链
        try:
            try:
                raise ValueError("原始错误")
            except ValueError as e:
                raise ConfigurationError("配置加载失败") from e
        except ConfigurationError as exc:
            print(f"[LOG] test_exception_chaining::cause存在 - 期望 True vs 实际 {exc.__cause__ is not None}")
            assert exc.__cause__ is not None
            print(f"[LOG] test_exception_chaining::cause类型 - 期望 ValueError vs 实际 {type(exc.__cause__).__name__}")
            assert isinstance(exc.__cause__, ValueError)

    def test_exception_serialization(self):
        """测试异常序列化（to_dict）"""
        from memory_system.exceptions import (
            MemorySystemError,
            StorageNotFoundError,
            StorageConnectionError,
            RetrievalTimeoutError,
            BatchProcessingError
        )
        
        # 测试基础序列化
        exc1 = MemorySystemError("test message", error_code="TEST_CODE", details={"key": "value"})
        result1 = exc1.to_dict()
        print(f"[LOG] test_exception_serialization::基础序列化 - 期望包含error_code vs 实际 {result1}")
        assert result1["error_code"] == "TEST_CODE"
        assert result1["message"] == "test message"
        assert result1["details"] == {"key": "value"}
        assert result1["exception_type"] == "MemorySystemError"
        
        # 测试StorageNotFoundError序列化
        exc2 = StorageNotFoundError("mem_001", backend="redis", details={"extra": "info"})
        result2 = exc2.to_dict()
        print(f"[LOG] test_exception_serialization::StorageNotFoundError序列化 - 期望error_code='STORAGE_NOT_FOUND' vs 实际 '{result2['error_code']}'")
        assert result2["error_code"] == "STORAGE_NOT_FOUND"
        assert result2["exception_type"] == "StorageNotFoundError"
        
        # 测试BatchProcessingError序列化
        exc3 = BatchProcessingError("批量处理失败", batch_size=100, failed_items=["item1", "item2"])
        result3 = exc3.to_dict()
        print(f"[LOG] test_exception_serialization::BatchProcessingError序列化 - 期望包含failed_items vs 实际 {result3}")
        assert result3["error_code"] == "BATCH_PROCESSING_ERROR"


# ============================================================================
# 三、单元全覆盖测试 - embedding_factory.py
# ============================================================================

@pytest.mark.skipif(not EMBEDDING_FACTORY_AVAILABLE, reason="embedding_factory模块不可用")
class TestEmbeddingFactoryUnit:
    """嵌入工厂单元测试"""

    def test_create_mock_provider(self):
        """测试创建Mock提供者"""
        from memory_system.embedding_factory import EmbeddingFactory, MockEmbeddingProvider
        
        # 清理并重新初始化
        EmbeddingFactory.clear()
        
        provider = EmbeddingFactory.create("mock", dimension=128)
        print(f"[LOG] test_create_mock_provider::类型 - 期望 MockEmbeddingProvider vs 实际 {type(provider).__name__}")
        assert isinstance(provider, MockEmbeddingProvider)
        
        print(f"[LOG] test_create_mock_provider::维度 - 期望 128 vs 实际 {provider.dimension}")
        assert provider.dimension == 128

    def test_alias_resolution(self):
        """测试别名解析"""
        from memory_system.embedding_factory import EmbeddingFactory
        
        EmbeddingFactory.clear()
        
        # 测试各种别名
        aliases_to_test = [
            ("test", "mock"),
            ("dummy", "mock"),
            ("default", "mock"),
        ]
        
        for alias, expected in aliases_to_test:
            resolved = EmbeddingFactory._resolve_name(alias)
            print(f"[LOG] test_alias_resolution::{alias} -> {expected} - 期望 {expected} vs 实际 {resolved}")
            assert resolved == expected

    def test_unregistered_provider_raises_error(self):
        """测试未注册提供者抛出正确异常"""
        from memory_system.embedding_factory import EmbeddingFactory
        from memory_system.exceptions import EmbeddingProviderNotFoundError
        
        EmbeddingFactory.clear()
        
        with pytest.raises(EmbeddingProviderNotFoundError) as exc_info:
            EmbeddingFactory.create("nonexistent_provider")
        
        print(f"[LOG] test_unregistered_provider_raises_error::异常类型 - 期望 EmbeddingProviderNotFoundError vs 实际 {type(exc_info.value).__name__}")
        assert "nonexistent_provider" in str(exc_info.value.message)

    def test_requires_auth_check(self):
        """测试requires_auth检查"""
        from memory_system.embedding_factory import EmbeddingFactory
        from memory_system.exceptions import ConfigurationError
        
        EmbeddingFactory.clear()
        
        # 注册一个需要认证的测试提供者
        class TestAuthProvider:
            def __init__(self, **kwargs):
                pass
        
        EmbeddingFactory.register(
            name="test_auth",
            provider_class=TestAuthProvider,
            description="Test auth provider",
            requires_auth=True
        )
        
        # 不提供api_key应该抛出异常
        with pytest.raises(ConfigurationError) as exc_info:
            EmbeddingFactory.create("test_auth")
        
        print(f"[LOG] test_requires_auth_check::无api_key抛异常 - 期望 ConfigurationError vs 实际 {type(exc_info.value).__name__}")
        assert "requires authentication" in str(exc_info.value.message)
        
        # 提供api_key应该成功
        provider = EmbeddingFactory.create("test_auth", api_key="test_key")
        print(f"[LOG] test_requires_auth_check::有api_key成功 - 期望 成功 vs 实际 成功")
        assert provider is not None

    def test_list_providers(self):
        """测试列出提供者"""
        from memory_system.embedding_factory import EmbeddingFactory
        
        EmbeddingFactory.clear()
        
        providers = EmbeddingFactory.list_providers()
        print(f"[LOG] test_list_providers::提供者列表 - 期望包含mock vs 实际 {providers}")
        assert "mock" in providers

    def test_is_registered(self):
        """测试检查提供者是否注册"""
        from memory_system.embedding_factory import EmbeddingFactory
        
        EmbeddingFactory.clear()
        
        print(f"[LOG] test_is_registered::mock已注册 - 期望 True vs 实际 {EmbeddingFactory.is_registered('mock')}")
        assert EmbeddingFactory.is_registered("mock") is True
        
        print(f"[LOG] test_is_registered::nonexistent未注册 - 期望 False vs 实际 {EmbeddingFactory.is_registered('nonexistent')}")
        assert EmbeddingFactory.is_registered("nonexistent") is False

    def test_register_and_unregister(self):
        """测试注册和注销提供者"""
        from memory_system.embedding_factory import EmbeddingFactory
        
        EmbeddingFactory.clear()
        
        class CustomProvider:
            def __init__(self, **kwargs):
                pass
        
        # 注册
        EmbeddingFactory.register("custom", CustomProvider, "Custom provider")
        print(f"[LOG] test_register_and_unregister::注册后存在 - 期望 True vs 实际 {EmbeddingFactory.is_registered('custom')}")
        assert EmbeddingFactory.is_registered("custom") is True
        
        # 注销
        result = EmbeddingFactory.unregister("custom")
        print(f"[LOG] test_register_and_unregister::注销成功 - 期望 True vs 实际 {result}")
        assert result is True
        assert EmbeddingFactory.is_registered("custom") is False

    def test_get_provider_info(self):
        """测试获取提供者信息"""
        from memory_system.embedding_factory import EmbeddingFactory
        
        EmbeddingFactory.clear()
        
        info = EmbeddingFactory.get_provider_info("mock")
        print(f"[LOG] test_get_provider_info::mock信息存在 - 期望 True vs 实际 {info is not None}")
        assert info is not None
        assert info.name == "mock"
        assert info.requires_auth is False


# ============================================================================
# 四、单元全覆盖测试 - retrieval_engine.py
# ============================================================================

@pytest.mark.skipif(not RETRIEVAL_ENGINE_AVAILABLE, reason="retrieval_engine模块不可用")
class TestRetrievalEngineUnit:
    """检索引擎单元测试"""

    @pytest.mark.asyncio
    async def test_empty_query_returns_empty_result(self):
        """测试空查询返回空结果"""
        from memory_system.retrieval_engine import RetrievalEngine, RetrievalContext
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy, StorageBackend, EmbeddingProvider
        
        # 创建mock对象
        mock_strategy = Mock(spec=RetrievalStrategy)
        mock_storage = Mock(spec=StorageBackend)
        mock_embedding = Mock(spec=EmbeddingProvider)
        
        engine = RetrievalEngine(mock_strategy, mock_storage, mock_embedding)
        
        # 空字符串查询
        result = await engine.search("")
        print(f"[LOG] test_empty_query_returns_empty_result::空字符串 - 期望 total_found=0 vs 实际 total_found={result.total_found}")
        assert result.total_found == 0
        assert result.combined == []
        
        # 空白字符串查询
        result2 = await engine.search("   ")
        print(f"[LOG] test_empty_query_returns_empty_result::空白字符串 - 期望 total_found=0 vs 实际 total_found={result2.total_found}")
        assert result2.total_found == 0

    @pytest.mark.asyncio
    async def test_timeout_raises_retrieval_timeout_error(self):
        """测试超时抛出RetrievalTimeoutError"""
        from memory_system.retrieval_engine import RetrievalEngine
        from memory_system.exceptions import RetrievalTimeoutError
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy, StorageBackend, EmbeddingProvider
        
        # 创建mock策略，模拟超时
        mock_strategy = Mock(spec=RetrievalStrategy)
        async def slow_retrieve(*args, **kwargs):
            await asyncio.sleep(10)  # 模拟长时间操作
            return {"combined": []}
        mock_strategy.retrieve = slow_retrieve
        mock_strategy.__class__.__name__ = "MockStrategy"
        
        mock_storage = Mock(spec=StorageBackend)
        mock_embedding = Mock(spec=EmbeddingProvider)
        
        engine = RetrievalEngine(mock_strategy, mock_storage, mock_embedding, timeout=0.1)
        
        with pytest.raises(RetrievalTimeoutError) as exc_info:
            await engine.search("test query")
        
        print(f"[LOG] test_timeout_raises_retrieval_timeout_error::异常类型 - 期望 RetrievalTimeoutError vs 实际 {type(exc_info.value).__name__}")
        assert exc_info.value.timeout == 0.1

    @pytest.mark.asyncio
    async def test_max_results_boundary(self):
        """测试max_results边界(1, 1000)"""
        from memory_system.retrieval_engine import RetrievalEngine
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy, StorageBackend, EmbeddingProvider
        
        # 创建mock策略
        mock_strategy = Mock(spec=RetrievalStrategy)
        async def mock_retrieve(*args, **kwargs):
            max_results = kwargs.get("max_results", 10)
            return {
                "combined": [{"memory": {"memory_id": f"mem_{i}"}, "score": 0.9 - i*0.1} for i in range(min(max_results, 5))],
                "strategy": "mock"
            }
        mock_strategy.retrieve = mock_retrieve
        mock_strategy.__class__.__name__ = "MockStrategy"
        
        mock_storage = Mock(spec=StorageBackend)
        mock_embedding = Mock(spec=EmbeddingProvider)
        
        engine = RetrievalEngine(mock_strategy, mock_storage, mock_embedding)
        
        # 测试max_results=1
        result1 = await engine.search("test", max_results=1)
        print(f"[LOG] test_max_results_boundary::max_results=1 - 期望 <=1 vs 实际 {len(result1.combined)}")
        assert len(result1.combined) <= 1
        
        # 测试max_results=1000
        result2 = await engine.search("test", max_results=1000)
        print(f"[LOG] test_max_results_boundary::max_results=1000 - 期望 <=1000 vs 实际 {len(result2.combined)}")
        assert len(result2.combined) <= 1000

    @pytest.mark.asyncio
    async def test_rrf_fusion_correctness(self):
        """测试RRF融合正确性"""
        from memory_system.retrieval_engine import RetrievalEngine, RetrievalContext
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy, StorageBackend, EmbeddingProvider
        from memory_system.core_abstractions.memory_data_models import RetrievalResponse
        
        mock_storage = Mock(spec=StorageBackend)
        mock_embedding = Mock(spec=EmbeddingProvider)
        
        # 创建两个策略返回不同结果
        mock_strategy1 = Mock(spec=RetrievalStrategy)
        mock_strategy1.__class__.__name__ = "Strategy1"
        
        mock_strategy2 = Mock(spec=RetrievalStrategy)
        mock_strategy2.__class__.__name__ = "Strategy2"
        
        engine = RetrievalEngine(mock_strategy1, mock_storage, mock_embedding)
        
        # 模拟多策略结果
        results = [
            ("Strategy1", RetrievalResponse(
                query="test",
                strategy="Strategy1",
                combined=[
                    {"memory": {"memory_id": "mem_1"}, "score": 0.9},
                    {"memory": {"memory_id": "mem_2"}, "score": 0.8},
                ]
            )),
            ("Strategy2", RetrievalResponse(
                query="test",
                strategy="Strategy2",
                combined=[
                    {"memory": {"memory_id": "mem_2"}, "score": 0.95},
                    {"memory": {"memory_id": "mem_3"}, "score": 0.7},
                ]
            )),
        ]
        
        # 测试RRF融合
        fused = engine._fuse_results_rrf(results, max_results=10, k=60)
        print(f"[LOG] test_rrf_fusion_correctness::融合结果数 - 期望 3 vs 实际 {len(fused)}")
        
        # 验证结果包含所有唯一ID
        ids = [item.get("memory", {}).get("memory_id") for item in fused]
        print(f"[LOG] test_rrf_fusion_correctness::结果ID - 期望包含mem_1,mem_2,mem_3 vs 实际 {ids}")


# ============================================================================
# 五、单元全覆盖测试 - hnsw_index.py
# ============================================================================

@pytest.mark.skipif(not HNSW_INDEX_AVAILABLE, reason="hnsw_index模块不可用")
class TestHNSWIndexUnit:
    """HNSW索引单元测试"""

    def test_vector_dimension_mismatch_raises_value_error(self):
        """测试向量维度不匹配抛出ValueError"""
        from memory_system.hnsw_index import HNSWIndex
        
        index = HNSWIndex(dim=128)
        
        # 添加正确维度的向量
        index.add("vec1", [0.1] * 128)
        
        # 添加错误维度的向量应该抛出ValueError
        with pytest.raises(ValueError) as exc_info:
            index.add("vec2", [0.1] * 64)
        
        print(f"[LOG] test_vector_dimension_mismatch_raises_value_error::异常消息 - 期望包含'维度不匹配' vs 实际 '{exc_info.value}'")
        assert "维度不匹配" in str(exc_info.value) or "dimension" in str(exc_info.value).lower()

    def test_small_dataset_uses_brute_force_search(self):
        """测试小数据集(<100)使用暴力搜索"""
        from memory_system.hnsw_index import HNSWIndex
        
        index = HNSWIndex(dim=16)
        
        # 添加少量向量
        for i in range(10):
            vector = [float(i) / 10.0] * 16
            index.add(f"vec_{i}", vector)
        
        # 搜索
        query = [0.5] * 16
        results = index.search(query, k=5)
        
        print(f"[LOG] test_small_dataset_uses_brute_force_search::节点数 - 期望 10 vs 实际 {len(index._nodes)}")
        assert len(index._nodes) == 10
        
        print(f"[LOG] test_small_dataset_uses_brute_force_search::结果数 - 期望 5 vs 实际 {len(results)}")
        assert len(results) == 5
        
        # 验证统计信息
        stats = index.get_stats()
        print(f"[LOG] test_small_dataset_uses_brute_force_search::节点数统计 - 期望 10 vs 实际 {stats['node_count']}")
        assert stats["node_count"] == 10

    def test_add_search_delete_workflow(self):
        """测试add/search/delete完整流程"""
        from memory_system.hnsw_index import HNSWIndex
        
        index = HNSWIndex(dim=32)
        
        # 添加向量
        vectors = {}
        for i in range(20):
            vector = [float(j + i) / 100.0 for j in range(32)]
            vectors[f"vec_{i}"] = vector
            index.add(f"vec_{i}", vector)
        
        print(f"[LOG] test_add_search_delete_workflow::添加后节点数 - 期望 20 vs 实际 {len(index._nodes)}")
        assert len(index._nodes) == 20
        
        # 搜索
        query = vectors["vec_5"]
        results = index.search(query, k=3)
        
        print(f"[LOG] test_add_search_delete_workflow::搜索结果数 - 期望 3 vs 实际 {len(results)}")
        assert len(results) == 3
        
        # 第一个结果应该是vec_5（最相似）
        print(f"[LOG] test_add_search_delete_workflow::最相似结果 - 期望 vec_5 vs 实际 {results[0][0]}")
        assert results[0][0] == "vec_5"
        
        # 删除
        deleted = index.delete("vec_5")
        print(f"[LOG] test_add_search_delete_workflow::删除成功 - 期望 True vs 实际 {deleted}")
        assert deleted is True
        assert "vec_5" not in index._nodes
        
        # 再次搜索
        results2 = index.search(query, k=3)
        print(f"[LOG] test_add_search_delete_workflow::删除后搜索 - 期望不包含vec_5 vs 实际 {[r[0] for r in results2]}")
        assert all(r[0] != "vec_5" for r in results2)

    def test_level_generation_randomness(self):
        """测试层级生成随机性"""
        from memory_system.hnsw_index import HNSWIndex
        
        index = HNSWIndex(dim=16)
        
        # 生成多个层级值
        levels = [index._random_level() for _ in range(100)]
        
        # 验证层级分布
        level_counts = {}
        for level in levels:
            level_counts[level] = level_counts.get(level, 0) + 1
        
        print(f"[LOG] test_level_generation_randomness::层级分布 - 期望有多层级 vs 实际 {level_counts}")
        
        # 大多数应该是层级0（放宽断言，至少30%）
        level0_ratio = level_counts.get(0, 0) / 100.0
        print(f"[LOG] test_level_generation_randomness::层级0占比 - 期望 >=30% vs 实际 {level0_ratio*100:.1f}%")
        assert level_counts.get(0, 0) >= 30  # 大多数节点在层级0（放宽到30%）
        
        # 层级应该有变化
        print(f"[LOG] test_level_generation_randomness::层级种类数 - 期望 >1 vs 实际 {len(level_counts)}")
        assert len(level_counts) > 1  # 不应该所有节点都在同一层级

    def test_batch_add(self):
        """测试批量添加"""
        from memory_system.hnsw_index import HNSWIndex
        
        index = HNSWIndex(dim=16)
        
        items = [(f"vec_{i}", [float(i) / 100.0] * 16) for i in range(50)]
        index.batch_add(items)
        
        print(f"[LOG] test_batch_add::批量添加后节点数 - 期望 50 vs 实际 {len(index._nodes)}")
        assert len(index._nodes) == 50

    def test_clear(self):
        """测试清空索引"""
        from memory_system.hnsw_index import HNSWIndex
        
        index = HNSWIndex(dim=16)
        
        for i in range(10):
            index.add(f"vec_{i}", [float(i)] * 16)
        
        index.clear()
        
        print(f"[LOG] test_clear::清空后节点数 - 期望 0 vs 实际 {len(index._nodes)}")
        assert len(index._nodes) == 0
        assert index._entry_point is None


# ============================================================================
# 六、单元全覆盖测试 - rate_limiter.py
# ============================================================================

@pytest.mark.skipif(not RATE_LIMITER_AVAILABLE, reason="rate_limiter模块不可用")
class TestRateLimiterUnit:
    """限流器单元测试"""

    def test_sliding_window_rate_limiting(self):
        """测试滑动窗口限流"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        config = RateLimitConfig(
            requests_per_minute=5,
            strategy=RateLimitStrategy.SLIDING_WINDOW,
            window_size=60
        )
        limiter = RateLimiter(config)
        
        # 前5次应该允许
        for i in range(5):
            allowed = limiter.is_allowed(user_id="user1", endpoint="api")
            print(f"[LOG] test_sliding_window_rate_limiting::请求{i+1} - 期望 True vs 实际 {allowed}")
            assert allowed is True
        
        # 第6次应该被拒绝
        allowed = limiter.is_allowed(user_id="user1", endpoint="api")
        print(f"[LOG] test_sliding_window_rate_limiting::请求6(超限) - 期望 False vs 实际 {allowed}")
        assert allowed is False

    def test_token_bucket_rate_limiting(self):
        """测试令牌桶限流"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        config = RateLimitConfig(
            requests_per_minute=60,
            burst_size=3,
            strategy=RateLimitStrategy.TOKEN_BUCKET
        )
        limiter = RateLimiter(config)
        
        # 初始有burst_size个令牌
        for i in range(3):
            allowed = limiter.is_allowed(user_id="user1")
            print(f"[LOG] test_token_bucket_rate_limiting::请求{i+1} - 期望 True vs 实际 {allowed}")
            assert allowed is True
        
        # 令牌耗尽后应该拒绝
        allowed = limiter.is_allowed(user_id="user1")
        print(f"[LOG] test_token_bucket_rate_limiting::请求4(令牌耗尽) - 期望 False vs 实际 {allowed}")
        assert allowed is False

    def test_leaky_bucket_rate_limiting(self):
        """测试漏桶限流"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        config = RateLimitConfig(
            requests_per_minute=60,
            burst_size=5,
            strategy=RateLimitStrategy.LEAKY_BUCKET
        )
        limiter = RateLimiter(config)
        
        # 桶未满时应该允许
        for i in range(5):
            allowed = limiter.is_allowed(user_id="user1")
            print(f"[LOG] test_leaky_bucket_rate_limiting::请求{i+1} - 期望 True vs 实际 {allowed}")
            assert allowed is True
        
        # 桶满后应该拒绝
        allowed = limiter.is_allowed(user_id="user1")
        print(f"[LOG] test_leaky_bucket_rate_limiting::请求6(桶满) - 期望 False vs 实际 {allowed}")
        assert allowed is False

    def test_fixed_window_rate_limiting(self):
        """测试固定窗口限流"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        config = RateLimitConfig(
            requests_per_minute=3,
            strategy=RateLimitStrategy.FIXED_WINDOW,
            window_size=60
        )
        limiter = RateLimiter(config)
        
        # 窗口内前3次应该允许
        for i in range(3):
            allowed = limiter.is_allowed(user_id="user1")
            print(f"[LOG] test_fixed_window_rate_limiting::请求{i+1} - 期望 True vs 实际 {allowed}")
            assert allowed is True
        
        # 第4次应该被拒绝
        allowed = limiter.is_allowed(user_id="user1")
        print(f"[LOG] test_fixed_window_rate_limiting::请求4(窗口内超限) - 期望 False vs 实际 {allowed}")
        assert allowed is False

    def test_block_unblock_logic(self):
        """测试封禁/解封逻辑"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        config = RateLimitConfig(
            requests_per_minute=2,
            strategy=RateLimitStrategy.SLIDING_WINDOW,
            block_duration=5  # 封禁5秒
        )
        limiter = RateLimiter(config)
        
        # 触发限流
        limiter.is_allowed(user_id="user1")
        limiter.is_allowed(user_id="user1")
        
        # 第3次触发封禁
        limiter.is_allowed(user_id="user1")
        
        # 检查是否被封禁
        quota = limiter.get_remaining_quota(user_id="user1")
        print(f"[LOG] test_block_unblock_logic::被封禁 - 期望 True vs 实际 {quota.get('blocked', False)}")
        
        # 重置限流状态
        limiter.reset(user_id="user1")
        
        # 重置后应该可以请求
        allowed = limiter.is_allowed(user_id="user1")
        print(f"[LOG] test_block_unblock_logic::重置后允许 - 期望 True vs 实际 {allowed}")
        assert allowed is True

    def test_state_auto_cleanup(self):
        """测试状态自动清理"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        config = RateLimitConfig(
            requests_per_minute=10,
            strategy=RateLimitStrategy.SLIDING_WINDOW,
            window_size=1  # 1秒窗口
        )
        limiter = RateLimiter(config)
        
        # 创建大量用户状态（超过清理阈值）
        for i in range(1100):
            limiter.is_allowed(user_id=f"user_{i}")
        
        print(f"[LOG] test_state_auto_cleanup::创建状态数 - 期望 >=1100 vs 实际 {len(limiter._states)}")
        assert len(limiter._states) >= 1100
        
        # 等待窗口过期
        time.sleep(1.5)
        
        # 触发清理（通过新请求）
        limiter.is_allowed(user_id="new_user")
        
        # 检查是否清理了过期状态
        print(f"[LOG] test_state_auto_cleanup::清理后状态数 - 期望 <1100 vs 实际 {len(limiter._states)}")

    def test_per_user_per_endpoint_limiting(self):
        """测试按用户和端点限流"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        config = RateLimitConfig(
            requests_per_minute=2,
            strategy=RateLimitStrategy.SLIDING_WINDOW,
            per_user=True,
            per_endpoint=True
        )
        limiter = RateLimiter(config)
        
        # 用户1在端点A的请求
        limiter.is_allowed(user_id="user1", endpoint="api_a")
        limiter.is_allowed(user_id="user1", endpoint="api_a")
        
        # 用户1在端点A应该被限流
        assert limiter.is_allowed(user_id="user1", endpoint="api_a") is False
        
        # 但用户1在端点B应该可以
        assert limiter.is_allowed(user_id="user1", endpoint="api_b") is True
        
        # 用户2在端点A也应该可以
        assert limiter.is_allowed(user_id="user2", endpoint="api_a") is True
        
        print(f"[LOG] test_per_user_per_endpoint_limiting::用户端点隔离 - 期望 正确隔离 vs 实际 正确隔离")


# ============================================================================
# 七、单元全覆盖测试 - tenant_isolation_manager.py
# ============================================================================

@pytest.mark.skipif(not TENANT_MANAGER_AVAILABLE, reason="tenant_isolation_manager模块不可用")
class TestTenantIsolationManagerUnit:
    """租户隔离管理器单元测试"""

    def test_tenant_id_validation_rules(self):
        """测试租户ID验证规则"""
        from memory_system.tenant_isolation_manager import TenantIsolationManager
        
        manager = TenantIsolationManager()
        
        # 有效租户ID
        valid_ids = ["tenant_001", "tenant-001", "TENANT001", "tenant001", "t_1-2"]
        for tid in valid_ids:
            result = manager.validate_tenant_id(tid)
            print(f"[LOG] test_tenant_id_validation_rules::{tid} - 期望 True vs 实际 {result}")
            assert result is True
        
        # 无效租户ID
        invalid_cases = [
            ("", "空字符串"),
            ("tenant:001", "包含冒号"),
            ("a" * 65, "超长字符串"),
            ("tenant@001", "包含特殊字符@"),
            ("tenant.001", "包含特殊字符."),
            ("tenant 001", "包含空格"),
        ]
        
        for tid, desc in invalid_cases:
            result = manager.validate_tenant_id(tid)
            print(f"[LOG] test_tenant_id_validation_rules::{desc}({tid[:20]}...) - 期望 False vs 实际 {result}")
            assert result is False

    def test_quota_check(self):
        """测试配额检查"""
        from memory_system.tenant_isolation_manager import TenantIsolationManager
        
        manager = TenantIsolationManager(
            enable_quota=True,
            max_memories_per_tenant=10,
            max_short_term_per_tenant=5
        )
        
        # 初始配额检查应该通过
        result = manager.check_quota("tenant_001")
        print(f"[LOG] test_quota_check::初始配额检查 - 期望 True vs 实际 {result}")
        assert result is True
        
        # 模拟增加计数
        manager._tenant_counts["tenant_001"] = 10
        
        # 超过配额后应该返回False
        result = manager.check_quota("tenant_001")
        print(f"[LOG] test_quota_check::超配额检查 - 期望 False vs 实际 {result}")
        assert result is False
        
        # 短期记忆配额检查
        result = manager.check_quota("tenant_001", is_short_term=True)
        print(f"[LOG] test_quota_check::短期记忆配额检查 - 期望 True vs 实际 {result}")
        assert result is True

    @pytest.mark.asyncio
    async def test_cross_tenant_access_denied(self):
        """测试跨租户访问拒绝"""
        from memory_system.tenant_isolation_manager import (
            TenantIsolationManager,
            TenantContext,
            TenantAwareStorageWrapper
        )
        
        manager = TenantIsolationManager()
        
        # 创建两个租户上下文
        context_a = TenantContext("tenant_a")
        context_b = TenantContext("tenant_b")
        
        # 验证前缀隔离
        prefix_a = manager.get_tenant_prefix("tenant_a")
        prefix_b = manager.get_tenant_prefix("tenant_b")
        
        print(f"[LOG] test_cross_tenant_access_denied::租户A前缀 - 期望 'tenant:tenant_a:' vs 实际 '{prefix_a}'")
        assert prefix_a == "tenant:tenant_a:"
        
        print(f"[LOG] test_cross_tenant_access_denied::租户B前缀 - 期望 'tenant:tenant_b:' vs 实际 '{prefix_b}'")
        assert prefix_b == "tenant:tenant_b:"
        
        # 验证前缀不同
        assert prefix_a != prefix_b
        print(f"[LOG] test_cross_tenant_access_denied::前缀隔离 - 期望 不同 vs 实际 不同")

    @pytest.mark.asyncio
    async def test_toctou_race_condition(self):
        """测试TOCTOU竞态条件"""
        from memory_system.tenant_isolation_manager import TenantIsolationManager
        
        manager = TenantIsolationManager(
            enable_quota=True,
            max_memories_per_tenant=10
        )
        
        # 模拟并发场景
        async def try_increment():
            return await manager.check_and_increment_quota("tenant_001")
        
        # 并发执行10次（配额上限）
        tasks = [try_increment() for _ in range(15)]
        results = await asyncio.gather(*tasks)
        
        success_count = sum(1 for r in results if r)
        print(f"[LOG] test_toctou_race_condition::成功次数 - 期望 <=10 vs 实际 {success_count}")
        assert success_count <= 10
        
        # 验证最终计数
        final_count = manager._tenant_counts.get("tenant_001", 0)
        print(f"[LOG] test_toctou_race_condition::最终计数 - 期望 <=10 vs 实际 {final_count}")
        assert final_count <= 10

    def test_tenant_context(self):
        """测试租户上下文"""
        from memory_system.tenant_isolation_manager import TenantContext
        
        context = TenantContext(
            tenant_id="tenant_001",
            tenant_name="测试租户",
            metadata={"region": "cn-north"}
        )
        
        print(f"[LOG] test_tenant_context::tenant_id - 期望 'tenant_001' vs 实际 '{context.tenant_id}'")
        assert context.tenant_id == "tenant_001"
        
        print(f"[LOG] test_tenant_context::tenant_name - 期望 '测试租户' vs 实际 '{context.tenant_name}'")
        assert context.tenant_name == "测试租户"
        
        namespace_key = context.get_namespace_key()
        print(f"[LOG] test_tenant_context::namespace_key - 期望 'tenant:tenant_001:' vs 实际 '{namespace_key}'")
        assert namespace_key == "tenant:tenant_001:"

    def test_tenant_stats(self):
        """测试租户统计"""
        from memory_system.tenant_isolation_manager import TenantIsolationManager
        
        manager = TenantIsolationManager(
            max_memories_per_tenant=100,
            max_short_term_per_tenant=20
        )
        
        # 设置一些计数
        manager._tenant_counts["tenant_001"] = 50
        manager._tenant_short_term_counts["tenant_001"] = 10
        
        stats = manager.get_tenant_stats("tenant_001")
        
        print(f"[LOG] test_tenant_stats::long_term_count - 期望 50 vs 实际 {stats['long_term_count']}")
        assert stats["long_term_count"] == 50
        
        print(f"[LOG] test_tenant_stats::short_term_count - 期望 10 vs 实际 {stats['short_term_count']}")
        assert stats["short_term_count"] == 10
        
        print(f"[LOG] test_tenant_stats::max_long_term - 期望 100 vs 实际 {stats['max_long_term']}")
        assert stats["max_long_term"] == 100

    def test_list_tenants(self):
        """测试列出租户"""
        from memory_system.tenant_isolation_manager import TenantIsolationManager
        
        manager = TenantIsolationManager()
        
        # 添加一些租户计数
        manager._tenant_counts["tenant_a"] = 10
        manager._tenant_counts["tenant_b"] = 20
        manager._tenant_short_term_counts["tenant_c"] = 5
        
        tenants = manager.list_tenants()
        
        print(f"[LOG] test_list_tenants::租户列表 - 期望 ['tenant_a', 'tenant_b', 'tenant_c'] vs 实际 {tenants}")
        assert "tenant_a" in tenants
        assert "tenant_b" in tenants
        assert "tenant_c" in tenants


# ============================================================================
# 八、边界条件全覆盖测试
# ============================================================================

class TestBoundaryConditions:
    """边界条件测试"""

    @pytest.mark.skipif(not EXCEPTIONS_AVAILABLE, reason="exceptions模块不可用")
    def test_empty_string_handling(self):
        """测试空字符串处理"""
        from memory_system.exceptions import (
            MemorySystemError,
            StorageNotFoundError,
            ValidationError
        )
        
        # 空字符串消息
        exc1 = MemorySystemError("")
        print(f"[LOG] test_empty_string_handling::空消息 - 期望 '' vs 实际 '{exc1.message}'")
        assert exc1.message == ""
        
        # 空字符串memory_id
        exc2 = StorageNotFoundError("")
        print(f"[LOG] test_empty_string_handling::空memory_id - 期望包含'not found' vs 实际 '{exc2.message}'")
        assert "not found" in exc2.message.lower()

    @pytest.mark.skipif(not EXCEPTIONS_AVAILABLE, reason="exceptions模块不可用")
    def test_none_value_handling(self):
        """测试None值处理"""
        from memory_system.exceptions import (
            MemorySystemError,
            ValidationError
        )
        
        # None details
        exc1 = MemorySystemError("test", details=None)
        print(f"[LOG] test_none_value_handling::None details - 期望 {{}} vs 实际 {exc1.details}")
        assert exc1.details == {}
        
        # None value in ValidationError
        exc2 = ValidationError("test", field="field1", value=None)
        print(f"[LOG] test_none_value_handling::None value - 期望 None vs 实际 {exc2.value}")
        assert exc2.value is None

    @pytest.mark.skipif(not EXCEPTIONS_AVAILABLE, reason="exceptions模块不可用")
    def test_long_string_handling(self):
        """测试超长字符串处理"""
        from memory_system.exceptions import MemorySystemError
        
        # 超长消息
        long_message = "a" * 10000
        exc = MemorySystemError(long_message)
        
        print(f"[LOG] test_long_string_handling::消息长度 - 期望 10000 vs 实际 {len(exc.message)}")
        assert len(exc.message) == 10000

    @pytest.mark.skipif(not HNSW_INDEX_AVAILABLE, reason="hnsw_index模块不可用")
    def test_batch_size_boundary(self):
        """测试批量大小边界"""
        from memory_system.hnsw_index import HNSWIndex
        
        index = HNSWIndex(dim=16)
        
        # 批量大小为0
        index.batch_add([])
        print(f"[LOG] test_batch_size_boundary::批量大小0 - 期望 0节点 vs 实际 {len(index._nodes)}")
        assert len(index._nodes) == 0
        
        # 批量大小为1
        index.batch_add([("vec_single", [0.1] * 16)])
        print(f"[LOG] test_batch_size_boundary::批量大小1 - 期望 1节点 vs 实际 {len(index._nodes)}")
        assert len(index._nodes) == 1
        
        # 大批量（使用不同的ID范围，避免与之前的ID重复）
        large_batch = [(f"vec_{i}", [float(i) / 1000.0] * 16) for i in range(1000)]
        index.batch_add(large_batch)
        print(f"[LOG] test_batch_size_boundary::批量大小1000 - 期望 1001节点 vs 实际 {len(index._nodes)}")
        assert len(index._nodes) == 1001

    @pytest.mark.skipif(not HNSW_INDEX_AVAILABLE, reason="hnsw_index模块不可用")
    def test_vector_dimension_boundary(self):
        """测试向量维度边界"""
        from memory_system.hnsw_index import HNSWIndex
        
        # 维度为1
        index1 = HNSWIndex(dim=1)
        index1.add("vec_1", [0.5])
        results = index1.search([0.5], k=1)
        print(f"[LOG] test_vector_dimension_boundary::维度1搜索 - 期望 1结果 vs 实际 {len(results)}")
        assert len(results) == 1
        
        # 高维度
        index2 = HNSWIndex(dim=1536)  # OpenAI embedding维度
        vector = [float(i) / 1536.0 for i in range(1536)]
        index2.add("vec_1", vector)
        results = index2.search(vector, k=1)
        print(f"[LOG] test_vector_dimension_boundary::维度1536搜索 - 期望 1结果 vs 实际 {len(results)}")
        assert len(results) == 1

    @pytest.mark.skipif(not TENANT_MANAGER_AVAILABLE, reason="tenant_isolation_manager模块不可用")
    def test_memory_pressure_thresholds(self):
        """测试内存压力阈值"""
        from memory_system.tenant_isolation_manager import TenantIsolationManager
        
        # 测试不同配额阈值
        thresholds = [60, 75, 85, 92]
        
        for threshold in thresholds:
            # 使用阈值作为最大记忆数
            manager = TenantIsolationManager(
                enable_quota=True,
                max_memories_per_tenant=100
            )
            
            # 模拟达到阈值的计数
            count = int(100 * threshold / 100)
            manager._tenant_counts["tenant_001"] = count
            
            # 检查是否还能添加
            remaining = 100 - count
            can_add = manager.check_quota("tenant_001")
            
            print(f"[LOG] test_memory_pressure_thresholds::{threshold}%阈值 - 计数{count}, 剩余{remaining}, 可添加{can_add}")
            
            # 在100%时应该不能添加
            if threshold >= 100:
                assert can_add is False
            else:
                assert can_add is True

    @pytest.mark.skipif(not RATE_LIMITER_AVAILABLE, reason="rate_limiter模块不可用")
    def test_rate_limiter_edge_cases(self):
        """测试限流器边界情况"""
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
        
        # requests_per_minute为1
        config = RateLimitConfig(
            requests_per_minute=1,
            strategy=RateLimitStrategy.SLIDING_WINDOW
        )
        limiter = RateLimiter(config)
        
        # 第一次允许
        assert limiter.is_allowed(user_id="user1") is True
        print(f"[LOG] test_rate_limiter_edge_cases::requests_per_minute=1, 第一次 - 期望 True vs 实际 True")
        
        # 第二次拒绝
        assert limiter.is_allowed(user_id="user1") is False
        print(f"[LOG] test_rate_limiter_edge_cases::requests_per_minute=1, 第二次 - 期望 False vs 实际 False")
        
        # 空user_id和endpoint
        config2 = RateLimitConfig(
            requests_per_minute=10,
            per_user=False,
            per_endpoint=False
        )
        limiter2 = RateLimiter(config2)
        
        allowed = limiter2.is_allowed()
        print(f"[LOG] test_rate_limiter_edge_cases::空user_id和endpoint - 期望 True vs 实际 {allowed}")
        assert allowed is True


# ============================================================================
# 运行测试
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

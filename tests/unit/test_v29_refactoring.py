"""V2.29.0 重构项目单元测试

测试所有重构项目的功能模块。
"""

import pytest
import asyncio
import time
import json
from datetime import datetime
from typing import List, Dict, Any

# 测试所有重构模块
from memory_system.exceptions import (
    MemorySystemError,
    StorageError,
    StorageNotFoundError,
    RetrievalError,
    ValidationError,
    ConfigurationError,
)
from memory_system.metrics_collector import (
    MetricsCollector,
    MetricRecord,
    MetricDecorator,
)
from memory_system.validators import (
    MemoryConfigValidator,
    TenantConfigValidator,
    RetrievalConfigValidator,
    SystemConfigValidator,
)
from memory_system.embedding_factory import (
    EmbeddingFactory,
    MockEmbeddingProvider,
)
from memory_system.retrieval_utils import (
    RetrievalResultProcessor,
    ProcessingConfig,
    cosine_similarity,
    euclidean_distance,
)
from memory_system.ttl_storage_interface import (
    InMemoryTTLStorage,
    TTLEntry,
)
from memory_system.structured_logging_config import (
    StructuredLogFormatter,
    LogContext,
)
from memory_system.snapshot_serializer import (
    JSONSerializer,
    MsgPackSerializer,
    SnapshotSerializerManager,
)
from memory_system.di_container import (
    DIContainer,
    Scope,
)
from memory_system.batch_processor import (
    BatchProcessor,
    ParallelBatchProcessor,
    BatchStatus,
)


# ============================================================================
# R-01: 统一异常层次结构测试
# ============================================================================

class TestExceptions:
    """测试统一异常层次结构"""
    
    def test_base_exception(self):
        """测试基础异常"""
        exc = MemorySystemError("Test error", error_code="TEST_001", details={"key": "value"})
        assert exc.message == "Test error"
        assert exc.error_code == "TEST_001"
        assert exc.details == {"key": "value"}
        
        # 测试 to_dict
        d = exc.to_dict()
        assert d["error_code"] == "TEST_001"
        assert d["message"] == "Test error"
    
    def test_storage_error(self):
        """测试存储异常"""
        exc = StorageNotFoundError("mem_123", backend="chroma")
        assert exc.memory_id == "mem_123"
        assert exc.backend == "chroma"
        assert "mem_123" in exc.message
    
    def test_retrieval_error(self):
        """测试检索异常"""
        exc = RetrievalError("Search failed", strategy="hybrid")
        assert exc.strategy == "hybrid"
        assert "Search failed" in str(exc)
    
    def test_validation_error(self):
        """测试验证异常"""
        exc = ValidationError("Invalid field", field="content", value="bad")
        assert exc.field == "content"
        assert exc.value == "bad"
    
    def test_configuration_error(self):
        """测试配置异常"""
        exc = ConfigurationError("Bad config", config_key="storage.path")
        assert exc.config_key == "storage.path"


# ============================================================================
# R-02: 检索引擎测试
# ============================================================================

class TestRetrievalEngine:
    """测试检索引擎"""
    
    @pytest.fixture
    def mock_strategy(self):
        """模拟检索策略"""
        class MockStrategy:
            async def retrieve(self, query, storage, embedding, max_results, context):
                return {
                    "strategy": "mock",
                    "combined": [{"memory": {"memory_id": "1"}, "score": 0.9, "layer": "test"}],
                    "layers": {"test": []}
                }
        return MockStrategy()
    
    @pytest.fixture
    def mock_storage(self):
        """模拟存储后端"""
        class MockStorage:
            pass
        return MockStorage()
    
    @pytest.fixture
    def mock_embedding(self):
        """模拟嵌入提供者"""
        class MockEmbedding:
            @property
            def dimension(self):
                return 384
            
            async def embed(self, text):
                return [0.1] * 384
        return MockEmbedding()
    
    @pytest.mark.asyncio
    async def test_search(self, mock_strategy, mock_storage, mock_embedding):
        """测试搜索功能"""
        from memory_system.retrieval_engine import RetrievalEngine, RetrievalContext
        
        engine = RetrievalEngine(
            strategy=mock_strategy,
            storage=mock_storage,
            embedding=mock_embedding,
            timeout=30.0
        )
        
        context = RetrievalContext(session_id="test_session")
        response = await engine.search("test query", max_results=5, context=context)
        
        assert response.query == "test query"
        assert response.strategy == "mock"
        assert len(response.combined) == 1
        assert response.retrieval_time >= 0
    
    def test_retrieval_context(self):
        """测试检索上下文"""
        from memory_system.retrieval_engine import RetrievalContext
        
        context = RetrievalContext(
            short_term_memories=[],
            session_id="sess_1",
            user_id="user_1",
            filters={"type": "test"},
            previous_queries=["q1", "q2"]
        )
        
        d = context.to_dict()
        assert d["session_id"] == "sess_1"
        assert d["user_id"] == "user_1"
        assert d["filters"]["type"] == "test"


# ============================================================================
# R-03: 指标收集器测试
# ============================================================================

class TestMetricsCollector:
    """测试指标收集器"""
    
    @pytest.fixture
    def collector(self):
        """创建指标收集器"""
        # 重置单例状态
        MetricsCollector._instance = None
        return MetricsCollector()
    
    def test_record_metric(self, collector):
        """测试记录指标"""
        collector.record("test_metric", 1.5, label1="value1")
        
        metrics = collector.get_metrics("test_metric")
        assert len(metrics) == 1
        assert metrics[0].value == 1.5
        assert metrics[0].labels["label1"] == "value1"
    
    def test_counter(self, collector):
        """测试计数器"""
        collector.increment("counter1")
        collector.increment("counter1", 5)
        
        assert collector.get_counter("counter1") == 6
    
    def test_gauge(self, collector):
        """测试仪表盘"""
        collector.gauge("gauge1", 42.0)
        assert collector.get_gauge("gauge1") == 42.0
        
        collector.gauge("gauge1", 100.0)
        assert collector.get_gauge("gauge1") == 100.0
    
    def test_histogram(self, collector):
        """测试直方图"""
        collector.histogram("hist1", 0.5)
        collector.histogram("hist1", 1.0)
        collector.histogram("hist1", 1.5)
        
        snapshot = collector.get_histogram("hist1")
        assert snapshot is not None
        assert snapshot["count"] == 3
    
    @pytest.mark.asyncio
    async def test_timeit_async(self, collector):
        """测试异步计时"""
        async with collector.timeit_async("async_op"):
            await asyncio.sleep(0.01)
        
        metrics = collector.get_metrics("async_op")
        assert len(metrics) == 1
        assert metrics[0].value > 0
    
    def test_aggregation(self, collector):
        """测试聚合"""
        for i in range(10):
            collector.record("agg_metric", float(i))
        
        assert collector.aggregate("agg_metric", "avg") == 4.5
        assert collector.aggregate("agg_metric", "sum") == 45.0
        assert collector.aggregate("agg_metric", "min") == 0.0
        assert collector.aggregate("agg_metric", "max") == 9.0
        assert collector.aggregate("agg_metric", "count") == 10


# ============================================================================
# R-04: 验证器测试
# ============================================================================

class TestValidators:
    """测试验证器"""
    
    def test_memory_config_validator(self):
        """测试内存配置验证器"""
        config = MemoryConfigValidator(
            max_content_length=50000,
            max_batch_size=500
        )
        assert config.max_content_length == 50000
        assert config.max_batch_size == 500
    
    def test_memory_config_validation_error(self):
        """测试内存配置验证错误"""
        with pytest.raises(ValueError):
            MemoryConfigValidator(max_content_length=50)  # 小于最小值
    
    def test_tenant_config_validator(self):
        """测试租户配置验证器"""
        config = TenantConfigValidator(
            tenant_id="tenant_123",
            quota=1000000,
            max_memories=10000
        )
        assert config.tenant_id == "tenant_123"
        assert config.quota == 1000000
    
    def test_tenant_id_validation(self):
        """测试租户ID验证"""
        with pytest.raises(ValueError):
            TenantConfigValidator(tenant_id="ab")  # 太短
        
        with pytest.raises(ValueError):
            TenantConfigValidator(tenant_id="invalid@id")  # 非法字符
    
    def test_retrieval_config_validator(self):
        """测试检索配置验证器"""
        config = RetrievalConfigValidator(
            strategy="hybrid",
            max_results=20,
            vector_weight=0.7,
            keyword_weight=0.3
        )
        assert config.strategy == "hybrid"
        assert config.max_results == 20
    
    def test_retrieval_weights_validation(self):
        """测试检索权重验证"""
        with pytest.raises(ValueError):
            RetrievalConfigValidator(
                vector_weight=0.6,
                keyword_weight=0.5  # 和不等于1
            )


# ============================================================================
# R-05: 嵌入工厂测试
# ============================================================================

class TestEmbeddingFactory:
    """测试嵌入工厂"""
    
    def test_factory_singleton(self):
        """测试工厂单例"""
        factory1 = EmbeddingFactory
        factory2 = EmbeddingFactory
        assert factory1 is factory2
    
    def test_register_provider(self):
        """测试注册提供者"""
        class TestProvider:
            def __init__(self, **kwargs):
                self.config = kwargs
        
        EmbeddingFactory.register("test", TestProvider)
        assert "test" in EmbeddingFactory.list_providers()
    
    def test_create_provider_not_found(self):
        """测试创建不存在的提供者"""
        from memory_system.exceptions import EmbeddingProviderNotFoundError
        
        with pytest.raises(EmbeddingProviderNotFoundError):
            EmbeddingFactory.create("nonexistent")
    
    @pytest.mark.asyncio
    async def test_mock_embedding_provider(self):
        """测试Mock嵌入提供者"""
        provider = MockEmbeddingProvider(dimension=128)
        
        assert provider.dimension == 128
        
        embedding = await provider.embed("test text")
        assert len(embedding) == 128
        
        # 测试一致性（相同输入应产生相同输出）
        embedding2 = await provider.embed("test text")
        assert embedding == embedding2
        
        # 测试相似度
        sim = await provider.similarity(embedding, embedding2)
        assert sim == pytest.approx(1.0, abs=0.01)


# ============================================================================
# R-06: 检索工具测试
# ============================================================================

class TestRetrievalUtils:
    """测试检索工具"""
    
    @pytest.fixture
    def sample_results(self):
        """创建示例检索结果"""
        from memory_system.core_abstractions.memory_data_models import MemoryItem, RetrievalResult
        
        results = []
        for i in range(5):
            memory = MemoryItem(
                memory_id=f"mem_{i}",
                content=f"Content {i}",
                embedding=[0.1 * i] * 384
            )
            results.append(RetrievalResult(
                memory=memory,
                score=0.9 - i * 0.1,
                layer="test"
            ))
        return results
    
    def test_retrieval_result_processor(self, sample_results):
        """测试检索结果处理器"""
        config = ProcessingConfig(max_results=3, deduplicate=True)
        processor = RetrievalResultProcessor(config)
        
        results = processor.process(sample_results)
        
        assert len(results) == 3
        assert results[0]["score"] >= results[1]["score"]  # 已排序
    
    def test_merge_and_rank(self, sample_results):
        """测试合并排序"""
        results = RetrievalResultProcessor.merge_and_rank(
            sample_results,
            max_results=3,
            deduplicate=True
        )
        
        assert len(results) == 3
        assert results[0]["score"] >= results[1]["score"]
    
    def test_cosine_similarity(self):
        """测试余弦相似度"""
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        vec3 = [0.0, 1.0, 0.0]
        
        assert cosine_similarity(vec1, vec2) == pytest.approx(1.0)
        assert cosine_similarity(vec1, vec3) == pytest.approx(0.0)
    
    def test_euclidean_distance(self):
        """测试欧几里得距离"""
        vec1 = [0.0, 0.0, 0.0]
        vec2 = [3.0, 4.0, 0.0]
        
        assert euclidean_distance(vec1, vec2) == pytest.approx(5.0)


# ============================================================================
# R-07: TTL存储接口测试
# ============================================================================

class TestTTLStorageInterface:
    """测试TTL存储接口"""
    
    @pytest.fixture
    async def ttl_storage(self):
        """创建TTL存储"""
        storage = InMemoryTTLStorage()
        return storage
    
    @pytest.mark.asyncio
    async def test_add_and_get_ttl_entry(self):
        """测试添加和获取TTL条目"""
        storage = InMemoryTTLStorage()
        
        expiration = time.time() + 3600
        result = await storage.add_ttl_entry("mem_1", expiration, {"type": "test"})
        assert result is True
        
        entry = await storage.get_ttl_entry("mem_1")
        assert entry is not None
        assert entry.memory_id == "mem_1"
        assert entry.expiration_time == expiration
    
    @pytest.mark.asyncio
    async def test_get_expired_entries(self):
        """测试获取过期条目"""
        storage = InMemoryTTLStorage()
        
        # 添加已过期条目
        past_time = time.time() - 100
        await storage.add_ttl_entry("expired_1", past_time)
        await storage.add_ttl_entry("expired_2", past_time)
        
        # 添加未过期条目
        future_time = time.time() + 3600
        await storage.add_ttl_entry("active_1", future_time)
        
        expired = await storage.get_expired_entries()
        assert len(expired) == 2
        assert "expired_1" in expired
        assert "expired_2" in expired
    
    @pytest.mark.asyncio
    async def test_extend_ttl(self):
        """测试延长TTL"""
        storage = InMemoryTTLStorage()
        
        old_expiration = time.time() + 3600
        await storage.add_ttl_entry("mem_1", old_expiration)
        
        new_expiration = time.time() + 7200
        result = await storage.extend_ttl("mem_1", new_expiration)
        assert result is True
        
        entry = await storage.get_ttl_entry("mem_1")
        assert entry.expiration_time == new_expiration
    
    @pytest.mark.asyncio
    async def test_remove_ttl_entry(self):
        """测试移除TTL条目"""
        storage = InMemoryTTLStorage()
        
        await storage.add_ttl_entry("mem_1", time.time() + 3600)
        
        result = await storage.remove_ttl_entry("mem_1")
        assert result is True
        
        entry = await storage.get_ttl_entry("mem_1")
        assert entry is None
    
    @pytest.mark.asyncio
    async def test_count_entries(self):
        """测试统计条目"""
        storage = InMemoryTTLStorage()
        
        await storage.add_ttl_entry("mem_1", time.time() + 3600)
        await storage.add_ttl_entry("mem_2", time.time() + 3600)
        await storage.add_ttl_entry("mem_3", time.time() - 100)  # 已过期
        
        assert await storage.count_entries(include_expired=True) == 3
        assert await storage.count_entries(include_expired=False) == 2


# ============================================================================
# R-08: 结构化日志测试
# ============================================================================

class TestStructuredLogging:
    """测试结构化日志"""
    
    def test_structured_log_formatter(self):
        """测试结构化日志格式器"""
        import logging
        
        formatter = StructuredLogFormatter()
        
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        output = formatter.format(record)
        data = json.loads(output)
        
        assert data["level"] == "INFO"
        assert data["message"] == "Test message"
        assert "timestamp" in data
    
    def test_log_context(self):
        """测试日志上下文"""
        from memory_system.structured_logging_config import (
            set_request_id, set_tenant_id, clear_context
        )
        
        set_request_id("req_123")
        set_tenant_id("tenant_456")
        
        # 使用上下文管理器
        with LogContext(request_id="req_789"):
            # 上下文内的值
            pass
        
        clear_context()


# ============================================================================
# R-09: 快照序列化器测试
# ============================================================================

class TestSnapshotSerializer:
    """测试快照序列化器"""
    
    def test_json_serializer(self):
        """测试JSON序列化器"""
        serializer = JSONSerializer()
        
        data = {"key": "value", "number": 42, "list": [1, 2, 3]}
        
        serialized = serializer.serialize(data)
        deserialized = serializer.deserialize(serialized)
        
        assert deserialized == data
    
    def test_json_serializer_with_compression(self):
        """测试带压缩的JSON序列化器"""
        serializer = JSONSerializer(compression=True)
        
        data = {"key": "value" * 1000}  # 大数据
        
        serialized = serializer.serialize(data)
        deserialized = serializer.deserialize(serialized)
        
        assert deserialized == data
    
    def test_msgpack_serializer(self):
        """测试MessagePack序列化器"""
        serializer = MsgPackSerializer()
        
        data = {"key": "value", "number": 42}
        
        serialized = serializer.serialize(data)
        deserialized = serializer.deserialize(serialized)
        
        assert deserialized == data
    
    def test_serializer_manager(self):
        """测试序列化管理器"""
        manager = SnapshotSerializerManager(default_serializer="json")
        
        data = {"test": "data", "timestamp": datetime.now()}
        
        serialized = manager.serialize(data)
        deserialized = manager.deserialize(serialized)
        
        assert deserialized["test"] == "data"


# ============================================================================
# R-10: 依赖注入容器测试
# ============================================================================

class TestDIContainer:
    """测试依赖注入容器"""
    
    @pytest.fixture
    def container(self):
        """创建容器"""
        DIContainer._instance = None
        return DIContainer()
    
    def test_register_and_resolve(self, container):
        """测试注册和解析"""
        def factory():
            return {"type": "test_service"}
        
        container.register("test_service", factory)
        
        service = container.resolve("test_service")
        assert service["type"] == "test_service"
    
    def test_singleton(self, container):
        """测试单例"""
        counter = [0]
        
        def factory():
            counter[0] += 1
            return {"instance": counter[0]}
        
        container.register_singleton("singleton_service", factory)
        
        s1 = container.resolve("singleton_service")
        s2 = container.resolve("singleton_service")
        
        assert s1 is s2
        assert s1["instance"] == 1
    
    def test_transient(self, container):
        """测试瞬态"""
        counter = [0]
        
        def factory():
            counter[0] += 1
            return {"instance": counter[0]}
        
        container.register_transient("transient_service", factory)
        
        t1 = container.resolve("transient_service")
        t2 = container.resolve("transient_service")
        
        assert t1 is not t2
        assert t1["instance"] == 1
        assert t2["instance"] == 2
    
    def test_register_instance(self, container):
        """测试注册实例"""
        instance = {"name": "my_instance"}
        container.register_instance("my_service", instance)
        
        resolved = container.resolve("my_service")
        assert resolved is instance
    
    def test_is_registered(self, container):
        """测试是否已注册"""
        assert not container.is_registered("unknown")
        
        container.register("known", lambda: {})
        assert container.is_registered("known")


# ============================================================================
# R-11: 批处理器测试
# ============================================================================

class TestBatchProcessor:
    """测试批处理器"""
    
    @pytest.mark.asyncio
    async def test_batch_processor(self):
        """测试批处理器"""
        def processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        batch_processor = BatchProcessor(
            processor=processor,
            batch_size=3,
            max_wait_time=0.1
        )
        
        await batch_processor.start()
        
        try:
            future = await batch_processor.submit("hello")
            result = await future
            assert result == 5
            
            # 批量提交
            futures = await batch_processor.submit_batch(["a", "bb", "ccc"])
            results = [await f for f in futures]
            assert results == [1, 2, 3]
        finally:
            await batch_processor.stop()
    
    @pytest.mark.asyncio
    async def test_parallel_batch_processor(self):
        """测试并行批处理器"""
        def processor(items: List[int]) -> List[int]:
            return [x * 2 for x in items]
        
        parallel_processor = ParallelBatchProcessor(
            processor=processor,
            batch_size=10,
            max_workers=2
        )
        
        items = list(range(100))
        result = await parallel_processor.process(items)
        
        assert result.total_count == 100
        assert result.success_count == 100
        assert len(result.successful) == 100
        assert result.successful[0] == 0
        assert result.successful[1] == 2
    
    @pytest.mark.asyncio
    async def test_batch_processor_stats(self):
        """测试批处理器统计"""
        def processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        batch_processor = BatchProcessor(
            processor=processor,
            batch_size=5
        )
        
        await batch_processor.start()
        
        try:
            await batch_processor.submit("test")
            await asyncio.sleep(0.1)  # 等待处理
            
            stats = batch_processor.get_stats()
            assert "total_processed" in stats
            assert "batches_processed" in stats
        finally:
            await batch_processor.stop()


# ============================================================================
# 集成测试
# ============================================================================

class TestIntegration:
    """集成测试"""
    
    @pytest.mark.asyncio
    async def test_full_workflow(self):
        """测试完整工作流"""
        # 1. 创建配置
        config = MemoryConfigValidator(
            max_content_length=10000,
            max_batch_size=100
        )
        assert config.max_content_length == 10000
        
        # 2. 创建嵌入提供者
        provider = MockEmbeddingProvider(dimension=128)
        embedding = await provider.embed("test")
        assert len(embedding) == 128
        
        # 3. 使用指标收集器
        MetricsCollector._instance = None
        collector = MetricsCollector()
        collector.increment("test_counter")
        assert collector.get_counter("test_counter") == 1
        
        # 4. 使用TTL存储
        ttl_storage = InMemoryTTLStorage()
        await ttl_storage.add_ttl_entry("mem_1", time.time() + 3600)
        count = await ttl_storage.count_entries()
        assert count == 1
        
        # 5. 序列化数据
        serializer = JSONSerializer()
        data = {"test": "data"}
        serialized = serializer.serialize(data)
        deserialized = serializer.deserialize(serialized)
        assert deserialized == data
        
        # 6. 使用DI容器
        DIContainer._instance = None
        container = DIContainer()
        container.register("test_service", lambda: {"status": "ok"})
        service = container.resolve("test_service")
        assert service["status"] == "ok"


# ============================================================================
# 运行测试
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

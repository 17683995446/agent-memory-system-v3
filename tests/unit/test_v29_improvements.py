"""
Agent Memory System V2.29.0 改进项综合测试
测试所有11个创新改进项
"""

import asyncio
import time
import pytest
from typing import List

# 导入所有新模块
from memory_system.cache_warmer import CacheWarmer
from memory_system.async_batch_writer import AsyncBatchWriter
from memory_system.enhanced_tenant_isolation import (
    EnhancedTenantIsolationManager,
    TenantResourceUsage,
    TenantQuota,
    ResourceType,
)
from memory_system.retrieval_strategies.adaptive_strategy_selector import (
    AdaptiveStrategySelector,
    ContextAwareStrategySelector,
    StrategyType,
)
from memory_system.llm_summarizer import LLMSummarizer
from memory_system.cache_replacement_policies import WTinyLFUCache, LRUCache
from memory_system.time_indexed_ttl_manager import TimeIndexedTTLManager
from memory_system.hnsw_index import HNSWIndex, SimpleANNIndex
from memory_system.read_write_lock import (
    ReadWriteLock,
    OptimizedReadWriteLock,
    ShortTermMemoryRWLock,
)
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    ModalityType,
    MultiModalContent,
    ContextInfo,
)
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval


# ============== I-05: 智能缓存预热测试 ==============

class TestCacheWarmer:
    """测试智能缓存预热功能"""

    @pytest.mark.asyncio
    async def test_record_access(self):
        """测试访问记录"""
        warmer = CacheWarmer(None, None, {}, top_n=10)

        warmer.record_access("mem_1")
        warmer.record_access("mem_1")
        warmer.record_access("mem_2")

        hot = warmer.get_hot_memories(10)
        assert len(hot) == 2
        assert hot[0][0] == "mem_1"
        assert hot[0][1] == 2

    @pytest.mark.asyncio
    async def test_record_access_batch(self):
        """测试批量访问记录"""
        warmer = CacheWarmer(None, None, {}, top_n=10)

        warmer.record_access_batch(["mem_1", "mem_2", "mem_1"])

        hot = warmer.get_hot_memories(10)
        assert len(hot) == 2

    def test_get_stats(self):
        """测试统计信息"""
        warmer = CacheWarmer(None, None, {}, top_n=10)

        warmer.record_access("mem_1")
        warmer.record_access("mem_2")

        stats = warmer.get_stats()
        assert stats["total_tracked"] == 2
        assert stats["total_accesses"] == 2

    def test_clear_access_log(self):
        """测试清空访问日志"""
        warmer = CacheWarmer(None, None, {}, top_n=10)

        warmer.record_access("mem_1")
        warmer.clear_access_log()

        assert len(warmer._access_log) == 0


# ============== I-06: 异步批量写入队列测试 ==============

class TestAsyncBatchWriter:
    """测试异步批量写入队列"""

    @pytest.mark.asyncio
    async def test_enqueue_and_flush(self):
        """测试入队和刷新"""
        # 模拟存储
        class MockStorage:
            def __init__(self):
                self.items = []

            async def add(self, item):
                self.items.append(item)

            async def batch_add(self, items):
                self.items.extend(items)

        storage = MockStorage()
        writer = AsyncBatchWriter(storage, max_batch_size=2, flush_interval=0.1)

        await writer.start()

        # 添加项目
        future1 = await writer.enqueue("item_1")
        future2 = await writer.enqueue("item_2")

        # 等待刷新
        await asyncio.sleep(0.2)

        assert len(storage.items) == 2

        await writer.stop()

    @pytest.mark.asyncio
    async def test_merge_similar_items(self):
        """测试合并相似项目"""
        class MockItem:
            def __init__(self, content, importance=3):
                self.content = content
                self.importance = importance

        class MockStorage:
            async def batch_add(self, items):
                pass

        writer = AsyncBatchWriter(MockStorage(), enable_merge=True)

        items = [
            MockItem("content_a", 3),
            MockItem("content_a", 5),  # 应该合并，保留高重要性
            MockItem("content_b", 2),
        ]

        merged, count = writer._merge_similar_items(items)
        assert len(merged) == 2
        assert count == 1

    def test_get_stats(self):
        """测试统计信息"""
        class MockStorage:
            pass

        writer = AsyncBatchWriter(MockStorage())
        stats = writer.get_stats()

        assert "total_enqueued" in stats
        assert "queue_size" in stats


# ============== I-08: 增强多租户资源隔离测试 ==============

class TestEnhancedTenantIsolation:
    """测试增强多租户资源隔离"""

    @pytest.mark.asyncio
    async def test_record_memory_add(self):
        """测试记录记忆添加"""
        manager = EnhancedTenantIsolationManager()

        await manager.record_memory_add("tenant_1", 1024)
        await manager.record_memory_add("tenant_1", 2048)

        usage = manager.get_tenant_resource_usage("tenant_1")
        assert usage.memory_count == 2
        assert usage.storage_bytes == 3072

    @pytest.mark.asyncio
    async def test_record_query(self):
        """测试记录查询"""
        manager = EnhancedTenantIsolationManager()

        await manager.record_query("tenant_1")
        await manager.record_query("tenant_1")

        usage = manager.get_tenant_resource_usage("tenant_1")
        assert usage.query_count == 2

    @pytest.mark.asyncio
    async def test_check_resource_quota(self):
        """测试资源配额检查"""
        manager = EnhancedTenantIsolationManager(enable_quota=True)

        # 设置配额
        quota = TenantQuota(max_memories=5)
        manager.set_tenant_quota("tenant_1", quota)

        # 添加4条记忆，应该通过
        for i in range(4):
            await manager.record_memory_add("tenant_1", 100)

        assert await manager.check_resource_quota("tenant_1", ResourceType.MEMORY_COUNT)

    def test_tenant_resource_usage_dataclass(self):
        """测试TenantResourceUsage数据类"""
        usage = TenantResourceUsage(
            memory_count=10,
            storage_bytes=1024,
            query_count=5
        )

        data = usage.to_dict()
        assert data["memory_count"] == 10
        assert data["storage_bytes"] == 1024

    def test_tenant_quota_dataclass(self):
        """测试TenantQuota数据类"""
        quota = TenantQuota(max_memories=100, max_storage_bytes=1024000)

        data = quota.to_dict()
        assert data["max_memories"] == 100
        assert data["max_storage_bytes"] == 1024000


# ============== I-10: 自适应检索策略选择测试 ==============

class TestAdaptiveStrategySelector:
    """测试自适应检索策略选择"""

    def test_select_strategy_short_query(self):
        """测试短查询选择fast策略"""
        selector = AdaptiveStrategySelector()

        selection = selector.select_strategy("短查询")
        assert selection.strategy == StrategyType.FAST
        assert selection.confidence > 0.8

    def test_select_strategy_with_keywords(self):
        """测试关键词触发hybrid策略"""
        selector = AdaptiveStrategySelector()

        selection = selector.select_strategy("What is the name of the user?")
        assert selection.strategy == StrategyType.HYBRID

    def test_select_strategy_long_query(self):
        """测试长查询选择thorough策略"""
        selector = AdaptiveStrategySelector()

        long_query = "This is a very long query with many words to test the thorough strategy selection mechanism"
        selection = selector.select_strategy(long_query)
        assert selection.strategy == StrategyType.THOROUGH

    def test_select_strategy_temporal(self):
        """测试时间相关查询"""
        selector = AdaptiveStrategySelector()

        selection = selector.select_strategy("What happened yesterday?")
        assert selection.strategy == StrategyType.ADAPTIVE

    def test_batch_select(self):
        """测试批量选择"""
        selector = AdaptiveStrategySelector()

        queries = ["短", "What is the name?", "This is a very long query with many words"]
        selections = selector.batch_select(queries)

        assert len(selections) == 3

    def test_get_stats(self):
        """测试统计信息"""
        selector = AdaptiveStrategySelector()

        selector.select_strategy("短查询")
        selector.select_strategy("What is the name?")

        stats = selector.get_stats()
        assert stats["total_selections"] == 2


class TestContextAwareStrategySelector:
    """测试上下文感知策略选择器"""

    def test_select_with_session(self):
        """测试会话感知选择"""
        selector = ContextAwareStrategySelector()

        selection = selector.select_strategy_with_session(
            "test query",
            "session_1"
        )

        assert selection.strategy is not None
        assert len(selector._session_history["session_1"]) == 1

    def test_session_preference(self):
        """测试会话偏好"""
        selector = ContextAwareStrategySelector()

        selector.set_session_preference("session_1", StrategyType.THOROUGH)

        selection = selector.select_strategy_with_session(
            "test query",
            "session_1"
        )

        # 如果偏好匹配，置信度应该提高
        if selection.strategy == StrategyType.THOROUGH:
            assert selection.confidence > 0.8


# ============== N-01: LLM记忆摘要生成测试 ==============

class TestLLMSummarizer:
    """测试LLM记忆摘要生成"""

    @pytest.mark.asyncio
    async def test_generate_tags(self):
        """测试标签生成"""
        summarizer = LLMSummarizer()

        content = "Python is a great programming language for data science and machine learning"
        tags = await summarizer.generate_tags(content)

        assert len(tags) > 0
        assert len(tags) <= summarizer.max_tags

    @pytest.mark.asyncio
    async def test_summarize_extractive(self):
        """测试抽取式摘要"""
        summarizer = LLMSummarizer()

        content = "First sentence about Python. Second sentence about programming. Third sentence about learning."
        result = await summarizer.summarize(content, strategy="extractive")

        assert result.summary is not None
        assert len(result.tags) > 0
        assert result.confidence > 0

    @pytest.mark.asyncio
    async def test_batch_summarize(self):
        """测试批量摘要"""
        summarizer = LLMSummarizer()

        contents = [
            "Python is great for data science",
            "Machine learning is a subset of AI",
        ]
        results = await summarizer.batch_summarize(contents)

        assert len(results) == 2
        for r in results:
            assert r.summary is not None

    def test_get_stats(self):
        """测试统计信息"""
        summarizer = LLMSummarizer()
        stats = summarizer.get_stats()

        assert "total_summarized" in stats
        assert "total_tags_generated" in stats


# ============== M-01: W-TinyLFU缓存策略测试 ==============

class TestWTinyLFUCache:
    """测试W-TinyLFU缓存"""

    def test_basic_operations(self):
        """测试基本操作"""
        cache = WTinyLFUCache(maxsize=3)

        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")

        assert cache.get("key1") == "value1"
        assert cache.get("key2") == "value2"

    def test_eviction(self):
        """测试淘汰机制"""
        cache = WTinyLFUCache(maxsize=2)

        cache.put("key1", "value1")
        cache.put("key2", "value2")

        # 访问key1增加频率
        cache.get("key1")
        cache.get("key1")

        # 添加新项，应该淘汰key2
        cache.put("key3", "value3")

        assert cache.get("key1") is not None
        assert cache.get("key3") is not None

    def test_contains(self):
        """测试包含检查"""
        cache = WTinyLFUCache(maxsize=3)

        cache.put("key1", "value1")

        assert "key1" in cache
        assert "key2" not in cache

    def test_get_stats(self):
        """测试统计信息"""
        cache = WTinyLFUCache(maxsize=3)

        cache.put("key1", "value1")
        cache.get("key1")

        stats = cache.get_stats()
        assert stats["size"] == 1
        assert stats["maxsize"] == 3


class TestLRUCache:
    """测试LRU缓存"""

    def test_basic_operations(self):
        """测试基本操作"""
        cache = LRUCache(maxsize=3)

        cache.put("key1", "value1")
        cache.put("key2", "value2")

        assert cache.get("key1") == "value1"

    def test_lru_eviction(self):
        """测试LRU淘汰"""
        cache = LRUCache(maxsize=2)

        cache.put("key1", "value1")
        cache.put("key2", "value2")

        # 访问key1使其变为最近使用
        cache.get("key1")

        # 添加新项，应该淘汰key2
        cache.put("key3", "value3")

        assert cache.get("key1") is not None
        assert cache.get("key2") is None
        assert cache.get("key3") is not None


# ============== M-05: 时间索引TTL管理测试 ==============

class TestTimeIndexedTTLManager:
    """测试时间索引TTL管理"""

    @pytest.mark.asyncio
    async def test_add_with_ttl(self):
        """测试添加带TTL的记忆"""
        class MockStorage:
            pass

        manager = TimeIndexedTTLManager(MockStorage())

        item = MemoryItem(content="test", memory_type=MemoryType.EPISODIC)
        await manager.add_with_ttl(item, ttl=3600)

        assert item.memory_id in manager._memory_to_bucket

    @pytest.mark.asyncio
    async def test_remove_from_index(self):
        """测试从索引移除"""
        class MockStorage:
            pass

        manager = TimeIndexedTTLManager(MockStorage())

        item = MemoryItem(content="test", memory_type=MemoryType.EPISODIC)
        await manager.add_with_ttl(item, ttl=3600)

        result = await manager.remove_from_index(item.memory_id)
        assert result is True

        result = await manager.remove_from_index("non_existent")
        assert result is False

    @pytest.mark.asyncio
    async def test_get_index_stats(self):
        """测试索引统计"""
        class MockStorage:
            pass

        manager = TimeIndexedTTLManager(MockStorage())

        item = MemoryItem(content="test", memory_type=MemoryType.EPISODIC)
        await manager.add_with_ttl(item, ttl=3600)

        stats = await manager.get_index_stats()
        assert stats["index_size"] == 1
        assert stats["buckets_count"] >= 1


# ============== M-07: HNSW近似最近邻索引测试 ==============

class TestHNSWIndex:
    """测试HNSW索引"""

    def test_add_and_search(self):
        """测试添加和搜索"""
        index = HNSWIndex(dim=3, m=8, ef_construction=50)

        # 添加向量
        index.add("id1", [1.0, 0.0, 0.0])
        index.add("id2", [0.0, 1.0, 0.0])
        index.add("id3", [0.0, 0.0, 1.0])

        # 搜索
        results = index.search([1.0, 0.1, 0.1], k=2)

        assert len(results) == 2
        assert results[0][0] == "id1"  # 最相似的应该是id1

    def test_batch_add(self):
        """测试批量添加"""
        index = HNSWIndex(dim=2, m=8)

        items = [
            ("id1", [1.0, 0.0]),
            ("id2", [0.0, 1.0]),
            ("id3", [0.5, 0.5]),
        ]
        index.batch_add(items)

        assert len(index._nodes) == 3

    def test_delete(self):
        """测试删除"""
        index = HNSWIndex(dim=2, m=8)

        index.add("id1", [1.0, 0.0])
        result = index.delete("id1")

        assert result is True
        assert "id1" not in index._nodes

        result = index.delete("non_existent")
        assert result is False

    def test_clear(self):
        """测试清空"""
        index = HNSWIndex(dim=2, m=8)

        index.add("id1", [1.0, 0.0])
        index.clear()

        assert len(index._nodes) == 0
        assert index._entry_point is None

    def test_get_stats(self):
        """测试统计信息"""
        index = HNSWIndex(dim=2, m=8)

        index.add("id1", [1.0, 0.0])
        index.search([1.0, 0.0])

        stats = index.get_stats()
        assert stats["node_count"] == 1
        assert stats["total_added"] == 1


class TestSimpleANNIndex:
    """测试简单ANN索引"""

    def test_add_and_search(self):
        """测试添加和搜索"""
        index = SimpleANNIndex(dim=3, num_hash_tables=4)

        index.add("id1", [1.0, 0.0, 0.0])
        index.add("id2", [0.0, 1.0, 0.0])
        index.add("id3", [0.0, 0.0, 1.0])

        results = index.search([1.0, 0.1, 0.1], k=2)

        assert len(results) > 0


# ============== M-09: 读写锁测试 ==============

class TestReadWriteLock:
    """测试读写锁"""

    @pytest.mark.asyncio
    async def test_read_lock(self):
        """测试读锁"""
        lock = ReadWriteLock()

        async with lock.read_lock():
            assert lock.is_read_locked()

    @pytest.mark.asyncio
    async def test_write_lock(self):
        """测试写锁"""
        lock = ReadWriteLock()

        async with lock.write_lock():
            assert lock.is_write_locked()

    @pytest.mark.asyncio
    async def test_concurrent_reads(self):
        """测试并发读"""
        lock = ReadWriteLock()
        counter = 0

        async def reader():
            nonlocal counter
            async with lock.read_lock():
                counter += 1
                await asyncio.sleep(0.01)
                counter -= 1

        await asyncio.gather(*[reader() for _ in range(5)])
        assert counter == 0

    def test_get_stats(self):
        """测试统计信息"""
        lock = ReadWriteLock()
        stats = lock.get_stats()

        assert "read_count" in stats
        assert "waiting_writers" in stats


class TestOptimizedReadWriteLock:
    """测试优化读写锁"""

    @pytest.mark.asyncio
    async def test_read_lock_with_timeout(self):
        """测试带超时的读锁"""
        lock = OptimizedReadWriteLock()

        result = await lock.acquire_read(timeout=0.1)
        assert result is True

        await lock.release_read()

    @pytest.mark.asyncio
    async def test_write_lock_with_timeout(self):
        """测试带超时的写锁"""
        lock = OptimizedReadWriteLock()

        result = await lock.acquire_write(timeout=0.1)
        assert result is True

        await lock.release_write()

    def test_get_stats(self):
        """测试统计信息"""
        lock = OptimizedReadWriteLock()
        stats = lock.get_stats()

        assert "read_count" in stats
        assert "write_count" in stats
        assert "write_priority" in stats


class TestShortTermMemoryRWLock:
    """测试短期记忆专用读写锁"""

    @pytest.mark.asyncio
    async def test_read(self):
        """测试读锁"""
        lock = ShortTermMemoryRWLock()

        async with lock.read():
            pass

    @pytest.mark.asyncio
    async def test_write(self):
        """测试写锁"""
        lock = ShortTermMemoryRWLock()

        async with lock.write():
            pass

    def test_get_stats(self):
        """测试统计信息"""
        lock = ShortTermMemoryRWLock()
        stats = lock.get_stats()

        assert "pending_reads" in stats


# ============== N-04: 多模态记忆支持测试 ==============

class TestMultiModalContent:
    """测试多模态内容"""

    def test_text_modality(self):
        """测试文本模态"""
        content = MultiModalContent(
            modality=ModalityType.TEXT,
            text="Hello world"
        )

        assert content.get_primary_content() == "Hello world"

    def test_image_modality(self):
        """测试图像模态"""
        content = MultiModalContent(
            modality=ModalityType.IMAGE,
            image_url="http://example.com/image.jpg"
        )

        primary = content.get_primary_content()
        assert "Image" in primary

    def test_to_dict(self):
        """测试转换为字典"""
        content = MultiModalContent(
            modality=ModalityType.TEXT,
            text="test"
        )

        data = content.to_dict()
        assert data["modality"] == "text"
        assert data["text"] == "test"

    def test_from_dict(self):
        """测试从字典创建"""
        data = {
            "modality": "image",
            "image_url": "http://example.com/img.jpg"
        }

        content = MultiModalContent.from_dict(data)
        assert content.modality == ModalityType.IMAGE
        assert content.image_url == "http://example.com/img.jpg"


class TestContextInfo:
    """测试情境信息"""

    def test_basic_creation(self):
        """测试基本创建"""
        context = ContextInfo(
            session_id="session_1",
            current_time=time.time(),
            location="Beijing",
            session_topics=["Python", "AI"]
        )

        assert context.session_id == "session_1"
        assert len(context.session_topics) == 2

    def test_to_dict(self):
        """测试转换为字典"""
        context = ContextInfo(session_id="session_1")

        data = context.to_dict()
        assert data["session_id"] == "session_1"

    def test_from_dict(self):
        """测试从字典创建"""
        data = {
            "session_id": "session_2",
            "location": "Shanghai",
            "session_topics": ["ML"]
        }

        context = ContextInfo.from_dict(data)
        assert context.session_id == "session_2"
        assert context.location == "Shanghai"


# ============== N-08: 情境感知检索测试 ==============

class TestContextAwareRetrieval:
    """测试情境感知检索"""

    def test_calculate_context_score_time(self):
        """测试时间上下文评分"""
        strategy = HierarchicalRetrieval()

        memory = MemoryItem(content="test", memory_type=MemoryType.EPISODIC)
        memory.timestamp = time.time() - 1800  # 30分钟前

        context = {"current_time": time.time()}
        score = strategy._calculate_context_score(memory, context)

        assert score > 0

    def test_calculate_context_score_topics(self):
        """测试主题上下文评分"""
        strategy = HierarchicalRetrieval()

        memory = MemoryItem(content="Python programming is great", memory_type=MemoryType.EPISODIC)

        context = {"session_topics": ["Python", "programming"]}
        score = strategy._calculate_context_score(memory, context)

        assert score > 0

    def test_calculate_context_score_location(self):
        """测试位置上下文评分"""
        strategy = HierarchicalRetrieval()

        memory = MemoryItem(
            content="test",
            memory_type=MemoryType.EPISODIC,
            metadata={"location": "Beijing"}
        )

        context = {"location": "Beijing"}
        score = strategy._calculate_context_score(memory, context)

        assert score == 0.2


# ============== 集成测试 ==============

class TestV29Integration:
    """V2.29.0 集成测试"""

    @pytest.mark.asyncio
    async def test_full_workflow(self):
        """测试完整工作流"""
        # 创建各个组件
        cache = WTinyLFUCache(maxsize=100)
        summarizer = LLMSummarizer()
        selector = AdaptiveStrategySelector()

        # 测试缓存
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"

        # 测试摘要
        result = await summarizer.summarize("Python is great for AI development")
        assert result.summary is not None

        # 测试策略选择
        selection = selector.select_strategy("What is Python?")
        assert selection.strategy is not None

    def test_all_modules_importable(self):
        """测试所有模块可导入"""
        # 这个测试验证所有模块都能正确导入
        from memory_system import (
            CacheWarmer,
            AsyncBatchWriter,
            EnhancedTenantIsolationManager,
            AdaptiveStrategySelector,
            LLMSummarizer,
            WTinyLFUCache,
            TimeIndexedTTLManager,
            HNSWIndex,
            ReadWriteLock,
            ModalityType,
            MultiModalContent,
            ContextInfo,
        )

        # 如果能执行到这里，说明所有模块都成功导入了
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

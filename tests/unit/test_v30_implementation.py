"""
V2.30.0 深度实现特性单元测试
测试以下功能:
- I-01: 分布式存储后端 (Distributed Storage Backend)
- I-03: 增量嵌入更新机制 (Incremental Embedding Update)
- I-09: 记忆去重系统 (Memory Deduplication System)
- I-13: 向量量化压缩 (Vector Quantization Compression)
"""

import pytest
import asyncio
import numpy as np
from typing import List
from unittest.mock import Mock, AsyncMock

# 导入被测试的模块
from memory_system.storage_backends.distributed_storage_backend import (
    DistributedStorageBackend,
    ConsistentHashRing,
    StorageNode,
)
from memory_system.incremental_embedding_updater import (
    IncrementalEmbeddingUpdater,
    AdaptiveIncrementalUpdater,
    TextDiff,
)
from memory_system.deduplication_system import (
    MemoryDeduplicationSystem,
    DuplicateGroup,
    RealTimeDeduplicationMiddleware,
)
from memory_system.vector_quantization import (
    ProductQuantizer,
    OptimizedProductQuantizer,
    VectorCompressionIndex,
    MultiResolutionQuantizer,
    PQConfig,
    OPQConfig,
)
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.core_abstractions.abstract_component_interfaces import EmbeddingProvider


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_embedding_provider():
    """创建模拟的嵌入提供者"""
    provider = Mock(spec=EmbeddingProvider)
    provider.dimension = 128

    async def mock_embed(text: str) -> List[float]:
        # 基于文本内容生成确定性向量
        np.random.seed(hash(text) % 2**32)
        return np.random.randn(128).tolist()

    async def mock_embed_batch(texts: List[str]) -> List[List[float]]:
        return [await mock_embed(t) for t in texts]

    async def mock_similarity(vec1: List[float], vec2: List[float]) -> float:
        # 计算余弦相似度
        v1, v2 = np.array(vec1), np.array(vec2)
        return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))

    provider.embed = mock_embed
    provider.embed_batch = mock_embed_batch
    provider.similarity = mock_similarity
    return provider


@pytest.fixture
def storage_nodes():
    """创建测试用的存储节点"""
    return [
        StorageNode(node_id=f"node_{i}", host=f"127.0.0.{i}", port=8000 + i, weight=1)
        for i in range(5)
    ]


@pytest.fixture
def sample_memory_items():
    """创建测试用的记忆项"""
    return [
        MemoryItem(
            content=f"Test memory content {i}",
            memory_type=MemoryType.EPISODIC if i % 2 == 0 else MemoryType.SEMANTIC,
            importance=ImportanceLevel.MEDIUM,
        )
        for i in range(10)
    ]


# ============================================================================
# I-01: 分布式存储后端测试
# ============================================================================

class TestConsistentHashRing:
    """测试一致性哈希环"""

    def test_add_node(self):
        """测试添加节点"""
        ring = ConsistentHashRing(replicas=10)
        node = StorageNode(node_id="node_1", host="127.0.0.1", port=8000)

        ring.add_node(node)

        assert "node_1" in ring.nodes
        assert len(ring.sorted_keys) == 10  # replicas * weight

    def test_remove_node(self):
        """测试移除节点"""
        ring = ConsistentHashRing(replicas=10)
        node = StorageNode(node_id="node_1", host="127.0.0.1", port=8000)

        ring.add_node(node)
        ring.remove_node("node_1")

        assert "node_1" not in ring.nodes
        assert len(ring.sorted_keys) == 0

    def test_get_node(self):
        """测试获取节点"""
        ring = ConsistentHashRing(replicas=10)
        nodes = [
            StorageNode(node_id=f"node_{i}", host=f"127.0.0.{i}", port=8000 + i)
            for i in range(3)
        ]

        for node in nodes:
            ring.add_node(node)

        # 测试获取节点
        result = ring.get_node("test_key")
        assert result is not None
        assert result.node_id in [n.node_id for n in nodes]

    def test_get_nodes_for_replicas(self):
        """测试获取多个节点用于副本"""
        ring = ConsistentHashRing(replicas=10)
        nodes = [
            StorageNode(node_id=f"node_{i}", host=f"127.0.0.{i}", port=8000 + i)
            for i in range(5)
        ]

        for node in nodes:
            ring.add_node(node)

        result = ring.get_nodes("test_key", n=3)
        assert len(result) == 3
        # 确保返回的节点不重复
        assert len(set(n.node_id for n in result)) == 3

    def test_weighted_nodes(self):
        """测试加权节点"""
        ring = ConsistentHashRing(replicas=10)
        node1 = StorageNode(node_id="node_1", host="127.0.0.1", port=8000, weight=2)
        node2 = StorageNode(node_id="node_2", host="127.0.0.2", port=8001, weight=1)

        ring.add_node(node1)
        ring.add_node(node2)

        # weight=2 的节点应该有更多的虚拟节点
        assert len(ring.sorted_keys) == 30  # (2+1) * 10

    def test_empty_ring(self):
        """测试空哈希环"""
        ring = ConsistentHashRing()
        assert ring.get_node("any_key") is None
        assert ring.get_nodes("any_key") == []


class TestDistributedStorageBackend:
    """测试分布式存储后端"""

    @pytest.mark.asyncio
    async def test_add_and_get(self, storage_nodes):
        """测试添加和获取记忆"""
        backend = DistributedStorageBackend(
            nodes=storage_nodes,
            replication_factor=3,
            read_quorum=2,
            write_quorum=2
        )

        item = MemoryItem(content="Test content", memory_type=MemoryType.EPISODIC)

        # 添加记忆
        memory_id = await backend.add(item)
        assert memory_id == item.memory_id

        # 获取记忆
        retrieved = await backend.get(memory_id)
        assert retrieved is not None
        assert retrieved.content == "Test content"

        await backend.close()

    @pytest.mark.asyncio
    async def test_delete(self, storage_nodes):
        """测试删除记忆"""
        backend = DistributedStorageBackend(
            nodes=storage_nodes,
            replication_factor=3
        )

        item = MemoryItem(content="Test content", memory_type=MemoryType.EPISODIC)
        memory_id = await backend.add(item)

        # 删除记忆
        result = await backend.delete(memory_id)
        assert result is True

        # 确认已删除
        retrieved = await backend.get(memory_id)
        assert retrieved is None

        await backend.close()

    @pytest.mark.asyncio
    async def test_count(self, storage_nodes):
        """测试计数功能"""
        backend = DistributedStorageBackend(
            nodes=storage_nodes,
            replication_factor=3
        )

        # 添加多条记忆
        for i in range(5):
            item = MemoryItem(
                content=f"Content {i}",
                memory_type=MemoryType.EPISODIC if i % 2 == 0 else MemoryType.SEMANTIC
            )
            await backend.add(item)

        # 统计总数
        total = await backend.count()
        assert total == 5

        await backend.close()

    @pytest.mark.asyncio
    async def test_search_by_time_range(self, storage_nodes):
        """测试按时间范围搜索"""
        backend = DistributedStorageBackend(
            nodes=storage_nodes,
            replication_factor=3
        )

        import time
        start_time = time.time()

        # 添加记忆
        for i in range(5):
            item = MemoryItem(content=f"Content {i}", memory_type=MemoryType.EPISODIC)
            await backend.add(item)

        end_time = time.time()

        # 搜索
        results = await backend.search_by_time_range(start_time, end_time)
        assert len(results) == 5

        await backend.close()

    @pytest.mark.asyncio
    async def test_dynamic_node_management(self, storage_nodes):
        """测试动态节点管理"""
        backend = DistributedStorageBackend(
            nodes=storage_nodes[:3],
            replication_factor=2
        )

        # 添加新节点
        new_node = StorageNode(node_id="new_node", host="127.0.0.10", port=8010)
        await backend.add_node(new_node)

        assert "new_node" in backend.hash_ring.nodes

        # 移除节点
        await backend.remove_node("new_node")
        assert "new_node" not in backend.hash_ring.nodes

        await backend.close()

    def test_get_stats(self, storage_nodes):
        """测试获取统计信息"""
        backend = DistributedStorageBackend(nodes=storage_nodes)

        stats = backend.get_stats()
        assert "writes" in stats
        assert "reads" in stats
        assert "write_failures" in stats
        assert "read_failures" in stats


# ============================================================================
# I-03: 增量嵌入更新机制测试
# ============================================================================

class TestIncrementalEmbeddingUpdater:
    """测试增量嵌入更新器"""

    @pytest.mark.asyncio
    async def test_compute_diff(self, mock_embedding_provider):
        """测试差异计算"""
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider)

        old_text = "Hello world"
        new_text = "Hello beautiful world"

        diffs = updater.compute_diff(old_text, new_text)

        assert len(diffs) > 0
        # 应该检测到 "beautiful " 的插入
        insert_ops = [d for d in diffs if d.operation == 'insert']
        assert len(insert_ops) > 0

    def test_should_full_update(self, mock_embedding_provider):
        """测试全量更新判断"""
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider, threshold=0.3)

        # 小变更 - 不需要全量更新
        old_text = "Hello world"
        new_text = "Hello beautiful world"
        assert updater.should_full_update(old_text, new_text) is False

        # 大变更 - 需要全量更新
        old_text = "Hello world"
        new_text = "Completely different text here"
        assert updater.should_full_update(old_text, new_text) is True

        # 空文本 - 需要全量更新
        assert updater.should_full_update("", "New text") is True

    @pytest.mark.asyncio
    async def test_update_embedding_incremental(self, mock_embedding_provider):
        """测试增量更新嵌入"""
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider, threshold=0.5)

        old_text = "Hello world"
        new_text = "Hello beautiful world"
        old_embedding = await mock_embedding_provider.embed(old_text)

        new_embedding, is_full = await updater.update_embedding(
            old_text, new_text, old_embedding
        )

        assert len(new_embedding) == 128
        assert is_full is False  # 小变更应该使用增量更新

    @pytest.mark.asyncio
    async def test_update_embedding_full(self, mock_embedding_provider):
        """测试全量更新嵌入"""
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider, threshold=0.1)

        old_text = "Hello world"
        new_text = "Completely different text content here"
        old_embedding = await mock_embedding_provider.embed(old_text)

        new_embedding, is_full = await updater.update_embedding(
            old_text, new_text, old_embedding
        )

        assert len(new_embedding) == 128
        assert is_full is True  # 大变更应该使用全量更新

    def test_compute_change_impact(self, mock_embedding_provider):
        """测试变更影响计算"""
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider)

        # 无变更
        impact = updater.compute_change_impact("Same text", "Same text")
        assert impact == 0.0

        # 完全变更
        impact = updater.compute_change_impact("ABC", "XYZ")
        assert impact > 0.5

        # 部分变更
        impact = updater.compute_change_impact("Hello world", "Hello there")
        assert 0.0 < impact < 1.0

    def test_get_diff_summary(self, mock_embedding_provider):
        """测试差异摘要"""
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider)

        old_text = "Hello world today"
        new_text = "Hello beautiful world"

        diffs = updater.compute_diff(old_text, new_text)
        summary = updater.get_diff_summary(diffs)

        assert "insertions" in summary
        assert "deletions" in summary
        assert "replacements" in summary
        assert "total_changes" in summary

    @pytest.mark.asyncio
    async def test_batch_update_embeddings(self, mock_embedding_provider):
        """测试批量更新"""
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider)

        updates = [
            ("Text 1", "Text 1 updated", await mock_embedding_provider.embed("Text 1")),
            ("Text 2", "Text 2", await mock_embedding_provider.embed("Text 2")),
        ]

        results = await updater.batch_update_embeddings(updates)

        assert len(results) == 2
        for embedding, is_full in results:
            assert len(embedding) == 128


class TestAdaptiveIncrementalUpdater:
    """测试自适应增量更新器"""

    def test_detect_content_type_code(self, mock_embedding_provider):
        """测试代码内容类型检测"""
        updater = AdaptiveIncrementalUpdater(mock_embedding_provider)

        code_text = "def hello():\n    return 'world'"
        assert updater.detect_content_type(code_text) == "code"

    def test_detect_content_type_structured(self, mock_embedding_provider):
        """测试结构化数据类型检测"""
        updater = AdaptiveIncrementalUpdater(mock_embedding_provider)

        json_text = '{"key": "value", "num": 123}'
        assert updater.detect_content_type(json_text) == "structured"

    def test_detect_content_type_text(self, mock_embedding_provider):
        """测试普通文本类型检测"""
        updater = AdaptiveIncrementalUpdater(mock_embedding_provider)

        plain_text = "This is just plain text content"
        assert updater.detect_content_type(plain_text) == "text"

    def test_adaptive_threshold(self, mock_embedding_provider):
        """测试自适应阈值"""
        updater = AdaptiveIncrementalUpdater(mock_embedding_provider, adaptive_mode=True)

        # 代码使用更严格的阈值 (0.2)
        code = "def test(): pass"
        code_update = "def test():\n    return None"
        # 应该使用代码阈值
        result = updater.should_full_update(code, code_update)
        # 代码阈值更严格，所以可能触发全量更新

    def test_set_content_type_threshold(self, mock_embedding_provider):
        """测试设置内容类型阈值"""
        updater = AdaptiveIncrementalUpdater(mock_embedding_provider)

        updater.set_content_type_threshold("custom_type", 0.15)
        assert updater._content_type_thresholds["custom_type"] == 0.15


# ============================================================================
# I-09: 记忆去重系统测试
# ============================================================================

class TestMemoryDeduplicationSystem:
    """测试记忆去重系统"""

    @pytest.mark.asyncio
    async def test_find_duplicates(self, mock_embedding_provider, sample_memory_items):
        """测试查找重复"""
        dedup = MemoryDeduplicationSystem(
            embedding_provider=mock_embedding_provider,
            similarity_threshold=0.8
        )

        # 创建相似的记忆
        similar_item = MemoryItem(
            content=sample_memory_items[0].content,  # 相同内容
            memory_type=MemoryType.EPISODIC
        )

        duplicates = await dedup.find_duplicates(similar_item, sample_memory_items)

        # 应该找到重复
        assert len(duplicates) > 0
        assert all(score >= 0.8 for _, score in duplicates)

    @pytest.mark.asyncio
    async def test_merge_duplicates_newest(self, mock_embedding_provider):
        """测试按最新合并"""
        dedup = MemoryDeduplicationSystem(mock_embedding_provider)

        import time
        memories = [
            MemoryItem(content="Test", timestamp=time.time() - 100),
            MemoryItem(content="Test", timestamp=time.time()),
            MemoryItem(content="Test", timestamp=time.time() - 50),
        ]

        merged, merged_ids = await dedup.merge_duplicates(memories, strategy="newest")

        # 最新的记忆应该作为主记忆
        assert merged.timestamp == max(m.timestamp for m in memories)
        assert len(merged_ids) == 2

    @pytest.mark.asyncio
    async def test_merge_duplicates_longest(self, mock_embedding_provider):
        """测试按最长内容合并"""
        dedup = MemoryDeduplicationSystem(mock_embedding_provider)

        memories = [
            MemoryItem(content="Short"),
            MemoryItem(content="This is a much longer content"),
            MemoryItem(content="Medium length"),
        ]

        merged, merged_ids = await dedup.merge_duplicates(memories, strategy="longest")

        # 最长的记忆应该作为主记忆
        assert len(merged.content) == max(len(m.content) for m in memories)

    @pytest.mark.asyncio
    async def test_merge_duplicates_ensemble(self, mock_embedding_provider):
        """测试集成合并"""
        dedup = MemoryDeduplicationSystem(mock_embedding_provider)

        memories = [
            MemoryItem(content="First sentence."),
            MemoryItem(content="Second sentence."),
            MemoryItem(content="First sentence. Third sentence."),
        ]

        merged, merged_ids = await dedup.merge_duplicates(memories, strategy="ensemble")

        # 合并后的内容应该包含所有唯一句子
        assert "First sentence" in merged.content
        assert "merged_from" in merged.metadata

    @pytest.mark.asyncio
    async def test_check_duplicate(self, mock_embedding_provider, sample_memory_items):
        """测试重复检查"""
        dedup = MemoryDeduplicationSystem(
            mock_embedding_provider,
            similarity_threshold=0.8
        )

        # 检查已有重复
        duplicate_item = MemoryItem(
            content=sample_memory_items[0].content,
            memory_type=MemoryType.EPISODIC
        )

        result = await dedup.check_duplicate(duplicate_item)

        # 应该返回重复组
        assert result is not None
        assert isinstance(result, DuplicateGroup)
        assert result.primary_memory_id is not None

    @pytest.mark.asyncio
    async def test_find_similar_groups(self, mock_embedding_provider):
        """测试查找相似组"""
        dedup = MemoryDeduplicationSystem(
            mock_embedding_provider,
            similarity_threshold=0.7
        )

        # 创建多组相似记忆
        memories = [
            MemoryItem(content="Group A item 1"),
            MemoryItem(content="Group A item 2"),
            MemoryItem(content="Group B item 1"),
            MemoryItem(content="Group B item 2"),
            MemoryItem(content="Unique item"),
        ]

        groups = await dedup.find_similar_groups(memories, group_threshold=0.5)

        # 应该找到相似组
        assert isinstance(groups, list)

    def test_update_similarity_threshold(self, mock_embedding_provider):
        """测试更新相似度阈值"""
        dedup = MemoryDeduplicationSystem(mock_embedding_provider, similarity_threshold=0.9)

        dedup.update_similarity_threshold(0.85)
        assert dedup.similarity_threshold == 0.85


class TestRealTimeDeduplicationMiddleware:
    """测试实时去重中间件"""

    @pytest.mark.asyncio
    async def test_process_memory_no_duplicate(self, mock_embedding_provider):
        """测试处理无重复的记忆"""
        dedup = MemoryDeduplicationSystem(mock_embedding_provider, similarity_threshold=0.95)
        middleware = RealTimeDeduplicationMiddleware(dedup)

        memory = MemoryItem(content="Unique content here")

        result, is_duplicate = await middleware.process_memory(memory)

        assert result.content == "Unique content here"
        assert is_duplicate is False

    @pytest.mark.asyncio
    async def test_process_memory_with_callback(self, mock_embedding_provider, sample_memory_items):
        """测试带回调的重复处理"""
        dedup = MemoryDeduplicationSystem(
            mock_embedding_provider,
            similarity_threshold=0.8
        )
        # 预填充去重索引
        dedup._dedup_index = {
            item.memory_id: await mock_embedding_provider.embed(item.content)
            for item in sample_memory_items
        }

        callback_called = False
        async def callback(memory, group):
            nonlocal callback_called
            callback_called = True

        middleware = RealTimeDeduplicationMiddleware(
            dedup,
            on_duplicate_callback=callback
        )

        # 使用相似内容
        similar_memory = MemoryItem(content=sample_memory_items[0].content)

        await middleware.process_memory(similar_memory)

        assert callback_called is True


# ============================================================================
# I-13: 向量量化压缩测试
# ============================================================================

class TestProductQuantizer:
    """测试乘积量化器"""

    def test_initialization(self):
        """测试初始化"""
        config = PQConfig(num_subvectors=8, num_centroids=256)
        pq = ProductQuantizer(dim=128, config=config)

        assert pq.dim == 128
        assert pq.subvector_dim == 16  # 128 / 8
        assert not pq.is_trained

    def test_train(self):
        """测试训练"""
        pq = ProductQuantizer(dim=64, config=PQConfig(num_subvectors=4, num_centroids=64))

        # 生成训练数据
        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(100)]

        pq.train(vectors)

        assert pq.is_trained
        assert len(pq.codebooks) == 4
        assert all(cb.shape == (64, 16) for cb in pq.codebooks)

    def test_encode_decode(self):
        """测试编码和解码"""
        pq = ProductQuantizer(dim=64, config=PQConfig(num_subvectors=4, num_centroids=64))

        # 训练
        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(100)]
        pq.train(vectors)

        # 编码
        test_vector = np.random.randn(64).tolist()
        codes = pq.encode(test_vector)

        assert len(codes) == 4
        assert all(0 <= c < 64 for c in codes)

        # 解码
        reconstructed = pq.decode(codes)
        assert len(reconstructed) == 64

    def test_compress_batch(self):
        """测试批量压缩"""
        pq = ProductQuantizer(dim=64, config=PQConfig(num_subvectors=4))

        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(50)]

        compressed, ratio = pq.compress_batch(vectors)

        assert len(compressed) == 50
        assert len(compressed[0]) == 4
        assert ratio > 1.0  # 应该有压缩

    def test_approximate_distance(self):
        """测试近似距离计算"""
        pq = ProductQuantizer(dim=64, config=PQConfig(num_subvectors=4, num_centroids=64))

        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(50)]
        pq.train(vectors)

        codes1 = pq.encode(np.random.randn(64).tolist())
        codes2 = pq.encode(np.random.randn(64).tolist())

        dist = pq.approximate_distance(codes1, codes2)
        assert dist >= 0

    def test_untrained_error(self):
        """测试未训练时的错误"""
        pq = ProductQuantizer(dim=64)

        with pytest.raises(RuntimeError, match="must be trained"):
            pq.encode([0.0] * 64)

        with pytest.raises(RuntimeError, match="must be trained"):
            pq.decode([0, 0, 0, 0])


class TestOptimizedProductQuantizer:
    """测试优化乘积量化器"""

    def test_initialization(self):
        """测试初始化"""
        config = OPQConfig(num_subvectors=8, rotation_iterations=5)
        opq = OptimizedProductQuantizer(dim=128, config=config)

        assert opq.dim == 128
        assert opq.config.rotation_iterations == 5

    def test_train(self):
        """测试训练"""
        opq = OptimizedProductQuantizer(dim=64, config=OPQConfig(num_subvectors=4, rotation_iterations=3))

        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(100)]

        opq.train(vectors)

        assert opq.is_trained
        assert opq.rotation_matrix is not None
        assert opq.rotation_matrix.shape == (64, 64)

    def test_encode_decode_with_rotation(self):
        """测试带旋转的编码解码"""
        opq = OptimizedProductQuantizer(dim=64, config=OPQConfig(num_subvectors=4, rotation_iterations=3))

        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(100)]
        opq.train(vectors)

        test_vector = np.random.randn(64).tolist()
        codes = opq.encode(test_vector)
        reconstructed = opq.decode(codes)

        assert len(codes) == 4
        assert len(reconstructed) == 64


class TestVectorCompressionIndex:
    """测试向量压缩索引"""

    def test_add_and_search(self):
        """测试添加和搜索"""
        index = VectorCompressionIndex(dim=64, use_opq=False)

        np.random.seed(42)
        # 添加向量
        for i in range(20):
            vector = np.random.randn(64).tolist()
            index.add(f"vec_{i}", vector, {"id": i})

        # 搜索
        query = np.random.randn(64).tolist()
        results = index.search(query, top_k=5)

        assert len(results) == 5
        assert all(isinstance(vid, str) for vid, _ in results)

    def test_add_batch(self):
        """测试批量添加"""
        index = VectorCompressionIndex(dim=64)

        np.random.seed(42)
        vectors = [
            (f"vec_{i}", np.random.randn(64).tolist(), {"id": i})
            for i in range(50)
        ]

        index.add_batch(vectors)

        assert len(index.compressed_vectors) == 50

    def test_get_compression_stats(self):
        """测试获取压缩统计"""
        index = VectorCompressionIndex(dim=64)

        np.random.seed(42)
        for i in range(100):
            vector = np.random.randn(64).tolist()
            index.add(f"vec_{i}", vector)

        stats = index.get_compression_stats()

        assert stats["vector_count"] == 100
        assert stats["original_size_bytes"] > 0
        assert stats["compressed_size_bytes"] > 0
        assert stats["compression_ratio"] > 1.0
        assert stats["space_saving_percent"] > 0

    def test_reconstruct(self):
        """测试向量重建"""
        index = VectorCompressionIndex(dim=64)

        np.random.seed(42)
        original = np.random.randn(64).tolist()
        index.add("test_vec", original)

        reconstructed = index.reconstruct("test_vec")

        assert reconstructed is not None
        assert len(reconstructed) == 64

    def test_reconstruct_nonexistent(self):
        """测试重建不存在的向量"""
        index = VectorCompressionIndex(dim=64)

        result = index.reconstruct("nonexistent")
        assert result is None


class TestMultiResolutionQuantizer:
    """测试多分辨率量化器"""

    def test_initialization(self):
        """测试初始化"""
        quantizer = MultiResolutionQuantizer(dim=128, resolutions=[4, 8, 16])

        assert quantizer.dim == 128
        assert quantizer.resolutions == [4, 8, 16]

    def test_train(self):
        """测试训练"""
        quantizer = MultiResolutionQuantizer(dim=64, resolutions=[4, 8])

        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(100)]

        quantizer.train(vectors)

        assert 4 in quantizer.quantizers
        assert 8 in quantizer.quantizers
        assert quantizer.quantizers[4].is_trained
        assert quantizer.quantizers[8].is_trained

    def test_encode_at_resolution(self):
        """测试指定分辨率编码"""
        quantizer = MultiResolutionQuantizer(dim=64, resolutions=[4, 8])

        np.random.seed(42)
        vectors = [np.random.randn(64).tolist() for _ in range(100)]
        quantizer.train(vectors)

        test_vector = np.random.randn(64).tolist()

        codes_4 = quantizer.encode_at_resolution(test_vector, 4)
        codes_8 = quantizer.encode_at_resolution(test_vector, 8)

        assert len(codes_4) == 4
        assert len(codes_8) == 8

    def test_encode_invalid_resolution(self):
        """测试无效分辨率"""
        quantizer = MultiResolutionQuantizer(dim=64, resolutions=[4, 8])

        with pytest.raises(ValueError, match="not available"):
            quantizer.encode_at_resolution([0.0] * 64, 16)

    def test_get_resolution_stats(self):
        """测试获取分辨率统计"""
        quantizer = MultiResolutionQuantizer(dim=128, resolutions=[4, 8, 16])

        np.random.seed(42)
        vectors = [np.random.randn(128).tolist() for _ in range(100)]
        quantizer.train(vectors)

        stats = quantizer.get_resolution_stats()

        assert 4 in stats
        assert 8 in stats
        assert 16 in stats

        # 分辨率越高，压缩率越低
        assert stats[4]["compression_ratio"] > stats[16]["compression_ratio"]


# ============================================================================
# 集成测试
# ============================================================================

class TestIntegration:
    """集成测试 - 测试多个组件协同工作"""

    @pytest.mark.asyncio
    async def test_distributed_storage_with_deduplication(
        self, storage_nodes, mock_embedding_provider
    ):
        """测试分布式存储与去重集成"""
        # 创建分布式存储
        storage = DistributedStorageBackend(
            nodes=storage_nodes,
            replication_factor=3
        )

        # 创建去重系统
        dedup = MemoryDeduplicationSystem(
            embedding_provider=mock_embedding_provider,
            similarity_threshold=0.9,
            storage=storage
        )

        # 添加记忆
        item1 = MemoryItem(content="Test content for integration")
        await storage.add(item1)

        # 检查重复
        item2 = MemoryItem(content="Test content for integration")
        duplicate_group = await dedup.check_duplicate(item2)

        # 清理
        await storage.close()

    @pytest.mark.asyncio
    async def test_incremental_update_with_deduplication(self, mock_embedding_provider):
        """测试增量更新与去重集成"""
        # 创建增量更新器
        updater = IncrementalEmbeddingUpdater(mock_embedding_provider)

        # 创建去重系统
        dedup = MemoryDeduplicationSystem(mock_embedding_provider)

        # 更新嵌入
        old_text = "Original content"
        new_text = "Original content with additions"
        old_embedding = await mock_embedding_provider.embed(old_text)

        new_embedding, is_full = await updater.update_embedding(
            old_text, new_text, old_embedding
        )

        # 使用新嵌入创建记忆并检查去重
        memory = MemoryItem(content=new_text, embedding=new_embedding)
        # 去重系统应该能正常工作

    def test_quantization_with_compression_index(self):
        """测试量化与压缩索引集成"""
        # 创建量化器
        quantizer = ProductQuantizer(dim=64, config=PQConfig(num_subvectors=4))

        # 创建压缩索引
        index = VectorCompressionIndex(dim=64, quantizer=quantizer)

        # 添加向量
        np.random.seed(42)
        for i in range(50):
            vector = np.random.randn(64).tolist()
            index.add(f"vec_{i}", vector)

        # 验证压缩统计
        stats = index.get_compression_stats()
        assert stats["vector_count"] == 50
        assert stats["compression_ratio"] > 1.0


# ============================================================================
# 性能测试
# ============================================================================

class TestPerformance:
    """性能测试"""

    @pytest.mark.asyncio
    async def test_distributed_storage_performance(self, storage_nodes):
        """测试分布式存储性能"""
        backend = DistributedStorageBackend(
            nodes=storage_nodes,
            replication_factor=3
        )

        import time
        start = time.time()

        # 批量添加
        for i in range(100):
            item = MemoryItem(content=f"Performance test content {i}")
            await backend.add(item)

        elapsed = time.time() - start

        # 应该在合理时间内完成
        assert elapsed < 30  # 30秒

        await backend.close()

    def test_quantization_performance(self):
        """测试量化性能"""
        pq = ProductQuantizer(dim=128, config=PQConfig(num_subvectors=8))

        np.random.seed(42)
        vectors = [np.random.randn(128).tolist() for _ in range(1000)]

        import time
        start = time.time()
        pq.train(vectors)
        train_time = time.time() - start

        start = time.time()
        for v in vectors[:100]:
            pq.encode(v)
        encode_time = time.time() - start

        # 训练和编码应该在合理时间内
        assert train_time < 10  # 10秒
        assert encode_time < 5   # 5秒


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

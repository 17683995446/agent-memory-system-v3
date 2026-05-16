"""
SimilarityConsolidation 策略单元测试
"""

import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.conftest import MockEmbeddingProvider
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.consolidation_strategies.similarity_based_consolidation_strategy import SimilarityConsolidation
from memory_system.core_abstractions.component_registry_manager import _consolidation_registry


class TestShouldConsolidateTrue:
    """should_consolidate 在情景记忆数量足够时返回 True"""

    @pytest.mark.asyncio
    async def test_should_consolidate_true(self):
        strategy = SimilarityConsolidation(min_cluster_size=2)
        memories = [
            MemoryItem(content=f"情景{i}", memory_type=MemoryType.EPISODIC)
            for i in range(4)
        ]
        stats = {}
        result = await strategy.should_consolidate(memories, stats)
        assert result is True


class TestShouldConsolidateFalse:
    """should_consolidate 在情景记忆数量不足时返回 False"""

    @pytest.mark.asyncio
    async def test_should_consolidate_false(self):
        strategy = SimilarityConsolidation(min_cluster_size=2)
        memories = [
            MemoryItem(content="情景1", memory_type=MemoryType.EPISODIC),
            MemoryItem(content="语义1", memory_type=MemoryType.SEMANTIC),
        ]
        stats = {}
        result = await strategy.should_consolidate(memories, stats)
        assert result is False


class TestConsolidateClustersSimilar:
    """consolidate 将相似的记忆聚类并生成语义记忆"""

    @pytest.mark.asyncio
    async def test_consolidate_clusters_similar(self, mock_embedding):
        strategy = SimilarityConsolidation(
            similarity_threshold=0.85,
            min_cluster_size=2,
            importance_threshold=ImportanceLevel.MEDIUM,
        )
        # 使用相同前缀创建记忆，使嵌入向量相同（MockEmbeddingProvider 基于文本 hash）
        # 相同文本 -> 相同嵌入 -> 相似度 1.0
        base_text = "用户讨论了机器学习算法的优化方法"
        memories = [
            MemoryItem(
                content=base_text,
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                embedding=await mock_embedding.embed(base_text),
            )
            for _ in range(3)
        ]
        new_memories = await strategy.consolidate(memories, mock_embedding)
        assert len(new_memories) >= 1
        assert new_memories[0].memory_type == MemoryType.SEMANTIC


class TestConsolidateSkipsNoEmbedding:
    """consolidate 跳过没有嵌入向量的记忆"""

    @pytest.mark.asyncio
    async def test_consolidate_skips_no_embedding(self, mock_embedding):
        strategy = SimilarityConsolidation(
            similarity_threshold=0.85,
            min_cluster_size=2,
        )
        # 一条有嵌入，一条没有嵌入 -> 不足 min_cluster_size -> 返回空
        memories = [
            MemoryItem(
                content="有嵌入的记忆",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                embedding=await mock_embedding.embed("有嵌入的记忆"),
            ),
            MemoryItem(
                content="无嵌入的记忆",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                embedding=None,
            ),
        ]
        new_memories = await strategy.consolidate(memories, mock_embedding)
        assert new_memories == []


class TestConsolidateMetadata:
    """consolidate 生成的语义记忆包含正确的元数据"""

    @pytest.mark.asyncio
    async def test_consolidate_metadata(self, mock_embedding):
        strategy = SimilarityConsolidation(
            similarity_threshold=0.85,
            min_cluster_size=2,
        )
        base_text = "关于深度学习模型训练的讨论"
        memories = [
            MemoryItem(
                content=base_text,
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                embedding=await mock_embedding.embed(base_text),
            )
            for _ in range(2)
        ]
        new_memories = await strategy.consolidate(memories, mock_embedding)
        assert len(new_memories) == 1

        meta = new_memories[0].metadata
        assert "consolidated_from" in meta
        assert "cluster_size" in meta
        assert "avg_similarity" in meta
        assert meta["cluster_size"] == 2
        assert isinstance(meta["consolidated_from"], list)
        assert len(meta["consolidated_from"]) == 2


class TestSimilarityThreshold:
    """similarity_threshold 参数控制聚类行为"""

    @pytest.mark.asyncio
    async def test_high_threshold_no_cluster(self, mock_embedding):
        """极高的相似度阈值导致无法聚类"""
        strategy = SimilarityConsolidation(
            similarity_threshold=1.0,  # 要求完全相同的嵌入
            min_cluster_size=2,
        )
        # 使用不同文本，产生不同嵌入
        memories = [
            MemoryItem(
                content=f"不同的记忆内容编号{i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                embedding=await mock_embedding.embed(f"不同的记忆内容编号{i}"),
            )
            for i in range(3)
        ]
        new_memories = await strategy.consolidate(memories, mock_embedding)
        # 不同文本产生不同嵌入，相似度不会达到 1.0，无法聚类
        assert new_memories == []

    @pytest.mark.asyncio
    async def test_low_threshold_clusters(self, mock_embedding):
        """低相似度阈值允许聚类"""
        strategy = SimilarityConsolidation(
            similarity_threshold=0.0,  # 极低阈值，任何记忆都能聚类
            min_cluster_size=2,
        )
        memories = [
            MemoryItem(
                content=f"记忆内容{i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                embedding=await mock_embedding.embed(f"记忆内容{i}"),
            )
            for i in range(3)
        ]
        new_memories = await strategy.consolidate(memories, mock_embedding)
        assert len(new_memories) >= 1


class TestRegistryRegistration:
    """SimilarityConsolidation 注册为 'similarity' 策略"""

    def test_registered_as_similarity(self):
        assert "similarity" in _consolidation_registry
        assert _consolidation_registry["similarity"] is SimilarityConsolidation

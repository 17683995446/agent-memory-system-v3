"""
测试 core/models.py 中的数据模型
"""

import time
import pytest

from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    MemoryStats,
    RetrievalResponse,
)


class TestMemoryItemCreation:
    """test_memory_item_creation: 创建 MemoryItem，验证默认值"""

    def test_memory_item_creation(self):
        item = MemoryItem(content="测试记忆内容")
        assert item.content == "测试记忆内容"
        assert item.memory_type == MemoryType.SHORT_TERM
        assert item.importance == ImportanceLevel.MEDIUM
        assert item.access_count == 0
        assert item.embedding is None
        assert item.metadata == {}
        assert item.memory_id is not None
        assert len(item.memory_id) == 32
        assert isinstance(item.timestamp, float)
        assert isinstance(item.last_access, float)


class TestMemoryItemUuidUniqueness:
    """test_memory_item_uuid_uniqueness: 创建100个 MemoryItem，验证 memory_id 唯一"""

    def test_memory_item_uuid_uniqueness(self):
        ids = set()
        for _ in range(100):
            item = MemoryItem(content="唯一性测试")
            ids.add(item.memory_id)
        assert len(ids) == 100


class TestMemoryTypeEnum:
    """test_memory_type_enum: 验证枚举值"""

    def test_memory_type_enum(self):
        assert MemoryType.SHORT_TERM.value == "short_term"
        assert MemoryType.EPISODIC.value == "episodic"
        assert MemoryType.SEMANTIC.value == "semantic"
        assert len(MemoryType) == 3


class TestImportanceLevelEnum:
    """test_importance_level_enum: 验证枚举值和 int 比较"""

    def test_importance_level_enum(self):
        assert ImportanceLevel.TRIVIAL.value == 1
        assert ImportanceLevel.LOW.value == 2
        assert ImportanceLevel.MEDIUM.value == 3
        assert ImportanceLevel.HIGH.value == 4
        assert ImportanceLevel.CRITICAL.value == 5
        # int 比较
        assert ImportanceLevel.HIGH > ImportanceLevel.MEDIUM
        assert ImportanceLevel.CRITICAL >= 5
        assert ImportanceLevel.TRIVIAL < 2


class TestCalculateRelevanceScore:
    """test_calculate_relevance_score: 验证分数在 [0,1] 范围，高重要性分数更高"""

    def test_calculate_relevance_score_range(self):
        item = MemoryItem(
            content="测试",
            importance=ImportanceLevel.MEDIUM,
        )
        score = item.calculate_relevance_score(current_time=time.time())
        assert 0.0 <= score <= 1.0

    def test_calculate_relevance_score_high_importance(self):
        now = time.time()
        high_item = MemoryItem(
            content="高重要性",
            importance=ImportanceLevel.CRITICAL,
        )
        low_item = MemoryItem(
            content="低重要性",
            importance=ImportanceLevel.TRIVIAL,
        )
        high_score = high_item.calculate_relevance_score(current_time=now)
        low_score = low_item.calculate_relevance_score(current_time=now)
        assert high_score > low_score


class TestCalculateRelevanceScoreShortTermDecay:
    """test_calculate_relevance_score_short_term_decay: 短期记忆衰减更快"""

    def test_calculate_relevance_score_short_term_decay(self):
        now = time.time()
        old_time = now - 3600 * 2  # 2小时前

        short_term = MemoryItem(
            content="短期记忆",
            memory_type=MemoryType.SHORT_TERM,
            importance=ImportanceLevel.MEDIUM,
        )
        short_term.timestamp = old_time

        long_term = MemoryItem(
            content="长期记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.MEDIUM,
        )
        long_term.timestamp = old_time

        st_score = short_term.calculate_relevance_score(current_time=now)
        lt_score = long_term.calculate_relevance_score(current_time=now)
        assert st_score < lt_score


class TestTouch:
    """test_touch: 验证 access_count 增加和 last_access 更新"""

    def test_touch(self):
        item = MemoryItem(content="测试touch")
        old_count = item.access_count
        old_access = item.last_access
        time.sleep(0.01)
        item.touch()
        assert item.access_count == old_count + 1
        assert item.last_access > old_access


class TestToStorageDictAndFromStorageDict:
    """test_to_storage_dict_and_from_storage_dict: 序列化/反序列化往返"""

    def test_to_storage_dict_and_from_storage_dict(self):
        original = MemoryItem(
            content="序列化测试",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
            metadata={"key": "value"},
        )
        data = original.to_storage_dict()
        assert data["content"] == "序列化测试"
        assert data["memory_type"] == MemoryType.EPISODIC
        assert data["importance"] == ImportanceLevel.HIGH
        assert data["metadata"] == {"key": "value"}

        restored = MemoryItem.from_storage_dict(data)
        assert restored.content == original.content
        assert restored.memory_type == original.memory_type
        assert restored.importance == original.importance
        assert restored.memory_id == original.memory_id
        assert restored.metadata == original.metadata


class TestRetrievalResponseModel:
    """test_retrieval_response_model: 验证 RetrievalResponse 创建"""

    def test_retrieval_response_model(self):
        resp = RetrievalResponse(
            query="测试查询",
            strategy="hierarchical",
        )
        assert resp.query == "测试查询"
        assert resp.strategy == "hierarchical"
        assert resp.layers == {}
        assert resp.combined == []

        # 带数据
        resp2 = RetrievalResponse(
            query="查询",
            strategy="vector",
            layers={"vector": [1, 2, 3]},
            combined=[{"memory": "data"}],
        )
        assert resp2.layers == {"vector": [1, 2, 3]}
        assert len(resp2.combined) == 1


class TestMemoryStatsModel:
    """test_memory_stats_model: 验证 MemoryStats 默认值"""

    def test_memory_stats_model(self):
        stats = MemoryStats()
        assert stats.short_term_count == 0
        assert stats.episodic_count == 0
        assert stats.semantic_count == 0
        assert stats.total_count == 0
        assert stats.total_added == 0
        assert stats.total_retrieved == 0
        assert stats.uptime_hours == 0.0

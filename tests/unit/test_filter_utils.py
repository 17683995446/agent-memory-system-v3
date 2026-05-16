"""
filter_utility_functions 单元测试
覆盖之前零测试的 matches_filters 和 separate_metadata 函数
"""
import pytest
from memory_system.core_abstractions.filter_utility_functions import (
    matches_filters,
    separate_metadata,
    STORAGE_SYSTEM_KEYS,
)
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem, MemoryType, ImportanceLevel
)


class TestMatchesFilters:
    """测试 matches_filters 函数"""

    def _make_memory(self, memory_type="episodic", importance="high", metadata=None):
        return MemoryItem(
            content="test content",
            memory_type=MemoryType(memory_type),
            importance=ImportanceLevel[importance.upper()],
            metadata=metadata or {},
        )

    def test_no_filters(self):
        """无过滤条件应返回 True"""
        memory = self._make_memory()
        assert matches_filters(memory, {}) is True

    def test_memory_type_match(self):
        """memory_type 匹配"""
        memory = self._make_memory(memory_type="episodic")
        assert matches_filters(memory, {"memory_type": "episodic"}) is True

    def test_memory_type_no_match(self):
        """memory_type 不匹配"""
        memory = self._make_memory(memory_type="semantic")
        assert matches_filters(memory, {"memory_type": "episodic"}) is False

    def test_importance_match(self):
        """importance 匹配"""
        memory = self._make_memory(importance="high")
        assert matches_filters(memory, {"importance": 4}) is True

    def test_importance_no_match(self):
        """importance 不匹配"""
        memory = self._make_memory(importance="high")
        assert matches_filters(memory, {"importance": 1}) is False

    def test_metadata_filter_match(self):
        """metadata 前缀过滤匹配"""
        memory = self._make_memory(metadata={"source": "user"})
        assert matches_filters(memory, {"metadata.source": "user"}) is True

    def test_metadata_filter_no_match(self):
        """metadata 前缀过滤不匹配"""
        memory = self._make_memory(metadata={"source": "user"})
        assert matches_filters(memory, {"metadata.source": "system"}) is False

    def test_metadata_filter_missing_key(self):
        """metadata 中缺少过滤键"""
        memory = self._make_memory(metadata={})
        assert matches_filters(memory, {"metadata.source": "user"}) is False

    def test_multiple_filters_all_match(self):
        """多条件全部匹配"""
        memory = self._make_memory(
            memory_type="episodic",
            importance="high",
            metadata={"source": "user"}
        )
        filters = {"memory_type": "episodic", "importance": 4, "metadata.source": "user"}
        assert matches_filters(memory, filters) is True

    def test_multiple_filters_partial_match(self):
        """多条件部分匹配应返回 False"""
        memory = self._make_memory(memory_type="episodic", importance="high")
        filters = {"memory_type": "episodic", "importance": 1}
        assert matches_filters(memory, filters) is False


class TestSeparateMetadata:
    """测试 separate_metadata 函数"""

    def test_separate_system_keys(self):
        """系统键应被分离到 system_data"""
        data = {
            "content": "hello",
            "memory_type": "episodic",
            "importance": "high",
            "custom_field": "value",
            "another_custom": 42,
        }
        system_data, metadata = separate_metadata(data, STORAGE_SYSTEM_KEYS)
        assert "content" in system_data
        assert "memory_type" in system_data
        assert "custom_field" in metadata
        assert "another_custom" in metadata

    def test_empty_dict(self):
        """空字典"""
        system_data, metadata = separate_metadata({}, STORAGE_SYSTEM_KEYS)
        assert system_data == {}
        assert metadata == {}

    def test_all_system_keys(self):
        """全部为系统键"""
        data = {"content": "test", "memory_id": "abc"}
        system_data, metadata = separate_metadata(data, STORAGE_SYSTEM_KEYS)
        assert len(metadata) == 0
        assert len(system_data) == 2

    def test_all_metadata_keys(self):
        """全部为元数据键"""
        data = {"custom1": "a", "custom2": "b"}
        system_data, metadata = separate_metadata(data, STORAGE_SYSTEM_KEYS)
        assert len(system_data) == 0
        assert len(metadata) == 2

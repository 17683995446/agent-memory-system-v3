"""
测试 consolidation/ 下的两个策略:
- TimeBasedConsolidation
- ScoreBasedForgetting
"""

import time
import pytest

from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.consolidation_strategies.time_based_consolidation_strategy import TimeBasedConsolidation
from memory_system.forgetting_strategies.score_based_forgetting_strategy import ScoreBasedForgetting


# ============================================================
# TimeBasedConsolidation 测试
# ============================================================


class TestShouldConsolidate:
    """test_should_consolidate: 情景记忆足够多时返回 True"""

    @pytest.mark.asyncio
    async def test_should_consolidate(self):
        strategy = TimeBasedConsolidation(min_cluster_size=3)
        memories = [
            MemoryItem(content=f"情景{i}", memory_type=MemoryType.EPISODIC)
            for i in range(6)
        ]
        stats = {}
        result = await strategy.should_consolidate(memories, stats)
        assert result is True


class TestShouldNotConsolidate:
    """test_should_not_consolidate: 情景记忆不足时返回 False"""

    @pytest.mark.asyncio
    async def test_should_not_consolidate(self):
        strategy = TimeBasedConsolidation(min_cluster_size=3)
        memories = [
            MemoryItem(content="情景1", memory_type=MemoryType.EPISODIC),
            MemoryItem(content="语义1", memory_type=MemoryType.SEMANTIC),
        ]
        stats = {}
        result = await strategy.should_consolidate(memories, stats)
        assert result is False


class TestConsolidateCreatesSummary:
    """test_consolidate_creates_summary: 整合后生成语义记忆"""

    @pytest.mark.asyncio
    async def test_consolidate_creates_summary(self, mock_embedding):
        strategy = TimeBasedConsolidation(
            time_window_seconds=1,  # 很小的时间窗口
            min_cluster_size=2,
        )
        now = time.time()
        memories = [
            MemoryItem(
                content=f"事件{i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                timestamp=now - 10,  # 超过 time_window_seconds
            )
            for i in range(3)
        ]
        new_memories = await strategy.consolidate(memories, mock_embedding)
        assert len(new_memories) >= 1
        assert new_memories[0].memory_type == MemoryType.SEMANTIC
        assert new_memories[0].importance == ImportanceLevel.HIGH
        assert new_memories[0].embedding is not None


class TestConsolidateBelowThreshold:
    """test_consolidate_below_threshold: 不足 min_cluster_size 时返回空"""

    @pytest.mark.asyncio
    async def test_consolidate_below_threshold(self, mock_embedding):
        strategy = TimeBasedConsolidation(
            min_cluster_size=5,
        )
        now = time.time()
        memories = [
            MemoryItem(
                content=f"事件{i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.HIGH,
                timestamp=now - 3600,
            )
            for i in range(3)
        ]
        new_memories = await strategy.consolidate(memories, mock_embedding)
        assert new_memories == []


class TestClusterByTime:
    """test_cluster_by_time: 验证时间窗口聚类"""

    def test_cluster_by_time(self):
        strategy = TimeBasedConsolidation(time_window_seconds=100)
        now = time.time()

        memories = [
            MemoryItem(
                content="事件A",
                memory_type=MemoryType.EPISODIC,
                timestamp=now - 300,
            ),
            MemoryItem(
                content="事件B",
                memory_type=MemoryType.EPISODIC,
                timestamp=now - 250,
            ),
            MemoryItem(
                content="事件C",
                memory_type=MemoryType.EPISODIC,
                timestamp=now - 50,
            ),
            MemoryItem(
                content="事件D",
                memory_type=MemoryType.EPISODIC,
                timestamp=now - 10,
            ),
        ]
        clusters = strategy._cluster_by_time(memories)
        # 事件A和B在100秒窗口内，事件C和D在100秒窗口内，但B和C间隔200秒
        assert len(clusters) == 2
        assert len(clusters[0]) == 2
        assert len(clusters[1]) == 2


# ============================================================
# ScoreBasedForgetting 测试
# ============================================================


class TestNoForgettingWhenBelowMax:
    """test_no_forgetting_when_below_max: 不超过 max_keep 时不遗忘"""

    @pytest.mark.asyncio
    async def test_no_forgetting_when_below_max(self):
        strategy = ScoreBasedForgetting()
        memories = [
            MemoryItem(content=f"记忆{i}", memory_type=MemoryType.EPISODIC)
            for i in range(5)
        ]
        result = await strategy.select_for_forgetting(memories, max_keep=10)
        assert result == []


class TestForgettingSelectsLowScore:
    """test_forgetting_selects_low_score: 验证选择低分记忆遗忘"""

    @pytest.mark.asyncio
    async def test_forgetting_selects_low_score(self):
        strategy = ScoreBasedForgetting(min_retention_days=0.0, forget_ratio=0.1)
        now = time.time()

        # 高分记忆：高重要性 + 高访问
        high_score = MemoryItem(
            content="重要且频繁访问",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.CRITICAL,
            timestamp=now - 86400 * 5,
            access_count=10,
            last_access=now - 3600,
        )
        # 低分记忆：低重要性 + 低访问 + 很旧
        low_score = MemoryItem(
            content="不重要且很少访问",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.TRIVIAL,
            timestamp=now - 86400 * 60,
            access_count=0,
            last_access=now - 86400 * 30,
        )
        memories = [high_score, low_score]
        # len(memories)=2 > max_keep=1, 触发遗忘
        # target_count = int(1 * 0.9) = 0
        # 由于 target_count=0，所有记忆都会被选中
        # 验证低分记忆在遗忘列表中
        result = await strategy.select_for_forgetting(memories, max_keep=1)
        assert len(result) >= 1
        assert low_score.memory_id in result


class TestMinRetentionDaysRespected:
    """test_min_retention_days_respected: 新记忆不被遗忘"""

    @pytest.mark.asyncio
    async def test_min_retention_days_respected(self):
        strategy = ScoreBasedForgetting(min_retention_days=7.0)
        now = time.time()

        # 新记忆（不到1天）
        new_memory = MemoryItem(
            content="新记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.LOW,
            timestamp=now - 3600,  # 1小时前
        )
        old_memory = MemoryItem(
            content="旧记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.LOW,
            timestamp=now - 86400 * 30,  # 30天前
        )
        memories = [new_memory, old_memory]
        result = await strategy.select_for_forgetting(memories, max_keep=1)
        # 新记忆不应被遗忘（在 min_retention_days 内）
        assert new_memory.memory_id not in result


class TestRetentionScoreCalculation:
    """test_retention_score_calculation: 验证分数计算（高重要性+高访问=高分）"""

    def test_retention_score_calculation(self):
        strategy = ScoreBasedForgetting()
        now = time.time()

        # 高重要性 + 高访问
        high_item = MemoryItem(
            content="高分记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.CRITICAL,
            timestamp=now - 86400,
            access_count=10,
            last_access=now - 3600,
        )
        # 低重要性 + 低访问
        low_item = MemoryItem(
            content="低分记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.TRIVIAL,
            timestamp=now - 86400,
            access_count=0,
            last_access=now - 86400 * 7,
        )

        high_score = strategy._calculate_retention_score(high_item, now)
        low_score = strategy._calculate_retention_score(low_item, now)
        assert high_score > low_score
        assert 0.0 <= high_score <= 1.0
        assert 0.0 <= low_score <= 1.0

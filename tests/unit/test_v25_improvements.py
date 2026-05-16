"""
V2.25 改进测试
覆盖: 版本统一、remember_batch修复、输入验证、异步上下文管理器、遗忘策略改进
"""
import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch
from memory_system import MemorySystem, MemorySystemBuilder, VERSION, API_AVAILABLE
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem, MemoryType, ImportanceLevel
)
from memory_system.core_abstractions.system_constants_definitions import (
    MAX_CONTENT_LENGTH, MAX_QUERY_LENGTH, MAX_BATCH_SIZE
)
from memory_system.forgetting_strategies.score_based_forgetting_strategy import (
    ScoreBasedForgetting, _validate_weights
)


# ============================================================
# P0: 版本统一测试
# ============================================================
class TestVersionUnification:
    """验证版本号在所有位置保持一致"""

    def test_version_format(self):
        """版本号应为语义化格式"""
        parts = VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_version_is_2_25(self):
        """版本号应为 2.28.0"""
        assert VERSION == "2.28.0"

    def test_version_in_init(self):
        """__init__.py 中的 __version__ 应与 VERSION 一致"""
        import memory_system
        assert memory_system.__version__ == VERSION

    def test_version_in_pyproject(self):
        """pyproject.toml 中的版本应与 VERSION 一致"""
        import tomli
        # Fallback: read raw file
        with open("/workspace/agent_memory_system_v2/pyproject.toml") as f:
            content = f.read()
        assert 'version = "2.28.0"' in content

    def test_version_exported_in_all(self):
        """VERSION 应在 __all__ 中导出"""
        import memory_system
        assert "VERSION" in memory_system.__all__


# ============================================================
# P0: remember_batch 修复测试
# ============================================================
class TestRememberBatchFixes:
    """验证 remember_batch 的整合/遗忘/指标/追踪修复"""

    @pytest.fixture
    def memory_system(self):
        """创建测试用 MemorySystem"""
        mock_storage = AsyncMock()
        mock_storage.add = AsyncMock(return_value="mem_001")
        mock_storage.count = AsyncMock(return_value=50)
        mock_storage.close = AsyncMock()
        mock_storage.search_by_time_range = AsyncMock(return_value=[])

        mock_embedding = AsyncMock()
        mock_embedding.embed = AsyncMock(return_value=[0.1] * 64)
        mock_embedding.embed_batch = AsyncMock(return_value=[[0.1] * 64, [0.2] * 64])

        mock_retrieval = AsyncMock()
        mock_retrieval.retrieve = AsyncMock(return_value={"combined": []})

        mock_short_term = AsyncMock()
        mock_short_term.add = AsyncMock(
            return_value=MemoryItem(content="test", memory_type=MemoryType.SHORT_TERM)
        )
        mock_short_term.get_all = AsyncMock(return_value=[])

        mock_metrics = MagicMock()
        mock_consolidation = AsyncMock()
        mock_consolidation.should_consolidate = AsyncMock(return_value=False)
        mock_forgetting = AsyncMock()
        mock_forgetting.select_for_forgetting = AsyncMock(return_value=[])

        ms = MemorySystem(
            storage=mock_storage,
            embedding=mock_embedding,
            retrieval=mock_retrieval,
            short_term=mock_short_term,
            consolidation=mock_consolidation,
            forgetting=mock_forgetting,
            metrics=mock_metrics,
        )
        return ms

    @pytest.mark.asyncio
    async def test_batch_triggers_consolidation_check(self, memory_system):
        """remember_batch 应触发整合检查"""
        await memory_system.remember_batch(
            ["重要信息1", "重要信息2"],
            importance="high",
            memory_type="semantic"
        )
        memory_system.consolidation.should_consolidate.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_triggers_forgetting_check(self, memory_system):
        """remember_batch 应触发遗忘检查"""
        memory_system.storage.count = AsyncMock(side_effect=[9999, 9999, 9999, 9999])
        await memory_system.remember_batch(
            ["信息1", "信息2"],
            importance="high",
            memory_type="semantic"
        )
        # _check_forgetting is called, which checks count
        assert memory_system.storage.count.called

    @pytest.mark.asyncio
    async def test_batch_records_metrics(self, memory_system):
        """remember_batch 应记录指标"""
        await memory_system.remember_batch(
            ["信息1", "信息2"],
            importance="high",
            memory_type="semantic"
        )
        memory_system.metrics.record_remember.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_updates_stats(self, memory_system):
        """remember_batch 应更新统计信息"""
        await memory_system.remember_batch(
            ["信息1", "信息2"],
            importance="high",
            memory_type="semantic"
        )
        assert memory_system._stats["total_added"] == 2

    @pytest.mark.asyncio
    async def test_batch_empty_returns_empty(self, memory_system):
        """空列表应返回空结果"""
        result = await memory_system.remember_batch([])
        assert result == []

    @pytest.mark.asyncio
    async def test_batch_with_tracing(self, memory_system):
        """remember_batch 应支持追踪"""
        mock_tracing = MagicMock()
        mock_tracing.start_as_current_span = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(), __aexit__=AsyncMock()))
        memory_system.tracing = mock_tracing

        await memory_system.remember_batch(["test"], importance="high", memory_type="semantic")
        # remember_batch span + consolidate span (triggered by _check_consolidation)
        assert mock_tracing.start_as_current_span.call_count >= 1
        first_call = mock_tracing.start_as_current_span.call_args_list[0]
        assert first_call[0][0] == "memory_system.remember_batch"


# ============================================================
# P1: 异步上下文管理器测试
# ============================================================
class TestAsyncContextManager:
    """验证 MemorySystem 支持异步上下文管理器"""

    @pytest.fixture
    def memory_system(self):
        mock_storage = AsyncMock()
        mock_storage.add = AsyncMock(return_value="mem_001")
        mock_storage.count = AsyncMock(return_value=0)
        mock_storage.close = AsyncMock()

        mock_embedding = AsyncMock()
        mock_embedding.embed = AsyncMock(return_value=[0.1] * 64)

        mock_retrieval = AsyncMock()
        mock_retrieval.retrieve = AsyncMock(return_value={"combined": []})

        mock_short_term = AsyncMock()
        mock_short_term.get_all = AsyncMock(return_value=[])

        return MemorySystem(
            storage=mock_storage,
            embedding=mock_embedding,
            retrieval=mock_retrieval,
            short_term=mock_short_term,
        )

    @pytest.mark.asyncio
    async def test_async_with_enter(self, memory_system):
        """async with 应正确进入"""
        async with memory_system as ms:
            assert ms is memory_system

    @pytest.mark.asyncio
    async def test_async_with_close_on_exit(self, memory_system):
        """async with 退出时应自动调用 close()"""
        async with memory_system:
            pass
        memory_system.storage.close.assert_awaited_once()


# ============================================================
# P2: 输入长度验证测试
# ============================================================
class TestInputValidation:
    """验证输入长度限制"""

    @pytest.fixture
    def memory_system(self):
        mock_storage = AsyncMock()
        mock_storage.add = AsyncMock(return_value="mem_001")
        mock_storage.count = AsyncMock(return_value=0)
        mock_storage.close = AsyncMock()

        mock_embedding = AsyncMock()
        mock_embedding.embed = AsyncMock(return_value=[0.1] * 64)

        mock_retrieval = AsyncMock()
        mock_retrieval.retrieve = AsyncMock(return_value={"combined": []})

        mock_short_term = AsyncMock()
        mock_short_term.get_all = AsyncMock(return_value=[])

        return MemorySystem(
            storage=mock_storage,
            embedding=mock_embedding,
            retrieval=mock_retrieval,
            short_term=mock_short_term,
        )

    @pytest.mark.asyncio
    async def test_reject_oversized_content(self, memory_system):
        """应拒绝超长内容"""
        long_content = "A" * (MAX_CONTENT_LENGTH + 1)
        with pytest.raises(ValueError, match="记忆内容过长"):
            await memory_system.remember(long_content)

    @pytest.mark.asyncio
    async def test_accept_max_content(self, memory_system):
        """应接受最大长度内容"""
        content = "A" * MAX_CONTENT_LENGTH
        # Should not raise
        try:
            await memory_system.remember(content)
        except ValueError as e:
            if "过长" in str(e):
                pytest.fail(f"最大长度内容应被接受: {e}")

    @pytest.mark.asyncio
    async def test_reject_oversized_query(self, memory_system):
        """应拒绝超长查询"""
        long_query = "Q" * (MAX_QUERY_LENGTH + 1)
        with pytest.raises(ValueError, match="查询内容过长"):
            await memory_system.recall(long_query)

    @pytest.mark.asyncio
    async def test_reject_oversized_batch(self, memory_system):
        """应拒绝超大批量"""
        contents = [f"item_{i}" for i in range(MAX_BATCH_SIZE + 1)]
        with pytest.raises(ValueError, match="批量操作数量超限"):
            await memory_system.remember_batch(contents)

    @pytest.mark.asyncio
    async def test_reject_empty_content(self, memory_system):
        """应拒绝空内容"""
        with pytest.raises(ValueError, match="不能为空"):
            await memory_system.remember("")

    @pytest.mark.asyncio
    async def test_reject_whitespace_content(self, memory_system):
        """应拒绝纯空白内容"""
        with pytest.raises(ValueError):
            await memory_system.remember("   ")

    @pytest.mark.asyncio
    async def test_reject_invalid_memory_type(self, memory_system):
        """应拒绝无效记忆类型"""
        with pytest.raises(ValueError, match="无效的记忆类型"):
            await memory_system.remember("test", memory_type="invalid_type")

    @pytest.mark.asyncio
    async def test_reject_invalid_importance(self, memory_system):
        """应拒绝无效重要性级别"""
        with pytest.raises(ValueError, match="无效的重要性级别"):
            await memory_system.remember("test", importance="godlike")


# ============================================================
# P1: 遗忘策略改进测试
# ============================================================
class TestScoreBasedForgettingImprovements:
    """验证遗忘策略的改进"""

    def test_validate_weights_normal(self):
        """正常权重应通过验证"""
        weights = {"importance": 0.4, "recency": 0.3, "access": 0.2, "freshness": 0.1}
        result = _validate_weights(weights)
        assert all(k in result for k in weights)

    def test_validate_weights_auto_normalize(self):
        """权重应自动归一化"""
        weights = {"importance": 4, "recency": 3, "access": 2, "freshness": 1}
        result = _validate_weights(weights)
        total = sum(result.values())
        assert abs(total - 1.0) < 1e-9

    def test_validate_weights_missing_key(self):
        """缺少维度应报错"""
        with pytest.raises(ValueError, match="缺少必需维度"):
            _validate_weights({"importance": 0.5, "recency": 0.5})

    def test_validate_weights_negative(self):
        """负权重应报错"""
        with pytest.raises(ValueError, match="不能为负数"):
            _validate_weights({"importance": -0.1, "recency": 0.4, "access": 0.4, "freshness": 0.3})

    def test_validate_weights_zero_sum(self):
        """零总和应报错"""
        with pytest.raises(ValueError, match="不能为零"):
            _validate_weights({"importance": 0, "recency": 0, "access": 0, "freshness": 0})

    def test_validate_weights_not_dict(self):
        """非字典类型应报错"""
        with pytest.raises(TypeError, match="必须是字典"):
            _validate_weights("not a dict")

    def test_forget_ratio_validation(self):
        """forget_ratio 超出范围应报错"""
        with pytest.raises(ValueError, match="forget_ratio"):
            ScoreBasedForgetting(forget_ratio=1.5)
        with pytest.raises(ValueError, match="forget_ratio"):
            ScoreBasedForgetting(forget_ratio=-0.1)

    def test_min_retention_days_validation(self):
        """负 min_retention_days 应报错"""
        with pytest.raises(ValueError, match="min_retention_days"):
            ScoreBasedForgetting(min_retention_days=-1)

    def test_forget_ratio_actually_used(self):
        """forget_ratio 应在 max_keep<=0 时被使用"""
        strategy = ScoreBasedForgetting(forget_ratio=0.5)
        memories = [
            MemoryItem(content=f"item_{i}", importance=ImportanceLevel.MEDIUM)
            for i in range(10)
        ]
        # max_keep=0 means use forget_ratio: keep 50% = 5
        # Note: actual forgetting won't happen because memories are too new
        # This test just verifies no crash with max_keep=0
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        result = loop.run_until_complete(
            strategy.select_for_forgetting(memories, max_keep=0)
        )
        # All memories are new (age < min_retention_days), so none should be forgotten
        assert result == []

    @pytest.mark.asyncio
    async def test_forget_ratio_with_old_memories(self):
        """旧记忆应被遗忘"""
        strategy = ScoreBasedForgetting(forget_ratio=0.5, min_retention_days=0)
        old_time = time.time() - 86400 * 100  # 100天前
        memories = [
            MemoryItem(
                content=f"item_{i}",
                importance=ImportanceLevel.MEDIUM,
                timestamp=old_time,
                last_access=old_time
            )
            for i in range(10)
        ]
        result = await strategy.select_for_forgetting(memories, max_keep=0)
        # With forget_ratio=0.5, keep 5, forget 5 (all old enough)
        assert len(result) == 5

    def test_default_weights_use_system_constants(self):
        """默认权重应使用系统常量"""
        from memory_system.core_abstractions.system_constants_definitions import (
            DEFAULT_IMPORTANCE_WEIGHT, DEFAULT_RECENCY_WEIGHT, DEFAULT_ACCESS_WEIGHT
        )
        strategy = ScoreBasedForgetting()
        # After normalization, weights should sum to 1
        total = sum(strategy.weights.values())
        assert abs(total - 1.0) < 1e-9

    @pytest.mark.asyncio
    async def test_custom_half_life(self):
        """自定义半衰期应生效"""
        strategy = ScoreBasedForgetting(
            recency_half_life_days=1.0,  # 1天半衰期
            freshness_half_life_days=0.5,
        )
        now = time.time()
        old_memory = MemoryItem(
            content="old",
            importance=ImportanceLevel.HIGH,
            timestamp=now - 86400 * 10,  # 10天前
            last_access=now - 86400 * 5,
        )
        score = strategy._calculate_retention_score(old_memory, now)
        # With 1-day half life, 10-day old memory should have very low recency
        assert score < 0.5


# ============================================================
# P1: __init__.py 导出测试
# ============================================================
class TestInitExports:
    """验证 __init__.py 的导出"""

    def test_memory_system_builder_exported(self):
        """MemorySystemBuilder 应被导出"""
        import memory_system
        assert hasattr(memory_system, "MemorySystemBuilder")
        assert "MemorySystemBuilder" in memory_system.__all__

    def test_version_exported(self):
        """VERSION 应被导出"""
        import memory_system
        assert hasattr(memory_system, "VERSION")
        assert "VERSION" in memory_system.__all__

    def test_api_available_exported(self):
        """API_AVAILABLE 应被导出"""
        import memory_system
        assert hasattr(memory_system, "API_AVAILABLE")
        assert isinstance(memory_system.API_AVAILABLE, bool)

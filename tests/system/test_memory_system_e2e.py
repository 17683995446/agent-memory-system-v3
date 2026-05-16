"""
端到端系统测试: MemorySystem 完整生命周期
"""

import asyncio
import pytest

from memory_system.agent_memory_system_facade import MemorySystem
from memory_system.core_abstractions.memory_data_models import MemoryType, ImportanceLevel
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from memory_system.retrieval_strategies.vector_only_retrieval_strategy import VectorOnlyRetrieval
from memory_system.configuration_settings.memory_system_configuration import MemoryConfig


def _create_system(mock_embedding, storage=None, retrieval=None):
    """辅助函数：创建 MemorySystem 实例"""
    return MemorySystem(
        storage=storage or MemoryStorage(max_memories=100),
        embedding=mock_embedding,
        retrieval=retrieval or HierarchicalRetrieval(strategy="adaptive"),
        short_term=ShortTermMemory(capacity=10, ttl_seconds=300),
        config=MemoryConfig(),
    )


class TestFullLifecycle:
    """test_full_lifecycle: 完整生命周期 - 创建系统 -> remember多条 -> recall -> get_stats -> close"""

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, mock_embedding):
        system = _create_system(mock_embedding)

        # remember 多条
        id1 = await system.remember(
            "Python是一种编程语言",
            memory_type="semantic",
            importance="high",
        )
        id2 = await system.remember(
            "用户正在调试代码",
            memory_type="episodic",
            importance="medium",
        )
        id3 = await system.remember(
            "临时笔记",
            memory_type="short_term",
            importance="low",
        )
        assert id1 is not None
        assert id2 is not None
        assert id3 is not None

        # recall
        results = await system.recall("Python编程")
        assert results is not None
        assert "combined" in results

        # get_stats
        stats = await system.get_stats()
        assert stats.total_added >= 2  # 至少有 episodic 和 semantic
        assert stats.total_retrieved >= 1

        # close
        await system.close()


class TestAutoMemoryTypeSelection:
    """test_auto_memory_type_selection: auto模式下高重要性存为semantic，低重要性存为short_term"""

    @pytest.mark.asyncio
    async def test_auto_memory_type_selection(self, mock_embedding):
        system = _create_system(mock_embedding)

        # 高重要性 -> semantic
        high_id = await system.remember("重要知识", memory_type="auto", importance="high")
        high_item = await system.storage.get(high_id)
        assert high_item is not None
        assert high_item.memory_type == MemoryType.SEMANTIC

        # 低重要性 -> short_term
        low_id = await system.remember("临时信息", memory_type="auto", importance="low")
        # short_term 不存入 storage
        low_item = await system.storage.get(low_id)
        assert low_item is None
        # 但应该在短期记忆中
        st_memories = await system.short_term.get_all()
        assert any(m.memory_id == low_id for m in st_memories)

        await system.close()


class TestParameterValidation:
    """test_parameter_validation: 空内容、无效类型、无效重要性等参数验证"""

    @pytest.mark.asyncio
    async def test_parameter_validation(self, mock_embedding):
        system = _create_system(mock_embedding)

        # 空内容
        with pytest.raises(ValueError, match="记忆内容不能为空"):
            await system.remember("", memory_type="episodic")

        with pytest.raises(ValueError, match="记忆内容不能为空"):
            await system.remember("   ", memory_type="episodic")

        # 无效类型
        with pytest.raises(ValueError, match="无效的记忆类型"):
            await system.remember("测试", memory_type="invalid_type")

        # 无效重要性
        with pytest.raises(ValueError, match="无效的重要性级别"):
            await system.remember("测试", memory_type="episodic", importance="super")

        # 空查询
        with pytest.raises(ValueError, match="查询内容不能为空"):
            await system.recall("")

        # 无效 max_results
        with pytest.raises(ValueError, match="max_results"):
            await system.recall("测试", max_results=0)

        await system.close()


class TestGetRecentContext:
    """test_get_recent_context: 验证获取最近上下文"""

    @pytest.mark.asyncio
    async def test_get_recent_context(self, mock_embedding):
        system = _create_system(mock_embedding)

        # 存入短期记忆
        await system.remember("第一条上下文", memory_type="short_term")
        await system.remember("第二条上下文", memory_type="short_term")
        await system.remember("第三条上下文", memory_type="short_term")

        context = await system.get_recent_context(n=2)
        assert len(context) == 2
        assert context[0] == "第三条上下文"
        assert context[1] == "第二条上下文"

        # 获取更多
        context_all = await system.get_recent_context(n=10)
        assert len(context_all) == 3

        await system.close()


class TestConcurrentOperations:
    """test_concurrent_operations: 并发存储和检索（使用 asyncio.gather）"""

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, mock_embedding):
        system = _create_system(mock_embedding)

        # 并发存储
        tasks = []
        for i in range(10):
            tasks.append(
                system.remember(
                    f"并发记忆{i}",
                    memory_type="semantic",
                    importance="medium",
                )
            )
        ids = await asyncio.gather(*tasks)
        assert len(ids) == 10
        assert len(set(ids)) == 10  # 全部唯一

        # 并发检索
        recall_tasks = []
        for i in range(5):
            recall_tasks.append(system.recall(f"并发记忆{i}"))
        results = await asyncio.gather(*recall_tasks)
        assert len(results) == 5

        # 验证统计
        stats = await system.get_stats()
        assert stats.total_added >= 10

        await system.close()

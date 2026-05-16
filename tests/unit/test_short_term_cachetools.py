"""
测试用 cachetools 重写后的 ShortTermMemory
"""

import time
import pytest

from memory_system.working_memory_cache import ShortTermMemory
from memory_system.core_abstractions.memory_data_models import ImportanceLevel


class TestShortTermMemoryCachetools:
    """ShortTermMemory (cachetools 版本) 单元测试"""

    @pytest.mark.asyncio
    async def test_add_and_retrieve(self):
        """添加后能获取"""
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        item = await stm.add("hello world", importance=ImportanceLevel.HIGH)
        assert item.content == "hello world"
        assert item.importance == ImportanceLevel.HIGH

        recent = await stm.get_recent(n=1)
        assert len(recent) == 1
        assert recent[0].content == "hello world"

    @pytest.mark.asyncio
    async def test_capacity_limit(self):
        """超过容量时保留最新的"""
        stm = ShortTermMemory(capacity=3, ttl_seconds=60)

        await stm.add("first")
        await stm.add("second")
        await stm.add("third")
        # 超过容量
        await stm.add("fourth")

        all_items = await stm.get_all()
        contents = [item.content for item in all_items]
        # "first" 应该被淘汰（TTLCache 的 maxsize=3，添加第四个时淘汰最早的）
        assert "first" not in contents
        assert "fourth" in contents
        assert len(contents) == 3

    @pytest.mark.asyncio
    async def test_ttl_expiration(self):
        """验证 TTLCache 的 TTL 机制（基于实际时间过期）"""
        stm = ShortTermMemory(capacity=5, ttl_seconds=1)  # 1秒 TTL

        await stm.add("will_expire")
        await stm.add("still_valid")

        # 立即获取，两条都应该存在
        all_items = await stm.get_all()
        assert len(all_items) == 2
        contents = [item.content for item in all_items]
        assert "will_expire" in contents
        assert "still_valid" in contents

        # 等待 TTL 过期（1.5秒）
        time.sleep(1.5)

        # 过期后，缓存应该为空（TTLCache 自动清理）
        remaining = await stm.get_all()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_clear(self):
        """清空后为空"""
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        await stm.add("item1")
        await stm.add("item2")

        await stm.clear()
        assert await stm.get_all() == []
        assert await stm.is_full() is False

    @pytest.mark.asyncio
    async def test_is_full(self):
        """容量检查"""
        stm = ShortTermMemory(capacity=3, ttl_seconds=60)

        assert await stm.is_full() is False

        await stm.add("a")
        assert await stm.is_full() is False

        await stm.add("b")
        await stm.add("c")
        assert await stm.is_full() is True

    @pytest.mark.asyncio
    async def test_get_recent_returns_latest(self):
        """get_recent 返回最新的 n 条"""
        stm = ShortTermMemory(capacity=10, ttl_seconds=60)

        for i in range(5):
            await stm.add(f"item_{i}")

        # 获取最近 3 条
        recent = await stm.get_recent(n=3)
        assert len(recent) == 3
        assert recent[0].content == "item_4"
        assert recent[1].content == "item_3"
        assert recent[2].content == "item_2"

    @pytest.mark.asyncio
    async def test_get_all(self):
        """获取全部"""
        stm = ShortTermMemory(capacity=10, ttl_seconds=60)

        await stm.add("alpha")
        await stm.add("beta")
        await stm.add("gamma")

        all_items = await stm.get_all()
        assert len(all_items) == 3
        contents = [item.content for item in all_items]
        assert contents == ["alpha", "beta", "gamma"]

    @pytest.mark.asyncio
    async def test_metadata_preserved(self):
        """元数据正确保存"""
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)

        metadata = {"source": "test", "tags": ["unit", "cache"]}
        item = await stm.add("with metadata", importance=ImportanceLevel.LOW, metadata=metadata)

        assert item.metadata == metadata
        assert item.metadata["source"] == "test"
        assert item.metadata["tags"] == ["unit", "cache"]

        # 通过 get_recent 获取后元数据仍然保留
        recent = await stm.get_recent(n=1)
        assert recent[0].metadata == metadata

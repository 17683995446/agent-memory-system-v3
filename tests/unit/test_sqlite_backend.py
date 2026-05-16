"""
SQLite 短期记忆后端单元测试
"""

import sys
import os
import time

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
)
from memory_system.working_memory_backends.memory_backend_sqlite import SQLiteShortTermMemoryBackend


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def sqlite_backend(tmp_path):
    """创建 SQLiteShortTermMemoryBackend 实例，使用临时数据库"""
    db_path = str(tmp_path / "test_short_term.db")
    backend = SQLiteShortTermMemoryBackend(
        capacity=5,
        ttl_seconds=300,
        db_path=db_path,
    )
    yield backend
    # 清理
    await backend.clear()


# ---------------------------------------------------------------------------
# test_add_and_get
# ---------------------------------------------------------------------------


class TestAddAndGet:
    """添加后获取，验证内容一致"""

    @pytest.mark.asyncio
    async def test_add_and_get(self, sqlite_backend):
        item = MemoryItem(
            content="SQLite添加获取测试",
            memory_type=MemoryType.SHORT_TERM,
            importance=ImportanceLevel.HIGH,
            metadata={"source": "test"},
        )
        memory_id = await sqlite_backend.add(item)
        assert memory_id == item.memory_id

        # 通过 get_recent 获取
        recent = await sqlite_backend.get_recent(1)
        assert len(recent) == 1
        retrieved = recent[0]
        assert retrieved.content == "SQLite添加获取测试"
        assert retrieved.memory_type == MemoryType.SHORT_TERM
        assert retrieved.importance == ImportanceLevel.HIGH
        assert retrieved.metadata == {"source": "test"}

    @pytest.mark.asyncio
    async def test_add_multiple_and_get_all(self, sqlite_backend):
        """添加多条后通过 get_all 获取"""
        items = [
            MemoryItem(content=f"记忆{i}", memory_type=MemoryType.SHORT_TERM)
            for i in range(3)
        ]
        for item in items:
            await sqlite_backend.add(item)

        all_items = await sqlite_backend.get_all()
        assert len(all_items) == 3
        contents = [m.content for m in all_items]
        for i in range(3):
            assert f"记忆{i}" in contents


# ---------------------------------------------------------------------------
# test_capacity_enforcement
# ---------------------------------------------------------------------------


class TestCapacityEnforcement:
    """容量限制执行"""

    @pytest.mark.asyncio
    async def test_capacity_enforcement(self, sqlite_backend):
        """超过容量时自动淘汰最旧的"""
        # capacity=5, 添加7条
        for i in range(7):
            item = MemoryItem(
                content=f"容量测试记忆{i}",
                memory_type=MemoryType.SHORT_TERM,
            )
            await sqlite_backend.add(item)

        # 验证只保留最新的5条
        all_items = await sqlite_backend.get_all()
        assert len(all_items) == 5

        # 验证最旧的2条被淘汰
        contents = [m.content for m in all_items]
        assert "容量测试记忆0" not in contents
        assert "容量测试记忆1" not in contents
        assert "容量测试记忆2" in contents
        assert "容量测试记忆6" in contents

    @pytest.mark.asyncio
    async def test_exact_capacity(self, sqlite_backend):
        """恰好达到容量时不淘汰"""
        for i in range(5):
            item = MemoryItem(content=f"记忆{i}", memory_type=MemoryType.SHORT_TERM)
            await sqlite_backend.add(item)

        all_items = await sqlite_backend.get_all()
        assert len(all_items) == 5


# ---------------------------------------------------------------------------
# test_ttl_cleanup
# ---------------------------------------------------------------------------


class TestTTLCleanup:
    """TTL 过期清理"""

    @pytest.mark.asyncio
    async def test_ttl_cleanup(self, tmp_path):
        """过期记忆在访问时被清理"""
        # 使用非常短的 TTL
        db_path = str(tmp_path / "ttl_test.db")
        backend = SQLiteShortTermMemoryBackend(
            capacity=10,
            ttl_seconds=0.1,  # 100ms
            db_path=db_path,
        )

        item = MemoryItem(
            content="即将过期的记忆",
            memory_type=MemoryType.SHORT_TERM,
        )
        await backend.add(item)

        # 立即获取应该存在
        all_items = await backend.get_all()
        assert len(all_items) == 1

        # 等待过期
        time.sleep(0.2)

        # 过期后应该被清理
        all_items = await backend.get_all()
        assert len(all_items) == 0

        await backend.clear()

    @pytest.mark.asyncio
    async def test_ttl_expired_on_get_recent(self, tmp_path):
        """过期记忆在 get_recent 时被清理"""
        db_path = str(tmp_path / "ttl_recent.db")
        backend = SQLiteShortTermMemoryBackend(
            capacity=10,
            ttl_seconds=0.1,
            db_path=db_path,
        )

        await backend.add(
            MemoryItem(content="记忆1", memory_type=MemoryType.SHORT_TERM)
        )
        await backend.add(
            MemoryItem(content="记忆2", memory_type=MemoryType.SHORT_TERM)
        )

        time.sleep(0.2)

        recent = await backend.get_recent(5)
        assert len(recent) == 0

        await backend.clear()


# ---------------------------------------------------------------------------
# test_clear
# ---------------------------------------------------------------------------


class TestClear:
    """清空记忆"""

    @pytest.mark.asyncio
    async def test_clear(self, sqlite_backend):
        """清空后所有记忆被删除"""
        for i in range(3):
            item = MemoryItem(content=f"待清除{i}", memory_type=MemoryType.SHORT_TERM)
            await sqlite_backend.add(item)

        assert len(await sqlite_backend.get_all()) == 3

        await sqlite_backend.clear()

        assert len(await sqlite_backend.get_all()) == 0
        assert len(await sqlite_backend.get_recent(5)) == 0

    @pytest.mark.asyncio
    async def test_clear_empty(self, sqlite_backend):
        """清空空存储不报错"""
        await sqlite_backend.clear()
        assert len(await sqlite_backend.get_all()) == 0


# ---------------------------------------------------------------------------
# test_is_full
# ---------------------------------------------------------------------------


class TestIsFull:
    """检查是否已满"""

    @pytest.mark.asyncio
    async def test_is_full(self, sqlite_backend):
        """达到容量时 is_full 返回 True"""
        # capacity=5
        for i in range(4):
            item = MemoryItem(content=f"记忆{i}", memory_type=MemoryType.SHORT_TERM)
            await sqlite_backend.add(item)
            assert await sqlite_backend.is_full() is False

        # 第5条，达到容量
        await sqlite_backend.add(
            MemoryItem(content="记忆5", memory_type=MemoryType.SHORT_TERM)
        )
        assert await sqlite_backend.is_full() is True

    @pytest.mark.asyncio
    async def test_is_full_after_delete(self, sqlite_backend):
        """删除后不再满"""
        for i in range(5):
            item = MemoryItem(content=f"记忆{i}", memory_type=MemoryType.SHORT_TERM)
            await sqlite_backend.add(item)

        assert await sqlite_backend.is_full() is True

        await sqlite_backend.clear()
        assert await sqlite_backend.is_full() is False

    @pytest.mark.asyncio
    async def test_is_full_empty(self, sqlite_backend):
        """空存储不满"""
        assert await sqlite_backend.is_full() is False

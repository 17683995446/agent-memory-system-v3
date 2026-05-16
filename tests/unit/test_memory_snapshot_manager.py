"""
MemorySnapshotManager 单元测试
"""

import sys
import os
import json
import time

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.conftest import MockEmbeddingProvider, MemoryStorage
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
)
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.memory_snapshot_manager import (
    MemorySnapshotManager,
    MemorySnapshotMetadata,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def seeded_storage():
    """预填充数据的 MemoryStorage"""
    storage = MemoryStorage(max_memories=100)
    items = [
        MemoryItem(
            content="情景记忆：用户讨论了Python装饰器",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
            metadata={"source": "conversation"},
        ),
        MemoryItem(
            content="语义记忆：Python装饰器是一种高级语法特性",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.HIGH,
            metadata={"source": "knowledge_base"},
        ),
        MemoryItem(
            content="情景记忆：用户询问了异步编程的最佳实践",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.MEDIUM,
            metadata={"source": "conversation"},
        ),
    ]
    for item in items:
        await storage.add(item)
    return storage


@pytest_asyncio.fixture
async def seeded_short_term():
    """预填充数据的 ShortTermMemory"""
    stm = ShortTermMemory(capacity=10, ttl_seconds=60)
    await stm.add("当前任务：修复内存泄漏", ImportanceLevel.HIGH, {"source": "context"})
    await stm.add("用户偏好：使用中文回复", ImportanceLevel.MEDIUM, {"source": "preference"})
    return stm


@pytest.fixture
def snapshot_dir(tmp_path):
    """快照存储目录"""
    return str(tmp_path / "snapshots")


# ---------------------------------------------------------------------------
# test_create_snapshot
# ---------------------------------------------------------------------------


class TestCreateSnapshot:
    """创建快照并验证元数据"""

    @pytest.mark.asyncio
    async def test_create_snapshot(self, seeded_storage, seeded_short_term, snapshot_dir):
        manager = MemorySnapshotManager(seeded_storage, seeded_short_term, snapshot_dir=snapshot_dir)
        snapshot_id = await manager.create_snapshot("初始快照")

        # 验证返回的 snapshot_id 格式
        assert snapshot_id.startswith("snap_")

        # 验证快照文件已创建
        from pathlib import Path
        filepath = Path(snapshot_dir) / f"{snapshot_id}.json"
        assert filepath.exists()

        # 验证文件内容
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["snapshot_id"] == snapshot_id
        assert data["metadata"]["description"] == "初始快照"
        assert data["metadata"]["memory_count"] == 3
        assert data["metadata"]["short_term_count"] == 2
        assert data["metadata"]["version"] == "2.1.0"
        assert len(data["memories"]) == 3
        assert len(data["short_term_memories"]) == 2

    @pytest.mark.asyncio
    async def test_create_snapshot_empty(self, snapshot_dir):
        """空存储创建快照"""
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)
        snapshot_id = await manager.create_snapshot("空快照")

        assert snapshot_id.startswith("snap_")

        from pathlib import Path
        filepath = Path(snapshot_dir) / f"{snapshot_id}.json"
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["memory_count"] == 0
        assert data["metadata"]["short_term_count"] == 0
        assert len(data["memories"]) == 0
        assert len(data["short_term_memories"]) == 0


# ---------------------------------------------------------------------------
# test_create_snapshot_with_tags
# ---------------------------------------------------------------------------


class TestCreateSnapshotWithTags:
    """带标签创建快照"""

    @pytest.mark.asyncio
    async def test_create_snapshot_with_tags(self, seeded_storage, snapshot_dir):
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)
        tags = ["experiment", "baseline", "v1.0"]
        snapshot_id = await manager.create_snapshot(
            description="实验基线",
            tags=tags,
        )

        from pathlib import Path
        filepath = Path(snapshot_dir) / f"{snapshot_id}.json"
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["tags"] == ["experiment", "baseline", "v1.0"]
        assert data["metadata"]["description"] == "实验基线"

    @pytest.mark.asyncio
    async def test_create_snapshot_default_tags(self, seeded_storage, snapshot_dir):
        """不传 tags 时默认为空列表"""
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)
        snapshot_id = await manager.create_snapshot()

        from pathlib import Path
        filepath = Path(snapshot_dir) / f"{snapshot_id}.json"
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["metadata"]["tags"] == []


# ---------------------------------------------------------------------------
# test_restore_snapshot
# ---------------------------------------------------------------------------


class TestRestoreSnapshot:
    """恢复快照验证记忆数量"""

    @pytest.mark.asyncio
    async def test_restore_snapshot(self, seeded_storage, seeded_short_term, snapshot_dir):
        manager = MemorySnapshotManager(seeded_storage, seeded_short_term, snapshot_dir=snapshot_dir)
        snapshot_id = await manager.create_snapshot("备份")

        # 添加新记忆以改变存储状态
        new_item = MemoryItem(
            content="新增记忆：测试恢复功能",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.LOW,
        )
        await seeded_storage.add(new_item)
        count_before = await seeded_storage.count()
        assert count_before == 4  # 原始3条 + 新增1条

        # 恢复快照
        restored = await manager.restore_snapshot(snapshot_id)
        assert restored == 3

        # 验证存储中只有快照中的3条记忆
        count_after = await seeded_storage.count()
        assert count_after == 3


# ---------------------------------------------------------------------------
# test_restore_nonexistent_snapshot
# ---------------------------------------------------------------------------


class TestRestoreNonexistentSnapshot:
    """恢复不存在的快照抛出异常"""

    @pytest.mark.asyncio
    async def test_restore_nonexistent_snapshot(self, snapshot_dir):
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)

        with pytest.raises(FileNotFoundError, match="快照不存在"):
            await manager.restore_snapshot("snap_nonexistent_000000000000")


# ---------------------------------------------------------------------------
# test_list_snapshots
# ---------------------------------------------------------------------------


class TestListSnapshots:
    """列出快照"""

    @pytest.mark.asyncio
    async def test_list_snapshots(self, seeded_storage, snapshot_dir):
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)

        # 初始时没有快照
        snapshots = await manager.list_snapshots()
        assert len(snapshots) == 0

        # 创建两个快照（中间添加一条记忆以确保ID不同）
        id1 = await manager.create_snapshot("第一个快照")

        # 添加新记忆使第二个快照内容不同
        new_item = MemoryItem(
            content="中间新增的记忆",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.LOW,
        )
        await seeded_storage.add(new_item)

        id2 = await manager.create_snapshot("第二个快照")

        snapshots = await manager.list_snapshots()
        assert len(snapshots) == 2

        # 验证返回的是 MemorySnapshotMetadata 对象
        for meta in snapshots:
            assert isinstance(meta, MemorySnapshotMetadata)

        # 验证按时间倒序排列（最新的在前）
        assert snapshots[0].snapshot_id == id2
        assert snapshots[1].snapshot_id == id1

    @pytest.mark.asyncio
    async def test_list_snapshots_empty_dir(self, snapshot_dir):
        """空目录时返回空列表"""
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)

        snapshots = await manager.list_snapshots()
        assert snapshots == []


# ---------------------------------------------------------------------------
# test_delete_snapshot
# ---------------------------------------------------------------------------


class TestDeleteSnapshot:
    """删除快照"""

    @pytest.mark.asyncio
    async def test_delete_snapshot(self, seeded_storage, snapshot_dir):
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)

        snapshot_id = await manager.create_snapshot("待删除快照")

        # 验证快照存在
        from pathlib import Path
        filepath = Path(snapshot_dir) / f"{snapshot_id}.json"
        assert filepath.exists()

        # 删除快照
        result = await manager.delete_snapshot(snapshot_id)
        assert result is True
        assert not filepath.exists()

        # 列表中不再包含该快照
        snapshots = await manager.list_snapshots()
        assert len(snapshots) == 0

    @pytest.mark.asyncio
    async def test_delete_nonexistent_snapshot(self, snapshot_dir):
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)

        result = await manager.delete_snapshot("snap_nonexistent_000000000000")
        assert result is False


# ---------------------------------------------------------------------------
# test_compare_snapshots
# ---------------------------------------------------------------------------


class TestCompareSnapshots:
    """比较快照差异"""

    @pytest.mark.asyncio
    async def test_compare_snapshots(self, seeded_storage, snapshot_dir):
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)

        # 创建第一个快照
        id1 = await manager.create_snapshot("快照1")

        # 添加新记忆
        new_item = MemoryItem(
            content="新增记忆：用于比较测试",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.LOW,
        )
        await seeded_storage.add(new_item)

        # 创建第二个快照
        id2 = await manager.create_snapshot("快照2")

        # 比较两个快照
        diff = await manager.compare_snapshots(id1, id2)

        assert diff["snapshot_1"] == id1
        assert diff["snapshot_2"] == id2
        assert diff["count_1"] == 3
        assert diff["count_2"] == 4
        assert diff["added"] == 1   # 快照2比快照1多1条
        assert diff["removed"] == 0  # 快照2没有缺少快照1的记忆
        assert diff["common"] == 3   # 3条共同记忆

    @pytest.mark.asyncio
    async def test_compare_identical_snapshots(self, seeded_storage, snapshot_dir):
        """比较相同的快照"""
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)

        snapshot_id = await manager.create_snapshot("相同快照")

        diff = await manager.compare_snapshots(snapshot_id, snapshot_id)

        assert diff["added"] == 0
        assert diff["removed"] == 0
        assert diff["common"] == 3

    @pytest.mark.asyncio
    async def test_compare_nonexistent_snapshot(self, snapshot_dir):
        """比较不存在的快照抛出异常"""
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)

        with pytest.raises(ValueError, match="not found"):
            await manager.compare_snapshots("snap_fake1_000000000000", "snap_fake2_000000000000")


# ---------------------------------------------------------------------------
# test_export_import_snapshot
# ---------------------------------------------------------------------------


class TestExportImportSnapshot:
    """导出导入快照"""

    @pytest.mark.asyncio
    async def test_export_import_snapshot(self, seeded_storage, snapshot_dir, tmp_path):
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)

        snapshot_id = await manager.create_snapshot("导出测试")

        # 导出到临时目录
        export_path = str(tmp_path / "exports" / "my_snapshot.json")
        await manager.export_snapshot(snapshot_id, export_path)

        # 验证导出文件存在
        from pathlib import Path
        assert Path(export_path).exists()

        # 验证导出文件内容
        with open(export_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["metadata"]["snapshot_id"] == snapshot_id
        assert len(data["memories"]) == 3

        # 在新的管理器中导入
        new_snapshot_dir = str(tmp_path / "new_snapshots")
        new_manager = MemorySnapshotManager(
            MemoryStorage(max_memories=100),
            ShortTermMemory(capacity=5, ttl_seconds=60),
            snapshot_dir=new_snapshot_dir,
        )
        imported_id = await new_manager.import_snapshot(export_path)
        assert imported_id == snapshot_id

        # 验证导入后可以列出
        snapshots = await new_manager.list_snapshots()
        assert len(snapshots) == 1
        assert snapshots[0].snapshot_id == snapshot_id

    @pytest.mark.asyncio
    async def test_import_nonexistent_file(self, snapshot_dir):
        storage = MemoryStorage(max_memories=100)
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(storage, stm, snapshot_dir=snapshot_dir)

        with pytest.raises(FileNotFoundError, match="导入文件不存在"):
            await manager.import_snapshot("/nonexistent/path/snapshot.json")


# ---------------------------------------------------------------------------
# test_restore_preserves_memory_content
# ---------------------------------------------------------------------------


class TestRestorePreservesMemoryContent:
    """恢复后内容一致"""

    @pytest.mark.asyncio
    async def test_restore_preserves_memory_content(self, seeded_storage, snapshot_dir):
        stm = ShortTermMemory(capacity=5, ttl_seconds=60)
        manager = MemorySnapshotManager(seeded_storage, stm, snapshot_dir=snapshot_dir)

        # 记录原始记忆内容
        original_memories = await seeded_storage.search_by_time_range(0, time.time())
        original_contents = sorted(m.content for m in original_memories)
        original_types = {m.memory_id: m.memory_type for m in original_memories}
        original_importance = {m.memory_id: m.importance for m in original_memories}

        # 创建快照
        snapshot_id = await manager.create_snapshot("内容一致性测试")

        # 清空存储并添加不同的记忆
        existing = await seeded_storage.search_by_time_range(0, time.time())
        for item in existing:
            await seeded_storage.delete(item.memory_id)

        different_item = MemoryItem(
            content="完全不同的记忆内容",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.LOW,
        )
        await seeded_storage.add(different_item)

        # 恢复快照
        restored = await manager.restore_snapshot(snapshot_id)
        assert restored == 3

        # 验证恢复后的内容与原始一致
        restored_memories = await seeded_storage.search_by_time_range(0, time.time())
        restored_contents = sorted(m.content for m in restored_memories)

        assert restored_contents == original_contents

        # 验证 memory_type 和 importance 也一致
        for m in restored_memories:
            assert m.memory_type == original_types[m.memory_id]
            assert m.importance == original_importance[m.memory_id]

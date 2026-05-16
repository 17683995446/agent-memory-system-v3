"""
MemoryExporter / MemoryImporter 单元测试
"""

import sys
import os
import json
import time

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.conftest import MockEmbeddingProvider, MemoryStorage
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.memory_io_operations.memory_exporter_importer import MemoryExporter, MemoryImporter


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


# ---------------------------------------------------------------------------
# MemoryExporter 测试
# ---------------------------------------------------------------------------


class TestExportAll:
    """export_all 返回字典列表"""

    @pytest.mark.asyncio
    async def test_export_all_returns_list_of_dicts(self, seeded_storage):
        exporter = MemoryExporter(seeded_storage)
        result = await exporter.export_all()
        assert isinstance(result, list)
        assert len(result) == 3
        for item in result:
            assert isinstance(item, dict)
            assert "content" in item
            assert "memory_type" in item


class TestExportAllWithFilter:
    """export_all 支持按 memory_type 过滤"""

    @pytest.mark.asyncio
    async def test_export_all_with_memory_type_filter(self, seeded_storage):
        exporter = MemoryExporter(seeded_storage)
        result = await exporter.export_all(memory_type="episodic")
        assert len(result) == 2
        for item in result:
            assert item["memory_type"] == "episodic"

    @pytest.mark.asyncio
    async def test_export_all_filter_semantic(self, seeded_storage):
        exporter = MemoryExporter(seeded_storage)
        result = await exporter.export_all(memory_type="semantic")
        assert len(result) == 1
        assert result[0]["memory_type"] == "semantic"


class TestExportToFile:
    """export_to_file 创建有效的 JSON 文件"""

    @pytest.mark.asyncio
    async def test_export_to_file(self, seeded_storage, tmp_path):
        filepath = str(tmp_path / "export.json")
        exporter = MemoryExporter(seeded_storage)
        count = await exporter.export_to_file(filepath)
        assert count == 3

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert "version" in data
        assert "exported_at" in data
        assert "count" in data
        assert "memories" in data
        assert data["count"] == 3
        assert len(data["memories"]) == 3


class TestExportToString:
    """export_to_string 返回有效的 JSON 字符串"""

    @pytest.mark.asyncio
    async def test_export_to_string(self, seeded_storage):
        exporter = MemoryExporter(seeded_storage)
        json_str = await exporter.export_to_string()
        data = json.loads(json_str)
        assert "version" in data
        assert "memories" in data
        assert data["count"] == 3


# ---------------------------------------------------------------------------
# MemoryImporter 测试
# ---------------------------------------------------------------------------


class TestImportFromList:
    """import_from_list 从字典列表导入"""

    @pytest.mark.asyncio
    async def test_import_from_list_valid(self):
        storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(storage)

        memories_data = [
            {
                "content": "导入的语义记忆",
                "memory_type": "semantic",
                "importance": 4,
                "metadata": {"source": "import"},
            },
            {
                "content": "导入的情景记忆",
                "memory_type": "episodic",
                "importance": 3,
            },
        ]
        imported, skipped = await importer.import_from_list(memories_data)
        assert imported == 2
        assert skipped == 0

        # 验证存储中确实有数据
        count = await storage.count()
        assert count == 2


class TestImportFromListInvalid:
    """import_from_list 跳过无效数据"""

    @pytest.mark.asyncio
    async def test_import_from_list_invalid(self):
        storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(storage)

        memories_data = [
            {
                "content": "有效记忆",
                "memory_type": "semantic",
                "importance": 4,
            },
            {
                # 缺少 content，应被跳过
                "memory_type": "semantic",
            },
            {
                # importance 值无效（超出枚举范围），应被跳过
                "content": "无效重要性",
                "memory_type": "semantic",
                "importance": 999,
            },
        ]
        imported, skipped = await importer.import_from_list(memories_data)
        assert imported == 1
        assert skipped == 2


class TestImportFromFileWrapped:
    """import_from_file 支持包裹格式 {"memories": [...]}"""

    @pytest.mark.asyncio
    async def test_import_from_file_wrapped(self, tmp_path):
        filepath = str(tmp_path / "import_wrapped.json")
        export_data = {
            "version": "2.1.0",
            "memories": [
                {
                    "content": "包裹格式记忆1",
                    "memory_type": "semantic",
                    "importance": 4,
                },
                {
                    "content": "包裹格式记忆2",
                    "memory_type": "episodic",
                    "importance": 3,
                },
            ],
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(export_data, f)

        storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(storage)
        imported, skipped = await importer.import_from_file(filepath)
        assert imported == 2
        assert skipped == 0


class TestImportFromFilePlainList:
    """import_from_file 支持纯列表格式"""

    @pytest.mark.asyncio
    async def test_import_from_file_plain_list(self, tmp_path):
        filepath = str(tmp_path / "import_list.json")
        memories_data = [
            {
                "content": "纯列表格式记忆",
                "memory_type": "semantic",
                "importance": 4,
            },
        ]
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(memories_data, f)

        storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(storage)
        imported, skipped = await importer.import_from_file(filepath)
        assert imported == 1
        assert skipped == 0


class TestImportFromFileNotFound:
    """import_from_file 文件不存在时抛出 FileNotFoundError"""

    @pytest.mark.asyncio
    async def test_import_from_file_not_found(self):
        storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(storage)
        with pytest.raises(FileNotFoundError):
            await importer.import_from_file("/nonexistent/path/import.json")


class TestImportFromFileInvalidFormat:
    """import_from_file 无效格式时抛出 ValueError"""

    @pytest.mark.asyncio
    async def test_import_from_file_invalid_format(self, tmp_path):
        filepath = str(tmp_path / "invalid.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump({"not_memories": "bad format"}, f)

        storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(storage)
        with pytest.raises(ValueError):
            await importer.import_from_file(filepath)


class TestImportFromString:
    """import_from_string 从 JSON 字符串导入"""

    @pytest.mark.asyncio
    async def test_import_from_string(self):
        storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(storage)

        json_str = json.dumps([
            {
                "content": "字符串导入记忆",
                "memory_type": "semantic",
                "importance": 4,
            }
        ])
        imported, skipped = await importer.import_from_string(json_str)
        assert imported == 1
        assert skipped == 0


class TestRoundtrip:
    """导出后导入，数据保持一致"""

    @pytest.mark.asyncio
    async def test_roundtrip(self, seeded_storage):
        # 导出
        exporter = MemoryExporter(seeded_storage)
        exported = await exporter.export_all()

        # 导入到新的存储
        new_storage = MemoryStorage(max_memories=100)
        importer = MemoryImporter(new_storage)
        imported, skipped = await importer.import_from_list(exported)
        assert imported == 3
        assert skipped == 0

        # 验证内容一致
        re_exported = await MemoryExporter(new_storage).export_all()
        assert len(re_exported) == 3

        # 按内容排序后对比
        exported_contents = sorted(m["content"] for m in exported)
        re_exported_contents = sorted(m["content"] for m in re_exported)
        assert exported_contents == re_exported_contents

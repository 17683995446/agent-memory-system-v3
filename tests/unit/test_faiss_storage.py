"""
FAISS 存储后端单元测试 (Mock)
使用 Mock 替代 faiss-cpu 依赖
"""
import pytest
import numpy as np
from unittest.mock import MagicMock, patch, PropertyMock
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel


def _create_mock_faiss():
    """创建模拟的 faiss 模块"""
    mock_faiss = MagicMock()

    # Mock IndexFlatIP
    mock_index = MagicMock()
    mock_index.ntotal = 0
    mock_faiss.IndexFlatIP = MagicMock(return_value=mock_index)

    # Mock IndexIDMap
    mock_id_map = MagicMock()
    mock_id_map.ntotal = 0
    mock_faiss.IndexIDMap = MagicMock(return_value=mock_id_map)

    # Mock normalize_L2 - modify in place
    def normalize_fn(vec):
        pass
    mock_faiss.normalize_L2 = MagicMock(side_effect=normalize_fn)

    # Mock write_index / read_index
    mock_faiss.write_index = MagicMock()
    mock_faiss.read_index = MagicMock(return_value=mock_id_map)

    return mock_faiss, mock_index, mock_id_map


class TestFAISSStorage:
    """FAISS 存储后端测试"""

    def test_init_with_dimension(self):
        """测试初始化"""
        mock_faiss, _, _ = _create_mock_faiss()
        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=128, max_memories=1000)
            assert storage.dimension == 128
            assert storage.max_memories == 1000

    def test_init_import_error(self):
        """测试未安装 faiss 时抛出 ImportError"""
        with patch.dict("sys.modules", {"faiss": None}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            with pytest.raises((ImportError, TypeError, AttributeError)):
                faiss_ann_storage_backend.FAISSStorage()

    @pytest.mark.asyncio
    async def test_add_and_get(self):
        """测试添加和获取"""
        mock_faiss, mock_id_map, _ = _create_mock_faiss()
        mock_id_map.ntotal = 1

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            item = MemoryItem(
                content="test",
                memory_type=MemoryType.SEMANTIC,
                embedding=[0.1] * 64,
            )
            mem_id = await storage.add(item)
            assert mem_id == item.memory_id

            retrieved = await storage.get(mem_id)
            assert retrieved is not None
            assert retrieved.content == "test"

    @pytest.mark.asyncio
    async def test_add_without_embedding(self):
        """测试无嵌入的记忆也能存储"""
        mock_faiss, _, _ = _create_mock_faiss()

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            item = MemoryItem(content="no embedding", memory_type=MemoryType.SHORT_TERM)
            mem_id = await storage.add(item)

            retrieved = await storage.get(mem_id)
            assert retrieved is not None

    @pytest.mark.asyncio
    async def test_count(self):
        """测试计数"""
        mock_faiss, _, _ = _create_mock_faiss()

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            assert await storage.count() == 0

            item = MemoryItem(content="test", embedding=[0.1] * 64)
            await storage.add(item)
            assert await storage.count() == 1

    @pytest.mark.asyncio
    async def test_delete(self):
        """测试删除"""
        mock_faiss, _, _ = _create_mock_faiss()

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            item = MemoryItem(content="test", embedding=[0.1] * 64)
            await storage.add(item)

            result = await storage.delete(item.memory_id)
            assert result is True

            retrieved = await storage.get(item.memory_id)
            assert retrieved is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self):
        """测试删除不存在的记忆"""
        mock_faiss, _, _ = _create_mock_faiss()

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            result = await storage.delete("nonexistent")
            assert result is False

    @pytest.mark.asyncio
    async def test_search_by_vector_empty(self):
        """测试空索引搜索"""
        mock_faiss, mock_id_map, _ = _create_mock_faiss()
        mock_id_map.ntotal = 0

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            results = await storage.search_by_vector([0.1] * 64, top_k=5)
            assert results == []

    @pytest.mark.asyncio
    async def test_search_by_time_range(self):
        """测试时间范围搜索"""
        mock_faiss, _, _ = _create_mock_faiss()

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            item = MemoryItem(content="test", embedding=[0.1] * 64)
            await storage.add(item)

            import time
            results = await storage.search_by_time_range(0, time.time() + 1)
            assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_close(self):
        """测试关闭"""
        mock_faiss, _, _ = _create_mock_faiss()

        with patch.dict("sys.modules", {"faiss": mock_faiss}):
            import importlib
            from memory_system.storage_backends import faiss_ann_storage_backend
            importlib.reload(faiss_ann_storage_backend)

            storage = faiss_ann_storage_backend.FAISSStorage(dimension=64)
            item = MemoryItem(content="test", embedding=[0.1] * 64)
            await storage.add(item)

            await storage.close()
            assert await storage.count() == 0

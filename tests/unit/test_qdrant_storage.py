"""
Qdrant 存储后端单元测试 (Mock)
使用 Mock 替代 qdrant-client 依赖
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel


def _create_mock_qdrant():
    """创建模拟的 qdrant_client 模块"""
    mock_qdrant = MagicMock()

    # Mock QdrantClient
    mock_client = MagicMock()
    mock_qdrant.QdrantClient = MagicMock(return_value=mock_client)

    # Mock models
    mock_qdrant.models.Distance = MagicMock()
    mock_qdrant.models.VectorParams = MagicMock()
    mock_qdrant.models.PointStruct = MagicMock()
    mock_qdrant.models.Filter = MagicMock()
    mock_qdrant.models.FieldCondition = MagicMock()
    mock_qdrant.models.MatchValue = MagicMock()
    mock_qdrant.models.Range = MagicMock()

    # Mock get_collection (raises exception = collection doesn't exist)
    mock_client.get_collection.side_effect = Exception("not found")

    # Mock count response
    mock_count_result = MagicMock()
    mock_count_result.count = 0
    mock_client.count.return_value = mock_count_result

    # Mock retrieve
    mock_client.retrieve.return_value = []

    # Mock search
    mock_client.search.return_value = []

    # Mock scroll
    mock_client.scroll.return_value = ([], None)

    return mock_qdrant, mock_client


def _get_qdrant_storage_class():
    """获取 QdrantStorage 类，如果不可用则返回 None"""
    try:
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        return QdrantStorage
    except (ImportError, AttributeError):
        return None


@pytest.mark.skipif(_get_qdrant_storage_class() is None, reason="qdrant-client not installed")
class TestQdrantStorage:
    """Qdrant 存储后端测试 (需要 qdrant-client)"""

    def test_init(self):
        """测试初始化"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(
            collection_name="test",
            host="localhost",
            port=6333,
            embedding_dimension=128
        )
        assert storage.collection_name == "test"
        assert storage.embedding_dimension == 128

    def test_init_with_url(self):
        """测试使用 URL 初始化"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(
            url="https://qdrant.example.com",
            api_key="test-key"
        )
        assert storage.collection_name == "agent_memory"

    @pytest.mark.asyncio
    async def test_add(self):
        """测试添加记忆"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(collection_name=f"test_{id(storage)}")
        item = MemoryItem(
            content="test",
            memory_type=MemoryType.SEMANTIC,
            embedding=[0.1] * 128,
        )
        mem_id = await storage.add(item)
        assert mem_id == item.memory_id

    @pytest.mark.asyncio
    async def test_get_not_found(self):
        """测试获取不存在的记忆"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(collection_name=f"test_{id(storage)}")
        result = await storage.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_count(self):
        """测试计数"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(collection_name=f"test_{id(storage)}")
        count = await storage.count()
        assert count >= 0

    @pytest.mark.asyncio
    async def test_delete(self):
        """测试删除"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(collection_name=f"test_{id(storage)}")
        result = await storage.delete("some_id")
        assert result is True

    @pytest.mark.asyncio
    async def test_search_by_vector(self):
        """测试向量搜索"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(collection_name=f"test_{id(storage)}")
        results = await storage.search_by_vector([0.1] * 128, top_k=5)
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_close(self):
        """测试关闭"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(collection_name=f"test_{id(storage)}")
        await storage.close()

    @pytest.mark.asyncio
    async def test_convert_to_memory_item(self):
        """测试 Qdrant 点转换为 MemoryItem"""
        from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
        storage = QdrantStorage(collection_name=f"test_{id(storage)}")

        # Create mock point
        mock_point = MagicMock()
        mock_point.id = "test_id"
        mock_point.vector = [0.1] * 128
        mock_point.payload = {
            "content": "test content",
            "memory_type": "semantic",
            "importance": 4,
            "timestamp": 1000000.0,
            "access_count": 5,
            "last_access": 1000001.0,
            "custom_key": "custom_value",
        }

        item = storage._convert_to_memory_item(mock_point)
        assert item.memory_id == "test_id"
        assert item.content == "test content"
        assert item.memory_type == MemoryType.SEMANTIC
        assert item.importance == ImportanceLevel.HIGH
        assert item.metadata.get("custom_key") == "custom_value"

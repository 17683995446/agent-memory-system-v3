"""
共享测试 fixtures
"""

import sys
import os
import hashlib
import math

import pytest

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    MemoryStats,
    RetrievalResponse,
)
from memory_system.core_abstractions.abstract_component_interfaces import EmbeddingProvider
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.working_memory_cache import ShortTermMemory


class MockEmbeddingProvider(EmbeddingProvider):
    """
    Mock 嵌入提供者
    - dimension=64
    - embed 返回基于文本 hash 的确定性向量
    - embed_batch 返回列表
    - similarity 返回余弦相似度
    """

    def __init__(self, dimension: int = 64):
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    async def embed(self, text: str) -> list:
        """基于文本 hash 生成确定性向量"""
        h = hashlib.md5(text.encode("utf-8")).digest()
        # 用 hash 字节循环填充到 dimension 长度
        vec = []
        for i in range(self._dimension):
            vec.append((h[i % len(h)] / 255.0) * 2 - 1)
        # 归一化
        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]
        return vec

    async def embed_batch(self, texts: list) -> list:
        """批量嵌入"""
        return [await self.embed(t) for t in texts]

    async def similarity(self, vec1: list, vec2: list) -> float:
        """计算余弦相似度"""
        dot = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = math.sqrt(sum(a * a for a in vec1))
        norm2 = math.sqrt(sum(b * b for b in vec2))
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return dot / (norm1 * norm2)


@pytest.fixture
def mock_embedding():
    """Mock EmbeddingProvider, dimension=64"""
    return MockEmbeddingProvider(dimension=64)


@pytest.fixture
def memory_storage():
    """MemoryStorage 实例, max_memories=100"""
    return MemoryStorage(max_memories=100)


@pytest.fixture
def short_term_memory():
    """ShortTermMemory 实例, capacity=5, ttl_seconds=60"""
    return ShortTermMemory(capacity=5, ttl_seconds=60)


@pytest.fixture
def sample_memories():
    """返回3个预定义的 MemoryItem（不同类型和重要性）"""
    return [
        MemoryItem(
            content="用户询问了Python的列表推导式用法",
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.MEDIUM,
            metadata={"source": "conversation"},
        ),
        MemoryItem(
            content="Python是一种通用编程语言，由Guido van Rossum创建",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.HIGH,
            metadata={"source": "knowledge_base"},
        ),
        MemoryItem(
            content="用户当前正在调试一个内存泄漏问题",
            memory_type=MemoryType.SHORT_TERM,
            importance=ImportanceLevel.LOW,
            metadata={"source": "context"},
        ),
    ]


@pytest.fixture
async def memory_system():
    """MemorySystem 实例，用于集成测试"""
    from memory_system import MemorySystem
    from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
    from memory_system.working_memory_cache import ShortTermMemory
    from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval

    embedding = MockEmbeddingProvider(dimension=64)
    storage = MemoryStorage(max_memories=10000)
    retrieval = HierarchicalRetrieval(strategy="adaptive")
    short_term = ShortTermMemory(capacity=100, ttl_seconds=300)

    memory = MemorySystem(
        storage=storage, embedding=embedding,
        retrieval=retrieval, short_term=short_term
    )

    yield memory

    # 清理
    await memory.close()

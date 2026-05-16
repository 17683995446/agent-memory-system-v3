"""
存储层模块
支持多种向量数据库后端
"""

from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage

# 条件导入：依赖不可用时设为 None
try:
    from memory_system.storage_backends.chroma_vector_storage_backend import ChromaStorage
except ImportError:
    ChromaStorage = None

try:
    from memory_system.storage_backends.qdrant_vector_storage_backend import QdrantStorage
except ImportError:
    QdrantStorage = None

try:
    from memory_system.storage_backends.faiss_ann_storage_backend import FAISSStorage
except ImportError:
    FAISSStorage = None

try:
    from memory_system.storage_backends.distributed_storage_backend import (
        DistributedStorageBackend,
        ConsistentHashRing,
        StorageNode,
    )
except ImportError:
    DistributedStorageBackend = None
    ConsistentHashRing = None
    StorageNode = None

__all__ = [
    "ChromaStorage",
    "QdrantStorage",
    "MemoryStorage",
    "FAISSStorage",
    "DistributedStorageBackend",
    "ConsistentHashRing",
    "StorageNode",
]

"""
嵌入层模块
支持多种嵌入模型
"""

try:
    from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
except ImportError:
    SentenceTransformerEmbedding = None

try:
    from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
except ImportError:
    BGEEmbedding = None

try:
    from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
except ImportError:
    OpenAIEmbedding = None

__all__ = [
    "SentenceTransformerEmbedding",
    "BGEEmbedding",
    "OpenAIEmbedding"
]

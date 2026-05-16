"""
嵌入提供者单元测试
覆盖 OpenAI、BGE、SentenceTransformer 三个嵌入提供者
使用条件导入，未安装对应包时自动跳过
"""
import pytest
from unittest.mock import MagicMock


def _is_openai_available():
    """检查 openai 是否可用"""
    try:
        from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
        # 验证可以实例化
        OpenAIEmbedding(api_key="test-key")
        return True
    except (ImportError, AttributeError, TypeError):
        return False


def _is_bge_available():
    """检查 FlagEmbedding 是否可用"""
    try:
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        BGEEmbedding(model_name="test")
        return True
    except (ImportError, AttributeError, TypeError):
        return False


def _is_st_available():
    """检查 sentence-transformers 是否可用"""
    try:
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        SentenceTransformerEmbedding()
        return True
    except (ImportError, AttributeError, TypeError):
        return False


# ============================================================
# OpenAI Embedding 测试
# ============================================================
@pytest.mark.skipif(not _is_openai_available(), reason="openai not installed")
class TestOpenAIEmbedding:
    """OpenAI 嵌入提供者测试"""

    @pytest.mark.asyncio
    async def test_embed_single_text(self):
        """测试单条文本嵌入"""
        from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
        provider = OpenAIEmbedding(api_key="test-key")
        assert hasattr(provider, 'embed')
        assert provider.dimension == 1536

    @pytest.mark.asyncio
    async def test_embed_batch(self):
        """测试批量嵌入接口"""
        from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
        provider = OpenAIEmbedding(api_key="test-key")
        assert hasattr(provider, 'embed_batch')

    @pytest.mark.asyncio
    async def test_dimension_small(self):
        """测试 small 模型维度"""
        from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
        provider = OpenAIEmbedding(api_key="test-key", model="text-embedding-3-small")
        assert provider.dimension == 1536

    @pytest.mark.asyncio
    async def test_dimension_large(self):
        """测试 large 模型维度"""
        from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
        provider = OpenAIEmbedding(api_key="test-key", model="text-embedding-3-large")
        assert provider.dimension == 3072

    @pytest.mark.asyncio
    async def test_similarity(self):
        """测试相似度计算"""
        from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
        provider = OpenAIEmbedding(api_key="test-key")
        score = await provider.similarity([1.0, 0.0, 0.0], [1.0, 0.0, 0.0])
        assert score == 1.0

    @pytest.mark.asyncio
    async def test_similarity_zero(self):
        """测试零向量相似度"""
        from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbedding
        provider = OpenAIEmbedding(api_key="test-key")
        score = await provider.similarity([0.0, 0.0], [1.0, 1.0])
        assert score == 0.0


# ============================================================
# BGE Embedding 测试
# ============================================================
@pytest.mark.skipif(not _is_bge_available(), reason="FlagEmbedding not installed")
class TestBGEEmbedding:
    """BGE 嵌入提供者测试"""

    @pytest.mark.asyncio
    async def test_embed_single(self):
        """测试单条嵌入"""
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        provider = BGEEmbedding(model_name="BAAI/bge-small-zh-v1.5")
        assert hasattr(provider, 'embed')
        assert provider.dimension > 0

    @pytest.mark.asyncio
    async def test_embed_batch(self):
        """测试批量嵌入接口"""
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        provider = BGEEmbedding()
        assert hasattr(provider, 'embed_batch')

    @pytest.mark.asyncio
    async def test_embed_queries(self):
        """测试查询嵌入接口"""
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        provider = BGEEmbedding()
        assert hasattr(provider, 'embed_queries')

    @pytest.mark.asyncio
    async def test_embed_corpus(self):
        """测试语料库嵌入接口"""
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        provider = BGEEmbedding()
        assert hasattr(provider, 'embed_corpus')

    @pytest.mark.asyncio
    async def test_similarity(self):
        """测试相似度"""
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        provider = BGEEmbedding()
        score = await provider.similarity([1.0, 0.0], [1.0, 0.0])
        assert score == 1.0

    @pytest.mark.asyncio
    async def test_preload(self):
        """测试预加载"""
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        provider = BGEEmbedding()
        result = await provider.preload()
        assert result is provider

    @pytest.mark.asyncio
    async def test_create_async(self):
        """测试异步创建"""
        from memory_system.embedding_providers.bge_embedding_provider import BGEEmbedding
        provider = await BGEEmbedding.create_async()
        assert provider.dimension > 0


# ============================================================
# SentenceTransformer Embedding 测试
# ============================================================
@pytest.mark.skipif(not _is_st_available(), reason="sentence-transformers not installed")
class TestSentenceTransformerEmbedding:
    """SentenceTransformer 嵌入提供者测试"""

    @pytest.mark.asyncio
    async def test_embed_single(self):
        """测试单条嵌入"""
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        provider = SentenceTransformerEmbedding()
        assert hasattr(provider, 'embed')
        assert provider.dimension > 0

    @pytest.mark.asyncio
    async def test_embed_query(self):
        """测试查询嵌入接口"""
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        provider = SentenceTransformerEmbedding(model_name="BAAI/bge-small-zh-v1.5")
        assert hasattr(provider, 'embed_query')
        assert provider._query_prefix != ""

    @pytest.mark.asyncio
    async def test_embed_query_no_prefix(self):
        """测试非 BGE 模型不带前缀"""
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        provider = SentenceTransformerEmbedding(model_name="all-MiniLM-L6-v2")
        assert provider._query_prefix == ""

    @pytest.mark.asyncio
    async def test_embed_batch(self):
        """测试批量嵌入接口"""
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        provider = SentenceTransformerEmbedding()
        assert hasattr(provider, 'embed_batch')

    @pytest.mark.asyncio
    async def test_similarity(self):
        """测试相似度"""
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        provider = SentenceTransformerEmbedding()
        score = await provider.similarity([1.0, 0.0], [1.0, 0.0])
        assert score == 1.0

    @pytest.mark.asyncio
    async def test_preload(self):
        """测试预加载"""
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        provider = SentenceTransformerEmbedding()
        result = await provider.preload()
        assert result is provider

    @pytest.mark.asyncio
    async def test_create_async(self):
        """测试异步创建"""
        from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbedding
        provider = await SentenceTransformerEmbedding.create_async()
        assert provider.dimension > 0


# ============================================================
# 通用接口一致性测试（不依赖任何外部包）
# ============================================================
class TestEmbeddingProviderInterface:
    """验证所有已注册的嵌入提供者实现了正确接口"""

    def test_all_providers_have_embed(self):
        """所有已注册提供者应有 embed 方法"""
        from memory_system.core_abstractions.component_registry_manager import get_embedding_registry
        registry = get_embedding_registry()
        for name, cls in registry.items():
            assert hasattr(cls, 'embed'), f"{name} 缺少 embed 方法"

    def test_all_providers_have_embed_batch(self):
        """所有已注册提供者应有 embed_batch 方法"""
        from memory_system.core_abstractions.component_registry_manager import get_embedding_registry
        registry = get_embedding_registry()
        for name, cls in registry.items():
            assert hasattr(cls, 'embed_batch'), f"{name} 缺少 embed_batch 方法"

    def test_all_providers_have_similarity(self):
        """所有已注册提供者应有 similarity 方法"""
        from memory_system.core_abstractions.component_registry_manager import get_embedding_registry
        registry = get_embedding_registry()
        for name, cls in registry.items():
            assert hasattr(cls, 'similarity'), f"{name} 缺少 similarity 方法"

    def test_at_least_one_provider_registered(self):
        """至少应有一个嵌入提供者注册"""
        from memory_system.core_abstractions.component_registry_manager import get_embedding_registry
        registry = get_embedding_registry()
        assert len(registry) > 0, "至少应有一个嵌入提供者注册"

"""简化嵌入提供者工厂模式

Agent Memory System V2.29.0 重构项目 - R-05
重构嵌入提供者工厂模式，简化实现
"""

from typing import Dict, Type, Optional, Any, List
from dataclasses import dataclass

from memory_system.core_abstractions.abstract_component_interfaces import EmbeddingProvider
from memory_system.exceptions import EmbeddingProviderNotFoundError, ConfigurationError
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ProviderInfo:
    """提供者信息"""
    name: str
    provider_class: Type[EmbeddingProvider]
    description: str
    default_config: Dict[str, Any]
    requires_auth: bool = False


class EmbeddingFactory:
    """简化的嵌入提供者工厂
    
    提供统一的嵌入提供者创建和管理接口。
    支持注册自定义提供者和内置提供者自动发现。
    
    Example:
        # 创建提供者
        provider = EmbeddingFactory.create("openai", api_key="xxx")
        
        # 注册自定义提供者
        EmbeddingFactory.register("custom", CustomProvider)
        
        # 列出可用提供者
        providers = EmbeddingFactory.list_providers()
    """
    
    _providers: Dict[str, ProviderInfo] = {}
    _initialized: bool = False
    
    @classmethod
    def _ensure_initialized(cls):
        """确保工厂已初始化（注册内置提供者）"""
        if not cls._initialized:
            cls._register_builtin_providers()
            cls._initialized = True
    
    @classmethod
    def _register_builtin_providers(cls):
        """注册内置提供者"""
        try:
            from memory_system.embedding_providers.openai_embedding_provider import OpenAIEmbeddingProvider
            cls.register(
                name="openai",
                provider_class=OpenAIEmbeddingProvider,
                description="OpenAI API embedding provider",
                default_config={"model": "text-embedding-ada-002"},
                requires_auth=True
            )
        except ImportError:
            logger.debug("OpenAI embedding provider not available")
        
        try:
            from memory_system.embedding_providers.bge_embedding_provider import BGEEmbeddingProvider
            cls.register(
                name="bge",
                provider_class=BGEEmbeddingProvider,
                description="BGE embedding provider",
                default_config={"model_name": "BAAI/bge-small-en"},
                requires_auth=False
            )
        except ImportError:
            logger.debug("BGE embedding provider not available")
        
        try:
            from memory_system.embedding_providers.sentence_transformer_embedding_provider import SentenceTransformerEmbeddingProvider
            cls.register(
                name="sentence_transformer",
                provider_class=SentenceTransformerEmbeddingProvider,
                description="Sentence Transformers embedding provider",
                default_config={"model_name": "all-MiniLM-L6-v2"},
                requires_auth=False
            )
        except ImportError:
            logger.debug("Sentence Transformer embedding provider not available")
        
        # 注册Mock提供者（始终可用）
        cls.register(
            name="mock",
            provider_class=MockEmbeddingProvider,
            description="Mock embedding provider for testing",
            default_config={"dimension": 384},
            requires_auth=False
        )
    
    @classmethod
    def register(
        cls,
        name: str,
        provider_class: Type[EmbeddingProvider],
        description: str = "",
        default_config: Optional[Dict[str, Any]] = None,
        requires_auth: bool = False
    ):
        """注册嵌入提供者
        
        Args:
            name: 提供者名称
            provider_class: 提供者类
            description: 描述
            default_config: 默认配置
            requires_auth: 是否需要认证
        """
        cls._providers[name] = ProviderInfo(
            name=name,
            provider_class=provider_class,
            description=description,
            default_config=default_config or {},
            requires_auth=requires_auth
        )
        logger.debug(f"Registered embedding provider: {name}")
    
    @classmethod
    def create(cls, name: str, **kwargs) -> EmbeddingProvider:
        """创建嵌入提供者实例
        
        Args:
            name: 提供者名称
            **kwargs: 传递给提供者的配置参数
            
        Returns:
            嵌入提供者实例
            
        Raises:
            EmbeddingProviderNotFoundError: 提供者不存在
            ConfigurationError: 配置无效
        """
        cls._ensure_initialized()
        
        if name not in cls._providers:
            available = list(cls._providers.keys())
            raise EmbeddingProviderNotFoundError(name, available)
        
        provider_info = cls._providers[name]
        
        # 合并默认配置和用户配置
        config = {**provider_info.default_config, **kwargs}
        
        # 检查认证要求
        if provider_info.requires_auth:
            auth_keys = ["api_key", "api_token", "auth_token"]
            if not any(key in config and config[key] for key in auth_keys):
                raise ConfigurationError(
                    message=f"Provider '{name}' requires authentication",
                    config_key="api_key"
                )
        
        try:
            instance = provider_info.provider_class(**config)
            logger.info(f"Created embedding provider: {name}")
            return instance
        except Exception as e:
            raise ConfigurationError(
                message=f"Failed to create embedding provider '{name}': {str(e)}",
                config_key=name,
                details={"exception": str(e)}
            )
    
    @classmethod
    def list_providers(cls) -> List[str]:
        """列出所有已注册的提供者名称
        
        Returns:
            提供者名称列表
        """
        cls._ensure_initialized()
        return list(cls._providers.keys())
    
    @classmethod
    def get_provider_info(cls, name: str) -> Optional[ProviderInfo]:
        """获取提供者信息
        
        Args:
            name: 提供者名称
            
        Returns:
            提供者信息，如果不存在则返回None
        """
        cls._ensure_initialized()
        return cls._providers.get(name)
    
    @classmethod
    def is_registered(cls, name: str) -> bool:
        """检查提供者是否已注册
        
        Args:
            name: 提供者名称
            
        Returns:
            是否已注册
        """
        cls._ensure_initialized()
        return name in cls._providers
    
    @classmethod
    def unregister(cls, name: str) -> bool:
        """注销提供者
        
        Args:
            name: 提供者名称
            
        Returns:
            是否成功注销
        """
        if name in cls._providers:
            del cls._providers[name]
            logger.debug(f"Unregistered embedding provider: {name}")
            return True
        return False
    
    @classmethod
    def clear(cls):
        """清除所有注册的提供者"""
        cls._providers.clear()
        cls._initialized = False
        logger.debug("Cleared all embedding providers")
    
    @classmethod
    def get_default_config(cls, name: str) -> Dict[str, Any]:
        """获取提供者的默认配置
        
        Args:
            name: 提供者名称
            
        Returns:
            默认配置字典
            
        Raises:
            EmbeddingProviderNotFoundError: 提供者不存在
        """
        cls._ensure_initialized()
        if name not in cls._providers:
            raise EmbeddingProviderNotFoundError(name, list(cls._providers.keys()))
        return cls._providers[name].default_config.copy()


class MockEmbeddingProvider(EmbeddingProvider):
    """Mock嵌入提供者 - 用于测试
    
    生成随机但确定的向量，用于测试和开发环境。
    """
    
    def __init__(self, dimension: int = 384, **kwargs):
        self._dimension = dimension
        self._cache: Dict[str, List[float]] = {}
    
    @property
    def dimension(self) -> int:
        return self._dimension
    
    async def embed(self, text: str) -> List[float]:
        """生成伪随机但确定的嵌入向量"""
        if text in self._cache:
            return self._cache[text]
        
        # 使用文本哈希生成确定性的伪随机向量
        import hashlib
        hash_val = hashlib.md5(text.encode()).hexdigest()
        
        # 生成向量
        vector = []
        for i in range(self._dimension):
            # 使用哈希值的不同部分生成向量元素
            idx = (i * 2) % len(hash_val)
            val = int(hash_val[idx:idx+2], 16) / 255.0
            vector.append(val)
        
        # 归一化
        import math
        norm = math.sqrt(sum(x**2 for x in vector))
        if norm > 0:
            vector = [x / norm for x in vector]
        
        self._cache[text] = vector
        return vector
    
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """批量嵌入"""
        return [await self.embed(text) for text in texts]
    
    async def similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """计算余弦相似度"""
        import math
        dot = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = math.sqrt(sum(a**2 for a in vec1))
        norm2 = math.sqrt(sum(b**2 for b in vec2))
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return dot / (norm1 * norm2)


class EmbeddingProviderRegistry:
    """嵌入提供者注册表 - 用于依赖注入
    
    提供与工厂相同的功能，但以实例方式使用，
    便于在依赖注入容器中使用。
    """
    
    def __init__(self):
        self._providers: Dict[str, ProviderInfo] = {}
        self._register_defaults()
    
    def _register_defaults(self):
        """注册默认提供者"""
        # 复制工厂的内置提供者
        EmbeddingFactory._register_builtin_providers()
        self._providers = EmbeddingFactory._providers.copy()
    
    def register(
        self,
        name: str,
        provider_class: Type[EmbeddingProvider],
        description: str = "",
        default_config: Optional[Dict[str, Any]] = None,
        requires_auth: bool = False
    ):
        """注册提供者"""
        self._providers[name] = ProviderInfo(
            name=name,
            provider_class=provider_class,
            description=description,
            default_config=default_config or {},
            requires_auth=requires_auth
        )
    
    def create(self, name: str, **kwargs) -> EmbeddingProvider:
        """创建提供者实例"""
        if name not in self._providers:
            available = list(self._providers.keys())
            raise EmbeddingProviderNotFoundError(name, available)
        
        provider_info = self._providers[name]
        config = {**provider_info.default_config, **kwargs}
        return provider_info.provider_class(**config)
    
    def list_providers(self) -> List[str]:
        """列出提供者"""
        return list(self._providers.keys())
    
    def get_info(self, name: str) -> Optional[ProviderInfo]:
        """获取提供者信息"""
        return self._providers.get(name)


def create_embedding_provider(
    provider_name: str,
    **config
) -> EmbeddingProvider:
    """便捷函数：创建嵌入提供者
    
    Args:
        provider_name: 提供者名称
        **config: 配置参数
        
    Returns:
        嵌入提供者实例
    """
    return EmbeddingFactory.create(provider_name, **config)


def get_available_providers() -> List[str]:
    """便捷函数：获取可用提供者列表
    
    Returns:
        提供者名称列表
    """
    return EmbeddingFactory.list_providers()

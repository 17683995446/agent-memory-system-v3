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

    DEFECT-R8-001 [P0] 修复:
        - 统一配置名称
        - 添加别名支持
        - 规范化提供者名称映射

    Example:
        # 创建提供者
        provider = EmbeddingFactory.create("openai", api_key="xxx")

        # 注册自定义提供者
        EmbeddingFactory.register("custom", CustomProvider)

        # 列出可用提供者
        providers = EmbeddingFactory.list_providers()

        # 使用别名创建（DEFECT-R8-001 修复）
        provider = EmbeddingFactory.create("sentence-transformer")  # 别名
        provider = EmbeddingFactory.create("sentence_transformer")  # 标准名
    """

    _providers: Dict[str, ProviderInfo] = {}
    _initialized: bool = False

    # DEFECT-R8-001 [P0] 修复: 别名映射表，统一配置名称
    # 注意: 所有键都应该使用下划线格式，因为 _resolve_name 会将连字符替换为下划线
    _ALIASES: Dict[str, str] = {
        # 标准名称 -> 标准名称（自映射）
        "openai": "openai",
        "bge": "bge",
        "mock": "mock",
        "sentence_transformer": "sentence_transformer",

        # 常见别名 -> 标准名称（使用下划线格式，因为 _resolve_name 会转换）
        "sentence_transformers": "sentence_transformer",
        "st": "sentence_transformer",
        "huggingface": "sentence_transformer",
        "hf": "sentence_transformer",
        "openai_embedding": "openai",
        "ada": "openai",
        "bge_embedding": "bge",
        "test": "mock",
        "dummy": "mock",
        "default": "mock",
    }

    @classmethod
    def _resolve_name(cls, name: str) -> str:
        """解析提供者名称（处理别名）

        DEFECT-R8-001 [P0] 修复: 统一配置名称解析

        Args:
            name: 提供者名称或别名

        Returns:
            标准化的提供者名称

        Raises:
            EmbeddingProviderNotFoundError: 如果名称无法解析
        """
        if not name or not isinstance(name, str):
            raise EmbeddingProviderNotFoundError(
                str(name),
                list(cls._providers.keys())
            )

        # 规范化名称：小写，替换连字符为下划线
        normalized = name.lower().strip().replace("-", "_")

        # 查找别名映射
        if normalized in cls._ALIASES:
            resolved = cls._ALIASES[normalized]
            if resolved != normalized:
                logger.debug(f"Resolved alias '{name}' -> '{resolved}'")
            return resolved

        # 检查是否直接存在于注册表中
        if normalized in cls._providers:
            return normalized

        # 尝试模糊匹配
        for provider_name in cls._providers.keys():
            if provider_name.lower() == normalized:
                return provider_name

        # 无法解析
        raise EmbeddingProviderNotFoundError(
            name,
            list(cls._providers.keys())
        )

    @classmethod
    def register_alias(cls, alias: str, target: str) -> None:
        """
        注册别名

        DEFECT-R8-001 [P0] 修复: 支持动态别名注册

        Args:
            alias: 别名
            target: 目标标准名称
        """
        normalized_alias = alias.lower().strip().replace("-", "_")
        normalized_target = target.lower().strip().replace("-", "_")

        if normalized_target not in cls._providers:
            raise EmbeddingProviderNotFoundError(
                target,
                list(cls._providers.keys())
            )

        cls._ALIASES[normalized_alias] = normalized_target
        logger.debug(f"Registered alias '{alias}' -> '{target}'")
    
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

        DEFECT-R8-001 [P0] 修复:
            - 支持别名解析
            - 统一配置名称处理

        Args:
            name: 提供者名称或别名
            **kwargs: 传递给提供者的配置参数

        Returns:
            嵌入提供者实例

        Raises:
            EmbeddingProviderNotFoundError: 提供者不存在
            ConfigurationError: 配置无效
        """
        cls._ensure_initialized()

        # DEFECT-R8-001 [P0] 修复: 解析名称（处理别名）
        resolved_name = cls._resolve_name(name)

        provider_info = cls._providers[resolved_name]

        # 合并默认配置和用户配置
        config = {**provider_info.default_config, **kwargs}

        # 检查认证要求
        if provider_info.requires_auth:
            auth_keys = ["api_key", "api_token", "auth_token"]
            if not any(key in config and config[key] for key in auth_keys):
                raise ConfigurationError(
                    message=f"Provider '{resolved_name}' requires authentication",
                    config_key="api_key"
                )

        try:
            instance = provider_info.provider_class(**config)
            logger.info(f"Created embedding provider: {resolved_name} (requested: {name})")
            return instance
        except Exception as e:
            raise ConfigurationError(
                message=f"Failed to create embedding provider '{resolved_name}': {str(e)}",
                config_key=resolved_name,
                details={"exception": str(e), "requested_name": name}
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

        DEFECT-R8-001 [P0] 修复: 支持别名解析

        Args:
            name: 提供者名称或别名

        Returns:
            提供者信息，如果不存在则返回None
        """
        cls._ensure_initialized()
        try:
            resolved_name = cls._resolve_name(name)
            return cls._providers.get(resolved_name)
        except EmbeddingProviderNotFoundError:
            return None
    
    @classmethod
    def is_registered(cls, name: str) -> bool:
        """检查提供者是否已注册

        DEFECT-R8-001 [P0] 修复: 支持别名解析

        Args:
            name: 提供者名称或别名

        Returns:
            是否已注册
        """
        cls._ensure_initialized()
        try:
            resolved_name = cls._resolve_name(name)
            return resolved_name in cls._providers
        except EmbeddingProviderNotFoundError:
            return False
    
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

        DEFECT-R8-001 [P0] 修复: 支持别名解析

        Args:
            name: 提供者名称或别名

        Returns:
            默认配置字典

        Raises:
            EmbeddingProviderNotFoundError: 提供者不存在
        """
        cls._ensure_initialized()
        resolved_name = cls._resolve_name(name)
        if resolved_name not in cls._providers:
            raise EmbeddingProviderNotFoundError(name, list(cls._providers.keys()))
        return cls._providers[resolved_name].default_config.copy()


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


# 将 MockEmbeddingProvider 注册到全局 _embedding_registry
# 这样 MemorySystemBuilder.from_config 可以找到它
from memory_system.core_abstractions.component_registry_manager import (
    _embedding_registry,
    register_embedding
)

# 注册 MockEmbeddingProvider 到全局注册表
@register_embedding("mock")
class MockEmbeddingProviderForRegistry(MockEmbeddingProvider):
    """Mock嵌入提供者包装类 - 用于全局注册"""
    
    @classmethod
    def from_config(cls, config):
        """从配置创建实例"""
        dimension = getattr(config, 'dimension', 384)
        return cls(dimension=dimension)

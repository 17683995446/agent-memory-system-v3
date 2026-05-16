"""
OpenAI API嵌入实现
云端服务，高质量
"""

from typing import List, Optional
import asyncio

from memory_system.core_abstractions.abstract_component_interfaces import EmbeddingProvider
from memory_system.core_abstractions.component_registry_manager import register_embedding

# tenacity 重试机制（可选依赖）
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# 如果 tenacity 可用，创建重试装饰器
if TENACITY_AVAILABLE:
    import openai as _openai_module
    _retry_on_api_error = retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(_openai_module.APIError if hasattr(_openai_module, 'APIError') else Exception),
        reraise=True
    )
else:
    def _retry_on_api_error(func):
        return func

# 条件注册：OpenAI 不可用时不注册
_OPENAI_AVAILABLE = False
try:
    import openai
    _OPENAI_AVAILABLE = True
except ImportError:
    _OPENAI_AVAILABLE = False


if _OPENAI_AVAILABLE:

    @register_embedding("openai")
    class OpenAIEmbedding(EmbeddingProvider):
        """
        OpenAI嵌入提供者
        
        特点:
            - 云端服务，无需本地部署
            - 高质量嵌入
            - 支持多语言
            - 按token计费
        
        可用模型:
            - text-embedding-3-small (1536维，性价比高)
            - text-embedding-3-large (3072维，高质量)
            - text-embedding-ada-002 (旧版，1536维)
        
        安装:
            pip install openai
        """
        
        def __init__(
            self,
            api_key: str,
            model: str = "text-embedding-3-small",
            base_url: Optional[str] = None
        ):
            """
            初始化OpenAI嵌入
            
            Args:
                api_key: OpenAI API密钥
                model: 模型名称
                base_url: 自定义API基础URL（用于代理或兼容服务）
            """
            try:
                import openai
            except ImportError:
                raise ImportError(
                    "OpenAI not installed. "
                    "Please install with: pip install openai"
                )
            
            self.api_key = api_key
            self.model = model
            
            # 创建客户端
            client_kwargs = {"api_key": api_key}
            if base_url:
                client_kwargs["base_url"] = base_url
            
            self.client = openai.OpenAI(**client_kwargs)
            
            # 维度映射
            self._dimension_map = {
                "text-embedding-3-small": 1536,
                "text-embedding-3-large": 3072,
                "text-embedding-ada-002": 1536
            }
            self._dimension = self._dimension_map.get(model, 1536)
        
        @property
        def dimension(self) -> int:
            """返回向量维度"""
            return self._dimension
        
        def _create_embeddings(self, **kwargs):
            """创建嵌入（带重试）"""
            if TENACITY_AVAILABLE:
                return _retry_on_api_error(self.client.embeddings.create)(**kwargs)
            return self.client.embeddings.create(**kwargs)
    
        async def embed(self, text: str) -> List[float]:
            """
            将文本转换为向量
    
            Args:
                text: 输入文本
    
            Returns:
                向量表示
            """
            try:
                response = await asyncio.to_thread(
                    self._create_embeddings,
                    model=self.model,
                    input=text
                )
                return response.data[0].embedding
            except Exception as e:
                import openai
                if isinstance(e, openai.APIError):
                    raise RuntimeError(f"OpenAI embedding API error: {e}") from e
                raise
    
        async def embed_batch(self, texts: List[str]) -> List[List[float]]:
            """批量嵌入"""
            try:
                response = await asyncio.to_thread(
                    self._create_embeddings,
                    model=self.model,
                    input=texts
                )
    
                # 按索引排序结果
                embeddings = [None] * len(texts)
                for item in response.data:
                    embeddings[item.index] = item.embedding
    
                return embeddings
            except Exception as e:
                import openai
                if isinstance(e, openai.APIError):
                    raise RuntimeError(f"OpenAI embedding API error: {e}") from e
                raise
        
        async def similarity(self, vec1: List[float], vec2: List[float]) -> float:
            """
            计算两个向量的余弦相似度
    
            OpenAI的嵌入已经归一化，直接使用余弦相似度
    
            Args:
                vec1: 向量1
                vec2: 向量2
    
            Returns:
                相似度分数
            """
            from memory_system.core_abstractions.mathematical_utility_functions import cosine_similarity
            return cosine_similarity(vec1, vec2)
    
        @classmethod
        def from_config(cls, config):
            return cls(
                api_key=config.openai_api_key or "",
                model=config.openai_model
            )

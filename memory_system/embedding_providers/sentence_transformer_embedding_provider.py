"""
Sentence-Transformers嵌入实现
生态成熟，模型丰富
"""

from typing import List
import asyncio

from memory_system.core_abstractions.abstract_component_interfaces import EmbeddingProvider
from memory_system.core_abstractions.component_registry_manager import register_embedding


@register_embedding("sentence_transformers")
class SentenceTransformerEmbedding(EmbeddingProvider):
    """
    Sentence-Transformers嵌入提供者
    
    特点:
        - 生态成熟，社区活跃
        - 支持多种预训练模型
        - 易于微调和部署
        - 支持多语言
    
    推荐模型:
        - BAAI/bge-small-zh-v1.5 (中文，快速)
        - BAAI/bge-base-zh-v1.5 (中文，平衡)
        - sentence-transformers/all-MiniLM-L6-v2 (英文，快速)
        - sentence-transformers/all-mpnet-base-v2 (英文，高质量)
    
    安装:
        pip install sentence-transformers
    """
    
    def __init__(
        self,
        model_name: str = "BAAI/bge-small-zh-v1.5",
        device: str = "cpu",
        normalize_embeddings: bool = True
    ):
        """
        初始化
        
        Args:
            model_name: 模型名称
            device: 运行设备 (cpu/cuda)
            normalize_embeddings: 是否归一化向量
        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers not installed. "
                "Please install with: pip install sentence-transformers"
            )
        
        self.model_name = model_name
        self.device = device
        self.normalize_embeddings = normalize_embeddings

        # 对于BGE模型，设置查询前缀以优化检索效果
        if "bge" in model_name.lower():
            self._query_prefix = "Represent this sentence for searching relevant passages: "
        else:
            self._query_prefix = ""

        # 加载模型
        self.model = SentenceTransformer(model_name, device=device)
        self._dimension = self.model.get_sentence_embedding_dimension()
    
    @property
    def dimension(self) -> int:
        """返回向量维度"""
        return self._dimension
    
    async def embed(self, text: str) -> List[float]:
        """
        将文本转换为向量（用于文档编码，不添加查询前缀）

        Args:
            text: 输入文本

        Returns:
            向量表示
        """
        embedding = await asyncio.to_thread(
            self.model.encode,
            text,
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True
        )

        return embedding.tolist()

    async def embed_query(self, text: str) -> List[float]:
        """
        将查询文本转换为向量（添加查询前缀以优化检索效果）

        Args:
            text: 查询文本

        Returns:
            向量表示
        """
        if self._query_prefix:
            text = f"{self._query_prefix}{text}"

        embedding = await asyncio.to_thread(
            self.model.encode,
            text,
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True
        )

        return embedding.tolist()

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """批量嵌入"""
        embeddings = await asyncio.to_thread(
            self.model.encode,
            texts,
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True,
            batch_size=32,
            show_progress_bar=False
        )

        return [emb.tolist() for emb in embeddings]
    
    async def similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        计算两个向量的余弦相似度

        Args:
            vec1: 向量1
            vec2: 向量2

        Returns:
            相似度分数 (-1 到 1)
        """
        from memory_system.core_abstractions.mathematical_utility_functions import cosine_similarity
        return cosine_similarity(vec1, vec2)

    async def preload(self) -> "SentenceTransformerEmbedding":
        """
        异步预加载模型
        
        在后台线程中加载模型，避免阻塞主线程
        
        Returns:
            self (支持链式调用)
        """
        # 模型已在 __init__ 中同步加载
        # 此方法主要用于 API 一致性和未来可能的异步加载
        return self

    @classmethod
    async def create_async(
        cls,
        model_name: str = "BAAI/bge-small-zh-v1.5",
        device: str = "cpu",
        normalize_embeddings: bool = True
    ) -> "SentenceTransformerEmbedding":
        """
        异步创建嵌入提供者
        
        在后台线程中加载模型
        
        Args:
            model_name: 模型名称
            device: 运行设备
            normalize_embeddings: 是否归一化
            
        Returns:
            SentenceTransformerEmbedding 实例
        """
        def _load():
            return cls(model_name, device, normalize_embeddings)
        
        return await asyncio.to_thread(_load)

    @classmethod
    def from_config(cls, config):
        return cls(
            model_name=config.model_name,
            device=config.device,
            normalize_embeddings=config.normalize_embeddings
        )

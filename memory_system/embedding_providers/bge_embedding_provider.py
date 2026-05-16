"""
BGE (BAAI General Embedding) 嵌入实现
智源研究院出品，MTEB榜单领先
"""

from typing import List
import asyncio

from memory_system.core_abstractions.abstract_component_interfaces import EmbeddingProvider
from memory_system.core_abstractions.component_registry_manager import register_embedding


@register_embedding("bge")
class BGEEmbedding(EmbeddingProvider):
    """
    BGE嵌入提供者
    
    特点:
        - 智源研究院出品，质量可靠
        - MTEB榜单领先性能
        - 支持多语言（中英）
        - 支持长文本（最长8192 token）
        - 支持混合检索（稠密+稀疏+多向量）
    
    推荐模型:
        - BAAI/bge-small-zh-v1.5 (中文，24M参数，512维)
        - BAAI/bge-base-zh-v1.5 (中文，110M参数，768维)
        - BAAI/bge-large-zh-v1.5 (中文，326M参数，1024维)
        - BAAI/bge-m3 (多语言，支持混合检索)
    
    安装:
        pip install FlagEmbedding
    """
    
    def __init__(
        self,
        model_name: str = "BAAI/bge-small-zh-v1.5",
        device: str = "cpu",
        normalize_embeddings: bool = True,
        use_fp16: bool = False
    ):
        """
        初始化BGE嵌入模型
        
        Args:
            model_name: 模型名称
            device: 运行设备
            normalize_embeddings: 是否归一化
            use_fp16: 是否使用半精度（加速推理）
        """
        try:
            from FlagEmbedding import FlagModel
        except ImportError:
            raise ImportError(
                "FlagEmbedding not installed. "
                "Please install with: pip install FlagEmbedding"
            )
        
        self.model_name = model_name
        self.device = device
        self.normalize_embeddings = normalize_embeddings
        
        # 加载模型
        self.model = FlagModel(
            model_name,
            query_instruction_for_retrieval="Represent this sentence for searching relevant passages:",
            use_fp16=use_fp16
        )
        
        # 获取维度
        self._dimension = self.model.model.config.hidden_size
    
    @property
    def dimension(self) -> int:
        """返回向量维度"""
        return self._dimension
    
    async def embed(self, text: str) -> List[float]:
        """
        将文本转换为向量

        Args:
            text: 输入文本

        Returns:
            向量表示
        """
        embedding = await asyncio.to_thread(
            self.model.encode,
            text,
            normalize_embeddings=self.normalize_embeddings
        )

        return embedding.tolist()

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """批量嵌入"""
        embeddings = await asyncio.to_thread(
            self.model.encode,
            texts,
            normalize_embeddings=self.normalize_embeddings,
            batch_size=32
        )

        return [emb.tolist() for emb in embeddings]
    
    async def similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        计算两个向量的余弦相似度

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
            model_name=config.model_name,
            device=config.device
        )
    
    async def embed_queries(self, queries: List[str]) -> List[List[float]]:
        """
        为检索任务编码查询（使用查询指令）

        Args:
            queries: 查询列表

        Returns:
            查询向量列表
        """
        embeddings = await asyncio.to_thread(self.model.encode_queries, queries)
        return [emb.tolist() for emb in embeddings]

    async def embed_corpus(self, passages: List[str]) -> List[List[float]]:
        """
        为语料库编码（不使用查询指令）

        Args:
            passages: 文档列表

        Returns:
            文档向量列表
        """
        embeddings = await asyncio.to_thread(self.model.encode_corpus, passages)
        return [emb.tolist() for emb in embeddings]

    async def preload(self) -> "BGEEmbedding":
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
        normalize_embeddings: bool = True,
        use_fp16: bool = False
    ) -> "BGEEmbedding":
        """
        异步创建嵌入提供者
        
        在后台线程中加载模型
        
        Args:
            model_name: 模型名称
            device: 运行设备
            normalize_embeddings: 是否归一化
            use_fp16: 是否使用半精度
            
        Returns:
            BGEEmbedding 实例
        """
        def _load():
            return cls(model_name, device, normalize_embeddings, use_fp16)
        
        return await asyncio.to_thread(_load)

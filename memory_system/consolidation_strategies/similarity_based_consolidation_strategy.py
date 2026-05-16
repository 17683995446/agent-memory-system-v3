"""
基于相似度的记忆整合策略
将语义相近的记忆聚类整合
"""

from typing import Any, Dict, List

from memory_system.core_abstractions.abstract_component_interfaces import ConsolidationStrategy, EmbeddingProvider
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.core_abstractions.component_registry_manager import register_consolidation
from memory_system.core_abstractions.mathematical_utility_functions import cosine_similarity
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@register_consolidation("similarity")
class SimilarityConsolidation(ConsolidationStrategy):
    """
    基于相似度的记忆整合

    策略:
        - 筛选重要性达到阈值的情景记忆
        - 使用余弦相似度对记忆进行聚类
        - 为每个聚类生成摘要语义记忆
    """

    def __init__(
        self,
        similarity_threshold: float = 0.85,
        min_cluster_size: int = 2,
        importance_threshold: ImportanceLevel = ImportanceLevel.MEDIUM
    ):
        """
        初始化

        Args:
            similarity_threshold: 最小余弦相似度阈值，达到该值才归入同一聚类
            min_cluster_size: 形成聚类所需的最小记忆数量
            importance_threshold: 重要性阈值，低于此值的记忆不参与整合
        """
        self.similarity_threshold = similarity_threshold
        self.min_cluster_size = min_cluster_size
        self.importance_threshold = importance_threshold

    async def should_consolidate(
        self,
        memories: List[MemoryItem],
        stats: Dict[str, Any]
    ) -> bool:
        """
        判断是否需要整合

        Args:
            memories: 记忆列表
            stats: 统计信息

        Returns:
            是否需要整合
        """
        episodic_count = sum(
            1 for m in memories
            if m.memory_type == MemoryType.EPISODIC
        )

        return episodic_count >= self.min_cluster_size * 2

    async def consolidate(
        self,
        memories: List[MemoryItem],
        embedding: EmbeddingProvider
    ) -> List[MemoryItem]:
        """
        执行整合

        Args:
            memories: 待整合的记忆
            embedding: 嵌入提供者

        Returns:
            新生成的语义记忆列表
        """
        # 筛选情景记忆中重要性达到阈值的
        to_consolidate = [
            m for m in memories
            if m.memory_type == MemoryType.EPISODIC
            and m.importance.value >= self.importance_threshold.value
        ]

        if len(to_consolidate) < self.min_cluster_size:
            return []

        # 过滤掉没有嵌入的记忆
        valid_memories = []
        for m in to_consolidate:
            if m.embedding is None or len(m.embedding) == 0:
                logger.warning(
                    "记忆 %s 没有嵌入向量，跳过整合",
                    m.memory_id
                )
            else:
                valid_memories.append(m)

        if len(valid_memories) < self.min_cluster_size:
            return []

        # 使用贪心算法按相似度聚类
        clusters = self._cluster_by_similarity(valid_memories)

        # 为每个满足最小聚类大小的簇生成摘要记忆
        new_memories = []
        for cluster in clusters:
            if len(cluster) >= self.min_cluster_size:
                summary, avg_sim = self._create_summary(cluster)

                # 生成嵌入
                summary_embedding = await embedding.embed(summary)

                # 创建新的语义记忆
                new_memory = MemoryItem(
                    content=summary,
                    memory_type=MemoryType.SEMANTIC,
                    importance=ImportanceLevel.HIGH,
                    embedding=summary_embedding,
                    metadata={
                        "consolidated_from": [m.memory_id for m in cluster],
                        "cluster_size": len(cluster),
                        "avg_similarity": round(avg_sim, 4)
                    }
                )
                new_memories.append(new_memory)

        return new_memories

    def _cluster_by_similarity(
        self,
        memories: List[MemoryItem]
    ) -> List[List[MemoryItem]]:
        """
        使用贪心算法按余弦相似度聚类

        遍历记忆列表，对每条记忆找到所有相似度 >= 阈值且尚未归入其他聚类的记忆，
        将它们归入同一聚类。

        Args:
            memories: 待聚类的记忆列表

        Returns:
            聚类列表，每个聚类是一个记忆列表
        """
        if not memories:
            return []

        clustered_indices: set = set()
        clusters: List[List[MemoryItem]] = []

        for i, memory in enumerate(memories):
            if i in clustered_indices:
                continue

            # 以当前记忆为种子，找到所有相似度达到阈值的未聚类记忆
            cluster = [memory]
            clustered_indices.add(i)

            for j, other in enumerate(memories):
                if j in clustered_indices:
                    continue

                sim = cosine_similarity(memory.embedding, other.embedding)
                if sim >= self.similarity_threshold:
                    cluster.append(other)
                    clustered_indices.add(j)

            clusters.append(cluster)

        return clusters

    def _create_summary(
        self,
        cluster: List[MemoryItem]
    ) -> tuple:
        """
        为聚类创建摘要

        Args:
            cluster: 聚类中的记忆列表

        Returns:
            (摘要文本, 平均相似度)
        """
        contents = [m.content for m in cluster]

        # 计算聚类内所有记忆对的平均相似度
        total_similarity = 0.0
        pair_count = 0
        for i in range(len(cluster)):
            for j in range(i + 1, len(cluster)):
                sim = cosine_similarity(
                    cluster[i].embedding,
                    cluster[j].embedding
                )
                total_similarity += sim
                pair_count += 1

        avg_similarity = total_similarity / pair_count if pair_count > 0 else 0.0

        summary = f"[相似度整合] 包含{len(contents)}条相关记忆: "
        summary += " | ".join(contents)

        return summary, avg_similarity

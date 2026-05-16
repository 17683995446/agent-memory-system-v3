"""
基于时间的记忆整合策略
将时间相近的记忆聚类整合
"""

from typing import Any, Dict, List
import time

from memory_system.core_abstractions.abstract_component_interfaces import ConsolidationStrategy, EmbeddingProvider
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel
from memory_system.core_abstractions.component_registry_manager import register_consolidation


@register_consolidation("time_based")
class TimeBasedConsolidation(ConsolidationStrategy):
    """
    基于时间的记忆整合
    
    策略:
        - 将时间窗口内的情景记忆聚类
        - 生成摘要记忆存入语义记忆
        - 删除原始情景记忆
    """
    
    def __init__(
        self,
        time_window_seconds: float = 3600,  # 1小时
        min_cluster_size: int = 3,
        importance_threshold: ImportanceLevel = ImportanceLevel.MEDIUM
    ):
        """
        初始化
        
        Args:
            time_window_seconds: 时间窗口大小
            min_cluster_size: 最小聚类大小
            importance_threshold: 重要性阈值
        """
        self.time_window_seconds = time_window_seconds
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
        # 检查情景记忆数量
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
        current_time = time.time()
        
        # 筛选需要整合的情景记忆
        to_consolidate = [
            m for m in memories
            if m.memory_type == MemoryType.EPISODIC
            and m.importance.value >= self.importance_threshold.value
            and (current_time - m.timestamp) > self.time_window_seconds
        ]
        
        if len(to_consolidate) < self.min_cluster_size:
            return []
        
        # 按时间排序
        to_consolidate.sort(key=lambda x: x.timestamp)
        
        # 聚类（简化版：按时间窗口分组）
        clusters = self._cluster_by_time(to_consolidate)
        
        # 为每个聚类生成摘要记忆
        new_memories = []
        for cluster in clusters:
            if len(cluster) >= self.min_cluster_size:
                summary = self._create_summary(cluster)
                
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
                        "time_range": (
                            min(m.timestamp for m in cluster),
                            max(m.timestamp for m in cluster)
                        )
                    }
                )
                new_memories.append(new_memory)
        
        return new_memories
    
    def _cluster_by_time(
        self,
        memories: List[MemoryItem]
    ) -> List[List[MemoryItem]]:
        """按时间窗口聚类"""
        if not memories:
            return []
        
        clusters = []
        current_cluster = [memories[0]]
        
        for memory in memories[1:]:
            # 检查时间差
            time_diff = memory.timestamp - current_cluster[-1].timestamp
            
            if time_diff <= self.time_window_seconds:
                current_cluster.append(memory)
            else:
                clusters.append(current_cluster)
                current_cluster = [memory]
        
        if current_cluster:
            clusters.append(current_cluster)
        
        return clusters
    
    def _create_summary(self, cluster: List[MemoryItem]) -> str:
        """为聚类创建摘要"""
        contents = [m.content for m in cluster]
        
        # 简化摘要：提取关键信息
        if len(contents) <= 3:
            summary = " | ".join(contents)
        else:
            summary = f"[整合记忆] 包含{len(contents)}条相关事件: "
            summary += " | ".join(contents[:3]) + "..."
        
        return summary

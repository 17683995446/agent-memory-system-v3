"""公共检索结果处理逻辑

Agent Memory System V2.29.0 重构项目 - R-06
提取公共的检索结果处理逻辑
"""

from typing import Dict, Any, List, Optional, Callable, Set, Tuple
from dataclasses import dataclass
import math

from memory_system.core_abstractions.memory_data_models import RetrievalResult, MemoryItem
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ProcessingConfig:
    """检索结果处理配置"""
    max_results: int = 10
    deduplicate: bool = True
    apply_diversity: bool = False
    diversity_factor: float = 0.5
    min_score_threshold: float = 0.0
    max_per_layer: Optional[int] = None


class RetrievalResultProcessor:
    """公共检索结果处理逻辑
    
    提供统一的检索结果处理方法，包括：
    - 结果合并与排序
    - 去重
    - 多样性重排序（MMR算法）
    - 分数归一化
    - 分层结果处理
    """
    
    def __init__(self, config: Optional[ProcessingConfig] = None):
        self.config = config or ProcessingConfig()
    
    def process(
        self,
        results: List[RetrievalResult],
        config: Optional[ProcessingConfig] = None
    ) -> List[Dict[str, Any]]:
        """处理检索结果
        
        执行完整的处理流程：过滤 -> 去重 -> 多样性 -> 排序 -> 格式化
        
        Args:
            results: 原始检索结果列表
            config: 处理配置（覆盖默认配置）
            
        Returns:
            处理后的结果列表
        """
        cfg = config or self.config
        
        if not results:
            return []
        
        # 1. 过滤低分结果
        filtered = self._filter_by_score(results, cfg.min_score_threshold)
        
        # 2. 去重
        if cfg.deduplicate:
            filtered = self._deduplicate(filtered)
        
        # 3. 应用多样性重排序
        if cfg.apply_diversity:
            filtered = self._apply_diversity(filtered, cfg.diversity_factor)
        
        # 4. 排序
        filtered = self._sort_by_score(filtered)
        
        # 5. 限制结果数量
        limited = filtered[:cfg.max_results]
        
        # 6. 格式化输出
        return self._format_results(limited)
    
    @staticmethod
    def merge_and_rank(
        results: List[RetrievalResult],
        max_results: int,
        deduplicate: bool = True
    ) -> List[Dict[str, Any]]:
        """合并并排序结果（静态方法，便捷使用）
        
        Args:
            results: 检索结果列表
            max_results: 最大结果数
            deduplicate: 是否去重
            
        Returns:
            处理后的结果字典列表
        """
        if deduplicate:
            seen: Set[str] = set()
            unique = []
            for r in results:
                if r.memory.memory_id not in seen:
                    seen.add(r.memory.memory_id)
                    unique.append(r)
            results = unique
        
        # 按分数降序排序
        results.sort(key=lambda x: x.score, reverse=True)
        
        return [
            {
                "memory": r.memory.model_dump(),
                "score": r.score,
                "layer": r.layer
            }
            for r in results[:max_results]
        ]
    
    @staticmethod
    def merge_multi_layer_results(
        layer_results: Dict[str, List[RetrievalResult]],
        max_results: int,
        layer_weights: Optional[Dict[str, float]] = None
    ) -> List[Dict[str, Any]]:
        """合并多层检索结果
        
        Args:
            layer_results: 各层结果字典 {layer_name: results}
            max_results: 最大结果数
            layer_weights: 层权重 {layer_name: weight}
            
        Returns:
            合并后的结果列表
        """
        all_results = []
        
        for layer_name, results in layer_results.items():
            weight = layer_weights.get(layer_name, 1.0) if layer_weights else 1.0
            for r in results:
                # 应用层权重
                weighted_result = RetrievalResult(
                    memory=r.memory,
                    score=r.score * weight,
                    layer=f"{r.layer}_weighted"
                )
                all_results.append(weighted_result)
        
        return RetrievalResultProcessor.merge_and_rank(all_results, max_results)
    
    def _filter_by_score(
        self,
        results: List[RetrievalResult],
        min_threshold: float
    ) -> List[RetrievalResult]:
        """按分数过滤结果"""
        if min_threshold <= 0:
            return results
        return [r for r in results if r.score >= min_threshold]
    
    def _deduplicate(self, results: List[RetrievalResult]) -> List[RetrievalResult]:
        """去重 - 保留分数最高的重复项"""
        seen: Dict[str, RetrievalResult] = {}
        
        for r in results:
            mem_id = r.memory.memory_id
            if mem_id not in seen or r.score > seen[mem_id].score:
                seen[mem_id] = r
        
        return list(seen.values())
    
    def _sort_by_score(self, results: List[RetrievalResult]) -> List[RetrievalResult]:
        """按分数降序排序"""
        return sorted(results, key=lambda x: x.score, reverse=True)
    
    def _format_results(self, results: List[RetrievalResult]) -> List[Dict[str, Any]]:
        """格式化结果为字典列表"""
        return [
            {
                "memory": r.memory.model_dump(),
                "score": r.score,
                "layer": r.layer
            }
            for r in results
        ]
    
    @staticmethod
    def _apply_diversity(
        results: List[RetrievalResult],
        diversity_factor: float = 0.5
    ) -> List[RetrievalResult]:
        """应用多样性重排序 (MMR算法简化版)
        
        MMR = lambda * Relevance - (1 - lambda) * max_sim(selected)
        
        Args:
            results: 检索结果列表
            diversity_factor: 多样性因子 (0-1)，越大越注重多样性
            
        Returns:
            重排序后的结果列表
        """
        if not results or diversity_factor <= 0:
            return results
        
        if len(results) <= 1:
            return results
        
        # 选择第一个（相关性最高）
        selected = [results[0]]
        remaining = results[1:]
        
        while remaining and len(selected) < len(results):
            max_mmr_score = -float('inf')
            max_mmr_idx = 0
            
            for i, candidate in enumerate(remaining):
                # 相关性分数
                relevance = candidate.score
                
                # 计算与已选结果的最大相似度
                max_sim = 0.0
                if candidate.memory.embedding:
                    for s in selected:
                        if s.memory.embedding:
                            sim = cosine_similarity(
                                candidate.memory.embedding,
                                s.memory.embedding
                            )
                            max_sim = max(max_sim, sim)
                
                # MMR分数
                mmr_score = (diversity_factor * relevance - 
                           (1 - diversity_factor) * max_sim)
                
                if mmr_score > max_mmr_score:
                    max_mmr_score = mmr_score
                    max_mmr_idx = i
            
            selected.append(remaining.pop(max_mmr_idx))
        
        return selected
    
    @staticmethod
    def normalize_scores(
        results: List[RetrievalResult],
        method: str = "minmax"
    ) -> List[RetrievalResult]:
        """归一化分数
        
        Args:
            results: 检索结果列表
            method: 归一化方法 ("minmax", "softmax", "zscore")
            
        Returns:
            分数归一化后的结果列表
        """
        if not results:
            return results
        
        scores = [r.score for r in results]
        
        if method == "minmax":
            min_score = min(scores)
            max_score = max(scores)
            if max_score == min_score:
                normalized = [0.5] * len(scores)
            else:
                normalized = [
                    (s - min_score) / (max_score - min_score)
                    for s in scores
                ]
        
        elif method == "softmax":
            import math
            exp_scores = [math.exp(s) for s in scores]
            sum_exp = sum(exp_scores)
            normalized = [s / sum_exp for s in exp_scores]
        
        elif method == "zscore":
            import statistics
            mean = statistics.mean(scores)
            std = statistics.stdev(scores) if len(scores) > 1 else 1
            if std == 0:
                normalized = [0.5] * len(scores)
            else:
                normalized = [(s - mean) / std for s in scores]
        
        else:
            raise ValueError(f"Unknown normalization method: {method}")
        
        # 创建新的结果对象
        normalized_results = []
        for r, norm_score in zip(results, normalized):
            normalized_results.append(RetrievalResult(
                memory=r.memory,
                score=norm_score,
                layer=r.layer
            ))
        
        return normalized_results
    
    @staticmethod
    def reciprocal_rank_fusion(
        result_lists: List[List[RetrievalResult]],
        k: int = 60
    ) -> List[RetrievalResult]:
        """倒数秩融合 (RRF)
        
        RRF公式: score = sum(1 / (k + rank))
        
        Args:
            result_lists: 多个结果列表
            k: RRF常数
            
        Returns:
            融合后的结果列表
        """
        from memory_system.core_abstractions.mathematical_utility_functions import (
            reciprocal_rank_fusion as rrf
        )
        
        # 构建输入格式
        rrf_inputs = []
        for results in result_lists:
            items = [(r.memory.memory_id, r.score) for r in results]
            rrf_inputs.append(items)
        
        # 应用RRF
        fused = rrf(rrf_inputs, k=k)
        
        # 重建结果列表
        # 需要从原始结果中查找完整信息
        memory_map = {}
        for results in result_lists:
            for r in results:
                memory_map[r.memory.memory_id] = r.memory
        
        fused_results = []
        for memory_id, score in fused:
            if memory_id in memory_map:
                fused_results.append(RetrievalResult(
                    memory=memory_map[memory_id],
                    score=score,
                    layer="rrf_fused"
                ))
        
        return fused_results


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """计算两个向量的余弦相似度
    
    Args:
        vec1: 向量1
        vec2: 向量2
        
    Returns:
        余弦相似度 (-1 到 1)
    """
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0
    
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return dot_product / (norm1 * norm2)


def euclidean_distance(vec1: List[float], vec2: List[float]) -> float:
    """计算两个向量的欧几里得距离
    
    Args:
        vec1: 向量1
        vec2: 向量2
        
    Returns:
        欧几里得距离
    """
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return float('inf')
    
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))


def jaccard_similarity(set1: Set, set2: Set) -> float:
    """计算Jaccard相似度
    
    Args:
        set1: 集合1
        set2: 集合2
        
    Returns:
        Jaccard相似度 (0 到 1)
    """
    if not set1 and not set2:
        return 1.0
    
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    
    if union == 0:
        return 0.0
    
    return intersection / union


class ResultAggregator:
    """结果聚合器 - 用于聚合多个检索结果"""
    
    def __init__(self):
        self._results: Dict[str, RetrievalResult] = {}
        self._sources: Dict[str, Set[str]] = {}
    
    def add(self, result: RetrievalResult, source: str):
        """添加结果
        
        Args:
            result: 检索结果
            source: 来源标识
        """
        mem_id = result.memory.memory_id
        
        if mem_id not in self._results:
            self._results[mem_id] = result
            self._sources[mem_id] = {source}
        else:
            # 保留更高分数
            if result.score > self._results[mem_id].score:
                self._results[mem_id] = result
            self._sources[mem_id].add(source)
    
    def add_batch(self, results: List[RetrievalResult], source: str):
        """批量添加结果"""
        for r in results:
            self.add(r, source)
    
    def get_results(
        self,
        min_sources: int = 1,
        sort_by: str = "score"
    ) -> List[Dict[str, Any]]:
        """获取聚合结果
        
        Args:
            min_sources: 最小来源数（用于过滤）
            sort_by: 排序方式 ("score", "sources")
            
        Returns:
            聚合后的结果列表
        """
        filtered = [
            (mem_id, self._results[mem_id], self._sources[mem_id])
            for mem_id in self._results
            if len(self._sources[mem_id]) >= min_sources
        ]
        
        if sort_by == "score":
            filtered.sort(key=lambda x: x[1].score, reverse=True)
        elif sort_by == "sources":
            filtered.sort(key=lambda x: len(x[2]), reverse=True)
        
        return [
            {
                "memory": r.memory.model_dump(),
                "score": r.score,
                "layer": r.layer,
                "sources": list(sources),
                "source_count": len(sources)
            }
            for _, r, sources in filtered
        ]
    
    def clear(self):
        """清空聚合器"""
        self._results.clear()
        self._sources.clear()


class LayeredResultBuilder:
    """分层结果构建器 - 构建分层检索结果"""
    
    def __init__(self, query: str, strategy: str):
        self.query = query
        self.strategy = strategy
        self._layers: Dict[str, List[Dict[str, Any]]] = {}
        self._combined: List[Dict[str, Any]] = []
    
    def add_layer(self, name: str, results: List[RetrievalResult]):
        """添加一层结果"""
        self._layers[name] = [
            {
                "memory": r.memory.model_dump(),
                "score": r.score,
                "layer": r.layer
            }
            for r in results
        ]
    
    def set_combined(self, results: List[Dict[str, Any]]):
        """设置合并结果"""
        self._combined = results
    
    def build(self) -> Dict[str, Any]:
        """构建结果字典"""
        return {
            "query": self.query,
            "strategy": self.strategy,
            "layers": self._layers,
            "combined": self._combined,
            "total_found": len(self._combined)
        }

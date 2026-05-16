"""
基于分数的遗忘策略
综合考虑重要性、时效性、访问频率

V2.25: 修复 forget_ratio 死代码、添加权重验证、统一评分逻辑
"""

from typing import Dict, List, Optional
import time
import math
import logging

from memory_system.core_abstractions.abstract_component_interfaces import ForgettingStrategy
from memory_system.core_abstractions.memory_data_models import MemoryItem
from memory_system.core_abstractions.component_registry_manager import register_forgetting
from memory_system.core_abstractions.system_constants_definitions import (
    DEFAULT_IMPORTANCE_WEIGHT,
    DEFAULT_RECENCY_WEIGHT,
    DEFAULT_ACCESS_WEIGHT,
    DEFAULT_FORGET_RATIO,
    DEFAULT_MIN_RETENTION_DAYS,
)

logger = logging.getLogger(__name__)

# 默认半衰期参数（天）
DEFAULT_RECENCY_HALF_LIFE_DAYS: float = 30.0
DEFAULT_FRESHNESS_HALF_LIFE_DAYS: float = 7.0

# 权重维度名称
_REQUIRED_WEIGHT_KEYS = {"importance", "recency", "access", "freshness"}


def _validate_weights(weights: Dict[str, float]) -> Dict[str, float]:
    """验证并规范化权重字典"""
    if not isinstance(weights, dict):
        raise TypeError(f"weights 必须是字典，当前类型: {type(weights).__name__}")

    missing = _REQUIRED_WEIGHT_KEYS - set(weights.keys())
    if missing:
        raise ValueError(f"weights 缺少必需维度: {missing}")

    for key, value in weights.items():
        if not isinstance(value, (int, float)):
            raise TypeError(f"weight[{key}] 必须是数字，当前类型: {type(value).__name__}")
        if value < 0:
            raise ValueError(f"weight[{key}] 不能为负数，当前值: {value}")

    total = sum(weights.values())
    if total == 0:
        raise ValueError("权重总和不能为零")

    # 归一化权重
    normalized = {k: v / total for k, v in weights.items()}
    return normalized


@register_forgetting("score_based")
class ScoreBasedForgetting(ForgettingStrategy):
    """
    基于综合评分的遗忘策略

    评分维度:
        - 重要性 (importance): 记忆本身的重要性等级
        - 时效性 (recency): 创建时间距今的衰减
        - 访问频率 (access): 被检索/访问的次数
        - 新鲜度 (freshness): 最后访问时间距今的衰减

    权重会在初始化时自动归一化，无需手动保证总和为1。
    """

    def __init__(
        self,
        forget_ratio: float = DEFAULT_FORGET_RATIO,
        min_retention_days: float = DEFAULT_MIN_RETENTION_DAYS,
        weights: Optional[Dict[str, float]] = None,
        recency_half_life_days: float = DEFAULT_RECENCY_HALF_LIFE_DAYS,
        freshness_half_life_days: float = DEFAULT_FRESHNESS_HALF_LIFE_DAYS,
    ):
        """
        初始化

        Args:
            forget_ratio: 遗忘比例 (0.0-1.0)，用于计算 max_keep = total * (1 - forget_ratio)
            min_retention_days: 最小保留天数，低于此天数的记忆不会被遗忘
            weights: 自定义评分权重字典，会自动归一化
            recency_half_life_days: 时效性衰减半衰期（天）
            freshness_half_life_days: 新鲜度衰减半衰期（天）
        """
        if not 0 <= forget_ratio <= 1:
            raise ValueError(f"forget_ratio 必须在 [0, 1] 范围内，当前值: {forget_ratio}")
        if min_retention_days < 0:
            raise ValueError(f"min_retention_days 不能为负数，当前值: {min_retention_days}")

        self.forget_ratio = forget_ratio
        self.min_retention_days = min_retention_days
        self.recency_half_life_days = recency_half_life_days
        self.freshness_half_life_days = freshness_half_life_days

        # 默认权重（归一化后总和为1）
        default_weights = {
            "importance": DEFAULT_IMPORTANCE_WEIGHT,
            "recency": DEFAULT_RECENCY_WEIGHT,
            "access": DEFAULT_ACCESS_WEIGHT,
            "freshness": max(0.0, 1.0 - DEFAULT_IMPORTANCE_WEIGHT - DEFAULT_RECENCY_WEIGHT - DEFAULT_ACCESS_WEIGHT),
        }
        self.weights = _validate_weights(weights if weights is not None else default_weights)

    async def select_for_forgetting(
        self,
        memories: List[MemoryItem],
        max_keep: int
    ) -> List[str]:
        """
        选择需要遗忘的记忆ID

        如果 max_keep 为 0 或负数，则使用 forget_ratio 计算实际保留数。

        Args:
            memories: 所有记忆
            max_keep: 最大保留数量

        Returns:
            需要删除的记忆ID列表
        """
        if not memories:
            return []

        # 使用 forget_ratio 计算 max_keep（当 max_keep 未明确设置时）
        effective_max_keep = max_keep
        if effective_max_keep <= 0:
            effective_max_keep = int(len(memories) * (1 - self.forget_ratio))
            effective_max_keep = max(1, effective_max_keep)  # 至少保留1条

        if len(memories) <= effective_max_keep:
            return []

        current_time = time.time()
        min_retention_seconds = self.min_retention_days * 86400

        # 计算每个记忆的保留分数
        scored_memories = []
        for memory in memories:
            score = self._calculate_retention_score(memory, current_time)
            scored_memories.append((memory, score))

        # 按分数降序排序
        scored_memories.sort(key=lambda x: x[1], reverse=True)

        # 选择需要遗忘的（分数最低的）
        to_forget = []
        for memory, score in scored_memories[effective_max_keep:]:
            # 检查最小保留时间
            age = current_time - memory.timestamp
            if age > min_retention_seconds:
                to_forget.append(memory.memory_id)
                logger.debug(
                    "遗忘记忆: id=%s, score=%.4f, age=%.1f天",
                    memory.memory_id, score, age / 86400
                )

        if to_forget:
            logger.info(
                "遗忘策略执行完成: 总计%d条, 保留%d条, 遗忘%d条",
                len(memories), effective_max_keep, len(to_forget)
            )

        return to_forget

    def _calculate_retention_score(
        self,
        memory: MemoryItem,
        current_time: float
    ) -> float:
        """
        计算记忆的保留分数

        使用与 MemoryItem.calculate_relevance_score() 一致的评分逻辑，
        但增加了 freshness 维度。

        Args:
            memory: 记忆项
            current_time: 当前时间

        Returns:
            保留分数 (0-1)
        """
        # 重要性 (归一化到 0-1)
        importance_score = memory.importance.value / 5.0

        # 时效性 - 指数衰减
        age_days = (current_time - memory.timestamp) / 86400
        recency_score = math.exp(-age_days / self.recency_half_life_days)

        # 访问频率 (归一化到 0-1)
        access_score = min(1.0, memory.access_count / 10.0)

        # 新鲜度 - 最后访问时间的指数衰减
        last_access_days = (current_time - memory.last_access) / 86400
        freshness_score = math.exp(-last_access_days / self.freshness_half_life_days)

        # 综合分数（权重已归一化）
        total_score = (
            importance_score * self.weights["importance"] +
            recency_score * self.weights["recency"] +
            access_score * self.weights["access"] +
            freshness_score * self.weights["freshness"]
        )

        return float(total_score)

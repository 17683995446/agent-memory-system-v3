"""
自适应检索策略选择模块 (I-10)
根据查询特征自动选择最优检索策略
"""

import time
import re
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class StrategyType(str, Enum):
    """检索策略类型"""
    FAST = "fast"           # 快速模式：仅短期记忆
    ADAPTIVE = "adaptive"   # 自适应模式：L1+L2+L3
    THOROUGH = "thorough"   # 彻底模式：L1+L2+L3+L4
    HYBRID = "hybrid"       # 混合模式：结合关键词和向量


@dataclass
class QueryFeatures:
    """
    查询特征

    Attributes:
        length: 查询长度
        has_keywords: 是否包含特定关键词
        has_temporal: 是否包含时间相关词
        has_entities: 是否包含实体名称
        complexity_score: 复杂度评分
    """
    length: int = 0
    has_keywords: bool = False
    has_temporal: bool = False
    has_entities: bool = False
    complexity_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "length": self.length,
            "has_keywords": self.has_keywords,
            "has_temporal": self.has_temporal,
            "has_entities": self.has_entities,
            "complexity_score": self.complexity_score,
        }


@dataclass
class StrategySelection:
    """
    策略选择结果

    Attributes:
        strategy: 选择的策略
        confidence: 选择置信度
        features: 查询特征
        reason: 选择原因
    """
    strategy: StrategyType
    confidence: float
    features: QueryFeatures
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "confidence": self.confidence,
            "features": self.features.to_dict(),
            "reason": self.reason,
        }


class AdaptiveStrategySelector:
    """
    自适应检索策略选择器

    根据查询特征自动选择最优检索策略：
        - 短查询 -> fast（仅短期记忆）
        - 包含特定关键词 -> hybrid（混合检索）
        - 长复杂查询 -> thorough（完整检索）
        - 默认 -> adaptive（平衡检索）

    使用示例:
        >>> selector = AdaptiveStrategySelector()
        >>> selection = selector.select_strategy("用户偏好", context={})
        >>> print(selection.strategy)  # StrategyType.FAST
    """

    # 特定关键词列表（用于触发hybrid模式）
    KEYWORD_PATTERNS = [
        r'\b(name|姓名)\b',
        r'\b(id|标识)\b',
        r'\b(date|日期)\b',
        r'\b(time|时间)\b',
        r'\b(location|位置)\b',
        r'\b(phone|电话)\b',
        r'\b(email|邮箱)\b',
        r'\b(address|地址)\b',
    ]

    # 时间相关词
    TEMPORAL_PATTERNS = [
        r'\b(yesterday|昨天)\b',
        r'\b(today|今天)\b',
        r'\b(tomorrow|明天)\b',
        r'\b(last|上|前)\b',
        r'\b(next|下|后)\b',
        r'\b(recent|最近)\b',
        r'\b(ago|之前)\b',
    ]

    # 实体名称模式（简化版）
    ENTITY_PATTERNS = [
        r'\b[A-Z][a-z]+\b',  # 大写开头的词
    ]

    def __init__(
        self,
        short_query_threshold: int = 10,
        long_query_threshold: int = 50,
        complexity_threshold: float = 0.7,
    ):
        """
        初始化策略选择器

        Args:
            short_query_threshold: 短查询长度阈值
            long_query_threshold: 长查询长度阈值
            complexity_threshold: 复杂度阈值
        """
        self.short_query_threshold = short_query_threshold
        self.long_query_threshold = long_query_threshold
        self.complexity_threshold = complexity_threshold

        # 统计信息
        self._stats = {
            "total_selections": 0,
            "strategy_counts": {s.value: 0 for s in StrategyType},
        }

    def select_strategy(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> StrategySelection:
        """
        根据查询特征选择最优检索策略

        Args:
            query: 查询内容
            context: 上下文信息

        Returns:
            策略选择结果
        """
        context = context or {}

        # 提取查询特征
        features = self._extract_features(query)

        # 策略选择逻辑
        strategy, confidence, reason = self._select_strategy_internal(features, context)

        # 更新统计
        self._stats["total_selections"] += 1
        self._stats["strategy_counts"][strategy.value] += 1

        return StrategySelection(
            strategy=strategy,
            confidence=confidence,
            features=features,
            reason=reason
        )

    def _extract_features(self, query: str) -> QueryFeatures:
        """提取查询特征"""
        features = QueryFeatures()
        features.length = len(query)

        query_lower = query.lower()

        # 检查关键词
        for pattern in self.KEYWORD_PATTERNS:
            if re.search(pattern, query_lower):
                features.has_keywords = True
                break

        # 检查时间相关词
        for pattern in self.TEMPORAL_PATTERNS:
            if re.search(pattern, query_lower):
                features.has_temporal = True
                break

        # 检查实体
        for pattern in self.ENTITY_PATTERNS:
            if re.search(pattern, query):
                features.has_entities = True
                break

        # 计算复杂度评分
        features.complexity_score = self._calculate_complexity(query, features)

        return features

    def _calculate_complexity(self, query: str, features: QueryFeatures) -> float:
        """计算查询复杂度评分"""
        score = 0.0

        # 长度因子
        if features.length > self.long_query_threshold:
            score += 0.4
        elif features.length > self.short_query_threshold:
            score += 0.2

        # 关键词因子
        if features.has_keywords:
            score += 0.2

        # 时间因子
        if features.has_temporal:
            score += 0.15

        # 实体因子
        if features.has_entities:
            score += 0.15

        # 特殊字符复杂度
        special_chars = len(re.findall(r'[^\w\s]', query))
        score += min(0.1, special_chars * 0.01)

        return min(1.0, score)

    def _select_strategy_internal(
        self,
        features: QueryFeatures,
        context: Dict[str, Any]
    ) -> Tuple[StrategyType, float, str]:
        """内部策略选择逻辑"""

        # 1. 短查询 -> fast
        if features.length < self.short_query_threshold:
            return StrategyType.FAST, 0.9, "短查询，使用快速模式"

        # 2. 包含特定关键词 -> hybrid
        if features.has_keywords:
            return StrategyType.HYBRID, 0.85, "包含特定关键词，使用混合检索"

        # 3. 时间相关查询 -> adaptive（包含时间范围检索）
        if features.has_temporal:
            return StrategyType.ADAPTIVE, 0.8, "时间相关查询，使用自适应模式"

        # 4. 长复杂查询 -> thorough
        if features.length > self.long_query_threshold:
            return StrategyType.THOROUGH, 0.9, "长复杂查询，使用彻底检索"

        # 5. 高复杂度 -> thorough
        if features.complexity_score > self.complexity_threshold:
            return StrategyType.THOROUGH, 0.85, "高复杂度查询，使用彻底检索"

        # 6. 上下文覆盖策略
        if context.get("strategy_override"):
            override = context["strategy_override"]
            try:
                return StrategyType(override), 1.0, f"上下文指定策略: {override}"
            except ValueError:
                pass

        # 默认 -> adaptive
        return StrategyType.ADAPTIVE, 0.75, "默认自适应模式"

    def batch_select(
        self,
        queries: List[str],
        context: Optional[Dict[str, Any]] = None
    ) -> List[StrategySelection]:
        """
        批量选择策略

        Args:
            queries: 查询列表
            context: 上下文信息

        Returns:
            策略选择结果列表
        """
        return [self.select_strategy(q, context) for q in queries]

    def get_stats(self) -> Dict[str, Any]:
        """获取选择统计信息"""
        total = max(self._stats["total_selections"], 1)
        return {
            **self._stats,
            "strategy_percentages": {
                k: v / total for k, v in self._stats["strategy_counts"].items()
            }
        }

    def reset_stats(self) -> None:
        """重置统计信息"""
        self._stats = {
            "total_selections": 0,
            "strategy_counts": {s.value: 0 for s in StrategyType},
        }


class ContextAwareStrategySelector(AdaptiveStrategySelector):
    """
    上下文感知的策略选择器

    在AdaptiveStrategySelector基础上增加会话上下文感知能力
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 会话历史
        self._session_history: Dict[str, List[Dict[str, Any]]] = {}
        # 会话策略偏好
        self._session_preferences: Dict[str, StrategyType] = {}

    def select_strategy_with_session(
        self,
        query: str,
        session_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> StrategySelection:
        """
        基于会话上下文选择策略

        Args:
            query: 查询内容
            session_id: 会话ID
            context: 上下文信息

        Returns:
            策略选择结果
        """
        context = context or {}

        # 基础选择
        selection = self.select_strategy(query, context)

        # 记录会话历史
        if session_id not in self._session_history:
            self._session_history[session_id] = []

        self._session_history[session_id].append({
            "timestamp": time.time(),
            "query": query,
            "strategy": selection.strategy.value,
        })

        # 限制历史大小
        if len(self._session_history[session_id]) > 100:
            self._session_history[session_id] = self._session_history[session_id][-100:]

        # 检查会话偏好
        if session_id in self._session_preferences:
            preferred = self._session_preferences[session_id]
            # 如果当前选择与偏好一致，提高置信度
            if selection.strategy == preferred:
                selection.confidence = min(1.0, selection.confidence + 0.1)
                selection.reason += " (与会话偏好一致)"

        return selection

    def set_session_preference(self, session_id: str, strategy: StrategyType) -> None:
        """设置会话策略偏好"""
        self._session_preferences[session_id] = strategy

    def clear_session(self, session_id: str) -> None:
        """清理会话数据"""
        if session_id in self._session_history:
            del self._session_history[session_id]
        if session_id in self._session_preferences:
            del self._session_preferences[session_id]

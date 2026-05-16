"""
基于启发式规则的记忆重要性自动评估器

评估维度:
    1. 内容长度和丰富度
    2. 事实性陈述检测（包含具体数字、日期、名称等）
    3. 情感/偏好表达检测（喜欢、讨厌、偏好等关键词）
    4. 关键词命中（过敏、重要、紧急、必须等高重要性信号词）
"""

import re
from typing import Any, Dict, Optional, Tuple

from memory_system.core_abstractions.abstract_component_interfaces import ImportanceScorer
from memory_system.core_abstractions.memory_data_models import ImportanceLevel
from memory_system.core_abstractions.component_registry_manager import register_importance_scorer
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)

# 高重要性信号词
_HIGH_IMPORTANCE_SIGNALS = [
    "过敏", "allergy", "重要", "important", "紧急", "urgent",
    "必须", "must", "禁止", "forbidden", "关键", "critical",
    "安全", "safety", "密码", "password", "密钥", "key",
    "地址", "address", "电话", "phone", "生日", "birthday",
]

# 偏好/情感表达信号词
_PREFERENCE_SIGNALS = [
    "喜欢", "love", "讨厌", "hate", "偏好", "prefer",
    "感兴趣", "interested", "擅长", "good at", "不会",
    "不能", "can't", "总是", "always", "从不", "never",
]

# 事实性陈述模式
_FACT_PATTERNS = [
    r'\d{4}年',           # 年份
    r'\d+月\d+日',        # 日期
    r'\d+岁',             # 年龄
    r'\$?\d[\d,]*\.?\d*', # 数字（含货币）
    r'[A-Z][a-z]+(?:\s[A-Z][a-z]+)+', # 英文人名（至少两个大写开头单词）
]


@register_importance_scorer("heuristic")
class HeuristicImportanceScorer(ImportanceScorer):
    """
    基于启发式规则的记忆重要性自动评估器
    
    通过多维度分析记忆内容，自动判断其重要性级别。
    适用于无用户标注 importance 时的自动评估场景。
    """
    
    def __init__(
        self,
        high_signal_weight: float = 0.4,
        preference_weight: float = 0.25,
        fact_weight: float = 0.2,
        length_weight: float = 0.15,
        high_threshold: float = 0.6,
        medium_threshold: float = 0.3,
    ):
        """
        Args:
            high_signal_weight: 高重要性信号词权重
            preference_weight: 偏好表达权重
            fact_weight: 事实性陈述权重
            length_weight: 内容长度权重
            high_threshold: HIGH 级别阈值
            medium_threshold: MEDIUM 级别阈值
        """
        self.high_signal_weight = high_signal_weight
        self.preference_weight = preference_weight
        self.fact_weight = fact_weight
        self.length_weight = length_weight
        self.high_threshold = high_threshold
        self.medium_threshold = medium_threshold
    
    async def score(
        self,
        content: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Tuple[ImportanceLevel, float]:
        """
        评估记忆内容的重要性
        
        Args:
            content: 记忆内容
            context: 上下文信息
            
        Returns:
            (重要性级别, 置信度)
        """
        if not content or not content.strip():
            return ImportanceLevel.TRIVIAL, 0.0
        
        content_lower = content.lower()
        
        # 维度1: 高重要性信号词命中
        high_score = 0.0
        matched_signals = []
        for signal in _HIGH_IMPORTANCE_SIGNALS:
            if signal in content_lower:
                high_score = min(1.0, high_score + 0.3)
                matched_signals.append(signal)
        
        # 维度2: 偏好/情感表达
        preference_score = 0.0
        for signal in _PREFERENCE_SIGNALS:
            if signal in content_lower:
                preference_score = min(1.0, preference_score + 0.25)
        
        # 维度3: 事实性陈述
        fact_score = 0.0
        for pattern in _FACT_PATTERNS:
            if re.search(pattern, content):
                fact_score = min(1.0, fact_score + 0.3)
        
        # 维度4: 内容长度和丰富度
        length_score = min(1.0, len(content) / 200.0)
        
        # 加权综合评分
        total_score = (
            high_score * self.high_signal_weight
            + preference_score * self.preference_weight
            + fact_score * self.fact_weight
            + length_score * self.length_weight
        )
        
        # 确定重要性级别
        if total_score >= self.high_threshold:
            level = ImportanceLevel.HIGH
        elif total_score >= self.medium_threshold:
            level = ImportanceLevel.MEDIUM
        elif total_score >= 0.15:
            level = ImportanceLevel.LOW
        else:
            level = ImportanceLevel.TRIVIAL
        
        # 置信度：基于评分与阈值的距离
        if level == ImportanceLevel.HIGH:
            confidence = min(1.0, (total_score - self.high_threshold) / 0.4 + 0.5)
        elif level == ImportanceLevel.MEDIUM:
            confidence = min(1.0, (total_score - self.medium_threshold) / 0.3 + 0.5)
        else:
            confidence = max(0.3, total_score / 0.3)
        
        logger.debug(
            f"重要性评估: score={total_score:.3f}, level={level.name}, "
            f"confidence={confidence:.3f}, signals={matched_signals}"
        )
        
        return level, round(confidence, 3)

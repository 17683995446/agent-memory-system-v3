"""
整合层模块
记忆巩固策略
"""
from memory_system.consolidation_strategies.time_based_consolidation_strategy import TimeBasedConsolidation
# 向后兼容：从新位置导入
from memory_system.forgetting_strategies.score_based_forgetting_strategy import ScoreBasedForgetting

__all__ = ["TimeBasedConsolidation", "ScoreBasedForgetting"]

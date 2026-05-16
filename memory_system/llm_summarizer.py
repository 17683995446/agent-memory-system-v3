"""
基于LLM的记忆摘要生成模块 (N-01)
提供智能记忆摘要和标签生成功能
"""

import re
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import Counter
from dataclasses import dataclass

from memory_system.core_abstractions.mathematical_utility_functions import tokenize
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class SummaryResult:
    """
    摘要结果

    Attributes:
        summary: 摘要文本
        tags: 标签列表
        key_points: 关键要点
        confidence: 置信度
    """
    summary: str
    tags: List[str]
    key_points: List[str]
    confidence: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary": self.summary,
            "tags": self.tags,
            "key_points": self.key_points,
            "confidence": self.confidence,
        }


class LLMSummarizer:
    """
    基于LLM的记忆摘要生成器

    功能:
        - 生成记忆内容的智能摘要
        - 提取关键词标签
        - 识别关键要点
        - 支持多种摘要策略

    使用示例:
        >>> summarizer = LLMSummarizer()
        >>> result = await summarizer.summarize("用户喜欢Python编程...")
        >>> print(result.summary)
        >>> tags = await summarizer.generate_tags("用户喜欢Python编程...")
    """

    # 停用词列表（简化版）
    STOP_WORDS = {
        '的', '了', '在', '是', '我', '有', '和', '就', '不', '人',
        '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去',
        '你', '会', '着', '没有', '看', '好', '自己', '这', '那',
        'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
        'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
        'would', 'could', 'should', 'may', 'might', 'must', 'shall',
        'can', 'need', 'dare', 'ought', 'used', 'to', 'of', 'in',
        'for', 'on', 'with', 'at', 'by', 'from', 'as', 'into',
        'through', 'during', 'before', 'after', 'above', 'below',
        'between', 'under', 'and', 'but', 'or', 'yet', 'so',
    }

    def __init__(
        self,
        max_summary_length: int = 100,
        max_tags: int = 5,
        min_tag_length: int = 2,
        language: str = "auto",
    ):
        """
        初始化摘要生成器

        Args:
            max_summary_length: 最大摘要长度
            max_tags: 最大标签数量
            min_tag_length: 最小标签长度
            language: 语言设置 (auto/zh/en)
        """
        self.max_summary_length = max_summary_length
        self.max_tags = max_tags
        self.min_tag_length = min_tag_length
        self.language = language

        # 统计
        self._stats = {
            "total_summarized": 0,
            "total_tags_generated": 0,
        }

    async def summarize(
        self,
        content: str,
        strategy: str = "extractive",
        max_length: Optional[int] = None
    ) -> SummaryResult:
        """
        生成记忆摘要

        Args:
            content: 记忆内容
            strategy: 摘要策略 (extractive/abstractive/hybrid)
            max_length: 最大长度限制

        Returns:
            摘要结果
        """
        max_len = max_length or self.max_summary_length

        if strategy == "extractive":
            summary, key_points = self._extractive_summarize(content, max_len)
        elif strategy == "abstractive":
            summary, key_points = await self._abstractive_summarize(content, max_len)
        else:  # hybrid
            summary, key_points = await self._hybrid_summarize(content, max_len)

        # 生成标签
        tags = await self.generate_tags(content)

        # 计算置信度
        confidence = self._calculate_confidence(content, summary)

        self._stats["total_summarized"] += 1

        return SummaryResult(
            summary=summary,
            tags=tags,
            key_points=key_points,
            confidence=confidence
        )

    def _extractive_summarize(self, content: str, max_length: int) -> Tuple[str, List[str]]:
        """
        抽取式摘要

        从原文中抽取最重要的句子组成摘要
        """
        # 分句
        sentences = self._split_sentences(content)

        if len(sentences) <= 2:
            return content, sentences

        # 计算句子重要性分数
        sentence_scores = []
        word_freq = self._calculate_word_freq(content)

        for sentence in sentences:
            score = self._calculate_sentence_score(sentence, word_freq)
            sentence_scores.append((score, sentence))

        # 按重要性排序
        sentence_scores.sort(reverse=True)

        # 选择最重要的句子
        selected = sentence_scores[:2]
        selected.sort(key=lambda x: sentences.index(x[1]))  # 按原文顺序排序

        summary = ' '.join([s[1] for s in selected])
        if len(summary) > max_length:
            summary = summary[:max_length] + "..."

        key_points = [s[1] for s in selected]

        return summary, key_points

    async def _abstractive_summarize(self, content: str, max_length: int) -> Tuple[str, List[str]]:
        """
        生成式摘要

        使用LLM生成新的摘要文本（当前为占位实现）
        """
        # 实际实现中，这里会调用LLM API
        # 目前使用抽取式作为降级方案
        return self._extractive_summarize(content, max_length)

    async def _hybrid_summarize(self, content: str, max_length: int) -> Tuple[str, List[str]]:
        """
        混合式摘要

        结合抽取和生成的方法
        """
        # 先抽取关键句子
        extractive_summary, key_points = self._extractive_summarize(content, max_length // 2)

        # 如果内容较短，直接返回抽取结果
        if len(content) < max_length:
            return content, key_points

        # 否则尝试生成更精简的摘要
        # 目前使用抽取结果
        return extractive_summary, key_points

    async def generate_tags(self, content: str) -> List[str]:
        """
        生成内容标签

        基于词频和TF-IDF-like算法提取关键词

        Args:
            content: 记忆内容

        Returns:
            标签列表
        """
        # 分词
        words = tokenize(content)

        # 过滤停用词和短词
        filtered_words = [
            w for w in words
            if w not in self.STOP_WORDS and len(w) >= self.min_tag_length
        ]

        if not filtered_words:
            return []

        # 计算词频
        word_counts = Counter(filtered_words)

        # 计算TF-IDF-like分数（简化版）
        # 考虑词的位置（标题/开头权重更高）
        word_scores = {}
        total_words = len(filtered_words)

        for word, count in word_counts.items():
            # 基础TF分数
            tf = count / total_words

            # 位置权重
            position_bonus = 0
            if word in filtered_words[:5]:  # 开头
                position_bonus = 0.3
            elif word in filtered_words[-5:]:  # 结尾
                position_bonus = 0.1

            # 长度奖励（中等长度词通常更有意义）
            length_bonus = 0
            word_len = len(word)
            if 4 <= word_len <= 8:
                length_bonus = 0.1

            word_scores[word] = tf + position_bonus + length_bonus

        # 选择得分最高的词作为标签
        top_words = sorted(word_scores.items(), key=lambda x: x[1], reverse=True)
        tags = [word for word, _ in top_words[:self.max_tags]]

        self._stats["total_tags_generated"] += len(tags)

        return tags

    def _split_sentences(self, text: str) -> List[str]:
        """分句"""
        # 支持中英文分句
        # 中文分句
        chinese_sentences = re.split(r'[。！？；]', text)
        # 英文分句
        english_sentences = re.split(r'[.!?;]', text)

        # 合并并去重
        all_sentences = chinese_sentences + english_sentences
        sentences = [s.strip() for s in all_sentences if len(s.strip()) > 5]

        return sentences if sentences else [text]

    def _calculate_word_freq(self, content: str) -> Dict[str, int]:
        """计算词频"""
        words = tokenize(content)
        return Counter(w for w in words if w not in self.STOP_WORDS)

    def _calculate_sentence_score(self, sentence: str, word_freq: Dict[str, int]) -> float:
        """计算句子重要性分数"""
        words = tokenize(sentence)
        if not words:
            return 0.0

        # 基于词频计算
        score = sum(word_freq.get(w, 0) for w in words if w not in self.STOP_WORDS)

        # 长度归一化
        score /= len(words)

        # 位置奖励（开头和结尾的句子通常更重要）
        # 这里简化处理，实际应该传入句子位置

        return score

    def _calculate_confidence(self, original: str, summary: str) -> float:
        """计算摘要置信度"""
        if not original or not summary:
            return 0.0

        # 基于摘要覆盖率的简单置信度计算
        original_words = set(tokenize(original))
        summary_words = set(tokenize(summary))

        if not original_words:
            return 0.0

        coverage = len(summary_words & original_words) / len(original_words)

        # 长度比例
        length_ratio = len(summary) / len(original)

        # 综合置信度
        confidence = (coverage * 0.6) + (min(1.0, length_ratio * 2) * 0.4)

        return min(1.0, confidence)

    async def batch_summarize(
        self,
        contents: List[str],
        strategy: str = "extractive"
    ) -> List[SummaryResult]:
        """
        批量生成摘要

        Args:
            contents: 内容列表
            strategy: 摘要策略

        Returns:
            摘要结果列表
        """
        results = []
        for content in contents:
            result = await self.summarize(content, strategy)
            results.append(result)
        return results

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """重置统计"""
        self._stats = {
            "total_summarized": 0,
            "total_tags_generated": 0,
        }

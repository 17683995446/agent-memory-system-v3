"""
增量嵌入更新机制
记忆内容小幅度变更时只更新差异部分
"""

import difflib
from typing import List, Tuple, Optional
from dataclasses import dataclass

from memory_system.core_abstractions.abstract_component_interfaces import EmbeddingProvider


@dataclass
class TextDiff:
    """文本差异"""
    operation: str  # 'insert', 'delete', 'replace', 'equal'
    old_start: int
    old_end: int
    new_start: int
    new_end: int
    content: str


class IncrementalEmbeddingUpdater:
    """增量嵌入更新器"""

    def __init__(self, embedding_provider: EmbeddingProvider, threshold: float = 0.3):
        self.embedding = embedding_provider
        self.threshold = threshold  # 触发全量更新的变更阈值

    def compute_diff(self, old_text: str, new_text: str) -> List[TextDiff]:
        """计算两段文本的差异"""
        differ = difflib.SequenceMatcher(None, old_text, new_text)
        diffs = []

        for tag, i1, i2, j1, j2 in differ.get_opcodes():
            if tag == 'equal':
                continue
            diffs.append(TextDiff(
                operation=tag,
                old_start=i1,
                old_end=i2,
                new_start=j1,
                new_end=j2,
                content=new_text[j1:j2] if tag != 'delete' else old_text[i1:i2]
            ))

        return diffs

    def should_full_update(self, old_text: str, new_text: str) -> bool:
        """判断是否需要全量更新"""
        if not old_text or not new_text:
            return True

        diff_ratio = difflib.SequenceMatcher(None, old_text, new_text).ratio()
        return diff_ratio < (1 - self.threshold)

    async def update_embedding(
        self,
        old_text: str,
        new_text: str,
        old_embedding: List[float]
    ) -> Tuple[List[float], bool]:
        """
        更新嵌入向量

        Returns:
            (new_embedding, is_full_update)
        """
        if self.should_full_update(old_text, new_text):
            # 变更太大，需要全量更新
            new_embedding = await self.embedding.embed(new_text)
            return new_embedding, True

        # 计算差异
        diffs = self.compute_diff(old_text, new_text)

        if not diffs:
            # 没有变化
            return old_embedding, False

        # 增量更新策略：基于差异调整嵌入
        # 简化实现：对新增内容计算嵌入并加权平均
        new_embedding = await self._incremental_update(old_embedding, diffs, old_text, new_text)

        return new_embedding, False

    async def _incremental_update(
        self,
        old_embedding: List[float],
        diffs: List[TextDiff],
        old_text: str,
        new_text: str
    ) -> List[float]:
        """执行增量更新"""
        # 获取新增内容的嵌入
        added_content = ''.join(d.content for d in diffs if d.operation in ('insert', 'replace'))

        if not added_content:
            return old_embedding

        added_embedding = await self.embedding.embed(added_content)

        # 加权融合：根据新增内容占比调整权重
        added_ratio = len(added_content) / len(new_text) if new_text else 0
        weight = min(added_ratio * 2, 0.5)  # 最大权重0.5

        # 线性插值
        new_embedding = [
            old_emb * (1 - weight) + add_emb * weight
            for old_emb, add_emb in zip(old_embedding, added_embedding)
        ]

        return new_embedding

    def compute_change_impact(self, old_text: str, new_text: str) -> float:
        """
        计算变更的影响程度

        Returns:
            影响程度分数 (0.0-1.0)，越高表示变更越大
        """
        if not old_text:
            return 1.0 if new_text else 0.0
        if not new_text:
            return 1.0

        # 使用相似度计算影响
        similarity = difflib.SequenceMatcher(None, old_text, new_text).ratio()
        return 1.0 - similarity

    def get_diff_summary(self, diffs: List[TextDiff]) -> dict:
        """
        获取差异摘要统计

        Returns:
            包含统计信息的字典
        """
        insertions = sum(1 for d in diffs if d.operation == 'insert')
        deletions = sum(1 for d in diffs if d.operation == 'delete')
        replacements = sum(1 for d in diffs if d.operation == 'replace')

        inserted_chars = sum(len(d.content) for d in diffs if d.operation == 'insert')
        deleted_chars = sum(len(d.content) for d in diffs if d.operation == 'delete')
        replaced_chars = sum(len(d.content) for d in diffs if d.operation == 'replace')

        return {
            "insertions": insertions,
            "deletions": deletions,
            "replacements": replacements,
            "inserted_chars": inserted_chars,
            "deleted_chars": deleted_chars,
            "replaced_chars": replaced_chars,
            "total_changes": insertions + deletions + replacements
        }

    async def batch_update_embeddings(
        self,
        updates: List[Tuple[str, str, List[float]]]
    ) -> List[Tuple[List[float], bool]]:
        """
        批量更新嵌入

        Args:
            updates: List of (old_text, new_text, old_embedding) tuples

        Returns:
            List of (new_embedding, is_full_update) tuples
        """
        results = []
        for old_text, new_text, old_embedding in updates:
            result = await self.update_embedding(old_text, new_text, old_embedding)
            results.append(result)
        return results


class AdaptiveIncrementalUpdater(IncrementalEmbeddingUpdater):
    """自适应增量更新器 - 根据内容类型自动调整策略"""

    def __init__(
        self,
        embedding_provider: EmbeddingProvider,
        base_threshold: float = 0.3,
        adaptive_mode: bool = True
    ):
        super().__init__(embedding_provider, base_threshold)
        self.adaptive_mode = adaptive_mode
        self._content_type_thresholds = {
            "code": 0.2,      # 代码变更敏感
            "text": 0.3,      # 普通文本
            "structured": 0.25,  # 结构化数据
        }

    def detect_content_type(self, text: str) -> str:
        """检测内容类型"""
        # 简单的启发式检测
        code_indicators = ['def ', 'class ', 'import ', '{', '}', 'function', 'var ', 'const ']
        if any(indicator in text for indicator in code_indicators):
            return "code"

        # 检测结构化数据（JSON-like）
        if text.strip().startswith(('{', '[')) and ('"' in text or "'" in text):
            return "structured"

        return "text"

    def should_full_update(self, old_text: str, new_text: str) -> bool:
        """自适应判断是否全量更新"""
        if not self.adaptive_mode:
            return super().should_full_update(old_text, new_text)

        # 检测内容类型并使用对应的阈值
        content_type = self.detect_content_type(new_text or old_text)
        threshold = self._content_type_thresholds.get(content_type, self.threshold)

        if not old_text or not new_text:
            return True

        diff_ratio = difflib.SequenceMatcher(None, old_text, new_text).ratio()
        return diff_ratio < (1 - threshold)

    def set_content_type_threshold(self, content_type: str, threshold: float):
        """设置特定内容类型的阈值"""
        self._content_type_thresholds[content_type] = threshold

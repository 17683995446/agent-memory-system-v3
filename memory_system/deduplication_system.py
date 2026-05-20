"""
记忆去重系统
基于语义相似度的重复记忆检测与合并
"""

import re
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass
import asyncio
from collections import defaultdict
import time
import logging

from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend, EmbeddingProvider
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel

logger = logging.getLogger(__name__)


@dataclass
class DuplicateGroup:
    """重复记忆组"""
    group_id: str
    primary_memory_id: str
    duplicate_memory_ids: List[str]
    similarity_scores: Dict[str, float]
    merged_content: Optional[str] = None


class MemoryDeduplicationSystem:
    """记忆去重系统"""

    def __init__(
        self,
        embedding_provider: EmbeddingProvider,
        similarity_threshold: float = 0.95,
        storage: Optional[StorageBackend] = None
    ):
        self.embedding = embedding_provider
        self.similarity_threshold = similarity_threshold
        self.storage = storage
        self._dedup_index: Dict[str, List[float]] = {}  # memory_id -> embedding

    async def find_duplicates(
        self,
        new_memory: MemoryItem,
        candidate_memories: Optional[List[MemoryItem]] = None
    ) -> List[Tuple[MemoryItem, float]]:
        """
        查找与新记忆重复的记忆

        Returns:
            List of (memory, similarity_score) tuples
        """
        new_embedding = new_memory.embedding or await self.embedding.embed(new_memory.content)

        if candidate_memories is None and self.storage:
            # 从存储中获取候选记忆（简化：获取最近100条）
            candidate_memories = await self.storage.list_memories(limit=100)

        duplicates = []
        for candidate in candidate_memories or []:
            if candidate.memory_id == new_memory.memory_id:
                continue

            candidate_embedding = candidate.embedding or await self.embedding.embed(candidate.content)
            similarity = await self.embedding.similarity(new_embedding, candidate_embedding)

            if similarity >= self.similarity_threshold:
                duplicates.append((candidate, similarity))

        # 按相似度排序
        duplicates.sort(key=lambda x: x[1], reverse=True)
        return duplicates

    async def merge_duplicates(
        self,
        memories: List[MemoryItem],
        strategy: str = "newest"
    ) -> Tuple[MemoryItem, List[str]]:
        """
        合并重复记忆

        Args:
            memories: 要合并的记忆列表
            strategy: 合并策略 - "newest", "longest", "ensemble"

        Returns:
            (merged_memory, list_of_merged_ids)
        """
        if not memories:
            raise ValueError("Cannot merge empty list of memories")

        if len(memories) == 1:
            return memories[0], []

        # 选择主记忆
        if strategy == "newest":
            primary = max(memories, key=lambda m: m.timestamp)
        elif strategy == "longest":
            primary = max(memories, key=lambda m: len(m.content))
        elif strategy == "ensemble":
            primary = await self._ensemble_merge(memories)
        else:
            primary = memories[0]

        merged_ids = [m.memory_id for m in memories if m.memory_id != primary.memory_id]

        return primary, merged_ids

    async def _ensemble_merge(self, memories: List[MemoryItem]) -> MemoryItem:
        """集成合并：综合多条记忆的信息"""
        # 选择最新的作为主记忆
        primary = max(memories, key=lambda m: m.timestamp)

        # 合并内容（去重句子）
        all_sentences = []
        for memory in memories:
            # BUG 9 修复：原实现只按中文句号('。')分割，
            # 现在支持中文句号、英文句号、问号、感叹号、分号等多种分隔符
            sentences = re.split(r'[。.!?！？；;]', memory.content)
            all_sentences.extend(s.strip() for s in sentences if s.strip())

        # 去重（保持顺序）
        seen = set()
        unique_sentences = []
        for sent in all_sentences:
            if sent not in seen:
                unique_sentences.append(sent)
                seen.add(sent)

        merged_content = '。'.join(unique_sentences) + '。'

        # 创建新的合并记忆
        merged = MemoryItem(
            content=merged_content,
            memory_type=primary.memory_type,
            importance=primary.importance,
            metadata={
                **primary.metadata,
                "merged_from": [m.memory_id for m in memories],
                "merge_count": len(memories)
            }
        )

        return merged

    async def deduplicate_storage(self, batch_size: int = 100) -> Dict[str, int]:
        """
        对整个存储执行去重

        Returns:
            Statistics dict with counts
        """
        if not self.storage:
            raise ValueError("Storage backend required for deduplication")

        stats = {"scanned": 0, "duplicates_found": 0, "merged": 0}

        # 获取所有记忆
        all_memories = await self.storage.list_memories(limit=10000)
        stats["scanned"] = len(all_memories)

        # 构建相似度图
        processed = set()
        groups = []

        for memory in all_memories:
            if memory.memory_id in processed:
                continue

            # 查找相似记忆
            duplicates = await self.find_duplicates(memory, all_memories)
            if duplicates:
                group = [memory] + [d[0] for d in duplicates]
                groups.append(group)
                processed.update(m.memory_id for m in group)
                stats["duplicates_found"] += len(duplicates)

        # 合并每组重复记忆
        for group in groups:
            merged, merged_ids = await self.merge_duplicates(group)

            # 保存合并后的记忆
            await self.storage.add(merged)

            # 删除重复记忆
            for memory_id in merged_ids:
                # BUG 10 修复：删除记忆失败时不应中断整个去重流程，
                # 添加 try-except 记录日志后继续处理其他记忆
                try:
                    await self.storage.delete(memory_id)
                except Exception as e:
                    logger.warning(f"Failed to delete duplicate memory {memory_id}: {e}")

            stats["merged"] += len(merged_ids)

        return stats

    async def check_duplicate(self, memory: MemoryItem) -> Optional[DuplicateGroup]:
        """
        检查记忆是否是重复

        Returns:
            DuplicateGroup if duplicate found, None otherwise
        """
        duplicates = await self.find_duplicates(memory)

        if not duplicates:
            return None

        duplicate_ids = [d[0].memory_id for d in duplicates]
        similarity_scores = {d[0].memory_id: d[1] for d in duplicates}

        # 选择相似度最高的作为主记忆
        primary_id = max(duplicates, key=lambda x: x[1])[0].memory_id

        return DuplicateGroup(
            group_id=f"group_{int(time.time() * 1000)}",
            primary_memory_id=primary_id,
            duplicate_memory_ids=duplicate_ids,
            similarity_scores=similarity_scores
        )

    async def smart_merge_with_conflicts(
        self,
        memories: List[MemoryItem]
    ) -> Tuple[MemoryItem, List[Dict[str, Any]]]:
        """
        智能合并，检测并报告冲突

        Returns:
            (merged_memory, conflicts)
        """
        if len(memories) < 2:
            return memories[0] if memories else None, []

        # 分析内容差异
        conflicts = []
        contents = [m.content for m in memories]

        # 检测内容冲突（简化：检查是否有明显矛盾的关键词）
        conflict_keywords = ["不是", "没有", "无", "否定"]
        for i, mem1 in enumerate(memories):
            for j, mem2 in enumerate(memories[i+1:], i+1):
                # 检查是否有否定关系
                has_negation1 = any(kw in mem1.content for kw in conflict_keywords)
                has_negation2 = any(kw in mem2.content for kw in conflict_keywords)

                if has_negation1 != has_negation2:
                    conflicts.append({
                        "memory_1": mem1.memory_id,
                        "memory_2": mem2.memory_id,
                        "type": "potential_negation_conflict",
                        "description": "检测到可能的否定冲突"
                    })

        # 执行合并
        merged, _ = await self.merge_duplicates(memories, strategy="ensemble")

        return merged, conflicts

    def update_similarity_threshold(self, threshold: float):
        """更新相似度阈值"""
        self.similarity_threshold = threshold

    async def find_similar_groups(
        self,
        memories: List[MemoryItem],
        group_threshold: float = None
    ) -> List[List[MemoryItem]]:
        """
        查找相似记忆组

        Args:
            memories: 记忆列表
            group_threshold: 分组阈值（默认使用 similarity_threshold）

        Returns:
            相似记忆组列表
        """
        threshold = group_threshold or self.similarity_threshold

        # 计算所有对的相似度
        n = len(memories)
        similarity_matrix = [[0.0] * n for _ in range(n)]

        for i in range(n):
            for j in range(i + 1, n):
                emb_i = memories[i].embedding
                emb_j = memories[j].embedding

                if emb_i is None:
                    emb_i = await self.embedding.embed(memories[i].content)
                if emb_j is None:
                    emb_j = await self.embedding.embed(memories[j].content)

                sim = await self.embedding.similarity(emb_i, emb_j)
                similarity_matrix[i][j] = sim
                similarity_matrix[j][i] = sim

        # 使用并查集找到相似组
        parent = list(range(n))

        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        # 合并相似度超过阈值的记忆
        for i in range(n):
            for j in range(i + 1, n):
                if similarity_matrix[i][j] >= threshold:
                    union(i, j)

        # 收集组
        groups = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(memories[i])

        # 返回包含多个记忆的组
        return [g for g in groups.values() if len(g) > 1]


class RealTimeDeduplicationMiddleware:
    """实时去重中间件 - 在写入时自动检查重复"""

    def __init__(
        self,
        dedup_system: MemoryDeduplicationSystem,
        auto_merge: bool = False,
        on_duplicate_callback: Optional[callable] = None
    ):
        self.dedup_system = dedup_system
        self.auto_merge = auto_merge
        self.on_duplicate_callback = on_duplicate_callback

    async def process_memory(
        self,
        memory: MemoryItem
    ) -> Tuple[MemoryItem, bool]:
        """
        处理记忆写入

        Returns:
            (memory_to_store, is_duplicate)
        """
        duplicate_group = await self.dedup_system.check_duplicate(memory)

        if duplicate_group is None:
            # 没有重复
            return memory, False

        # 发现重复
        if self.on_duplicate_callback:
            await self.on_duplicate_callback(memory, duplicate_group)

        if self.auto_merge:
            # 获取所有重复记忆
            duplicate_memories = []
            if self.dedup_system.storage:
                for mem_id in [duplicate_group.primary_memory_id] + duplicate_group.duplicate_memory_ids:
                    mem = await self.dedup_system.storage.get(mem_id)
                    if mem:
                        duplicate_memories.append(mem)

            if duplicate_memories:
                merged, _ = await self.dedup_system.merge_duplicates(
                    duplicate_memories + [memory],
                    strategy="ensemble"
                )
                return merged, True

        return memory, True

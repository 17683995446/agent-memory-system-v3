"""
内存存储实现
用于测试和小规模场景
"""

from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING
import logging

logger = logging.getLogger(__name__)
import asyncio
import time

from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, MemoryLink
from memory_system.core_abstractions.component_registry_manager import register_storage
from memory_system.core_abstractions.filter_utility_functions import matches_filters, STORAGE_SYSTEM_KEYS
from memory_system.core_abstractions.system_constants_definitions import DEFAULT_MAX_MEMORIES, DEFAULT_EMBEDDING_DIMENSION

if TYPE_CHECKING:
    from memory_system.configuration_settings.memory_system_configuration import StorageConfig


@register_storage("memory")
class MemoryStorage(StorageBackend):
    """
    内存存储后端

    特点:
        - 纯内存存储，速度快
        - 适合测试和小规模场景
        - 不支持持久化

    DEFECT-R9-003 [P1] 修复:
        - 增强异常处理
        - 添加数据一致性检查
        - 添加恢复机制
    """

    def __init__(self, max_memories: int = DEFAULT_MAX_MEMORIES):
        self.max_memories = max_memories
        if max_memories < 1:
            raise ValueError(f"max_memories must be >= 1, got {max_memories}")
        self._memories: Dict[str, MemoryItem] = {}
        self._episodic_ids: List[str] = []
        self._semantic_ids: List[str] = []
        self._links: Dict[str, List[MemoryLink]] = {}  # source_id -> [MemoryLink]
        self._inverted_index: Dict[str, Set[str]] = {}  # token -> set of memory_ids
        self._doc_lengths: Dict[str, int] = {}  # memory_id -> token count
        self._lock = asyncio.Lock()

        # DEFECT-R9-003 [P1] 修复: 异常恢复相关
        self._error_count = 0
        self._max_errors = 10  # 最大错误次数，超过后触发恢复
        self._last_error: Optional[Exception] = None
        self._recovery_attempts = 0
        self._is_recovering = False
    
    async def add(self, item: MemoryItem) -> str:
        """
        添加记忆

        DEFECT-R9-003 [P1] 修复:
            - 增强异常处理
            - 添加错误计数和恢复
        """
        try:
            # DEFECT-R9-003 [P1] 修复: 检查是否需要恢复
            if self._should_recover():
                await self._attempt_recovery()

            async with self._lock:
                self._memories[item.memory_id] = item

                if item.memory_type == MemoryType.EPISODIC:
                    self._episodic_ids.append(item.memory_id)
                elif item.memory_type == MemoryType.SEMANTIC:
                    self._semantic_ids.append(item.memory_id)

                # 维护倒排索引
                from memory_system.core_abstractions.mathematical_utility_functions import tokenize
                tokens = set(tokenize(item.content))
                self._doc_lengths[item.memory_id] = len(tokens)
                for token in tokens:
                    if token not in self._inverted_index:
                        self._inverted_index[token] = set()
                    self._inverted_index[token].add(item.memory_id)

                # 检查容量限制
                if len(self._memories) > self.max_memories:
                    await self._evict_oldest()

            # DEFECT-R9-003 [P1] 修复: 成功操作后重置错误计数
            self._error_count = 0
            return item.memory_id

        except Exception as e:
            self._handle_error(e, "add")
            raise

    def _handle_error(self, error: Exception, operation: str) -> None:
        """
        处理错误

        DEFECT-R9-003 [P1] 修复: 错误处理和计数
        """
        self._error_count += 1
        self._last_error = error
        logger.error(f"Storage error in {operation}: {error} (error count: {self._error_count})")

        if self._error_count >= self._max_errors:
            logger.warning(f"Max errors ({self._max_errors}) reached, recovery may be needed")

    def _should_recover(self) -> bool:
        """
        检查是否需要恢复

        DEFECT-R9-003 [P1] 修复: 恢复判断逻辑
        """
        return self._error_count >= self._max_errors and not self._is_recovering

    async def _attempt_recovery(self) -> bool:
        """
        尝试恢复存储状态

        DEFECT-R9-003 [P1] 修复: 实现恢复逻辑

        Returns:
            是否恢复成功
        """
        if self._is_recovering:
            return False

        self._is_recovering = True
        self._recovery_attempts += 1
        logger.info(f"Attempting storage recovery (attempt {self._recovery_attempts})")

        try:
            async with self._lock:
                # 检查数据一致性
                await self._check_consistency()

                # 清理无效数据
                await self._cleanup_invalid_data()

            # 重置错误状态
            self._error_count = 0
            self._last_error = None
            logger.info("Storage recovery completed successfully")
            return True

        except Exception as e:
            logger.error(f"Storage recovery failed: {e}")
            return False
        finally:
            self._is_recovering = False

    async def _check_consistency(self) -> None:
        """
        检查数据一致性

        DEFECT-R9-003 [P1] 修复: 一致性检查
        """
        # 检查索引一致性
        invalid_ids = []
        for memory_id in self._episodic_ids:
            if memory_id not in self._memories:
                invalid_ids.append(memory_id)
        for memory_id in invalid_ids:
            self._episodic_ids.remove(memory_id)
            logger.warning(f"Removed invalid episodic id: {memory_id}")

        invalid_ids = []
        for memory_id in self._semantic_ids:
            if memory_id not in self._memories:
                invalid_ids.append(memory_id)
        for memory_id in invalid_ids:
            self._semantic_ids.remove(memory_id)
            logger.warning(f"Removed invalid semantic id: {memory_id}")

    async def _cleanup_invalid_data(self) -> None:
        """
        清理无效数据

        DEFECT-R9-003 [P1] 修复: 数据清理
        """
        # 清理无效的倒排索引
        invalid_tokens = []
        for token, memory_ids in self._inverted_index.items():
            valid_ids = {mid for mid in memory_ids if mid in self._memories}
            if valid_ids != memory_ids:
                self._inverted_index[token] = valid_ids
                if not valid_ids:
                    invalid_tokens.append(token)

        for token in invalid_tokens:
            del self._inverted_index[token]
            logger.debug(f"Removed empty inverted index: {token}")

        # 清理无效的文档长度记录
        invalid_doc_lengths = [
            mid for mid in self._doc_lengths
            if mid not in self._memories
        ]
        for mid in invalid_doc_lengths:
            del self._doc_lengths[mid]
    
    async def get(self, memory_id: str) -> Optional[MemoryItem]:
        """
        获取记忆

        DEFECT-R9-003 [P1] 修复: 增强异常处理
        """
        try:
            async with self._lock:
                return self._memories.get(memory_id)
        except Exception as e:
            self._handle_error(e, "get")
            raise
    
    async def search_by_vector(
        self,
        query_vector: List[float],
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[MemoryItem, float]]:
        """向量相似度搜索"""
        async with self._lock:
            from memory_system.core_abstractions.mathematical_utility_functions import cosine_similarity

            results = []

            for memory in self._memories.values():
                if memory.embedding is None:
                    continue

                # 应用过滤条件
                if filters and not matches_filters(memory, filters):
                    continue

                # 计算余弦相似度
                similarity = cosine_similarity(query_vector, memory.embedding)
                results.append((memory, float(similarity)))

            # 排序并返回top_k
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:top_k]
    
    async def search_by_time_range(
        self,
        start_time: float,
        end_time: float,
        memory_type: Optional[str] = None
    ) -> List[MemoryItem]:
        """按时间范围搜索"""
        async with self._lock:
            results = []
            for memory in self._memories.values():
                if start_time <= memory.timestamp <= end_time:
                    if memory_type is None or memory.memory_type.value == memory_type:
                        results.append(memory)

            return sorted(results, key=lambda x: x.timestamp, reverse=True)
    
    async def delete(self, memory_id: str) -> bool:
        """删除记忆"""
        async with self._lock:
            if memory_id in self._memories:
                memory = self._memories[memory_id]
                del self._memories[memory_id]

                # 从索引中移除
                if memory.memory_type == MemoryType.EPISODIC and memory_id in self._episodic_ids:
                    self._episodic_ids.remove(memory_id)
                elif memory.memory_type == MemoryType.SEMANTIC and memory_id in self._semantic_ids:
                    self._semantic_ids.remove(memory_id)

                # 清理倒排索引
                if memory_id in self._doc_lengths:
                    del self._doc_lengths[memory_id]
                for token in list(self._inverted_index.keys()):
                    self._inverted_index[token].discard(memory_id)
                    if not self._inverted_index[token]:
                        del self._inverted_index[token]

                return True
            return False
    
    async def batch_delete(self, memory_ids: List[str]) -> int:
        """
        批量删除记忆（优化实现：单次获取锁）
        
        Args:
            memory_ids: 记忆ID列表
            
        Returns:
            成功删除的数量
        """
        async with self._lock:
            count = 0
            for memory_id in memory_ids:
                if memory_id in self._memories:
                    memory = self._memories[memory_id]
                    del self._memories[memory_id]

                    # 从索引中移除
                    if memory.memory_type == MemoryType.EPISODIC and memory_id in self._episodic_ids:
                        self._episodic_ids.remove(memory_id)
                    elif memory.memory_type == MemoryType.SEMANTIC and memory_id in self._semantic_ids:
                        self._semantic_ids.remove(memory_id)

                    # 清理倒排索引
                    if memory_id in self._doc_lengths:
                        del self._doc_lengths[memory_id]
                    for token in list(self._inverted_index.keys()):
                        self._inverted_index[token].discard(memory_id)
                        if not self._inverted_index[token]:
                            del self._inverted_index[token]

                    count += 1
            return count
    
    async def count(self, memory_type: Optional[str] = None) -> int:
        """统计记忆数量"""
        async with self._lock:
            if memory_type is None:
                return len(self._memories)

            return sum(
                1 for m in self._memories.values()
                if m.memory_type.value == memory_type
            )
    
    async def close(self):
        """
        关闭连接

        DEFECT-R9-003 [P1] 修复: 安全关闭并清理状态
        """
        try:
            async with self._lock:
                self._memories.clear()
                self._episodic_ids.clear()
                self._semantic_ids.clear()
                self._links.clear()
                self._inverted_index.clear()
                self._doc_lengths.clear()

            # DEFECT-R9-003 [P1] 修复: 重置错误状态
            self._error_count = 0
            self._last_error = None
            self._recovery_attempts = 0
            self._is_recovering = False

        except Exception as e:
            logger.error(f"Error during storage close: {e}")
            raise

    def get_health_status(self) -> Dict[str, Any]:
        """
        获取存储健康状态

        DEFECT-R9-003 [P1] 修复: 健康状态监控

        Returns:
            健康状态信息
        """
        return {
            "error_count": self._error_count,
            "max_errors": self._max_errors,
            "recovery_attempts": self._recovery_attempts,
            "is_recovering": self._is_recovering,
            "memory_count": len(self._memories),
            "episodic_count": len(self._episodic_ids),
            "semantic_count": len(self._semantic_ids),
            "last_error": str(self._last_error) if self._last_error else None,
            "healthy": self._error_count < self._max_errors,
        }
    
    # ---- 图关联操作 ----
    
    async def add_link(self, link: MemoryLink) -> bool:
        """
        添加记忆关联链接
        
        Args:
            link: 记忆关联链接
            
        Returns:
            是否添加成功
        """
        async with self._lock:
            source_id = link.source_id
            target_id = link.target_id
            if source_id not in self._links:
                self._links[source_id] = []
            self._links[source_id].append(link)
            # 同时添加反向索引
            if target_id not in self._links:
                self._links[target_id] = []
            reverse_link = MemoryLink(
                source_id=target_id,
                target_id=source_id,
                relation=link.relation,
                weight=link.weight,
                metadata=link.metadata,
                created_at=link.created_at,
            )
            self._links[target_id].append(reverse_link)
            return True
    
    async def remove_link(self, source_id: str, target_id: str) -> bool:
        """
        移除记忆关联链接
        
        Args:
            source_id: 源记忆ID
            target_id: 目标记忆ID
            
        Returns:
            是否移除成功
        """
        async with self._lock:
            removed = False
            # 移除正向
            if source_id in self._links:
                original_len = len(self._links[source_id])
                self._links[source_id] = [
                    l for l in self._links[source_id]
                    if not (l.source_id == source_id and l.target_id == target_id)
                ]
                if len(self._links[source_id]) < original_len:
                    removed = True
            # 移除反向
            if target_id in self._links:
                original_len = len(self._links[target_id])
                self._links[target_id] = [
                    l for l in self._links[target_id]
                    if not (l.source_id == target_id and l.target_id == source_id)
                ]
                if len(self._links[target_id]) < original_len:
                    removed = True
            return removed
    
    async def get_linked_memories(
        self,
        memory_id: str,
        relation: Optional[str] = None,
        max_depth: int = 1
    ) -> List[MemoryItem]:
        """
        获取关联记忆（BFS多跳遍历）
        
        Args:
            memory_id: 起始记忆ID
            relation: 关联类型过滤 (None=全部)
            max_depth: 最大遍历深度
            
        Returns:
            关联记忆列表
        """
        async with self._lock:
            visited = {memory_id}
            queue = [(memory_id, 0)]
            linked_ids = []
            
            while queue:
                current_id, depth = queue.pop(0)
                if depth >= max_depth:
                    continue
                
                neighbors = self._links.get(current_id, [])
                for link in neighbors:
                    neighbor_id = link.target_id
                    if neighbor_id in visited:
                        continue
                    if relation and link.relation.value != relation:
                        continue
                    visited.add(neighbor_id)
                    linked_ids.append(neighbor_id)
                    queue.append((neighbor_id, depth + 1))
            
            result = []
            for mid in linked_ids:
                if mid in self._memories:
                    result.append(self._memories[mid])
            return result
    
    async def get_links(
        self,
        memory_id: str,
        direction: str = "both"
    ) -> List[MemoryLink]:
        """
        获取指定记忆的所有关联链接
        
        Args:
            memory_id: 记忆ID
            direction: 方向 ("outgoing"/"incoming"/"both")
            
        Returns:
            关联链接列表
        """
        async with self._lock:
            all_links = self._links.get(memory_id, [])
            result = []
            for link in all_links:
                if direction == "outgoing":
                    if link.source_id == memory_id:
                        result.append(link)
                elif direction == "incoming":
                    if link.source_id == memory_id:
                        result.append(link)
                else:  # both
                    result.append(link)
            return result
    
    async def list_memories(
        self,
        memory_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        ascending: bool = False
    ) -> List[MemoryItem]:
        """
        分页列出记忆（优化实现：直接操作内存数据结构）
        
        Args:
            memory_type: 记忆类型过滤 (episodic/semantic/None=全部)
            limit: 返回数量上限
            offset: 跳过数量
            order_by: 排序字段 (created_at/importance/access_count)
            ascending: 是否升序
            
        Returns:
            记忆项列表
        """
        async with self._lock:
            # 按类型过滤
            if memory_type is not None:
                if memory_type == MemoryType.EPISODIC.value:
                    candidates = [self._memories[mid] for mid in self._episodic_ids if mid in self._memories]
                elif memory_type == MemoryType.SEMANTIC.value:
                    candidates = [self._memories[mid] for mid in self._semantic_ids if mid in self._memories]
                else:
                    candidates = [
                        m for m in self._memories.values()
                        if m.memory_type.value == memory_type
                    ]
            else:
                candidates = list(self._memories.values())
            
            # 排序
            reverse = not ascending
            if order_by == "created_at":
                candidates.sort(key=lambda m: m.timestamp, reverse=reverse)
            elif order_by == "importance":
                candidates.sort(key=lambda m: m.importance.value, reverse=reverse)
            elif order_by == "access_count":
                candidates.sort(key=lambda m: m.access_count, reverse=reverse)
            
            # 分页
            return candidates[offset:offset + limit]
    
    async def _evict_oldest(self):
        """淘汰最旧的记忆（调用方需持有 self._lock）"""
        if not self._memories:
            return

        # 找到最旧的记忆
        oldest_id = min(
            self._memories.keys(),
            key=lambda k: self._memories[k].timestamp
        )
        memory = self._memories[oldest_id]
        del self._memories[oldest_id]

        # 从索引中移除
        if memory.memory_type == MemoryType.EPISODIC and oldest_id in self._episodic_ids:
            self._episodic_ids.remove(oldest_id)
        elif memory.memory_type == MemoryType.SEMANTIC and oldest_id in self._semantic_ids:
            self._semantic_ids.remove(oldest_id)

        # 清理倒排索引
        if oldest_id in self._doc_lengths:
            del self._doc_lengths[oldest_id]
        for token in list(self._inverted_index.keys()):
            self._inverted_index[token].discard(oldest_id)
            if not self._inverted_index[token]:
                del self._inverted_index[token]

    async def search_by_keyword(
        self,
        query_terms: List[str],
        top_k: int = 10,
        memory_type: Optional[str] = None
    ) -> List[Tuple[MemoryItem, float]]:
        from memory_system.core_abstractions.mathematical_utility_functions import bm25_score, tokenize

        # Get candidate memory IDs from inverted index
        candidate_ids: Set[str] = set()
        for term in query_terms:
            term_lower = term.lower()
            # Also check tokenized form
            for token in self._inverted_index:
                if term_lower in token or token in term_lower:
                    candidate_ids.update(self._inverted_index[token])

        if not candidate_ids:
            return []

        # Filter by memory type
        if memory_type:
            type_ids = set()
            if memory_type == "episodic":
                type_ids = set(self._episodic_ids)
            elif memory_type == "semantic":
                type_ids = set(self._semantic_ids)
            candidate_ids &= type_ids

        # Calculate BM25 scores
        doc_count = len(self._memories)
        avg_doc_length = sum(self._doc_lengths.values()) / max(doc_count, 1)

        scored: List[Tuple[MemoryItem, float]] = []
        for memory_id in candidate_ids:
            memory = self._memories.get(memory_id)
            if not memory:
                continue

            doc_tokens = set(tokenize(memory.content))
            doc_length = self._doc_lengths.get(memory_id, 1)

            total_score = 0.0
            for term in query_terms:
                term_lower = term.lower()
                # Count term frequency in document
                tf = sum(1 for t in doc_tokens if term_lower in t or t in term_lower)
                # Count document frequency
                df = 0
                for idx_token in self._inverted_index:
                    if term_lower in idx_token or idx_token in term_lower:
                        df += 1

                total_score += bm25_score(
                    term_freq=tf,
                    doc_length=doc_length,
                    avg_doc_length=avg_doc_length,
                    doc_count=doc_count,
                    doc_freq=df,
                )

            if total_score > 0:
                scored.append((memory, total_score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]

    @classmethod
    def from_config(
        cls,
        config: "StorageConfig",
        embedding_dim: int = DEFAULT_EMBEDDING_DIMENSION
    ) -> "MemoryStorage":
        return cls(max_memories=config.max_memories)

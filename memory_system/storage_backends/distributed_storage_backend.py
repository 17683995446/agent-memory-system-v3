"""
分布式存储后端 - 基于一致性哈希的数据分片
支持水平扩展，线性扩展能力
"""

import hashlib
import bisect
import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
import asyncio
from collections import defaultdict
import time

from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, MemoryLink
from memory_system.exceptions import StorageError

# 配置日志
logger = logging.getLogger(__name__)


@dataclass
class StorageNode:
    """存储节点"""
    node_id: str
    host: str
    port: int
    weight: int = 1
    is_healthy: bool = True
    last_heartbeat: float = 0.0


class ConsistentHashRing:
    """一致性哈希环"""

    def __init__(self, replicas: int = 150):
        self.replicas = replicas
        self.ring: Dict[int, StorageNode] = {}
        self.sorted_keys: List[int] = []
        self.nodes: Dict[str, StorageNode] = {}

    def _hash(self, key: str) -> int:
        """计算key的hash值"""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node: StorageNode):
        """添加节点到哈希环"""
        self.nodes[node.node_id] = node
        for i in range(self.replicas * node.weight):
            virtual_key = self._hash(f"{node.node_id}:{i}")
            self.ring[virtual_key] = node
        self.sorted_keys = sorted(self.ring.keys())

    def remove_node(self, node_id: str):
        """从哈希环移除节点"""
        if node_id not in self.nodes:
            return
        node = self.nodes.pop(node_id)
        for i in range(self.replicas * node.weight):
            virtual_key = self._hash(f"{node_id}:{i}")
            if virtual_key in self.ring:
                del self.ring[virtual_key]
        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key: str) -> Optional[StorageNode]:
        """获取key对应的节点"""
        if not self.ring:
            return None
        hash_key = self._hash(key)
        idx = bisect.bisect_right(self.sorted_keys, hash_key)
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]

    def get_nodes(self, key: str, n: int = 3) -> List[StorageNode]:
        """获取key对应的n个节点（用于副本）"""
        if not self.ring:
            return []
        hash_key = self._hash(key)
        idx = bisect.bisect_right(self.sorted_keys, hash_key)
        nodes = []
        seen = set()
        while len(nodes) < n and len(seen) < len(self.nodes):
            if idx >= len(self.sorted_keys):
                idx = 0
            node = self.ring[self.sorted_keys[idx]]
            if node.node_id not in seen:
                nodes.append(node)
                seen.add(node.node_id)
            idx += 1
        return nodes

    def get_all_healthy_nodes(self) -> List[StorageNode]:
        """获取所有健康节点"""
        return [node for node in self.nodes.values() if node.is_healthy]

    def update_node_health(self, node_id: str, is_healthy: bool):
        """更新节点健康状态"""
        if node_id in self.nodes:
            self.nodes[node_id].is_healthy = is_healthy
            if is_healthy:
                self.nodes[node_id].last_heartbeat = time.time()


class DistributedStorageBackend(StorageBackend):
    """分布式存储后端"""

    def __init__(
        self,
        nodes: List[StorageNode],
        replication_factor: int = 3,
        read_quorum: int = 2,
        write_quorum: int = 2,
        default_timeout: float = 5.0
    ):
        self.hash_ring = ConsistentHashRing()
        for node in nodes:
            self.hash_ring.add_node(node)

        self.replication_factor = min(replication_factor, len(nodes))
        self.read_quorum = min(read_quorum, self.replication_factor)
        self.write_quorum = min(write_quorum, self.replication_factor)
        self.default_timeout = default_timeout

        # 本地存储缓存（实际应使用RPC连接到远程节点）
        self._local_stores: Dict[str, StorageBackend] = {}
        self._node_health: Dict[str, bool] = {}
        self._lock = asyncio.Lock()

        # 统计信息
        self._stats = {
            "writes": 0,
            "reads": 0,
            "write_failures": 0,
            "read_failures": 0,
            "quorum_failures": 0
        }

    async def add(self, item: MemoryItem) -> str:
        """添加记忆到分布式存储"""
        nodes = self.hash_ring.get_nodes(item.memory_id, self.replication_factor)

        if not nodes:
            raise StorageError("No available storage nodes")

        # 并行写入所有副本
        write_tasks = []
        for node in nodes:
            if node.is_healthy:
                store = self._get_store(node)
                write_tasks.append(self._write_with_timeout(store, item))

        if not write_tasks:
            raise StorageError("No healthy nodes available for write")

        # 等待达到写仲裁
        success_count = 0
        exceptions = []
        for task in asyncio.as_completed(write_tasks):
            try:
                await task
                success_count += 1
                if success_count >= self.write_quorum:
                    break
            except Exception as e:
                exceptions.append(e)

        async with self._lock:
            self._stats["writes"] += 1
            if success_count < self.write_quorum:
                self._stats["write_failures"] += 1
                self._stats["quorum_failures"] += 1

        if success_count < self.write_quorum:
            raise StorageError(
                f"Write quorum not reached: {success_count}/{self.write_quorum}"
            )

        return item.memory_id

    async def get(self, memory_id: str) -> Optional[MemoryItem]:
        """从分布式存储获取记忆"""
        nodes = self.hash_ring.get_nodes(memory_id, self.replication_factor)

        if not nodes:
            raise StorageError("No available storage nodes")

        # 并行读取所有副本
        read_tasks = []
        for node in nodes:
            if node.is_healthy:
                store = self._get_store(node)
                read_tasks.append(self._read_with_timeout(store, memory_id))

        if not read_tasks:
            raise StorageError("No healthy nodes available for read")

        # 等待达到读仲裁
        results = []
        exceptions = []
        for task in asyncio.as_completed(read_tasks):
            try:
                result = await task
                if result is not None:
                    results.append(result)
                    if len(results) >= self.read_quorum:
                        break
            except Exception as e:
                exceptions.append(e)

        async with self._lock:
            self._stats["reads"] += 1
            if not results:
                self._stats["read_failures"] += 1

        if not results:
            return None

        # 返回最新的版本（基于timestamp）
        return max(results, key=lambda x: x.timestamp)

    async def search_by_vector(
        self,
        query_vector: List[float],
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[MemoryItem, float]]:
        """向量相似度搜索 - 在所有节点上搜索并合并结果
        
        DEFECT-R3-001 [P0] 修复: 异常不再被静默忽略，而是记录并传播严重错误
        """
        all_results = []
        exceptions = []

        # 在所有健康节点上并行搜索
        search_tasks = []
        healthy_nodes = self.hash_ring.get_all_healthy_nodes()

        if not healthy_nodes:
            raise StorageError("No healthy nodes available for search")

        for node in healthy_nodes:
            store = self._get_store(node)
            search_tasks.append(
                self._search_with_timeout(store, query_vector, top_k, filters)
            )

        # 收集结果
        for task in asyncio.as_completed(search_tasks):
            try:
                results = await task
                all_results.extend(results)
            except Exception as e:
                # DEFECT-R3-001: 记录异常而非静默忽略
                exceptions.append(e)
                logger.warning(f"Search task failed on node: {e}")
                continue

        # DEFECT-R3-001: 如果所有任务都失败，传播异常
        if not all_results and exceptions:
            raise StorageError(f"All search tasks failed: {exceptions[0]}")

        # 去重并按相似度排序
        seen_ids = set()
        unique_results = []
        for item, score in sorted(all_results, key=lambda x: x[1], reverse=True):
            if item.memory_id not in seen_ids:
                unique_results.append((item, score))
                seen_ids.add(item.memory_id)
                if len(unique_results) >= top_k:
                    break

        return unique_results

    async def search_by_time_range(
        self,
        start_time: float,
        end_time: float,
        memory_type: Optional[str] = None
    ) -> List[MemoryItem]:
        """按时间范围搜索 - 在所有节点上搜索并合并结果
        
        DEFECT-R3-002 [P0] 修复: 异常不再被静默忽略
        """
        all_results = []
        exceptions = []

        # 在所有健康节点上并行搜索
        search_tasks = []
        healthy_nodes = self.hash_ring.get_all_healthy_nodes()

        if not healthy_nodes:
            raise StorageError("No healthy nodes available for time range search")

        for node in healthy_nodes:
            store = self._get_store(node)
            search_tasks.append(
                self._time_search_with_timeout(store, start_time, end_time, memory_type)
            )

        # 收集结果
        for task in asyncio.as_completed(search_tasks):
            try:
                results = await task
                all_results.extend(results)
            except Exception as e:
                # DEFECT-R3-002: 记录异常
                exceptions.append(e)
                logger.warning(f"Time range search task failed: {e}")
                continue

        # DEFECT-R3-002: 如果所有任务都失败，传播异常
        if not all_results and exceptions:
            raise StorageError(f"All time range search tasks failed: {exceptions[0]}")

        # 去重并排序
        seen_ids = set()
        unique_results = []
        for item in sorted(all_results, key=lambda x: x.timestamp, reverse=True):
            if item.memory_id not in seen_ids:
                unique_results.append(item)
                seen_ids.add(item.memory_id)

        return unique_results

    async def delete(self, memory_id: str) -> bool:
        """删除记忆 - 从所有副本删除
        
        DEFECT-R3-003 [P0] 修复: 删除异常处理增强
        """
        nodes = self.hash_ring.get_nodes(memory_id, self.replication_factor)

        if not nodes:
            raise StorageError(f"No nodes available for delete operation: {memory_id}")

        # 并行删除所有副本
        delete_tasks = []
        for node in nodes:
            if node.is_healthy:
                store = self._get_store(node)
                delete_tasks.append(self._delete_with_timeout(store, memory_id))

        if not delete_tasks:
            raise StorageError(f"No healthy nodes available for delete: {memory_id}")

        # 等待删除完成，至少一个成功
        success_count = 0
        exceptions = []
        for task in asyncio.as_completed(delete_tasks):
            try:
                result = await task
                if result:
                    success_count += 1
            except Exception as e:
                # DEFECT-R3-003: 记录异常
                exceptions.append(e)
                logger.warning(f"Delete task failed on node: {e}")
                continue

        # DEFECT-R3-003: 如果所有删除都失败，传播异常
        if success_count == 0 and exceptions:
            raise StorageError(f"All delete tasks failed for {memory_id}: {exceptions[0]}")

        return success_count > 0

    async def count(self, memory_type: Optional[str] = None) -> int:
        """统计记忆数量 - 在所有节点上统计
        
        DEFECT-R3-004 [P0] 修复: 计数异常处理增强
        """
        total = 0
        exceptions = []

        healthy_nodes = self.hash_ring.get_all_healthy_nodes()

        if not healthy_nodes:
            raise StorageError("No healthy nodes available for count operation")

        count_tasks = []

        for node in healthy_nodes:
            store = self._get_store(node)
            count_tasks.append(self._count_with_timeout(store, memory_type))

        for task in asyncio.as_completed(count_tasks):
            try:
                count = await task
                total += count
            except Exception as e:
                # DEFECT-R3-004: 记录异常
                exceptions.append(e)
                logger.warning(f"Count task failed on node: {e}")
                continue

        # DEFECT-R3-004: 如果所有计数都失败，传播异常
        if total == 0 and exceptions and len(exceptions) == len(count_tasks):
            raise StorageError(f"All count tasks failed: {exceptions[0]}")

        return total

    async def close(self):
        """关闭所有存储连接"""
        close_tasks = []
        for store in self._local_stores.values():
            close_tasks.append(store.close())

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        self._local_stores.clear()

    async def _write_with_timeout(
        self,
        store: StorageBackend,
        item: MemoryItem,
        timeout: float = None
    ):
        """带超时的写入"""
        timeout = timeout or self.default_timeout
        await asyncio.wait_for(store.add(item), timeout=timeout)

    async def _read_with_timeout(
        self,
        store: StorageBackend,
        memory_id: str,
        timeout: float = None
    ) -> Optional[MemoryItem]:
        """带超时的读取"""
        timeout = timeout or self.default_timeout
        return await asyncio.wait_for(store.get(memory_id), timeout=timeout)

    async def _search_with_timeout(
        self,
        store: StorageBackend,
        query_vector: List[float],
        top_k: int,
        filters: Optional[Dict[str, Any]],
        timeout: float = None
    ) -> List[Tuple[MemoryItem, float]]:
        """带超时的向量搜索"""
        timeout = timeout or self.default_timeout
        return await asyncio.wait_for(
            store.search_by_vector(query_vector, top_k, filters),
            timeout=timeout
        )

    async def _time_search_with_timeout(
        self,
        store: StorageBackend,
        start_time: float,
        end_time: float,
        memory_type: Optional[str],
        timeout: float = None
    ) -> List[MemoryItem]:
        """带超时的按时间搜索"""
        timeout = timeout or self.default_timeout
        return await asyncio.wait_for(
            store.search_by_time_range(start_time, end_time, memory_type),
            timeout=timeout
        )

    async def _delete_with_timeout(
        self,
        store: StorageBackend,
        memory_id: str,
        timeout: float = None
    ) -> bool:
        """带超时的删除"""
        timeout = timeout or self.default_timeout
        return await asyncio.wait_for(store.delete(memory_id), timeout=timeout)

    async def _count_with_timeout(
        self,
        store: StorageBackend,
        memory_type: Optional[str],
        timeout: float = None
    ) -> int:
        """带超时的计数"""
        timeout = timeout or self.default_timeout
        return await asyncio.wait_for(store.count(memory_type), timeout=timeout)

    def _get_store(self, node: StorageNode) -> StorageBackend:
        """获取节点的存储后端（实际应使用RPC）"""
        if node.node_id not in self._local_stores:
            from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
            self._local_stores[node.node_id] = MemoryStorage()
        return self._local_stores[node.node_id]

    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return self._stats.copy()

    def get_node_health(self) -> Dict[str, bool]:
        """获取节点健康状态"""
        return {
            node_id: node.is_healthy
            for node_id, node in self.hash_ring.nodes.items()
        }

    async def add_node(self, node: StorageNode):
        """动态添加节点"""
        async with self._lock:
            self.hash_ring.add_node(node)

    async def remove_node(self, node_id: str):
        """动态移除节点"""
        async with self._lock:
            self.hash_ring.remove_node(node_id)
            if node_id in self._local_stores:
                await self._local_stores[node_id].close()
                del self._local_stores[node_id]

    async def rebalance(self):
        """重新平衡数据（数据迁移）"""
        # 简化的重平衡逻辑：遍历所有节点，将不属于该节点的数据迁移
        # 实际生产环境中需要更复杂的逻辑
        pass

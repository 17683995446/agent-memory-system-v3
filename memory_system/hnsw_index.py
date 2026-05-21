"""
HNSW近似最近邻索引模块 (M-07)
实现分层可导航小世界图算法进行高效向量检索
"""

import random
import math
import heapq
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field

from memory_system.core_abstractions.mathematical_utility_functions import cosine_similarity
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class HNSWNode:
    """HNSW图中的节点"""
    id: str
    vector: List[float]
    level: int
    neighbors: Dict[int, List[str]] = field(default_factory=dict)  # level -> neighbor_ids


class HNSWIndex:
    """
    HNSW (Hierarchical Navigable Small World) 近似最近邻索引

    HNSW是一种基于图的近似最近邻搜索算法，通过构建多层图结构实现高效检索：

    特点:
        - 分层结构：上层稀疏，下层密集
        - 每层使用贪婪搜索找到最近邻
        - 支持增量添加，无需重建索引
        - 检索复杂度O(log n)，远优于线性扫描

    使用示例:
        >>> index = HNSWIndex(dim=128, m=16, ef_construction=200)
        >>> index.add("id1", vector1)
        >>> index.add("id2", vector2)
        >>> results = index.search(query_vector, k=10)
    """

    def __init__(
        self,
        dim: int,
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 64,
        m_max: Optional[int] = None,
        level_multiplier: float = 1.0 / math.log(2),
    ):
        """
        初始化HNSW索引

        Args:
            dim: 向量维度
            m: 每个节点的最大连接数
            ef_construction: 构建时的搜索范围
            ef_search: 搜索时的搜索范围
            m_max: 最大连接数（默认2*m）
            level_multiplier: 层级乘数，控制层级分布
        """
        self.dim = dim
        self.m = m
        self.m_max = m_max or 2 * m
        self.ef_construction = ef_construction
        self.ef_search = ef_search
        self.level_multiplier = level_multiplier

        # 存储所有节点
        self._nodes: Dict[str, HNSWNode] = {}
        # 入口点（最高层的节点）
        self._entry_point: Optional[str] = None
        # 当前最大层级
        self._max_level = 0

        # 距离函数（默认余弦相似度）
        self._distance_func: Callable[[List[float], List[float]], float] = self._cosine_distance

        # 统计
        self._stats = {
            "total_added": 0,
            "total_searches": 0,
            "total_comparisons": 0,
        }

    def _cosine_distance(self, vec1: List[float], vec2: List[float]) -> float:
        """计算余弦距离（1 - 相似度）"""
        sim = cosine_similarity(vec1, vec2)
        return 1.0 - sim

    def _euclidean_distance(self, vec1: List[float], vec2: List[float]) -> float:
        """计算欧氏距离"""
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))

    def _random_level(self) -> int:
        """
        生成随机层级

        使用指数分布决定新节点的层级
        """
        level = 0
        while random.random() < math.exp(-1.0 / self.level_multiplier) and level < 16:
            level += 1
        return level

    def _search_layer(
        self,
        query: List[float],
        entry_point: str,
        ef: int,
        level: int
    ) -> List[Tuple[float, str]]:
        """
        在指定层进行贪婪搜索

        Args:
            query: 查询向量
            entry_point: 入口点ID
            ef: 搜索范围
            level: 当前层级

        Returns:
            [(distance, node_id), ...] 最近邻列表
        """
        # 已访问集合
        visited: Set[str] = {entry_point}
        # 候选集（优先队列，按距离排序）
        entry_dist = self._distance_func(query, self._nodes[entry_point].vector)
        candidates: List[Tuple[float, str]] = [(entry_dist, entry_point)]
        # 结果集（最近邻）
        results: List[Tuple[float, str]] = [(entry_dist, entry_point)]

        self._stats["total_comparisons"] += 1

        while candidates:
            # 取出最近的候选
            current_dist, current_id = heapq.heappop(candidates)

            # 如果当前候选比结果中最远的还远，结束搜索
            if current_dist > results[-1][0]:
                break

            # 遍历邻居
            current_node = self._nodes[current_id]
            for neighbor_id in current_node.neighbors.get(level, []):
                if neighbor_id in visited:
                    continue
                visited.add(neighbor_id)

                neighbor_dist = self._distance_func(query, self._nodes[neighbor_id].vector)
                self._stats["total_comparisons"] += 1

                # 如果邻居比结果中最远的还近，加入候选和结果
                if neighbor_dist < results[-1][0] or len(results) < ef:
                    heapq.heappush(candidates, (neighbor_dist, neighbor_id))
                    heapq.heappush(results, (neighbor_dist, neighbor_id))
                    # 保持结果集大小为ef
                    if len(results) > ef:
                        results = results[:ef]

        return sorted(results)

    def add(self, id: str, vector: List[float]) -> None:
        """
        添加向量到索引

        Args:
            id: 向量ID
            vector: 向量数据
        """
        if len(vector) != self.dim:
            raise ValueError(f"向量维度不匹配: 期望 {self.dim}, 实际 {len(vector)}")

        if id in self._nodes:
            # 已存在，更新向量
            self._nodes[id].vector = vector
            return

        # 创建新节点
        level = self._random_level()
        new_node = HNSWNode(
            id=id,
            vector=vector,
            level=level,
            neighbors={}
        )

        # 如果是第一个节点
        if self._entry_point is None:
            self._nodes[id] = new_node
            self._entry_point = id
            self._max_level = level
            self._stats["total_added"] += 1
            return

        # 从最高层开始搜索
        curr_ep = self._entry_point
        curr_level = self._max_level

        # 找到插入层的入口点
        for l in range(curr_level, level, -1):
            nearest = self._search_layer(vector, curr_ep, 1, l)
            if nearest:
                curr_ep = nearest[0][1]

        # 从插入层向下建立连接
        for l in range(min(level, curr_level), -1, -1):
            nearest = self._search_layer(vector, curr_ep, self.ef_construction, l)

            # 选择最近的m个邻居
            m = self.m if l == 0 else self.m_max
            selected_neighbors = nearest[:m]

            # 建立双向连接
            new_node.neighbors[l] = [n[1] for n in selected_neighbors]

            for _, neighbor_id in selected_neighbors:
                neighbor_node = self._nodes[neighbor_id]
                if l not in neighbor_node.neighbors:
                    neighbor_node.neighbors[l] = []
                neighbor_node.neighbors[l].append(id)

                # 如果邻居连接过多，剪枝
                if len(neighbor_node.neighbors[l]) > self.m_max:
                    self._prune_neighbors(neighbor_id, l)

            # 更新入口点
            if nearest:
                curr_ep = nearest[0][1]

        self._nodes[id] = new_node

        # 更新全局入口点和最大层级
        if level > self._max_level:
            self._max_level = level
            self._entry_point = id

        self._stats["total_added"] += 1

    def _prune_neighbors(self, node_id: str, level: int) -> None:
        """
        剪枝邻居连接

        当节点连接数超过限制时，保留距离最近的连接
        """
        node = self._nodes[node_id]
        neighbors = node.neighbors[level]

        if len(neighbors) <= self.m_max:
            return

        # 计算到所有邻居的距离（跳过不存在的节点）
        distances = []
        for n in neighbors:
            if n in self._nodes:
                dist = self._distance_func(node.vector, self._nodes[n].vector)
                distances.append((dist, n))
        distances.sort()

        # 保留最近的m_max个
        node.neighbors[level] = [n for _, n in distances[:self.m_max]]

    def search(self, query: List[float], k: int = 10) -> List[Tuple[str, float]]:
        """
        搜索最近邻

        Args:
            query: 查询向量
            k: 返回结果数量

        Returns:
            [(id, similarity), ...] 按相似度排序
        """
        if not self._nodes:
            return []

        if len(query) != self.dim:
            raise ValueError(f"查询向量维度不匹配: 期望 {self.dim}, 实际 {len(query)}")

        self._stats["total_searches"] += 1

        # 小数据集直接使用暴力搜索
        if len(self._nodes) < 100:
            return self._brute_force_search(query, k)

        # 从最高层开始搜索
        curr_ep = self._entry_point
        curr_level = self._max_level

        # 贪心下降到最底层
        for l in range(curr_level, 0, -1):
            nearest = self._search_layer(query, curr_ep, 1, l)
            if nearest:
                curr_ep = nearest[0][1]

        # 在最底层进行完整搜索
        results = self._search_layer(query, curr_ep, self.ef_search, 0)

        # 转换为相似度并返回top-k
        return [
            (node_id, 1.0 - dist) for dist, node_id in results[:k]
        ]

    def _brute_force_search(self, query: List[float], k: int) -> List[Tuple[str, float]]:
        """暴力搜索（用于小数据集）"""
        scores = []
        for node_id, node in self._nodes.items():
            dist = self._distance_func(query, node.vector)
            scores.append((node_id, 1.0 - dist))
            self._stats["total_comparisons"] += 1

        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:k]

    def batch_add(self, items: List[Tuple[str, List[float]]]) -> None:
        """
        批量添加向量

        Args:
            items: [(id, vector), ...] 列表
        """
        for id, vector in items:
            self.add(id, vector)

    def delete(self, id: str) -> bool:
        """
        删除向量

        Args:
            id: 向量ID

        Returns:
            是否成功删除
        """
        if id not in self._nodes:
            return False

        node = self._nodes[id]

        # 从所有邻居的连接中移除
        for level, neighbors in node.neighbors.items():
            for neighbor_id in neighbors:
                if neighbor_id in self._nodes:
                    neighbor = self._nodes[neighbor_id]
                    if level in neighbor.neighbors and id in neighbor.neighbors[level]:
                        neighbor.neighbors[level].remove(id)

        # 删除节点
        del self._nodes[id]

        # 如果删除的是入口点，需要重新选择
        if id == self._entry_point:
            if self._nodes:
                # 选择层级最高的节点作为新入口点
                self._entry_point = max(
                    self._nodes.keys(),
                    key=lambda x: self._nodes[x].level
                )
                self._max_level = self._nodes[self._entry_point].level
            else:
                self._entry_point = None
                self._max_level = 0

        return True

    def clear(self) -> None:
        """清空索引"""
        self._nodes.clear()
        self._entry_point = None
        self._max_level = 0

    def get_stats(self) -> Dict[str, Any]:
        """获取索引统计"""
        avg_comparisons = (
            self._stats["total_comparisons"] / max(self._stats["total_searches"], 1)
        )
        return {
            **self._stats,
            "node_count": len(self._nodes),
            "max_level": self._max_level,
            "avg_comparisons_per_search": avg_comparisons,
        }


class SimpleANNIndex:
    """
    简化版近似最近邻索引

    基于倒排索引和局部敏感哈希(LSH)的简单实现
    适用于中等规模数据集
    """

    def __init__(self, dim: int, num_hash_tables: int = 8, hash_size: int = 16):
        """
        初始化简单ANN索引

        Args:
            dim: 向量维度
            num_hash_tables: 哈希表数量
            hash_size: 每个哈希表的哈希位数
        """
        self.dim = dim
        self.num_hash_tables = num_hash_tables
        self.hash_size = hash_size

        self._vectors: Dict[str, List[float]] = {}
        # 随机投影向量
        self._projections: List[List[List[float]]] = [
            [[random.gauss(0, 1) for _ in range(dim)] for _ in range(hash_size)]
            for _ in range(num_hash_tables)
        ]
        # 哈希表
        self._hash_tables: List[Dict[str, List[str]]] = [
            {} for _ in range(num_hash_tables)
        ]

    def _compute_hash(self, vector: List[float], table_idx: int) -> str:
        """计算LSH哈希值"""
        projections = self._projections[table_idx]
        bits = []
        for proj in projections:
            dot = sum(a * b for a, b in zip(vector, proj))
            bits.append('1' if dot > 0 else '0')
        return ''.join(bits)

    def add(self, id: str, vector: List[float]) -> None:
        """添加向量"""
        self._vectors[id] = vector

        for i in range(self.num_hash_tables):
            hash_val = self._compute_hash(vector, i)
            if hash_val not in self._hash_tables[i]:
                self._hash_tables[i][hash_val] = []
            self._hash_tables[i][hash_val].append(id)

    def search(self, query: List[float], k: int = 10) -> List[Tuple[str, float]]:
        """搜索最近邻"""
        if not self._vectors:
            return []

        # 收集候选
        candidates: Set[str] = set()
        for i in range(self.num_hash_tables):
            hash_val = self._compute_hash(query, i)
            candidates.update(self._hash_tables[i].get(hash_val, []))

        # 精确计算相似度
        scores = []
        for id in candidates:
            sim = cosine_similarity(query, self._vectors[id])
            scores.append((id, sim))

        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:k]

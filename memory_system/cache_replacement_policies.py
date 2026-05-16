"""
缓存替换策略模块 (M-01)
实现W-TinyLFU等高级缓存淘汰策略
"""

import time
from typing import Any, Dict, List, Optional, OrderedDict as OrderedDictType
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class CacheEntry:
    """缓存条目"""
    value: Any
    frequency: int = 1
    last_access: float = field(default_factory=time.time)


class WTinyLFUCache:
    """
    W-TinyLFU缓存淘汰策略实现

    W-TinyLFU (Window Tiny Least Frequently Used) 是一种高效的缓存淘汰算法，
    结合了LFU（最少使用）和LRU（最近最少使用）的优点：

    特点:
        - 使用窗口缓存处理突发访问
        - 使用主缓存存储长期热门数据
        - 使用频率草图（Count-Min Sketch）近似统计访问频率
        - 相比简单TTLCache，命中率更高

    使用示例:
        >>> cache = WTinyLFUCache(maxsize=512)
        >>> cache.put("key", value)
        >>> value = cache.get("key")
    """

    def __init__(
        self,
        maxsize: int = 512,
        window_size_ratio: float = 0.1,
        sketch_width: int = 1000,
        sketch_depth: int = 4,
    ):
        """
        初始化W-TinyLFU缓存

        Args:
            maxsize: 缓存最大容量
            window_size_ratio: 窗口缓存占总容量的比例
            sketch_width: 频率草图宽度
            sketch_depth: 频率草图深度（哈希函数数量）
        """
        self.maxsize = maxsize
        self.window_size = max(1, int(maxsize * window_size_ratio))
        self.main_size = maxsize - self.window_size

        # 窗口缓存（LRU）
        self._window: OrderedDictType[str, CacheEntry] = OrderedDict()
        # 主缓存（LFU）
        self._main: Dict[str, CacheEntry] = {}
        # 频率草图（Count-Min Sketch简化版）
        self._sketch_width = sketch_width
        self._sketch_depth = sketch_depth
        self._sketch: List[List[int]] = [
            [0] * sketch_width for _ in range(sketch_depth)
        ]

        # 访问计数
        self._access_count = 0
        self._hit_count = 0

    def _hash(self, key: str, seed: int) -> int:
        """简单的哈希函数"""
        hash_val = seed
        for char in key:
            hash_val = (hash_val * 31 + ord(char)) & 0xFFFFFFFF
        return hash_val % self._sketch_width

    def _increment_sketch(self, key: str) -> None:
        """增加频率草图计数"""
        for i in range(self._sketch_depth):
            idx = self._hash(key, i + 1)
            self._sketch[i][idx] = min(self._sketch[i][idx] + 1, 255)  # 防止溢出

    def _get_frequency(self, key: str) -> int:
        """从频率草图获取估计频率"""
        min_count = float('inf')
        for i in range(self._sketch_depth):
            idx = self._hash(key, i + 1)
            min_count = min(min_count, self._sketch[i][idx])
        return min_count

    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存值

        Args:
            key: 缓存键

        Returns:
            缓存值，不存在则返回None
        """
        self._access_count += 1

        # 先在窗口缓存中查找
        if key in self._window:
            entry = self._window[key]
            entry.frequency += 1
            entry.last_access = time.time()
            # 移动到末尾（LRU）
            self._window.move_to_end(key)
            self._hit_count += 1
            return entry.value

        # 在主缓存中查找
        if key in self._main:
            entry = self._main[key]
            entry.frequency += 1
            entry.last_access = time.time()
            self._increment_sketch(key)
            self._hit_count += 1
            return entry.value

        return None

    def put(self, key: str, value: Any) -> None:
        """
        放入缓存

        Args:
            key: 缓存键
            value: 缓存值
        """
        # 如果已存在，更新值
        if key in self._window:
            self._window[key].value = value
            self._window[key].last_access = time.time()
            return

        if key in self._main:
            self._main[key].value = value
            self._main[key].last_access = time.time()
            return

        # 检查是否需要淘汰
        if len(self._window) + len(self._main) >= self.maxsize:
            self._evict()

        # 先放入窗口缓存
        self._window[key] = CacheEntry(value=value)

        # 如果窗口缓存满了，尝试晋升到主缓存
        if len(self._window) > self.window_size:
            self._promote_from_window()

    def _promote_from_window(self) -> None:
        """从窗口缓存晋升到主缓存"""
        # 获取窗口中最老的条目
        oldest_key, oldest_entry = self._window.popitem(last=False)

        # 如果主缓存已满，需要淘汰
        if len(self._main) >= self.main_size:
            self._evict_from_main()

        # 晋升到主缓存
        self._main[oldest_key] = oldest_entry
        self._increment_sketch(oldest_key)

    def _evict(self) -> None:
        """执行淘汰"""
        # 优先淘汰窗口缓存中的条目
        if len(self._window) > 0:
            self._window.popitem(last=False)
        else:
            self._evict_from_main()

    def _evict_from_main(self) -> None:
        """从主缓存淘汰"""
        if not self._main:
            return

        # 找到频率最低的条目
        min_freq = float('inf')
        victim_key = None

        for key, entry in self._main.items():
            sketch_freq = self._get_frequency(key)
            if sketch_freq < min_freq:
                min_freq = sketch_freq
                victim_key = key

        if victim_key:
            del self._main[victim_key]

    def __contains__(self, key: str) -> bool:
        """检查键是否在缓存中"""
        return key in self._window or key in self._main

    def __len__(self) -> int:
        """获取缓存大小"""
        return len(self._window) + len(self._main)

    def keys(self) -> List[str]:
        """获取所有键"""
        return list(self._window.keys()) + list(self._main.keys())

    def clear(self) -> None:
        """清空缓存"""
        self._window.clear()
        self._main.clear()
        self._sketch = [
            [0] * self._sketch_width for _ in range(self._sketch_depth)
        ]

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        hit_rate = self._hit_count / max(self._access_count, 1)
        return {
            "size": len(self),
            "maxsize": self.maxsize,
            "window_size": len(self._window),
            "main_size": len(self._main),
            "access_count": self._access_count,
            "hit_count": self._hit_count,
            "hit_rate": hit_rate,
        }


class LRUCache:
    """
    简单LRU缓存实现

    作为W-TinyLFU的降级选项使用
    """

    def __init__(self, maxsize: int = 512):
        self.maxsize = maxsize
        self._cache: OrderedDictType[str, Any] = OrderedDict()
        self._access_count = 0
        self._hit_count = 0

    def get(self, key: str) -> Optional[Any]:
        self._access_count += 1
        if key in self._cache:
            self._hit_count += 1
            # 移动到末尾
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def put(self, key: str, value: Any) -> None:
        if key in self._cache:
            self._cache[key] = value
            self._cache.move_to_end(key)
            return

        if len(self._cache) >= self.maxsize:
            # 淘汰最老的
            self._cache.popitem(last=False)

        self._cache[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)

    def clear(self) -> None:
        self._cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        hit_rate = self._hit_count / max(self._access_count, 1)
        return {
            "size": len(self),
            "maxsize": self.maxsize,
            "access_count": self._access_count,
            "hit_count": self._hit_count,
            "hit_rate": hit_rate,
        }

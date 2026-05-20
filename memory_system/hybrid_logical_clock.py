"""
混合逻辑时钟 (Hybrid Logical Clock, HLC) 实现模块

结合物理时间和逻辑计数器，提供全序关系，同时处理时钟漂移问题。

核心特性:
- 物理时间 + 逻辑计数器的混合表示
- 自动处理时钟漂移
- 与NTP兼容
- 保证因果一致性
- 全序关系（可比较任意两个事件）

示例用法:
    >>> from hybrid_logical_clock import HybridLogicalClock
    >>> hlc = HybridLogicalClock(node_id="node1")
    >>> ts1 = hlc.now()
    >>> ts2 = hlc.now()
    >>> print(hlc.compare(ts1, ts2))
"""

import time
import json
import struct
import logging
from typing import Dict, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum, auto
import threading
import hashlib

logger = logging.getLogger(__name__)


class HLCComparisonResult(Enum):
    """HLC比较结果"""
    LESS = -1
    EQUAL = 0
    GREATER = 1


@dataclass(frozen=True)
class HLCTimestamp:
    """
    HLC时间戳
    
    不可变的数据类，表示一个混合逻辑时钟时间戳
    
    Attributes:
        physical: 物理时间（毫秒）
        logical: 逻辑计数器
        node_id: 节点ID（用于打破平局）
    """
    physical: int
    logical: int
    node_id: str
    
    def __lt__(self, other):
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        return (self.physical, self.logical, self.node_id) < (other.physical, other.logical, other.node_id)
    
    def __le__(self, other):
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        return (self.physical, self.logical, self.node_id) <= (other.physical, other.logical, other.node_id)
    
    def __gt__(self, other):
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        return (self.physical, self.logical, self.node_id) > (other.physical, other.logical, other.node_id)
    
    def __ge__(self, other):
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        return (self.physical, self.logical, self.node_id) >= (other.physical, other.logical, other.node_id)
    
    def to_tuple(self) -> Tuple[int, int, str]:
        """转换为元组"""
        return (self.physical, self.logical, self.node_id)
    
    def to_bytes(self) -> bytes:
        """转换为字节（用于序列化）"""
        # 8字节物理时间 + 4字节逻辑计数器 + 变长节点ID
        node_bytes = self.node_id.encode('utf-8')
        return struct.pack('>QI', self.physical, self.logical) + node_bytes
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "HLCTimestamp":
        """从字节创建"""
        physical, logical = struct.unpack('>QI', data[:12])
        node_id = data[12:].decode('utf-8')
        return cls(physical=physical, logical=logical, node_id=node_id)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "physical": self.physical,
            "logical": self.logical,
            "node_id": self.node_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HLCTimestamp":
        """从字典创建"""
        return cls(
            physical=data["physical"],
            logical=data.get("logical", 0),
            node_id=data["node_id"],
        )
    
    def __str__(self) -> str:
        return f"HLC({self.physical}, {self.logical}, {self.node_id})"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def __hash__(self) -> int:
        return hash((self.physical, self.logical, self.node_id))


class HybridLogicalClock:
    """
    混合逻辑时钟实现
    
    结合物理时间（毫秒）和逻辑计数器，提供：
    1. 因果一致性保证
    2. 全序关系（任意两个时间戳可比较）
    3. 时钟漂移容忍
    4. 与物理时间的兼容性
    
    Attributes:
        node_id: 当前节点ID
        max_drift_ms: 最大允许的时钟漂移（毫秒）
    """
    
    def __init__(self, node_id: str, max_drift_ms: int = 1000):
        """
        初始化HLC
        
        Args:
            node_id: 当前节点唯一标识
            max_drift_ms: 最大允许的时钟漂移（毫秒）
        """
        self._node_id = node_id
        self._max_drift_ms = max_drift_ms
        
        # 当前HLC状态
        self._physical_time: int = 0
        self._logical_counter: int = 0
        
        # 用于检测时钟回退
        self._last_physical_time: int = 0
        
        # 锁
        self._lock = threading.RLock()
        
        # 统计信息
        self._stats = {
            "total_ticks": 0,
            "logical_increments": 0,
            "physical_jumps": 0,
            "clock_drifts_detected": 0,
        }
    
    def now(self) -> HLCTimestamp:
        """
        获取当前HLC时间戳
        
        这是生成新时间戳的主要方法
        
        Returns:
            当前HLC时间戳
        """
        with self._lock:
            current_physical = self._get_physical_time()
            
            # 检查时钟回退
            if current_physical < self._last_physical_time:
                # [BUG 1 修复] 时钟回退时，_physical_time 应保持为 _last_physical_time
                # 以确保生成的HLC时间戳单调递增；同时 _last_physical_time 不更新，
                # 避免丢失"已观测到的最大物理时间"信息导致后续判断异常
                self._physical_time = self._last_physical_time
                self._logical_counter += 1
                self._stats["clock_drifts_detected"] += 1
            elif current_physical > self._physical_time:
                # 物理时间前进，重置逻辑计数器
                self._physical_time = current_physical
                self._logical_counter = 0
                self._stats["physical_jumps"] += 1
            else:
                # 物理时间相同，递增逻辑计数器
                self._logical_counter += 1
                self._stats["logical_increments"] += 1
            
            # [BUG 1 修复] _last_physical_time 只在物理时间前进或相等时更新，
            # 时钟回退时保持不变，以正确追踪已观测到的最大物理时间
            if current_physical >= self._last_physical_time:
                self._last_physical_time = current_physical
            self._stats["total_ticks"] += 1
            
            # [BUG 2 修复] 当逻辑计数器超过 max_drift_ms 对应的阈值时，记录警告日志
            if self._logical_counter > self._max_drift_ms:
                logger.warning(
                    "[HLC] 逻辑计数器(%d)超过最大漂移阈值(%dms)，"
                    "物理时钟可能存在严重漂移，node_id=%s",
                    self._logical_counter, self._max_drift_ms, self._node_id
                )
            
            return HLCTimestamp(
                physical=self._physical_time,
                logical=self._logical_counter,
                node_id=self._node_id,
            )
    
    def update(self, received_ts: HLCTimestamp) -> HLCTimestamp:
        """
        根据接收到的HLC更新本地时钟
        
        在接收远程消息时调用，确保因果一致性
        
        Args:
            received_ts: 接收到的HLC时间戳
            
        Returns:
            更新后的本地HLC时间戳
        """
        with self._lock:
            current_physical = self._get_physical_time()
            recv_physical = received_ts.physical
            recv_logical = received_ts.logical
            
            # 更新物理时间
            if recv_physical > self._physical_time:
                # 接收到的物理时间更新
                self._physical_time = recv_physical
                self._logical_counter = recv_logical + 1
            elif recv_physical == self._physical_time:
                # 物理时间相同，取最大逻辑计数器
                self._logical_counter = max(self._logical_counter, recv_logical) + 1
            else:
                # 接收到的物理时间较旧
                if current_physical > self._physical_time:
                    self._physical_time = current_physical
                    self._logical_counter = 0
                else:
                    self._logical_counter += 1
            
            self._last_physical_time = current_physical
            self._stats["total_ticks"] += 1
            
            # [BUG 2 修复] 当逻辑计数器超过 max_drift_ms 对应的阈值时，记录警告日志
            if self._logical_counter > self._max_drift_ms:
                logger.warning(
                    "[HLC] update()逻辑计数器(%d)超过最大漂移阈值(%dms)，"
                    "物理时钟可能存在严重漂移，node_id=%s",
                    self._logical_counter, self._max_drift_ms, self._node_id
                )
            
            return HLCTimestamp(
                physical=self._physical_time,
                logical=self._logical_counter,
                node_id=self._node_id,
            )
    
    def update_from_tuple(self, received: Tuple[int, int]) -> HLCTimestamp:
        """
        从元组更新时钟
        
        Args:
            received: (physical, logical) 元组
            
        Returns:
            更新后的HLC时间戳
        """
        recv_ts = HLCTimestamp(
            physical=received[0],
            logical=received[1],
            node_id="remote",  # 临时节点ID
        )
        return self.update(recv_ts)
    
    def peek(self) -> HLCTimestamp:
        """
        查看当前HLC值（不递增）
        
        Returns:
            当前HLC时间戳（不修改状态）
        """
        with self._lock:
            return HLCTimestamp(
                physical=self._physical_time,
                logical=self._logical_counter,
                node_id=self._node_id,
            )
    
    @staticmethod
    def compare(ts1: HLCTimestamp, ts2: HLCTimestamp) -> HLCComparisonResult:
        """
        比较两个HLC时间戳
        
        Args:
            ts1: 第一个时间戳
            ts2: 第二个时间戳
            
        Returns:
            HLCComparisonResult: 比较结果
        """
        if ts1.physical < ts2.physical:
            return HLCComparisonResult.LESS
        elif ts1.physical > ts2.physical:
            return HLCComparisonResult.GREATER
        else:
            # 物理时间相同，比较逻辑计数器
            if ts1.logical < ts2.logical:
                return HLCComparisonResult.LESS
            elif ts1.logical > ts2.logical:
                return HLCComparisonResult.GREATER
            else:
                # 逻辑计数器也相同，比较节点ID（打破平局）
                if ts1.node_id < ts2.node_id:
                    return HLCComparisonResult.LESS
                elif ts1.node_id > ts2.node_id:
                    return HLCComparisonResult.GREATER
                else:
                    return HLCComparisonResult.EQUAL
    
    @staticmethod
    def less_than(ts1: HLCTimestamp, ts2: HLCTimestamp) -> bool:
        """检查ts1 < ts2"""
        return HybridLogicalClock.compare(ts1, ts2) == HLCComparisonResult.LESS
    
    @staticmethod
    def greater_than(ts1: HLCTimestamp, ts2: HLCTimestamp) -> bool:
        """检查ts1 > ts2"""
        return HybridLogicalClock.compare(ts1, ts2) == HLCComparisonResult.GREATER
    
    @staticmethod
    def equal(ts1: HLCTimestamp, ts2: HLCTimestamp) -> bool:
        """检查ts1 == ts2"""
        return HybridLogicalClock.compare(ts1, ts2) == HLCComparisonResult.EQUAL
    
    @staticmethod
    def max(ts1: HLCTimestamp, ts2: HLCTimestamp) -> HLCTimestamp:
        """返回较大的时间戳"""
        if HybridLogicalClock.compare(ts1, ts2) >= HLCComparisonResult.EQUAL:
            return ts1
        return ts2
    
    @staticmethod
    def min(ts1: HLCTimestamp, ts2: HLCTimestamp) -> HLCTimestamp:
        """返回较小的时间戳"""
        if HybridLogicalClock.compare(ts1, ts2) <= HLCComparisonResult.EQUAL:
            return ts1
        return ts2
    
    def _get_physical_time(self) -> int:
        """
        获取当前物理时间（毫秒）
        
        Returns:
            当前物理时间戳（毫秒）
        """
        return int(time.time() * 1000)
    
    def get_stats(self) -> Dict[str, int]:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        with self._lock:
            return self._stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        with self._lock:
            self._stats = {
                "total_ticks": 0,
                "logical_increments": 0,
                "physical_jumps": 0,
                "clock_drifts_detected": 0,
            }
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典
        
        Returns:
            包含当前状态的字典
        """
        with self._lock:
            return {
                "node_id": self._node_id,
                "physical_time": self._physical_time,
                "logical_counter": self._logical_counter,
                "max_drift_ms": self._max_drift_ms,
                "stats": self._stats.copy(),
            }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HybridLogicalClock":
        """
        从字典创建
        
        Args:
            data: 包含状态的字典
            
        Returns:
            新创建的HLC实例
        """
        hlc = cls(
            node_id=data["node_id"],
            max_drift_ms=data.get("max_drift_ms", 1000),
        )
        hlc._physical_time = data.get("physical_time", 0)
        hlc._logical_counter = data.get("logical_counter", 0)
        hlc._stats = data.get("stats", {})
        return hlc


class HLCClockManager:
    """
    HLC时钟管理器
    
    管理多个HLC实例，提供分布式系统范围的时钟服务
    """
    
    def __init__(self, node_id: str, max_entries: int = 10000):
        """
        初始化管理器
        
        Args:
            node_id: 当前节点ID
            max_entries: 事件日志最大条目数，超过时自动清理旧条目
        """
        self._node_id = node_id
        self._hlc = HybridLogicalClock(node_id)
        self._event_log: list = []
        # [BUG 3 修复] 添加 max_entries 限制，防止 _event_log 无界增长
        self._max_entries = max_entries
        self._lock = threading.RLock()
    
    def _trim_event_log(self):
        """[BUG 3 修复] 当事件日志超过 max_entries 时，清理最旧的条目"""
        if len(self._event_log) > self._max_entries:
            # 保留最新的 max_entries 条，丢弃旧条目
            trim_count = len(self._event_log) - self._max_entries
            self._event_log = self._event_log[trim_count:]
            logger.debug(
                "[HLCClockManager] 事件日志已清理 %d 条旧记录，当前 %d 条",
                trim_count, len(self._event_log)
            )
    
    def generate_timestamp(self, event_type: str = "", metadata: Optional[Dict[str, Any]] = None) -> HLCTimestamp:
        """
        生成新的时间戳并记录事件
        
        Args:
            event_type: 事件类型
            metadata: 事件元数据
            
        Returns:
            新生成的HLC时间戳
        """
        with self._lock:
            ts = self._hlc.now()
            
            event = {
                "timestamp": ts,
                "event_type": event_type,
                "metadata": metadata or {},
                "local": True,
            }
            self._event_log.append(event)
            # [BUG 3 修复] 每次添加事件后检查是否需要清理
            self._trim_event_log()
            
            return ts
    
    def receive_timestamp(self, remote_ts: HLCTimestamp, event_type: str = "", metadata: Optional[Dict[str, Any]] = None) -> HLCTimestamp:
        """
        接收远程时间戳并更新本地时钟
        
        Args:
            remote_ts: 远程HLC时间戳
            event_type: 事件类型
            metadata: 事件元数据
            
        Returns:
            更新后的本地HLC时间戳
        """
        with self._lock:
            local_ts = self._hlc.update(remote_ts)
            
            event = {
                "timestamp": local_ts,
                "event_type": event_type,
                "metadata": metadata or {},
                "local": False,
                "remote_timestamp": remote_ts,
            }
            self._event_log.append(event)
            # [BUG 3 修复] 每次添加事件后检查是否需要清理
            self._trim_event_log()
            
            return local_ts
    
    def get_current_timestamp(self) -> HLCTimestamp:
        """
        获取当前时间戳（不生成新事件）
        
        Returns:
            当前HLC时间戳
        """
        with self._lock:
            return self._hlc.peek()
    
    def get_event_log(self) -> list:
        """
        获取事件日志
        
        Returns:
            事件日志列表
        """
        with self._lock:
            return self._event_log.copy()
    
    def clear_event_log(self):
        """清空事件日志"""
        with self._lock:
            self._event_log.clear()


# 全局HLC实例（用于向后兼容）
_global_hlc_instances: Dict[str, HybridLogicalClock] = {}
_global_hlc_lock = threading.Lock()


def get_hlc_instance(node_id: str) -> HybridLogicalClock:
    """
    获取或创建全局HLC实例
    
    Args:
        node_id: 节点ID
        
    Returns:
        HLC实例
    """
    with _global_hlc_lock:
        if node_id not in _global_hlc_instances:
            _global_hlc_instances[node_id] = HybridLogicalClock(node_id)
        return _global_hlc_instances[node_id]


def get_timestamp(node_id: str) -> HLCTimestamp:
    """
    获取时间戳（便捷函数）
    
    Args:
        node_id: 节点ID
        
    Returns:
        HLC时间戳
    """
    hlc = get_hlc_instance(node_id)
    return hlc.now()


def update_timestamp(node_id: str, received: HLCTimestamp) -> HLCTimestamp:
    """
    更新时钟（便捷函数）
    
    Args:
        node_id: 节点ID
        received: 接收到的HLC时间戳
        
    Returns:
        更新后的HLC时间戳
    """
    hlc = get_hlc_instance(node_id)
    return hlc.update(received)


def compare_hlc_timestamps(ts1: HLCTimestamp, ts2: HLCTimestamp) -> int:
    """
    比较两个HLC时间戳（便捷函数）
    
    Args:
        ts1: 第一个时间戳
        ts2: 第二个时间戳
        
    Returns:
        -1 if ts1 < ts2, 0 if equal, 1 if ts1 > ts2
    """
    result = HybridLogicalClock.compare(ts1, ts2)
    return result.value


def hlc_to_tuple(ts: HLCTimestamp) -> Tuple[int, int, str]:
    """
    将HLC时间戳转换为元组
    
    Args:
        ts: HLC时间戳
        
    Returns:
        (physical, logical, node_id) 元组
    """
    return (ts.physical, ts.logical, ts.node_id)


def tuple_to_hlc(t: Tuple[int, int, str]) -> HLCTimestamp:
    """
    从元组创建HLC时间戳
    
    Args:
        t: (physical, logical, node_id) 元组
        
    Returns:
        HLC时间戳
    """
    return HLCTimestamp(physical=t[0], logical=t[1], node_id=t[2])

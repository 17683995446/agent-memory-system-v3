"""
向量时钟 (Vector Clock) 实现模块

用于追踪分布式系统中的happens-before关系，保证因果一致性。

核心特性:
- 完整的向量时钟操作（递增、合并、比较）
- happens-before关系检测
- 并发操作识别
- 与Raft集成支持

示例用法:
    >>> from vector_clock import VectorClock
    >>> vc1 = VectorClock(node_id="node1")
    >>> vc1.increment()
    >>> vc2 = VectorClock(node_id="node2")
    >>> vc2.update(vc1)
    >>> vc2.increment()
    >>> print(vc1.compare(vc2))
"""

import copy
import json
import logging
from typing import Dict, List, Optional, Tuple, Set, Any
from dataclasses import dataclass, field
from enum import Enum, auto
import threading
import time

logger = logging.getLogger(__name__)


class VectorClockRelation(Enum):
    """向量时钟比较关系"""
    BEFORE = auto()      # self happens-before other
    AFTER = auto()       # other happens-before self
    CONCURRENT = auto()  # 并发（无因果关系）
    EQUAL = auto()       # 相等


@dataclass
class CausalEvent:
    """
    因果事件
    
    表示一个带有向量时钟的事件，用于追踪因果关系
    
    Attributes:
        event_id: 事件唯一标识
        node_id: 产生事件的节点ID
        vector_clock: 事件发生时向量时钟的快照
        timestamp: 物理时间戳（用于调试和日志）
        event_type: 事件类型
        payload: 事件负载数据
    """
    event_id: str
    node_id: str
    vector_clock: Dict[str, int]
    timestamp: float = field(default_factory=time.time)
    event_type: str = ""
    payload: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "event_id": self.event_id,
            "node_id": self.node_id,
            "vector_clock": self.vector_clock.copy(),
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "payload": self.payload,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CausalEvent":
        """从字典创建"""
        return cls(
            event_id=data["event_id"],
            node_id=data["node_id"],
            vector_clock=data.get("vector_clock", {}),
            timestamp=data.get("timestamp", time.time()),
            event_type=data.get("event_type", ""),
            payload=data.get("payload"),
        )


class VectorClock:
    """
    向量时钟实现
    
    用于追踪分布式操作之间的happens-before关系，
    保证因果一致性。
    
    Attributes:
        node_id: 当前节点ID
        clock: 向量时钟字典 {node_id: counter}
    """
    
    def __init__(self, node_id: str, initial_clock: Optional[Dict[str, int]] = None):
        """
        初始化向量时钟
        
        Args:
            node_id: 当前节点唯一标识
            initial_clock: 初始时钟值（用于从远程复制）
        """
        self._node_id = node_id
        self._clock: Dict[str, int] = copy.deepcopy(initial_clock) if initial_clock else {}
        self._lock = threading.RLock()
        
        # 确保当前节点在时钟中有条目
        if node_id not in self._clock:
            self._clock[node_id] = 0
    
    @property
    def node_id(self) -> str:
        """获取节点ID"""
        return self._node_id
    
    @property
    def clock(self) -> Dict[str, int]:
        """获取时钟副本"""
        with self._lock:
            return copy.deepcopy(self._clock)
    
    def increment(self) -> Dict[str, int]:
        """
        递增本地时钟
        
        在本地发生事件时调用
        
        Returns:
            更新后的时钟副本
        """
        with self._lock:
            self._clock[self._node_id] = self._clock.get(self._node_id, 0) + 1
            return copy.deepcopy(self._clock)
    
    def update(self, other: "VectorClock") -> Dict[str, int]:
        """
        合并另一个向量时钟
        
        在接收远程事件时调用，合并远程时钟到本地
        
        Args:
            other: 要合并的向量时钟
            
        Returns:
            更新后的时钟副本
        """
        with self._lock:
            other_clock = other.clock
            for node_id, counter in other_clock.items():
                self._clock[node_id] = max(self._clock.get(node_id, 0), counter)
            return copy.deepcopy(self._clock)
    
    def update_from_dict(self, other_clock: Dict[str, int]) -> Dict[str, int]:
        """
        从字典更新时钟
        
        Args:
            other_clock: 要合并的时钟字典
            
        Returns:
            更新后的时钟副本
        """
        with self._lock:
            for node_id, counter in other_clock.items():
                self._clock[node_id] = max(self._clock.get(node_id, 0), counter)
            return copy.deepcopy(self._clock)
    
    def compare(self, other: "VectorClock") -> VectorClockRelation:
        """
        比较两个向量时钟
        
        确定两个事件之间的happens-before关系
        
        Args:
            other: 要比较的向量时钟
            
        Returns:
            VectorClockRelation: 比较结果
            - BEFORE: self happens-before other
            - AFTER: other happens-before self
            - CONCURRENT: 并发（无因果关系）
            - EQUAL: 相等
        """
        with self._lock:
            clock1 = self._clock
            clock2 = other.clock
            
            # 获取所有节点ID
            all_nodes = set(clock1.keys()) | set(clock2.keys())
            
            # 比较每个节点的计数器
            dominates = False  # self dominates other
            dominated = False  # other dominates self
            
            for node_id in all_nodes:
                c1 = clock1.get(node_id, 0)
                c2 = clock2.get(node_id, 0)
                
                if c1 > c2:
                    dominates = True
                elif c2 > c1:
                    dominated = True
                
                # 如果同时存在dominates和dominated，则是并发的
                if dominates and dominated:
                    return VectorClockRelation.CONCURRENT
            
            if dominates and not dominated:
                return VectorClockRelation.AFTER
            elif dominated and not dominates:
                return VectorClockRelation.BEFORE
            else:
                return VectorClockRelation.EQUAL
    
    def compare_to_dict(self, other_clock: Dict[str, int]) -> VectorClockRelation:
        """
        与时钟字典比较
        
        Args:
            other_clock: 要比较的时钟字典
            
        Returns:
            VectorClockRelation: 比较结果
        """
        with self._lock:
            clock1 = self._clock
            clock2 = other_clock
            
            all_nodes = set(clock1.keys()) | set(clock2.keys())
            
            dominates = False
            dominated = False
            
            for node_id in all_nodes:
                c1 = clock1.get(node_id, 0)
                c2 = clock2.get(node_id, 0)
                
                if c1 > c2:
                    dominates = True
                elif c2 > c1:
                    dominated = True
                
                if dominates and dominated:
                    return VectorClockRelation.CONCURRENT
            
            if dominates and not dominated:
                return VectorClockRelation.AFTER
            elif dominated and not dominates:
                return VectorClockRelation.BEFORE
            else:
                return VectorClockRelation.EQUAL
    
    def happens_before(self, other: "VectorClock") -> bool:
        """
        检查self是否happens-before other
        
        Args:
            other: 要比较的向量时钟
            
        Returns:
            True如果self happens-before other
        """
        return self.compare(other) == VectorClockRelation.BEFORE
    
    def is_concurrent_with(self, other: "VectorClock") -> bool:
        """
        检查是否与other并发
        
        Args:
            other: 要比较的向量时钟
            
        Returns:
            True如果是并发关系
        """
        return self.compare(other) == VectorClockRelation.CONCURRENT
    
    def merge(self, other: "VectorClock") -> "VectorClock":
        """
        创建合并后的新向量时钟
        
        Args:
            other: 要合并的向量时钟
            
        Returns:
            新的合并后的向量时钟
        """
        with self._lock:
            new_clock = copy.deepcopy(self._clock)
            other_clock = other.clock
            
            for node_id, counter in other_clock.items():
                new_clock[node_id] = max(new_clock.get(node_id, 0), counter)
            
            return VectorClock(self._node_id, new_clock)
    
    def copy(self) -> "VectorClock":
        """
        创建副本
        
        Returns:
            向量时钟的深拷贝
        """
        with self._lock:
            return VectorClock(self._node_id, copy.deepcopy(self._clock))
    
    def get_timestamp(self) -> int:
        """
        获取本地节点的时钟值
        
        Returns:
            本地节点的逻辑时间戳
        """
        with self._lock:
            return self._clock.get(self._node_id, 0)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典（用于序列化）
        
        Returns:
            包含时钟状态的字典
        """
        with self._lock:
            return {
                "node_id": self._node_id,
                "clock": copy.deepcopy(self._clock),
            }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VectorClock":
        """
        从字典创建
        
        Args:
            data: 包含时钟状态的字典
            
        Returns:
            新创建的向量时钟
        """
        return cls(
            node_id=data["node_id"],
            initial_clock=data.get("clock", {}),
        )
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "VectorClock":
        """从JSON字符串创建"""
        return cls.from_dict(json.loads(json_str))
    
    def __str__(self) -> str:
        """字符串表示"""
        with self._lock:
            items = ", ".join(f"{k}:{v}" for k, v in sorted(self._clock.items()))
            return f"VC[{self._node_id}]({items})"
    
    def __repr__(self) -> str:
        """详细表示"""
        return self.__str__()
    
    def __eq__(self, other) -> bool:
        """相等比较"""
        if not isinstance(other, VectorClock):
            return False
        return self.compare(other) == VectorClockRelation.EQUAL
    
    def __hash__(self) -> int:
        """哈希值"""
        with self._lock:
            return hash(tuple(sorted(self._clock.items())))


class VectorClockManager:
    """
    向量时钟管理器
    
    管理多个事件的向量时钟，提供因果一致性检查
    """
    
    def __init__(self, node_id: str, max_history_size: int = 10000):
        """
        初始化管理器
        
        Args:
            node_id: 当前节点ID
            max_history_size: [BUG 4 修复] 事件历史最大条目数，超过时自动清理旧事件
        """
        self._node_id = node_id
        self._current_clock = VectorClock(node_id)
        self._event_history: List[CausalEvent] = []
        self._causal_dependencies: Dict[str, Set[str]] = {}  # event_id -> {dependent_event_ids}
        # [BUG 4 修复] 添加 max_history_size 限制，防止 _event_history 无界增长
        self._max_history_size = max_history_size
        self._lock = threading.RLock()
    
    def _trim_event_history(self):
        """[BUG 4 修复] 当事件历史超过 max_history_size 时，清理最旧的事件"""
        if len(self._event_history) > self._max_history_size:
            trim_count = len(self._event_history) - self._max_history_size
            removed_events = self._event_history[:trim_count]
            self._event_history = self._event_history[trim_count:]
            # 同步清理已移除事件的因果依赖关系
            removed_ids = {e.event_id for e in removed_events}
            for removed_id in removed_ids:
                self._causal_dependencies.pop(removed_id, None)
                # 从其他事件的依赖集合中移除已删除的事件引用
                for deps in self._causal_dependencies.values():
                    deps.discard(removed_id)
            logger.debug(
                "[VectorClockManager] 事件历史已清理 %d 条旧记录，当前 %d 条",
                trim_count, len(self._event_history)
            )
    
    def record_event(self, event_id: str, event_type: str = "", payload: Optional[Dict[str, Any]] = None) -> CausalEvent:
        """
        记录一个新事件
        
        Args:
            event_id: 事件唯一标识
            event_type: 事件类型
            payload: 事件负载
            
        Returns:
            创建的因果事件
        """
        with self._lock:
            # 递增本地时钟
            clock_snapshot = self._current_clock.increment()
            
            # 创建事件
            event = CausalEvent(
                event_id=event_id,
                node_id=self._node_id,
                vector_clock=clock_snapshot,
                event_type=event_type,
                payload=payload,
            )
            
            self._event_history.append(event)
            # [BUG 4 修复] 每次添加事件后检查是否需要清理
            self._trim_event_history()
            return event
    
    def receive_event(self, event: CausalEvent) -> bool:
        """
        接收远程事件
        
        Args:
            event: 接收到的因果事件
            
        Returns:
            True如果成功接收
        """
        with self._lock:
            # 更新本地时钟
            self._current_clock.update_from_dict(event.vector_clock)
            
            # 记录事件
            self._event_history.append(event)
            # [BUG 4 修复] 每次添加事件后检查是否需要清理
            self._trim_event_history()
            
            # 记录因果关系
            for prev_event in self._event_history[:-1]:
                vc_prev = VectorClock(self._node_id, prev_event.vector_clock)
                vc_curr = VectorClock(self._node_id, event.vector_clock)
                
                if vc_prev.happens_before(vc_curr):
                    if event.event_id not in self._causal_dependencies:
                        self._causal_dependencies[event.event_id] = set()
                    self._causal_dependencies[event.event_id].add(prev_event.event_id)
            
            return True
    
    def check_causal_consistency(self, event1_id: str, event2_id: str) -> Optional[VectorClockRelation]:
        """
        检查两个事件之间的因果关系
        
        Args:
            event1_id: 第一个事件ID
            event2_id: 第二个事件ID
            
        Returns:
            事件之间的关系，如果找不到事件则返回None
        """
        with self._lock:
            event1 = self._find_event(event1_id)
            event2 = self._find_event(event2_id)
            
            if not event1 or not event2:
                return None
            
            vc1 = VectorClock(self._node_id, event1.vector_clock)
            vc2 = VectorClock(self._node_id, event2.vector_clock)
            
            return vc1.compare(vc2)
    
    def get_causal_dependencies(self, event_id: str) -> Set[str]:
        """
        获取事件的因果依赖
        
        Args:
            event_id: 事件ID
            
        Returns:
            该事件依赖的所有事件ID集合
        """
        with self._lock:
            return self._causal_dependencies.get(event_id, set()).copy()
    
    def get_concurrent_events(self, event_id: str) -> List[CausalEvent]:
        """
        获取与指定事件并发的所有事件
        
        Args:
            event_id: 事件ID
            
        Returns:
            并发事件列表
        """
        with self._lock:
            event = self._find_event(event_id)
            if not event:
                return []
            
            vc_target = VectorClock(self._node_id, event.vector_clock)
            concurrent = []
            
            for e in self._event_history:
                if e.event_id != event_id:
                    vc_e = VectorClock(self._node_id, e.vector_clock)
                    if vc_target.is_concurrent_with(vc_e):
                        concurrent.append(e)
            
            return concurrent
    
    def get_current_clock(self) -> VectorClock:
        """
        获取当前向量时钟
        
        Returns:
            当前时钟的副本
        """
        with self._lock:
            return self._current_clock.copy()
    
    def _find_event(self, event_id: str) -> Optional[CausalEvent]:
        """查找事件"""
        for event in self._event_history:
            if event.event_id == event_id:
                return event
        return None
    
    def get_event_history(self) -> List[CausalEvent]:
        """
        获取事件历史
        
        Returns:
            所有事件的列表
        """
        with self._lock:
            return self._event_history.copy()
    
    def clear_history(self):
        """清空事件历史"""
        with self._lock:
            self._event_history.clear()
            self._causal_dependencies.clear()


def merge_vector_clocks(clocks: List[VectorClock]) -> Dict[str, int]:
    """
    合并多个向量时钟
    
    Args:
        clocks: 要合并的向量时钟列表
        
    Returns:
        合并后的时钟字典
    """
    if not clocks:
        return {}
    
    result = {}
    for clock in clocks:
        clock_dict = clock.clock
        for node_id, counter in clock_dict.items():
            result[node_id] = max(result.get(node_id, 0), counter)
    
    return result


def are_concurrent(clock1: Dict[str, int], clock2: Dict[str, int]) -> bool:
    """
    检查两个时钟是否并发
    
    Args:
        clock1: 第一个时钟字典
        clock2: 第二个时钟字典
        
    Returns:
        True如果是并发关系
    """
    all_nodes = set(clock1.keys()) | set(clock2.keys())
    
    dominates = False
    dominated = False
    
    for node_id in all_nodes:
        c1 = clock1.get(node_id, 0)
        c2 = clock2.get(node_id, 0)
        
        if c1 > c2:
            dominates = True
        elif c2 > c1:
            dominated = True
        
        if dominates and dominated:
            return True
    
    return not dominates and not dominated

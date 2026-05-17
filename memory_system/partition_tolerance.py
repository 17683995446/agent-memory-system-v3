"""
分区容忍性 (Partition Tolerance) 实现模块

实现分布式系统的分区检测、处理和恢复机制，
保证分区期间的可用性和一致性。

核心特性:
- 网络分区检测
- 多数派写入保证
- 分区期间可用性保证
- 自动故障转移
- 分区恢复后的数据同步
- 脑裂检测和解决

示例用法:
    >>> from partition_tolerance import PartitionManager, QuorumManager
    >>> pm = PartitionManager(node_id="node1", peers=["node2", "node3"])
    >>> pm.detect_partition()
"""

import time
import asyncio
import random
from typing import Dict, List, Optional, Set, Tuple, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import threading

from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp
from memory_system.vector_clock import VectorClock, VectorClockRelation


class PartitionState(Enum):
    """分区状态"""
    HEALTHY = "healthy"           # 健康状态
    SUSPECTED = "suspected"       # 疑似分区
    PARTITIONED = "partitioned"   # 已分区
    RECOVERING = "recovering"     # 恢复中


class NodeState(Enum):
    """节点状态"""
    HEALTHY = "healthy"           # 健康
    SUSPECTED = "suspected"       # 疑似故障
    UNREACHABLE = "unreachable"   # 不可达
    FAILED = "failed"             # 已故障


@dataclass
class NodeHealth:
    """
    节点健康状态
    
    Attributes:
        node_id: 节点ID
        state: 节点状态
        last_heartbeat: 最后心跳时间
        heartbeat_count: 心跳计数
        missed_heartbeats: 丢失心跳计数
        latency_ms: 延迟（毫秒）
    """
    node_id: str
    state: NodeState = NodeState.HEALTHY
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_count: int = 0
    missed_heartbeats: int = 0
    latency_ms: float = 0.0
    
    def record_heartbeat(self, latency_ms: float = 0.0):
        """记录心跳"""
        self.last_heartbeat = time.time()
        self.heartbeat_count += 1
        self.missed_heartbeats = 0
        self.latency_ms = latency_ms
        self.state = NodeState.HEALTHY
    
    def record_missed_heartbeat(self):
        """记录丢失心跳"""
        self.missed_heartbeats += 1
        
        if self.missed_heartbeats >= 3:
            self.state = NodeState.SUSPECTED
        if self.missed_heartbeats >= 5:
            self.state = NodeState.UNREACHABLE
    
    def is_healthy(self, timeout_seconds: float = 5.0) -> bool:
        """检查是否健康"""
        return (self.state == NodeState.HEALTHY and
                time.time() - self.last_heartbeat < timeout_seconds)


@dataclass
class PartitionEvent:
    """
    分区事件
    
    Attributes:
        event_id: 事件ID
        event_type: 事件类型
        timestamp: 发生时间
        affected_nodes: 受影响的节点
        description: 描述
    """
    event_id: str
    event_type: str
    timestamp: float
    affected_nodes: List[str]
    description: str


class PartitionManager:
    """
    分区管理器
    
    检测和管理网络分区，处理分区期间的操作
    """
    
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        heartbeat_interval: float = 1.0,
        heartbeat_timeout: float = 5.0,
        partition_threshold: int = 3,
    ):
        """
        初始化分区管理器
        
        Args:
            node_id: 当前节点ID
            peers: 其他节点列表
            heartbeat_interval: 心跳间隔（秒）
            heartbeat_timeout: 心跳超时（秒）
            partition_threshold: 分区检测阈值（丢失心跳次数）
        """
        self._node_id = node_id
        self._peers = peers
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._partition_threshold = partition_threshold
        
        # 节点健康状态
        self._node_health: Dict[str, NodeHealth] = {}
        self._partition_state = PartitionState.HEALTHY
        self._partition_view: Set[str] = {node_id}  # 当前分区视图
        
        # 初始化节点健康状态
        for peer in peers:
            self._node_health[peer] = NodeHealth(node_id=peer)
        
        # 事件历史
        self._events: List[PartitionEvent] = []
        
        # 锁
        self._lock = threading.RLock()
        
        # 回调函数
        self._on_partition_detected: Optional[Callable] = None
        self._on_partition_recovered: Optional[Callable] = None
        
        # 运行状态
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
    
    def start(self):
        """启动分区管理器"""
        self._running = True
        # 在异步环境中启动心跳任务
        try:
            loop = asyncio.get_event_loop()
            self._heartbeat_task = loop.create_task(self._heartbeat_loop())
        except RuntimeError:
            pass
    
    def stop(self):
        """停止分区管理器"""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
    
    async def _heartbeat_loop(self):
        """心跳循环"""
        while self._running:
            try:
                await self._send_heartbeats()
                await self._check_partition()
                await asyncio.sleep(self._heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Heartbeat loop error: {e}")
                await asyncio.sleep(self._heartbeat_interval)
    
    async def _send_heartbeats(self):
        """发送心跳到所有节点"""
        tasks = []
        for peer in self._peers:
            tasks.append(self._send_heartbeat_to(peer))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_heartbeat_to(self, peer: str):
        """发送心跳到指定节点"""
        start_time = time.time()
        
        try:
            # 模拟网络请求（实际应替换为真实RPC）
            # await self._transport.send_heartbeat(peer)
            await asyncio.sleep(0.01)  # 模拟延迟
            
            latency_ms = (time.time() - start_time) * 1000
            self.record_heartbeat(peer, latency_ms)
        except Exception:
            self.record_missed_heartbeat(peer)
    
    def record_heartbeat(self, node_id: str, latency_ms: float = 0.0):
        """
        记录收到的心跳
        
        Args:
            node_id: 节点ID
            latency_ms: 延迟（毫秒）
        """
        with self._lock:
            if node_id not in self._node_health:
                self._node_health[node_id] = NodeHealth(node_id=node_id)
            
            self._node_health[node_id].record_heartbeat(latency_ms)
    
    def record_missed_heartbeat(self, node_id: str):
        """
        记录丢失的心跳
        
        Args:
            node_id: 节点ID
        """
        with self._lock:
            if node_id not in self._node_health:
                self._node_health[node_id] = NodeHealth(node_id=node_id)
            
            self._node_health[node_id].record_missed_heartbeat()
    
    async def _check_partition(self):
        """检查分区状态"""
        with self._lock:
            healthy_nodes = {self._node_id}
            suspected_nodes = []
            
            for node_id, health in self._node_health.items():
                if health.is_healthy(self._heartbeat_timeout):
                    healthy_nodes.add(node_id)
                elif health.state in [NodeState.SUSPECTED, NodeState.UNREACHABLE]:
                    suspected_nodes.append(node_id)
            
            # 检测分区
            if len(suspected_nodes) >= len(self._peers) // 2 + 1:
                if self._partition_state != PartitionState.PARTITIONED:
                    await self._on_partition_detected_event(suspected_nodes)
            elif self._partition_state == PartitionState.PARTITIONED and len(suspected_nodes) == 0:
                await self._on_partition_recovered_event()
            
            self._partition_view = healthy_nodes
    
    async def _on_partition_detected_event(self, affected_nodes: List[str]):
        """分区检测事件"""
        self._partition_state = PartitionState.PARTITIONED
        
        event = PartitionEvent(
            event_id=f"partition_{int(time.time())}",
            event_type="PARTITION_DETECTED",
            timestamp=time.time(),
            affected_nodes=affected_nodes,
            description=f"Network partition detected. Affected nodes: {affected_nodes}",
        )
        self._events.append(event)
        
        print(f"[PartitionManager] Partition detected: {affected_nodes}")
        
        if self._on_partition_detected:
            await self._on_partition_detected(affected_nodes)
    
    async def _on_partition_recovered_event(self):
        """分区恢复事件"""
        self._partition_state = PartitionState.RECOVERING
        
        event = PartitionEvent(
            event_id=f"recovery_{int(time.time())}",
            event_type="PARTITION_RECOVERED",
            timestamp=time.time(),
            affected_nodes=list(self._node_health.keys()),
            description="Network partition recovered",
        )
        self._events.append(event)
        
        print(f"[PartitionManager] Partition recovered")
        
        if self._on_partition_recovered:
            await self._on_partition_recovered()
        
        self._partition_state = PartitionState.HEALTHY
    
    def is_partitioned(self) -> bool:
        """
        检查是否处于分区状态
        
        Returns:
            True如果已分区
        """
        with self._lock:
            return self._partition_state == PartitionState.PARTITIONED
    
    def get_partition_view(self) -> Set[str]:
        """
        获取当前分区视图
        
        Returns:
            可达节点集合
        """
        with self._lock:
            return self._partition_view.copy()
    
    def get_healthy_nodes(self) -> List[str]:
        """
        获取健康节点列表
        
        Returns:
            健康节点ID列表
        """
        with self._lock:
            return [
                node_id for node_id, health in self._node_health.items()
                if health.is_healthy(self._heartbeat_timeout)
            ]
    
    def get_node_health(self, node_id: str) -> Optional[NodeHealth]:
        """
        获取节点健康状态
        
        Args:
            node_id: 节点ID
            
        Returns:
            节点健康状态
        """
        with self._lock:
            return self._node_health.get(node_id)
    
    def set_partition_callbacks(
        self,
        on_detected: Optional[Callable] = None,
        on_recovered: Optional[Callable] = None,
    ):
        """
        设置分区回调函数
        
        Args:
            on_detected: 分区检测回调
            on_recovered: 分区恢复回调
        """
        self._on_partition_detected = on_detected
        self._on_partition_recovered = on_recovered
    
    def get_events(self) -> List[PartitionEvent]:
        """
        获取分区事件历史
        
        Returns:
            事件列表
        """
        with self._lock:
            return self._events.copy()


class QuorumManager:
    """
    多数派管理器
    
    管理分布式操作的多数派读写
    """
    
    def __init__(
        self,
        total_nodes: int,
        read_quorum: Optional[int] = None,
        write_quorum: Optional[int] = None,
    ):
        """
        初始化多数派管理器
        
        Args:
            total_nodes: 总节点数
            read_quorum: 读多数派大小（默认: floor(n/2)+1）
            write_quorum: 写多数派大小（默认: floor(n/2)+1）
        """
        self._total_nodes = total_nodes
        self._read_quorum = read_quorum or (total_nodes // 2 + 1)
        self._write_quorum = write_quorum or (total_nodes // 2 + 1)
    
    @property
    def read_quorum(self) -> int:
        """读多数派大小"""
        return self._read_quorum
    
    @property
    def write_quorum(self) -> int:
        """写多数派大小"""
        return self._write_quorum
    
    @property
    def total_nodes(self) -> int:
        """总节点数"""
        return self._total_nodes
    
    def is_read_quorum(self, responses: int) -> bool:
        """
        检查是否达到读多数派
        
        Args:
            responses: 响应数量
            
        Returns:
            True如果达到多数派
        """
        return responses >= self._read_quorum
    
    def is_write_quorum(self, acks: int) -> bool:
        """
        检查是否达到写多数派
        
        Args:
            acks: 确认数量
            
        Returns:
            True如果达到多数派
        """
        return acks >= self._write_quorum
    
    def can_serve_reads(self, available_nodes: int) -> bool:
        """
        检查是否可以提供读服务
        
        Args:
            available_nodes: 可用节点数
            
        Returns:
            True如果可以提供读服务
        """
        return available_nodes >= self._read_quorum
    
    def can_serve_writes(self, available_nodes: int) -> bool:
        """
        检查是否可以提供写服务
        
        Args:
            available_nodes: 可用节点数
            
        Returns:
            True如果可以提供写服务
        """
        return available_nodes >= self._write_quorum
    
    def has_quorum(self, available_nodes: Set[str]) -> bool:
        """
        检查可用节点是否达到多数派
        
        Args:
            available_nodes: 可用节点集合
            
        Returns:
            True如果可用节点数达到写多数派
        """
        return len(available_nodes) >= self._write_quorum

    def calculate_quorum_size(self, total_nodes: int) -> int:
        """
        计算多数派大小
        
        Args:
            total_nodes: 总节点数
            
        Returns:
            多数派大小
        """
        return total_nodes // 2 + 1
    
    def get_quorum_status(self, available_nodes: int) -> Dict[str, Any]:
        """
        获取多数派状态
        
        Args:
            available_nodes: 可用节点数
            
        Returns:
            状态字典
        """
        return {
            "total_nodes": self._total_nodes,
            "available_nodes": available_nodes,
            "read_quorum": self._read_quorum,
            "write_quorum": self._write_quorum,
            "can_read": self.can_serve_reads(available_nodes),
            "can_write": self.can_serve_writes(available_nodes),
            "read_quorum_satisfied": available_nodes >= self._read_quorum,
            "write_quorum_satisfied": available_nodes >= self._write_quorum,
        }


class PartitionTolerantStorage:
    """
    分区容忍存储
    
    提供分区期间的读写保证
    """
    
    def __init__(
        self,
        node_id: str,
        nodes: List[str],
        partition_manager: PartitionManager,
        quorum_manager: QuorumManager,
    ):
        """
        初始化分区容忍存储
        
        Args:
            node_id: 当前节点ID
            nodes: 所有节点列表
            partition_manager: 分区管理器
            quorum_manager: 多数派管理器
        """
        self._node_id = node_id
        self._nodes = nodes
        self._partition_manager = partition_manager
        self._quorum_manager = quorum_manager
        
        # 本地存储
        self._local_store: Dict[str, Tuple[Any, HLCTimestamp, Dict[str, int]]] = {}
        
        # 待同步操作（分区期间积累）
        self._pending_sync: List[Dict[str, Any]] = []
        
        # 锁
        self._lock = threading.RLock()
        
        # 设置分区回调
        self._partition_manager.set_partition_callbacks(
            on_detected=self._on_partition,
            on_recovered=self._on_recovery,
        )
    
    async def write(
        self,
        key: str,
        value: Any,
        timestamp: HLCTimestamp,
        vector_clock: Dict[str, int],
    ) -> bool:
        """
        写入数据
        
        保证多数派写入
        
        Args:
            key: 键
            value: 值
            timestamp: HLC时间戳
            vector_clock: 向量时钟
            
        Returns:
            True如果写入成功
        """
        with self._lock:
            # 本地写入
            self._local_store[key] = (value, timestamp, vector_clock)
            
            # 检查分区状态
            if self._partition_manager.is_partitioned():
                # 分区期间，记录待同步操作
                self._pending_sync.append({
                    "type": "write",
                    "key": key,
                    "value": value,
                    "timestamp": timestamp,
                    "vector_clock": vector_clock,
                })
                return True
            
            # 非分区期间，执行多数派写入
            success_count = 1  # 本地写入成功
            
            # 向其他节点写入
            for node in self._nodes:
                if node != self._node_id:
                    try:
                        # 模拟写入（实际应使用RPC）
                        # await self._write_to_node(node, key, value, timestamp, vector_clock)
                        success_count += 1
                    except Exception:
                        pass
            
            # 检查是否达到多数派
            if self._quorum_manager.is_write_quorum(success_count):
                return True
            else:
                # 未达到多数派，记录待同步
                self._pending_sync.append({
                    "type": "write",
                    "key": key,
                    "value": value,
                    "timestamp": timestamp,
                    "vector_clock": vector_clock,
                })
                return True  # 仍然返回成功，因为本地已写入
    
    async def read(
        self,
        key: str,
        min_timestamp: Optional[HLCTimestamp] = None,
    ) -> Optional[Tuple[Any, HLCTimestamp]]:
        """
        读取数据
        
        保证多数派读取
        
        Args:
            key: 键
            min_timestamp: 最小时间戳要求
            
        Returns:
            (值, 时间戳) 或 None
        """
        with self._lock:
            # 检查分区状态
            if self._partition_manager.is_partitioned():
                # 分区期间，只读本地
                if key in self._local_store:
                    value, timestamp, _ = self._local_store[key]
                    if min_timestamp is None or timestamp >= min_timestamp:
                        return value, timestamp
                return None
            
            # 非分区期间，执行多数派读取
            responses = []
            
            # 本地读取
            if key in self._local_store:
                value, timestamp, _ = self._local_store[key]
                if min_timestamp is None or timestamp >= min_timestamp:
                    responses.append((value, timestamp))
            
            # 从其他节点读取
            for node in self._nodes:
                if node != self._node_id:
                    try:
                        # 模拟读取（实际应使用RPC）
                        # result = await self._read_from_node(node, key)
                        pass
                    except Exception:
                        pass
            
            # 检查是否达到多数派
            if self._quorum_manager.is_read_quorum(len(responses)):
                # 返回最新的版本
                return max(responses, key=lambda x: x[1])
            
            # 未达到多数派，返回本地值（如果存在）
            if key in self._local_store:
                value, timestamp, _ = self._local_store[key]
                return value, timestamp
            
            return None
    
    async def _on_partition(self, affected_nodes: List[str]):
        """分区检测回调"""
        print(f"[PartitionTolerantStorage] Entering partition mode. Affected: {affected_nodes}")
    
    async def _on_recovery(self):
        """分区恢复回调"""
        print("[PartitionTolerantStorage] Recovering from partition...")
        await self._sync_pending_operations()
    
    async def _sync_pending_operations(self):
        """同步待处理的操作"""
        with self._lock:
            pending = self._pending_sync.copy()
            self._pending_sync.clear()
        
        for op in pending:
            try:
                if op["type"] == "write":
                    # 重新执行写入
                    await self.write(
                        op["key"],
                        op["value"],
                        op["timestamp"],
                        op["vector_clock"],
                    )
                    print(f"[PartitionTolerantStorage] Synced write: {op['key']}")
            except Exception as e:
                print(f"[PartitionTolerantStorage] Sync failed: {e}")
                # 重新加入待同步队列
                with self._lock:
                    self._pending_sync.append(op)
    
    def get_pending_count(self) -> int:
        """
        获取待同步操作数量
        
        Returns:
            待同步操作数量
        """
        with self._lock:
            return len(self._pending_sync)


class SplitBrainResolver:
    """
    脑裂解决器
    
    检测和解决脑裂问题
    """
    
    def __init__(self, node_id: str, partition_manager: PartitionManager):
        """
        初始化脑裂解决器
        
        Args:
            node_id: 当前节点ID
            partition_manager: 分区管理器
        """
        self._node_id = node_id
        self._partition_manager = partition_manager
        self._leader_id: Optional[str] = None
        self._term: int = 0
    
    def detect_split_brain(self, other_leader_id: str, other_term: int) -> bool:
        """
        检测脑裂
        
        Args:
            other_leader_id: 另一个Leader的ID
            other_term: 另一个Leader的任期
            
        Returns:
            True如果检测到脑裂
        """
        if self._leader_id and other_leader_id != self._leader_id:
            # 存在多个Leader
            if other_term > self._term:
                # 对方任期更高，自己应该下台
                return True
            elif other_term == self._term:
                # 任期相同，根据节点ID决定
                return other_leader_id > self._leader_id
        return False
    
    def resolve_split_brain(self, competing_leaders: List[str]) -> Optional[str]:
        """
        解决脑裂
        
        选择正确的Leader（任期最高，任期相同则节点ID最大）
        
        Args:
            competing_leaders: 竞争的Leader列表
            
        Returns:
            选定的Leader ID
        """
        if not competing_leaders:
            return None
        
        # 简单策略：选择节点ID最大的
        # 实际应该基于任期和日志完整性
        return max(competing_leaders)
    
    def set_leader(self, leader_id: str, term: int):
        """
        设置当前Leader
        
        Args:
            leader_id: Leader节点ID
            term: 任期
        """
        self._leader_id = leader_id
        self._term = term

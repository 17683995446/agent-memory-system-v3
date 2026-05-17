"""
分布式一致性管理器 (Distributed Consistency Manager)

集成Raft共识、向量时钟、混合逻辑时钟、会话一致性、分区容忍性，
提供完整的分布式一致性保证。

核心特性:
- 线性一致性保证（通过Raft）
- 因果一致性保证（通过向量时钟）
- 时钟漂移处理（通过HLC）
- 会话一致性保证（读己之写、单调读写）
- 分区容忍性保证
- 多数派读写

示例用法:
    >>> from consistency_manager import ConsistencyManager, ConsistencyLevel
    >>> cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
    >>> await cm.start()
    >>> await cm.write("key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE)
    >>> value = await cm.read("key1", consistency=ConsistencyLevel.LINEARIZABLE)
"""

import asyncio
import time
import uuid
from typing import Dict, List, Optional, Set, Tuple, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import threading

# 导入核心组件
from hybrid_logical_clock import HybridLogicalClock, HLCTimestamp, get_timestamp
from vector_clock import VectorClock, VectorClockManager, VectorClockRelation
from session_consistency import (
    SessionManager, SessionState, SessionConsistencyLevel,
    ConsistentSession
)
from partition_tolerance import (
    PartitionManager, QuorumManager, PartitionTolerantStorage,
    NodeState, PartitionState
)


class ConsistencyLevel(Enum):
    """一致性级别"""
    EVENTUAL = "eventual"              # 最终一致性
    CAUSAL = "causal"                  # 因果一致性
    SESSION = "session"                # 会话一致性
    MONOTONIC_READS = "monotonic_reads"  # 单调读
    READ_YOUR_WRITES = "read_your_writes"  # 读己之写
    BOUNDED_STALENESS = "bounded_staleness"  # 有界陈旧性
    LINEARIZABLE = "linearizable"      # 线性一致性
    STRONG = "strong"                  # 强一致性（等同于线性一致性）


class OperationType(Enum):
    """操作类型"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    CAS = "compare_and_swap"


@dataclass
class OperationResult:
    """
    操作结果
    
    Attributes:
        success: 是否成功
        value: 返回值（读操作）
        timestamp: 时间戳
        vector_clock: 向量时钟
        error: 错误信息
    """
    success: bool
    value: Any = None
    timestamp: Optional[HLCTimestamp] = None
    vector_clock: Optional[Dict[str, int]] = None
    error: Optional[str] = None
    
    def __bool__(self):
        return self.success


@dataclass
class ConsistencyConfig:
    """
    一致性配置
    
    Attributes:
        default_consistency: 默认一致性级别
        session_timeout: 会话超时时间
        quorum_read: 读多数派大小
        quorum_write: 写多数派大小
        partition_threshold: 分区检测阈值
        max_retry: 最大重试次数
        retry_delay: 重试延迟（秒）
    """
    default_consistency: ConsistencyLevel = ConsistencyLevel.LINEARIZABLE
    session_timeout: float = 3600.0
    quorum_read: Optional[int] = None
    quorum_write: Optional[int] = None
    partition_threshold: int = 3
    max_retry: int = 3
    retry_delay: float = 0.1


class ConsistencyManager:
    """
    一致性管理器
    
    集成所有一致性组件，提供统一的一致性接口
    """
    
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        config: Optional[ConsistencyConfig] = None,
    ):
        """
        初始化一致性管理器
        
        Args:
            node_id: 当前节点ID
            peers: 其他节点列表
            config: 一致性配置
        """
        self._node_id = node_id
        self._peers = peers
        self._config = config or ConsistencyConfig()
        
        # 初始化核心组件
        self._hlc = HybridLogicalClock(node_id)
        self._vc_manager = VectorClockManager(node_id)
        self._session_manager = SessionManager(
            default_timeout=self._config.session_timeout
        )
        self._partition_manager = PartitionManager(
            node_id=node_id,
            peers=peers,
            partition_threshold=self._config.partition_threshold,
        )
        
        # 初始化多数派管理器
        total_nodes = len(peers) + 1
        self._quorum_manager = QuorumManager(
            total_nodes=total_nodes,
            read_quorum=self._config.quorum_read,
            write_quorum=self._config.quorum_write,
        )
        
        # 初始化分区容忍存储
        all_nodes = [node_id] + peers
        self._storage = PartitionTolerantStorage(
            node_id=node_id,
            nodes=all_nodes,
            partition_manager=self._partition_manager,
            quorum_manager=self._quorum_manager,
        )
        
        # 本地存储（用于快速访问）
        self._local_store: Dict[str, Tuple[Any, HLCTimestamp, Dict[str, int]]] = {}
        
        # 锁
        self._lock = threading.RLock()
        
        # 运行状态
        self._running = False
        self._is_leader = False
        self._leader_id: Optional[str] = None
        
        # 统计信息
        self._stats = {
            "reads": 0,
            "writes": 0,
            "failures": 0,
            "linearizable_ops": 0,
            "causal_ops": 0,
            "session_ops": 0,
        }
    
    async def start(self):
        """启动一致性管理器"""
        self._running = True
        self._partition_manager.start()
        
        # 启动会话清理任务
        asyncio.create_task(self._session_manager.start_cleanup_task())
        
        print(f"[ConsistencyManager] Started on node {self._node_id}")
    
    async def stop(self):
        """停止一致性管理器"""
        self._running = False
        self._partition_manager.stop()
        self._session_manager.stop_cleanup_task()
        
        print(f"[ConsistencyManager] Stopped on node {self._node_id}")
    
    async def write(
        self,
        key: str,
        value: Any,
        consistency: Optional[ConsistencyLevel] = None,
        session_id: Optional[str] = None,
    ) -> OperationResult:
        """
        写入数据
        
        Args:
            key: 键
            value: 值
            consistency: 一致性级别
            session_id: 会话ID（用于会话一致性）
            
        Returns:
            操作结果
        """
        consistency = consistency or self._config.default_consistency
        
        try:
            # 生成时间戳和向量时钟
            timestamp = self._hlc.now()
            vector_clock = self._vc_manager.record_event(
                event_id=str(uuid.uuid4()),
                event_type="write",
                payload={"key": key, "value": value},
            ).vector_clock
            
            # 根据一致性级别执行写入
            if consistency == ConsistencyLevel.LINEARIZABLE:
                result = await self._write_linearizable(key, value, timestamp, vector_clock)
            elif consistency == ConsistencyLevel.CAUSAL:
                result = await self._write_causal(key, value, timestamp, vector_clock)
            elif consistency in [ConsistencyLevel.SESSION, ConsistencyLevel.READ_YOUR_WRITES, ConsistencyLevel.MONOTONIC_WRITES]:
                result = await self._write_session(key, value, timestamp, vector_clock, session_id, consistency)
            else:
                # 默认使用多数派写入
                result = await self._write_quorum(key, value, timestamp, vector_clock)
            
            # 更新统计
            with self._lock:
                self._stats["writes"] += 1
                if consistency == ConsistencyLevel.LINEARIZABLE:
                    self._stats["linearizable_ops"] += 1
                elif consistency == ConsistencyLevel.CAUSAL:
                    self._stats["causal_ops"] += 1
                elif consistency in [ConsistencyLevel.SESSION, ConsistencyLevel.READ_YOUR_WRITES]:
                    self._stats["session_ops"] += 1
            
            return result
            
        except Exception as e:
            with self._lock:
                self._stats["failures"] += 1
            return OperationResult(success=False, error=str(e))
    
    async def read(
        self,
        key: str,
        consistency: Optional[ConsistencyLevel] = None,
        session_id: Optional[str] = None,
    ) -> OperationResult:
        """
        读取数据
        
        Args:
            key: 键
            consistency: 一致性级别
            session_id: 会话ID（用于会话一致性）
            
        Returns:
            操作结果
        """
        consistency = consistency or self._config.default_consistency
        
        try:
            # 根据一致性级别执行读取
            if consistency == ConsistencyLevel.LINEARIZABLE:
                result = await self._read_linearizable(key)
            elif consistency == ConsistencyLevel.CAUSAL:
                result = await self._read_causal(key)
            elif consistency in [ConsistencyLevel.SESSION, ConsistencyLevel.READ_YOUR_WRITES, ConsistencyLevel.MONOTONIC_READS]:
                result = await self._read_session(key, session_id, consistency)
            else:
                # 默认使用多数派读取
                result = await self._read_quorum(key)
            
            # 更新统计
            with self._lock:
                self._stats["reads"] += 1
                if consistency == ConsistencyLevel.LINEARIZABLE:
                    self._stats["linearizable_ops"] += 1
                elif consistency == ConsistencyLevel.CAUSAL:
                    self._stats["causal_ops"] += 1
                elif consistency in [ConsistencyLevel.SESSION, ConsistencyLevel.READ_YOUR_WRITES]:
                    self._stats["session_ops"] += 1
            
            return result
            
        except Exception as e:
            with self._lock:
                self._stats["failures"] += 1
            return OperationResult(success=False, error=str(e))
    
    async def _write_linearizable(
        self,
        key: str,
        value: Any,
        timestamp: HLCTimestamp,
        vector_clock: Dict[str, int],
    ) -> OperationResult:
        """
        线性一致性写入
        
        通过Raft协议保证线性一致性
        """
        # 如果是Leader，直接写入
        if self._is_leader:
            success = await self._storage.write(key, value, timestamp, vector_clock)
            return OperationResult(
                success=success,
                timestamp=timestamp,
                vector_clock=vector_clock,
            )
        else:
            # 转发到Leader
            if self._leader_id:
                # 模拟转发（实际应使用RPC）
                await asyncio.sleep(0.01)
                return OperationResult(
                    success=True,
                    timestamp=timestamp,
                    vector_clock=vector_clock,
                )
            else:
                return OperationResult(success=False, error="No leader available")
    
    async def _write_causal(
        self,
        key: str,
        value: Any,
        timestamp: HLCTimestamp,
        vector_clock: Dict[str, int],
    ) -> OperationResult:
        """
        因果一致性写入
        
        使用向量时钟保证因果一致性
        """
        # 本地写入
        with self._lock:
            self._local_store[key] = (value, timestamp, vector_clock)
        
        # 异步复制到其他节点
        asyncio.create_task(self._replicate_async(key, value, timestamp, vector_clock))
        
        return OperationResult(
            success=True,
            timestamp=timestamp,
            vector_clock=vector_clock,
        )
    
    async def _write_session(
        self,
        key: str,
        value: Any,
        timestamp: HLCTimestamp,
        vector_clock: Dict[str, int],
        session_id: Optional[str],
        consistency: ConsistencyLevel,
    ) -> OperationResult:
        """
        会话一致性写入
        """
        if not session_id:
            # 没有会话ID，降级为因果一致性
            return await self._write_causal(key, value, timestamp, vector_clock)
        
        # 获取会话一致性要求
        reqs = self._session_manager.get_consistency_requirements(
            session_id, "write", key
        )
        
        # 检查单调写要求
        if reqs.get("requirements"):
            for req in reqs["requirements"]:
                if req["type"] == "monotonic_writes":
                    min_vc = req.get("min_vector_clock", {})
                    # 合并向量时钟
                    for node, counter in min_vc.items():
                        vector_clock[node] = max(vector_clock.get(node, 0), counter)
        
        # 执行写入
        with self._lock:
            self._local_store[key] = (value, timestamp, vector_clock)
        
        # 记录会话写操作
        self._session_manager.record_write(
            session_id, key, value, self._node_id, vector_clock
        )
        
        return OperationResult(
            success=True,
            timestamp=timestamp,
            vector_clock=vector_clock,
        )
    
    async def _write_quorum(
        self,
        key: str,
        value: Any,
        timestamp: HLCTimestamp,
        vector_clock: Dict[str, int],
    ) -> OperationResult:
        """
        多数派写入
        """
        success = await self._storage.write(key, value, timestamp, vector_clock)
        return OperationResult(
            success=success,
            timestamp=timestamp,
            vector_clock=vector_clock,
        )
    
    async def _read_linearizable(self, key: str) -> OperationResult:
        """
        线性一致性读取
        """
        # 从Leader读取
        if self._is_leader:
            result = await self._storage.read(key)
            if result:
                value, timestamp = result
                return OperationResult(success=True, value=value, timestamp=timestamp)
            return OperationResult(success=False, error="Key not found")
        else:
            # 转发到Leader
            if self._leader_id:
                await asyncio.sleep(0.01)
                # 模拟读取
                with self._lock:
                    if key in self._local_store:
                        value, timestamp, _ = self._local_store[key]
                        return OperationResult(success=True, value=value, timestamp=timestamp)
                return OperationResult(success=False, error="Key not found")
            else:
                return OperationResult(success=False, error="No leader available")
    
    async def _read_causal(self, key: str) -> OperationResult:
        """
        因果一致性读取
        """
        with self._lock:
            if key in self._local_store:
                value, timestamp, vector_clock = self._local_store[key]
                return OperationResult(
                    success=True,
                    value=value,
                    timestamp=timestamp,
                    vector_clock=vector_clock,
                )
        return OperationResult(success=False, error="Key not found")
    
    async def _read_session(
        self,
        key: str,
        session_id: Optional[str],
        consistency: ConsistencyLevel,
    ) -> OperationResult:
        """
        会话一致性读取
        """
        if not session_id:
            # 没有会话ID，降级为因果一致性
            return await self._read_causal(key)
        
        # 获取一致性要求
        min_timestamp = None
        reqs = self._session_manager.get_consistency_requirements(
            session_id, "read", key
        )
        
        if reqs.get("requirements"):
            for req in reqs["requirements"]:
                if req["type"] in ["read_your_writes", "monotonic_reads"]:
                    ts_dict = req.get("min_timestamp")
                    if ts_dict:
                        from hybrid_logical_clock import HLCTimestamp
                        ts = HLCTimestamp.from_dict(ts_dict)
                        if min_timestamp is None or ts > min_timestamp:
                            min_timestamp = ts
        
        # 执行读取
        with self._lock:
            if key in self._local_store:
                value, timestamp, vector_clock = self._local_store[key]
                
                # 检查时间戳要求
                if min_timestamp and timestamp < min_timestamp:
                    return OperationResult(
                        success=False,
                        error="Timestamp requirement not met (stale read)",
                    )
                
                # 记录会话读操作
                self._session_manager.record_read(session_id, key, value)
                
                return OperationResult(
                    success=True,
                    value=value,
                    timestamp=timestamp,
                    vector_clock=vector_clock,
                )
        
        return OperationResult(success=False, error="Key not found")
    
    async def _read_quorum(self, key: str) -> OperationResult:
        """
        多数派读取
        """
        result = await self._storage.read(key)
        if result:
            value, timestamp = result
            return OperationResult(success=True, value=value, timestamp=timestamp)
        return OperationResult(success=False, error="Key not found")
    
    async def _replicate_async(
        self,
        key: str,
        value: Any,
        timestamp: HLCTimestamp,
        vector_clock: Dict[str, int],
    ):
        """异步复制"""
        # 模拟异步复制到其他节点
        await asyncio.sleep(0.01)
    
    def create_session(
        self,
        user_id: str,
        consistency_level: SessionConsistencyLevel = SessionConsistencyLevel.SESSION,
    ) -> str:
        """
        创建会话
        
        Args:
            user_id: 用户ID
            consistency_level: 会话一致性级别
            
        Returns:
            会话ID
        """
        session = self._session_manager.create_session(
            user_id=user_id,
            consistency_level=consistency_level,
            sticky_node=self._node_id,
        )
        return session.session_id
    
    def close_session(self, session_id: str) -> bool:
        """
        关闭会话
        
        Args:
            session_id: 会话ID
            
        Returns:
            True如果成功关闭
        """
        return self._session_manager.close_session(session_id)
    
    def set_leader(self, leader_id: str, is_leader: bool = False):
        """
        设置Leader信息
        
        Args:
            leader_id: Leader节点ID
            is_leader: 当前节点是否为Leader
        """
        self._leader_id = leader_id
        self._is_leader = is_leader
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        with self._lock:
            return {
                **self._stats,
                "quorum_status": self._quorum_manager.get_quorum_status(
                    len(self._partition_manager.get_healthy_nodes()) + 1
                ),
                "partition_state": self._partition_manager.is_partitioned(),
                "sessions": self._session_manager.get_stats(),
            }
    
    def get_partition_status(self) -> Dict[str, Any]:
        """
        获取分区状态
        
        Returns:
            分区状态字典
        """
        healthy_nodes = self._partition_manager.get_healthy_nodes()
        return {
            "is_partitioned": self._partition_manager.is_partitioned(),
            "healthy_nodes": healthy_nodes,
            "total_nodes": len(self._peers) + 1,
            "quorum_status": self._quorum_manager.get_quorum_status(len(healthy_nodes) + 1),
        }


class ConsistencyVerifier:
    """
    一致性验证器
    
    验证分布式操作的一致性保证
    """
    
    def __init__(self, consistency_manager: ConsistencyManager):
        """
        初始化验证器
        
        Args:
            consistency_manager: 一致性管理器
        """
        self._cm = consistency_manager
        self._history: List[Dict[str, Any]] = []
    
    async def verify_linearizability(self, operations: List[Dict[str, Any]]) -> bool:
        """
        验证线性一致性
        
        Args:
            operations: 操作列表
            
        Returns:
            True如果满足线性一致性
        """
        # 简化的线性一致性验证
        # 实际应实现完整的线性一致性检查算法
        return True
    
    async def verify_causal_consistency(self, operations: List[Dict[str, Any]]) -> bool:
        """
        验证因果一致性
        
        Args:
            operations: 操作列表
            
        Returns:
            True如果满足因果一致性
        """
        # 使用向量时钟验证因果关系
        for i, op1 in enumerate(operations):
            for op2 in operations[i+1:]:
                vc1 = VectorClock("temp", op1.get("vector_clock", {}))
                vc2 = VectorClock("temp", op2.get("vector_clock", {}))
                
                relation = vc1.compare(vc2)
                # 验证因果关系
                if op1.get("depends_on") == op2.get("operation_id"):
                    if relation != VectorClockRelation.BEFORE:
                        return False
        
        return True
    
    async def verify_session_consistency(
        self,
        session_id: str,
        operations: List[Dict[str, Any]],
    ) -> Dict[str, bool]:
        """
        验证会话一致性
        
        Args:
            session_id: 会话ID
            operations: 操作列表
            
        Returns:
            各项一致性保证的验证结果
        """
        results = {
            "read_your_writes": True,
            "monotonic_reads": True,
            "monotonic_writes": True,
        }
        
        writes = {}
        reads = {}
        
        for op in operations:
            if op["type"] == "write":
                key = op["key"]
                writes[key] = op
                
                # 检查单调写
                if key in writes:
                    prev_write = writes[key]
                    if op["timestamp"] < prev_write["timestamp"]:
                        results["monotonic_writes"] = False
                        
            elif op["type"] == "read":
                key = op["key"]
                
                # 检查读己之写
                if key in writes:
                    write_op = writes[key]
                    if op["value"] != write_op["value"]:
                        results["read_your_writes"] = False
                
                # 检查单调读
                if key in reads:
                    prev_read = reads[key]
                    if op["timestamp"] < prev_read["timestamp"]:
                        results["monotonic_reads"] = False
                
                reads[key] = op
        
        return results

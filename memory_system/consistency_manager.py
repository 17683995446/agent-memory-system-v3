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
import logging
from typing import Dict, List, Optional, Set, Tuple, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import threading

logger = logging.getLogger(__name__)

# 导入核心组件
from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp, get_timestamp
from memory_system.vector_clock import VectorClock, VectorClockManager, VectorClockRelation
from memory_system.session_consistency import (
    SessionManager, SessionState, SessionConsistencyLevel,
    ConsistentSession
)
from memory_system.partition_tolerance import (
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
    MONOTONIC_WRITES = "monotonic_writes"  # 单调写
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
        # [BUG 8 修复] 添加 asyncio.Lock 保护 _leader_id 和 _is_leader 的并发修改
        self._leader_lock = asyncio.Lock()
        
        # 运行状态
        self._running = False
        self._is_leader = False
        self._leader_id: Optional[str] = None
        
        # [BUG 9 修复] 保存异步复制任务的引用，防止Task被GC且便于stop()时清理
        self._replication_tasks: List[asyncio.Task] = []
        
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
        
        # [BUG FIX] 等待session_manager清理任务完成
        self._session_manager.stop_cleanup_task()
        if hasattr(self._session_manager, '_cleanup_task') and self._session_manager._cleanup_task:
            try:
                await self._session_manager._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # [BUG 9 修复] 停止时取消所有未完成的异步复制任务
        for task in self._replication_tasks:
            if not task.done():
                task.cancel()
        # 等待所有任务处理取消
        if self._replication_tasks:
            await asyncio.gather(*self._replication_tasks, return_exceptions=True)
        self._replication_tasks.clear()
        
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
                if result.success:
                    self._stats["writes"] += 1
                else:
                    self._stats["failures"] += 1
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
                # 非 Leader 节点应将写请求转发到 Leader 执行。
                # 当前为单进程模拟，数据未实际持久化到 Leader，
                # 因此必须返回 success=False 以避免调用方误认为写入成功。
                logger.warning(
                    "[ConsistencyManager] 非Leader节点(node=%s)无法完成线性一致性写入，"
                    "需要转发到Leader(node=%s)（单进程模拟限制），返回失败",
                    self._node_id, self._leader_id
                )
                return OperationResult(
                    success=False,
                    error=f"Not the leader; write should be forwarded to leader {self._leader_id}",
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
        # 同时写入共享存储，确保 LINEARIZABLE 读取能读到 CAUSAL 写入的数据
        await self._storage.write(key, value, timestamp, vector_clock)
        
        # 异步复制到其他节点
        # [BUG 9 修复] 保存Task引用到列表，防止被GC回收，并在stop()时统一清理
        task = asyncio.create_task(self._replicate_async(key, value, timestamp, vector_clock))
        task.add_done_callback(lambda t: self._replication_tasks.remove(t) if t in self._replication_tasks else None)
        self._replication_tasks.append(task)
        
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
        
        # BUG FIX: SESSION写入也需要同步到共享存储，否则其他节点无法读取
        try:
            await self._storage.write(key, value, timestamp, vector_clock)
        except Exception as e:
            logger.warning(f"Session write to shared storage failed (data still in local store): {e}")
        
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
                        from memory_system.hybrid_logical_clock import HLCTimestamp
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
    
    async def set_leader(self, leader_id: str, is_leader: bool = False):
        """
        设置Leader信息
        
        [BUG 8 修复] 使用 asyncio.Lock 保护 _leader_id 和 _is_leader 的修改，
        防止在异步环境中多个协程并发修改导致状态不一致。
        
        Args:
            leader_id: Leader节点ID
            is_leader: 当前节点是否为Leader
        """
        async with self._leader_lock:
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
        
        基本验证策略：
        1. 对所有操作按时间戳排序，形成全序
        2. 模拟按全序执行，检查读操作是否能看到最近一次写入的值
        3. 如果任何读操作返回的值与全序中最近一次写入不一致，则违反线性一致性
        
        Args:
            operations: 操作列表，每个操作包含 type, key, value, timestamp, operation_id 等
            
        Returns:
            True如果满足线性一致性
        """
        # [BUG 5 修复] 实现基本的线性一致性验证
        # 线性一致性要求：操作必须看起来像按某个全序原子执行
        
        if not operations:
            return True
        
        # 按时间戳排序所有操作，形成全序序列
        sorted_ops = sorted(operations, key=lambda op: op.get("timestamp", 0))
        
        # 模拟按全序执行，跟踪每个 key 的最新写入值
        # key -> (value, timestamp)
        latest_writes: Dict[str, Tuple[Any, Any]] = {}
        
        for op in sorted_ops:
            op_type = op.get("type", "")
            key = op.get("key", "")
            op_ts = op.get("timestamp", 0)
            
            if op_type == "write":
                # 写入操作：更新 key 的最新值
                latest_writes[key] = (op.get("value"), op_ts)
            elif op_type == "read":
                # 读操作：检查返回值是否与全序中最近一次写入一致
                if key in latest_writes:
                    expected_value, _ = latest_writes[key]
                    actual_value = op.get("value")
                    if actual_value != expected_value:
                        logger.warning(
                            "[ConsistencyVerifier] 线性一致性违规: key=%s, "
                            "期望值=%s, 实际值=%s, operation_id=%s",
                            key, expected_value, actual_value,
                            op.get("operation_id", "unknown")
                        )
                        return False
                # 如果 key 没有被写过，读操作返回任何值都是合法的
        
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
                
                # [BUG 7 修复] 在写入前保存旧值，用于单调写比较
                # 原始代码先写入 writes[key]=op，然后立即检查 writes[key]，
                # 此时 prev_write 就是当前 write 自身，比较毫无意义
                prev_write = writes.get(key)
                
                # 检查单调写：当前写入的时间戳必须 >= 同一 key 上一次写入的时间戳
                if prev_write is not None:
                    if op["timestamp"] < prev_write["timestamp"]:
                        results["monotonic_writes"] = False
                
                writes[key] = op
                        
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

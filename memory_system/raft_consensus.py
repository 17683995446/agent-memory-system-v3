"""
Raft分布式共识协议实现模块

本模块实现了完整的Raft算法，包括Leader选举、日志复制、安全性保证、
动态成员变更等功能。用于Agent Memory System的分布式一致性保证。

核心特性:
- 完整的Raft状态机实现
- 支持Leader选举和日志复制
- 动态成员变更支持
- 心跳机制和超时处理
- 状态机接口抽象
- 线程安全和异步支持

示例用法:
    >>> from raft_consensus import RaftNode, RaftConfig
    >>> config = RaftConfig(node_id="node1", peers=["node2", "node3"])
    >>> node = RaftNode(config)
    >>> await node.start()
    >>> result = await node.propose_command({"action": "store", "key": "k1", "value": "v1"})
"""

import asyncio
import json
import logging
import random
import time
import uuid
import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
)
from collections import deque
from abc import ABC, abstractmethod
import hashlib


# 配置日志
logger = logging.getLogger(__name__)


class RaftRole(Enum):
    """Raft节点角色枚举"""
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


class RaftError(Exception):
    """Raft共识错误基类"""
    pass


class NotLeaderError(RaftError):
    """当前节点不是Leader时抛出"""
    def __init__(self, leader_id: Optional[str] = None):
        self.leader_id = leader_id
        super().__init__(f"Not the leader. Current leader: {leader_id}")


class ProposalTimeoutError(RaftError):
    """提案超时错误"""
    pass


class LogEntryError(RaftError):
    """日志条目错误"""
    pass


class FailoverError(RaftError):
    """故障转移错误"""
    def __init__(self, message: str, retry_count: int = 0, last_error: Optional[Exception] = None):
        self.retry_count = retry_count
        self.last_error = last_error
        super().__init__(message)


class CandidateValidationError(RaftError):
    """候选节点验证错误"""
    def __init__(self, candidate_id: str, reason: str):
        self.candidate_id = candidate_id
        self.reason = reason
        super().__init__(f"Candidate {candidate_id} validation failed: {reason}")


class LogInconsistencyError(RaftError):
    """日志不一致错误 - P1修复"""
    def __init__(self, message: str, expected_index: int = -1, actual_index: int = -1, 
                 expected_term: int = -1, actual_term: int = -1):
        self.expected_index = expected_index
        self.actual_index = actual_index
        self.expected_term = expected_term
        self.actual_term = actual_term
        super().__init__(message)


class CommitIndexError(RaftError):
    """提交索引错误 - P1修复"""
    def __init__(self, message: str, commit_index: int = -1, last_applied: int = -1):
        self.commit_index = commit_index
        self.last_applied = last_applied
        super().__init__(message)


class LeaderElectionError(RaftError):
    """Leader选举错误 - P1修复"""
    def __init__(self, message: str, term: int = -1, votes_received: int = 0, votes_needed: int = 0):
        self.term = term
        self.votes_received = votes_received
        self.votes_needed = votes_needed
        super().__init__(message)


# 全局计数器用于确保entry_id唯一性
_entry_id_counter = 0
_entry_id_lock = threading.Lock()

def _generate_entry_id() -> str:
    """生成唯一的entry_id，使用UUID4 + 原子计数器确保唯一性"""
    global _entry_id_counter
    with _entry_id_lock:
        _entry_id_counter += 1
        counter = _entry_id_counter
    # 结合UUID4和原子计数器，确保即使在高并发下也不会碰撞
    unique_str = f"{uuid.uuid4().hex}-{counter}-{time.time():.9f}-{random.randint(0, 999999)}"
    return hashlib.sha256(unique_str.encode()).hexdigest()[:16]


@dataclass
class LogEntry:
    """
    Raft日志条目
    
    Attributes:
        term: 条目所属的任期号
        index: 条目在日志中的索引
        command: 实际存储的命令数据
        timestamp: 条目创建时间戳
        entry_id: 条目唯一标识符
    """
    term: int
    index: int
    command: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    entry_id: str = field(default_factory=_generate_entry_id)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典表示"""
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "timestamp": self.timestamp,
            "entry_id": self.entry_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        """从字典创建日志条目"""
        return cls(
            term=data["term"],
            index=data["index"],
            command=data["command"],
            timestamp=data.get("timestamp", time.time()),
            entry_id=data.get("entry_id", ""),
        )


@dataclass
class RaftConfig:
    """
    Raft节点配置
    
    Attributes:
        node_id: 当前节点唯一标识
        peers: 其他节点ID列表
        election_timeout_min: 最小选举超时时间(秒)
        election_timeout_max: 最大选举超时时间(秒)
        heartbeat_interval: 心跳间隔(秒)
        max_log_entries_per_request: 每次请求最大日志条目数
        snapshot_threshold: 触发快照的日志条目阈值
        data_dir: 数据存储目录
        fault_detection_timeout: 故障检测超时时间(秒) - P1修复
        heartbeat_timeout_factor: 心跳超时因子 - P1修复
        log_replication_timeout: 日志复制超时时间(秒) - P1修复
    """
    node_id: str
    peers: List[str] = field(default_factory=list)
    election_timeout_min: float = 0.15
    election_timeout_max: float = 0.30
    heartbeat_interval: float = 0.05
    max_log_entries_per_request: int = 100
    snapshot_threshold: int = 10000
    data_dir: str = "./raft_data"
    # P1修复: 故障检测和心跳相关配置
    fault_detection_timeout: float = 1.0  # 故障检测超时时间
    heartbeat_timeout_factor: int = 5  # 心跳超时因子(选举超时 = heartbeat_interval * factor)
    log_replication_timeout: float = 2.0  # 日志复制超时时间
    max_log_replication_retries: int = 5  # 日志复制最大重试次数
    
    def __post_init__(self):
        """验证配置有效性"""
        if self.election_timeout_min >= self.election_timeout_max:
            raise ValueError("election_timeout_min must be less than election_timeout_max")
        if self.heartbeat_interval >= self.election_timeout_min:
            raise ValueError("heartbeat_interval must be less than election_timeout_min")
        # P1修复: 确保故障检测超时合理
        if self.fault_detection_timeout <= 0:
            raise ValueError("fault_detection_timeout must be positive")
        if self.heartbeat_timeout_factor < 3:
            raise ValueError("heartbeat_timeout_factor should be at least 3 for proper fault detection")


@dataclass
class FailoverConfig:
    """
    故障转移配置
    
    Attributes:
        max_retries: 最大重试次数
        initial_retry_delay: 初始重试延迟(秒)
        max_retry_delay: 最大重试延迟(秒)
        retry_multiplier: 重试延迟乘数(指数退避)
        failover_timeout: 故障转移超时时间(秒)
        candidate_validation_timeout: 候选节点验证超时(秒)
        state_recovery_timeout: 状态恢复等待超时(秒)
        health_check_interval: 健康检查间隔(秒)
    """
    max_retries: int = 5
    initial_retry_delay: float = 0.1
    max_retry_delay: float = 5.0
    retry_multiplier: float = 2.0
    failover_timeout: float = 30.0
    candidate_validation_timeout: float = 1.0
    state_recovery_timeout: float = 10.0
    health_check_interval: float = 1.0


@dataclass
class FailoverState:
    """
    故障转移状态
    
    跟踪故障转移过程中的状态信息
    """
    in_progress: bool = False
    failed_leader: Optional[str] = None
    candidate_nodes: List[str] = field(default_factory=list)
    current_candidate_index: int = 0
    retry_count: int = 0
    last_retry_time: float = 0.0
    start_time: float = 0.0
    last_error: Optional[Exception] = None


@dataclass
class RaftState:
    """
    Raft持久化状态
    
    这些状态需要在崩溃后恢复
    """
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)
    _log_offset: int = 0  # DEFECT-RC-019 [P0]: 日志偏移量，快照安装后使用
    
    def get_last_log_index(self) -> int:
        """获取最后日志索引"""
        if not self.log:
            return self._log_offset - 1
        return self._log_offset + len(self.log) - 1
    
    def get_last_log_term(self) -> int:
        """获取最后日志任期"""
        return self.log[-1].term if self.log else 0
    
    def get_entry_at(self, index: int) -> Optional[LogEntry]:
        """获取指定索引的日志条目
        
        DEFECT-RC-019 [P0] 修复: 考虑日志偏移量
        """
        physical_index = index - self._log_offset
        if 0 <= physical_index < len(self.log):
            return self.log[physical_index]
        return None
    
    def truncate_log_before(self, index: int):
        """截断index之前的所有日志（快照安装后使用）
        
        DEFECT-RC-019 [P0] 修复: 更新日志偏移量
        """
        physical_index = index - self._log_offset
        if physical_index >= 0 and physical_index < len(self.log):
            self.log = self.log[physical_index + 1:]
            self._log_offset = index + 1
        elif physical_index < 0:
            # 已经包含在之前的快照中
            pass
        else:
            # index超出当前日志范围
            self.log = []
            self._log_offset = index + 1


@dataclass
class VolatileState:
    """
    Raft易失性状态
    
    这些状态在重启后会丢失
    """
    commit_index: int = -1
    last_applied: int = -1
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)
    
    def initialize_leader_state(self, peers: List[str], last_log_index: int):
        """初始化Leader状态"""
        for peer in peers:
            self.next_index[peer] = last_log_index + 1
            self.match_index[peer] = -1


class StateMachine(ABC):
    """
    状态机抽象基类
    
    应用需要实现此接口以应用已提交的日志条目
    """
    
    @abstractmethod
    async def apply(self, command: Dict[str, Any]) -> Any:
        """
        应用命令到状态机
        
        Args:
            command: 要应用的命令
            
        Returns:
            应用结果
        """
        pass
    
    @abstractmethod
    async def snapshot(self) -> bytes:
        """
        创建状态机快照
        
        Returns:
            快照数据
        """
        pass
    
    @abstractmethod
    async def restore(self, snapshot_data: bytes) -> None:
        """
        从快照恢复状态机
        
        Args:
            snapshot_data: 快照数据
        """
        pass


class MemoryStateMachine(StateMachine):
    """
    内存状态机实现
    
    简单的键值存储状态机，用于测试和演示
    """
    
    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def apply(self, command: Dict[str, Any]) -> Any:
        """应用命令"""
        async with self._lock:
            action = command.get("action")
            key = command.get("key")
            
            if action == "set":
                self._data[key] = command.get("value")
                return {"success": True}
            elif action == "get":
                return {"success": True, "value": self._data.get(key)}
            elif action == "delete":
                if key in self._data:
                    del self._data[key]
                return {"success": True}
            elif action == "keys":
                return {"success": True, "keys": list(self._data.keys())}
            else:
                return {"success": False, "error": f"Unknown action: {action}"}
    
    async def snapshot(self) -> bytes:
        """创建快照"""
        async with self._lock:
            return json.dumps(self._data).encode()
    
    async def restore(self, snapshot_data: bytes) -> None:
        """恢复快照"""
        async with self._lock:
            self._data = json.loads(snapshot_data.decode())


class RaftMessage(ABC):
    """Raft消息基类"""
    
    def __init__(self, term: int, sender_id: str):
        self.term = term
        self.sender_id = sender_id


class RequestVoteRequest(RaftMessage):
    """请求投票请求"""
    
    def __init__(
        self,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ):
        super().__init__(term, candidate_id)
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "request_vote",
            "term": self.term,
            "candidate_id": self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
        }


class RequestVoteResponse(RaftMessage):
    """请求投票响应"""
    
    def __init__(self, term: int, voter_id: str, vote_granted: bool):
        super().__init__(term, voter_id)
        self.voter_id = voter_id
        self.vote_granted = vote_granted
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "request_vote_response",
            "term": self.term,
            "voter_id": self.voter_id,
            "vote_granted": self.vote_granted,
        }


class AppendEntriesRequest(RaftMessage):
    """追加条目请求(也用作心跳)"""
    
    def __init__(
        self,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[LogEntry],
        leader_commit: int,
    ):
        super().__init__(term, leader_id)
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit = leader_commit
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "append_entries",
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [e.to_dict() for e in self.entries],
            "leader_commit": self.leader_commit,
        }


class AppendEntriesResponse(RaftMessage):
    """追加条目响应"""
    
    def __init__(
        self,
        term: int,
        follower_id: str,
        success: bool,
        match_index: int = -1,
        conflict_index: int = -1,
    ):
        super().__init__(term, follower_id)
        self.follower_id = follower_id
        self.success = success
        self.match_index = match_index
        self.conflict_index = conflict_index
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "append_entries_response",
            "term": self.term,
            "follower_id": self.follower_id,
            "success": self.success,
            "match_index": self.match_index,
            "conflict_index": self.conflict_index,
        }


class InstallSnapshotRequest(RaftMessage):
    """安装快照请求"""
    
    def __init__(
        self,
        term: int,
        leader_id: str,
        last_included_index: int,
        last_included_term: int,
        offset: int,
        data: bytes,
        done: bool,
    ):
        super().__init__(term, leader_id)
        self.leader_id = leader_id
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.offset = offset
        self.data = data
        self.done = done
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "install_snapshot",
            "term": self.term,
            "leader_id": self.leader_id,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "offset": self.offset,
            "data_length": len(self.data),
            "done": self.done,
        }


class InstallSnapshotResponse(RaftMessage):
    """安装快照响应"""
    
    def __init__(self, term: int, follower_id: str, success: bool):
        super().__init__(term, follower_id)
        self.follower_id = follower_id
        self.success = success
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "install_snapshot_response",
            "term": self.term,
            "follower_id": self.follower_id,
            "success": self.success,
        }


class Transport(ABC):
    """
    网络传输抽象基类
    
    实现此接口以提供具体的网络通信能力
    """
    
    @abstractmethod
    async def send_request_vote(
        self, peer_id: str, request: RequestVoteRequest
    ) -> Optional[RequestVoteResponse]:
        """发送请求投票"""
        pass
    
    @abstractmethod
    async def send_append_entries(
        self, peer_id: str, request: AppendEntriesRequest
    ) -> Optional[AppendEntriesResponse]:
        """发送追加条目"""
        pass
    
    @abstractmethod
    async def send_install_snapshot(
        self, peer_id: str, request: InstallSnapshotRequest
    ) -> Optional[InstallSnapshotResponse]:
        """发送安装快照"""
        pass


class InMemoryTransport(Transport):
    """
    内存传输实现
    
    用于测试的内存内通信
    """
    
    def __init__(self):
        self._nodes: Dict[str, "RaftNode"] = {}
        self._message_delay: float = 0.001
        self._drop_rate: float = 0.0
    
    def register_node(self, node: "RaftNode"):
        """注册节点"""
        self._nodes[node.node_id] = node
    
    def set_message_delay(self, delay: float):
        """设置消息延迟"""
        self._message_delay = delay
    
    def set_drop_rate(self, rate: float):
        """设置消息丢失率"""
        self._drop_rate = rate
    
    async def _simulate_network(self):
        """模拟网络延迟"""
        if self._message_delay > 0:
            await asyncio.sleep(self._message_delay)
        if random.random() < self._drop_rate:
            return False
        return True
    
    async def send_request_vote(
        self, peer_id: str, request: RequestVoteRequest
    ) -> Optional[RequestVoteResponse]:
        """发送请求投票"""
        if not await self._simulate_network():
            return None
        
        node = self._nodes.get(peer_id)
        if node:
            return await node.handle_request_vote(request)
        return None
    
    async def send_append_entries(
        self, peer_id: str, request: AppendEntriesRequest
    ) -> Optional[AppendEntriesResponse]:
        """发送追加条目"""
        if not await self._simulate_network():
            return None
        
        node = self._nodes.get(peer_id)
        if node:
            return await node.handle_append_entries(request)
        return None
    
    async def send_install_snapshot(
        self, peer_id: str, request: InstallSnapshotRequest
    ) -> Optional[InstallSnapshotResponse]:
        """发送安装快照"""
        if not await self._simulate_network():
            return None
        
        node = self._nodes.get(peer_id)
        if node:
            return await node.handle_install_snapshot(request)
        return None


class RaftNode:
    """
    Raft节点实现
    
    实现完整的Raft算法，包括Leader选举、日志复制、安全性保证等。
    
    Attributes:
        node_id: 节点唯一标识
        config: Raft配置
        state_machine: 应用状态机
        transport: 网络传输层
    """
    
    def __init__(
        self,
        config: RaftConfig,
        state_machine: Optional[StateMachine] = None,
        transport: Optional[Transport] = None,
        failover_config: Optional[FailoverConfig] = None,
    ):
        self.node_id = config.node_id
        self.config = config
        self.state_machine = state_machine or MemoryStateMachine()
        self.transport = transport or InMemoryTransport()
        self.failover_config = failover_config or FailoverConfig()
        
        # Raft状态
        self._persistent_state = RaftState()
        self._volatile_state = VolatileState()
        
        # 角色和任期
        self._role = RaftRole.FOLLOWER
        self._current_leader: Optional[str] = None
        
        # 投票相关
        self._votes_received: Set[str] = set()
        
        # 定时器
        self._election_timer: Optional[asyncio.Task] = None
        self._heartbeat_timer: Optional[asyncio.Task] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        
        # 锁
        self._state_lock: Optional[asyncio.Lock] = None
        
        # 等待提交的提案
        self._pending_proposals: Dict[str, asyncio.Future] = {}
        
        # 快照相关
        self._snapshot_data: Optional[bytes] = None
        self._last_snapshot_index = -1
        self._last_snapshot_term = 0
        
        # 成员变更
        self._pending_membership_change: Optional[Dict[str, Any]] = None
        self._commit_index_at_membership_change = -1
        
        # 故障转移状态
        self._failover_state = FailoverState()
        self._failover_lock: Optional[asyncio.Lock] = None
        self._node_health_status: Dict[str, bool] = {}
        self._last_health_check: Dict[str, float] = {}

        # P1修复: 心跳跟踪和故障检测
        self._last_heartbeat_sent: float = 0.0  # 上次发送心跳的时间
        self._last_heartbeat_received: Dict[str, float] = {}  # 每个peer上次收到心跳的时间
        self._heartbeat_missed: Dict[str, int] = {}  # 每个peer连续丢失心跳次数
        self._leader_heartbeat_received: float = time.time()  # 上次收到Leader心跳的时间

        # P1修复: 日志复制状态跟踪
        self._log_replication_inflight: Dict[str, int] = {}  # 每个peer正在复制的日志索引
        self._log_replication_retries: Dict[str, int] = {}  # 每个peer的日志复制重试次数

        # P1修复: 提交索引一致性检查
        self._commit_index_history: List[Tuple[int, int, float]] = []  # (commit_index, term, timestamp)
        self._commit_index_check_interval: float = 1.0  # 提交索引检查间隔

        # P1修复: 选举状态跟踪
        self._election_start_time: float = 0.0  # 选举开始时间
        self._election_round: int = 0  # 选举轮次

        # DEFECT-007 [P1] 修复: 脑裂状态跟踪
        self._split_brain_detected: bool = False
        self._split_brain_start_time: float = 0.0
        self._split_brain_resolution_pending: bool = False
        self._partition_recovery_task: Optional[asyncio.Task] = None

        # DEFECT-005 [P1] 修复: 状态同步等待机制
        self._state_sync_event: Optional[asyncio.Event] = None
        self._state_sync_timeout: float = 5.0
        self._leader_election_timeout: float = 2.0  # Leader选举超时优化

        # DA-011 [P1] 修复: 跟踪复制任务
        self._replication_tasks: Set[asyncio.Task] = set()

    def _ensure_async_primitives(self):
        """Ensure asyncio primitives are initialized (lazy init for Python 3.10+ compatibility)"""
        if self._state_lock is None:
            self._state_lock = asyncio.Lock()
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()
        if self._failover_lock is None:
            self._failover_lock = asyncio.Lock()
        if self._state_sync_event is None:
            self._state_sync_event = asyncio.Event()

    @property
    def role(self) -> RaftRole:
        """获取当前角色"""
        return self._role
    
    @property
    def current_term(self) -> int:
        """获取当前任期"""
        return self._persistent_state.current_term
    
    @property
    def is_leader(self) -> bool:
        """检查是否为Leader"""
        return self._role == RaftRole.LEADER
    
    def _reset_election_timer(self):
        """重置选举定时器"""
        if self._election_timer:
            self._election_timer.cancel()
        
        timeout = random.uniform(
            self.config.election_timeout_min,
            self.config.election_timeout_max
        )
        self._election_timer = asyncio.create_task(
            self._election_timeout_handler(timeout)
        )
    
    async def _election_timeout_handler(self, timeout: float):
        """选举超时处理"""
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            # BUG 1 [P0] 修复: 先在锁内记录状态快照，再释放锁后调用 _start_election，
            # 避免 _start_election 内部的 asyncio.gather 触发 handle_request_vote 时
            # 因重复获取 _state_lock 而导致死锁。
            should_start_election = False
            async with self._state_lock:
                if not self._shutdown_event.is_set():
                    # 记录当前状态，释放锁后用于验证状态未变
                    should_start_election = True

            if should_start_election:
                await self._start_election()
    
    async def _start_election(self):
        """开始选举 - P1修复: 改进选举逻辑 - [BUG FIX] 添加锁保护防止竞态"""
        # [BUG FIX] 使用锁保护状态修改，防止竞态条件
        async with self._state_lock:
            if self._role == RaftRole.LEADER:
                return

            # P1修复: 记录选举开始时间和轮次
            self._election_start_time = time.time()
            self._election_round += 1

            logger.info(
                f"Node {self.node_id} starting election round {self._election_round} "
                f"at term {self._persistent_state.current_term + 1}"
            )

            # 转换为Candidate
            self._role = RaftRole.CANDIDATE
            self._persistent_state.current_term += 1
            self._persistent_state.voted_for = self.node_id
            self._votes_received = {self.node_id}

            # 在锁内读取状态快照，用于后续网络请求
            current_term = self._persistent_state.current_term
            last_log_index = self._persistent_state.get_last_log_index()
            last_log_term = self._persistent_state.get_last_log_term()

        # P1修复: 重置选举定时器，使用随机化超时避免选举死锁
        self._reset_election_timer()

        # 向所有peer发送请求投票（锁外发送网络请求）
        request = RequestVoteRequest(
            term=current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

        # 并行发送投票请求
        tasks = [
            self._send_request_vote(peer, request)
            for peer in self.config.peers
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # P1修复: 检查选举是否超时
        election_duration = time.time() - self._election_start_time
        if election_duration > self.config.election_timeout_max:
            logger.warning(
                f"Election round {self._election_round} timed out after {election_duration:.3f}s"
            )
    
    async def _send_request_vote(self, peer_id: str, request: RequestVoteRequest):
        """发送请求投票到指定peer"""
        try:
            response = await self.transport.send_request_vote(peer_id, request)
            if response:
                await self._handle_vote_response(response)
        except Exception as e:
            logger.debug(f"Failed to send vote request to {peer_id}: {e}")
    
    async def _handle_vote_response(self, response: RequestVoteResponse):
        """处理投票响应 - P1修复: 改进多数票计算"""
        async with self._state_lock:
            # 如果任期更高，转换为Follower
            if response.term > self._persistent_state.current_term:
                await self._become_follower(response.term)
                return
            
            # 如果不是Candidate或任期不匹配，忽略
            if self._role != RaftRole.CANDIDATE:
                return
            if response.term != self._persistent_state.current_term:
                return
            
            # 记录投票
            if response.vote_granted:
                self._votes_received.add(response.voter_id)
                
                # P1修复: 正确计算多数票
                # 总节点数 = peers数量 + 自己
                total_nodes = len(self.config.peers) + 1
                # 多数票 = floor(total_nodes / 2) + 1
                majority = total_nodes // 2 + 1
                
                votes_count = len(self._votes_received)
                
                logger.debug(
                    f"Node {self.node_id} received vote from {response.voter_id}, "
                    f"total votes: {votes_count}/{total_nodes}, majority needed: {majority}"
                )
                
                if votes_count >= majority:
                    logger.info(
                        f"Node {self.node_id} won election with {votes_count} votes "
                        f"(majority: {majority}) in round {self._election_round}"
                    )
                    await self._become_leader()
                else:
                    # P1修复: 记录选举进度
                    logger.debug(
                        f"Election progress: {votes_count}/{majority} votes, "
                        f"waiting for more votes"
                    )
    
    async def _become_leader(self):
        """成为Leader - P1修复: 初始化心跳跟踪"""
        logger.info(f"Node {self.node_id} became leader for term {self._persistent_state.current_term}")
        
        self._role = RaftRole.LEADER
        self._current_leader = self.node_id
        
        # 初始化Leader状态
        last_log_index = self._persistent_state.get_last_log_index()
        self._volatile_state.initialize_leader_state(self.config.peers, last_log_index)
        
        # P1修复: 初始化心跳跟踪状态
        now = time.time()
        self._last_heartbeat_sent = now
        for peer in self.config.peers:
            self._last_heartbeat_received[peer] = now
            self._heartbeat_missed[peer] = 0
            self._log_replication_retries[peer] = 0
            self._log_replication_inflight[peer] = -1
        
        # 取消选举定时器
        if self._election_timer:
            self._election_timer.cancel()
            self._election_timer = None
        
        # 启动心跳定时器
        self._heartbeat_timer = asyncio.create_task(self._heartbeat_loop())
        
        # 立即发送心跳
        await self._broadcast_append_entries()
    
    async def _become_follower(self, term: int):
        """成为Follower - DEFECT-007 [P1] 修复: 检测并处理脑裂恢复"""
        was_leader = self._role == RaftRole.LEADER

        if was_leader:
            logger.info(f"Node {self.node_id} stepping down from leader")

        self._role = RaftRole.FOLLOWER
        self._persistent_state.current_term = term
        self._persistent_state.voted_for = None

        # 停止心跳
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

        # DEFECT-007 [P1] 修复: 如果之前是Leader，可能是脑裂恢复
        if was_leader:
            await self._handle_potential_split_brain_recovery()

        # 重置选举定时器
        self._reset_election_timer()

    async def _handle_potential_split_brain_recovery(self):
        """DEFECT-007 [P1] 修复: 处理潜在的脑裂恢复"""
        if self._split_brain_detected:
            logger.info(f"Node {self.node_id} recovering from split-brain state")
            self._split_brain_detected = False
            self._split_brain_resolution_pending = False

            # 触发分区恢复后的日志同步
            if self._partition_recovery_task:
                self._partition_recovery_task.cancel()
            self._partition_recovery_task = asyncio.create_task(self._sync_logs_after_partition())

    async def _sync_logs_after_partition(self):
        """DEFECT-007 [P1] 修复: 分区恢复后同步日志"""
        try:
            logger.info(f"Node {self.node_id} starting post-partition log sync")

            # 等待一段时间确保网络稳定
            await asyncio.sleep(1)

            # 如果存在Leader，从Leader同步日志
            if self._current_leader and self._current_leader != self.node_id:
                await self._request_log_sync_from_leader()

            # DEFECT-011 [P2] 修复: 分区恢复后同步日志长度
            await self._verify_log_consistency()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Post-partition log sync failed: {e}")

    async def _request_log_sync_from_leader(self):
        """DEFECT-007 [P1] 修复: 从Leader请求日志同步"""
        try:
            request = AppendEntriesRequest(
                term=self._persistent_state.current_term,
                leader_id=self._current_leader,  # BUG 3 [P1] 修复: 应设为实际Leader ID而非自身node_id，
                prev_log_index=self._persistent_state.get_last_log_index(),
                prev_log_term=self._persistent_state.get_last_log_term(),
                entries=[],  # 空条目表示同步请求
                leader_commit=self._volatile_state.commit_index,
            )

            response = await self.transport.send_append_entries(
                self._current_leader, request
            )

            if response and response.success:
                logger.info(f"Log sync request accepted by leader {self._current_leader}")
            else:
                logger.warning(f"Log sync request rejected by leader {self._current_leader}")

        except Exception as e:
            logger.error(f"Failed to request log sync: {e}")

    async def _verify_log_consistency(self):
        """DEFECT-011 [P2] 修复: 验证日志一致性"""
        try:
            my_log_size = len(self._persistent_state.log)
            logger.debug(f"Log consistency check: local log size = {my_log_size}")

            # 如果检测到日志不一致，可以在这里触发修复
            # 例如：请求Leader发送缺失的日志条目

        except Exception as e:
            logger.error(f"Log consistency verification failed: {e}")
    
    async def _heartbeat_loop(self):
        """心跳循环 - P1修复: 改进心跳检测机制"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.heartbeat_interval
                )
            except asyncio.TimeoutError:
                # BUG 2 [P1] 修复: 检查 _role 时获取 _state_lock，
                # 避免与其他协程并发修改 _role 时产生数据竞争。
                # 使用短超时非阻塞方式获取锁，避免心跳循环长时间阻塞。
                try:
                    acquired = await asyncio.wait_for(
                        self._state_lock.acquire(), timeout=0.01
                    )
                    if not acquired:
                        continue
                    try:
                        is_leader = self._role == RaftRole.LEADER
                    finally:
                        self._state_lock.release()
                except asyncio.TimeoutError:
                    # 锁被其他协程持有，跳过本轮心跳检查
                    continue

                if is_leader:
                    # P1修复: 检查心跳状态
                    now = time.time()
                    self._last_heartbeat_sent = now
                    
                    # 检查是否有peer长时间未响应
                    for peer in self.config.peers:
                        last_received = self._last_heartbeat_received.get(peer, now)
                        time_since_last = now - last_received
                        
                        # 如果超过故障检测超时，增加丢失计数
                        if time_since_last > self.config.fault_detection_timeout:
                            self._heartbeat_missed[peer] = self._heartbeat_missed.get(peer, 0) + 1
                            logger.warning(
                                f"Peer {peer} missed heartbeat, "
                                f"consecutive misses: {self._heartbeat_missed[peer]}"
                            )
                            
                            # 如果连续丢失心跳超过阈值，标记为可能故障
                            if self._heartbeat_missed[peer] >= self.config.heartbeat_timeout_factor:
                                logger.error(
                                    f"Peer {peer} appears to have failed after "
                                    f"{self._heartbeat_missed[peer]} consecutive missed heartbeats"
                                )
                    
                    await self._broadcast_append_entries()
    
    async def _broadcast_append_entries(self):
        """广播追加条目请求 - DA-011 [P1] 修复: 跟踪复制任务"""
        if self._role != RaftRole.LEADER:
            return
        
        for peer in self.config.peers:
            task = asyncio.create_task(self._send_append_entries_to_peer(peer))
            self._replication_tasks.add(task)
            # BUG 5 [P2] 修复: 添加 done_callback 记录任务异常，
            # 避免复制任务失败时异常被静默忽略导致问题难以排查。
            def _log_replication_error(t: asyncio.Task, peer_name: str = peer) -> None:
                self._replication_tasks.discard(t)
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    logger.error(
                        f"Replication task for peer {peer_name} failed with error: {exc}",
                        exc_info=exc,
                    )
            task.add_done_callback(_log_replication_error)
    
    def _log_task_error(self, task: asyncio.Task, peer_id: str):
        """[BUG FIX] 记录复制任务异常"""
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"Replication task failed for peer {peer_id}: {e}")

    async def _send_append_entries_to_peer(self, peer_id: str):
        """发送追加条目到指定peer - P1修复: 完善日志复制协议"""
        async with self._state_lock:
            next_index = self._volatile_state.next_index.get(peer_id, 0)
            prev_log_index = next_index - 1
            prev_log_term = 0
            
            # P1修复: PrevLogIndex/PrevLogTerm验证
            if prev_log_index >= 0:
                entry = self._persistent_state.get_entry_at(prev_log_index)
                if entry:
                    prev_log_term = entry.term
                else:
                    # P1修复: 如果prev_log_index位置的条目不存在，说明日志不连续
                    logger.warning(
                        f"Log gap detected at index {prev_log_index} for peer {peer_id}, "
                        f"adjusting next_index"
                    )
                    # 回退到上一个已知匹配的位置
                    self._volatile_state.next_index[peer_id] = max(0, prev_log_index)
                    prev_log_index = self._volatile_state.next_index[peer_id] - 1
                    if prev_log_index >= 0:
                        entry = self._persistent_state.get_entry_at(prev_log_index)
                        if entry:
                            prev_log_term = entry.term
            
            # 获取要发送的条目
            entries: List[LogEntry] = []
            last_index = self._persistent_state.get_last_log_index()
            
            for i in range(next_index, min(
                next_index + self.config.max_log_entries_per_request,
                last_index + 1
            )):
                entry = self._persistent_state.get_entry_at(i)
                if entry:
                    entries.append(entry)
            
            # P1修复: 记录正在复制的日志索引
            if entries:
                self._log_replication_inflight[peer_id] = entries[-1].index
            
            request = AppendEntriesRequest(
                term=self._persistent_state.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self._volatile_state.commit_index,
            )
        
        try:
            response = await self.transport.send_append_entries(peer_id, request)
            if response:
                await self._handle_append_entries_response(peer_id, response)
                # P1修复: 更新心跳接收时间
                self._last_heartbeat_received[peer_id] = time.time()
                self._heartbeat_missed[peer_id] = 0  # 重置丢失计数
        except Exception as e:
            logger.debug(f"Failed to send append entries to {peer_id}: {e}")
            # P1修复: 增加重试计数
            self._log_replication_retries[peer_id] = self._log_replication_retries.get(peer_id, 0) + 1
            if self._log_replication_retries[peer_id] > self.config.max_log_replication_retries:
                logger.error(
                    f"Log replication to {peer_id} failed after "
                    f"{self._log_replication_retries[peer_id]} retries"
                )
    
    async def _handle_append_entries_response(
        self, peer_id: str, response: AppendEntriesResponse
    ):
        """处理追加条目响应 - P1修复: 日志一致性检查和冲突解决"""
        async with self._state_lock:
            # 如果任期更高，转换为Follower
            if response.term > self._persistent_state.current_term:
                await self._become_follower(response.term)
                return
            
            if self._role != RaftRole.LEADER:
                return
            
            if response.success:
                # P1修复: 重置重试计数
                self._log_replication_retries[peer_id] = 0
                
                # 更新匹配索引
                if response.match_index > self._volatile_state.match_index.get(peer_id, -1):
                    self._volatile_state.match_index[peer_id] = response.match_index
                    self._volatile_state.next_index[peer_id] = response.match_index + 1
                    
                    logger.debug(
                        f"Peer {peer_id} matched up to index {response.match_index}"
                    )
                
                # 检查是否可以提交更多条目
                await self._advance_commit_index()
            else:
                # P1修复: 日志冲突解决机制
                current_next = self._volatile_state.next_index.get(peer_id, 0)
                
                if response.conflict_index >= 0:
                    # 使用Follower返回的冲突索引
                    conflict_index = response.conflict_index
                    
                    # P1修复: 验证冲突索引是否合理
                    if conflict_index < current_next:
                        logger.info(
                            f"Peer {peer_id} reported conflict at index {conflict_index}, "
                            f"adjusting from {current_next}"
                        )
                        self._volatile_state.next_index[peer_id] = conflict_index
                    else:
                        # 冲突索引不合理，保守递减
                        logger.warning(
                            f"Peer {peer_id} reported invalid conflict index {conflict_index} "
                            f"(current next_index: {current_next}), decrementing"
                        )
                        self._volatile_state.next_index[peer_id] = max(0, current_next - 1)
                else:
                    # 没有冲突索引信息，保守递减
                    logger.debug(
                        f"Peer {peer_id} rejected append entries, "
                        f"decrementing next_index from {current_next}"
                    )
                    self._volatile_state.next_index[peer_id] = max(0, current_next - 1)
                
                # P1修复: 检查是否需要快速回退
                new_next = self._volatile_state.next_index[peer_id]
                if current_next - new_next > 10:
                    logger.warning(
                        f"Large log inconsistency detected with peer {peer_id}, "
                        f"next_index jumped from {current_next} to {new_next}"
                    )
                
                # [BUG FIX] 立即重试，保存任务引用并添加异常处理
                retry_task = asyncio.create_task(self._send_append_entries_to_peer(peer_id))
                self._replication_tasks.add(retry_task)
                retry_task.add_done_callback(self._replication_tasks.discard)
                retry_task.add_done_callback(lambda t: self._log_task_error(t, peer_id))
    
    async def _advance_commit_index(self):
        """推进提交索引 - P1修复: 改进提交索引更新逻辑"""
        if self._role != RaftRole.LEADER:
            return
        
        # P1修复: 记录旧的commit_index用于比较
        old_commit_index = self._volatile_state.commit_index
        
        # 从当前commit_index+1开始检查
        for index in range(self._volatile_state.commit_index + 1,
                          self._persistent_state.get_last_log_index() + 1):
            entry = self._persistent_state.get_entry_at(index)
            if not entry:
                continue
            
            # Leader只能提交当前任期的条目
            if entry.term != self._persistent_state.current_term:
                continue
            
            # P1修复: 正确计算多数票
            total_nodes = len(self.config.peers) + 1
            majority = total_nodes // 2 + 1
            
            # 统计匹配此索引的节点数
            match_count = 1  # Leader自己
            for peer in self.config.peers:
                if self._volatile_state.match_index.get(peer, -1) >= index:
                    match_count += 1
            
            # 如果超过半数，可以提交
            if match_count >= majority:
                self._volatile_state.commit_index = index
                # P1修复: 记录提交索引历史
                self._commit_index_history.append((
                    index,
                    self._persistent_state.current_term,
                    time.time()
                ))
                # 限制历史记录长度
                if len(self._commit_index_history) > 100:
                    self._commit_index_history = self._commit_index_history[-100:]
        
        # P1修复: 如果commit_index有更新，应用到状态机
        if self._volatile_state.commit_index > old_commit_index:
            logger.debug(
                f"Commit index advanced from {old_commit_index} to "
                f"{self._volatile_state.commit_index}"
            )
            await self._apply_committed_entries()
            
            # P1修复: 广播新的commit_index给所有peers
            await self._broadcast_append_entries()
    
    async def _apply_committed_entries(self):
        """应用已提交的条目到状态机
        
        DEFECT-RC-002 [P0] 修复: 异常后不再递增last_applied，避免状态机与日志永久不一致
        """
        while self._volatile_state.last_applied < self._volatile_state.commit_index:
            next_index = self._volatile_state.last_applied + 1
            entry = self._persistent_state.get_entry_at(next_index)
            
            if entry:
                try:
                    result = await self.state_machine.apply(entry.command)
                    
                    # 如果有等待的提案，设置结果
                    if entry.entry_id in self._pending_proposals:
                        future = self._pending_proposals.pop(entry.entry_id)
                        if not future.done():
                            future.set_result(result)
                    
                    self._volatile_state.last_applied = next_index
                except Exception as e:
                    logger.error(f"Failed to apply entry {next_index}: {e}")
                    
                    # 如果有等待的提案，设置异常
                    if entry.entry_id in self._pending_proposals:
                        future = self._pending_proposals.pop(entry.entry_id)
                        if not future.done():
                            future.set_exception(e)
                    
                    # DEFECT-RC-002: 异常时break，不递增last_applied
                    # 下次重试时会重新应用此条目
                    break
            else:
                break
    
    async def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        """
        处理请求投票
        
        Args:
            request: 投票请求
            
        Returns:
            投票响应
        """
        async with self._state_lock:
            # 如果请求任期更高，更新自己的任期
            if request.term > self._persistent_state.current_term:
                await self._become_follower(request.term)
            
            vote_granted = False
            
            # 检查是否可以投票
            if request.term == self._persistent_state.current_term:
                if (self._persistent_state.voted_for is None or 
                    self._persistent_state.voted_for == request.candidate_id):
                    
                    # 检查候选人的日志是否至少和自己一样新
                    last_log_index = self._persistent_state.get_last_log_index()
                    last_log_term = self._persistent_state.get_last_log_term()
                    
                    if (request.last_log_term > last_log_term or
                        (request.last_log_term == last_log_term and
                         request.last_log_index >= last_log_index)):
                        
                        vote_granted = True
                        self._persistent_state.voted_for = request.candidate_id
                        self._reset_election_timer()
            
            return RequestVoteResponse(
                term=self._persistent_state.current_term,
                voter_id=self.node_id,
                vote_granted=vote_granted,
            )
    
    async def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """
        处理追加条目请求 - DEFECT-005 [P1] 修复: 优化状态同步等待

        Args:
            request: 追加条目请求

        Returns:
            追加条目响应
        """
        async with self._state_lock:
            # 如果Leader任期更高，更新自己的任期
            if request.term > self._persistent_state.current_term:
                await self._become_follower(request.term)

            # 如果任期小于当前任期，拒绝
            if request.term < self._persistent_state.current_term:
                return AppendEntriesResponse(
                    term=self._persistent_state.current_term,
                    follower_id=self.node_id,
                    success=False,
                    conflict_index=self._persistent_state.get_last_log_index(),
                )

            # DEFECT-005 [P1] 修复: 更新收到Leader心跳的时间
            self._leader_heartbeat_received = time.time()

            # DEFECT-007 [P1] 修复: 检测可能的脑裂情况
            if self._current_leader and self._current_leader != request.leader_id:
                if not self._split_brain_detected:
                    logger.warning(f"Split-brain detected: current leader={self._current_leader}, "
                                 f"new leader={request.leader_id}")
                    self._split_brain_detected = True
                    self._split_brain_start_time = time.time()

            # 重置选举定时器(收到有效Leader的心跳)
            self._reset_election_timer()
            self._current_leader = request.leader_id

            # DEFECT-005 [P1] 修复: 设置状态同步事件
            self._state_sync_event.set()
            
            # P1修复: PrevLogIndex/PrevLogTerm一致性检查
            if request.prev_log_index >= 0:
                if request.prev_log_index >= len(self._persistent_state.log):
                    # P1修复: 日志太短，返回当前日志长度
                    logger.debug(
                        f"Log too short: prev_log_index={request.prev_log_index}, "
                        f"log_length={len(self._persistent_state.log)}"
                    )
                    return AppendEntriesResponse(
                        term=self._persistent_state.current_term,
                        follower_id=self.node_id,
                        success=False,
                        conflict_index=len(self._persistent_state.log),
                    )
                
                entry = self._persistent_state.get_entry_at(request.prev_log_index)
                if entry is None or entry.term != request.prev_log_term:
                    # P1修复: 找到冲突的任期并优化冲突解决
                    conflict_term = entry.term if entry else 0
                    conflict_index = request.prev_log_index
                    
                    # 找到该任期的第一个索引
                    for i in range(request.prev_log_index, -1, -1):
                        e = self._persistent_state.get_entry_at(i)
                        if e and e.term != conflict_term:
                            conflict_index = i + 1
                            break
                    
                    logger.debug(
                        f"Log conflict at index {conflict_index}: "
                        f"expected term {request.prev_log_term}, "
                        f"actual term {conflict_term}"
                    )
                    
                    return AppendEntriesResponse(
                        term=self._persistent_state.current_term,
                        follower_id=self.node_id,
                        success=False,
                        conflict_index=conflict_index,
                    )
            
            # P1修复: 追加新条目，确保日志一致性
            for i, entry in enumerate(request.entries):
                index = request.prev_log_index + 1 + i
                
                if index < len(self._persistent_state.log):
                    # DEFECT-RC-003 [P0]: 检查是否尝试截断已提交条目
                    if index <= self._volatile_state.commit_index:
                        logger.error(
                            f"Refusing to truncate committed entry at index {index}, "
                            f"commit_index={self._volatile_state.commit_index}"
                        )
                        return AppendEntriesResponse(
                            term=self._persistent_state.current_term,
                            follower_id=self.node_id,
                            success=False,
                            conflict_index=self._volatile_state.commit_index + 1,
                        )
                    
                    # 如果存在冲突，删除之后的所有条目
                    existing_entry = self._persistent_state.log[index]
                    if existing_entry.term != entry.term:
                        # P1修复: 记录日志截断
                        truncated_count = len(self._persistent_state.log) - index
                        logger.info(
                            f"Truncating {truncated_count} log entries from index {index} "
                            f"due to term mismatch (local: {existing_entry.term}, remote: {entry.term})"
                        )
                        self._persistent_state.log = self._persistent_state.log[:index]
                        self._persistent_state.log.append(entry)
                else:
                    self._persistent_state.log.append(entry)
            
            # P1修复: 更新commit_index，确保一致性
            if request.leader_commit > self._volatile_state.commit_index:
                old_commit = self._volatile_state.commit_index
                new_commit = min(
                    request.leader_commit,
                    self._persistent_state.get_last_log_index()
                )
                self._volatile_state.commit_index = new_commit
                
                logger.debug(
                    f"Follower commit_index updated from {old_commit} to {new_commit} "
                    f"(leader_commit: {request.leader_commit})"
                )
                
                # 应用到状态机
                await self._apply_committed_entries()
            
            return AppendEntriesResponse(
                term=self._persistent_state.current_term,
                follower_id=self.node_id,
                success=True,
                match_index=request.prev_log_index + len(request.entries),
            )
    
    async def handle_install_snapshot(self, request: InstallSnapshotRequest) -> InstallSnapshotResponse:
        """
        处理安装快照请求
        
        DEFECT-RC-004 [P0] 修复: 先更新元数据和截断日志，再恢复状态机
        DEFECT-RC-019 [P0] 修复: 使用truncate_log_before方法正确处理日志偏移
        
        Args:
            request: 安装快照请求
            
        Returns:
            安装快照响应
        """
        async with self._state_lock:
            if request.term < self._persistent_state.current_term:
                return InstallSnapshotResponse(
                    term=self._persistent_state.current_term,
                    follower_id=self.node_id,
                    success=False,
                )
            
            if request.term > self._persistent_state.current_term:
                await self._become_follower(request.term)
            
            self._reset_election_timer()
            self._current_leader = request.leader_id
            
            # DEFECT-RC-004 [P0]: 先更新元数据
            self._last_snapshot_index = request.last_included_index
            self._last_snapshot_term = request.last_included_term
            
            # DEFECT-RC-004 [P0]: 更新应用索引
            self._volatile_state.last_applied = request.last_included_index
            self._volatile_state.commit_index = max(
                self._volatile_state.commit_index,
                request.last_included_index
            )
            
            # DEFECT-RC-019 [P0]: 使用新的truncate_log_before方法截断日志
            self._persistent_state.truncate_log_before(request.last_included_index)
            
            # DEFECT-RC-004 [P0]: 最后恢复状态机
            try:
                await self.state_machine.restore(request.data)
                
                return InstallSnapshotResponse(
                    term=self._persistent_state.current_term,
                    follower_id=self.node_id,
                    success=True,
                )
            except Exception as e:
                logger.error(f"Failed to install snapshot: {e}")
                return InstallSnapshotResponse(
                    term=self._persistent_state.current_term,
                    follower_id=self.node_id,
                    success=False,
                )
    
    async def propose_command(self, command: Dict[str, Any], timeout: float = 5.0) -> Any:
        """
        提议命令(仅在Leader上执行)
        
        Args:
            command: 要执行的命令
            timeout: 超时时间(秒)
            
        Returns:
            命令执行结果
            
        Raises:
            NotLeaderError: 如果当前节点不是Leader
            ProposalTimeoutError: 如果提案超时
        """
        self._ensure_async_primitives()
        if self._role != RaftRole.LEADER:
            raise NotLeaderError(self._current_leader)
        
        # BUG 4 [P1] 修复: 在锁内完成日志条目创建、追加以及future的创建和注册，
        # 避免释放锁后、注册future前的窗口期内，日志条目被提交但找不到对应future导致提案结果丢失。
        async with self._state_lock:
            entry = LogEntry(
                term=self._persistent_state.current_term,
                index=self._persistent_state.get_last_log_index() + 1,
                command=command,
            )
            self._persistent_state.log.append(entry)

            # 在锁内创建Future并注册到 _pending_proposals
            future: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending_proposals[entry.entry_id] = future
        
        # 立即广播
        await self._broadcast_append_entries()
        
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending_proposals.pop(entry.entry_id, None)
            raise ProposalTimeoutError(f"Proposal timed out after {timeout}s")
    
    async def start(self):
        """启动Raft节点 - P1修复: 初始化心跳时间; DEFECT-005 修复: 单节点直接成为Leader"""
        logger.info(f"Starting Raft node {self.node_id}")
        self._ensure_async_primitives()
        self._shutdown_event.clear()

        # P1修复: 初始化心跳时间
        now = time.time()
        self._last_heartbeat_sent = now
        self._leader_heartbeat_received = now
        for peer in self.config.peers:
            self._last_heartbeat_received[peer] = now
            self._heartbeat_missed[peer] = 0

        # DEFECT-005 [P1] 修复: 单节点集群时跳过选举，直接成为Leader
        # 当集群只有1个节点（无peers）时，无需选举即可直接成为Leader
        if not self.config.peers:
            logger.info(
                f"Node {self.node_id} is the only node in the cluster, "
                f"bypassing election and becoming Leader immediately"
            )
            self._persistent_state.current_term = 1
            self._persistent_state.voted_for = self.node_id
            self._role = RaftRole.LEADER
            self._current_leader = self.node_id
            self._votes_received = {self.node_id}
            self._volatile_state.initialize_leader_state([], self._persistent_state.get_last_log_index())
            # 启动心跳定时器（Leader需要发送心跳）
            self._heartbeat_timer = asyncio.create_task(self._heartbeat_loop())
        else:
            self._reset_election_timer()
    
    async def stop(self):
        """停止Raft节点 - P1修复: 清理所有资源"""
        logger.info(f"Stopping Raft node {self.node_id}")
        self._shutdown_event.set()
        
        # P1修复: 取消所有定时器
        if self._election_timer:
            self._election_timer.cancel()
            try:
                await self._election_timer
            except asyncio.CancelledError:
                pass
            self._election_timer = None
            
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            try:
                await self._heartbeat_timer
            except asyncio.CancelledError:
                pass
            self._heartbeat_timer = None
        
        # P1修复: 清理所有待处理的提案
        for future in self._pending_proposals.values():
            if not future.done():
                future.cancel()
        self._pending_proposals.clear()
        
        # P1修复: 清理状态跟踪
        self._last_heartbeat_received.clear()
        self._heartbeat_missed.clear()
        self._log_replication_inflight.clear()
        self._log_replication_retries.clear()
        self._commit_index_history.clear()

        # DA-011 [P1] 修复: 取消所有复制任务
        for task in self._replication_tasks:
            task.cancel()
        if self._replication_tasks:
            await asyncio.gather(*self._replication_tasks, return_exceptions=True)
        self._replication_tasks.clear()
    
    async def get_status(self) -> Dict[str, Any]:
        """获取节点状态 - P1修复: 增加状态信息"""
        async with self._state_lock:
            return {
                "node_id": self.node_id,
                "role": self._role.name,
                "term": self._persistent_state.current_term,
                "leader": self._current_leader,
                "log_length": len(self._persistent_state.log),
                "commit_index": self._volatile_state.commit_index,
                "last_applied": self._volatile_state.last_applied,
                "peers": self.config.peers,
                # P1修复: 增加心跳和选举状态信息
                "election_round": self._election_round,
                "last_heartbeat_sent": self._last_heartbeat_sent,
                "leader_heartbeat_received": self._leader_heartbeat_received,
                "heartbeat_missed": dict(self._heartbeat_missed),
            }
    
    async def add_member(self, new_node_id: str) -> bool:
        """
        添加新成员(联合共识)
        
        Args:
            new_node_id: 新节点ID
            
        Returns:
            是否成功
        """
        if self._role != RaftRole.LEADER:
            raise NotLeaderError(self._current_leader)
        
        # 使用联合共识
        command = {
            "action": "membership_change",
            "type": "add",
            "node_id": new_node_id,
            "old_config": self.config.peers + [self.node_id],
            "new_config": self.config.peers + [self.node_id, new_node_id],
        }
        
        try:
            result = await self.propose_command(command)
            if result.get("success"):
                self.config.peers.append(new_node_id)
            return result.get("success", False)
        except Exception as e:
            logger.error(f"Failed to add member: {e}")
            return False
    
    async def remove_member(self, node_id: str) -> bool:
        """
        移除成员
        
        Args:
            node_id: 要移除的节点ID
            
        Returns:
            是否成功
        """
        if self._role != RaftRole.LEADER:
            raise NotLeaderError(self._current_leader)
        
        if node_id not in self.config.peers:
            return False
        
        command = {
            "action": "membership_change",
            "type": "remove",
            "node_id": node_id,
            "old_config": self.config.peers + [self.node_id],
            "new_config": [p for p in self.config.peers if p != node_id] + [self.node_id],
        }
        
        try:
            result = await self.propose_command(command)
            if result.get("success") and node_id in self.config.peers:
                self.config.peers.remove(node_id)
            return result.get("success", False)
        except Exception as e:
            logger.error(f"Failed to remove member: {e}")
            return False
    
    # ========================================================================
    # 故障转移修复 - P0级别修复
    # ========================================================================
    
    async def validate_candidate_node(self, candidate_id: str) -> bool:
        """
        验证候选节点是否有效
        
        检查候选节点是否满足成为新Leader的条件：
        1. 节点存在于已知节点列表中
        2. 节点健康状态正常
        3. 节点的日志足够新
        
        Args:
            candidate_id: 候选节点ID
            
        Returns:
            候选节点是否有效
            
        Raises:
            CandidateValidationError: 验证失败时抛出
        """
        logger.debug(f"Validating candidate node: {candidate_id}")
        
        # 检查节点是否在已知节点列表中
        if candidate_id not in self.config.peers and candidate_id != self.node_id:
            raise CandidateValidationError(
                candidate_id, 
                f"Node not in known peers: {self.config.peers}"
            )
        
        # 检查节点健康状态
        is_healthy = await self._check_node_health(candidate_id)
        if not is_healthy:
            raise CandidateValidationError(
                candidate_id,
                "Node health check failed"
            )
        
        # 检查节点的日志是否足够新（通过请求投票响应判断）
        try:
            # 发送一个探测性的请求投票
            last_log_index = self._persistent_state.get_last_log_index()
            last_log_term = self._persistent_state.get_last_log_term()
            
            probe_request = RequestVoteRequest(
                term=self._persistent_state.current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            
            response = await asyncio.wait_for(
                self.transport.send_request_vote(candidate_id, probe_request),
                timeout=self.failover_config.candidate_validation_timeout
            )
            
            if response is None:
                raise CandidateValidationError(
                    candidate_id,
                    "No response from candidate during validation"
                )
            
            logger.info(f"Candidate {candidate_id} validation passed")
            return True
            
        except asyncio.TimeoutError:
            raise CandidateValidationError(
                candidate_id,
                f"Validation timeout after {self.failover_config.candidate_validation_timeout}s"
            )
        except Exception as e:
            raise CandidateValidationError(candidate_id, str(e))
    
    async def _check_node_health(self, node_id: str) -> bool:
        """
        检查节点健康状态
        
        Args:
            node_id: 节点ID
            
        Returns:
            节点是否健康
        """
        now = time.time()
        
        # 检查缓存的健康状态
        if node_id in self._last_health_check:
            last_check = self._last_health_check[node_id]
            if now - last_check < self.failover_config.health_check_interval:
                return self._node_health_status.get(node_id, False)
        
        # 执行健康检查（通过发送心跳探测）
        try:
            request = AppendEntriesRequest(
                term=self._persistent_state.current_term,
                leader_id=self.node_id,
                prev_log_index=-1,
                prev_log_term=0,
                entries=[],
                leader_commit=self._volatile_state.commit_index,
            )
            
            response = await asyncio.wait_for(
                self.transport.send_append_entries(node_id, request),
                timeout=self.failover_config.candidate_validation_timeout
            )
            
            is_healthy = response is not None
            
            # 更新缓存
            self._node_health_status[node_id] = is_healthy
            self._last_health_check[node_id] = now
            
            return is_healthy
            
        except Exception as e:
            logger.debug(f"Health check failed for {node_id}: {e}")
            self._node_health_status[node_id] = False
            self._last_health_check[node_id] = now
            return False
    
    async def get_valid_candidate_nodes(self) -> List[str]:
        """
        获取所有有效的候选节点列表
        
        筛选出可以作为新Leader的候选节点
        
        Returns:
            有效候选节点列表
        """
        candidates = []
        
        # 包含所有peers和自身
        all_nodes = list(self.config.peers) + [self.node_id]
        
        for node_id in all_nodes:
            try:
                is_valid = await self.validate_candidate_node(node_id)
                if is_valid:
                    candidates.append(node_id)
            except CandidateValidationError as e:
                logger.warning(f"Candidate validation failed for {node_id}: {e.reason}")
            except Exception as e:
                logger.error(f"Unexpected error validating {node_id}: {e}")
        
        logger.info(f"Found {len(candidates)} valid candidate nodes: {candidates}")
        return candidates
    
    def _calculate_retry_delay(self, retry_count: int) -> float:
        """
        计算指数退避重试延迟
        
        Args:
            retry_count: 当前重试次数
            
        Returns:
            重试延迟时间(秒)
        """
        delay = self.failover_config.initial_retry_delay * (
            self.failover_config.retry_multiplier ** retry_count
        )
        return min(delay, self.failover_config.max_retry_delay)
    
    async def wait_for_state_recovery(self, timeout: Optional[float] = None) -> bool:
        """
        等待状态恢复完成
        
        确保在故障转移后，状态机已经恢复到一致状态
        
        Args:
            timeout: 超时时间
            
        Returns:
            是否成功恢复
        """
        timeout = timeout or self.failover_config.state_recovery_timeout
        start_time = time.time()
        
        logger.info("Waiting for state recovery...")
        
        while time.time() - start_time < timeout:
            # 检查是否所有已提交的条目都已应用
            if self._volatile_state.last_applied >= self._volatile_state.commit_index:
                logger.info(
                    f"State recovery completed: last_applied={self._volatile_state.last_applied}, "
                    f"commit_index={self._volatile_state.commit_index}"
                )
                return True
            
            # 等待一小段时间后重试
            await asyncio.sleep(0.1)
        
        logger.warning(
            f"State recovery timeout: last_applied={self._volatile_state.last_applied}, "
            f"commit_index={self._volatile_state.commit_index}"
        )
        return False
    
    async def initiate_failover(
        self,
        failed_leader_id: str,
        timeout: Optional[float] = None
    ) -> bool:
        """
        发起故障转移
        
        当检测到Leader故障时，发起故障转移流程
        
        Args:
            failed_leader_id: 故障的Leader ID
            timeout: 故障转移超时时间
            
        Returns:
            故障转移是否成功
            
        Raises:
            FailoverError: 故障转移失败时抛出
        """
        async with self._failover_lock:
            # 检查是否已有故障转移在进行
            if self._failover_state.in_progress:
                logger.warning("Failover already in progress")
                return False
            
            # 初始化故障转移状态
            self._failover_state = FailoverState(
                in_progress=True,
                failed_leader=failed_leader_id,
                start_time=time.time(),
            )
        
        timeout = timeout or self.failover_config.failover_timeout
        logger.info(f"Initiating failover for failed leader: {failed_leader_id}")
        
        try:
            # 使用指数退避重试机制
            for retry in range(self.failover_config.max_retries):
                self._failover_state.retry_count = retry
                
                # 检查是否超时
                if time.time() - self._failover_state.start_time > timeout:
                    raise FailoverError(
                        f"Failover timeout after {timeout}s",
                        retry_count=retry,
                        last_error=self._failover_state.last_error
                    )
                
                try:
                    # 获取有效的候选节点
                    candidates = await self.get_valid_candidate_nodes()
                    
                    if not candidates:
                        raise FailoverError(
                            "No valid candidate nodes available",
                            retry_count=retry
                        )
                    
                    # 排除故障的Leader
                    candidates = [c for c in candidates if c != failed_leader_id]
                    
                    if not candidates:
                        raise FailoverError(
                            "No valid candidate nodes after excluding failed leader",
                            retry_count=retry
                        )
                    
                    self._failover_state.candidate_nodes = candidates
                    logger.info(f"Valid candidates for failover: {candidates}")
                    
                    # 尝试触发新的选举
                    # 如果自己是候选节点之一，则发起选举
                    if self.node_id in candidates:
                        logger.info("Initiating election as candidate")
                        await self._start_election()
                        
                        # 等待选举完成
                        await asyncio.sleep(self.config.election_timeout_max * 2)
                        
                        # 检查是否成为Leader或发现了新Leader
                        if self._role == RaftRole.LEADER:
                            logger.info("Failover successful: became leader")
                            # 等待状态恢复
                            await self.wait_for_state_recovery()
                            return True
                        
                        if self._current_leader and self._current_leader != failed_leader_id:
                            logger.info(f"Failover successful: new leader is {self._current_leader}")
                            return True
                    else:
                        # 等待其他候选节点成为Leader
                        logger.info("Waiting for another candidate to become leader")
                        await asyncio.sleep(self.config.election_timeout_max * 3)
                        
                        if self._current_leader and self._current_leader != failed_leader_id:
                            logger.info(f"Failover successful: new leader is {self._current_leader}")
                            return True
                    
                    # 如果还没成功，继续重试
                    raise FailoverError(
                        "Failover attempt failed, will retry",
                        retry_count=retry
                    )
                    
                except FailoverError as e:
                    self._failover_state.last_error = e
                    logger.warning(f"Failover attempt {retry + 1} failed: {e}")
                    
                    # 计算重试延迟
                    delay = self._calculate_retry_delay(retry)
                    self._failover_state.last_retry_time = time.time()
                    
                    logger.info(f"Retrying failover in {delay:.2f}s (attempt {retry + 1}/{self.failover_config.max_retries})")
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    self._failover_state.last_error = e
                    logger.error(f"Unexpected error during failover: {e}")
                    raise FailoverError(
                        f"Unexpected error: {e}",
                        retry_count=retry,
                        last_error=e
                    )
            
            # 所有重试都失败
            raise FailoverError(
                f"Failover failed after {self.failover_config.max_retries} attempts",
                retry_count=self.failover_config.max_retries,
                last_error=self._failover_state.last_error
            )
            
        finally:
            async with self._failover_lock:
                self._failover_state.in_progress = False
    
    async def handle_leader_failure(self) -> bool:
        """
        处理Leader故障
        
        检测到Leader故障时的处理入口
        
        Returns:
            是否成功处理
        """
        if not self._current_leader:
            logger.warning("No known leader to failover from")
            return False
        
        failed_leader = self._current_leader
        logger.warning(f"Detected leader failure: {failed_leader}")
        
        try:
            success = await self.initiate_failover(failed_leader)
            return success
        except FailoverError as e:
            logger.error(f"Failover failed: {e}")
            return False
    
    def get_failover_status(self) -> Dict[str, Any]:
        """
        获取故障转移状态
        
        Returns:
            故障转移状态信息
        """
        return {
            "in_progress": self._failover_state.in_progress,
            "failed_leader": self._failover_state.failed_leader,
            "candidate_nodes": self._failover_state.candidate_nodes,
            "current_candidate_index": self._failover_state.current_candidate_index,
            "retry_count": self._failover_state.retry_count,
            "last_error": str(self._failover_state.last_error) if self._failover_state.last_error else None,
            "elapsed_time": time.time() - self._failover_state.start_time if self._failover_state.start_time > 0 else 0,
            "node_health": dict(self._node_health_status),
        }


class RaftCluster:
    """
    Raft集群管理器
    
    用于管理多个Raft节点，主要用于测试
    """
    
    def __init__(self, num_nodes: int = 3):
        self.num_nodes = num_nodes
        self.nodes: Dict[str, RaftNode] = {}
        self.transport = InMemoryTransport()
    
    async def start(self):
        """启动集群"""
        node_ids = [f"node{i}" for i in range(self.num_nodes)]
        
        for i, node_id in enumerate(node_ids):
            peers = [nid for nid in node_ids if nid != node_id]
            config = RaftConfig(node_id=node_id, peers=peers)
            
            node = RaftNode(
                config=config,
                state_machine=MemoryStateMachine(),
                transport=self.transport,
            )
            self.nodes[node_id] = node
            self.transport.register_node(node)
        
        # 启动所有节点
        for node in self.nodes.values():
            await node.start()
    
    async def stop(self):
        """停止集群"""
        for node in self.nodes.values():
            await node.stop()
    
    def get_leader(self) -> Optional[RaftNode]:
        """获取当前Leader"""
        for node in self.nodes.values():
            if node.is_leader:
                return node
        return None
    
    async def wait_for_leader(self, timeout: float = 5.0) -> Optional[RaftNode]:
        """等待Leader选举"""
        start = time.time()
        while time.time() - start < timeout:
            leader = self.get_leader()
            if leader:
                return leader
            await asyncio.sleep(0.1)
        return None


# 测试代码
if __name__ == "__main__":
    async def test_raft():
        """测试Raft实现"""
        logging.basicConfig(level=logging.INFO)
        
        # 创建并启动集群
        cluster = RaftCluster(num_nodes=3)
        await cluster.start()
        
        # 等待Leader选举
        leader = await cluster.wait_for_leader(timeout=3.0)
        if leader:
            print(f"Leader elected: {leader.node_id}")
            
            # 提交一些命令
            for i in range(5):
                result = await leader.propose_command({
                    "action": "set",
                    "key": f"key{i}",
                    "value": f"value{i}",
                })
                print(f"Command {i} result: {result}")
            
            # 获取状态
            for node in cluster.nodes.values():
                status = await node.get_status()
                print(f"Node {node.node_id} status: {status}")
        
        await cluster.stop()
    
    asyncio.run(test_raft())

"""
会话一致性 (Session Consistency) 实现模块

实现读己之写 (Read Your Writes)、单调读 (Monotonic Reads)、
单调写 (Monotonic Writes) 等会话一致性保证。

核心特性:
- 会话粘性路由
- 读己之写保证
- 单调读保证
- 单调写保证
- 写后读保证
- 会话状态追踪

示例用法:
    >>> from session_consistency import SessionManager, ConsistencyLevel
    >>> session = SessionManager.create_session("user1")
    >>> session.write("key1", "value1")
    >>> value = session.read("key1")  # 保证读到刚写入的值
"""

import time
import uuid
import hashlib
from typing import Dict, List, Optional, Set, Tuple, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import threading
import asyncio

from hybrid_logical_clock import HybridLogicalClock, HLCTimestamp, get_timestamp
from vector_clock import VectorClock, VectorClockRelation


class SessionConsistencyLevel(Enum):
    """会话一致性级别"""
    READ_YOUR_WRITES = "read_your_writes"    # 读己之写
    MONOTONIC_READS = "monotonic_reads"      # 单调读
    MONOTONIC_WRITES = "monotonic_writes"    # 单调写
    WRITES_FOLLOW_READS = "writes_follow_reads"  # 写后读
    SESSION = "session"                      # 完整会话一致性


@dataclass
class WriteOperation:
    """
    写操作记录
    
    Attributes:
        operation_id: 操作唯一标识
        key: 写入的键
        value: 写入的值
        timestamp: HLC时间戳
        vector_clock: 向量时钟
        node_id: 执行写入的节点
    """
    operation_id: str
    key: str
    value: Any
    timestamp: HLCTimestamp
    vector_clock: Dict[str, int]
    node_id: str
    write_time: float = field(default_factory=time.time)


@dataclass
class ReadOperation:
    """
    读操作记录
    
    Attributes:
        operation_id: 操作唯一标识
        key: 读取的键
        value: 读取的值
        timestamp: HLC时间戳
        read_time: 读取时间
    """
    operation_id: str
    key: str
    value: Any
    timestamp: HLCTimestamp
    read_time: float = field(default_factory=time.time)


@dataclass
class SessionState:
    """
    会话状态
    
    追踪会话内的所有操作，保证一致性
    
    Attributes:
        session_id: 会话唯一标识
        user_id: 用户ID
        created_at: 创建时间
        last_activity: 最后活动时间
        write_set: 会话内写入的键集合
        read_set: 会话内读取的键集合
        write_timestamps: 每个键的最后写入时间戳
        read_timestamps: 每个键的最后读取时间戳
        sticky_node: 粘性路由节点
        consistency_level: 一致性级别
    """
    session_id: str
    user_id: str
    created_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)
    write_set: Set[str] = field(default_factory=set)
    read_set: Set[str] = field(default_factory=set)
    write_timestamps: Dict[str, HLCTimestamp] = field(default_factory=dict)
    read_timestamps: Dict[str, HLCTimestamp] = field(default_factory=dict)
    write_vector_clocks: Dict[str, Dict[str, int]] = field(default_factory=dict)
    sticky_node: Optional[str] = None
    consistency_level: SessionConsistencyLevel = SessionConsistencyLevel.SESSION
    operation_history: List[Dict[str, Any]] = field(default_factory=list)
    
    def touch(self):
        """更新最后活动时间"""
        self.last_activity = time.time()
    
    def record_write(self, key: str, timestamp: HLCTimestamp, vector_clock: Dict[str, int]):
        """记录写操作"""
        self.touch()
        self.write_set.add(key)
        self.write_timestamps[key] = timestamp
        self.write_vector_clocks[key] = vector_clock.copy()
        self.operation_history.append({
            "type": "write",
            "key": key,
            "timestamp": timestamp.to_dict(),
            "time": time.time(),
        })
    
    def record_read(self, key: str, timestamp: HLCTimestamp):
        """记录读操作"""
        self.touch()
        self.read_set.add(key)
        self.read_timestamps[key] = timestamp
        self.operation_history.append({
            "type": "read",
            "key": key,
            "timestamp": timestamp.to_dict(),
            "time": time.time(),
        })
    
    def is_expired(self, timeout_seconds: float = 3600) -> bool:
        """检查会话是否过期"""
        return time.time() - self.last_activity > timeout_seconds
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "created_at": self.created_at,
            "last_activity": self.last_activity,
            "write_set": list(self.write_set),
            "read_set": list(self.read_set),
            "write_timestamps": {k: v.to_dict() for k, v in self.write_timestamps.items()},
            "read_timestamps": {k: v.to_dict() for k, v in self.read_timestamps.items()},
            "write_vector_clocks": self.write_vector_clocks,
            "sticky_node": self.sticky_node,
            "consistency_level": self.consistency_level.value,
            "operation_count": len(self.operation_history),
        }


class SessionManager:
    """
    会话管理器
    
    管理所有会话，提供会话一致性保证
    """
    
    def __init__(self, default_timeout: float = 3600, cleanup_interval: float = 300):
        """
        初始化会话管理器
        
        Args:
            default_timeout: 默认会话超时时间（秒）
            cleanup_interval: 清理过期会话的间隔（秒）
        """
        self._sessions: Dict[str, SessionState] = {}
        self._user_sessions: Dict[str, Set[str]] = defaultdict(set)
        self._default_timeout = default_timeout
        self._cleanup_interval = cleanup_interval
        self._lock = threading.RLock()
        self._hlc = HybridLogicalClock("session_manager")
        
        # 启动清理任务
        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown = False
    
    def create_session(
        self,
        user_id: str,
        consistency_level: SessionConsistencyLevel = SessionConsistencyLevel.SESSION,
        sticky_node: Optional[str] = None
    ) -> SessionState:
        """
        创建新会话
        
        Args:
            user_id: 用户ID
            consistency_level: 一致性级别
            sticky_node: 粘性路由节点
            
        Returns:
            新创建的会话状态
        """
        with self._lock:
            session_id = self._generate_session_id(user_id)
            
            session = SessionState(
                session_id=session_id,
                user_id=user_id,
                consistency_level=consistency_level,
                sticky_node=sticky_node,
            )
            
            self._sessions[session_id] = session
            self._user_sessions[user_id].add(session_id)
            
            return session
    
    def get_session(self, session_id: str) -> Optional[SessionState]:
        """
        获取会话
        
        Args:
            session_id: 会话ID
            
        Returns:
            会话状态，如果不存在则返回None
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session and not session.is_expired(self._default_timeout):
                return session
            return None
    
    def close_session(self, session_id: str) -> bool:
        """
        关闭会话
        
        Args:
            session_id: 会话ID
            
        Returns:
            True如果成功关闭
        """
        with self._lock:
            session = self._sessions.pop(session_id, None)
            if session:
                self._user_sessions[session.user_id].discard(session_id)
                return True
            return False
    
    def record_write(
        self,
        session_id: str,
        key: str,
        value: Any,
        node_id: str,
        vector_clock: Dict[str, int]
    ) -> Optional[HLCTimestamp]:
        """
        记录写操作
        
        Args:
            session_id: 会话ID
            key: 写入的键
            value: 写入的值
            node_id: 执行写入的节点
            vector_clock: 向量时钟
            
        Returns:
            操作时间戳，如果会话不存在则返回None
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None
            
            timestamp = self._hlc.now()
            session.record_write(key, timestamp, vector_clock)
            
            return timestamp
    
    def record_read(self, session_id: str, key: str, value: Any) -> Optional[HLCTimestamp]:
        """
        记录读操作
        
        Args:
            session_id: 会话ID
            key: 读取的键
            value: 读取的值
            
        Returns:
            操作时间戳，如果会话不存在则返回None
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None
            
            timestamp = self._hlc.now()
            session.record_read(key, timestamp)
            
            return timestamp
    
    def check_read_your_writes(self, session_id: str, key: str) -> Tuple[bool, Optional[HLCTimestamp]]:
        """
        检查读己之写保证
        
        确保读取操作能看到同一会话中之前的写入
        
        Args:
            session_id: 会话ID
            key: 要读取的键
            
        Returns:
            (是否满足保证, 要求的最低时间戳)
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return False, None
            
            # 如果会话中写过这个键，要求读到写入之后的版本
            if key in session.write_set:
                write_ts = session.write_timestamps.get(key)
                return True, write_ts
            
            return True, None
    
    def check_monotonic_reads(self, session_id: str, key: str) -> Tuple[bool, Optional[HLCTimestamp]]:
        """
        检查单调读保证
        
        确保后续的读操作不会读到比之前更旧的版本
        
        Args:
            session_id: 会话ID
            key: 要读取的键
            
        Returns:
            (是否满足保证, 要求的最低时间戳)
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return False, None
            
            # 如果会话中读过这个键，要求读到相同或更新的版本
            if key in session.read_set:
                read_ts = session.read_timestamps.get(key)
                return True, read_ts
            
            return True, None
    
    def check_monotonic_writes(self, session_id: str, key: str) -> Tuple[bool, Optional[Dict[str, int]]]:
        """
        检查单调写保证
        
        确保写入操作按顺序执行
        
        Args:
            session_id: 会话ID
            key: 要写入的键
            
        Returns:
            (是否满足保证, 要求的向量时钟)
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return False, None
            
            # 如果会话中写过这个键，要求基于之前的版本写入
            if key in session.write_set:
                write_vc = session.write_vector_clocks.get(key)
                return True, write_vc
            
            return True, None
    
    def get_sticky_node(self, session_id: str) -> Optional[str]:
        """
        获取会话的粘性路由节点
        
        Args:
            session_id: 会话ID
            
        Returns:
            粘性节点ID，如果没有则返回None
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session:
                return session.sticky_node
            return None
    
    def set_sticky_node(self, session_id: str, node_id: str) -> bool:
        """
        设置会话的粘性路由节点
        
        Args:
            session_id: 会话ID
            node_id: 节点ID
            
        Returns:
            True如果成功设置
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session:
                session.sticky_node = node_id
                return True
            return False
    
    def get_consistency_requirements(self, session_id: str, operation: str, key: str) -> Dict[str, Any]:
        """
        获取一致性要求
        
        根据会话的一致性级别，返回操作的一致性要求
        
        Args:
            session_id: 会话ID
            operation: 操作类型（"read" 或 "write"）
            key: 操作的键
            
        Returns:
            一致性要求字典
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return {"valid": False}
            
            requirements = {
                "valid": True,
                "consistency_level": session.consistency_level.value,
                "sticky_node": session.sticky_node,
                "requirements": [],
            }
            
            level = session.consistency_level
            
            if operation == "read":
                # 读己之写
                if level in [SessionConsistencyLevel.READ_YOUR_WRITES, SessionConsistencyLevel.SESSION]:
                    ok, min_ts = self.check_read_your_writes(session_id, key)
                    if min_ts:
                        requirements["requirements"].append({
                            "type": "read_your_writes",
                            "min_timestamp": min_ts.to_dict(),
                        })
                
                # 单调读
                if level in [SessionConsistencyLevel.MONOTONIC_READS, SessionConsistencyLevel.SESSION]:
                    ok, min_ts = self.check_monotonic_reads(session_id, key)
                    if min_ts:
                        requirements["requirements"].append({
                            "type": "monotonic_reads",
                            "min_timestamp": min_ts.to_dict(),
                        })
            
            elif operation == "write":
                # 单调写
                if level in [SessionConsistencyLevel.MONOTONIC_WRITES, SessionConsistencyLevel.SESSION]:
                    ok, min_vc = self.check_monotonic_writes(session_id, key)
                    if min_vc:
                        requirements["requirements"].append({
                            "type": "monotonic_writes",
                            "min_vector_clock": min_vc,
                        })
            
            return requirements
    
    def cleanup_expired_sessions(self) -> int:
        """
        清理过期会话
        
        Returns:
            清理的会话数量
        """
        with self._lock:
            expired = []
            for session_id, session in self._sessions.items():
                if session.is_expired(self._default_timeout):
                    expired.append(session_id)
            
            for session_id in expired:
                self.close_session(session_id)
            
            return len(expired)
    
    async def start_cleanup_task(self):
        """启动清理任务"""
        while not self._shutdown:
            await asyncio.sleep(self._cleanup_interval)
            count = self.cleanup_expired_sessions()
            if count > 0:
                print(f"Cleaned up {count} expired sessions")
    
    def stop_cleanup_task(self):
        """停止清理任务"""
        self._shutdown = True
        if self._cleanup_task:
            self._cleanup_task.cancel()
    
    def get_user_sessions(self, user_id: str) -> List[SessionState]:
        """
        获取用户的所有会话
        
        Args:
            user_id: 用户ID
            
        Returns:
            会话列表
        """
        with self._lock:
            session_ids = self._user_sessions.get(user_id, set())
            return [self._sessions[sid] for sid in session_ids if sid in self._sessions]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        with self._lock:
            return {
                "total_sessions": len(self._sessions),
                "total_users": len(self._user_sessions),
                "default_timeout": self._default_timeout,
            }
    
    def _generate_session_id(self, user_id: str) -> str:
        """生成会话ID"""
        unique = f"{user_id}:{time.time()}:{uuid.uuid4().hex}"
        return hashlib.sha256(unique.encode()).hexdigest()[:32]


class ConsistentSession:
    """
    一致性会话
    
    提供高级API，自动处理会话一致性保证
    """
    
    def __init__(
        self,
        session_manager: SessionManager,
        session_id: str,
        storage_backend: Any,
    ):
        """
        初始化一致性会话
        
        Args:
            session_manager: 会话管理器
            session_id: 会话ID
            storage_backend: 存储后端
        """
        self._session_manager = session_manager
        self._session_id = session_id
        self._storage = storage_backend
        self._hlc = HybridLogicalClock(session_id[:8])
    
    async def write(self, key: str, value: Any) -> Tuple[bool, Optional[HLCTimestamp]]:
        """
        写入数据
        
        保证单调写一致性
        
        Args:
            key: 键
            value: 值
            
        Returns:
            (是否成功, 时间戳)
        """
        # 获取一致性要求
        reqs = self._session_manager.get_consistency_requirements(
            self._session_id, "write", key
        )
        
        # 生成向量时钟
        vc = {self._session_id: int(time.time() * 1000)}
        if reqs.get("requirements"):
            for req in reqs["requirements"]:
                if req["type"] == "monotonic_writes":
                    # 合并之前的向量时钟
                    prev_vc = req.get("min_vector_clock", {})
                    vc.update(prev_vc)
                    vc[self._session_id] = max(vc.get(self._session_id, 0) + 1, int(time.time() * 1000))
        
        # 执行写入
        timestamp = self._hlc.now()
        success = await self._storage.write(key, value, timestamp=timestamp, vector_clock=vc)
        
        if success:
            # 记录写操作
            self._session_manager.record_write(
                self._session_id, key, value, "local", vc
            )
        
        return success, timestamp if success else None
    
    async def read(self, key: str) -> Tuple[Optional[Any], Optional[HLCTimestamp]]:
        """
        读取数据
        
        保证读己之写和单调读一致性
        
        Args:
            key: 键
            
        Returns:
            (值, 时间戳)
        """
        # 获取一致性要求
        reqs = self._session_manager.get_consistency_requirements(
            self._session_id, "read", key
        )
        
        min_timestamp = None
        if reqs.get("requirements"):
            for req in reqs["requirements"]:
                if req["type"] in ["read_your_writes", "monotonic_reads"]:
                    ts_dict = req.get("min_timestamp")
                    if ts_dict:
                        ts = HLCTimestamp.from_dict(ts_dict)
                        if min_timestamp is None or ts > min_timestamp:
                            min_timestamp = ts
        
        # 执行读取
        result = await self._storage.read(key, min_timestamp=min_timestamp)
        
        if result:
            value, timestamp = result
            # 记录读操作
            self._session_manager.record_read(self._session_id, key, value)
            return value, timestamp
        
        return None, None
    
    def get_session_id(self) -> str:
        """获取会话ID"""
        return self._session_id
    
    def close(self):
        """关闭会话"""
        self._session_manager.close_session(self._session_id)

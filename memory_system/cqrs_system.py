"""
CQRS读写分离系统模块 - CQRS System Module

该模块实现了命令查询责任分离（CQRS）模式，包含：
- 命令端（Command Side）实现
- 查询端（Query Side）实现
- 事件存储（Event Store）
- 读模型投影（Read Model Projection）
- 最终一致性保证
- 读写模型同步机制

作者: Agent Memory System Team
版本: 3.0.0
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum, auto
from typing import (
    Any, Callable, Coroutine, Dict, Generic, List, Optional, Set, TypeVar,
    Union, AsyncIterator, Protocol, runtime_checkable, Tuple
)
from contextlib import asynccontextmanager
from functools import wraps

# 配置日志
logger = logging.getLogger(__name__)


# ============================================================================
# 类型定义和枚举
# ============================================================================

class CommandResultStatus(Enum):
    """命令执行结果状态"""
    SUCCESS = auto()
    FAILURE = auto()
    VALIDATION_ERROR = auto()
    CONCURRENCY_ERROR = auto()
    TIMEOUT = auto()


class ProjectionStatus(Enum):
    """投影处理状态"""
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()


class ConsistencyLevel(Enum):
    """一致性级别"""
    EVENTUAL = "eventual"  # 最终一致性
    SESSION = "session"    # 会话一致性
    BOUNDED = "bounded"    # 有界一致性
    STRONG = "strong"      # 强一致性


T = TypeVar('T')
AggregateId = str


# ============================================================================
# 命令基类和接口
# ============================================================================

@dataclass
class Command:
    """
    命令基类
    
    所有写操作命令都应继承此类
    """
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    aggregate_id: Optional[AggregateId] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "command_id": self.command_id,
            "aggregate_id": self.aggregate_id,
            "timestamp": self.timestamp.isoformat(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "metadata": self.metadata,
            "command_type": self.__class__.__name__
        }


@dataclass
class CommandResult:
    """命令执行结果"""
    command_id: str
    status: CommandResultStatus
    aggregate_id: Optional[AggregateId] = None
    version: int = 0
    events: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def is_success(self) -> bool:
        return self.status == CommandResultStatus.SUCCESS
    
    @classmethod
    def success(
        cls,
        command_id: str,
        aggregate_id: AggregateId,
        version: int,
        events: List[Dict[str, Any]] = None
    ) -> CommandResult:
        return cls(
            command_id=command_id,
            status=CommandResultStatus.SUCCESS,
            aggregate_id=aggregate_id,
            version=version,
            events=events or []
        )
    
    @classmethod
    def failure(cls, command_id: str, errors: List[str]) -> CommandResult:
        return cls(
            command_id=command_id,
            status=CommandResultStatus.FAILURE,
            errors=errors
        )


class CommandHandler(ABC, Generic[T]):
    """
    命令处理器抽象基类
    
    处理特定类型的命令，执行业务逻辑
    """
    
    @abstractmethod
    async def handle(self, command: T) -> CommandResult:
        """处理命令"""
        pass
    
    @abstractmethod
    def get_command_type(self) -> type:
        """返回处理的命令类型"""
        pass


# ============================================================================
# 事件存储实现
# ============================================================================

@dataclass
class StoredEvent:
    """存储的事件记录"""
    event_id: str
    aggregate_id: AggregateId
    event_type: str
    event_data: Dict[str, Any]
    version: int
    timestamp: datetime
    sequence_number: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class EventStore(ABC):
    """
    事件存储抽象
    
    持久化领域事件，支持事件溯源
    """
    
    @abstractmethod
    async def append(
        self,
        aggregate_id: AggregateId,
        events: List[StoredEvent],
        expected_version: Optional[int] = None
    ) -> bool:
        """
        追加事件到聚合流
        
        Args:
            aggregate_id: 聚合ID
            events: 要追加的事件列表
            expected_version: 期望的当前版本（乐观并发控制）
            
        Returns:
            是否追加成功
        """
        pass
    
    @abstractmethod
    async def get_events(
        self,
        aggregate_id: AggregateId,
        from_version: int = 1,
        to_version: Optional[int] = None
    ) -> List[StoredEvent]:
        """获取聚合的事件流"""
        pass
    
    @abstractmethod
    async def get_all_events(
        self,
        after_position: int = 0,
        limit: int = 100
    ) -> List[StoredEvent]:
        """获取所有事件（用于投影重建）"""
        pass
    
    @abstractmethod
    async def get_current_version(self, aggregate_id: AggregateId) -> int:
        """获取聚合当前版本"""
        pass


class InMemoryEventStore(EventStore):
    """内存事件存储实现"""
    
    def __init__(self):
        self._streams: Dict[AggregateId, List[StoredEvent]] = defaultdict(list)
        self._all_events: List[StoredEvent] = []
        self._lock = asyncio.Lock()
        self._sequence = 0
    
    async def append(
        self,
        aggregate_id: AggregateId,
        events: List[StoredEvent],
        expected_version: Optional[int] = None
    ) -> bool:
        """追加事件"""
        async with self._lock:
            stream = self._streams[aggregate_id]
            current_version = len(stream)
            
            # 乐观并发控制
            if expected_version is not None and current_version != expected_version:
                raise ConcurrencyException(
                    f"Expected version {expected_version} but found {current_version}"
                )
            
            for i, event in enumerate(events):
                event.version = current_version + i + 1
                event.sequence_number = self._sequence + i + 1
                stream.append(event)
                self._all_events.append(event)
            
            self._sequence += len(events)
            return True
    
    async def get_events(
        self,
        aggregate_id: AggregateId,
        from_version: int = 1,
        to_version: Optional[int] = None
    ) -> List[StoredEvent]:
        """获取聚合事件"""
        stream = self._streams.get(aggregate_id, [])
        to_version = to_version or len(stream)
        return stream[from_version - 1:to_version]
    
    async def get_all_events(
        self,
        after_position: int = 0,
        limit: int = 100
    ) -> List[StoredEvent]:
        """获取所有事件"""
        return self._all_events[after_position:after_position + limit]
    
    async def get_current_version(self, aggregate_id: AggregateId) -> int:
        """获取当前版本"""
        return len(self._streams.get(aggregate_id, []))


class PostgreSQLEventStore(EventStore):
    """
    PostgreSQL事件存储实现
    
    生产级持久化存储，支持事务和索引
    """
    
    def __init__(self, dsn: str, pool_size: int = 10):
        self.dsn = dsn
        self.pool_size = pool_size
        self._pool: Optional[Any] = None
    
    async def connect(self) -> None:
        """连接数据库"""
        try:
            import asyncpg
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=5,
                max_size=self.pool_size
            )
            await self._create_tables()
            logger.info("PostgreSQL event store connected")
        except ImportError:
            logger.error("asyncpg not installed")
            raise
    
    async def disconnect(self) -> None:
        """断开连接"""
        if self._pool:
            await self._pool.close()
    
    async def _create_tables(self) -> None:
        """创建事件表"""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    sequence_number BIGSERIAL PRIMARY KEY,
                    event_id UUID UNIQUE NOT NULL,
                    aggregate_id UUID NOT NULL,
                    event_type VARCHAR(255) NOT NULL,
                    event_data JSONB NOT NULL,
                    version INTEGER NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    metadata JSONB DEFAULT '{}',
                    UNIQUE(aggregate_id, version)
                );
                
                CREATE INDEX IF NOT EXISTS idx_events_aggregate 
                    ON events(aggregate_id, version);
                CREATE INDEX IF NOT EXISTS idx_events_sequence 
                    ON events(sequence_number);
                CREATE INDEX IF NOT EXISTS idx_events_type 
                    ON events(event_type);
            """)
    
    async def append(
        self,
        aggregate_id: AggregateId,
        events: List[StoredEvent],
        expected_version: Optional[int] = None
    ) -> bool:
        """追加事件"""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 检查版本
                if expected_version is not None:
                    current = await conn.fetchval(
                        "SELECT MAX(version) FROM events WHERE aggregate_id = $1",
                        aggregate_id
                    )
                    current = current or 0
                    if current != expected_version:
                        raise ConcurrencyException(
                            f"Expected version {expected_version} but found {current}"
                        )
                
                # 插入事件
                for event in events:
                    await conn.execute(
                        """
                        INSERT INTO events 
                        (event_id, aggregate_id, event_type, event_data, version, timestamp, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        event.event_id,
                        aggregate_id,
                        event.event_type,
                        json.dumps(event.event_data),
                        event.version,
                        event.timestamp,
                        json.dumps(event.metadata)
                    )
        return True
    
    async def get_events(
        self,
        aggregate_id: AggregateId,
        from_version: int = 1,
        to_version: Optional[int] = None
    ) -> List[StoredEvent]:
        """获取聚合事件"""
        async with self._pool.acquire() as conn:
            if to_version:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events 
                    WHERE aggregate_id = $1 AND version >= $2 AND version <= $3
                    ORDER BY version
                    """,
                    aggregate_id, from_version, to_version
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events 
                    WHERE aggregate_id = $1 AND version >= $2
                    ORDER BY version
                    """,
                    aggregate_id, from_version
                )
            
            return [self._row_to_event(row) for row in rows]
    
    async def get_all_events(
        self,
        after_position: int = 0,
        limit: int = 100
    ) -> List[StoredEvent]:
        """获取所有事件"""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM events 
                WHERE sequence_number > $1
                ORDER BY sequence_number
                LIMIT $2
                """,
                after_position, limit
            )
            return [self._row_to_event(row) for row in rows]
    
    async def get_current_version(self, aggregate_id: AggregateId) -> int:
        """获取当前版本"""
        async with self._pool.acquire() as conn:
            version = await conn.fetchval(
                "SELECT MAX(version) FROM events WHERE aggregate_id = $1",
                aggregate_id
            )
            return version or 0
    
    def _row_to_event(self, row: Any) -> StoredEvent:
        """将数据库行转换为事件"""
        return StoredEvent(
            event_id=str(row['event_id']),
            aggregate_id=str(row['aggregate_id']),
            event_type=row['event_type'],
            event_data=json.loads(row['event_data']),
            version=row['version'],
            timestamp=row['timestamp'],
            sequence_number=row['sequence_number'],
            metadata=json.loads(row['metadata'])
        )


class ConcurrencyException(Exception):
    """并发冲突异常"""
    pass


# ============================================================================
# 聚合根基类
# ============================================================================

class AggregateRoot(ABC):
    """
    聚合根基类
    
    实现事件溯源模式的核心，通过应用事件来重建状态
    """
    
    def __init__(self, aggregate_id: AggregateId):
        self._id = aggregate_id
        self._version = 0
        self._uncommitted_events: List[StoredEvent] = []
        self._is_replaying = False
    
    @property
    def id(self) -> AggregateId:
        return self._id
    
    @property
    def version(self) -> int:
        return self._version
    
    @property
    def uncommitted_events(self) -> List[StoredEvent]:
        return self._uncommitted_events.copy()
    
    def apply_event(self, event: StoredEvent) -> None:
        """应用事件到聚合"""
        # 查找事件处理器
        handler_name = f"_on_{event.event_type.lower()}"
        handler = getattr(self, handler_name, None)
        
        if handler:
            handler(event.event_data)
        
        if not self._is_replaying:
            self._uncommitted_events.append(event)
        
        self._version = event.version
    
    def create_event(
        self,
        event_type: str,
        event_data: Dict[str, Any],
        metadata: Dict[str, Any] = None
    ) -> StoredEvent:
        """创建新事件"""
        return StoredEvent(
            event_id=str(uuid.uuid4()),
            aggregate_id=self._id,
            event_type=event_type,
            event_data=event_data,
            version=self._version + 1,
            timestamp=datetime.now(timezone.utc),
            sequence_number=0,  # 由EventStore设置
            metadata=metadata or {}
        )
    
    def mark_committed(self) -> None:
        """标记事件已提交"""
        self._uncommitted_events.clear()
    
    @classmethod
    async def load(
        cls,
        aggregate_id: AggregateId,
        event_store: EventStore
    ) -> Optional[AggregateRoot]:
        """从事件存储加载聚合"""
        events = await event_store.get_events(aggregate_id)
        
        if not events:
            return None
        
        aggregate = cls(aggregate_id)
        aggregate._is_replaying = True
        
        for event in events:
            aggregate.apply_event(event)
        
        aggregate._is_replaying = False
        aggregate._uncommitted_events.clear()
        
        return aggregate


# ============================================================================
# 命令总线
# ============================================================================

class CommandBus:
    """
    命令总线
    
    负责路由命令到对应的处理器，支持中间件和事务
    """
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self._handlers: Dict[type, CommandHandler] = {}
        self._middlewares: List[Callable[[Command, Callable], Coroutine[Any, Any, CommandResult]]] = []
        self._lock = asyncio.Lock()
    
    def register_handler(self, handler: CommandHandler) -> None:
        """注册命令处理器"""
        command_type = handler.get_command_type()
        self._handlers[command_type] = handler
        logger.info(f"Registered handler for {command_type.__name__}")
    
    def add_middleware(
        self,
        middleware: Callable[[Command, Callable], Coroutine[Any, Any, CommandResult]]
    ) -> None:
        """添加中间件"""
        self._middlewares.append(middleware)
    
    async def send(self, command: Command) -> CommandResult:
        """发送命令"""
        handler = self._handlers.get(type(command))
        
        if not handler:
            return CommandResult.failure(
                command.command_id,
                [f"No handler found for command type: {type(command).__name__}"]
            )
        
        # 构建处理链
        handler_call = handler.handle
        for middleware in reversed(self._middlewares):
            handler_call = self._wrap_middleware(middleware, handler_call)
        
        try:
            return await handler_call(command)
        except ConcurrencyException as e:
            return CommandResult(
                command_id=command.command_id,
                status=CommandResultStatus.CONCURRENCY_ERROR,
                errors=[str(e)]
            )
        except Exception as e:
            logger.exception(f"Command handling error: {e}")
            return CommandResult.failure(command.command_id, [str(e)])
    
    def _wrap_middleware(
        self,
        middleware: Callable,
        next_handler: Callable
    ) -> Callable:
        """包装中间件"""
        async def wrapped(command: Command) -> CommandResult:
            return await middleware(command, lambda: next_handler(command))
        return wrapped
    
    async def send_batch(self, commands: List[Command]) -> List[CommandResult]:
        """批量发送命令"""
        results = []
        for command in commands:
            result = await self.send(command)
            results.append(result)
        return results


# ============================================================================
# 查询模型和接口
# ============================================================================

@dataclass
class Query:
    """查询基类"""
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult(Generic[T]):
    """查询结果"""
    query_id: str
    data: Optional[T] = None
    is_success: bool = True
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @classmethod
    def success(cls, query_id: str, data: T, metadata: Dict[str, Any] = None) -> QueryResult[T]:
        return cls(
            query_id=query_id,
            data=data,
            is_success=True,
            metadata=metadata or {}
        )
    
    @classmethod
    def failure(cls, query_id: str, errors: List[str]) -> QueryResult[T]:
        return cls(
            query_id=query_id,
            is_success=False,
            errors=errors
        )


class QueryHandler(ABC, Generic[T]):
    """查询处理器抽象基类"""
    
    @abstractmethod
    async def handle(self, query: Query) -> QueryResult[T]:
        """处理查询"""
        pass
    
    @abstractmethod
    def get_query_type(self) -> type:
        """返回处理的查询类型"""
        pass


# ============================================================================
# 读模型存储
# ============================================================================

class ReadModelStore(ABC, Generic[T]):
    """
    读模型存储抽象
    
    优化查询性能的数据存储
    """
    
    @abstractmethod
    async def get(self, id: str) -> Optional[T]:
        """根据ID获取模型"""
        pass
    
    @abstractmethod
    async def find(self, criteria: Dict[str, Any]) -> List[T]:
        """根据条件查询"""
        pass
    
    @abstractmethod
    async def save(self, id: str, model: T) -> bool:
        """保存模型"""
        pass
    
    @abstractmethod
    async def delete(self, id: str) -> bool:
        """删除模型"""
        pass


class InMemoryReadModelStore(ReadModelStore[T]):
    """内存读模型存储"""
    
    def __init__(self):
        self._data: Dict[str, T] = {}
        self._lock = asyncio.Lock()
    
    async def get(self, id: str) -> Optional[T]:
        return self._data.get(id)
    
    async def find(self, criteria: Dict[str, Any]) -> List[T]:
        results = []
        for item in self._data.values():
            if isinstance(item, dict):
                match = all(
                    item.get(k) == v or (isinstance(v, list) and item.get(k) in v)
                    for k, v in criteria.items()
                )
                if match:
                    results.append(item)
        return results
    
    async def save(self, id: str, model: T) -> bool:
        async with self._lock:
            self._data[id] = model
        return True
    
    async def delete(self, id: str) -> bool:
        async with self._lock:
            if id in self._data:
                del self._data[id]
                return True
        return False


# ============================================================================
# 投影管理器
# ============================================================================

@dataclass
class ProjectionCheckpoint:
    """投影检查点"""
    projection_name: str
    last_sequence: int
    timestamp: datetime
    status: ProjectionStatus


class Projection(ABC):
    """
    投影抽象
    
    将事件转换为读模型
    """
    
    def __init__(self, name: str, read_model_store: ReadModelStore):
        self.name = name
        self.read_model_store = read_model_store
        self._last_sequence = 0
        self._status = ProjectionStatus.PENDING
    
    @abstractmethod
    async def project(self, event: StoredEvent) -> None:
        """投影单个事件"""
        pass
    
    @abstractmethod
    def get_handled_event_types(self) -> Set[str]:
        """返回处理的事件类型"""
        pass
    
    async def reset(self) -> None:
        """重置投影"""
        self._last_sequence = 0
        self._status = ProjectionStatus.PENDING


class ProjectionManager:
    """
    投影管理器
    
    管理所有投影的创建、更新和重建
    """
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self._projections: Dict[str, Projection] = {}
        self._checkpoints: Dict[str, ProjectionCheckpoint] = {}
        self._running = False
        self._poll_interval = 1.0
        self._processor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
    
    def register_projection(self, projection: Projection) -> None:
        """注册投影"""
        self._projections[projection.name] = projection
        self._checkpoints[projection.name] = ProjectionCheckpoint(
            projection_name=projection.name,
            last_sequence=0,
            timestamp=datetime.now(timezone.utc),
            status=ProjectionStatus.PENDING
        )
        logger.info(f"Registered projection: {projection.name}")
    
    async def start(self) -> None:
        """启动投影处理"""
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        logger.info("Projection manager started")
    
    async def stop(self) -> None:
        """停止投影处理"""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        logger.info("Projection manager stopped")
    
    async def _process_events(self) -> None:
        """处理事件的循环"""
        while self._running:
            try:
                for projection in self._projections.values():
                    await self._process_projection(projection)
                
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Projection processing error: {e}")
                await asyncio.sleep(self._poll_interval)
    
    async def _process_projection(self, projection: Projection) -> None:
        """处理单个投影"""
        checkpoint = self._checkpoints[projection.name]
        
        if checkpoint.status == ProjectionStatus.PROCESSING:
            return
        
        checkpoint.status = ProjectionStatus.PROCESSING
        
        try:
            # 获取新事件
            events = await self.event_store.get_all_events(
                checkpoint.last_sequence,
                limit=100
            )
            
            if not events:
                checkpoint.status = ProjectionStatus.COMPLETED
                return
            
            # 处理事件
            for event in events:
                if event.event_type in projection.get_handled_event_types():
                    await projection.project(event)
                checkpoint.last_sequence = event.sequence_number
            
            checkpoint.timestamp = datetime.now(timezone.utc)
            checkpoint.status = ProjectionStatus.COMPLETED
            
        except Exception as e:
            logger.exception(f"Projection {projection.name} error: {e}")
            checkpoint.status = ProjectionStatus.FAILED
    
    async def rebuild_projection(self, projection_name: str) -> None:
        """重建指定投影"""
        projection = self._projections.get(projection_name)
        if not projection:
            raise ValueError(f"Projection not found: {projection_name}")
        
        logger.info(f"Rebuilding projection: {projection_name}")
        
        # 重置投影
        await projection.reset()
        
        # 重置检查点
        self._checkpoints[projection_name] = ProjectionCheckpoint(
            projection_name=projection_name,
            last_sequence=0,
            timestamp=datetime.now(timezone.utc),
            status=ProjectionStatus.PENDING
        )
        
        # 重新处理所有事件
        await self._process_projection(projection)
        
        logger.info(f"Projection {projection_name} rebuilt")
    
    async def rebuild_all_projections(self) -> None:
        """重建所有投影"""
        for name in self._projections:
            await self.rebuild_projection(name)
    
    def get_checkpoint(self, projection_name: str) -> Optional[ProjectionCheckpoint]:
        """获取投影检查点"""
        return self._checkpoints.get(projection_name)


# ============================================================================
# 查询总线
# ============================================================================

class QueryBus:
    """
    查询总线
    
    负责路由查询到对应的处理器，支持缓存
    """
    
    def __init__(self):
        self._handlers: Dict[type, QueryHandler] = {}
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        self._cache_ttl = 60  # 默认缓存60秒
        self._lock = asyncio.Lock()
    
    def register_handler(self, handler: QueryHandler) -> None:
        """注册查询处理器"""
        query_type = handler.get_query_type()
        self._handlers[query_type] = handler
        logger.info(f"Registered query handler for {query_type.__name__}")
    
    async def query(self, query: Query, use_cache: bool = False) -> QueryResult:
        """执行查询"""
        cache_key = None
        
        # 检查缓存
        if use_cache:
            cache_key = self._get_cache_key(query)
            cached = await self._get_cached(cache_key)
            if cached is not None:
                return QueryResult.success(query.query_id, cached)
        
        # 查找处理器
        handler = self._handlers.get(type(query))
        
        if not handler:
            return QueryResult.failure(
                query.query_id,
                [f"No handler found for query type: {type(query).__name__}"]
            )
        
        try:
            result = await handler.handle(query)
            
            # 缓存结果
            if use_cache and result.is_success and cache_key:
                await self._set_cached(cache_key, result.data)
            
            return result
        except Exception as e:
            logger.exception(f"Query handling error: {e}")
            return QueryResult.failure(query.query_id, [str(e)])
    
    def _get_cache_key(self, query: Query) -> str:
        """生成缓存键"""
        query_dict = query.to_dict() if hasattr(query, 'to_dict') else asdict(query)
        query_str = json.dumps(query_dict, sort_keys=True, default=str)
        return hashlib.md5(query_str.encode()).hexdigest()
    
    async def _get_cached(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        async with self._lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if (datetime.now(timezone.utc) - timestamp).seconds < self._cache_ttl:
                    return value
                else:
                    del self._cache[key]
        return None
    
    async def _set_cached(self, key: str, value: Any) -> None:
        """设置缓存值"""
        async with self._lock:
            self._cache[key] = (value, datetime.now(timezone.utc))
    
    async def invalidate_cache(self, pattern: Optional[str] = None) -> None:
        """使缓存失效"""
        async with self._lock:
            if pattern:
                keys_to_remove = [k for k in self._cache if pattern in k]
                for k in keys_to_remove:
                    del self._cache[k]
            else:
                self._cache.clear()


# ============================================================================
# CQRS门面
# ============================================================================

class CQRSSystem:
    """
    CQRS系统门面
    
    整合命令和查询端，提供统一的API
    """
    
    def __init__(
        self,
        event_store: EventStore,
        consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    ):
        self.event_store = event_store
        self.consistency_level = consistency_level
        self.command_bus = CommandBus(event_store)
        self.query_bus = QueryBus()
        self.projection_manager = ProjectionManager(event_store)
        self._readiness_callbacks: List[Callable[[], Coroutine[Any, Any, bool]]] = []
    
    async def start(self) -> None:
        """启动CQRS系统"""
        await self.projection_manager.start()
        logger.info("CQRS system started")
    
    async def stop(self) -> None:
        """停止CQRS系统"""
        await self.projection_manager.stop()
        logger.info("CQRS system stopped")
    
    async def execute(self, command: Command) -> CommandResult:
        """执行命令"""
        result = await self.command_bus.send(command)
        
        # 如果需要强一致性，等待投影更新
        if self.consistency_level == ConsistencyLevel.STRONG and result.is_success:
            await self._wait_for_projections(result.aggregate_id)
        
        return result
    
    async def query(self, query: Query, use_cache: bool = False) -> QueryResult:
        """执行查询"""
        return await self.query_bus.query(query, use_cache)
    
    def register_command_handler(self, handler: CommandHandler) -> None:
        """注册命令处理器"""
        self.command_bus.register_handler(handler)
    
    def register_query_handler(self, handler: QueryHandler) -> None:
        """注册查询处理器"""
        self.query_bus.register_handler(handler)
    
    def register_projection(self, projection: Projection) -> None:
        """注册投影"""
        self.projection_manager.register_projection(projection)
    
    async def _wait_for_projections(self, aggregate_id: AggregateId, timeout: float = 5.0) -> bool:
        """等待投影更新"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_synced = True
            for name, checkpoint in self.projection_manager._checkpoints.items():
                # 获取聚合的最新序列号
                events = await self.event_store.get_events(aggregate_id)
                if events:
                    latest_sequence = events[-1].sequence_number
                    if checkpoint.last_sequence < latest_sequence:
                        all_synced = False
                        break
            
            if all_synced:
                return True
            
            await asyncio.sleep(0.1)
        
        logger.warning(f"Timeout waiting for projections for aggregate {aggregate_id}")
        return False
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            "status": "healthy",
            "command_handlers": len(self.command_bus._handlers),
            "query_handlers": len(self.query_bus._handlers),
            "projections": {
                name: {
                    "last_sequence": checkpoint.last_sequence,
                    "status": checkpoint.status.name
                }
                for name, checkpoint in self.projection_manager._checkpoints.items()
            }
        }


# ============================================================================
# 中间件实现
# ============================================================================

async def logging_middleware(command: Command, next: Callable) -> CommandResult:
    """日志中间件"""
    logger.info(f"Executing command: {type(command).__name__} ({command.command_id})")
    start_time = time.time()
    
    result = await next()
    
    duration = time.time() - start_time
    logger.info(
        f"Command {command.command_id} completed in {duration:.3f}s "
        f"with status: {result.status.name}"
    )
    
    return result


async def validation_middleware(command: Command, next: Callable) -> CommandResult:
    """验证中间件"""
    # 检查命令是否有验证方法
    validate = getattr(command, 'validate', None)
    if validate:
        errors = validate()
        if errors:
            return CommandResult(
                command_id=command.command_id,
                status=CommandResultStatus.VALIDATION_ERROR,
                errors=errors
            )
    
    return await next()


async def transaction_middleware(command: Command, next: Callable) -> CommandResult:
    """事务中间件"""
    # 在实际实现中，这里会管理数据库事务
    try:
        return await next()
    except Exception as e:
        logger.exception(f"Transaction failed: {e}")
        # 回滚事务
        raise


# ============================================================================
# 使用示例
# ============================================================================

# 示例：订单聚合
class OrderAggregate(AggregateRoot):
    """订单聚合根"""
    
    def __init__(self, aggregate_id: AggregateId):
        super().__init__(aggregate_id)
        self._customer_id: Optional[str] = None
        self._items: List[Dict[str, Any]] = []
        self._status = "pending"
        self._total: float = 0.0
    
    @classmethod
    def create(cls, order_id: str, customer_id: str, items: List[Dict[str, Any]]) -> OrderAggregate:
        """创建订单"""
        aggregate = cls(order_id)
        event_data = {
            "order_id": order_id,
            "customer_id": customer_id,
            "items": items,
            "total": sum(item["price"] * item["quantity"] for item in items)
        }
        event = aggregate.create_event("order_created", event_data)
        aggregate.apply_event(event)
        return aggregate
    
    def confirm(self) -> None:
        """确认订单"""
        if self._status != "pending":
            raise ValueError(f"Cannot confirm order in status: {self._status}")
        
        event = self.create_event("order_confirmed", {"order_id": self._id})
        self.apply_event(event)
    
    def cancel(self, reason: str) -> None:
        """取消订单"""
        if self._status == "completed":
            raise ValueError("Cannot cancel completed order")
        
        event = self.create_event("order_cancelled", {
            "order_id": self._id,
            "reason": reason
        })
        self.apply_event(event)
    
    # 事件处理器
    def _on_order_created(self, data: Dict[str, Any]) -> None:
        self._customer_id = data["customer_id"]
        self._items = data["items"]
        self._total = data["total"]
        self._status = "pending"
    
    def _on_order_confirmed(self, data: Dict[str, Any]) -> None:
        self._status = "confirmed"
    
    def _on_order_cancelled(self, data: Dict[str, Any]) -> None:
        self._status = "cancelled"


# 示例命令
@dataclass
class CreateOrderCommand(Command):
    """创建订单命令"""
    customer_id: str = ""
    items: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ConfirmOrderCommand(Command):
    """确认订单命令"""
    order_id: str = ""


# 示例命令处理器
class CreateOrderHandler(CommandHandler[CreateOrderCommand]):
    """创建订单处理器"""
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
    
    def get_command_type(self) -> type:
        return CreateOrderCommand
    
    async def handle(self, command: CreateOrderCommand) -> CommandResult:
        order_id = str(uuid.uuid4())
        
        # 创建聚合
        aggregate = OrderAggregate.create(
            order_id,
            command.customer_id,
            command.items
        )
        
        # 保存事件
        await self.event_store.append(
            order_id,
            aggregate.uncommitted_events,
            expected_version=0
        )
        
        aggregate.mark_committed()
        
        return CommandResult.success(
            command_id=command.command_id,
            aggregate_id=order_id,
            version=aggregate.version,
            events=[e.event_data for e in aggregate.uncommitted_events]
        )


class ConfirmOrderHandler(CommandHandler[ConfirmOrderCommand]):
    """确认订单处理器"""
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
    
    def get_command_type(self) -> type:
        return ConfirmOrderCommand
    
    async def handle(self, command: ConfirmOrderCommand) -> CommandResult:
        # 加载聚合
        aggregate = await OrderAggregate.load(command.order_id, self.event_store)
        
        if not aggregate:
            return CommandResult.failure(
                command.command_id,
                [f"Order not found: {command.order_id}"]
            )
        
        # 执行业务操作
        try:
            aggregate.confirm()
        except ValueError as e:
            return CommandResult.failure(command.command_id, [str(e)])
        
        # 保存事件
        await self.event_store.append(
            command.order_id,
            aggregate.uncommitted_events,
            expected_version=aggregate.version - len(aggregate.uncommitted_events)
        )
        
        aggregate.mark_committed()
        
        return CommandResult.success(
            command_id=command.command_id,
            aggregate_id=command.order_id,
            version=aggregate.version
        )


# 示例查询
@dataclass
class GetOrderQuery(Query):
    """获取订单查询"""
    order_id: str = ""


@dataclass
class ListOrdersQuery(Query):
    """列出订单查询"""
    customer_id: Optional[str] = None
    status: Optional[str] = None
    page: int = 1
    page_size: int = 20


# 示例读模型
@dataclass
class OrderReadModel:
    """订单读模型"""
    order_id: str
    customer_id: str
    items: List[Dict[str, Any]]
    total: float
    status: str
    created_at: str


# 示例投影
class OrderProjection(Projection):
    """订单投影"""
    
    def __init__(self, read_model_store: ReadModelStore[OrderReadModel]):
        super().__init__("order_projection", read_model_store)
    
    def get_handled_event_types(self) -> Set[str]:
        return {"order_created", "order_confirmed", "order_cancelled"}
    
    async def project(self, event: StoredEvent) -> None:
        if event.event_type == "order_created":
            model = OrderReadModel(
                order_id=event.event_data["order_id"],
                customer_id=event.event_data["customer_id"],
                items=event.event_data["items"],
                total=event.event_data["total"],
                status="pending",
                created_at=event.timestamp.isoformat()
            )
            await self.read_model_store.save(model.order_id, model)
        
        elif event.event_type == "order_confirmed":
            model = await self.read_model_store.get(event.event_data["order_id"])
            if model:
                # 更新状态
                updated = OrderReadModel(
                    order_id=model.order_id,
                    customer_id=model.customer_id,
                    items=model.items,
                    total=model.total,
                    status="confirmed",
                    created_at=model.created_at
                )
                await self.read_model_store.save(updated.order_id, updated)
        
        elif event.event_type == "order_cancelled":
            model = await self.read_model_store.get(event.event_data["order_id"])
            if model:
                updated = OrderReadModel(
                    order_id=model.order_id,
                    customer_id=model.customer_id,
                    items=model.items,
                    total=model.total,
                    status="cancelled",
                    created_at=model.created_at
                )
                await self.read_model_store.save(updated.order_id, updated)


# 示例查询处理器
class GetOrderHandler(QueryHandler[OrderReadModel]):
    """获取订单处理器"""
    
    def __init__(self, read_model_store: ReadModelStore[OrderReadModel]):
        self.read_model_store = read_model_store
    
    def get_query_type(self) -> type:
        return GetOrderQuery
    
    async def handle(self, query: GetOrderQuery) -> QueryResult[OrderReadModel]:
        model = await self.read_model_store.get(query.order_id)
        
        if not model:
            return QueryResult.failure(
                query.query_id,
                [f"Order not found: {query.order_id}"]
            )
        
        return QueryResult.success(query.query_id, model)


async def example_usage():
    """使用示例"""
    # 创建事件存储
    event_store = InMemoryEventStore()
    
    # 创建CQRS系统
    cqrs = CQRSSystem(event_store)
    
    # 注册命令处理器
    cqrs.register_command_handler(CreateOrderHandler(event_store))
    cqrs.register_command_handler(ConfirmOrderHandler(event_store))
    
    # 创建读模型存储
    read_model_store = InMemoryReadModelStore[OrderReadModel]()
    
    # 注册投影
    order_projection = OrderProjection(read_model_store)
    cqrs.register_projection(order_projection)
    
    # 注册查询处理器
    cqrs.register_query_handler(GetOrderHandler(read_model_store))
    
    # 启动系统
    await cqrs.start()
    
    try:
        # 执行创建订单命令
        create_cmd = CreateOrderCommand(
            customer_id="customer-123",
            items=[
                {"product_id": "p1", "name": "Product 1", "price": 10.0, "quantity": 2},
                {"product_id": "p2", "name": "Product 2", "price": 20.0, "quantity": 1}
            ]
        )
        
        result = await cqrs.execute(create_cmd)
        print(f"Create order result: {result}")
        
        if result.is_success:
            order_id = result.aggregate_id
            
            # 等待投影更新
            await asyncio.sleep(0.5)
            
            # 查询订单
            query = GetOrderQuery(order_id=order_id)
            query_result = await cqrs.query(query)
            print(f"Query result: {query_result}")
            
            # 确认订单
            confirm_cmd = ConfirmOrderCommand(order_id=order_id)
            confirm_result = await cqrs.execute(confirm_cmd)
            print(f"Confirm order result: {confirm_result}")
            
            # 等待投影更新
            await asyncio.sleep(0.5)
            
            # 再次查询
            query_result = await cqrs.query(query)
            print(f"Updated query result: {query_result}")
        
        # 健康检查
        health = await cqrs.health_check()
        print(f"Health check: {health}")
        
    finally:
        await cqrs.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())

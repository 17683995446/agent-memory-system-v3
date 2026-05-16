"""
事件驱动架构核心模块 - Event Driven Core Module

该模块实现了云原生事件驱动架构的核心组件，包括：
- 事件总线（Event Bus）抽象
- Kafka和RabbitMQ后端支持
- 事件发布/订阅模式
- 事件溯源（Event Sourcing）
- Saga编排器实现
- 死信队列（DLQ）处理

作者: Agent Memory System Team
版本: 3.0.0
"""

from __future__ import annotations

import asyncio
import hashlib
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
    Union, AsyncIterator, Protocol, runtime_checkable
)
from contextlib import asynccontextmanager
import threading

# 配置日志
logger = logging.getLogger(__name__)


# ============================================================================
# 类型定义和枚举
# ============================================================================

class EventPriority(Enum):
    """事件优先级枚举"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class EventStatus(Enum):
    """事件处理状态"""
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()
    RETRYING = auto()
    DLQ = auto()


class SagaStatus(Enum):
    """Saga执行状态"""
    STARTED = auto()
    RUNNING = auto()
    COMPENSATING = auto()
    COMPLETED = auto()
    FAILED = auto()


class BackendType(Enum):
    """消息后端类型"""
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
    IN_MEMORY = "in_memory"


T = TypeVar('T')
EventHandler = Callable[['DomainEvent'], Coroutine[Any, Any, None]]


# ============================================================================
# 事件数据模型
# ============================================================================

@dataclass(frozen=True)
class EventId:
    """事件唯一标识符"""
    value: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class AggregateId:
    """聚合根唯一标识符"""
    value: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __str__(self) -> str:
        return self.value


@dataclass
class DomainEvent:
    """
    领域事件基类
    
    所有业务事件都应继承此类，包含完整的元数据信息
    """
    event_id: EventId = field(default_factory=EventId)
    aggregate_id: AggregateId = field(default_factory=AggregateId)
    event_type: str = field(default="base_event")
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1
    priority: EventPriority = EventPriority.NORMAL
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    payload: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "event_id": str(self.event_id),
            "aggregate_id": str(self.aggregate_id),
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "version": self.version,
            "priority": self.priority.name,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "metadata": self.metadata,
            "payload": self.payload
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DomainEvent:
        """从字典创建事件"""
        return cls(
            event_id=EventId(data.get("event_id", str(uuid.uuid4()))),
            aggregate_id=AggregateId(data.get("aggregate_id", str(uuid.uuid4()))),
            event_type=data.get("event_type", "base_event"),
            timestamp=datetime.fromisoformat(data.get("timestamp", datetime.now(timezone.utc).isoformat())),
            version=data.get("version", 1),
            priority=EventPriority[data.get("priority", "NORMAL")],
            correlation_id=data.get("correlation_id"),
            causation_id=data.get("causation_id"),
            metadata=data.get("metadata", {}),
            payload=data.get("payload", {})
        )
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> DomainEvent:
        """从JSON字符串创建事件"""
        return cls.from_dict(json.loads(json_str))


@dataclass
class EventRecord:
    """事件记录，用于事件存储"""
    event: DomainEvent
    sequence_number: int
    status: EventStatus = EventStatus.PENDING
    retry_count: int = 0
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ============================================================================
# 事件总线抽象
# ============================================================================

class EventBus(ABC):
    """
    事件总线抽象基类
    
    定义事件发布和订阅的标准接口，支持多种后端实现
    """
    
    def __init__(self, backend_type: BackendType):
        self.backend_type = backend_type
        self._handlers: Dict[str, List[EventHandler]] = defaultdict(list)
        self._running = False
        self._lock = asyncio.Lock()
        
    @abstractmethod
    async def connect(self) -> None:
        """连接到消息后端"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """断开与消息后端的连接"""
        pass
    
    @abstractmethod
    async def publish(self, event: DomainEvent, topic: Optional[str] = None) -> bool:
        """
        发布事件到指定主题
        
        Args:
            event: 要发布的事件
            topic: 目标主题，默认为None使用事件类型
            
        Returns:
            发布是否成功
        """
        pass
    
    @abstractmethod
    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """
        订阅特定类型的事件
        
        Args:
            event_type: 事件类型
            handler: 事件处理器
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        """取消订阅"""
        pass
    
    async def start(self) -> None:
        """启动事件总线"""
        async with self._lock:
            if not self._running:
                await self.connect()
                self._running = True
                logger.info(f"EventBus [{self.backend_type.value}] started")
    
    async def stop(self) -> None:
        """停止事件总线"""
        async with self._lock:
            if self._running:
                await self.disconnect()
                self._running = False
                logger.info(f"EventBus [{self.backend_type.value}] stopped")
    
    @asynccontextmanager
    async def session(self):
        """上下文管理器，自动启动和停止"""
        await self.start()
        try:
            yield self
        finally:
            await self.stop()


# ============================================================================
# In-Memory 事件总线实现
# ============================================================================

class InMemoryEventBus(EventBus):
    """
    内存事件总线实现
    
    用于开发和测试环境，支持异步事件处理
    """
    
    def __init__(self, max_queue_size: int = 10000):
        super().__init__(BackendType.IN_MEMORY)
        self._event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._consumer_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
    async def connect(self) -> None:
        """启动消费者任务"""
        self._consumer_task = asyncio.create_task(self._consume_events())
        
    async def disconnect(self) -> None:
        """停止消费者任务"""
        self._shutdown_event.set()
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
    
    async def publish(self, event: DomainEvent, topic: Optional[str] = None) -> bool:
        """发布事件到内存队列"""
        try:
            # 使用优先级队列，数值越小优先级越高
            priority = event.priority.value
            await self._event_queue.put((priority, time.time(), event))
            logger.debug(f"Published event {event.event_id} to memory queue")
            return True
        except asyncio.QueueFull:
            logger.error(f"Event queue full, failed to publish {event.event_id}")
            return False
    
    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """订阅事件"""
        self._handlers[event_type].append(handler)
        logger.info(f"Subscribed handler to event type: {event_type}")
    
    async def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        """取消订阅"""
        if handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)
            logger.info(f"Unsubscribed handler from event type: {event_type}")
    
    async def _consume_events(self) -> None:
        """消费事件的循环"""
        while not self._shutdown_event.is_set():
            try:
                # 等待事件，超时1秒检查关闭信号
                priority, _, event = await asyncio.wait_for(
                    self._event_queue.get(), timeout=1.0
                )
                await self._process_event(event)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error consuming event: {e}")
    
    async def _process_event(self, event: DomainEvent) -> None:
        """处理单个事件"""
        handlers = self._handlers.get(event.event_type, [])
        
        if not handlers:
            logger.warning(f"No handlers for event type: {event.event_type}")
            return
        
        # 并行执行所有处理器
        tasks = [self._safe_handle(handler, event) for handler in handlers]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _safe_handle(self, handler: EventHandler, event: DomainEvent) -> None:
        """安全地执行处理器"""
        try:
            await handler(event)
        except Exception as e:
            logger.exception(f"Handler error for event {event.event_id}: {e}")


# ============================================================================
# Kafka 事件总线实现
# ============================================================================

class KafkaEventBus(EventBus):
    """
    Kafka事件总线实现
    
    生产级消息队列支持，支持分区和消费者组
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: Optional[str] = None,
        group_id: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        super().__init__(BackendType.KAFKA)
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id or f"ams-client-{uuid.uuid4().hex[:8]}"
        self.group_id = group_id or "ams-consumer-group"
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._producer: Optional[Any] = None
        self._consumer: Optional[Any] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
    async def connect(self) -> None:
        """连接到Kafka集群"""
        try:
            # 延迟导入，避免强制依赖
            from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
            
            # 创建生产者
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retry_backoff_ms=int(self.retry_delay * 1000),
                max_retries=self.max_retries
            )
            await self._producer.start()
            
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            
        except ImportError:
            logger.error("aiokafka not installed, cannot use Kafka backend")
            raise
        except Exception as e:
            logger.exception(f"Failed to connect to Kafka: {e}")
            raise
    
    async def disconnect(self) -> None:
        """断开Kafka连接"""
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        
        if self._producer:
            await self._producer.stop()
        
        if self._consumer:
            await self._consumer.stop()
            
        logger.info("Disconnected from Kafka")
    
    async def publish(
        self,
        event: DomainEvent,
        topic: Optional[str] = None,
        key: Optional[str] = None
    ) -> bool:
        """发布事件到Kafka主题"""
        if not self._producer:
            logger.error("Kafka producer not connected")
            return False
        
        topic = topic or f"ams-events-{event.event_type}"
        
        try:
            # 使用聚合ID作为分区键，保证顺序性
            partition_key = key or str(event.aggregate_id)
            
            await self._producer.send(
                topic,
                key=partition_key,
                value=event.to_dict()
            )
            
            logger.debug(f"Published event {event.event_id} to Kafka topic {topic}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to publish to Kafka: {e}")
            return False
    
    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """订阅事件类型"""
        self._handlers[event_type].append(handler)
        
        # 如果这是第一个处理器，启动消费者
        if len(self._handlers[event_type]) == 1:
            await self._start_consumer(event_type)
    
    async def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        """取消订阅"""
        if handler in self._handlers.get(event_type, []):
            self._handlers[event_type].remove(handler)
    
    async def _start_consumer(self, event_type: str) -> None:
        """启动Kafka消费者"""
        try:
            from aiokafka import AIOKafkaConsumer
            
            topic = f"ams-events-{event_type}"
            
            self._consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                client_id=f"{self.client_id}-consumer",
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
            
            await self._consumer.start()
            self._consumer_task = asyncio.create_task(self._consume_loop())
            
            logger.info(f"Started Kafka consumer for topic: {topic}")
            
        except ImportError:
            logger.error("aiokafka not installed")
        except Exception as e:
            logger.exception(f"Failed to start Kafka consumer: {e}")
    
    async def _consume_loop(self) -> None:
        """Kafka消费循环"""
        try:
            async for msg in self._consumer:
                try:
                    event = DomainEvent.from_dict(msg.value)
                    await self._process_event(event)
                except Exception as e:
                    logger.exception(f"Error processing Kafka message: {e}")
                    
        except asyncio.CancelledError:
            logger.info("Kafka consumer cancelled")
        except Exception as e:
            logger.exception(f"Kafka consumer error: {e}")
    
    async def _process_event(self, event: DomainEvent) -> None:
        """处理事件"""
        handlers = self._handlers.get(event.event_type, [])
        
        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:
                logger.exception(f"Handler error: {e}")


# ============================================================================
# RabbitMQ 事件总线实现
# ============================================================================

class RabbitMQEventBus(EventBus):
    """
    RabbitMQ事件总线实现
    
    支持复杂路由模式、死信队列和消息确认
    """
    
    def __init__(
        self,
        amqp_url: str = "amqp://guest:guest@localhost:5672/",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        enable_dlq: bool = True
    ):
        super().__init__(BackendType.RABBITMQ)
        self.amqp_url = amqp_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.enable_dlq = enable_dlq
        self._connection: Optional[Any] = None
        self._channel: Optional[Any] = None
        self._dlq_channel: Optional[Any] = None
        self._exchange_name = "ams.events"
        self._dlq_exchange = "ams.events.dlq"
        
    async def connect(self) -> None:
        """连接到RabbitMQ"""
        try:
            import aio_pika
            
            self._connection = await aio_pika.connect_robust(self.amqp_url)
            self._channel = await self._connection.channel()
            
            # 声明主交换器
            self._exchange = await self._channel.declare_exchange(
                self._exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            # 配置死信队列
            if self.enable_dlq:
                await self._setup_dlq()
            
            logger.info("Connected to RabbitMQ")
            
        except ImportError:
            logger.error("aio-pika not installed, cannot use RabbitMQ backend")
            raise
        except Exception as e:
            logger.exception(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def _setup_dlq(self) -> None:
        """配置死信队列"""
        import aio_pika
        
        self._dlq_channel = await self._connection.channel()
        
        # 声明死信交换器
        dlq_exchange = await self._dlq_channel.declare_exchange(
            self._dlq_exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )
        
        # 声明死信队列
        dlq_queue = await self._dlq_channel.declare_queue(
            "ams.events.dlq.queue",
            durable=True
        )
        await dlq_queue.bind(dlq_exchange, routing_key="#")
        
        logger.info("DLQ configured")
    
    async def disconnect(self) -> None:
        """断开RabbitMQ连接"""
        if self._connection:
            await self._connection.close()
        logger.info("Disconnected from RabbitMQ")
    
    async def publish(self, event: DomainEvent, topic: Optional[str] = None) -> bool:
        """发布事件到RabbitMQ"""
        if not self._channel:
            logger.error("RabbitMQ channel not available")
            return False
        
        try:
            import aio_pika
            
            routing_key = topic or f"event.{event.event_type}"
            
            message = aio_pika.Message(
                body=json.dumps(event.to_dict(), default=str).encode('utf-8'),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=event.priority.value,
                message_id=str(event.event_id),
                correlation_id=event.correlation_id,
                timestamp=event.timestamp
            )
            
            await self._exchange.publish(message, routing_key=routing_key)
            
            logger.debug(f"Published event {event.event_id} to RabbitMQ")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to publish to RabbitMQ: {e}")
            return False
    
    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """订阅事件"""
        self._handlers[event_type].append(handler)
        
        # 为事件类型创建队列
        await self._create_queue_for_event_type(event_type)
    
    async def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        """取消订阅"""
        if handler in self._handlers.get(event_type, []):
            self._handlers[event_type].remove(handler)
    
    async def _create_queue_for_event_type(self, event_type: str) -> None:
        """为事件类型创建队列和绑定"""
        try:
            import aio_pika
            
            queue_name = f"ams.queue.{event_type}"
            routing_key = f"event.{event_type}"
            
            # 队列参数
            arguments = {}
            if self.enable_dlq:
                arguments["x-dead-letter-exchange"] = self._dlq_exchange
                arguments["x-dead-letter-routing-key"] = f"failed.{event_type}"
            
            queue = await self._channel.declare_queue(
                queue_name,
                durable=True,
                arguments=arguments
            )
            
            await queue.bind(self._exchange, routing_key=routing_key)
            
            # 开始消费
            await queue.consume(self._on_message)
            
            logger.info(f"Created queue {queue_name} for event type {event_type}")
            
        except Exception as e:
            logger.exception(f"Failed to create queue: {e}")
    
    async def _on_message(self, message: Any) -> None:
        """处理收到的消息"""
        async with message.process():
            try:
                body = json.loads(message.body.decode('utf-8'))
                event = DomainEvent.from_dict(body)
                
                await self._process_event(event)
                
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                # 消息会被拒绝并发送到DLQ（如果配置了）
                raise
    
    async def _process_event(self, event: DomainEvent) -> None:
        """处理事件"""
        handlers = self._handlers.get(event.event_type, [])
        
        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:
                logger.exception(f"Handler error: {e}")


# ============================================================================
# 事件溯源存储
# ============================================================================

class EventStore(ABC):
    """
    事件存储抽象
    
    用于持久化领域事件，支持事件溯源模式
    """
    
    @abstractmethod
    async def append(self, event: DomainEvent, expected_version: Optional[int] = None) -> bool:
        """追加事件到存储"""
        pass
    
    @abstractmethod
    async def get_events(
        self,
        aggregate_id: AggregateId,
        from_version: int = 1,
        to_version: Optional[int] = None
    ) -> List[DomainEvent]:
        """获取聚合的事件流"""
        pass
    
    @abstractmethod
    async def get_all_events(
        self,
        after_position: int = 0,
        limit: int = 100
    ) -> List[EventRecord]:
        """获取所有事件（用于投影重建）"""
        pass


class InMemoryEventStore(EventStore):
    """内存事件存储实现"""
    
    def __init__(self):
        self._streams: Dict[str, List[DomainEvent]] = defaultdict(list)
        self._all_events: List[EventRecord] = []
        self._lock = asyncio.Lock()
        self._sequence = 0
    
    async def append(self, event: DomainEvent, expected_version: Optional[int] = None) -> bool:
        """追加事件"""
        async with self._lock:
            stream_id = str(event.aggregate_id)
            stream = self._streams[stream_id]
            
            # 乐观并发控制
            if expected_version is not None and len(stream) != expected_version - 1:
                raise ConcurrencyException(
                    f"Expected version {expected_version} but found {len(stream) + 1}"
                )
            
            stream.append(event)
            
            self._sequence += 1
            record = EventRecord(
                event=event,
                sequence_number=self._sequence
            )
            self._all_events.append(record)
            
            logger.debug(f"Appended event {event.event_id} to stream {stream_id}")
            return True
    
    async def get_events(
        self,
        aggregate_id: AggregateId,
        from_version: int = 1,
        to_version: Optional[int] = None
    ) -> List[DomainEvent]:
        """获取聚合事件"""
        stream = self._streams.get(str(aggregate_id), [])
        to_version = to_version or len(stream)
        return stream[from_version - 1:to_version]
    
    async def get_all_events(
        self,
        after_position: int = 0,
        limit: int = 100
    ) -> List[EventRecord]:
        """获取所有事件"""
        return self._all_events[after_position:after_position + limit]


class ConcurrencyException(Exception):
    """并发冲突异常"""
    pass


# ============================================================================
# Saga编排器
# ============================================================================

@dataclass
class SagaStep:
    """Saga步骤定义"""
    name: str
    action: Callable[[Any], Coroutine[Any, Any, Any]]
    compensation: Optional[Callable[[Any], Coroutine[Any, Any, Any]]] = None
    retry_count: int = 3
    retry_delay: float = 1.0


@dataclass
class SagaInstance:
    """Saga实例状态"""
    saga_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: SagaStatus = SagaStatus.STARTED
    current_step: int = 0
    context: Dict[str, Any] = field(default_factory=dict)
    completed_steps: List[str] = field(default_factory=list)
    failed_step: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class SagaOrchestrator:
    """
    Saga编排器
    
    实现分布式事务的Saga模式，支持长事务的补偿机制
    """
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._sagas: Dict[str, List[SagaStep]] = {}
        self._instances: Dict[str, SagaInstance] = {}
        self._lock = asyncio.Lock()
        
    def register_saga(self, saga_name: str, steps: List[SagaStep]) -> None:
        """
        注册Saga流程
        
        Args:
            saga_name: Saga名称
            steps: Saga步骤列表
        """
        self._sagas[saga_name] = steps
        logger.info(f"Registered saga: {saga_name} with {len(steps)} steps")
    
    async def start_saga(self, saga_name: str, context: Dict[str, Any]) -> SagaInstance:
        """
        启动Saga实例
        
        Args:
            saga_name: Saga名称
            context: 初始上下文
            
        Returns:
            Saga实例
        """
        if saga_name not in self._sagas:
            raise ValueError(f"Saga not found: {saga_name}")
        
        instance = SagaInstance(context=context)
        
        async with self._lock:
            self._instances[instance.saga_id] = instance
        
        # 异步执行Saga
        asyncio.create_task(self._execute_saga(instance.saga_id, saga_name))
        
        logger.info(f"Started saga {saga_name} with instance {instance.saga_id}")
        return instance
    
    async def _execute_saga(self, instance_id: str, saga_name: str) -> None:
        """执行Saga"""
        instance = self._instances[instance_id]
        steps = self._sagas[saga_name]
        
        try:
            for i, step in enumerate(steps):
                instance.current_step = i
                instance.status = SagaStatus.RUNNING
                instance.updated_at = datetime.now(timezone.utc)
                
                # 执行步骤
                success = await self._execute_step(instance, step)
                
                if not success:
                    # 执行补偿
                    await self._compensate(instance, steps)
                    return
                
                instance.completed_steps.append(step.name)
            
            # 所有步骤完成
            instance.status = SagaStatus.COMPLETED
            instance.updated_at = datetime.now(timezone.utc)
            
            # 发布Saga完成事件
            await self._publish_saga_event(instance, saga_name, "completed")
            
        except Exception as e:
            logger.exception(f"Saga execution error: {e}")
            instance.status = SagaStatus.FAILED
            instance.error_message = str(e)
            await self._compensate(instance, steps)
    
    async def _execute_step(self, instance: SagaInstance, step: SagaStep) -> bool:
        """执行单个步骤"""
        for attempt in range(step.retry_count):
            try:
                result = await step.action(instance.context)
                
                # 将结果存入上下文
                if result:
                    instance.context[f"{step.name}_result"] = result
                
                logger.debug(f"Step {step.name} completed successfully")
                return True
                
            except Exception as e:
                logger.warning(f"Step {step.name} attempt {attempt + 1} failed: {e}")
                if attempt < step.retry_count - 1:
                    await asyncio.sleep(step.retry_delay * (2 ** attempt))  # 指数退避
        
        instance.failed_step = step.name
        instance.error_message = f"Step {step.name} failed after {step.retry_count} attempts"
        return False
    
    async def _compensate(self, instance: SagaInstance, steps: List[SagaStep]) -> None:
        """执行补偿"""
        instance.status = SagaStatus.COMPENSATING
        instance.updated_at = datetime.now(timezone.utc)
        
        # 逆序执行补偿
        for step_name in reversed(instance.completed_steps):
            step = next((s for s in steps if s.name == step_name), None)
            
            if step and step.compensation:
                try:
                    await step.compensation(instance.context)
                    logger.info(f"Compensation for {step.name} executed")
                except Exception as e:
                    logger.exception(f"Compensation failed for {step.name}: {e}")
        
        instance.status = SagaStatus.FAILED
        instance.updated_at = datetime.now(timezone.utc)
        
        await self._publish_saga_event(instance, "", "failed")
    
    async def _publish_saga_event(
        self,
        instance: SagaInstance,
        saga_name: str,
        event_type: str
    ) -> None:
        """发布Saga状态事件"""
        event = DomainEvent(
            event_type=f"saga.{event_type}",
            payload={
                "saga_id": instance.saga_id,
                "saga_name": saga_name,
                "status": instance.status.name,
                "completed_steps": instance.completed_steps,
                "failed_step": instance.failed_step
            }
        )
        await self.event_bus.publish(event)
    
    def get_instance(self, saga_id: str) -> Optional[SagaInstance]:
        """获取Saga实例状态"""
        return self._instances.get(saga_id)


# ============================================================================
# 死信队列处理器
# ============================================================================

@dataclass
class DLQMessage:
    """死信队列消息"""
    original_event: DomainEvent
    failed_at: datetime
    error_message: str
    retry_count: int
    handler_name: str
    dead_reason: str  # 死亡原因：max_retries, rejected, expired


class DeadLetterQueueHandler:
    """
    死信队列处理器
    
    处理无法成功处理的事件，支持重试、分析和告警
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        max_retries: int = 3,
        retry_delays: List[float] = None,
        alert_threshold: int = 10
    ):
        self.event_bus = event_bus
        self.max_retries = max_retries
        self.retry_delays = retry_delays or [1.0, 5.0, 30.0]
        self.alert_threshold = alert_threshold
        self._dlq: asyncio.Queue = asyncio.Queue()
        self._failed_events: Dict[str, DLQMessage] = {}
        self._handlers: Dict[str, EventHandler] = {}
        self._alert_handlers: List[Callable[[DLQMessage], Coroutine[Any, Any, None]]] = []
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        
    def register_handler(self, event_type: str, handler: EventHandler) -> None:
        """注册重试处理器"""
        self._handlers[event_type] = handler
    
    def register_alert_handler(
        self,
        handler: Callable[[DLQMessage], Coroutine[Any, Any, None]]
    ) -> None:
        """注册告警处理器"""
        self._alert_handlers.append(handler)
    
    async def start(self) -> None:
        """启动DLQ处理器"""
        self._running = True
        self._processor_task = asyncio.create_task(self._process_dlq())
        logger.info("DLQ handler started")
    
    async def stop(self) -> None:
        """停止DLQ处理器"""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        logger.info("DLQ handler stopped")
    
    async def enqueue(
        self,
        event: DomainEvent,
        error_message: str,
        handler_name: str,
        dead_reason: str = "max_retries"
    ) -> None:
        """将事件加入死信队列"""
        dlq_message = DLQMessage(
            original_event=event,
            failed_at=datetime.now(timezone.utc),
            error_message=error_message,
            retry_count=0,
            handler_name=handler_name,
            dead_reason=dead_reason
        )
        
        await self._dlq.put(dlq_message)
        self._failed_events[str(event.event_id)] = dlq_message
        
        logger.warning(f"Event {event.event_id} moved to DLQ: {error_message}")
        
        # 检查告警阈值
        if len(self._failed_events) >= self.alert_threshold:
            await self._trigger_alert(dlq_message)
    
    async def _process_dlq(self) -> None:
        """处理死信队列"""
        while self._running:
            try:
                message = await asyncio.wait_for(self._dlq.get(), timeout=1.0)
                await self._handle_dlq_message(message)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"DLQ processing error: {e}")
    
    async def _handle_dlq_message(self, message: DLQMessage) -> None:
        """处理单个DLQ消息"""
        event = message.original_event
        handler = self._handlers.get(event.event_type)
        
        if not handler:
            logger.error(f"No handler for DLQ event type: {event.event_type}")
            return
        
        # 尝试重试
        if message.retry_count < len(self.retry_delays):
            delay = self.retry_delays[message.retry_count]
            logger.info(f"Retrying event {event.event_id} in {delay}s (attempt {message.retry_count + 1})")
            
            await asyncio.sleep(delay)
            
            try:
                await handler(event)
                # 重试成功，从失败列表移除
                del self._failed_events[str(event.event_id)]
                logger.info(f"DLQ event {event.event_id} processed successfully")
                return
            except Exception as e:
                message.retry_count += 1
                message.error_message = str(e)
                # 重新入队
                await self._dlq.put(message)
        else:
            # 重试次数用尽，触发告警
            logger.error(f"Event {event.event_id} failed permanently after all retries")
            await self._trigger_alert(message)
    
    async def _trigger_alert(self, message: DLQMessage) -> None:
        """触发告警"""
        for handler in self._alert_handlers:
            try:
                await handler(message)
            except Exception as e:
                logger.exception(f"Alert handler error: {e}")
    
    def get_failed_events(self) -> Dict[str, DLQMessage]:
        """获取所有失败的事件"""
        return self._failed_events.copy()
    
    async def replay_event(self, event_id: str) -> bool:
        """手动重放事件"""
        message = self._failed_events.get(event_id)
        if not message:
            return False
        
        handler = self._handlers.get(message.original_event.event_type)
        if not handler:
            return False
        
        try:
            await handler(message.original_event)
            del self._failed_events[event_id]
            return True
        except Exception as e:
            logger.exception(f"Replay failed for event {event_id}: {e}")
            return False


# ============================================================================
# 事件总线工厂
# ============================================================================

class EventBusFactory:
    """事件总线工厂"""
    
    @staticmethod
    def create(backend_type: BackendType, **kwargs) -> EventBus:
        """
        创建事件总线实例
        
        Args:
            backend_type: 后端类型
            **kwargs: 后端特定配置
            
        Returns:
            事件总线实例
        """
        if backend_type == BackendType.IN_MEMORY:
            return InMemoryEventBus(**kwargs)
        elif backend_type == BackendType.KAFKA:
            return KafkaEventBus(**kwargs)
        elif backend_type == BackendType.RABBITMQ:
            return RabbitMQEventBus(**kwargs)
        else:
            raise ValueError(f"Unknown backend type: {backend_type}")


# ============================================================================
# 健康检查和监控
# ============================================================================

@dataclass
class EventBusHealth:
    """事件总线健康状态"""
    backend_type: BackendType
    connected: bool
    queue_depth: int
    handlers_count: int
    last_error: Optional[str] = None
    latency_ms: float = 0.0


class EventBusMonitor:
    """事件总线监控器"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._metrics: Dict[str, Any] = {
            "events_published": 0,
            "events_processed": 0,
            "events_failed": 0,
            "processing_latency": []
        }
        
    async def health_check(self) -> EventBusHealth:
        """执行健康检查"""
        start_time = time.time()
        
        try:
            # 发送测试事件
            test_event = DomainEvent(
                event_type="health.check",
                payload={"timestamp": time.time()}
            )
            
            connected = await self.event_bus.publish(test_event, topic="health.check")
            latency = (time.time() - start_time) * 1000
            
            return EventBusHealth(
                backend_type=self.event_bus.backend_type,
                connected=connected,
                queue_depth=0,  # 具体实现可以覆盖
                handlers_count=sum(len(h) for h in self.event_bus._handlers.values()),
                latency_ms=latency
            )
        except Exception as e:
            return EventBusHealth(
                backend_type=self.event_bus.backend_type,
                connected=False,
                queue_depth=0,
                handlers_count=0,
                last_error=str(e),
                latency_ms=(time.time() - start_time) * 1000
            )
    
    def record_publish(self) -> None:
        """记录发布事件"""
        self._metrics["events_published"] += 1
    
    def record_processed(self, latency_ms: float) -> None:
        """记录处理事件"""
        self._metrics["events_processed"] += 1
        self._metrics["processing_latency"].append(latency_ms)
        
        # 保留最近1000个延迟样本
        if len(self._metrics["processing_latency"]) > 1000:
            self._metrics["processing_latency"] = self._metrics["processing_latency"][-1000:]
    
    def record_failed(self) -> None:
        """记录失败事件"""
        self._metrics["events_failed"] += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        latencies = self._metrics["processing_latency"]
        return {
            **self._metrics,
            "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
            "p99_latency_ms": sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
        }


# ============================================================================
# 使用示例和测试
# ============================================================================

async def example_usage():
    """使用示例"""
    # 创建内存事件总线
    event_bus = EventBusFactory.create(BackendType.IN_MEMORY)
    
    # 创建监控器
    monitor = EventBusMonitor(event_bus)
    
    # 定义事件处理器
    async def handle_user_created(event: DomainEvent) -> None:
        print(f"User created: {event.payload}")
        monitor.record_processed(10.0)
    
    async def handle_order_placed(event: DomainEvent) -> None:
        print(f"Order placed: {event.payload}")
        monitor.record_processed(15.0)
    
    # 启动事件总线
    await event_bus.start()
    
    try:
        # 订阅事件
        await event_bus.subscribe("user.created", handle_user_created)
        await event_bus.subscribe("order.placed", handle_order_placed)
        
        # 发布事件
        user_event = DomainEvent(
            event_type="user.created",
            payload={"user_id": "123", "name": "John"},
            priority=EventPriority.HIGH
        )
        
        order_event = DomainEvent(
            event_type="order.placed",
            payload={"order_id": "456", "amount": 100.0}
        )
        
        await event_bus.publish(user_event)
        await event_bus.publish(order_event)
        
        # 等待事件处理
        await asyncio.sleep(1)
        
        # 健康检查
        health = await monitor.health_check()
        print(f"Health: {health}")
        
        # 创建Saga编排器
        saga_orchestrator = SagaOrchestrator(event_bus)
        
        # 定义Saga步骤
        async def reserve_inventory(context: Dict[str, Any]) -> bool:
            print("Reserving inventory...")
            return True
        
        async def release_inventory(context: Dict[str, Any]) -> None:
            print("Releasing inventory...")
        
        async def process_payment(context: Dict[str, Any]) -> bool:
            print("Processing payment...")
            return True
        
        async def refund_payment(context: Dict[str, Any]) -> None:
            print("Refunding payment...")
        
        steps = [
            SagaStep("reserve_inventory", reserve_inventory, release_inventory),
            SagaStep("process_payment", process_payment, refund_payment)
        ]
        
        saga_orchestrator.register_saga("order_processing", steps)
        
        # 启动Saga
        instance = await saga_orchestrator.start_saga(
            "order_processing",
            {"order_id": "789", "items": ["item1", "item2"]}
        )
        
        # 等待Saga完成
        await asyncio.sleep(2)
        
        # 检查Saga状态
        final_instance = saga_orchestrator.get_instance(instance.saga_id)
        print(f"Saga status: {final_instance.status}")
        
    finally:
        await event_bus.stop()


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 运行示例
    asyncio.run(example_usage())

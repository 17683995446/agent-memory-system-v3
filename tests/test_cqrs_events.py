"""CQRS和事件系统单元测试"""
import pytest
import asyncio
from memory_system.cqrs_system import (
    InMemoryEventStore, ConcurrencyException,
    CommandBus, QueryBus, StoredEvent
)
from memory_system.event_driven_core import (
    InMemoryEventBus, DomainEvent, EventPriority, EventId, AggregateId
)


class TestInMemoryEventStore:
    """内存事件存储测试"""

    @pytest.mark.asyncio
    async def test_append_and_get_events(self):
        """追加和获取事件"""
        store = InMemoryEventStore()
        events = [
            StoredEvent(
                event_id="e1", aggregate_id="agg1",
                event_type="Created", event_data={"name": "test"},
                version=1, timestamp="2024-01-01", sequence_number=1, metadata={}
            ),
            StoredEvent(
                event_id="e2", aggregate_id="agg1",
                event_type="Updated", event_data={"name": "test2"},
                version=2, timestamp="2024-01-02", sequence_number=2, metadata={}
            ),
        ]
        result = await store.append("agg1", events)
        assert result is True

        retrieved = await store.get_events("agg1")
        assert len(retrieved) == 2
        assert retrieved[0].event_type == "Created"

    @pytest.mark.asyncio
    async def test_optimistic_concurrency(self):
        """乐观并发控制"""
        store = InMemoryEventStore()
        events = [StoredEvent(
            event_id="e1", aggregate_id="agg1",
            event_type="Created", event_data={}, version=1,
            timestamp="2024-01-01", sequence_number=1, metadata={}
        )]
        await store.append("agg1", events)

        # Wrong expected version should raise
        with pytest.raises(ConcurrencyException):
            await store.append("agg1", [StoredEvent(
                event_id="e2", aggregate_id="agg1",
                event_type="Updated", event_data={}, version=2,
                timestamp="2024-01-02", sequence_number=2, metadata={}
            )], expected_version=0)


class TestInMemoryEventBus:
    """内存事件总线测试"""

    @pytest.mark.asyncio
    async def test_publish_subscribe(self):
        """发布订阅测试"""
        bus = InMemoryEventBus()
        await bus.start()

        received = []
        async def handler(event):
            received.append(event)

        await bus.subscribe("TestEvent", handler)

        event = DomainEvent(
            event_id=EventId("e1"), event_type="TestEvent",
            aggregate_id=AggregateId("agg1"), payload={"key": "value"},
            priority=EventPriority.NORMAL, version=1,
            metadata={}
        )

        result = await bus.publish(event)
        assert result is True

        await asyncio.sleep(0.2)  # Wait for async processing
        assert len(received) >= 1

        await bus.stop()

    @pytest.mark.asyncio
    async def test_same_priority_no_crash(self):
        """相同优先级事件不应导致比较崩溃"""
        bus = InMemoryEventBus()
        await bus.start()

        for i in range(10):
            event = DomainEvent(
                event_id=EventId(f"e{i}"), event_type="TestEvent",
                aggregate_id=AggregateId("agg1"), payload={},
                priority=EventPriority.NORMAL, version=1,
                metadata={}
            )
            await bus.publish(event)

        await bus.stop()  # Should not crash


class TestConfigCenter:
    """配置中心测试"""

    @pytest.mark.asyncio
    async def test_in_memory_storage(self):
        """内存配置存储测试"""
        from memory_system.config_center import ConfigCenter, InMemoryConfigStorage

        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()

        await center.set("db.host", "localhost", namespace="database")
        value = await center.get("db.host", namespace="database")
        assert value == "localhost"

        keys = await center.list_keys(namespace="database")
        assert "db.host" in keys

        await center.delete("db.host", namespace="database")
        value = await center.get("db.host", namespace="database")
        assert value is None

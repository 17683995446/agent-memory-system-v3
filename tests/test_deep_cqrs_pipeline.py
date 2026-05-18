"""CQRS/事件/流水线/配置深度测试"""
import pytest
import asyncio
from datetime import datetime, timezone
from memory_system.cqrs_system import (
    InMemoryEventStore, StoredEvent, ConcurrencyException, CommandResultStatus
)
from memory_system.event_driven_core import (
    InMemoryEventBus, DomainEvent, EventId, AggregateId, EventPriority
)
from memory_system.config_center import (
    ConfigCenter, ConfigItem, InMemoryConfigStorage, ConfigFormat, ConfigScope
)


class TestInMemoryEventStoreBoundary:
    """事件存储边界测试"""
    
    @pytest.mark.asyncio
    async def test_append_empty_events_list(self):
        """追加空事件列表"""
        store = InMemoryEventStore()
        result = await store.append("agg1", [])
        assert result is True
    
    @pytest.mark.asyncio
    async def test_get_events_nonexistent_aggregate(self):
        """获取不存在聚合的事件"""
        store = InMemoryEventStore()
        events = await store.get_events("nonexistent")
        assert events == []
    
    @pytest.mark.asyncio
    async def test_get_events_version_range(self):
        """版本范围查询"""
        store = InMemoryEventStore()
        events = [
            StoredEvent(
                event_id=f"e{i}", aggregate_id="agg1",
                event_type=f"Event{i}", event_data={"i": i},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )
            for i in range(10)
        ]
        await store.append("agg1", events)
        
        # Get events from version 5 to 8
        result = await store.get_events("agg1", from_version=5, to_version=8)
        assert len(result) == 4
        assert all(5 <= e.version <= 8 for e in result)
    
    @pytest.mark.asyncio
    async def test_get_current_version_nonexistent(self):
        """不存在聚合的当前版本应为0"""
        store = InMemoryEventStore()
        version = await store.get_current_version("nonexistent")
        assert version == 0
    
    @pytest.mark.asyncio
    async def test_get_all_events_pagination(self):
        """分页获取所有事件"""
        store = InMemoryEventStore()
        for agg_id in ["agg1", "agg2", "agg3"]:
            events = [
                StoredEvent(
                    event_id=f"e_{agg_id}_{i}", aggregate_id=agg_id,
                    event_type="Test", event_data={},
                    version=i+1, timestamp=datetime.now(timezone.utc),
                    sequence_number=i+1, metadata={}
                )
                for i in range(5)
            ]
            await store.append(agg_id, events)
        
        all_events = await store.get_all_events(after_position=0, limit=100)
        assert len(all_events) == 15
        
        first_page = await store.get_all_events(after_position=0, limit=5)
        assert len(first_page) == 5
    
    @pytest.mark.asyncio
    async def test_concurrent_appends(self):
        """并发追加应正确处理"""
        store = InMemoryEventStore()
        
        async def append_events(agg_id):
            events = [
                StoredEvent(
                    event_id=f"e_{agg_id}_{i}", aggregate_id=agg_id,
                    event_type="Test", event_data={},
                    version=i+1, timestamp=datetime.now(timezone.utc),
                    sequence_number=i+1, metadata={}
                )
                for i in range(10)
            ]
            await store.append(agg_id, events)
        
        # Append to different aggregates concurrently
        await asyncio.gather(
            append_events("agg1"),
            append_events("agg2"),
            append_events("agg3"),
        )
        
        total = await store.get_all_events(limit=1000)
        assert len(total) == 30
    
    @pytest.mark.asyncio
    async def test_optimistic_concurrency_wrong_version(self):
        """乐观并发: 错误版本号"""
        store = InMemoryEventStore()
        events = [StoredEvent(
            event_id="e1", aggregate_id="agg1",
            event_type="Created", event_data={},
            version=1, timestamp=datetime.now(timezone.utc),
            sequence_number=1, metadata={}
        )]
        await store.append("agg1", events)
        
        with pytest.raises(ConcurrencyException):
            await store.append("agg1", [StoredEvent(
                event_id="e2", aggregate_id="agg1",
                event_type="Updated", event_data={},
                version=2, timestamp=datetime.now(timezone.utc),
                sequence_number=2, metadata={}
            )], expected_version=0)  # Wrong: should be 1


class TestInMemoryEventBusBoundary:
    """事件总线边界测试"""
    
    @pytest.mark.asyncio
    async def test_publish_without_start(self):
        """未启动时发布应不崩溃"""
        bus = InMemoryEventBus()
        event = DomainEvent(event_type="Test")
        # Should not crash - may return False or True
        result = await bus.publish(event)
        # Just verify no exception
    
    @pytest.mark.asyncio
    async def test_subscribe_no_handler(self):
        """无处理器时发布应不崩溃"""
        bus = InMemoryEventBus()
        await bus.start()
        event = DomainEvent(event_type="NoHandler")
        result = await bus.publish(event)
        await asyncio.sleep(0.1)
        await bus.stop()
    
    @pytest.mark.asyncio
    async def test_unsubscribe_handler(self):
        """取消订阅后不应收到事件"""
        bus = InMemoryEventBus()
        await bus.start()
        
        received = []
        async def handler(event):
            received.append(event)
        
        await bus.subscribe("Test", handler)
        await bus.unsubscribe("Test", handler)
        
        await bus.publish(DomainEvent(event_type="Test"))
        await asyncio.sleep(0.1)
        assert len(received) == 0
        
        await bus.stop()
    
    @pytest.mark.asyncio
    async def test_multiple_subscribers(self):
        """多个订阅者都应收到事件"""
        bus = InMemoryEventBus()
        await bus.start()
        
        received1, received2 = [], []
        await bus.subscribe("Test", lambda e: received1.append(e))
        await bus.subscribe("Test", lambda e: received2.append(e))
        
        await bus.publish(DomainEvent(event_type="Test"))
        await asyncio.sleep(0.2)
        
        assert len(received1) >= 1
        assert len(received2) >= 1
        
        await bus.stop()
    
    @pytest.mark.asyncio
    async def test_event_serialization_roundtrip(self):
        """事件序列化往返"""
        event = DomainEvent(
            event_type="TestEvent",
            payload={"key": "value", "number": 42},
            metadata={"source": "test"},
            priority=EventPriority.HIGH
        )
        
        # dict roundtrip
        d = event.to_dict()
        restored = DomainEvent.from_dict(d)
        assert restored.event_type == "TestEvent"
        assert restored.payload == {"key": "value", "number": 42}
        
        # json roundtrip
        j = event.to_json()
        restored2 = DomainEvent.from_json(j)
        assert restored2.event_type == "TestEvent"
    
    @pytest.mark.asyncio
    async def test_high_volume_events(self):
        """高吞吐量事件发布"""
        bus = InMemoryEventBus(max_queue_size=10000)
        await bus.start()
        
        received = []
        await bus.subscribe("Test", lambda e: received.append(e))
        
        for i in range(500):
            await bus.publish(DomainEvent(event_type="Test", payload={"i": i}))
        
        await asyncio.sleep(1.0)
        # Not all may be processed, but should not crash
        await bus.stop()


class TestConfigCenterBoundary:
    """配置中心边界测试"""
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self):
        """获取不存在的配置"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        result = await center.get("nonexistent_key")
        assert result is None
        
        result = await center.get("nonexistent", default="fallback")
        assert result == "fallback"
    
    @pytest.mark.asyncio
    async def test_set_and_get_various_types(self):
        """设置和获取各种类型的值"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        test_values = {
            "string_val": "hello",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": True,
            "null_val": None,
            "list_val": [1, 2, 3],
            "dict_val": {"nested": {"key": "value"}},
        }
        
        for key, value in test_values.items():
            await center.set(key, value)
            retrieved = await center.get(key)
            assert retrieved == value, f"Mismatch for {key}: {retrieved} != {value}"
    
    @pytest.mark.asyncio
    async def test_delete_nonexistent_key(self):
        """删除不存在的键应返回False"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        result = await center.delete("nonexistent")
        assert result is False
    
    @pytest.mark.asyncio
    async def test_namespace_isolation(self):
        """命名空间隔离"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        await center.set("key", "value1", namespace="ns1")
        await center.set("key", "value2", namespace="ns2")
        
        assert await center.get("key", namespace="ns1") == "value1"
        assert await center.get("key", namespace="ns2") == "value2"
    
    @pytest.mark.asyncio
    async def test_list_keys_empty_namespace(self):
        """空命名空间的键列表"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        keys = await center.list_keys(namespace="empty_ns")
        assert keys == []
    
    @pytest.mark.asyncio
    async def test_get_all_namespace(self):
        """获取命名空间所有配置"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        await center.set("k1", "v1", namespace="test")
        await center.set("k2", "v2", namespace="test")
        
        all_config = await center.get_all(namespace="test")
        assert all_config["k1"] == "v1"
        assert all_config["k2"] == "v2"
    
    @pytest.mark.asyncio
    async def test_create_namespace(self):
        """创建命名空间"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        ns = await center.create_namespace("production", description="Production config")
        assert ns is not None
        
        ns_retrieved = center.get_namespace("production")
        assert ns_retrieved is not None
    
    @pytest.mark.asyncio
    async def test_list_namespaces(self):
        """列出命名空间"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        await center.create_namespace("ns1")
        await center.create_namespace("ns2")
        
        namespaces = center.list_namespaces()
        assert len(namespaces) >= 2

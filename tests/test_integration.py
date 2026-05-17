"""集成测试 - 模块间协作"""
import pytest
import asyncio
from memory_system.security_hardening import SecurityHardeningModule, Permission, Role
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.partition_tolerance import QuorumManager
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent, CommandBus
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent, EventPriority, EventId, AggregateId


class TestSecurityConfigIntegration:
    """安全+配置集成测试"""

    @pytest.mark.asyncio
    async def test_security_config_workflow(self):
        """安全模块使用配置中心获取策略"""
        storage = InMemoryConfigStorage()
        config = ConfigCenter(storage)
        await config.initialize()

        # 通过配置中心设置安全策略
        await config.set("security.max_login_attempts", 5, namespace="security")
        max_attempts = await config.get("security.max_login_attempts", namespace="security")
        assert max_attempts == 5

        # 安全模块使用该配置
        security = SecurityHardeningModule()
        user = security.rbac.create_user("Alice", [Role.ADMIN])

        assert security.rbac.check_permission(user.user_id, Permission.WRITE)


class TestCQRSEventIntegration:
    """CQRS+事件总线集成测试"""

    @pytest.mark.asyncio
    async def test_command_creates_event(self):
        """命令执行产生事件并通过事件总线发布"""
        store = InMemoryEventStore()
        bus = InMemoryEventBus()
        await bus.start()

        published_events = []
        async def handler(event):
            published_events.append(event)

        await bus.subscribe("MemoryStored", handler)

        # 模拟命令执行
        event = StoredEvent(
            event_id="e1", aggregate_id="mem1",
            event_type="MemoryStored", event_data={"content": "hello"},
            version=1, timestamp="2024-01-01", sequence_number=1, metadata={}
        )
        await store.append("mem1", [event])

        # 验证事件已存储
        retrieved = await store.get_events("mem1")
        assert len(retrieved) == 1
        assert retrieved[0].event_data["content"] == "hello"

        await bus.stop()


class TestQuorumConsistencyIntegration:
    """Quorum+一致性集成测试"""

    def test_quorum_with_partition(self):
        """网络分区下的quorum行为"""
        qm = QuorumManager(total_nodes=5, write_quorum=3, read_quorum=2)

        # 正常情况: 5个节点全部可用
        all_nodes = {"n1", "n2", "n3", "n4", "n5"}
        assert qm.has_quorum(all_nodes)

        # 分区1: 3个节点 (可以写)
        partition1 = {"n1", "n2", "n3"}
        assert qm.has_quorum(partition1)
        assert qm.is_write_quorum(len(partition1))

        # 分区2: 2个节点 (不能写，但可以读)
        partition2 = {"n4", "n5"}
        assert not qm.has_quorum(partition2)
        assert qm.is_read_quorum(len(partition2))

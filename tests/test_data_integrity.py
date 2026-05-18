"""数据完整性测试 - 验证复杂操作序列后数据一致性"""
import pytest
import asyncio
from datetime import datetime, timezone
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent, ConcurrencyException
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.security_hardening import RBACManager, AuditLogger, Permission, Role
from memory_system.vector_clock import VectorClock
from memory_system.partition_tolerance import QuorumManager
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent


class TestEventDataIntegrity:
    """事件数据完整性"""

    @pytest.mark.asyncio
    async def test_event_version_sequence_after_append(self):
        """追加后事件版本和序列号应连续"""
        store = InMemoryEventStore()
        events = [
            StoredEvent(
                event_id=f"e{i}", aggregate_id="agg1",
                event_type="Update", event_data={"step": i},
                version=i+1, timestamp=datetime.now(timezone.utc),
                sequence_number=i+1, metadata={}
            )
            for i in range(100)
        ]
        await store.append("agg1", events)

        retrieved = await store.get_events("agg1")
        for i, event in enumerate(retrieved):
            assert event.version == i + 1, f"Version mismatch at {i}: {event.version} != {i+1}"
            assert event.sequence_number == i + 1

    @pytest.mark.asyncio
    async def test_overwrite_protection(self):
        """覆盖保护 - 同一版本不应被覆盖"""
        store = InMemoryEventStore()
        e1 = StoredEvent(
            event_id="e1", aggregate_id="agg1",
            event_type="Created", event_data={"name": "original"},
            version=1, timestamp=datetime.now(timezone.utc),
            sequence_number=1, metadata={}
        )
        await store.append("agg1", [e1])

        # Try to append with wrong expected version
        with pytest.raises(ConcurrencyException):
            await store.append("agg1", [StoredEvent(
                event_id="e2", aggregate_id="agg1",
                event_type="Updated", event_data={"name": "modified"},
                version=2, timestamp=datetime.now(timezone.utc),
                sequence_number=2, metadata={}
            )], expected_version=0)

        # Original data should be unchanged
        events = await store.get_events("agg1")
        assert events[0].event_data["name"] == "original"

    @pytest.mark.asyncio
    async def test_multi_aggregate_isolation(self):
        """多聚合数据隔离"""
        store = InMemoryEventStore()

        for agg_id in ["agg1", "agg2", "agg3"]:
            for i in range(10):
                await store.append(agg_id, [StoredEvent(
                    event_id=f"e_{agg_id}_{i}", aggregate_id=agg_id,
                    event_type="Update", event_data={"agg": agg_id, "step": i},
                    version=i+1, timestamp=datetime.now(timezone.utc),
                    sequence_number=i+1, metadata={}
                )])

        # Each aggregate should have exactly 10 events
        for agg_id in ["agg1", "agg2", "agg3"]:
            events = await store.get_events(agg_id)
            assert len(events) == 10
            for e in events:
                assert e.aggregate_id == agg_id
                assert e.event_data["agg"] == agg_id


class TestConfigDataIntegrity:
    """配置数据完整性"""

    @pytest.mark.asyncio
    async def test_overwrite_preserves_history(self):
        """覆盖配置应保留历史"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()

        await center.set("key", "v1", namespace="test")
        # Create version snapshot after first set
        await center.create_version("test", comment="v1 snapshot")

        await center.set("key", "v2", namespace="test")
        await center.create_version("test", comment="v2 snapshot")

        await center.set("key", "v3", namespace="test")
        await center.create_version("test", comment="v3 snapshot")

        # Current value should be v3
        assert await center.get("key", namespace="test") == "v3"

        # Version history should exist (explicit snapshots)
        versions = center.get_versions("test")
        assert len(versions) >= 1

    @pytest.mark.asyncio
    async def test_delete_get_returns_none(self):
        """删除后获取应返回None"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()

        await center.set("key", "value", namespace="test")
        assert await center.get("key", namespace="test") == "value"

        await center.delete("key", namespace="test")
        assert await center.get("key", namespace="test") is None

    @pytest.mark.asyncio
    async def test_namespace_data_isolation(self):
        """命名空间数据隔离"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()

        # Set same keys in different namespaces
        for ns in ["dev", "staging", "prod"]:
            await center.set("db.host", f"{ns}.db.example.com", namespace=ns)
            await center.set("db.port", 5432 + hash(ns) % 1000, namespace=ns)

        # Each namespace should have its own values
        for ns in ["dev", "staging", "prod"]:
            host = await center.get("db.host", namespace=ns)
            assert host == f"{ns}.db.example.com"


class TestVectorClockConsistency:
    """向量时钟一致性"""

    def test_merge_is_commutative(self):
        """合并操作应满足交换律"""
        vc1 = VectorClock(node_id="n1")
        vc2 = VectorClock(node_id="n2")
        vc1.increment()
        vc2.increment()
        vc2.increment()

        merge_1_2 = vc1.merge(vc2)
        merge_2_1 = vc2.merge(vc1)

        assert merge_1_2.clock == merge_2_1.clock

    def test_merge_is_associative(self):
        """合并操作应满足结合律"""
        vc1 = VectorClock(node_id="n1")
        vc2 = VectorClock(node_id="n2")
        vc3 = VectorClock(node_id="n3")
        vc1.increment()
        vc2.increment()
        vc2.increment()
        vc3.increment()
        vc3.increment()
        vc3.increment()

        # (vc1.merge(vc2)).merge(vc3) == vc1.merge(vc2.merge(vc3))
        left = vc1.merge(vc2).merge(vc3)
        right = vc2.merge(vc3)
        right = vc1.merge(right)
        assert left.clock == right.clock

    def test_increment_monotonic(self):
        """递增应单调"""
        vc = VectorClock(node_id="n1")
        prev = vc.get_timestamp()
        for _ in range(1000):
            vc.increment()
            curr = vc.get_timestamp()
            assert curr > prev
            prev = curr


class TestRBACDataIntegrity:
    """RBAC数据完整性"""

    def test_role_assignment_persists(self):
        """角色分配应持久化"""
        rbac = RBACManager()
        user = rbac.create_user("alice", [Role.USER])

        rbac.assign_role(user.user_id, Role.ADMIN)
        # Re-fetch user
        user = rbac.get_user(user.user_id)
        assert Role.ADMIN in user.roles
        assert Role.USER in user.roles

    def test_permission_check_after_complex_operations(self):
        """复杂操作后权限检查应正确"""
        rbac = RBACManager()
        user = rbac.create_user("alice", [Role.USER])

        # Assign admin
        rbac.assign_role(user.user_id, Role.ADMIN)
        assert rbac.check_permission(user.user_id, Permission.ADMIN)

        # Revoke admin
        rbac.revoke_role(user.user_id, Role.ADMIN)
        assert not rbac.check_permission(user.user_id, Permission.ADMIN)

        # Should still have USER permissions
        assert rbac.check_permission(user.user_id, Permission.READ)

    def test_resource_permission_isolation(self):
        """资源权限隔离"""
        rbac = RBACManager()
        user = rbac.create_user("alice", [Role.USER])

        rbac.set_resource_permission("doc1", Role.USER, [Permission.READ])
        rbac.set_resource_permission("doc2", Role.USER, [Permission.WRITE])

        assert rbac.check_permission(user.user_id, Permission.READ, resource="doc1")
        assert not rbac.check_permission(user.user_id, Permission.WRITE, resource="doc1")
        assert rbac.check_permission(user.user_id, Permission.WRITE, resource="doc2")
        assert not rbac.check_permission(user.user_id, Permission.READ, resource="doc2")

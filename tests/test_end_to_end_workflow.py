"""端到端工作流测试 - 完整用户场景链"""
import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent, CommandBus, QueryBus
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.security_hardening import CryptoManager, RBACManager, AuditLogger, Permission, Role
from memory_system.monitoring_alerting import MetricsCollector, AlertManager, AlertRule, AlertSeverity
from memory_system.partition_tolerance import QuorumManager
from memory_system.vector_clock import VectorClock
from memory_system.hybrid_logical_clock import HybridLogicalClock


class TestCompleteUserJourney:
    """完整用户旅程测试"""
    
    @pytest.mark.asyncio
    async def test_user_registration_to_deletion(self):
        """用户注册到删除的完整流程"""
        # Setup
        rbac = RBACManager()
        audit = AuditLogger()
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        store = InMemoryEventStore()
        bus = InMemoryEventBus()
        await bus.start()
        
        events_log = []
        async def track_event(e):
            events_log.append(e)
        await bus.subscribe("UserEvent", track_event)
        
        # Step 1: User registration
        user = rbac.create_user("alice", [Role.USER])
        audit.log(user.user_id, "register", "user", "success", details={"username": "alice"})
        await store.append(user.user_id, [StoredEvent(
            event_id="e1", aggregate_id=user.user_id,
            event_type="UserRegistered", event_data={"username": "alice"},
            version=1, timestamp=datetime.now(timezone.utc),
            sequence_number=1, metadata={}
        )])
        await bus.publish(DomainEvent(event_type="UserEvent", payload={"action": "registered", "user_id": user.user_id}))
        
        # Step 2: Password setup
        password_hash = crypto.hash_password_bcrypt("secure_password_123")
        await store.append(user.user_id, [StoredEvent(
            event_id="e2", aggregate_id=user.user_id,
            event_type="PasswordSet", event_data={"hash": password_hash[:20]+"..."},
            version=2, timestamp=datetime.now(timezone.utc),
            sequence_number=2, metadata={}
        )])
        
        # Step 3: Role upgrade
        rbac.assign_role(user.user_id, Role.ADMIN)
        audit.log(user.user_id, "role_upgrade", "user", "success", details={"new_role": "admin"})
        await store.append(user.user_id, [StoredEvent(
            event_id="e3", aggregate_id=user.user_id,
            event_type="RoleUpgraded", event_data={"role": "admin"},
            version=3, timestamp=datetime.now(timezone.utc),
            sequence_number=3, metadata={}
        )])
        
        # Step 4: Permission check
        assert rbac.check_permission(user.user_id, Permission.ADMIN)
        assert rbac.check_permission(user.user_id, Permission.WRITE)
        
        # Step 5: User deletion
        rbac.revoke_role(user.user_id, Role.ADMIN)
        rbac.revoke_role(user.user_id, Role.USER)
        audit.log(user.user_id, "delete", "user", "success")
        await store.append(user.user_id, [StoredEvent(
            event_id="e4", aggregate_id=user.user_id,
            event_type="UserDeleted", event_data={},
            version=4, timestamp=datetime.now(timezone.utc),
            sequence_number=4, metadata={}
        )])
        
        await asyncio.sleep(0.2)
        await bus.stop()
        
        # Verify complete journey
        events = await store.get_events(user.user_id)
        assert len(events) == 4
        assert events[0].event_type == "UserRegistered"
        assert events[3].event_type == "UserDeleted"
        
        audit_entries = audit.query(user_id=user.user_id)
        assert len(audit_entries) == 3  # register, role_upgrade, delete
    
    @pytest.mark.asyncio
    async def test_document_lifecycle(self):
        """文档完整生命周期"""
        store = InMemoryEventStore()
        bus = InMemoryEventBus()
        await bus.start()
        rbac = RBACManager()
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        
        # Create author
        author = rbac.create_user("author", [Role.USER])
        rbac.assign_role(author.user_id, Role.ADMIN)
        
        doc_id = "doc_001"
        
        # Create document
        await store.append(doc_id, [StoredEvent(
            event_id="e1", aggregate_id=doc_id,
            event_type="DocumentCreated", 
            event_data={"title": "Test Doc", "author": author.user_id},
            version=1, timestamp=datetime.now(timezone.utc),
            sequence_number=1, metadata={}
        )])
        
        # Edit document multiple times
        for i in range(5):
            content = f"Version {i+1} content"
            encrypted = crypto.encrypt(content)
            await store.append(doc_id, [StoredEvent(
                event_id=f"e{i+2}", aggregate_id=doc_id,
                event_type="DocumentEdited",
                event_data={"content_encrypted": encrypted[:50], "version": i+2},
                version=i+2, timestamp=datetime.now(timezone.utc),
                sequence_number=i+2, metadata={"editor": author.user_id}
            )])
        
        # Publish document
        await store.append(doc_id, [StoredEvent(
            event_id="e7", aggregate_id=doc_id,
            event_type="DocumentPublished",
            event_data={"published_at": datetime.now(timezone.utc).isoformat()},
            version=7, timestamp=datetime.now(timezone.utc),
            sequence_number=7, metadata={}
        )])
        
        # Archive document
        await store.append(doc_id, [StoredEvent(
            event_id="e8", aggregate_id=doc_id,
            event_type="DocumentArchived",
            event_data={},
            version=8, timestamp=datetime.now(timezone.utc),
            sequence_number=8, metadata={}
        )])
        
        await bus.stop()
        
        # Verify lifecycle
        events = await store.get_events(doc_id)
        assert len(events) == 8
        assert events[0].event_type == "DocumentCreated"
        assert events[6].event_type == "DocumentPublished"
        assert events[7].event_type == "DocumentArchived"
        
        version = await store.get_current_version(doc_id)
        assert version == 8


class TestMultiUserCollaboration:
    """多用户协作测试"""
    
    @pytest.mark.asyncio
    async def test_concurrent_document_editing(self):
        """并发文档编辑"""
        store = InMemoryEventStore()
        bus = InMemoryEventBus()
        await bus.start()
        
        doc_id = "shared_doc"
        users = ["user_a", "user_b", "user_c"]
        
        # Initial document
        await store.append(doc_id, [StoredEvent(
            event_id="e0", aggregate_id=doc_id,
            event_type="DocumentCreated",
            event_data={"title": "Shared Document"},
            version=1, timestamp=datetime.now(timezone.utc),
            sequence_number=1, metadata={}
        )])
        
        # Concurrent edits from multiple users
        async def user_edit(user_id, edit_count):
            for i in range(edit_count):
                await store.append(doc_id, [StoredEvent(
                    event_id=f"e_{user_id}_{i}", aggregate_id=doc_id,
                    event_type="DocumentEdited",
                    event_data={"editor": user_id, "change": f"edit_{i}"},
                    version=2 + i,  # Simplified versioning
                    timestamp=datetime.now(timezone.utc),
                    sequence_number=2 + i,
                    metadata={"editor": user_id}
                )])
                await asyncio.sleep(0.01)
        
        # Each user makes 3 edits concurrently
        await asyncio.gather(
            user_edit("user_a", 3),
            user_edit("user_b", 3),
            user_edit("user_c", 3),
        )
        
        await bus.stop()
        
        # All edits should be recorded
        events = await store.get_events(doc_id)
        edit_events = [e for e in events if e.event_type == "DocumentEdited"]
        assert len(edit_events) == 9


class TestDistributedTransaction:
    """分布式事务测试"""
    
    @pytest.mark.asyncio
    async def test_two_phase_commit_simulation(self):
        """两阶段提交模拟"""
        store = InMemoryEventStore()
        
        # Simulate 2PC for money transfer
        tx_id = "tx_transfer_001"
        
        # Phase 1: Prepare
        await store.append(f"tx_{tx_id}", [StoredEvent(
            event_id="e1", aggregate_id=f"tx_{tx_id}",
            event_type="TransactionInitiated",
            event_data={"from": "account_a", "to": "account_b", "amount": 100},
            version=1, timestamp=datetime.now(timezone.utc),
            sequence_number=1, metadata={}
        )])
        
        # Participant prepare
        await store.append(f"tx_{tx_id}", [StoredEvent(
            event_id="e2", aggregate_id=f"tx_{tx_id}",
            event_type="ParticipantPrepared",
            event_data={"participant": "account_a", "status": "prepared"},
            version=2, timestamp=datetime.now(timezone.utc),
            sequence_number=2, metadata={}
        )])
        
        await store.append(f"tx_{tx_id}", [StoredEvent(
            event_id="e3", aggregate_id=f"tx_{tx_id}",
            event_type="ParticipantPrepared",
            event_data={"participant": "account_b", "status": "prepared"},
            version=3, timestamp=datetime.now(timezone.utc),
            sequence_number=3, metadata={}
        )])
        
        # Phase 2: Commit
        await store.append(f"tx_{tx_id}", [StoredEvent(
            event_id="e4", aggregate_id=f"tx_{tx_id}",
            event_type="TransactionCommitted",
            event_data={"status": "committed"},
            version=4, timestamp=datetime.now(timezone.utc),
            sequence_number=4, metadata={}
        )])
        
        # Verify
        events = await store.get_events(f"tx_{tx_id}")
        assert len(events) == 4
        assert events[-1].event_type == "TransactionCommitted"


class TestMonitoringIntegration:
    """监控集成测试"""
    
    @pytest.mark.asyncio
    async def test_full_monitoring_pipeline(self):
        """完整监控管道"""
        mc = MetricsCollector()
        am = AlertManager()
        audit = AuditLogger()
        
        # Add alert rule
        am.add_rule(AlertRule(
            rule_id="high_error_rate",
            name="HighErrorRate",
            expr="error_rate > 0.1",
            duration=timedelta(seconds=60),
            severity=AlertSeverity.WARNING,
            summary="Error rate is too high",
            description="Alert when error rate exceeds 10%"
        ))
        
        # Simulate traffic
        for i in range(1000):
            status = "200" if i % 10 != 0 else "500"
            mc.counter("http_requests", 1.0, labels={"status": status, "endpoint": "/api"})
            mc.histogram("latency", 0.1 + (i % 100) * 0.01)
            
            if status == "500":
                audit.log("system", "error", "/api", "failure", details={"status": 500})
        
        # Verify metrics
        metrics = mc.get_metrics("http_requests")
        assert len(metrics) == 1000
        
        # Verify audit
        error_entries = audit.query(action="error")
        assert len(error_entries) == 100  # 10% error rate


class TestConfigCenterWorkflow:
    """配置中心工作流测试"""
    
    @pytest.mark.asyncio
    async def test_config_lifecycle(self):
        """配置完整生命周期"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        # Create namespace
        await center.create_namespace("production", description="Production environment")
        
        # Set configurations
        await center.set("database.host", "db.example.com", namespace="production")
        await center.set("database.port", 5432, namespace="production")
        await center.set("cache.enabled", True, namespace="production")
        
        # Read configurations
        host = await center.get("database.host", namespace="production")
        port = await center.get("database.port", namespace="production")
        enabled = await center.get("cache.enabled", namespace="production")
        
        assert host == "db.example.com"
        assert port == 5432
        assert enabled is True
        
        # Update configuration
        await center.set("database.host", "new-db.example.com", namespace="production")
        new_host = await center.get("database.host", namespace="production")
        assert new_host == "new-db.example.com"
        
        # Create version snapshot
        version = await center.create_version("production", comment="v1.0 snapshot")
        assert version is not None
        
        # Delete configuration
        await center.delete("cache.enabled", namespace="production")
        deleted_value = await center.get("cache.enabled", namespace="production")
        assert deleted_value is None


class TestVectorClockWorkflow:
    """向量时钟工作流测试"""
    
    def test_distributed_event_ordering(self):
        """分布式事件排序"""
        # Create vector clocks for 3 nodes
        vc1 = VectorClock("node1")
        vc2 = VectorClock("node2")
        vc3 = VectorClock("node3")
        
        # Node1 creates event A
        clock_a = vc1.increment()
        
        # Node2 receives event A and creates event B
        vc2.update_from_dict(clock_a)
        clock_b = vc2.increment()
        
        # Node3 receives event B and creates event C
        vc3.update_from_dict(clock_b)
        clock_c = vc3.increment()
        
        # Verify happens-before relationships
        from memory_system.vector_clock import VectorClockRelation
        
        # A happens-before B
        vc_a = VectorClock("node1", clock_a)
        vc_b = VectorClock("node2", clock_b)
        assert vc_a.compare(vc_b) == VectorClockRelation.BEFORE
        
        # B happens-before C
        vc_c = VectorClock("node3", clock_c)
        assert vc_b.compare(vc_c) == VectorClockRelation.BEFORE


class TestHybridLogicalClockWorkflow:
    """混合逻辑时钟工作流测试"""
    
    def test_hlc_time_ordering(self):
        """HLC时间排序"""
        hlc1 = HybridLogicalClock("node1")
        hlc2 = HybridLogicalClock("node2")
        
        # Node1 generates timestamp
        ts1 = hlc1.now()
        
        # Node2 receives and updates
        ts2 = hlc2.update(ts1)
        
        # Node2 generates new timestamp
        ts3 = hlc2.now()
        
        # Verify ordering
        assert HybridLogicalClock.less_than(ts1, ts2)
        assert HybridLogicalClock.less_than(ts2, ts3)


class TestQuorumWorkflow:
    """仲裁工作流测试"""
    
    def test_quorum_operations(self):
        """仲裁操作"""
        # 5 nodes cluster
        qm = QuorumManager(total_nodes=5)
        
        # Write quorum is 3 (majority)
        assert qm.write_quorum == 3
        
        # Read quorum is 2
        assert qm.read_quorum == 2
        
        # Check if we can serve requests
        assert qm.can_serve_writes(3) is True
        assert qm.can_serve_writes(2) is False
        
        assert qm.can_serve_reads(2) is True
        assert qm.can_serve_reads(1) is False
        
        # Check quorum status
        status = qm.get_quorum_status(3)
        assert status["can_read"] is True
        assert status["can_write"] is True

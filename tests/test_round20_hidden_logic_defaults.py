"""
第20轮全面测试 - 隐藏逻辑/默认行为/异常兜底测试

测试策略:
- 抓核心：不漏任何判断分支、隐藏逻辑、默认行为、异常兜底
- 第一性本质：代码即规则，顺着代码本源做全覆盖验证

测试覆盖:
- 隐藏逻辑：内部状态转换、隐式行为
- 默认行为：未指定参数时的默认值和行为
- 异常兜底：所有异常路径和错误恢复
- 边界条件：极端输入和状态
"""

import pytest
import asyncio
import gzip
import json
import pickle
import zlib
import time
import threading
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from dataclasses import dataclass
from typing import Dict, Any, Optional

# 导入被测模块
from memory_system.snapshot_serializer import (
    JSONSerializer, MsgPackSerializer, PickleSerializer,
    SerializerFactory, SnapshotSerializerManager, SerializedData
)
from memory_system.event_driven_core import (
    InMemoryEventBus, DomainEvent, EventId, AggregateId,
    EventPriority, EventStatus, SagaStatus, SagaStep, SagaInstance,
    SagaOrchestrator, DeadLetterQueueHandler, DLQMessage,
    InMemoryEventStore, EventBusFactory, EventBusMonitor,
    BackendType
)
from memory_system.distributed_transaction_manager import (
    Transaction, TransactionStatus, TransactionContext,
    Participant, ParticipantStatus,
    TwoPhaseCommitCoordinator, SagaOrchestrator as TxSagaOrchestrator,
    DistributedTransactionManager, InMemoryTransactionStore,
    TransactionError, TransactionNotFoundError
)
from memory_system.partition_tolerance import (
    PartitionManager, QuorumManager, PartitionTolerantStorage,
    SplitBrainResolver, PartitionState, NodeState, NodeHealth
)
from memory_system.security_hardening import (
    InputValidator, CryptoManager, CSRFProtector,
    RBACManager, ABACManager, AuditLogger, VulnerabilityScanner,
    SecurityHardeningModule, Permission, Role, User
)
from memory_system.exceptions import (
    SerializationError, StorageError, MemorySystemError
)


# ============================================================================
# 第一部分：隐藏逻辑测试
# ============================================================================

class TestHiddenLogic:
    """测试隐藏逻辑"""

    def test_json_serializer_default_indent(self):
        """隐藏逻辑：默认indent=None"""
        serializer = JSONSerializer()
        data = {"key": "value"}
        result = serializer.serialize(data)
        # 没有indent时，应该是紧凑格式
        assert b'\n' not in result  # 没有换行

    def test_json_serializer_custom_indent(self):
        """隐藏逻辑：自定义indent"""
        serializer = JSONSerializer(indent=2)
        data = {"key": "value"}
        result = serializer.serialize(data)
        # 有indent时，应该有换行
        assert b'\n' in result

    def test_snapshot_serializer_manager_default_cache(self):
        """隐藏逻辑：SnapshotSerializerManager内部缓存"""
        manager = SnapshotSerializerManager()
        # 内部应该有缓存字典
        assert hasattr(manager, '_cache')
        assert isinstance(manager._cache, dict)

    def test_serialized_data_default_metadata(self):
        """隐藏逻辑：SerializedData默认metadata为None"""
        data = SerializedData(
            data=b"test",
            serializer_type="json",
            compressed=False,
            timestamp=time.time()
        )
        assert data.metadata is None

        # to_dict应该处理None metadata
        result = data.to_dict()
        assert result["metadata"] == {}

    def test_event_id_default_generation(self):
        """隐藏逻辑：EventId自动生成UUID"""
        event_id = EventId()
        assert event_id.value is not None
        assert len(event_id.value) == 36  # UUID格式

    def test_aggregate_id_default_generation(self):
        """隐藏逻辑：AggregateId自动生成UUID"""
        agg_id = AggregateId()
        assert agg_id.value is not None
        assert len(agg_id.value) == 36

    def test_domain_event_default_values(self):
        """隐藏逻辑：DomainEvent默认值"""
        event = DomainEvent()
        assert event.event_type == "base_event"
        assert event.version == 1
        assert event.priority == EventPriority.NORMAL
        assert event.correlation_id is None
        assert event.causation_id is None
        assert event.metadata == {}
        assert event.payload == {}

    def test_transaction_context_default_data(self):
        """隐藏逻辑：TransactionContext默认空字典"""
        context = TransactionContext()
        assert context.data == {}
        assert context.get("nonexistent") is None
        assert context.get("nonexistent", "default") == "default"

    def test_participant_default_status(self):
        """隐藏逻辑：Participant默认状态"""
        participant = Participant(
            participant_id="p1",
            resource_id="r1"
        )
        assert participant.status == ParticipantStatus.ACTIVE
        assert participant.prepare_handler is None
        assert participant.commit_handler is None
        assert participant.abort_handler is None

    def test_node_health_default_values(self):
        """隐藏逻辑：NodeHealth默认值"""
        health = NodeHealth(node_id="node1")
        assert health.state == NodeState.HEALTHY
        assert health.heartbeat_count == 0
        assert health.missed_heartbeats == 0
        assert health.latency_ms == 0.0

    def test_rbac_default_role_permissions(self):
        """隐藏逻辑：RBAC默认角色权限映射"""
        rbac = RBACManager()
        # GUEST只有READ权限
        assert Permission.READ in rbac._role_permissions[Role.GUEST]
        assert Permission.WRITE not in rbac._role_permissions[Role.GUEST]

        # ADMIN有所有权限
        assert Permission.ADMIN in rbac._role_permissions[Role.ADMIN]
        assert Permission.DELETE in rbac._role_permissions[Role.ADMIN]

    def test_abac_default_deny(self):
        """隐藏逻辑：ABAC默认拒绝"""
        abac = ABACManager()
        subject = {"user_id": "user1"}
        resource = {"id": "res1"}
        allowed, policy = abac.evaluate(subject, resource, "read")
        assert allowed == False
        assert policy is None


class TestInternalStateTransitions:
    """测试内部状态转换"""

    @pytest.mark.asyncio
    async def test_event_bus_running_state(self):
        """隐藏逻辑：EventBus运行状态转换"""
        bus = InMemoryEventBus()
        assert bus._running == False

        await bus.start()
        assert bus._running == True

        await bus.stop()
        assert bus._running == False

    @pytest.mark.asyncio
    async def test_saga_instance_state_transitions(self):
        """隐藏逻辑：Saga实例状态转换"""
        bus = InMemoryEventBus()
        await bus.connect()

        try:
            orchestrator = SagaOrchestrator(bus)

            async def action(context):
                return "result"

            async def compensate(context):
                pass

            steps = [SagaStep("step1", action, compensate, retry_count=1)]
            orchestrator.register_saga("test_saga", steps)

            instance = await orchestrator.start_saga("test_saga", {})
            # 初始状态
            assert instance.status in [SagaStatus.STARTED, SagaStatus.RUNNING, SagaStatus.COMPLETED]

            await asyncio.sleep(1)
            # 最终状态
            assert instance.status == SagaStatus.COMPLETED
        finally:
            await bus.disconnect()

    def test_node_health_state_transitions(self):
        """隐藏逻辑：节点健康状态转换"""
        health = NodeHealth(node_id="node1")

        # HEALTHY -> SUSPECTED
        health.record_missed_heartbeat()
        health.record_missed_heartbeat()
        health.record_missed_heartbeat()
        assert health.state == NodeState.SUSPECTED

        # SUSPECTED -> UNREACHABLE
        health.record_missed_heartbeat()
        health.record_missed_heartbeat()
        assert health.state == NodeState.UNREACHABLE

        # UNREACHABLE -> HEALTHY (收到心跳)
        health.record_heartbeat(latency_ms=10.0)
        assert health.state == NodeState.HEALTHY

    def test_partition_state_transitions(self):
        """隐藏逻辑：分区状态转换"""
        pm = PartitionManager(
            node_id="node1",
            peers=["node2", "node3"]
        )

        # 初始状态
        assert pm._partition_state == PartitionState.HEALTHY

    @pytest.mark.asyncio
    async def test_transaction_status_transitions(self):
        """隐藏逻辑：事务状态转换"""
        coordinator = TwoPhaseCommitCoordinator()
        await coordinator.start()

        try:
            tx = await coordinator.begin_transaction()
            assert tx.status == TransactionStatus.ACTIVE

            # 准备 - 零参与者事务会成功
            await coordinator.prepare(tx.tx_id)
            tx = await coordinator.store.load_transaction(tx.tx_id)
            # 零参与者事务prepare后状态可能是PREPARED或PREPARING
            assert tx.status in [TransactionStatus.PREPARED, TransactionStatus.PREPARING]
        finally:
            await coordinator.stop()


# ============================================================================
# 第二部分：默认行为测试
# ============================================================================

class TestDefaultBehavior:
    """测试默认行为"""

    def test_serializer_factory_default_serializers(self):
        """默认行为：SerializerFactory默认序列化器"""
        serializers = SerializerFactory.list_serializers()
        assert "json" in serializers
        assert "msgpack" in serializers
        assert "pickle" in serializers

    def test_json_serializer_default_compression(self):
        """默认行为：JSONSerializer默认不压缩"""
        serializer = JSONSerializer()
        assert serializer.compression == False

    def test_pickle_serializer_default_protocol(self):
        """默认行为：PickleSerializer使用最高协议"""
        serializer = PickleSerializer()
        assert serializer.protocol == pickle.HIGHEST_PROTOCOL

    def test_quorum_manager_default_quorums(self):
        """默认行为：QuorumManager默认仲裁大小"""
        qm = QuorumManager(total_nodes=5)
        # 读仲裁 = floor(5/2) = 2
        assert qm.read_quorum == 2
        # 写仲裁 = floor(5/2) + 1 = 3
        assert qm.write_quorum == 3

    def test_partition_manager_default_intervals(self):
        """默认行为：PartitionManager默认间隔"""
        pm = PartitionManager(node_id="node1", peers=["node2"])
        assert pm._heartbeat_interval == 1.0
        assert pm._heartbeat_timeout == 5.0
        assert pm._partition_threshold == 3

    def test_input_validator_default_patterns(self):
        """默认行为：InputValidator默认检测模式"""
        validator = InputValidator()
        # 应该有SQL注入模式
        assert len(validator._sql_regex) > 0
        # 应该有XSS模式
        assert len(validator._xss_regex) > 0
        # 应该有命令注入模式
        assert len(validator._cmd_regex) > 0

    def test_crypto_manager_default_key_generation(self):
        """默认行为：CryptoManager自动生成密钥"""
        crypto = CryptoManager()
        assert crypto._master_key is not None
        assert len(crypto._master_key) >= 32

    def test_csrf_protector_default_ttl(self):
        """默认行为：CSRFProtector默认TTL"""
        csrf = CSRFProtector()
        assert csrf.token_ttl == 3600  # 1小时

    def test_audit_logger_default_max_entries(self):
        """默认行为：AuditLogger默认最大条目"""
        audit = AuditLogger()
        assert audit._max_entries == 100000

    def test_vulnerability_scanner_default_checks(self):
        """默认行为：VulnerabilityScanner默认检查"""
        scanner = VulnerabilityScanner()
        assert len(scanner._checks) >= 3  # 至少3个默认检查

    def test_two_phase_commit_coordinator_default_timeout(self):
        """默认行为：2PC协调器默认超时"""
        coordinator = TwoPhaseCommitCoordinator()
        assert coordinator.default_timeout == 30.0


class TestParameterDefaults:
    """测试参数默认值"""

    def test_snapshot_serializer_manager_serialize_defaults(self):
        """默认行为：序列化方法默认参数"""
        manager = SnapshotSerializerManager(default_serializer="json")
        data = {"key": "value"}

        # 使用默认序列化器
        result = manager.serialize(data)
        assert result.serializer_type == "json"
        assert result.compressed == False

    def test_input_validator_sanitize_defaults(self):
        """默认行为：字符串清理默认参数"""
        validator = InputValidator()
        long_string = "x" * 2000

        # 使用默认max_length
        sanitized = validator.sanitize_string(long_string)
        assert len(sanitized) == 1000

    def test_input_validator_validate_defaults(self):
        """默认行为：输入验证默认检查"""
        validator = InputValidator()
        # 默认检查SQL注入和XSS
        valid, errors = validator.validate_input("normal input", {})
        assert valid == True

    def test_rbac_check_permission_defaults(self):
        """默认行为：权限检查默认参数"""
        rbac = RBACManager()
        user = rbac.create_user("test", roles=[Role.USER])

        # 不指定resource时使用默认权限
        assert rbac.check_permission(user.user_id, Permission.READ) == True


# ============================================================================
# 第三部分：异常兜底测试
# ============================================================================

class TestExceptionFallback:
    """测试异常兜底"""

    def test_json_deserialize_invalid_utf8(self):
        """异常兜底：无效UTF-8"""
        serializer = JSONSerializer()
        invalid_utf8 = b'\xff\xfe'

        with pytest.raises(SerializationError):
            serializer.deserialize(invalid_utf8)

    def test_json_deserialize_invalid_json(self):
        """异常兜底：无效JSON"""
        serializer = JSONSerializer()
        invalid_json = b'{not valid json}'

        with pytest.raises(SerializationError):
            serializer.deserialize(invalid_json)

    def test_gzip_decompress_invalid_data(self):
        """异常兜底：无效gzip数据"""
        serializer = JSONSerializer(compression=True)
        invalid_gzip = b'not gzip data'

        with pytest.raises(SerializationError):
            serializer.deserialize(invalid_gzip)

    def test_pickle_deserialize_corrupted_data(self):
        """异常兜底：损坏的pickle数据"""
        serializer = PickleSerializer()
        corrupted = b'\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x8c\x00\x94.'

        with pytest.raises(SerializationError):
            serializer.deserialize(corrupted)

    def test_serializer_factory_unknown_type(self):
        """异常兜底：未知序列化器类型"""
        with pytest.raises(ValueError):
            SerializerFactory.create("unknown")

    @pytest.mark.asyncio
    async def test_event_bus_handler_exception(self):
        """异常兜底：事件处理器异常"""
        bus = InMemoryEventBus()
        await bus.connect()

        try:
            handler_called = asyncio.Event()

            async def failing_handler(event):
                handler_called.set()
                raise RuntimeError("Handler error")

            await bus.subscribe("test", failing_handler)

            event = DomainEvent(event_type="test", payload={})
            await bus.publish(event)

            await asyncio.wait_for(handler_called.wait(), timeout=2.0)
            # 异常应该被捕获，不影响系统
        finally:
            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_transaction_not_found(self):
        """异常兜底：事务未找到"""
        coordinator = TwoPhaseCommitCoordinator()

        with pytest.raises(TransactionNotFoundError):
            await coordinator.prepare("nonexistent_tx")

    @pytest.mark.asyncio
    async def test_participant_not_registered(self):
        """异常兜底：参与者未注册"""
        coordinator = TwoPhaseCommitCoordinator()

        tx = await coordinator.begin_transaction()

        with pytest.raises(TransactionError):
            await coordinator.add_participant(tx.tx_id, "unregistered")

    def test_crypto_decrypt_invalid_data(self):
        """异常兜底：解密无效数据"""
        crypto = CryptoManager()

        with pytest.raises(Exception):
            crypto.decrypt("not valid encrypted data")

    def test_csrf_validate_invalid_token(self):
        """异常兜底：CSRF无效令牌"""
        csrf = CSRFProtector()
        session_id = "session1"
        csrf.generate_token(session_id)

        # 无效令牌
        assert csrf.validate_token(session_id, "invalid") == False

    def test_csrf_validate_expired_token(self):
        """异常兜底：CSRF过期令牌"""
        csrf = CSRFProtector(token_ttl=0.1)
        session_id = "session1"
        token = csrf.generate_token(session_id)

        time.sleep(0.2)

        assert csrf.validate_token(session_id, token) == False

    def test_rbac_check_permission_nonexistent_user(self):
        """异常兜底：检查不存在用户的权限"""
        rbac = RBACManager()

        assert rbac.check_permission("nonexistent", Permission.READ) == False

    def test_abac_evaluate_exception_in_condition(self):
        """异常兜底：ABAC条件异常"""
        abac = ABACManager()

        def failing_condition(subject, resource, env):
            raise RuntimeError("Condition error")

        abac.add_policy(
            name="failing_policy",
            subject_attrs={},
            resource_attrs={},
            action="read",
            condition=failing_condition,
            effect="allow"
        )

        subject = {}
        resource = {}
        # 条件异常应该被捕获，返回默认拒绝
        allowed, policy = abac.evaluate(subject, resource, "read")
        assert allowed == False


class TestErrorRecovery:
    """测试错误恢复"""

    @pytest.mark.asyncio
    async def test_dlq_retry_success(self):
        """错误恢复：DLQ重试成功"""
        bus = InMemoryEventBus()
        await bus.connect()

        try:
            dlq = DeadLetterQueueHandler(bus, max_retries=3, retry_delays=[0.1, 0.1, 0.1])

            attempt_count = 0

            async def eventually_succeeding_handler(event):
                nonlocal attempt_count
                attempt_count += 1
                if attempt_count < 2:
                    raise ValueError("Not yet")
                # 第二次成功

            dlq.register_handler("test", eventually_succeeding_handler)

            await dlq.start()

            event = DomainEvent(event_type="test", payload={})
            await dlq.enqueue(event, "Initial error", "handler")

            await asyncio.sleep(1)

            # 应该最终成功
            assert attempt_count >= 2

            await dlq.stop()
        finally:
            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_saga_compensation_recovery(self):
        """错误恢复：Saga补偿恢复"""
        bus = InMemoryEventBus()
        await bus.connect()

        try:
            orchestrator = SagaOrchestrator(bus)

            compensation_called = asyncio.Event()

            async def success_action(context):
                return "result"

            async def compensation(context):
                compensation_called.set()

            async def failing_action(context):
                raise ValueError("Failed")

            async def unused_compensation(context):
                pass

            steps = [
                SagaStep("step1", success_action, compensation, retry_count=1),
                SagaStep("step2", failing_action, unused_compensation, retry_count=1),
            ]

            orchestrator.register_saga("test", steps)
            instance = await orchestrator.start_saga("test", {})

            await asyncio.sleep(2)

            # 补偿应该被执行
            assert compensation_called.is_set()
        finally:
            await bus.disconnect()

    def test_audit_logger_overflow(self):
        """错误恢复：审计日志溢出"""
        audit = AuditLogger(max_entries=5)

        # 添加超过限制的日志
        for i in range(10):
            audit.log("user1", f"action_{i}", "resource", "success")

        # 应该只保留最新的5条
        stats = audit.get_stats()
        assert stats["total_entries"] == 5

    def test_csrf_cleanup_expired(self):
        """错误恢复：CSRF清理过期令牌"""
        csrf = CSRFProtector(token_ttl=0.1)

        # 添加多个令牌
        for i in range(5):
            csrf.generate_token(f"session_{i}")

        time.sleep(0.2)

        # 清理过期令牌
        cleaned = csrf.cleanup_expired()
        assert cleaned == 5


# ============================================================================
# 第四部分：边界条件测试
# ============================================================================

class TestBoundaryConditions:
    """测试边界条件"""

    def test_empty_data_serialization(self):
        """边界：空数据序列化"""
        serializer = JSONSerializer()
        result = serializer.serialize({})
        assert json.loads(result) == {}

    def test_none_values_in_data(self):
        """边界：数据中的None值"""
        serializer = JSONSerializer()
        data = {"key": None, "nested": {"value": None}}
        result = serializer.serialize(data)
        parsed = json.loads(result)
        assert parsed["key"] is None
        assert parsed["nested"]["value"] is None

    def test_very_long_string(self):
        """边界：超长字符串"""
        serializer = JSONSerializer()
        long_string = "x" * (10 * 1024 * 1024)  # 10MB
        data = {"value": long_string}

        result = serializer.serialize(data)
        parsed = json.loads(result)
        assert len(parsed["value"]) == len(long_string)

    def test_deeply_nested_structure(self):
        """边界：深层嵌套结构"""
        serializer = JSONSerializer()
        data = {"level": 0}
        current = data
        for i in range(1, 100):
            current["nested"] = {"level": i}
            current = current["nested"]

        result = serializer.serialize(data)
        parsed = json.loads(result)
        assert parsed["level"] == 0

    def test_special_float_values(self):
        """边界：特殊浮点值"""
        serializer = JSONSerializer()
        # 注意：JSON不支持NaN/Infinity，但这些会被转换为字符串
        data = {
            "normal": 1.5,
            "negative": -1.5,
            "zero": 0.0,
            "negative_zero": -0.0,
        }
        result = serializer.serialize(data)
        parsed = json.loads(result)
        assert parsed["normal"] == 1.5

    def test_unicode_all_ranges(self):
        """边界：Unicode全范围"""
        serializer = JSONSerializer()
        data = {
            "ascii": "Hello World",
            "latin1": "café",
            "chinese": "中文",
            "emoji": "😀🎉🚀",
            "mixed": "Hello 世界 🌍",
        }
        result = serializer.serialize(data)
        parsed = json.loads(result)
        assert parsed["chinese"] == "中文"
        assert parsed["emoji"] == "😀🎉🚀"

    @pytest.mark.asyncio
    async def test_event_with_empty_payload(self):
        """边界：空负载事件"""
        bus = InMemoryEventBus()
        await bus.connect()

        try:
            event = DomainEvent(event_type="test", payload={})
            success = await bus.publish(event)
            assert success == True
        finally:
            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_event_with_large_payload(self):
        """边界：大负载事件"""
        bus = InMemoryEventBus()
        await bus.connect()

        try:
            large_payload = {"data": "x" * 100000}
            event = DomainEvent(event_type="test", payload=large_payload)
            success = await bus.publish(event)
            assert success == True
        finally:
            await bus.disconnect()

    def test_quorum_single_node(self):
        """边界：单节点仲裁"""
        qm = QuorumManager(total_nodes=1)
        assert qm.read_quorum == 0
        assert qm.write_quorum == 1

    def test_quorum_two_nodes(self):
        """边界：两节点仲裁"""
        qm = QuorumManager(total_nodes=2)
        assert qm.read_quorum == 1
        assert qm.write_quorum == 2

    def test_partition_no_peers(self):
        """边界：无对等节点"""
        pm = PartitionManager(node_id="node1", peers=[])
        assert pm.is_partitioned() == False

    def test_input_validator_empty_input(self):
        """边界：空输入"""
        validator = InputValidator()
        sanitized = validator.sanitize_string("")
        assert sanitized == ""

    def test_input_validator_max_length_boundary(self):
        """边界：最大长度边界"""
        validator = InputValidator()
        # 恰好最大长度
        exact_length = "x" * 1000
        sanitized = validator.sanitize_string(exact_length)
        assert len(sanitized) == 1000

        # 超过最大长度
        over_length = "x" * 1001
        sanitized = validator.sanitize_string(over_length)
        assert len(sanitized) == 1000

    def test_crypto_empty_string_encryption(self):
        """边界：空字符串加密"""
        crypto = CryptoManager()
        encrypted = crypto.encrypt("")
        decrypted = crypto.decrypt(encrypted)
        assert decrypted == ""

    def test_rbac_user_no_roles(self):
        """边界：用户无角色"""
        rbac = RBACManager()
        # 创建用户时不指定角色，默认会分配USER角色
        user = rbac.create_user("test", roles=[])
        # 由于create_user默认分配USER角色，所以有READ权限
        # 如果要测试无角色用户，需要手动清空角色
        user.roles = []
        assert rbac.check_permission(user.user_id, Permission.READ) == False


# ============================================================================
# 第五部分：并发和线程安全测试
# ============================================================================

class TestConcurrencySafety:
    """测试并发安全"""

    def test_audit_logger_thread_safety(self):
        """并发安全：审计日志线程安全"""
        audit = AuditLogger()
        errors = []

        def log_entries(thread_id):
            try:
                for i in range(100):
                    audit.log(f"user_{thread_id}", f"action_{i}", "resource", "success")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=log_entries, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        stats = audit.get_stats()
        assert stats["total_entries"] == 1000

    def test_csrf_protector_thread_safety(self):
        """并发安全：CSRF保护器线程安全"""
        csrf = CSRFProtector()
        errors = []
        tokens = {}

        def generate_tokens(thread_id):
            try:
                for i in range(100):
                    token = csrf.generate_token(f"session_{thread_id}_{i}")
                    tokens[(thread_id, i)] = token
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=generate_tokens, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(tokens) == 1000

    def test_rbac_manager_thread_safety(self):
        """并发安全：RBAC管理器线程安全"""
        rbac = RBACManager()
        errors = []
        user_ids = []

        def create_users(thread_id):
            try:
                for i in range(100):
                    user = rbac.create_user(f"user_{thread_id}_{i}")
                    user_ids.append(user.user_id)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=create_users, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(user_ids) == 1000

    def test_crypto_manager_thread_safety(self):
        """并发安全：加密管理器线程安全"""
        crypto = CryptoManager()
        errors = []
        results = {}

        def encrypt_decrypt(thread_id):
            try:
                for i in range(100):
                    plaintext = f"data_{thread_id}_{i}"
                    encrypted = crypto.encrypt(plaintext)
                    decrypted = crypto.decrypt(encrypted)
                    results[(thread_id, i)] = (plaintext, decrypted)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=encrypt_decrypt, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        for (thread_id, i), (plaintext, decrypted) in results.items():
            assert plaintext == decrypted


# ============================================================================
# 运行测试
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

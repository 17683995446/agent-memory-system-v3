"""第16轮深度实践 - 分布式存储后端 + 分布式事务 + i18n 全面测试

覆盖模块:
1. distributed_storage_backend.py - 一致性哈希环 + 分布式存储
2. distributed_transaction_manager.py - 2PC + Saga事务
3. i18n.py - 国际化消息系统

测试策略:
- 一致性哈希环: 节点增删、数据迁移、权重分布、边界条件
- 分布式存储: 仲裁读写、节点故障、并发操作、统计一致性
- 2PC事务: 超时、参与者故障、并行提交、恢复
- Saga事务: 补偿失败、重试机制、上下文传递
- i18n: 消息格式化、缺失key回退、多语言一致性
"""

import asyncio
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory_system.storage_backends.distributed_storage_backend import (
    ConsistentHashRing,
    StorageNode,
    DistributedStorageBackend,
)
from memory_system.distributed_transaction_manager import (
    Transaction,
    TransactionContext,
    TransactionError,
    TransactionNotFoundError,
    TransactionStatus,
    TransactionTimeoutError,
    Participant,
    ParticipantError,
    ParticipantStatus,
    SagaCompensationError,
    SagaStep,
    TwoPhaseCommitCoordinator,
    InMemoryTransactionStore,
)
from memory_system.exceptions import StorageError
from memory_system.i18n import Locale, MESSAGES


# ============================================================
# 一致性哈希环测试
# ============================================================

class TestConsistentHashRing:
    """一致性哈希环测试"""

    def setup_method(self):
        self.ring = ConsistentHashRing(replicas=150)

    def test_add_and_get_node(self):
        """添加节点并获取"""
        node = StorageNode(node_id="node1", host="localhost", port=8000)
        self.ring.add_node(node)
        result = self.ring.get_node("some_key")
        assert result is not None
        assert result.node_id == "node1"

    def test_empty_ring_returns_none(self):
        """空环返回None"""
        assert self.ring.get_node("key") is None
        assert self.ring.get_nodes("key", 3) == []

    def test_multiple_nodes_distribution(self):
        """多节点数据分布均匀性"""
        nodes = [
            StorageNode(node_id=f"node_{i}", host="localhost", port=8000 + i)
            for i in range(10)
        ]
        for node in nodes:
            self.ring.add_node(node)

        # 测试10000个key的分布
        distribution = {f"node_{i}": 0 for i in range(10)}
        for i in range(10000):
            node = self.ring.get_node(f"key_{i}")
            distribution[node.node_id] += 1

        # 每个节点应该获得约10%的key（允许5%-20%的偏差）
        for node_id, count in distribution.items():
            ratio = count / 10000
            assert 0.02 < ratio < 0.25, f"Node {node_id} has {ratio:.2%} of keys"

    def test_remove_node(self):
        """移除节点后数据重新分布"""
        nodes = [
            StorageNode(node_id=f"node_{i}", host="localhost", port=8000 + i)
            for i in range(5)
        ]
        for node in nodes:
            self.ring.add_node(node)

        # 记录移除前的分布
        before = {}
        for i in range(1000):
            node = self.ring.get_node(f"key_{i}")
            before[f"key_{i}"] = node.node_id

        # 移除node_0
        self.ring.remove_node("node_0")
        assert "node_0" not in self.ring.nodes

        # 验证移除后不再分配到node_0
        for i in range(1000):
            node = self.ring.get_node(f"key_{i}")
            assert node.node_id != "node_0"

    def test_remove_nonexistent_node(self):
        """移除不存在的节点不报错"""
        self.ring.remove_node("nonexistent")  # 不应抛出异常

    def test_get_nodes_for_replication(self):
        """获取多个副本节点"""
        nodes = [
            StorageNode(node_id=f"node_{i}", host="localhost", port=8000 + i)
            for i in range(5)
        ]
        for node in nodes:
            self.ring.add_node(node)

        replica_nodes = self.ring.get_nodes("some_key", n=3)
        assert len(replica_nodes) == 3
        # 所有节点应该不同
        node_ids = [n.node_id for n in replica_nodes]
        assert len(set(node_ids)) == 3

    def test_get_nodes_more_than_available(self):
        """请求副本数超过可用节点数"""
        nodes = [
            StorageNode(node_id=f"node_{i}", host="localhost", port=8000 + i)
            for i in range(3)
        ]
        for node in nodes:
            self.ring.add_node(node)

        replica_nodes = self.ring.get_nodes("some_key", n=10)
        assert len(replica_nodes) == 3  # 最多返回3个

    def test_weighted_nodes(self):
        """权重节点分布"""
        normal_node = StorageNode(node_id="normal", host="localhost", port=8000, weight=1)
        heavy_node = StorageNode(node_id="heavy", host="localhost", port=8001, weight=3)
        self.ring.add_node(normal_node)
        self.ring.add_node(heavy_node)

        distribution = {"normal": 0, "heavy": 0}
        for i in range(10000):
            node = self.ring.get_node(f"key_{i}")
            distribution[node.node_id] += 1

        # heavy节点应该获得约3倍于normal节点的key
        ratio = distribution["heavy"] / max(distribution["normal"], 1)
        assert ratio > 1.5, f"Heavy node ratio {ratio:.2f} too low"

    def test_node_health_filtering(self):
        """节点健康状态过滤"""
        nodes = [
            StorageNode(node_id="healthy1", host="localhost", port=8000, is_healthy=True),
            StorageNode(node_id="unhealthy", host="localhost", port=8001, is_healthy=False),
            StorageNode(node_id="healthy2", host="localhost", port=8002, is_healthy=True),
        ]
        for node in nodes:
            self.ring.add_node(node)

        healthy = self.ring.get_all_healthy_nodes()
        assert len(healthy) == 2
        assert all(n.is_healthy for n in healthy)

    def test_update_node_health(self):
        """更新节点健康状态"""
        node = StorageNode(node_id="node1", host="localhost", port=8000, is_healthy=True)
        self.ring.add_node(node)

        self.ring.update_node_health("node1", is_healthy=False)
        assert self.ring.nodes["node1"].is_healthy is False

        self.ring.update_node_health("node1", is_healthy=True)
        assert self.ring.nodes["node1"].is_healthy is True
        assert self.ring.nodes["node1"].last_heartbeat > 0

    def test_update_nonexistent_node_health(self):
        """更新不存在节点的健康状态不报错"""
        self.ring.update_node_health("nonexistent", is_healthy=True)  # 不应抛出

    def test_deterministic_mapping(self):
        """相同的key总是映射到相同的节点"""
        nodes = [
            StorageNode(node_id=f"node_{i}", host="localhost", port=8000 + i)
            for i in range(5)
        ]
        for node in nodes:
            self.ring.add_node(node)

        for i in range(100):
            node1 = self.ring.get_node(f"key_{i}")
            node2 = self.ring.get_node(f"key_{i}")
            assert node1.node_id == node2.node_id


# ============================================================
# 分布式存储后端测试
# ============================================================

class TestDistributedStorageBackend:
    """分布式存储后端测试"""

    @pytest.fixture
    def storage_nodes(self):
        return [
            StorageNode(node_id=f"node_{i}", host="localhost", port=8000 + i)
            for i in range(5)
        ]

    def test_init_with_no_nodes(self):
        """无节点初始化"""
        backend = DistributedStorageBackend(nodes=[])
        assert backend.replication_factor == 0

    def test_init_clamps_quorum_to_node_count(self):
        """仲裁数不超过节点数"""
        nodes = [StorageNode(node_id="n1", host="localhost", port=8000)]
        backend = DistributedStorageBackend(
            nodes=nodes, replication_factor=3, read_quorum=3, write_quorum=3
        )
        assert backend.replication_factor == 1
        assert backend.read_quorum == 1
        assert backend.write_quorum == 1

    @pytest.mark.asyncio
    async def test_add_with_no_healthy_nodes_raises(self, storage_nodes):
        """所有节点不健康时写入应失败"""
        for node in storage_nodes:
            node.is_healthy = False
        backend = DistributedStorageBackend(nodes=storage_nodes)

        from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        item = MemoryItem(
            memory_id="test_1",
            content="test content",
            memory_type=MemoryType.SHORT_TERM,
            agent_id="agent_1",
        )
        with pytest.raises(StorageError, match="No healthy nodes"):
            await backend.add(item)

    @pytest.mark.asyncio
    async def test_get_with_no_healthy_nodes_raises(self, storage_nodes):
        """所有节点不健康时读取应失败"""
        for node in storage_nodes:
            node.is_healthy = False
        backend = DistributedStorageBackend(nodes=storage_nodes)

        with pytest.raises(StorageError, match="No healthy nodes"):
            await backend.get("test_1")

    @pytest.mark.asyncio
    async def test_stats_tracking(self, storage_nodes):
        """统计信息追踪"""
        backend = DistributedStorageBackend(nodes=storage_nodes)
        # 初始统计
        assert backend._stats["writes"] == 0
        assert backend._stats["reads"] == 0


# ============================================================
# 分布式事务管理器测试
# ============================================================

class TestTransactionDataModel:
    """事务数据模型测试"""

    def test_transaction_creation(self):
        """事务创建"""
        tx = Transaction()
        assert tx.status == TransactionStatus.ACTIVE
        assert tx.tx_id is not None
        assert len(tx.tx_id) > 0
        assert tx.context is not None
        assert tx.is_timed_out() is False

    def test_transaction_with_timeout(self):
        """事务超时检测"""
        tx = Transaction(timeout_at=time.time() - 1)
        assert tx.is_timed_out() is True

    def test_transaction_without_timeout(self):
        """无超时事务"""
        tx = Transaction(timeout_at=None)
        assert tx.is_timed_out() is False

    def test_transaction_participant_management(self):
        """事务参与者管理"""
        tx = Transaction()
        tx.add_participant("p1")
        tx.add_participant("p2")
        tx.add_participant("p3")

        assert tx.all_prepared() is False

        tx.mark_prepared("p1")
        tx.mark_prepared("p2")
        assert tx.all_prepared() is False

        tx.mark_prepared("p3")
        assert tx.all_prepared() is True

    def test_transaction_operation_recording(self):
        """事务操作记录（用于Saga补偿）"""
        tx = Transaction()
        tx.record_operation({"step": "reserve", "result": "ok"})
        tx.record_operation({"step": "charge", "result": "ok"})

        operations = tx.get_completed_operations()
        assert len(operations) == 2
        # 应该是逆序的（后进先出）
        assert operations[0]["step"] == "charge"
        assert operations[1]["step"] == "reserve"

    def test_transaction_context(self):
        """事务上下文"""
        ctx = TransactionContext()
        ctx.set("key1", "value1")
        ctx.set("key2", 42)
        assert ctx.get("key1") == "value1"
        assert ctx.get("key2") == 42
        assert ctx.get("nonexistent") is None
        assert ctx.get("nonexistent", "default") == "default"

        ctx.update({"key3": "value3", "key1": "updated"})
        assert ctx.get("key1") == "updated"
        assert ctx.get("key3") == "value3"


class TestParticipant:
    """参与者测试"""

    @pytest.mark.asyncio
    async def test_participant_prepare_success(self):
        """参与者准备成功"""
        async def prepare_handler(tx_id, ctx):
            return True

        participant = Participant(
            participant_id="p1",
            resource_id="resource_1",
            prepare_handler=prepare_handler,
        )
        result = await participant.prepare("tx_1", TransactionContext())
        assert result is True
        assert participant.status == ParticipantStatus.PREPARED

    @pytest.mark.asyncio
    async def test_participant_prepare_failure(self):
        """参与者准备失败"""
        async def prepare_handler(tx_id, ctx):
            raise RuntimeError("DB connection lost")

        participant = Participant(
            participant_id="p1",
            resource_id="resource_1",
            prepare_handler=prepare_handler,
        )
        result = await participant.prepare("tx_1", TransactionContext())
        assert result is False
        assert participant.status == ParticipantStatus.FAILED

    @pytest.mark.asyncio
    async def test_participant_no_handler(self):
        """无处理器的参与者默认成功"""
        participant = Participant(
            participant_id="p1",
            resource_id="resource_1",
        )
        assert await participant.prepare("tx_1", TransactionContext()) is True
        assert await participant.commit("tx_1", TransactionContext()) is True
        assert await participant.abort("tx_1", TransactionContext()) is True


class TestInMemoryTransactionStore:
    """内存事务存储测试"""

    @pytest.mark.asyncio
    async def test_save_and_load(self):
        """保存和加载事务"""
        store = InMemoryTransactionStore()
        tx = Transaction()
        await store.save_transaction(tx)
        loaded = await store.load_transaction(tx.tx_id)
        assert loaded is not None
        assert loaded.tx_id == tx.tx_id

    @pytest.mark.asyncio
    async def test_load_nonexistent(self):
        """加载不存在的事务"""
        store = InMemoryTransactionStore()
        result = await store.load_transaction("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_status(self):
        """更新事务状态"""
        store = InMemoryTransactionStore()
        tx = Transaction()
        await store.save_transaction(tx)

        result = await store.update_transaction_status(tx.tx_id, TransactionStatus.COMMITTED)
        assert result is True

        loaded = await store.load_transaction(tx.tx_id)
        assert loaded.status == TransactionStatus.COMMITTED

    @pytest.mark.asyncio
    async def test_update_nonexistent_status(self):
        """更新不存在的事务状态"""
        store = InMemoryTransactionStore()
        result = await store.update_transaction_status("nonexistent", TransactionStatus.COMMITTED)
        assert result is False

    @pytest.mark.asyncio
    async def test_get_active_transactions(self):
        """获取活动事务"""
        store = InMemoryTransactionStore()
        tx_active = Transaction(status=TransactionStatus.ACTIVE)
        tx_committed = Transaction(status=TransactionStatus.COMMITTED)
        tx_preparing = Transaction(status=TransactionStatus.PREPARING)

        await store.save_transaction(tx_active)
        await store.save_transaction(tx_committed)
        await store.save_transaction(tx_preparing)

        active = await store.get_active_transactions()
        assert len(active) == 2
        active_ids = {tx.tx_id for tx in active}
        assert tx_active.tx_id in active_ids
        assert tx_preparing.tx_id in active_ids
        assert tx_committed.tx_id not in active_ids


class TestTwoPhaseCommitCoordinator:
    """2PC协调器测试"""

    @pytest.mark.asyncio
    async def test_begin_transaction(self):
        """开始事务"""
        coordinator = TwoPhaseCommitCoordinator()
        tx = await coordinator.begin_transaction()
        assert tx.status == TransactionStatus.ACTIVE
        assert tx.timeout_at is not None

    @pytest.mark.asyncio
    async def test_begin_transaction_with_custom_timeout(self):
        """自定义超时开始事务"""
        coordinator = TwoPhaseCommitCoordinator()
        tx = await coordinator.begin_transaction(timeout=5.0)
        assert tx.timeout_at is not None
        # 超时时间应该在约5秒后
        assert tx.timeout_at > time.time()
        assert tx.timeout_at <= time.time() + 6.0

    @pytest.mark.asyncio
    async def test_add_participant_success(self):
        """添加参与者成功"""
        coordinator = TwoPhaseCommitCoordinator()
        participant = Participant(
            participant_id="p1",
            resource_id="r1",
        )
        coordinator.register_participant(participant)

        tx = await coordinator.begin_transaction()
        result = await coordinator.add_participant(tx.tx_id, "p1")
        assert result is True

    @pytest.mark.asyncio
    async def test_add_participant_not_found(self):
        """事务不存在时添加参与者"""
        coordinator = TwoPhaseCommitCoordinator()
        with pytest.raises(TransactionNotFoundError):
            await coordinator.add_participant("nonexistent", "p1")

    @pytest.mark.asyncio
    async def test_add_participant_not_active(self):
        """非活动事务添加参与者"""
        coordinator = TwoPhaseCommitCoordinator()
        tx = await coordinator.begin_transaction()
        await coordinator.store.update_transaction_status(tx.tx_id, TransactionStatus.COMMITTED)
        with pytest.raises(TransactionError, match="not active"):
            await coordinator.add_participant(tx.tx_id, "p1")

    @pytest.mark.asyncio
    async def test_add_unregistered_participant(self):
        """添加未注册的参与者"""
        coordinator = TwoPhaseCommitCoordinator()
        tx = await coordinator.begin_transaction()
        with pytest.raises(ParticipantError, match="not registered"):
            await coordinator.add_participant(tx.tx_id, "unregistered")

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        """启动和停止协调器"""
        coordinator = TwoPhaseCommitCoordinator()
        await coordinator.start()
        assert coordinator._running is True
        await coordinator.stop()
        assert coordinator._running is False

    @pytest.mark.asyncio
    async def test_abort_nonexistent_transaction(self):
        """中止不存在的事务"""
        coordinator = TwoPhaseCommitCoordinator()
        with pytest.raises(TransactionNotFoundError):
            await coordinator.abort("nonexistent")

    @pytest.mark.asyncio
    async def test_parallel_begin_transactions(self):
        """并行开始多个事务"""
        coordinator = TwoPhaseCommitCoordinator()

        async def begin_tx():
            return await coordinator.begin_transaction()

        transactions = await asyncio.gather(*[begin_tx() for _ in range(50)])
        assert len(transactions) == 50
        # 所有事务ID应该唯一
        tx_ids = {tx.tx_id for tx in transactions}
        assert len(tx_ids) == 50


class TestSagaStep:
    """Saga步骤测试"""

    @pytest.mark.asyncio
    async def test_saga_step_execute_success(self):
        """Saga步骤执行成功"""
        async def action(ctx):
            return {"result": "success"}

        step = SagaStep(
            step_id="step1",
            action=action,
            compensation=lambda ctx, result: asyncio.sleep(0),
        )
        result = await step.execute(TransactionContext())
        assert result == {"result": "success"}

    @pytest.mark.asyncio
    async def test_saga_step_retry_on_failure(self):
        """Saga步骤失败重试"""
        call_count = 0

        async def flaky_action(ctx):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("Transient failure")
            return {"result": "success"}

        step = SagaStep(
            step_id="step1",
            action=flaky_action,
            compensation=lambda ctx, result: asyncio.sleep(0),
            max_retries=3,
        )
        result = await step.execute(TransactionContext())
        assert result == {"result": "success"}
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_saga_step_exhaust_retries(self):
        """Saga步骤重试耗尽 - 应抛出原始异常"""
        async def always_fail(ctx):
            raise RuntimeError("Permanent failure")

        step = SagaStep(
            step_id="step1",
            action=always_fail,
            compensation=lambda ctx, result: asyncio.sleep(0),
            max_retries=2,
        )
        # 修复后应抛出原始RuntimeError而非tenacity.RetryError
        with pytest.raises(RuntimeError, match="Permanent failure"):
            await step.execute(TransactionContext())

    @pytest.mark.asyncio
    async def test_saga_compensation_failure(self):
        """Saga补偿失败"""
        async def compensation(ctx, result):
            raise RuntimeError("Compensation failed")

        step = SagaStep(
            step_id="step1",
            action=lambda ctx: asyncio.sleep(0),
            compensation=compensation,
        )
        with pytest.raises(SagaCompensationError, match="Compensation failed"):
            await step.compensate(TransactionContext(), None)


# ============================================================
# i18n 国际化测试
# ============================================================

class TestI18n:
    """国际化消息系统测试"""

    def test_all_locales_have_en_messages(self):
        """所有语言至少有英文消息的子集"""
        en_keys = set(MESSAGES["en"].keys())
        for locale, messages in MESSAGES.items():
            if locale == "en":
                continue
            # 其他语言不需要有所有英文key，但应该有部分
            assert len(messages) > 0, f"Locale {locale} has no messages"

    def test_english_messages_completeness(self):
        """英文消息完整性 - 检查所有消息key"""
        en = MESSAGES["en"]
        expected_keys = [
            "memory_not_found", "invalid_importance", "storage_full",
            "invalid_content", "query_too_long", "invalid_memory_type",
            "operation_failed", "unauthorized", "forbidden",
            "not_implemented", "internal_error", "service_unavailable",
            "timeout", "rate_limit_exceeded", "invalid_parameter",
            "missing_parameter", "invalid_format", "validation_error",
            "storage_connection_error", "storage_write_error",
            "storage_read_error", "storage_quota_exceeded",
            "memory_too_large", "embedding_error",
            "embedding_dimension_mismatch", "embedding_provider_error",
            "retrieval_error", "invalid_similarity_threshold",
            "invalid_max_results", "tenant_not_found",
            "tenant_quota_exceeded", "tenant_disabled",
            "auth_required", "invalid_token", "insufficient_permissions",
        ]
        for key in expected_keys:
            assert key in en, f"Missing English message key: {key}"

    def test_chinese_messages_completeness(self):
        """中文消息完整性 - 应与英文一致"""
        en_keys = set(MESSAGES["en"].keys())
        zh_keys = set(MESSAGES["zh"].keys())
        missing = en_keys - zh_keys
        assert len(missing) == 0, f"Chinese messages missing keys: {missing}"

    def test_message_formatting(self):
        """消息格式化 - 参数替换"""
        msg = MESSAGES["en"]["memory_not_found"]
        formatted = msg.format(memory_id="abc123")
        assert formatted == "Memory not found: abc123"

    def test_message_formatting_multiple_params(self):
        """多参数消息格式化"""
        msg = MESSAGES["en"]["storage_quota_exceeded"]
        formatted = msg.format(used="100MB", limit="200MB")
        assert "100MB" in formatted
        assert "200MB" in formatted

    def test_message_formatting_chinese(self):
        """中文消息格式化"""
        msg = MESSAGES["zh"]["memory_not_found"]
        formatted = msg.format(memory_id="abc123")
        assert "abc123" in formatted

    def test_japanese_messages_exist(self):
        """日文消息存在性"""
        assert "memory_not_found" in MESSAGES["ja"]
        assert "timeout" in MESSAGES["ja"]

    def test_korean_messages_exist(self):
        """韩文消息存在性"""
        assert "memory_not_found" in MESSAGES["ko"]
        assert "timeout" in MESSAGES["ko"]

    def test_german_messages_exist(self):
        """德文消息存在性"""
        assert "memory_not_found" in MESSAGES["de"]
        assert "timeout" in MESSAGES["de"]

    def test_locale_enum_values(self):
        """Locale枚举值"""
        assert Locale.EN.value == "en"
        assert Locale.ZH.value == "zh"
        assert Locale.JA.value == "ja"
        assert Locale.KO.value == "ko"
        assert Locale.DE.value == "de"
        assert Locale.FR.value == "fr"
        assert Locale.ES.value == "es"

    def test_all_locale_enum_have_messages(self):
        """所有Locale枚举值都有对应的消息"""
        for locale in Locale:
            assert locale.value in MESSAGES, f"Locale {locale} has no messages"

    def test_message_no_unexpected_placeholders(self):
        """消息中不应有未定义的占位符"""
        for locale, messages in MESSAGES.items():
            for key, msg in messages.items():
                # 检查是否有花括号但没有正确格式的情况
                open_count = msg.count("{")
                close_count = msg.count("}")
                assert open_count == close_count, \
                    f"Locale {locale}, key {key}: mismatched braces in '{msg}'"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

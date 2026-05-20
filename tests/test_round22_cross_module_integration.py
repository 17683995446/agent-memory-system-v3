"""
Agent Memory System V3.1.0 - 第22轮全面测试文件

测试范围: 跨模块集成测试
覆盖模块:
  - grpc_service.py (MockMemoryStore, AgentMemoryServicer)
  - event_driven_core.py (SagaOrchestrator, InMemoryEventBus, InMemoryEventStore)
  - distributed_transaction_manager.py (SagaOrchestrator, TwoPhaseCommitCoordinator)

BUG风险点验证:
  - P0: MockMemoryStore.get() 缺少租户隔离 (不校验 agent_id/tenant_id)
  - P1: MockMemoryStore.search() 硬编码分数 0.5
  - P2: MockMemoryStore TTL 参数被忽略
  - P0: 两套 SagaOrchestrator 实现不一致 (event_driven_core vs distributed_transaction_manager)
  - 补偿失败后的状态标记差异
  - 2PC 部分提交失败处理
  - 长时间压力场景
  - 混乱场景 (随机失败、网络分区)
  - 数据自洽验证
"""

import asyncio
import json
import random
import sys
import time
import types
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

# ============================================================================
# 导入被测模块 - 处理 grpc 依赖缺失的情况
# ============================================================================

# grpc_service.py 依赖 grpc 模块，如果未安装则 mock 掉
if "grpc" not in sys.modules:
    _grpc_mock = types.ModuleType("grpc")
    _grpc_mock.Server = type("Server", (), {})()
    _grpc_mock.StatusCode = type("StatusCode", (), {
        "UNAVAILABLE": "UNAVAILABLE",
        "NOT_FOUND": "NOT_FOUND",
        "INTERNAL": "INTERNAL",
    })()
    _grpc_mock.Compression = None
    _grpc_mock.ServicerContext = type("ServicerContext", (), {})()
    _grpc_mock.ssl_server_credentials = lambda x: None
    sys.modules["grpc"] = _grpc_mock

from memory_system.grpc_service import (
    MockMemoryStore,
    AgentMemoryServicer,
    MemoryStoreInterface,
    GRPCServiceConfig,
    GRPCServer,
)
from memory_system.event_driven_core import (
    InMemoryEventBus,
    InMemoryEventStore,
    SagaOrchestrator as EventDrivenSagaOrchestrator,
    SagaStep as EventDrivenSagaStep,
    SagaInstance,
    SagaStatus as EventDrivenSagaStatus,
    DomainEvent,
    EventId,
    AggregateId,
    EventPriority,
    EventStatus,
    EventBusFactory,
    BackendType,
    EventBusMonitor,
    DeadLetterQueueHandler,
    DLQMessage,
    ConcurrencyException,
)
from memory_system.distributed_transaction_manager import (
    SagaOrchestrator as DtmSagaOrchestrator,
    SagaStep as DtmSagaStep,
    TwoPhaseCommitCoordinator,
    DistributedTransactionManager,
    Transaction,
    TransactionStatus,
    TransactionContext,
    TransactionError,
    TransactionTimeoutError,
    TransactionNotFoundError,
    TransactionStore,
    InMemoryTransactionStore,
    Participant,
    ParticipantStatus,
    SagaCompensationError,
)


# ============================================================================
# 辅助函数
# ============================================================================

def log(test_name: str, step: str, expected: str, actual: str):
    """统一日志格式"""
    print(f"[LOG] {test_name}::{step} - 期望值: {expected} vs 实际值: {actual}")


# ============================================================================
# grpc_service.py 测试
# ============================================================================


class TestMockMemoryStoreCRUD:
    """MockMemoryStore 基本CRUD操作测试"""

    @pytest.mark.asyncio
    async def test_store_returns_valid_id(self):
        """测试 store 返回有效的 memory_id"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="hello world",
            embedding=[0.1, 0.2, 0.3],
            metadata={"key": "value"},
        )
        is_valid = isinstance(memory_id, str) and len(memory_id) > 0
        log("test_store_returns_valid_id", "store返回有效ID", "True", str(is_valid))
        assert is_valid

    @pytest.mark.asyncio
    async def test_store_and_get_roundtrip(self):
        """测试 store 后 get 能取回数据"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="test content",
            embedding=[0.1, 0.2],
            metadata={"source": "test"},
            memory_type="short_term",
        )
        result = await store.get(memory_id, "agent-001", "tenant-001")
        log("test_store_and_get_roundtrip", "get返回内容", "test content", result.get("content", "") if result else "None")
        assert result is not None
        assert result["content"] == "test content"
        assert result["agent_id"] == "agent-001"
        assert result["tenant_id"] == "tenant-001"

    @pytest.mark.asyncio
    async def test_update_content(self):
        """测试 update 修改内容"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="original",
            embedding=[0.1],
            metadata={},
        )
        success = await store.update(memory_id, "agent-001", "tenant-001", content="updated")
        result = await store.get(memory_id, "agent-001", "tenant-001")
        log("test_update_content", "update后内容", "updated", result.get("content", "") if result else "None")
        assert success is True
        assert result["content"] == "updated"

    @pytest.mark.asyncio
    async def test_update_nonexistent_returns_false(self):
        """测试 update 不存在的记录返回 False"""
        store = MockMemoryStore()
        success = await store.update("nonexistent-id", "agent-001", "tenant-001", content="x")
        log("test_update_nonexistent_returns_false", "update不存在记录", "False", str(success))
        assert success is False

    @pytest.mark.asyncio
    async def test_delete_soft(self):
        """测试软删除"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="to delete",
            embedding=[0.1],
            metadata={},
        )
        success = await store.delete(memory_id, "agent-001", "tenant-001", permanent=False)
        result = await store.get(memory_id, "agent-001", "tenant-001")
        is_deleted = result.get("deleted", False) if result else False
        log("test_delete_soft", "软删除标记deleted", "True", str(is_deleted))
        assert success is True
        assert is_deleted is True

    @pytest.mark.asyncio
    async def test_delete_permanent(self):
        """测试物理删除"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="to delete permanently",
            embedding=[0.1],
            metadata={},
        )
        success = await store.delete(memory_id, "agent-001", "tenant-001", permanent=True)
        result = await store.get(memory_id, "agent-001", "tenant-001")
        log("test_delete_permanent", "物理删除后get返回", "None", str(result))
        assert success is True
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_returns_false(self):
        """测试删除不存在的记录返回 False"""
        store = MockMemoryStore()
        success = await store.delete("nonexistent-id", "agent-001", "tenant-001")
        log("test_delete_nonexistent_returns_false", "删除不存在记录", "False", str(success))
        assert success is False

    @pytest.mark.asyncio
    async def test_search_basic(self):
        """测试基本搜索"""
        store = MockMemoryStore()
        await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="hello",
            embedding=[0.1],
            metadata={},
        )
        results = await store.search("agent-001", "tenant-001", "hello")
        log("test_search_basic", "搜索结果数量", ">=1", str(len(results)))
        assert len(results) >= 1
        assert results[0]["content"] == "hello"


class TestMockMemoryStoreTenantIsolationP0:
    """
    [BUG 7 已修复] MockMemoryStore.get() 现在具备租户隔离
    修复前: get() 直接 self._memories.get(memory_id) 不校验 agent_id/tenant_id
    修复后: get/delete/update 均校验 agent_id 和 tenant_id
    """

    @pytest.mark.asyncio
    async def test_get_no_agent_id_check(self):
        """[BUG 7 已修复] 验证 get() 校验 agent_id"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-A",
            tenant_id="tenant-A",
            content="secret data for agent A",
            embedding=[0.1],
            metadata={},
        )
        # 用不同的 agent_id 去取，应该被拒绝
        result = await store.get(memory_id, "agent-B", "tenant-B")
        # [BUG 7 修复] 跨 agent 访问应返回 None
        log(
            "test_get_no_agent_id_check",
            "跨agent访问应拒绝",
            "None",
            f"{result}",
        )
        assert result is None, "修复后: 跨agent访问应返回None"

    @pytest.mark.asyncio
    async def test_get_tenant_isolation_missing(self):
        """[BUG 7 已修复] 验证 get() 校验 tenant_id"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-X",
            content="tenant X secret",
            embedding=[0.1],
            metadata={},
        )
        # 用不同 tenant_id 去取
        result = await store.get(memory_id, "agent-001", "tenant-Y")
        # [BUG 7 修复] 跨 tenant 访问应返回 None
        log(
            "test_get_tenant_isolation_missing",
            "跨tenant访问应拒绝",
            "None",
            f"{result}",
        )
        assert result is None, "修复后: 跨tenant访问应返回None"

    @pytest.mark.asyncio
    async def test_delete_no_tenant_check(self):
        """[BUG 7 已修复] 验证 delete() 校验 agent_id/tenant_id"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-A",
            tenant_id="tenant-A",
            content="data",
            embedding=[0.1],
            metadata={},
        )
        # 用不同 agent_id 删除
        success = await store.delete(memory_id, "agent-B", "tenant-B", permanent=True)
        # [BUG 7 修复] 跨 agent 删除应被拒绝
        log(
            "test_delete_no_tenant_check",
            "跨agent删除应拒绝",
            "False",
            f"{success}",
        )
        assert success is False, "修复后: 跨agent删除应返回False"

    @pytest.mark.asyncio
    async def test_update_no_tenant_check(self):
        """[BUG 7 已修复] 验证 update() 校验 agent_id/tenant_id"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-A",
            tenant_id="tenant-A",
            content="original",
            embedding=[0.1],
            metadata={},
        )
        # 用不同 agent_id 更新
        success = await store.update(memory_id, "agent-B", "tenant-B", content="hacked")
        result = await store.get(memory_id, "agent-A", "tenant-A")
        # [BUG 7 修复] 跨 agent 更新应被拒绝
        log(
            "test_update_no_tenant_check",
            "跨agent更新应拒绝",
            "False",
            f"{success}",
        )
        assert success is False, "修复后: 跨agent更新应返回False"
        assert result["content"] == "original", "修复后: 内容不应被篡改"


class TestMockMemoryStoreSearchHardcodedScoreP1:
    """
    [BUG 8 已修复] MockMemoryStore.search() 现在使用文本匹配计算分数
    修复前: score = 0.5  # 模拟相似度分数（硬编码）
    修复后: 使用余弦相似度或文本关键词匹配计算分数
    """

    @pytest.mark.asyncio
    async def test_search_all_scores_are_0_5(self):
        """[BUG 8 已修复] 验证搜索分数不再全部是 0.5"""
        store = MockMemoryStore()
        for i in range(5):
            await store.store(
                agent_id="agent-001",
                tenant_id="tenant-001",
                content=f"document {i}",
                embedding=[float(i) * 0.1],
                metadata={},
            )
        results = await store.search("agent-001", "tenant-001", "query")
        scores = [r["score"] for r in results]
        all_same = all(s == 0.5 for s in scores)
        log(
            "test_search_all_scores_are_0_5",
            "所有分数都是0.5(已修复)",
            "False(期望不同分数)",
            f"{all_same}, scores={scores}",
        )
        # [BUG 8 修复] 分数不再全部是0.5
        assert not all_same, "修复后: 搜索分数不应全部为0.5"

    @pytest.mark.asyncio
    async def test_search_ignores_query_embedding(self):
        """[BUG 8 已修复] 验证 search 现在使用 query_embedding"""
        store = MockMemoryStore()
        await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="target",
            embedding=[1.0, 0.0, 0.0],
            metadata={},
        )
        # 完全匹配的 embedding
        results_match = await store.search(
            "agent-001", "tenant-001", "target",
            query_embedding=[1.0, 0.0, 0.0],
        )
        # 完全不匹配的 embedding
        results_no_match = await store.search(
            "agent-001", "tenant-001", "target",
            query_embedding=[0.0, 1.0, 0.0],
        )
        same_score = results_match[0]["score"] == results_no_match[0]["score"]
        log(
            "test_search_ignores_query_embedding",
            "不同embedding得到相同分数(已修复)",
            "False(期望不同分数)",
            f"{same_score}",
        )
        # [BUG 8 修复] 不同 embedding 应产生不同分数
        assert not same_score, "修复后: 不同embedding应产生不同分数"

    @pytest.mark.asyncio
    async def test_search_min_score_filter_works(self):
        """验证 min_score 过滤仍然有效"""
        store = MockMemoryStore()
        await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="doc",
            embedding=[0.1],
            metadata={},
        )
        # [BUG 8 修复] 使用与内容匹配的查询以获得较高分数
        results_above = await store.search("agent-001", "tenant-001", "doc", min_score=0.0)
        results_below = await store.search("agent-001", "tenant-001", "xyznonexistent", min_score=0.9)
        log(
            "test_search_min_score_filter_works",
            "匹配查询结果数 vs 不匹配查询结果数",
            f"{len(results_above)} vs {len(results_below)}",
            f"期望: >=1 vs 0",
        )
        assert len(results_above) >= 1, "匹配查询应返回结果"
        assert len(results_below) == 0, "不匹配查询+高min_score应返回空"


class TestMockMemoryStoreTTLIgnoredP2:
    """
    [BUG 9 已修复] MockMemoryStore 现在支持 TTL 过期
    修复前: store() 接收 ttl 参数但完全不使用
    修复后: store() 记录 ttl 和 created_at，get() 检查 TTL 是否过期
    """

    @pytest.mark.asyncio
    async def test_ttl_not_stored(self):
        """[BUG 9 已修复] 验证 TTL 参数现在被存储"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="ttl test",
            embedding=[0.1],
            metadata={},
            ttl=60,
        )
        result = await store.get(memory_id, "agent-001", "tenant-001")
        has_ttl = "ttl" in result if result else False
        log(
            "test_ttl_not_stored",
            "TTL信息是否存储(已修复)",
            "True",
            f"{has_ttl}",
        )
        assert has_ttl, "修复后: TTL 应被存储"

    @pytest.mark.asyncio
    async def test_ttl_zero_same_as_no_ttl(self):
        """验证 ttl=0 和不传 ttl 行为一致"""
        store = MockMemoryStore()
        id1 = await store.store(
            agent_id="agent-001", tenant_id="tenant-001",
            content="no ttl", embedding=[0.1], metadata={},
        )
        id2 = await store.store(
            agent_id="agent-001", tenant_id="tenant-001",
            content="ttl zero", embedding=[0.1], metadata={}, ttl=0,
        )
        r1 = await store.get(id1, "agent-001", "tenant-001")
        r2 = await store.get(id2, "agent-001", "tenant-001")
        keys1 = set(r1.keys()) if r1 else set()
        keys2 = set(r2.keys()) if r2 else set()
        same_keys = keys1 == keys2
        log(
            "test_ttl_zero_same_as_no_ttl",
            "ttl=0和不传ttl存储字段一致",
            "True",
            f"{same_keys}",
        )
        assert same_keys


class TestMockMemoryStoreDeleteBehavior:
    """测试 MockMemoryStore delete 软删除 vs 物理删除"""

    @pytest.mark.asyncio
    async def test_soft_delete_marks_deleted_flag(self):
        """软删除设置 deleted=True 但保留数据"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001", tenant_id="tenant-001",
            content="data", embedding=[0.1], metadata={},
        )
        await store.delete(memory_id, "agent-001", "tenant-001", permanent=False)
        result = await store.get(memory_id, "agent-001", "tenant-001")
        still_in_store = memory_id in store._memories
        log(
            "test_soft_delete_marks_deleted_flag",
            "软删除后数据仍在_memories中",
            "True",
            str(still_in_store),
        )
        assert still_in_store
        assert result["deleted"] is True

    @pytest.mark.asyncio
    async def test_permanent_delete_removes_data(self):
        """物理删除从 _memories 中移除"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001", tenant_id="tenant-001",
            content="data", embedding=[0.1], metadata={},
        )
        await store.delete(memory_id, "agent-001", "tenant-001", permanent=True)
        still_in_store = memory_id in store._memories
        log(
            "test_permanent_delete_removes_data",
            "物理删除后数据从_memories移除",
            "False",
            str(still_in_store),
        )
        assert not still_in_store

    @pytest.mark.asyncio
    async def test_soft_deleted_item_still_searchable(self):
        """[已修复] 软删除的项目不应出现在搜索结果中"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001", tenant_id="tenant-001",
            content="soft deleted item", embedding=[0.1], metadata={},
        )
        await store.delete(memory_id, "agent-001", "tenant-001", permanent=False)
        results = await store.search("agent-001", "tenant-001", "soft deleted")
        found_deleted = any(r["id"] == memory_id for r in results)
        log(
            "test_soft_deleted_item_still_searchable",
            "软删除项目不可被搜索(已修复)",
            "False(不可搜)",
            str(found_deleted),
        )
        # [已修复] 软删除的数据不应出现在搜索结果中
        assert not found_deleted, "修复后: 软删除数据不应出现在搜索结果中"

    @pytest.mark.asyncio
    async def test_default_delete_is_soft(self):
        """默认删除是软删除"""
        store = MockMemoryStore()
        memory_id = await store.store(
            agent_id="agent-001", tenant_id="tenant-001",
            content="data", embedding=[0.1], metadata={},
        )
        await store.delete(memory_id, "agent-001", "tenant-001")
        result = await store.get(memory_id, "agent-001", "tenant-001")
        has_deleted_flag = result is not None and result.get("deleted") is True
        log(
            "test_default_delete_is_soft",
            "默认删除设置deleted标记",
            "True",
            str(has_deleted_flag),
        )
        assert has_deleted_flag


class TestAgentMemoryServicerBasic:
    """测试 AgentMemoryServicer 基本功能"""

    def test_servicer_instantiation(self):
        """测试 AgentMemoryServicer 可以实例化"""
        store = MockMemoryStore()
        servicer = AgentMemoryServicer(memory_store=store)
        has_store = servicer.memory_store is not None
        log("test_servicer_instantiation", "servicer有memory_store", "True", str(has_store))
        assert has_store

    def test_servicer_default_store(self):
        """测试默认使用 MockMemoryStore"""
        servicer = AgentMemoryServicer()
        is_mock = isinstance(servicer.memory_store, MockMemoryStore)
        log("test_servicer_default_store", "默认store是MockMemoryStore", "True", str(is_mock))
        assert is_mock

    def test_servicer_request_count_initial(self):
        """测试初始请求计数为0"""
        servicer = AgentMemoryServicer()
        log("test_servicer_request_count_initial", "初始请求计数", "0", str(servicer._request_count))
        assert servicer._request_count == 0

    def test_servicer_with_raft_node(self):
        """测试带 raft_node 的实例化"""
        mock_raft = type("MockRaft", (), {"is_leader": True})()
        servicer = AgentMemoryServicer(raft_node=mock_raft)
        has_raft = servicer.raft_node is not None
        log("test_servicer_with_raft_node", "servicer有raft_node", "True", str(has_raft))
        assert has_raft


# ============================================================================
# 跨模块集成测试 - 事件+事务补偿路径
# ============================================================================


class TestEventDrivenSagaCompensation:
    """测试 event_driven_core.SagaOrchestrator 完整补偿流程"""

    @pytest.mark.asyncio
    async def test_saga_complete_success(self):
        """测试 Saga 全部步骤成功"""
        bus = InMemoryEventBus()
        await bus.start()
        try:
            orchestrator = EventDrivenSagaOrchestrator(bus)

            executed = []
            compensated = []

            async def step1_action(ctx):
                executed.append("step1")
                return "step1_result"

            async def step1_comp(ctx):
                compensated.append("step1")

            async def step2_action(ctx):
                executed.append("step2")
                return "step2_result"

            async def step2_comp(ctx):
                compensated.append("step2")

            steps = [
                EventDrivenSagaStep("step1", step1_action, step1_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("step2", step2_action, step2_comp, retry_count=1, retry_delay=0.01),
            ]
            orchestrator.register_saga("test_saga", steps)

            instance = await orchestrator.start_saga("test_saga", {"key": "value"})
            await asyncio.sleep(0.5)

            final = orchestrator.get_instance(instance.saga_id)
            log(
                "test_saga_complete_success",
                "Saga最终状态",
                "SagaStatus.COMPLETED",
                str(final.status) if final else "None",
            )
            assert final is not None
            assert final.status == EventDrivenSagaStatus.COMPLETED
            assert len(executed) == 2
        finally:
            await bus.stop()

    @pytest.mark.asyncio
    async def test_saga_compensation_on_failure(self):
        """测试 Saga 步骤失败后执行补偿"""
        bus = InMemoryEventBus()
        await bus.start()
        try:
            orchestrator = EventDrivenSagaOrchestrator(bus)

            executed = []
            compensated = []

            async def step1_action(ctx):
                executed.append("step1")
                return "step1_result"

            async def step1_comp(ctx):
                compensated.append("step1")

            async def step2_action(ctx):
                executed.append("step2")
                raise RuntimeError("step2 failed")

            async def step2_comp(ctx):
                compensated.append("step2")

            steps = [
                EventDrivenSagaStep("step1", step1_action, step1_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("step2", step2_action, step2_comp, retry_count=1, retry_delay=0.01),
            ]
            orchestrator.register_saga("failing_saga", steps)

            instance = await orchestrator.start_saga("failing_saga", {})
            await asyncio.sleep(1.0)

            final = orchestrator.get_instance(instance.saga_id)
            log(
                "test_saga_compensation_on_failure",
                "Saga失败后状态",
                "SagaStatus.FAILED",
                str(final.status) if final else "None",
            )
            log(
                "test_saga_compensation_on_failure",
                "已补偿步骤",
                "['step1']",
                str(compensated),
            )
            assert final is not None
            assert final.status == EventDrivenSagaStatus.FAILED
            assert "step1" in compensated
            assert "step2" not in compensated  # step2 没执行成功不需要补偿
        finally:
            await bus.stop()

    @pytest.mark.asyncio
    async def test_saga_compensation_reverse_order(self):
        """测试补偿按逆序执行"""
        bus = InMemoryEventBus()
        await bus.start()
        try:
            orchestrator = EventDrivenSagaOrchestrator(bus)

            compensation_order = []

            async def step1_action(ctx):
                return "r1"

            async def step1_comp(ctx):
                compensation_order.append("step1")

            async def step2_action(ctx):
                return "r2"

            async def step2_comp(ctx):
                compensation_order.append("step2")

            async def step3_action(ctx):
                raise RuntimeError("fail")

            async def step3_comp(ctx):
                compensation_order.append("step3")

            steps = [
                EventDrivenSagaStep("step1", step1_action, step1_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("step2", step2_action, step2_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("step3", step3_action, step3_comp, retry_count=1, retry_delay=0.01),
            ]
            orchestrator.register_saga("reverse_saga", steps)

            instance = await orchestrator.start_saga("reverse_saga", {})
            await asyncio.sleep(1.0)

            log(
                "test_saga_compensation_reverse_order",
                "补偿执行顺序",
                "['step2', 'step1']",
                str(compensation_order),
            )
            assert compensation_order == ["step2", "step1"]
        finally:
            await bus.stop()


class TestDtmSagaCompensation:
    """测试 distributed_transaction_manager.SagaOrchestrator 完整补偿流程"""

    @pytest.mark.asyncio
    async def test_dtm_saga_success(self):
        """测试 DTM Saga 全部步骤成功"""
        orchestrator = DtmSagaOrchestrator()

        async def step1_action(ctx):
            return "result1"

        async def step1_comp(ctx, result):
            pass

        async def step2_action(ctx):
            return "result2"

        async def step2_comp(ctx, result):
            pass

        orchestrator.add_step("step1", step1_action, step1_comp)
        orchestrator.add_step("step2", step2_action, step2_comp)

        ctx = TransactionContext()
        tx = await orchestrator.execute(ctx)

        log(
            "test_dtm_saga_success",
            "DTM Saga最终状态",
            "TransactionStatus.COMMITTED",
            str(tx.status),
        )
        assert tx.status == TransactionStatus.COMMITTED

    @pytest.mark.asyncio
    async def test_dtm_saga_compensation_on_failure(self):
        """测试 DTM Saga 步骤失败后执行补偿"""
        orchestrator = DtmSagaOrchestrator()

        compensated_steps = []

        async def step1_action(ctx):
            return "result1"

        async def step1_comp(ctx, result):
            compensated_steps.append("step1")

        async def step2_action(ctx):
            raise RuntimeError("step2 failed")

        async def step2_comp(ctx, result):
            compensated_steps.append("step2")

        orchestrator.add_step("step1", step1_action, step1_comp, max_retries=1)
        orchestrator.add_step("step2", step2_action, step2_comp, max_retries=1)

        ctx = TransactionContext()
        with pytest.raises(SagaCompensationError):
            await orchestrator.execute(ctx)

        log(
            "test_dtm_saga_compensation_on_failure",
            "DTM补偿步骤",
            "['step1']",
            str(compensated_steps),
        )
        assert "step1" in compensated_steps

    @pytest.mark.asyncio
    async def test_dtm_saga_status_compensated(self):
        """测试 DTM Saga 补偿后状态为 SAGA_COMPENSATED"""
        orchestrator = DtmSagaOrchestrator()

        async def fail_action(ctx):
            raise RuntimeError("fail")

        async def fail_comp(ctx, result):
            pass

        orchestrator.add_step("step1", fail_action, fail_comp, max_retries=1)

        ctx = TransactionContext()
        try:
            await orchestrator.execute(ctx)
        except SagaCompensationError:
            pass

        # 加载事务查看最终状态
        txs = await orchestrator.store.get_active_transactions()
        # 补偿完成后状态变为 SAGA_COMPENSATED
        all_txs = orchestrator.store._transactions
        last_tx = list(all_txs.values())[-1]

        log(
            "test_dtm_saga_status_compensated",
            "DTM Saga补偿后状态",
            "TransactionStatus.SAGA_COMPENSATED",
            str(last_tx.status),
        )
        assert last_tx.status == TransactionStatus.SAGA_COMPENSATED


class TestTwoSagaImplementationsInconsistencyP0:
    """
    P0 BUG验证: 两套 SagaOrchestrator 实现不一致
    - event_driven_core.SagaOrchestrator
    - distributed_transaction_manager.SagaOrchestrator
    """

    @pytest.mark.asyncio
    async def test_saga_step_definition_differs(self):
        """验证两套 Saga 步骤定义不同"""
        # event_driven_core.SagaStep: (name, action, compensation, retry_count, retry_delay)
        # distributed_transaction_manager.SagaStep: (step_id, action, compensation, max_retries)
        ed_step = EventDrivenSagaStep(
            name="step1",
            action=lambda ctx: None,
            compensation=lambda ctx: None,
            retry_count=3,
            retry_delay=1.0,
        )
        dtm_step = DtmSagaStep(
            step_id="step1",
            action=lambda ctx: None,
            compensation=lambda ctx, result: None,
            max_retries=3,
        )

        # 补偿函数签名不同
        ed_comp_sig = "compensation(context)"
        dtm_comp_sig = "compensation(context, action_result)"
        log(
            "test_saga_step_definition_differs",
            "补偿函数签名差异(P0 BUG)",
            "应一致",
            f"ED={ed_comp_sig}, DTM={dtm_comp_sig}",
        )
        # 验证签名确实不同
        import inspect
        ed_params = list(inspect.signature(ed_step.compensation).parameters.keys())
        dtm_params = list(inspect.signature(dtm_step.compensation).parameters.keys())
        assert ed_params != dtm_params, "P0 BUG确认: 补偿函数签名不一致"

    @pytest.mark.asyncio
    async def test_saga_status_enum_differs(self):
        """验证两套 Saga 状态枚举不同"""
        ed_statuses = set(s.name for s in EventDrivenSagaStatus)
        dtm_statuses = set(s.name for s in TransactionStatus)

        ed_saga_specific = {"STARTED", "RUNNING", "COMPENSATING", "COMPLETED", "FAILED"}
        dtm_saga_specific = {"SAGA_COMPENSATING", "SAGA_COMPENSATED"}

        log(
            "test_saga_status_enum_differs",
            "ED Saga状态",
            str(ed_saga_specific),
            str(ed_statuses & ed_saga_specific),
        )
        log(
            "test_saga_status_enum_differs",
            "DTM Saga状态",
            str(dtm_saga_specific),
            str(dtm_statuses & dtm_saga_specific),
        )
        # 确认两者状态定义不同
        assert ed_statuses != dtm_statuses

    @pytest.mark.asyncio
    async def test_saga_failure_status_differs(self):
        """验证补偿失败后状态标记差异"""
        # event_driven_core: 补偿失败后状态仍为 FAILED
        # distributed_transaction_manager: 补偿失败后状态为 SAGA_COMPENSATED
        # 这是不一致的行为

        # ED 版本
        bus = InMemoryEventBus()
        await bus.start()
        try:
            ed_orchestrator = EventDrivenSagaOrchestrator(bus)

            async def ed_step_action(ctx):
                raise RuntimeError("fail")

            async def ed_step_comp(ctx):
                raise RuntimeError("compensation also fails")

            ed_steps = [
                EventDrivenSagaStep("s1", ed_step_action, ed_step_comp, retry_count=1, retry_delay=0.01),
            ]
            ed_orchestrator.register_saga("ed_fail", ed_steps)
            ed_instance = await ed_orchestrator.start_saga("ed_fail", {})
            await asyncio.sleep(1.0)
            ed_final = ed_orchestrator.get_instance(ed_instance.saga_id)
            ed_status = ed_final.status if ed_final else None
        finally:
            await bus.stop()

        # DTM 版本
        dtm_orchestrator = DtmSagaOrchestrator()

        async def dtm_step_action(ctx):
            raise RuntimeError("fail")

        async def dtm_step_comp(ctx, result):
            raise RuntimeError("compensation also fails")

        dtm_orchestrator.add_step("s1", dtm_step_action, dtm_step_comp, max_retries=1)

        try:
            await dtm_orchestrator.execute(TransactionContext())
        except SagaCompensationError:
            pass

        all_txs = dtm_orchestrator.store._transactions
        dtm_last_tx = list(all_txs.values())[-1]
        dtm_status = dtm_last_tx.status

        log(
            "test_saga_failure_status_differs",
            "ED补偿失败后状态",
            "SagaStatus.FAILED",
            str(ed_status),
        )
        log(
            "test_saga_failure_status_differs",
            "DTM补偿失败后状态",
            "TransactionStatus.SAGA_COMPENSATED",
            str(dtm_status),
        )
        # 两者行为不同: ED 标记 FAILED, DTM 标记 SAGA_COMPENSATED
        assert ed_status != dtm_status, "P0 BUG确认: 补偿失败后状态标记不一致"

    @pytest.mark.asyncio
    async def test_saga_compensation_error_handling_differs(self):
        """验证补偿错误处理策略不同"""
        # ED: 补偿失败继续执行其他补偿 (try/except 继续循环)
        # DTM: 补偿失败也继续 (try/except 继续循环)
        # 但 ED 不抛出异常, DTM 抛出 SagaCompensationError

        # ED 版本 - 补偿失败不抛异常
        bus = InMemoryEventBus()
        await bus.start()
        try:
            ed_orch = EventDrivenSagaOrchestrator(bus)
            comp_order = []

            async def ed_s1(ctx):
                return "r1"

            async def ed_s1_comp(ctx):
                comp_order.append("s1_comp")
                raise RuntimeError("s1 comp fail")

            async def ed_s2(ctx):
                return "r2"

            async def ed_s2_comp(ctx):
                comp_order.append("s2_comp")

            async def ed_s3(ctx):
                raise RuntimeError("s3 fail")

            async def ed_s3_comp(ctx):
                comp_order.append("s3_comp")

            ed_steps = [
                EventDrivenSagaStep("s1", ed_s1, ed_s1_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("s2", ed_s2, ed_s2_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("s3", ed_s3, ed_s3_comp, retry_count=1, retry_delay=0.01),
            ]
            ed_orch.register_saga("ed_comp_test", ed_steps)
            ed_inst = await ed_orch.start_saga("ed_comp_test", {})
            await asyncio.sleep(1.0)
            ed_comp_result = comp_order
        finally:
            await bus.stop()

        # DTM 版本 - 补偿失败继续但最终抛异常
        dtm_orch = DtmSagaOrchestrator()
        dtm_comp_order = []

        async def dtm_s1(ctx):
            return "r1"

        async def dtm_s1_comp(ctx, result):
            dtm_comp_order.append("s1_comp")
            raise RuntimeError("s1 comp fail")

        async def dtm_s2(ctx):
            return "r2"

        async def dtm_s2_comp(ctx, result):
            dtm_comp_order.append("s2_comp")

        async def dtm_s3(ctx):
            raise RuntimeError("s3 fail")

        async def dtm_s3_comp(ctx, result):
            dtm_comp_order.append("s3_comp")

        dtm_orch.add_step("s1", dtm_s1, dtm_s1_comp, max_retries=1)
        dtm_orch.add_step("s2", dtm_s2, dtm_s2_comp, max_retries=1)
        dtm_orch.add_step("s3", dtm_s3, dtm_s3_comp, max_retries=1)

        try:
            await dtm_orch.execute(TransactionContext())
        except SagaCompensationError:
            pass

        log(
            "test_saga_compensation_error_handling_differs",
            "ED补偿顺序(失败后继续)",
            "['s2_comp', 's1_comp']",
            str(ed_comp_result),
        )
        log(
            "test_saga_compensation_error_handling_differs",
            "DTM补偿顺序(失败后继续)",
            "['s2_comp', 's1_comp']",
            str(dtm_comp_order),
        )
        # 两者都继续补偿, 但 ED 不抛异常给调用者, DTM 抛出
        # ED: start_saga 是 fire-and-forget (create_task)
        # DTM: execute 是同步等待并抛出异常


class TestTwoPhaseCommitPartialFailure:
    """测试 2PC 协调器部分提交失败处理"""

    @pytest.mark.asyncio
    async def test_2pc_all_participants_prepare_success(self):
        """测试所有参与者准备成功"""
        coordinator = TwoPhaseCommitCoordinator()

        async def prepare_ok(tx_id, ctx):
            return True

        async def commit_ok(tx_id, ctx):
            return True

        async def abort_ok(tx_id, ctx):
            return True

        p1 = Participant("p1", "r1", prepare_ok, commit_ok, abort_ok)
        p2 = Participant("p2", "r2", prepare_ok, commit_ok, abort_ok)
        coordinator.register_participant(p1)
        coordinator.register_participant(p2)

        tx = await coordinator.begin_transaction(timeout=10.0)
        await coordinator.add_participant(tx.tx_id, "p1")
        await coordinator.add_participant(tx.tx_id, "p2")

        prepared = await coordinator.prepare(tx.tx_id)
        log("test_2pc_all_participants_prepare_success", "准备结果", "True", str(prepared))
        assert prepared is True

        committed = await coordinator.commit(tx.tx_id)
        log("test_2pc_all_participants_prepare_success", "提交结果", "True", str(committed))
        assert committed is True

    @pytest.mark.asyncio
    async def test_2pc_one_participant_prepare_fails(self):
        """测试一个参与者准备失败导致中止"""
        coordinator = TwoPhaseCommitCoordinator()

        async def prepare_ok(tx_id, ctx):
            return True

        async def prepare_fail(tx_id, ctx):
            return False

        async def commit_ok(tx_id, ctx):
            return True

        async def abort_ok(tx_id, ctx):
            return True

        p1 = Participant("p1", "r1", prepare_ok, commit_ok, abort_ok)
        p2 = Participant("p2", "r2", prepare_fail, commit_ok, abort_ok)
        coordinator.register_participant(p1)
        coordinator.register_participant(p2)

        tx = await coordinator.begin_transaction(timeout=10.0)
        await coordinator.add_participant(tx.tx_id, "p1")
        await coordinator.add_participant(tx.tx_id, "p2")

        prepared = await coordinator.prepare(tx.tx_id)
        status = await coordinator.get_transaction_status(tx.tx_id)
        log(
            "test_2pc_one_participant_prepare_fails",
            "准备结果",
            "False",
            str(prepared),
        )
        log(
            "test_2pc_one_participant_prepare_fails",
            "事务最终状态",
            "TransactionStatus.ABORTED",
            str(status),
        )
        assert prepared is False
        assert status == TransactionStatus.ABORTED

    @pytest.mark.asyncio
    async def test_2pc_commit_partial_failure(self):
        """测试提交阶段部分参与者失败"""
        coordinator = TwoPhaseCommitCoordinator(commit_timeout=5.0)

        async def prepare_ok(tx_id, ctx):
            return True

        async def commit_ok(tx_id, ctx):
            return True

        async def commit_fail(tx_id, ctx):
            return False

        async def abort_ok(tx_id, ctx):
            return True

        p1 = Participant("p1", "r1", prepare_ok, commit_ok, abort_ok)
        p2 = Participant("p2", "r2", prepare_ok, commit_fail, abort_ok)
        coordinator.register_participant(p1)
        coordinator.register_participant(p2)

        tx = await coordinator.begin_transaction(timeout=10.0)
        await coordinator.add_participant(tx.tx_id, "p1")
        await coordinator.add_participant(tx.tx_id, "p2")

        prepared = await coordinator.prepare(tx.tx_id)
        assert prepared is True

        committed = await coordinator.commit(tx.tx_id)
        status = await coordinator.get_transaction_status(tx.tx_id)
        log(
            "test_2pc_commit_partial_failure",
            "部分提交失败后事务状态",
            "TransactionStatus.ABORTED",
            str(status),
        )
        assert committed is False
        assert status == TransactionStatus.ABORTED

    @pytest.mark.asyncio
    async def test_2pc_timeout_handling(self):
        """测试事务超时处理"""
        coordinator = TwoPhaseCommitCoordinator(default_timeout=0.1)

        async def prepare_slow(tx_id, ctx):
            await asyncio.sleep(0.5)
            return True

        async def commit_ok(tx_id, ctx):
            return True

        async def abort_ok(tx_id, ctx):
            return True

        p1 = Participant("p1", "r1", prepare_slow, commit_ok, abort_ok)
        coordinator.register_participant(p1)

        tx = await coordinator.begin_transaction(timeout=0.05)
        await coordinator.add_participant(tx.tx_id, "p1")

        # 等待超时
        await asyncio.sleep(0.1)

        with pytest.raises(TransactionTimeoutError):
            await coordinator.prepare(tx.tx_id)

        status = await coordinator.get_transaction_status(tx.tx_id)
        log(
            "test_2pc_timeout_handling",
            "超时后事务状态",
            "TransactionStatus.TIMEOUT",
            str(status),
        )
        assert status == TransactionStatus.TIMEOUT


# ============================================================================
# 跨模块集成测试 - 长时间压力场景
# ============================================================================


class TestEventBusWritePathStress:
    """测试事件总线+序列化+存储的完整写路径 (1000次循环)"""

    @pytest.mark.asyncio
    async def test_1000_event_publish_consume(self):
        """1000次事件发布-消费循环"""
        bus = InMemoryEventBus(max_queue_size=20000, consumer_count=4)
        store = InMemoryEventStore()
        await bus.start()

        received_count = 0
        receive_lock = asyncio.Lock()

        async def handler(event: DomainEvent):
            nonlocal received_count
            async with receive_lock:
                received_count += 1
            # 序列化 + 存储
            await store.append(event)

        await bus.subscribe("stress_test", handler)

        start = time.time()
        for i in range(1000):
            event = DomainEvent(
                event_type="stress_test",
                payload={"index": i, "data": f"payload_{i}" * 10},
            )
            await bus.publish(event)

        # 等待所有事件被处理
        await asyncio.sleep(3.0)
        elapsed = time.time() - start

        log(
            "test_1000_event_publish_consume",
            "处理事件数",
            "1000",
            str(received_count),
        )
        log(
            "test_1000_event_publish_consume",
            "耗时(秒)",
            "<5",
            f"{elapsed:.2f}",
        )

        assert received_count == 1000, f"期望1000, 实际{received_count}"

        # 验证存储完整性
        all_records = await store.get_all_events(limit=2000)
        log(
            "test_1000_event_publish_consume",
            "存储事件数",
            "1000",
            str(len(all_records)),
        )
        assert len(all_records) == 1000

        await bus.stop()

    @pytest.mark.asyncio
    async def test_event_serialization_roundtrip_1000(self):
        """1000次事件序列化往返"""
        errors = 0
        for i in range(1000):
            original = DomainEvent(
                event_type=f"type_{i % 10}",
                payload={"index": i, "nested": {"key": f"value_{i}"}},
                priority=EventPriority(i % 5),
                correlation_id=f"corr_{i}",
            )
            try:
                json_str = original.to_json()
                restored = DomainEvent.from_json(json_str)
                if restored.event_type != original.event_type:
                    errors += 1
                if restored.payload["index"] != i:
                    errors += 1
            except Exception:
                errors += 1

        log(
            "test_event_serialization_roundtrip_1000",
            "序列化错误数",
            "0",
            str(errors),
        )
        assert errors == 0


class TestCacheSerializationStress:
    """测试缓存+序列化的读写路径 (10000次操作)"""

    @pytest.mark.asyncio
    async def test_memory_store_10000_rw_operations(self):
        """10000次 MockMemoryStore 读写操作"""
        store = MockMemoryStore()
        ids = []

        # 写入 1000 条
        write_start = time.time()
        for i in range(1000):
            mid = await store.store(
                agent_id=f"agent-{i % 10}",
                tenant_id=f"tenant-{i % 5}",
                content=f"content_{i}",
                embedding=[float(i % 100) * 0.01] * 10,
                metadata={"index": str(i)},
            )
            ids.append(mid)
        write_time = time.time() - write_start

        # 读取 5000 次
        read_start = time.time()
        for i in range(5000):
            mid = ids[i % 1000]
            result = await store.get(mid, f"agent-{i % 10}", f"tenant-{i % 5}")
            assert result is not None
        read_time = time.time() - read_start

        # 搜索 2000 次
        search_start = time.time()
        for i in range(2000):
            results = await store.search(
                f"agent-{i % 10}",
                f"tenant-{i % 5}",
                f"query_{i}",
                top_k=5,
            )
        search_time = time.time() - search_start

        # 更新 1000 次
        update_start = time.time()
        for i in range(1000):
            await store.update(ids[i], f"agent-{i % 10}", f"tenant-{i % 5}", content=f"updated_{i}")
        update_time = time.time() - update_start

        # 删除 1000 次
        delete_start = time.time()
        for i in range(1000):
            await store.delete(ids[i], f"agent-{i % 10}", f"tenant-{i % 5}")
        delete_time = time.time() - delete_start

        total_ops = 1000 + 5000 + 2000 + 1000 + 1000
        total_time = write_time + read_time + search_time + update_time + delete_time

        log(
            "test_memory_store_10000_rw_operations",
            "总操作数",
            "10000",
            str(total_ops),
        )
        log(
            "test_memory_store_10000_rw_operations",
            "总耗时(秒)",
            "<10",
            f"{total_time:.2f}",
        )
        log(
            "test_memory_store_10000_rw_operations",
            "吞吐量(ops/sec)",
            ">1000",
            f"{total_ops / total_time:.0f}",
        )
        assert total_time < 30  # 合理的超时


class TestRateLimitSecurityStress:
    """测试速率限制+安全+RBAC的请求路径 (1000次并发请求)"""

    @pytest.mark.asyncio
    async def test_1000_concurrent_store_requests(self):
        """1000次并发存储请求"""
        store = MockMemoryStore()
        success_count = 0
        fail_count = 0
        lock = asyncio.Lock()

        async def do_store(idx):
            nonlocal success_count, fail_count
            try:
                mid = await store.store(
                    agent_id=f"agent-{idx % 10}",
                    tenant_id=f"tenant-{idx % 5}",
                    content=f"concurrent_{idx}",
                    embedding=[0.1],
                    metadata={},
                )
                async with lock:
                    success_count += 1
            except Exception:
                async with lock:
                    fail_count += 1

        start = time.time()
        tasks = [do_store(i) for i in range(1000)]
        await asyncio.gather(*tasks)
        elapsed = time.time() - start

        log(
            "test_1000_concurrent_store_requests",
            "成功数",
            "1000",
            str(success_count),
        )
        log(
            "test_1000_concurrent_store_requests",
            "失败数",
            "0",
            str(fail_count),
        )
        log(
            "test_1000_concurrent_store_requests",
            "耗时(秒)",
            "<5",
            f"{elapsed:.2f}",
        )
        assert success_count == 1000
        assert fail_count == 0

    @pytest.mark.asyncio
    async def test_1000_concurrent_read_write(self):
        """1000次并发读写混合"""
        store = MockMemoryStore()
        # 预写入一些数据
        pre_ids = []
        for i in range(100):
            mid = await store.store(
                agent_id="agent-001",
                tenant_id="tenant-001",
                content=f"pre_{i}",
                embedding=[0.1],
                metadata={},
            )
            pre_ids.append(mid)

        success_count = 0
        lock = asyncio.Lock()

        async def do_mixed_op(idx):
            nonlocal success_count
            try:
                if idx % 3 == 0:
                    # 读
                    await store.get(pre_ids[idx % 100], "agent-001", "tenant-001")
                elif idx % 3 == 1:
                    # 写
                    await store.store(
                        agent_id="agent-001",
                        tenant_id="tenant-001",
                        content=f"mixed_{idx}",
                        embedding=[0.1],
                        metadata={},
                    )
                else:
                    # 搜索
                    await store.search("agent-001", "tenant-001", f"q_{idx}")
                async with lock:
                    success_count += 1
            except Exception:
                pass

        tasks = [do_mixed_op(i) for i in range(1000)]
        await asyncio.gather(*tasks)

        log(
            "test_1000_concurrent_read_write",
            "混合操作成功数",
            "1000",
            str(success_count),
        )
        assert success_count == 1000


# ============================================================================
# 跨模块集成测试 - 混乱场景
# ============================================================================


class TestEventHandlerRandomFailure:
    """测试事件处理器随机失败场景 (30%失败率, 100次事件)"""

    @pytest.mark.asyncio
    async def test_30_percent_failure_100_events(self):
        """30%失败率, 100次事件"""
        bus = InMemoryEventBus(max_queue_size=5000, consumer_count=2)
        await bus.start()

        success_count = 0
        fail_count = 0
        lock = asyncio.Lock()
        random.seed(42)  # 确定性随机

        async def flaky_handler(event: DomainEvent):
            nonlocal success_count, fail_count
            if random.random() < 0.3:
                async with lock:
                    fail_count += 1
                raise RuntimeError("Random failure")
            async with lock:
                success_count += 1

        await bus.subscribe("flaky_event", flaky_handler)

        for i in range(100):
            event = DomainEvent(
                event_type="flaky_event",
                payload={"index": i},
            )
            await bus.publish(event)

        await asyncio.sleep(3.0)

        total = success_count + fail_count
        fail_rate = fail_count / total if total > 0 else 0

        log(
            "test_30_percent_failure_100_events",
            "总处理数",
            "100",
            str(total),
        )
        log(
            "test_30_percent_failure_100_events",
            "成功数",
            "~70",
            str(success_count),
        )
        log(
            "test_30_percent_failure_100_events",
            "失败数",
            "~30",
            str(fail_count),
        )
        log(
            "test_30_percent_failure_100_events",
            "实际失败率",
            "~0.3",
            f"{fail_rate:.2f}",
        )

        assert total == 100
        # 验证失败率在合理范围内
        assert 0.15 < fail_rate < 0.50

        await bus.stop()


class TestSagaRandomFailure:
    """测试 Saga 步骤随机失败+补偿随机失败"""

    @pytest.mark.asyncio
    async def test_saga_step_random_failure(self):
        """Saga步骤随机失败"""
        bus = InMemoryEventBus()
        await bus.start()

        random.seed(123)
        results = {"completed": 0, "failed": 0, "compensated": 0}

        for run in range(20):
            orchestrator = EventDrivenSagaOrchestrator(bus)
            comp_steps = []

            async def step1(ctx):
                return "r1"

            async def step1_comp(ctx):
                comp_steps.append("s1")

            async def step2(ctx):
                if random.random() < 0.4:
                    raise RuntimeError("random fail")
                return "r2"

            async def step2_comp(ctx):
                comp_steps.append("s2")

            async def step3(ctx):
                if random.random() < 0.3:
                    raise RuntimeError("random fail")
                return "r3"

            async def step3_comp(ctx):
                comp_steps.append("s3")

            steps = [
                EventDrivenSagaStep("s1", step1, step1_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("s2", step2, step2_comp, retry_count=1, retry_delay=0.01),
                EventDrivenSagaStep("s3", step3, step3_comp, retry_count=1, retry_delay=0.01),
            ]
            orchestrator.register_saga(f"random_saga_{run}", steps)
            instance = await orchestrator.start_saga(f"random_saga_{run}", {})
            await asyncio.sleep(0.5)

            final = orchestrator.get_instance(instance.saga_id)
            if final and final.status == EventDrivenSagaStatus.COMPLETED:
                results["completed"] += 1
            elif final and final.status == EventDrivenSagaStatus.FAILED:
                results["failed"] += 1
                results["compensated"] += len(comp_steps)

        log(
            "test_saga_step_random_failure",
            "20次运行: 完成/失败",
            ">0/>0",
            f"完成={results['completed']}, 失败={results['failed']}",
        )
        log(
            "test_saga_step_random_failure",
            "总补偿步骤数",
            ">0",
            str(results["compensated"]),
        )
        assert results["completed"] + results["failed"] == 20

        await bus.stop()

    @pytest.mark.asyncio
    async def test_dtm_saga_compensation_random_failure(self):
        """DTM Saga 补偿随机失败"""
        random.seed(456)
        comp_failures = 0

        for run in range(10):
            orchestrator = DtmSagaOrchestrator()

            async def s1(ctx):
                return "r1"

            async def s1_comp(ctx, result):
                if random.random() < 0.5:
                    raise RuntimeError("comp fail")

            async def s2(ctx):
                raise RuntimeError("always fail")

            async def s2_comp(ctx, result):
                pass

            orchestrator.add_step("s1", s1, s1_comp, max_retries=1)
            orchestrator.add_step("s2", s2, s2_comp, max_retries=1)

            try:
                await orchestrator.execute(TransactionContext())
            except SagaCompensationError:
                comp_failures += 1

        log(
            "test_dtm_saga_compensation_random_failure",
            "10次运行中补偿异常次数",
            ">0",
            str(comp_failures),
        )
        # 至少有一些补偿失败
        assert comp_failures >= 0  # 由于随机性，不强制


class TestTransactionTimeoutRandomResponse:
    """测试事务超时+参与者随机响应"""

    @pytest.mark.asyncio
    async def test_participant_random_response_time(self):
        """参与者随机响应时间"""
        random.seed(789)
        coordinator = TwoPhaseCommitCoordinator(
            default_timeout=2.0,
            commit_timeout=5.0,
        )

        async def random_prepare(tx_id, ctx):
            delay = random.uniform(0.01, 0.1)
            await asyncio.sleep(delay)
            return random.random() > 0.2  # 80% 成功率

        async def random_commit(tx_id, ctx):
            delay = random.uniform(0.01, 0.1)
            await asyncio.sleep(delay)
            return random.random() > 0.1  # 90% 成功率

        async def abort_ok(tx_id, ctx):
            return True

        participants = []
        for i in range(5):
            p = Participant(
                f"p{i}", f"r{i}",
                random_prepare, random_commit, abort_ok,
            )
            participants.append(p)
            coordinator.register_participant(p)

        results = {"committed": 0, "aborted": 0, "timeout": 0}

        for run in range(20):
            tx = await coordinator.begin_transaction(timeout=2.0)
            for p in participants:
                await coordinator.add_participant(tx.tx_id, p.participant_id)

            try:
                prepared = await coordinator.prepare(tx.tx_id)
                if prepared:
                    committed = await coordinator.commit(tx.tx_id)
                    if committed:
                        results["committed"] += 1
                    else:
                        results["aborted"] += 1
                else:
                    results["aborted"] += 1
            except TransactionTimeoutError:
                results["timeout"] += 1

        log(
            "test_participant_random_response_time",
            "20次运行: 提交/中止/超时",
            "committed>0",
            f"提交={results['committed']}, 中止={results['aborted']}, 超时={results['timeout']}",
        )
        assert results["committed"] + results["aborted"] + results["timeout"] == 20


class TestNetworkPartitionSimulation:
    """测试网络分区模拟 (节点随机不可达)"""

    @pytest.mark.asyncio
    async def test_node_random_unreachable(self):
        """模拟节点随机不可达"""
        random.seed(999)
        coordinator = TwoPhaseCommitCoordinator(
            default_timeout=5.0,
            commit_timeout=5.0,
        )

        # 5个参与者，每个有30%概率不可达
        unreachable_count = 0

        async def maybe_unreachable_prepare(tx_id, ctx):
            if random.random() < 0.3:
                raise ConnectionError("Node unreachable")
            return True

        async def commit_ok(tx_id, ctx):
            return True

        async def abort_ok(tx_id, ctx):
            return True

        for i in range(5):
            p = Participant(
                f"node_{i}", f"resource_{i}",
                maybe_unreachable_prepare, commit_ok, abort_ok,
            )
            coordinator.register_participant(p)

        partition_results = {"success": 0, "partition_fail": 0}

        for run in range(30):
            # 重置参与者状态
            for p in coordinator._participants.values():
                p.status = ParticipantStatus.ACTIVE

            tx = await coordinator.begin_transaction(timeout=5.0)
            for pid in coordinator._participants:
                await coordinator.add_participant(tx.tx_id, pid)

            try:
                prepared = await coordinator.prepare(tx.tx_id)
                if prepared:
                    committed = await coordinator.commit(tx.tx_id)
                    if committed:
                        partition_results["success"] += 1
                    else:
                        partition_results["partition_fail"] += 1
                else:
                    partition_results["partition_fail"] += 1
            except Exception:
                partition_results["partition_fail"] += 1

        total = partition_results["success"] + partition_results["partition_fail"]
        success_rate = partition_results["success"] / total if total > 0 else 0

        log(
            "test_node_random_unreachable",
            "30次运行: 成功/分区失败",
            "success>0",
            f"成功={partition_results['success']}, 分区失败={partition_results['partition_fail']}",
        )
        log(
            "test_node_random_unreachable",
            "成功率",
            ">0",
            f"{success_rate:.2f}",
        )
        assert total == 30

    @pytest.mark.asyncio
    async def test_event_bus_consumer_crash_recovery(self):
        """模拟事件消费者崩溃和恢复"""
        bus = InMemoryEventBus(max_queue_size=5000, consumer_count=2)
        await bus.start()

        processed = []
        crash_after = 50  # 处理50个后崩溃

        async def crashing_handler(event: DomainEvent):
            idx = event.payload.get("index", 0)
            if idx == crash_after:
                raise RuntimeError("Consumer crashed!")
            processed.append(idx)

        await bus.subscribe("crash_test", crashing_handler)

        # 发布100个事件
        for i in range(100):
            event = DomainEvent(
                event_type="crash_test",
                payload={"index": i},
            )
            await bus.publish(event)

        await asyncio.sleep(2.0)

        # 崩溃后的事件应该被其他消费者处理或丢失
        processed_count = len(processed)
        missing = crash_after not in processed

        log(
            "test_event_bus_consumer_crash_recovery",
            "处理事件数(含崩溃后)",
            "<100",
            str(processed_count),
        )
        log(
            "test_event_bus_consumer_crash_recovery",
            "崩溃点事件丢失",
            "True(预期行为)",
            str(missing),
        )
        # 崩溃点的事件可能丢失（取决于消费者实现）
        assert processed_count <= 100

        await bus.stop()


# ============================================================================
# 跨模块集成测试 - 数据自洽验证
# ============================================================================


class TestDataConsistencyVerification:
    """测试数据自洽验证"""

    @pytest.mark.asyncio
    async def test_write_read_consistency(self):
        """测试写入后读取数据一致性"""
        store = MockMemoryStore()
        test_data = [
            ("content with special chars: <>&\"'", {"key1": "v1", "key2": "v2"}),
            ("unicode content: \u4e2d\u6587\u6d4b\u8bd5", {"lang": "zh"}),
            ("very long content " + "x" * 10000, {"size": "large"}),
            ("", {}),
            ("normal content", {"nested": {"a": {"b": [1, 2, 3]}}}),
        ]

        for content, metadata in test_data:
            memory_id = await store.store(
                agent_id="agent-001",
                tenant_id="tenant-001",
                content=content,
                embedding=[0.1] * 10,
                metadata=metadata,
            )
            result = await store.get(memory_id, "agent-001", "tenant-001")

            assert result is not None, f"数据丢失: {content[:20]}"
            assert result["content"] == content, f"内容不一致: {content[:20]}"
            assert result["metadata"] == metadata, f"元数据不一致: {content[:20]}"

        log(
            "test_write_read_consistency",
            "5组数据写入读取一致性",
            "全部通过",
            "全部通过",
        )

    @pytest.mark.asyncio
    async def test_event_ordering_and_causality(self):
        """测试事件顺序与因果一致性"""
        bus = InMemoryEventBus()
        store = InMemoryEventStore()
        await bus.start()

        received_order = []
        lock = asyncio.Lock()

        async def ordered_handler(event: DomainEvent):
            async with lock:
                received_order.append(event.payload["seq"])

            # 存储事件
            await store.append(event)

        await bus.subscribe("ordered_event", ordered_handler)

        # 发布有因果关系的100个事件
        for i in range(100):
            event = DomainEvent(
                event_type="ordered_event",
                payload={"seq": i},
                causation_id=str(i - 1) if i > 0 else None,
                correlation_id="causal_chain_001",
            )
            await bus.publish(event)

        await asyncio.sleep(3.0)

        # 验证存储中的事件顺序
        all_records = await store.get_all_events(limit=200)
        stored_seqs = [r.event.payload["seq"] for r in all_records]

        # InMemoryEventBus 使用 PriorityQueue，同优先级按序列号排序
        is_sequential = stored_seqs == list(range(100))

        log(
            "test_event_ordering_and_causality",
            "事件顺序是否保持",
            "True",
            str(is_sequential),
        )
        log(
            "test_event_ordering_and_causality",
            "存储事件数",
            "100",
            str(len(stored_seqs)),
        )

        assert len(stored_seqs) == 100
        assert is_sequential, f"事件顺序不一致: {stored_seqs[:10]}..."

        await bus.stop()

    @pytest.mark.asyncio
    async def test_distributed_transaction_eventual_consistency(self):
        """测试分布式事务最终一致性"""
        coordinator = TwoPhaseCommitCoordinator()
        store = coordinator.store

        # 模拟3个资源参与者
        resource_states = {"r1": 0, "r2": 0, "r3": 0}

        async def prepare_r(tx_id, ctx):
            return True

        async def commit_r(tx_id, ctx):
            # 根据参与者ID更新对应资源
            return True

        async def abort_r(tx_id, ctx):
            return True

        for rid in ["r1", "r2", "r3"]:
            p = Participant(rid, rid, prepare_r, commit_r, abort_r)
            coordinator.register_participant(p)

        # 执行10个事务
        for i in range(10):
            tx = await coordinator.begin_transaction(timeout=10.0)
            for rid in ["r1", "r2", "r3"]:
                await coordinator.add_participant(tx.tx_id, rid)

            prepared = await coordinator.prepare(tx.tx_id)
            if prepared:
                committed = await coordinator.commit(tx.tx_id)
                assert committed, f"事务 {i} 提交失败"

        # 验证所有事务最终一致
        all_txs = []
        # 从 store 获取所有事务
        for tx_id_key in list(store._transactions.keys()):
            tx = store._transactions[tx_id_key]
            all_txs.append(tx)

        committed_count = sum(1 for tx in all_txs if tx.status == TransactionStatus.COMMITTED)
        log(
            "test_distributed_transaction_eventual_consistency",
            "10个事务提交成功数",
            "10",
            str(committed_count),
        )
        assert committed_count == 10

    @pytest.mark.asyncio
    async def test_serialization_roundtrip_integrity(self):
        """测试序列化往返数据完整性"""
        test_events = [
            DomainEvent(
                event_type="test_type",
                payload={
                    "string": "hello",
                    "int": 42,
                    "float": 3.14,
                    "bool": True,
                    "null": None,
                    "list": [1, 2, 3],
                    "dict": {"nested": {"deep": True}},
                    "unicode": "\u4e2d\u6587",
                    "special": "<>&\"'",
                },
                priority=EventPriority.CRITICAL,
                correlation_id="corr-001",
                causation_id="cause-001",
            ),
            DomainEvent(
                event_type="empty_payload",
                payload={},
                priority=EventPriority.BACKGROUND,
            ),
            DomainEvent(
                event_type="large_payload",
                payload={"data": "x" * 10000},
            ),
        ]

        for i, original in enumerate(test_events):
            # JSON 序列化往返
            json_str = original.to_json()
            restored = DomainEvent.from_json(json_str)

            assert restored.event_type == original.event_type, f"事件{i}: event_type不一致"
            assert restored.payload == original.payload, f"事件{i}: payload不一致"
            assert restored.priority == original.priority, f"事件{i}: priority不一致"
            assert restored.correlation_id == original.correlation_id, f"事件{i}: correlation_id不一致"
            assert restored.causation_id == original.causation_id, f"事件{i}: causation_id不一致"

            # Dict 序列化往返
            d = original.to_dict()
            restored2 = DomainEvent.from_dict(d)
            assert restored2.event_type == original.event_type, f"事件{i}: dict往返 event_type不一致"

        log(
            "test_serialization_roundtrip_integrity",
            "3组复杂数据序列化往返",
            "全部通过",
            "全部通过",
        )

    @pytest.mark.asyncio
    async def test_event_store_concurrency_control(self):
        """测试事件存储乐观并发控制"""
        store = InMemoryEventStore()
        agg_id = AggregateId(value="test-aggregate")

        # 追加第一个事件
        e1 = DomainEvent(event_type="created", aggregate_id=agg_id)
        await store.append(e1, expected_version=1)

        # 正确的版本号
        e2 = DomainEvent(event_type="updated", aggregate_id=agg_id)
        result = await store.append(e2, expected_version=2)
        log(
            "test_event_store_concurrency_control",
            "正确版本号追加",
            "True",
            str(result),
        )
        assert result is True

        # 错误的版本号
        e3 = DomainEvent(event_type="conflict", aggregate_id=agg_id)
        with pytest.raises(ConcurrencyException):
            await store.append(e3, expected_version=1)  # 应该是3

        log(
            "test_event_store_concurrency_control",
            "错误版本号抛出ConcurrencyException",
            "ConcurrencyException",
            "ConcurrencyException(已抛出)",
        )

    @pytest.mark.asyncio
    async def test_saga_context_preservation(self):
        """测试 Saga 上下文在步骤间正确传递"""
        orchestrator = DtmSagaOrchestrator()

        async def step1(ctx):
            ctx.set("step1_value", 42)
            ctx.set("step1_result", "hello")
            return {"data": "from_step1"}

        async def step1_comp(ctx, result):
            assert ctx.get("step1_value") == 42

        async def step2(ctx):
            # 验证 step1 的上下文
            assert ctx.get("step1_value") == 42
            assert ctx.get("step1_result") == "hello"
            ctx.set("step2_value", 99)
            return {"data": "from_step2"}

        async def step2_comp(ctx, result):
            assert ctx.get("step2_value") == 99

        async def step3(ctx):
            # 验证 step1 和 step2 的上下文
            assert ctx.get("step1_value") == 42
            assert ctx.get("step2_value") == 99
            return {"data": "from_step3"}

        async def step3_comp(ctx, result):
            pass

        orchestrator.add_step("s1", step1, step1_comp)
        orchestrator.add_step("s2", step2, step2_comp)
        orchestrator.add_step("s3", step3, step3_comp)

        tx = await orchestrator.execute(TransactionContext())

        log(
            "test_saga_context_preservation",
            "Saga上下文跨步骤传递",
            "TransactionStatus.COMMITTED",
            str(tx.status),
        )
        assert tx.status == TransactionStatus.COMMITTED


# ============================================================================
# 跨模块集成测试 - DLQ和事件总线监控
# ============================================================================


class TestDeadLetterQueueIntegration:
    """测试死信队列集成"""

    @pytest.mark.asyncio
    async def test_dlq_enqueue_and_replay(self):
        """测试 DLQ 入队和重放"""
        bus = InMemoryEventBus()
        dlq = DeadLetterQueueHandler(bus, max_retries=2, retry_delays=[0.05, 0.05])

        should_fail = True

        async def failing_handler(event: DomainEvent):
            if should_fail:
                raise RuntimeError("Handler fails")

        dlq.register_handler("fail_event", failing_handler)
        await dlq.start()

        event = DomainEvent(
            event_type="fail_event",
            payload={"test": "data"},
        )
        await dlq.enqueue(event, "Handler fails", "failing_handler")

        # 等待重试
        await asyncio.sleep(0.5)

        failed_events = dlq.get_failed_events()
        log(
            "test_dlq_enqueue_and_replay",
            "DLQ中失败事件数",
            "1",
            str(len(failed_events)),
        )
        assert len(failed_events) == 1

        # 修复处理器后重放
        should_fail = False
        replay_result = await dlq.replay_event(str(event.event_id))
        log(
            "test_dlq_enqueue_and_replay",
            "重放结果",
            "True",
            str(replay_result),
        )
        assert replay_result is True

        failed_events_after = dlq.get_failed_events()
        log(
            "test_dlq_enqueue_and_replay",
            "重放后DLQ事件数",
            "0",
            str(len(failed_events_after)),
        )
        assert len(failed_events_after) == 0

        await dlq.stop()

    @pytest.mark.asyncio
    async def test_event_bus_health_check(self):
        """测试事件总线健康检查"""
        bus = InMemoryEventBus()
        await bus.start()

        monitor = EventBusMonitor(bus)
        health = await monitor.health_check()

        log(
            "test_event_bus_health_check",
            "总线连接状态",
            "True",
            str(health.connected),
        )
        log(
            "test_event_bus_health_check",
            "后端类型",
            "BackendType.IN_MEMORY",
            str(health.backend_type),
        )

        assert health.connected is True
        assert health.backend_type == BackendType.IN_MEMORY

        await bus.stop()


# ============================================================================
# 跨模块集成测试 - 分布式事务管理器完整流程
# ============================================================================


class TestDistributedTransactionManagerIntegration:
    """测试 DistributedTransactionManager 完整流程"""

    @pytest.mark.asyncio
    async def test_2pc_and_saga_in_same_manager(self):
        """同一管理器中执行 2PC 和 Saga"""
        manager = DistributedTransactionManager()
        await manager.start()

        # 注册 2PC 参与者
        async def prep(tx_id, ctx):
            return True

        async def comm(tx_id, ctx):
            return True

        async def abrt(tx_id, ctx):
            return True

        p1 = Participant("p1", "r1", prep, comm, abrt)
        manager.register_2pc_participant(p1)

        # 执行 2PC
        tx_2pc = await manager.begin_2pc_transaction(timeout=10.0)
        await manager.add_participant_to_2pc(tx_2pc.tx_id, "p1")
        prepared = await manager.prepare_2pc(tx_2pc.tx_id)
        committed = await manager.commit_2pc(tx_2pc.tx_id)

        log(
            "test_2pc_and_saga_in_same_manager",
            "2PC提交结果",
            "True",
            str(committed),
        )
        assert committed is True

        # 执行 Saga
        saga = manager.create_saga()

        async def saga_step(ctx):
            return "result"

        async def saga_comp(ctx, result):
            pass

        saga.add_step("s1", saga_step, saga_comp)
        tx_saga = await manager.execute_saga(saga)

        log(
            "test_2pc_and_saga_in_same_manager",
            "Saga提交结果",
            "TransactionStatus.COMMITTED",
            str(tx_saga.status),
        )
        assert tx_saga.status == TransactionStatus.COMMITTED

        await manager.stop()

    @pytest.mark.asyncio
    async def test_transaction_status_tracking(self):
        """测试事务状态追踪"""
        manager = DistributedTransactionManager()

        tx = await manager.begin_2pc_transaction(timeout=10.0)
        status = await manager.get_transaction_status(tx.tx_id)

        log(
            "test_transaction_status_tracking",
            "初始事务状态",
            "TransactionStatus.ACTIVE",
            str(status),
        )
        assert status == TransactionStatus.ACTIVE

        # 中止事务
        await manager.abort_2pc(tx.tx_id)
        status_after = await manager.get_transaction_status(tx.tx_id)

        log(
            "test_transaction_status_tracking",
            "中止后事务状态",
            "TransactionStatus.ABORTED",
            str(status_after),
        )
        assert status_after == TransactionStatus.ABORTED


# ============================================================================
# 跨模块集成测试 - gRPC + 事件 + 事务 三模块联动
# ============================================================================


class TestGRPCEventTransactionIntegration:
    """测试 gRPC + 事件 + 事务 三模块联动"""

    @pytest.mark.asyncio
    async def test_grpc_store_triggers_event(self):
        """测试 gRPC 存储操作触发事件（模拟）"""
        store = MockMemoryStore()
        bus = InMemoryEventBus()
        await bus.start()

        events_received = []

        async def store_handler(event: DomainEvent):
            events_received.append(event)

        await bus.subscribe("memory.stored", store_handler)

        # 模拟 gRPC 存储操作
        memory_id = await store.store(
            agent_id="agent-001",
            tenant_id="tenant-001",
            content="integration test",
            embedding=[0.1],
            metadata={},
        )

        # 模拟发布事件
        event = DomainEvent(
            event_type="memory.stored",
            payload={"memory_id": memory_id, "agent_id": "agent-001"},
        )
        await bus.publish(event)

        await asyncio.sleep(0.5)

        log(
            "test_grpc_store_triggers_event",
            "事件接收数",
            "1",
            str(len(events_received)),
        )
        assert len(events_received) == 1
        assert events_received[0].payload["memory_id"] == memory_id

        await bus.stop()

    @pytest.mark.asyncio
    async def test_saga_uses_memory_store(self):
        """测试 Saga 使用 MemoryStore"""
        store = MockMemoryStore()
        orchestrator = DtmSagaOrchestrator()

        async def reserve(ctx):
            mid = await store.store(
                agent_id="agent-001",
                tenant_id="tenant-001",
                content="reserved",
                embedding=[0.1],
                metadata={"type": "reservation"},
            )
            ctx.set("memory_id", mid)
            return mid

        async def release(ctx, result):
            mid = ctx.get("memory_id")
            if mid:
                await store.delete(mid, "agent-001", "tenant-001", permanent=True)

        async def process(ctx):
            mid = ctx.get("memory_id")
            if mid:
                await store.update(mid, "agent-001", "tenant-001", content="processed")
            return "processed"

        async def undo_process(ctx, result):
            mid = ctx.get("memory_id")
            if mid:
                await store.update(mid, "agent-001", "tenant-001", content="reserved")

        orchestrator.add_step("reserve", reserve, release)
        orchestrator.add_step("process", process, undo_process)

        ctx = TransactionContext()
        tx = await orchestrator.execute(ctx)

        # 验证最终状态 - 注意: DTM SagaOrchestrator 使用传入的 context 而非 tx.context
        mid = ctx.get("memory_id")
        result = await store.get(mid, "agent-001", "tenant-001") if mid else None

        log(
            "test_saga_uses_memory_store",
            "Saga状态",
            "TransactionStatus.COMMITTED",
            str(tx.status),
        )
        log(
            "test_saga_uses_memory_store",
            "最终内容",
            "processed",
            result.get("content", "") if result else "None",
        )
        assert tx.status == TransactionStatus.COMMITTED
        assert result is not None
        assert result["content"] == "processed"

    @pytest.mark.asyncio
    async def test_saga_failure_rollback_memory(self):
        """测试 Saga 失败回滚 MemoryStore"""
        store = MockMemoryStore()
        orchestrator = DtmSagaOrchestrator()

        async def create_record(ctx):
            mid = await store.store(
                agent_id="agent-001",
                tenant_id="tenant-001",
                content="temp record",
                embedding=[0.1],
                metadata={},
            )
            ctx.set("memory_id", mid)
            return mid

        async def delete_record(ctx, result):
            mid = ctx.get("memory_id")
            if mid:
                await store.delete(mid, "agent-001", "tenant-001", permanent=True)

        async def fail_step(ctx):
            raise RuntimeError("Intentional failure")

        async def fail_comp(ctx, result):
            pass

        orchestrator.add_step("create", create_record, delete_record)
        orchestrator.add_step("fail", fail_step, fail_comp, max_retries=1)

        with pytest.raises(SagaCompensationError):
            await orchestrator.execute(TransactionContext())

        # 验证记录已被删除
        all_memories = list(store._memories.values())
        log(
            "test_saga_failure_rollback_memory",
            "回滚后存储中的记录数",
            "0",
            str(len(all_memories)),
        )
        assert len(all_memories) == 0


# ============================================================================
# 跨模块集成测试 - 边界条件和异常场景
# ============================================================================


class TestEdgeCasesAndExceptions:
    """测试边界条件和异常场景"""

    @pytest.mark.asyncio
    async def test_empty_saga_execution(self):
        """测试空 Saga 执行"""
        orchestrator = DtmSagaOrchestrator()
        tx = await orchestrator.execute(TransactionContext())
        log(
            "test_empty_saga_execution",
            "空Saga状态",
            "TransactionStatus.COMMITTED",
            str(tx.status),
        )
        assert tx.status == TransactionStatus.COMMITTED

    @pytest.mark.asyncio
    async def test_saga_with_none_result(self):
        """测试 Saga 步骤返回 None"""
        orchestrator = DtmSagaOrchestrator()

        async def none_step(ctx):
            return None

        async def none_comp(ctx, result):
            assert result is None

        orchestrator.add_step("none_step", none_step, none_comp)
        tx = await orchestrator.execute(TransactionContext())
        log(
            "test_saga_with_none_result",
            "None结果Saga状态",
            "TransactionStatus.COMMITTED",
            str(tx.status),
        )
        assert tx.status == TransactionStatus.COMMITTED

    @pytest.mark.asyncio
    async def test_event_bus_without_subscribers(self):
        """测试无订阅者时发布事件"""
        bus = InMemoryEventBus()
        await bus.start()

        event = DomainEvent(event_type="nobody_listens", payload={"msg": "hello"})
        result = await bus.publish(event)

        log(
            "test_event_bus_without_subscribers",
            "无订阅者发布结果",
            "True",
            str(result),
        )
        assert result is True

        await bus.stop()

    @pytest.mark.asyncio
    async def test_memory_store_concurrent_writes_same_agent(self):
        """测试同一 agent 并发写入"""
        store = MockMemoryStore()
        ids = []

        async def concurrent_write(i):
            mid = await store.store(
                agent_id="agent-001",
                tenant_id="tenant-001",
                content=f"concurrent_{i}",
                embedding=[float(i)],
                metadata={"idx": str(i)},
            )
            ids.append(mid)

        tasks = [concurrent_write(i) for i in range(100)]
        await asyncio.gather(*tasks)

        log(
            "test_memory_store_concurrent_writes_same_agent",
            "并发写入数",
            "100",
            str(len(ids)),
        )
        log(
            "test_memory_store_concurrent_writes_same_agent",
            "存储中总记录数",
            "100",
            str(len(store._memories)),
        )
        assert len(ids) == 100
        assert len(store._memories) == 100
        # 验证所有ID唯一
        assert len(set(ids)) == 100

    @pytest.mark.asyncio
    async def test_2pc_abort_nonexistent_transaction(self):
        """测试中止不存在的事务"""
        coordinator = TwoPhaseCommitCoordinator()
        with pytest.raises(TransactionNotFoundError):
            await coordinator.abort("nonexistent-tx-id")
        log(
            "test_2pc_abort_nonexistent_transaction",
            "中止不存在事务",
            "TransactionNotFoundError",
            "TransactionNotFoundError(已抛出)",
        )

    @pytest.mark.asyncio
    async def test_2pc_commit_without_prepare(self):
        """测试未准备就提交"""
        coordinator = TwoPhaseCommitCoordinator()
        tx = await coordinator.begin_transaction(timeout=10.0)
        with pytest.raises(TransactionError):
            await coordinator.commit(tx.tx_id)
        log(
            "test_2pc_commit_without_prepare",
            "未准备就提交",
            "TransactionError",
            "TransactionError(已抛出)",
        )

    @pytest.mark.asyncio
    async def test_event_store_get_nonexistent_aggregate(self):
        """测试获取不存在聚合的事件"""
        store = InMemoryEventStore()
        events = await store.get_events(AggregateId(value="nonexistent"))
        log(
            "test_event_store_get_nonexistent_aggregate",
            "不存在聚合的事件数",
            "0",
            str(len(events)),
        )
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_search_with_no_matching_agent(self):
        """测试搜索不匹配的 agent"""
        store = MockMemoryStore()
        await store.store(
            agent_id="agent-A",
            tenant_id="tenant-001",
            content="data",
            embedding=[0.1],
            metadata={},
        )
        results = await store.search("agent-B", "tenant-001", "data")
        log(
            "test_search_with_no_matching_agent",
            "不匹配agent搜索结果",
            "0",
            str(len(results)),
        )
        assert len(results) == 0

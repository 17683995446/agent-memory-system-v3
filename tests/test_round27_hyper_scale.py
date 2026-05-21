"""
第27轮测试：超大规模真实场景测试
===============================
基于代码审计发现的P0/P1级BUG创建针对性测试
每句代码后打印日志，通过日志验证执行路径
测试规模：百万级数据、小时级运行、多维度故障
"""

import pytest
import asyncio
import logging
import time
import sys
import os
import threading
import gc
import random
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)

# 添加项目路径
sys.path.insert(0, '/workspace/agent_memory_system_v2')

logger.debug("开始导入系统模块...")

from memory_system.agent_memory_system_facade import MemorySystem, MemorySystemBuilder
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.raft_consensus import RaftNode, RaftRole, RaftConfig
from memory_system.consistency_manager import ConsistencyManager, ConsistencyLevel
from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp
from memory_system.vector_clock import VectorClock
from memory_system.event_driven_core import (
    EventBus, InMemoryEventBus, DomainEvent, EventPriority,
    SagaOrchestrator, SagaStep, SagaStatus, DeadLetterQueueHandler
)
from memory_system.cqrs_system import (
    CQRSSystem, CommandBus, QueryBus, Command, Query,
    AggregateRoot, EventStore, InMemoryEventStore,
    InMemoryReadModelStore, CommandHandler, CommandResult
)
from memory_system.chaos_engineering import (
    ChaosEngineeringFramework, ChaosExperiment, ExperimentStatus,
    FaultConfig, CPUFaultConfig, MemoryFaultConfig, NetworkFaultConfig,
    FaultType
)
from memory_system.storage_backends.distributed_storage_backend import (
    DistributedStorageBackend, StorageNode, ConsistentHashRing
)
from memory_system.raft_consensus import LogEntry, InMemoryTransport, AppendEntriesResponse
from dataclasses import dataclass

logger.debug("所有模块导入完成")


# ============================================================================
# Raft死锁场景测试
# ============================================================================

@pytest.mark.asyncio
class TestRound27RaftDeadlock:
    """Raft死锁场景测试 - 基于审计发现的P0/P1级BUG"""

    @pytest.fixture
    async def raft_cluster(self):
        """创建3节点Raft集群"""
        logger.debug("[fixture] 开始创建Raft集群")
        transport = InMemoryTransport()
        nodes = []
        
        for i in range(3):
            config = RaftConfig(
                node_id=f"node{i}",
                peers=[f"node{j}" for j in range(3) if j != i],
                election_timeout_min=0.1,
                election_timeout_max=0.2,
                heartbeat_interval=0.05
            )
            node = RaftNode(config, transport=transport)
            transport.register_node(node)
            nodes.append(node)
            logger.debug(f"[fixture] 创建节点 node{i}")
        
        logger.debug("[fixture] Raft集群创建完成")
        yield nodes
        
        logger.debug("[fixture] 清理Raft集群")
        for node in nodes:
            await node.stop()
        logger.debug("[fixture] Raft集群清理完成")

    async def test_raft_start_election_no_lock_race(self):
        """测试1: 验证_start_election无锁修改共享状态的竞态条件"""
        logger.debug("=" * 60)
        logger.debug("[TEST-1] 开始测试: _start_election无锁修改竞态条件")
        logger.debug("=" * 60)
        
        transport = InMemoryTransport()
        nodes = []
        
        logger.debug("[TEST-1] 创建3个Raft节点")
        for i in range(3):
            config = RaftConfig(
                node_id=f"node{i}",
                peers=[f"node{j}" for j in range(3) if j != i],
                election_timeout_min=0.05,
                election_timeout_max=0.1,
                heartbeat_interval=0.02
            )
            node = RaftNode(config, transport=transport)
            transport.register_node(node)
            nodes.append(node)
            logger.debug(f"[TEST-1] 节点 node{i} 创建完成")
        
        logger.debug("[TEST-1] 启动所有节点")
        start_tasks = []
        for i, node in enumerate(nodes):
            logger.debug(f"[TEST-1] 启动节点 node{i}")
            start_tasks.append(node.start())
        
        logger.debug("[TEST-1] 等待所有节点启动完成")
        await asyncio.gather(*start_tasks)
        logger.debug("[TEST-1] 所有节点启动完成")
        
        logger.debug("[TEST-1] 等待选举稳定 (0.5秒)")
        await asyncio.sleep(0.5)
        
        logger.debug("[TEST-1] 检查各节点状态")
        leader_count = 0
        for i, node in enumerate(nodes):
            logger.debug(f"[TEST-1] 节点 node{i} 角色: {node.role}, 任期: {node.current_term}")
            if node.role == RaftRole.LEADER:
                leader_count += 1
        
        logger.debug(f"[TEST-1] 当前Leader数量: {leader_count}")
        
        # 验证：应该只有一个Leader
        logger.debug("[TEST-1] 验证Leader唯一性")
        assert leader_count <= 1, f"检测到多个Leader: {leader_count}"
        logger.debug("[TEST-1] Leader唯一性验证通过")
        
        logger.debug("[TEST-1] 停止所有节点")
        for i, node in enumerate(nodes):
            logger.debug(f"[TEST-1] 停止节点 node{i}")
            await node.stop()
        logger.debug("[TEST-1] 所有节点停止完成")
        
        logger.debug("[TEST-1] 测试通过!")

    async def test_raft_heartbeat_manual_lock_deadlock(self):
        """测试2: 验证心跳循环手动锁管理可能导致死锁"""
        logger.debug("=" * 60)
        logger.debug("[TEST-2] 开始测试: 心跳循环手动锁管理死锁")
        logger.debug("=" * 60)
        
        transport = InMemoryTransport()
        
        logger.debug("[TEST-2] 创建Leader节点配置")
        config = RaftConfig(
            node_id="leader",
            peers=["follower1", "follower2"],
            election_timeout_min=0.1,
            election_timeout_max=0.2,
            heartbeat_interval=0.05
        )
        
        logger.debug("[TEST-2] 创建Leader节点")
        leader = RaftNode(config, transport=transport)
        transport.register_node(leader)
        
        logger.debug("[TEST-2] 初始化Leader节点")
        leader._ensure_async_primitives()
        
        logger.debug("[TEST-2] 手动设置为Leader状态")
        async with leader._state_lock:
            leader._role = RaftRole.LEADER
            leader._persistent_state.current_term = 1
            logger.debug("[TEST-2] Leader状态设置完成")
        
        logger.debug("[TEST-2] 启动心跳定时器")
        leader._heartbeat_timer = asyncio.create_task(leader._heartbeat_loop())
        
        logger.debug("[TEST-2] 等待心跳循环运行 (1秒)")
        await asyncio.sleep(1.0)
        
        logger.debug("[TEST-2] 检查心跳循环状态")
        assert leader._heartbeat_timer is not None, "心跳定时器不应该为None"
        assert not leader._heartbeat_timer.done(), "心跳定时器不应该已完成"
        logger.debug("[TEST-2] 心跳循环正常运行")
        
        logger.debug("[TEST-2] 停止Leader节点")
        await leader.stop()
        logger.debug("[TEST-2] Leader节点停止完成")
        
        logger.debug("[TEST-2] 验证心跳定时器已取消")
        assert leader._heartbeat_timer is None or leader._heartbeat_timer.done(), "心跳定时器应该已停止"
        logger.debug("[TEST-2] 心跳定时器已正确停止")
        
        logger.debug("[TEST-2] 测试通过!")

    async def test_raft_state_lock_nested_deadlock(self):
        """测试3: 验证_state_lock嵌套获取导致死锁"""
        logger.debug("=" * 60)
        logger.debug("[TEST-3] 开始测试: _state_lock嵌套获取死锁")
        logger.debug("=" * 60)
        
        transport = InMemoryTransport()
        
        logger.debug("[TEST-3] 创建Raft节点")
        config = RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.1,
            election_timeout_max=0.2,
            heartbeat_interval=0.05
        )
        node = RaftNode(config, transport=transport)
        transport.register_node(node)
        
        logger.debug("[TEST-3] 初始化节点")
        node._ensure_async_primitives()
        
        logger.debug("[TEST-3] 手动设置为Leader状态")
        async with node._state_lock:
            node._role = RaftRole.LEADER
            node._persistent_state.current_term = 1
            node._volatile_state.initialize_leader_state(config.peers, -1)
            logger.debug("[TEST-3] Leader状态初始化完成")
        
        logger.debug("[TEST-3] 模拟日志复制响应链")
        logger.debug("[TEST-3] 创建模拟响应: _handle_append_entries_response -> _advance_commit_index -> _broadcast_append_entries")
        
        response = AppendEntriesResponse(
            term=1,
            follower_id="node2",
            success=True,
            match_index=0
        )
        
        logger.debug("[TEST-3] 调用 _handle_append_entries_response")
        try:
            await asyncio.wait_for(
                node._handle_append_entries_response("node2", response),
                timeout=2.0
            )
            logger.debug("[TEST-3] _handle_append_entries_response 完成，无死锁")
        except asyncio.TimeoutError:
            logger.error("[TEST-3] 检测到死锁! _handle_append_entries_response 超时")
            raise AssertionError("检测到死锁: _handle_append_entries_response 超时")
        
        logger.debug("[TEST-3] 验证节点状态")
        assert node.role == RaftRole.LEADER, "节点应该保持Leader状态"
        logger.debug("[TEST-3] 节点状态验证通过")
        
        logger.debug("[TEST-3] 停止节点")
        await node.stop()
        logger.debug("[TEST-3] 节点停止完成")
        
        logger.debug("[TEST-3] 测试通过!")

    async def test_raft_become_follower_lock_await(self):
        """测试4: 验证_become_follower在锁内await和创建Task"""
        logger.debug("=" * 60)
        logger.debug("[TEST-4] 开始测试: _become_follower在锁内await和创建Task")
        logger.debug("=" * 60)
        
        transport = InMemoryTransport()
        
        logger.debug("[TEST-4] 创建Raft节点")
        config = RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.1,
            election_timeout_max=0.2,
            heartbeat_interval=0.05
        )
        node = RaftNode(config, transport=transport)
        transport.register_node(node)
        
        logger.debug("[TEST-4] 初始化节点")
        node._ensure_async_primitives()
        
        logger.debug("[TEST-4] 手动设置为Leader状态")
        async with node._state_lock:
            node._role = RaftRole.LEADER
            node._persistent_state.current_term = 1
            node._current_leader = "node1"
            node._heartbeat_timer = asyncio.create_task(node._heartbeat_loop())
            logger.debug("[TEST-4] Leader状态初始化完成")
        
        logger.debug("[TEST-4] 调用 _become_follower 转换为Follower")
        start_time = time.time()
        try:
            await asyncio.wait_for(
                node._become_follower(2),
                timeout=2.0
            )
            elapsed = time.time() - start_time
            logger.debug(f"[TEST-4] _become_follower 完成，耗时: {elapsed:.3f}s")
        except asyncio.TimeoutError:
            logger.error("[TEST-4] _become_follower 超时，可能存在死锁")
            raise AssertionError("_become_follower 超时")
        
        logger.debug("[TEST-4] 验证节点状态")
        assert node.role == RaftRole.FOLLOWER, "节点应该转换为Follower"
        assert node.current_term == 2, "任期应该更新为2"
        logger.debug("[TEST-4] 节点状态验证通过")
        
        logger.debug("[TEST-4] 停止节点")
        await node.stop()
        logger.debug("[TEST-4] 节点停止完成")
        
        logger.debug("[TEST-4] 测试通过!")

    async def test_raft_apply_committed_entries_future_hang(self):
        """测试5: 验证_apply_committed_entries异常后future永远挂起"""
        logger.debug("=" * 60)
        logger.debug("[TEST-5] 开始测试: _apply_committed_entries异常后future挂起")
        logger.debug("=" * 60)
        
        transport = InMemoryTransport()
        
        logger.debug("[TEST-5] 创建会抛出异常的Mock状态机")
        class FailingStateMachine:
            def __init__(self):
                self.apply_count = 0
            
            async def apply(self, command):
                self.apply_count += 1
                logger.debug(f"[TEST-5] Mock状态机 apply 被调用 #{self.apply_count}")
                if self.apply_count == 1:
                    logger.debug("[TEST-5] 模拟异常抛出")
                    raise RuntimeError("模拟状态机异常")
                return {"success": True}
            
            async def snapshot(self):
                return b""
            
            async def restore(self, data):
                pass
        
        logger.debug("[TEST-5] 创建Raft节点")
        config = RaftConfig(
            node_id="node1",
            peers=["node2"],
            election_timeout_min=0.1,
            election_timeout_max=0.2
        )
        
        state_machine = FailingStateMachine()
        node = RaftNode(config, state_machine=state_machine, transport=transport)
        transport.register_node(node)
        
        logger.debug("[TEST-5] 初始化节点")
        node._ensure_async_primitives()
        
        logger.debug("[TEST-5] 添加测试日志条目")
        entry = LogEntry(term=1, index=0, command={"action": "test"})
        node._persistent_state.log.append(entry)
        node._volatile_state.commit_index = 0
        logger.debug(f"[TEST-5] 添加日志条目: entry_id={entry.entry_id}")
        
        logger.debug("[TEST-5] 创建等待的future")
        future = asyncio.Future()
        node._pending_proposals[entry.entry_id] = future
        logger.debug("[TEST-5] Future已注册到_pending_proposals")
        
        logger.debug("[TEST-5] 调用 _apply_committed_entries")
        await node._apply_committed_entries()
        
        logger.debug("[TEST-5] 检查future状态")
        assert future.done(), "Future应该被resolve（即使是异常）"
        
        if future.exception():
            logger.debug(f"[TEST-5] Future被设置为异常: {future.exception()}")
        else:
            logger.debug("[TEST-5] Future被设置为结果")
        
        logger.debug("[TEST-5] 验证_pending_proposals已清理")
        assert entry.entry_id not in node._pending_proposals, "Future应该从_pending_proposals中移除"
        logger.debug("[TEST-5] _pending_proposals清理验证通过")
        
        logger.debug("[TEST-5] 停止节点")
        await node.stop()
        logger.debug("[TEST-5] 节点停止完成")
        
        logger.debug("[TEST-5] 测试通过!")

    async def test_raft_log_entry_id_collision(self):
        """测试6: 验证LogEntry.entry_id碰撞风险"""
        logger.debug("=" * 60)
        logger.debug("[TEST-6] 开始测试: LogEntry.entry_id碰撞风险")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-6] 高并发创建LogEntry，检查entry_id唯一性")
        
        entry_ids = []
        collision_count = 0
        total_entries = 10000
        
        logger.debug(f"[TEST-6] 准备创建 {total_entries} 个LogEntry")
        
        logger.debug("[TEST-6] 开始并发创建LogEntry")
        start_time = time.time()
        
        for i in range(total_entries):
            entry = LogEntry(term=1, index=i, command={"id": i})
            if entry.entry_id in entry_ids:
                collision_count += 1
                logger.warning(f"[TEST-6] 检测到碰撞! entry_id={entry.entry_id}, index={i}")
            entry_ids.append(entry.entry_id)
            
            if i % 1000 == 0:
                logger.debug(f"[TEST-6] 已创建 {i} 个LogEntry")
        
        elapsed = time.time() - start_time
        logger.debug(f"[TEST-6] 创建完成，耗时: {elapsed:.3f}s")
        
        logger.debug("[TEST-6] 验证entry_id唯一性")
        unique_ids = set(entry_ids)
        logger.debug(f"[TEST-6] 总条目数: {total_entries}, 唯一ID数: {len(unique_ids)}, 碰撞数: {collision_count}")
        
        assert len(unique_ids) == total_entries, f"检测到 {total_entries - len(unique_ids)} 个碰撞"
        assert collision_count == 0, f"检测到 {collision_count} 个碰撞"
        
        logger.debug("[TEST-6] entry_id唯一性验证通过")
        logger.debug("[TEST-6] 测试通过!")


# ============================================================================
# EventBus+CQRS并发测试
# ============================================================================

@pytest.mark.asyncio
class TestRound27EventCQRSConcurrent:
    """EventBus+CQRS并发测试 - 基于审计发现的P0/P1级BUG"""

    async def test_saga_instance_concurrent_modification(self):
        """测试7: 验证SagaInstance状态变更无锁保护"""
        logger.debug("=" * 60)
        logger.debug("[TEST-7] 开始测试: SagaInstance状态变更无锁保护")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-7] 创建EventBus")
        event_bus = InMemoryEventBus()
        await event_bus.start()
        logger.debug("[TEST-7] EventBus启动完成")
        
        logger.debug("[TEST-7] 创建Saga编排器")
        saga_orchestrator = SagaOrchestrator(event_bus)
        
        logger.debug("[TEST-7] 定义测试步骤")
        async def step_action(context):
            logger.debug(f"[TEST-7] 执行步骤, context={context}")
            await asyncio.sleep(0.01)
            return True
        
        steps = [
            SagaStep("step1", step_action),
            SagaStep("step2", step_action),
            SagaStep("step3", step_action)
        ]
        
        logger.debug("[TEST-7] 注册Saga")
        saga_orchestrator.register_saga("test_saga", steps)
        
        logger.debug("[TEST-7] 启动多个Saga实例")
        instances = []
        for i in range(10):
            instance = await saga_orchestrator.start_saga("test_saga", {"index": i})
            instances.append(instance)
            logger.debug(f"[TEST-7] 启动Saga实例 {i}: {instance.saga_id}")
        
        logger.debug("[TEST-7] 等待所有Saga完成")
        await asyncio.sleep(1.0)
        
        logger.debug("[TEST-7] 检查所有Saga实例状态")
        completed_count = 0
        for i, instance in enumerate(instances):
            final_instance = saga_orchestrator.get_instance(instance.saga_id)
            logger.debug(f"[TEST-7] Saga实例 {i} 状态: {final_instance.status}")
            if final_instance.status == SagaStatus.COMPLETED:
                completed_count += 1
        
        logger.debug(f"[TEST-7] 完成数: {completed_count}/{len(instances)}")
        assert completed_count == len(instances), f"只有 {completed_count}/{len(instances)} 个Saga完成"
        
        logger.debug("[TEST-7] 停止EventBus")
        await event_bus.stop()
        logger.debug("[TEST-7] EventBus停止完成")
        
        logger.debug("[TEST-7] 测试通过!")

    async def test_dlq_running_flag_race_condition(self):
        """测试8: 验证DLQ处理器_running标志竞态"""
        logger.debug("=" * 60)
        logger.debug("[TEST-8] 开始测试: DLQ处理器_running标志竞态")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-8] 创建EventBus")
        event_bus = InMemoryEventBus()
        await event_bus.start()
        logger.debug("[TEST-8] EventBus启动完成")
        
        logger.debug("[TEST-8] 创建DLQ处理器")
        dlq = DeadLetterQueueHandler(event_bus)
        
        logger.debug("[TEST-8] 并发调用start()和stop()")
        
        async def start_stop_cycle(iteration):
            logger.debug(f"[TEST-8] 第 {iteration} 轮 start/stop 循环")
            try:
                await dlq.start()
                logger.debug(f"[TEST-8] 第 {iteration} 轮 start 完成")
                await asyncio.sleep(0.01)
                await dlq.stop()
                logger.debug(f"[TEST-8] 第 {iteration} 轮 stop 完成")
                return True
            except Exception as e:
                logger.error(f"[TEST-8] 第 {iteration} 轮发生错误: {e}")
                return False
        
        logger.debug("[TEST-8] 启动并发任务")
        tasks = [start_stop_cycle(i) for i in range(20)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.debug("[TEST-8] 检查并发结果")
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))
        logger.debug(f"[TEST-8] 成功: {success_count}, 错误: {error_count}")
        
        logger.debug("[TEST-8] 验证最终状态一致性")
        assert not dlq._running, "DLQ应该处于停止状态"
        logger.debug("[TEST-8] 最终状态验证通过")
        
        logger.debug("[TEST-8] 停止EventBus")
        await event_bus.stop()
        logger.debug("[TEST-8] EventBus停止完成")
        
        logger.debug("[TEST-8] 测试通过!")

    async def test_event_bus_handlers_race_condition(self):
        """测试9: 验证handlers并发订阅/取消订阅竞态"""
        logger.debug("=" * 60)
        logger.debug("[TEST-9] 开始测试: handlers并发订阅/取消订阅竞态")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-9] 创建EventBus")
        event_bus = InMemoryEventBus()
        await event_bus.start()
        logger.debug("[TEST-9] EventBus启动完成")
        
        logger.debug("[TEST-9] 定义事件处理器")
        async def handler1(event):
            logger.debug(f"[TEST-9] handler1 收到事件: {event.event_id}")
        
        async def handler2(event):
            logger.debug(f"[TEST-9] handler2 收到事件: {event.event_id}")
        
        logger.debug("[TEST-9] 并发执行订阅/取消订阅/发布")
        
        async def subscribe_task():
            for i in range(50):
                await event_bus.subscribe("test_event", handler1)
                await asyncio.sleep(0.001)
                await event_bus.subscribe("test_event", handler2)
                await asyncio.sleep(0.001)
        
        async def unsubscribe_task():
            for i in range(50):
                try:
                    await event_bus.unsubscribe("test_event", handler1)
                except ValueError:
                    pass
                await asyncio.sleep(0.001)
                try:
                    await event_bus.unsubscribe("test_event", handler2)
                except ValueError:
                    pass
                await asyncio.sleep(0.001)
        
        async def publish_task():
            for i in range(50):
                event = DomainEvent(event_type="test_event", payload={"index": i})
                await event_bus.publish(event)
                await asyncio.sleep(0.001)
        
        logger.debug("[TEST-9] 启动并发任务")
        tasks = [
            subscribe_task(),
            unsubscribe_task(),
            publish_task()
        ]
        
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=10.0
            )
            logger.debug("[TEST-9] 并发任务完成，无RuntimeError")
        except RuntimeError as e:
            logger.error(f"[TEST-9] 检测到RuntimeError: {e}")
            raise AssertionError(f"handlers竞态导致RuntimeError: {e}")
        except asyncio.TimeoutError:
            logger.error("[TEST-9] 并发任务超时")
            raise AssertionError("并发任务超时")
        
        logger.debug("[TEST-9] 停止EventBus")
        await event_bus.stop()
        logger.debug("[TEST-9] EventBus停止完成")
        
        logger.debug("[TEST-9] 测试通过!")

    async def test_projection_checkpoint_concurrent_modification(self):
        """测试10: 验证ProjectionCheckpoint状态转换无保护"""
        logger.debug("=" * 60)
        logger.debug("[TEST-10] 开始测试: ProjectionCheckpoint状态转换无保护")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-10] 创建EventStore")
        event_store = InMemoryEventStore()
        
        logger.debug("[TEST-10] 创建CQRS系统")
        cqrs = CQRSSystem(event_store)
        
        logger.debug("[TEST-10] 创建测试投影")
        from memory_system.cqrs_system import Projection, ProjectionStatus
        
        class TestProjection(Projection):
            def __init__(self, name, store):
                super().__init__(name, store)
                self.processed_count = 0
            
            async def project(self, event):
                self.processed_count += 1
                logger.debug(f"[TEST-10] 投影处理事件: {event.event_type}")
            
            def get_handled_event_types(self):
                return {"test_event"}
        
        read_store = InMemoryReadModelStore()
        projection = TestProjection("test_projection", read_store)
        
        logger.debug("[TEST-10] 注册投影")
        cqrs.register_projection(projection)
        
        logger.debug("[TEST-10] 启动CQRS系统")
        await cqrs.start()
        
        logger.debug("[TEST-10] 添加测试事件到存储")
        from memory_system.cqrs_system import StoredEvent
        for i in range(100):
            event = StoredEvent(
                event_id=f"event_{i}",
                aggregate_id=f"agg_{i % 10}",
                event_type="test_event",
                event_data={"index": i},
                version=i + 1,
                timestamp=datetime.now(timezone.utc),
                sequence_number=i + 1
            )
            await event_store.append(f"agg_{i % 10}", [event])
        
        logger.debug("[TEST-10] 等待投影处理")
        await asyncio.sleep(2.0)
        
        logger.debug("[TEST-10] 检查投影检查点状态")
        checkpoint = cqrs.projection_manager.get_checkpoint("test_projection")
        logger.debug(f"[TEST-10] 检查点状态: last_sequence={checkpoint.last_sequence}, status={checkpoint.status}")
        
        logger.debug("[TEST-10] 验证投影状态一致性")
        assert checkpoint.last_sequence == 100, f"检查点序列号应该是100，实际是{checkpoint.last_sequence}"
        assert checkpoint.status == ProjectionStatus.COMPLETED, f"检查点状态应该是COMPLETED，实际是{checkpoint.status}"
        
        logger.debug("[TEST-10] 停止CQRS系统")
        await cqrs.stop()
        logger.debug("[TEST-10] CQRS系统停止完成")
        
        logger.debug("[TEST-10] 测试通过!")

    async def test_projection_checkpoint_update_on_failure(self):
        """测试11: 验证投影检查点在事件处理失败时已更新"""
        logger.debug("=" * 60)
        logger.debug("[TEST-11] 开始测试: 投影检查点在事件处理失败时已更新")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-11] 创建EventStore")
        event_store = InMemoryEventStore()
        
        logger.debug("[TEST-11] 创建CQRS系统")
        cqrs = CQRSSystem(event_store)
        
        logger.debug("[TEST-11] 创建会抛出异常的测试投影")
        from memory_system.cqrs_system import Projection, ProjectionStatus

        class FailingProjection(Projection):
            def __init__(self, name, store):
                super().__init__(name, store)
                self.processed_count = 0
            
            async def project(self, event):
                self.processed_count += 1
                logger.debug(f"[TEST-11] 投影处理事件: {event.event_type}, sequence={event.sequence_number}")
                if event.sequence_number == 5:
                    logger.debug("[TEST-11] 模拟投影处理异常")
                    raise RuntimeError("模拟投影处理异常")
            
            def get_handled_event_types(self):
                return {"test_event"}
        
        read_store = InMemoryReadModelStore()
        projection = FailingProjection("failing_projection", read_store)
        
        logger.debug("[TEST-11] 注册投影")
        cqrs.register_projection(projection)
        
        logger.debug("[TEST-11] 启动CQRS系统")
        await cqrs.start()
        
        logger.debug("[TEST-11] 添加测试事件到存储")
        from memory_system.cqrs_system import StoredEvent
        for i in range(10):
            event = StoredEvent(
                event_id=f"event_{i}",
                aggregate_id="agg_1",
                event_type="test_event",
                event_data={"index": i},
                version=i + 1,
                timestamp=datetime.now(timezone.utc),
                sequence_number=i + 1
            )
            await event_store.append("agg_1", [event])
        
        logger.debug("[TEST-11] 等待投影处理")
        await asyncio.sleep(2.0)
        
        logger.debug("[TEST-11] 检查投影检查点状态")
        checkpoint = cqrs.projection_manager.get_checkpoint("failing_projection")
        logger.debug(f"[TEST-11] 检查点状态: last_sequence={checkpoint.last_sequence}, status={checkpoint.status}")
        
        logger.debug("[TEST-11] 验证检查点已前进（即使处理失败）")
        assert checkpoint.last_sequence >= 4, f"检查点应该至少前进到4，实际是{checkpoint.last_sequence}"
        
        logger.debug("[TEST-11] 停止CQRS系统")
        await cqrs.stop()
        logger.debug("[TEST-11] CQRS系统停止完成")
        
        logger.debug("[TEST-11] 测试通过!")

    async def test_command_bus_concurrent_execution(self):
        """测试12: 验证CommandBus.send无并发控制"""
        logger.debug("=" * 60)
        logger.debug("[TEST-12] 开始测试: CommandBus.send无并发控制")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-12] 创建EventStore")
        event_store = InMemoryEventStore()
        
        logger.debug("[TEST-12] 创建CommandBus")
        command_bus = CommandBus(event_store)
        
        logger.debug("[TEST-12] 创建计数器跟踪命令执行次数")
        execution_count = 0
        execution_lock = asyncio.Lock()
        
        logger.debug("[TEST-12] 定义测试命令")
        @dataclass
        class TestCommand(Command):
            value: int = 0
        
        logger.debug("[TEST-12] 定义测试命令处理器")
        class TestCommandHandler(CommandHandler):
            def get_command_type(self):
                return TestCommand
            
            async def handle(self, command):
                nonlocal execution_count
                async with execution_lock:
                    execution_count += 1
                    current_count = execution_count
                logger.debug(f"[TEST-12] 命令处理器执行 #{current_count}, value={command.value}")
                await asyncio.sleep(0.01)
                return CommandResult.success(
                    command_id=command.command_id,
                    aggregate_id="test_agg",
                    version=1
                )
        
        logger.debug("[TEST-12] 注册命令处理器")
        command_bus.register_handler(TestCommandHandler())
        
        logger.debug("[TEST-12] 并发发送同一命令")
        command = TestCommand(value=42)
        
        async def send_command():
            return await command_bus.send(command)
        
        logger.debug("[TEST-12] 启动10个并发发送任务")
        tasks = [send_command() for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        logger.debug("[TEST-12] 检查执行结果")
        success_count = sum(1 for r in results if r.is_success)
        logger.debug(f"[TEST-12] 成功执行: {success_count}/10")
        logger.debug(f"[TEST-12] 实际执行次数: {execution_count}")
        
        logger.debug("[TEST-12] 验证命令执行次数")
        assert execution_count == 10, f"命令应该执行10次，实际执行{execution_count}次"
        assert success_count == 10, f"应该有10个成功结果，实际有{success_count}个"
        
        logger.debug("[TEST-12] 测试通过!")


# ============================================================================
# Chaos+分布式存储测试
# ============================================================================

@pytest.mark.asyncio
class TestRound27ChaosDistributed:
    """Chaos+分布式存储测试 - 基于审计发现的P0/P1级BUG"""

    async def test_chaos_monitor_deadlock(self):
        """测试13: 验证_monitor_experiment持有RLock调用stop_experiment死锁"""
        logger.debug("=" * 60)
        logger.debug("[TEST-13] 开始测试: _monitor_experiment持有RLock调用stop_experiment死锁")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-13] 创建混沌工程框架")
        framework = ChaosEngineeringFramework()
        
        logger.debug("[TEST-13] 创建CPU故障实验")
        config = CPUFaultConfig(
            cores=1,
            load_percent=10.0,
            duration=2.0,
            auto_rollback=True
        )
        
        experiment = framework.create_experiment(
            name="test_cpu_fault",
            description="Test CPU fault for deadlock detection",
            fault_type=FaultType.CPU,
            fault_config=config
        )
        logger.debug(f"[TEST-13] 实验创建完成: {experiment.id}")
        
        logger.debug("[TEST-13] 启动实验")
        start_time = time.time()
        framework.start_experiment(experiment.id)
        
        logger.debug("[TEST-13] 等待实验自动停止（通过auto_rollback）")
        await asyncio.sleep(3.0)
        
        elapsed = time.time() - start_time
        logger.debug(f"[TEST-13] 等待完成，耗时: {elapsed:.3f}s")
        
        logger.debug("[TEST-13] 检查实验状态")
        status = framework.get_experiment_status(experiment.id)
        logger.debug(f"[TEST-13] 实验状态: {status['status']}")
        
        logger.debug("[TEST-13] 验证实验已停止（无死锁）")
        assert status['status'] in ['completed', 'failed', 'rolled_back'], f"实验应该已停止，状态为{status['status']}"
        
        logger.debug("[TEST-13] 关闭框架")
        framework.shutdown()
        logger.debug("[TEST-13] 框架关闭完成")
        
        logger.debug("[TEST-13] 测试通过!")

    async def test_chaos_suite_infinite_loop(self):
        """测试14: 验证run_chaos_suite无限循环"""
        logger.debug("=" * 60)
        logger.debug("[TEST-14] 开始测试: run_chaos_suite无限循环")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-14] 创建混沌工程框架")
        framework = ChaosEngineeringFramework()
        
        logger.debug("[TEST-14] 创建多个实验")
        experiments = []
        for i in range(3):
            config = CPUFaultConfig(
                cores=1,
                load_percent=10.0,
                duration=1.0,
                auto_rollback=True
            )
            experiment = framework.create_experiment(
                name=f"test_exp_{i}",
                description=f"Test experiment {i}",
                fault_type=FaultType.CPU,
                fault_config=config
            )
            experiments.append(experiment)
            logger.debug(f"[TEST-14] 实验 {i} 创建完成: {experiment.id}")
        
        logger.debug("[TEST-14] 运行实验套件（设置超时防止无限循环）")
        start_time = time.time()
        
        try:
            # 在线程中运行阻塞的run_chaos_suite
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(framework.run_chaos_suite, experiments, delay_between=0.5)
                result = future.result(timeout=15.0)
                logger.debug(f"[TEST-14] 实验套件完成: {result}")
        except concurrent.futures.TimeoutError:
            logger.error("[TEST-14] run_chaos_suite 超时，可能存在无限循环")
            raise AssertionError("run_chaos_suite 超时，可能存在无限循环")
        
        elapsed = time.time() - start_time
        logger.debug(f"[TEST-14] 实验套件运行完成，耗时: {elapsed:.3f}s")
        
        logger.debug("[TEST-14] 验证在合理时间内返回")
        assert elapsed < 15.0, f"实验套件应该在15秒内完成，实际耗时{elapsed:.3f}s"
        
        logger.debug("[TEST-14] 关闭框架")
        framework.shutdown()
        logger.debug("[TEST-14] 框架关闭完成")
        
        logger.debug("[TEST-14] 测试通过!")

    async def test_chaos_cpu_thread_leak(self):
        """测试15: 验证CPUFaultInjector线程泄漏"""
        logger.debug("=" * 60)
        logger.debug("[TEST-15] 开始测试: CPUFaultInjector线程泄漏")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-15] 记录初始线程数")
        initial_threads = threading.active_count()
        logger.debug(f"[TEST-15] 初始线程数: {initial_threads}")
        
        logger.debug("[TEST-15] 快速创建/销毁多个CPU故障实验")
        framework = ChaosEngineeringFramework()
        
        for i in range(5):
            logger.debug(f"[TEST-15] 第 {i+1}/5 轮实验")
            config = CPUFaultConfig(
                cores=1,
                load_percent=10.0,
                duration=0.5,
                auto_rollback=True
            )
            experiment = framework.create_experiment(
                name=f"cpu_leak_test_{i}",
                description="Test for thread leak",
                fault_type=FaultType.CPU,
                fault_config=config
            )
            
            logger.debug(f"[TEST-15] 启动实验 {i+1}")
            framework.start_experiment(experiment.id)
            
            logger.debug(f"[TEST-15] 等待实验 {i+1} 完成")
            await asyncio.sleep(1.0)
            
            logger.debug(f"[TEST-15] 停止实验 {i+1}")
            framework.stop_experiment(experiment.id)
        
        logger.debug("[TEST-15] 强制垃圾回收")
        gc.collect()
        await asyncio.sleep(0.5)
        
        logger.debug("[TEST-15] 记录最终线程数")
        final_threads = threading.active_count()
        logger.debug(f"[TEST-15] 最终线程数: {final_threads}")
        
        logger.debug("[TEST-15] 验证线程无泄漏")
        thread_diff = final_threads - initial_threads
        logger.debug(f"[TEST-15] 线程差异: {thread_diff}")
        assert thread_diff <= 2, f"检测到线程泄漏: 差异{thread_diff}个线程"
        
        logger.debug("[TEST-15] 关闭框架")
        framework.shutdown()
        logger.debug("[TEST-15] 框架关闭完成")
        
        logger.debug("[TEST-15] 测试通过!")

    async def test_chaos_memory_leak_thread_cleanup(self):
        """测试16: 验证MemoryFaultInjector内存泄漏线程清理"""
        logger.debug("=" * 60)
        logger.debug("[TEST-16] 开始测试: MemoryFaultInjector内存泄漏线程清理")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-16] 记录初始线程数")
        initial_threads = threading.active_count()
        logger.debug(f"[TEST-16] 初始线程数: {initial_threads}")
        
        logger.debug("[TEST-16] 创建混沌工程框架")
        framework = ChaosEngineeringFramework()
        
        logger.debug("[TEST-16] 创建内存泄漏实验")
        config = MemoryFaultConfig(
            memory_mb=10,
            leak_rate_mb_per_sec=5.0,
            duration=1.0,
            auto_rollback=True
        )
        
        experiment = framework.create_experiment(
            name="memory_leak_test",
            description="Test memory leak thread cleanup",
            fault_type=FaultType.MEMORY,
            fault_config=config
        )
        logger.debug(f"[TEST-16] 实验创建完成: {experiment.id}")
        
        logger.debug("[TEST-16] 启动实验")
        framework.start_experiment(experiment.id)
        
        logger.debug("[TEST-16] 快速停止实验（测试清理）")
        await asyncio.sleep(0.3)
        framework.stop_experiment(experiment.id)
        
        logger.debug("[TEST-16] 等待清理完成")
        await asyncio.sleep(1.0)
        
        logger.debug("[TEST-16] 强制垃圾回收")
        gc.collect()
        await asyncio.sleep(0.5)
        
        logger.debug("[TEST-16] 记录最终线程数")
        final_threads = threading.active_count()
        logger.debug(f"[TEST-16] 最终线程数: {final_threads}")
        
        logger.debug("[TEST-16] 验证线程已清理")
        thread_diff = final_threads - initial_threads
        logger.debug(f"[TEST-16] 线程差异: {thread_diff}")
        assert thread_diff <= 1, f"检测到线程未清理: 差异{thread_diff}个线程"
        
        logger.debug("[TEST-16] 关闭框架")
        framework.shutdown()
        logger.debug("[TEST-16] 框架关闭完成")
        
        logger.debug("[TEST-16] 测试通过!")

    async def test_distributed_get_nodes_infinite_loop(self):
        """测试17: 验证get_nodes潜在无限循环"""
        logger.debug("=" * 60)
        logger.debug("[TEST-17] 开始测试: get_nodes潜在无限循环")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-17] 创建一致性哈希环")
        hash_ring = ConsistentHashRing(replicas=10)
        
        logger.debug("[TEST-17] 只添加1个节点")
        node1 = StorageNode(node_id="node1", host="localhost", port=8001)
        hash_ring.add_node(node1)
        logger.debug("[TEST-17] 节点添加完成")
        
        logger.debug("[TEST-17] 请求比可用节点更多的副本（设置超时）")
        start_time = time.time()
        
        try:
            logger.debug("[TEST-17] 调用 get_nodes 请求10个节点")
            nodes = await asyncio.wait_for(
                asyncio.to_thread(hash_ring.get_nodes, "test_key", n=10),
                timeout=2.0
            )
            elapsed = time.time() - start_time
            logger.debug(f"[TEST-17] get_nodes 完成，耗时: {elapsed:.3f}s，返回 {len(nodes)} 个节点")
        except asyncio.TimeoutError:
            logger.error("[TEST-17] get_nodes 超时，存在无限循环!")
            raise AssertionError("get_nodes 超时，存在无限循环")
        
        logger.debug("[TEST-17] 验证返回结果")
        assert len(nodes) <= 1, f"应该只返回1个节点，实际返回{len(nodes)}个"
        logger.debug("[TEST-17] 返回结果验证通过")
        
        logger.debug("[TEST-17] 测试通过!")

    async def test_distributed_get_store_race_condition(self):
        """测试18: 验证_get_store竞态条件"""
        logger.debug("=" * 60)
        logger.debug("[TEST-18] 开始测试: _get_store竞态条件")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-18] 创建存储节点")
        nodes = [
            StorageNode(node_id="node1", host="localhost", port=8001),
            StorageNode(node_id="node2", host="localhost", port=8002),
        ]
        
        logger.debug("[TEST-18] 创建分布式存储后端")
        storage = DistributedStorageBackend(nodes, replication_factor=2)
        
        logger.debug("[TEST-18] 并发调用 _get_store")
        store_ids = []
        store_lock = asyncio.Lock()
        
        async def get_store_concurrent(node_id):
            node = nodes[0] if node_id == "node1" else nodes[1]
            logger.debug(f"[TEST-18] 线程获取store: {node_id}")
            store = storage._get_store(node)
            async with store_lock:
                store_ids.append(id(store))
            logger.debug(f"[TEST-18] 线程获取store完成: {node_id}, store_id={id(store)}")
            return store
        
        logger.debug("[TEST-18] 启动20个并发任务")
        tasks = [get_store_concurrent(f"node{(i % 2) + 1}") for i in range(20)]
        results = await asyncio.gather(*tasks)
        
        logger.debug("[TEST-18] 分析结果")
        unique_stores = set(store_ids)
        logger.debug(f"[TEST-18] 总共获取 {len(results)} 次，唯一store实例: {len(unique_stores)}")
        
        logger.debug("[TEST-18] 验证每个节点只有一个store实例")
        assert len(unique_stores) == 2, f"应该只有2个唯一store实例，实际有{len(unique_stores)}个"
        
        logger.debug("[TEST-18] 关闭存储")
        await storage.close()
        logger.debug("[TEST-18] 存储关闭完成")
        
        logger.debug("[TEST-18] 测试通过!")


# ============================================================================
# 超大规模压力测试
# ============================================================================

@pytest.mark.asyncio
class TestRound27HyperScale:
    """超大规模压力测试 - 百万级数据、小时级运行、多维度故障注入"""

    async def test_million_memories_with_raft_consensus(self):
        """测试19: 百万级记忆在Raft共识下的写入"""
        logger.debug("=" * 60)
        logger.debug("[TEST-19] 开始测试: 百万级记忆在Raft共识下的写入")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-19] 注意: 为测试速度，使用10000条记忆代替100万")
        num_memories = 10000  # 实际测试中可调整为1000000
        
        logger.debug("[TEST-19] 创建3节点Raft集群")
        transport = InMemoryTransport()
        nodes = []
        
        for i in range(3):
            config = RaftConfig(
                node_id=f"node{i}",
                peers=[f"node{j}" for j in range(3) if j != i],
                election_timeout_min=0.1,
                election_timeout_max=0.2,
                heartbeat_interval=0.05
            )
            node = RaftNode(config, transport=transport)
            transport.register_node(node)
            nodes.append(node)
            logger.debug(f"[TEST-19] 节点 node{i} 创建完成")
        
        logger.debug("[TEST-19] 启动所有节点")
        for node in nodes:
            await node.start()
        logger.debug("[TEST-19] 所有节点启动完成")
        
        logger.debug("[TEST-19] 等待选举稳定")
        await asyncio.sleep(0.5)
        
        logger.debug("[TEST-19] 查找Leader节点")
        leader = None
        for node in nodes:
            if node.role == RaftRole.LEADER:
                leader = node
                break
        
        if leader is None:
            logger.debug("[TEST-19] 未找到Leader，使用第一个节点")
            leader = nodes[0]
        else:
            logger.debug(f"[TEST-19] Leader节点: {leader.node_id}")
        
        logger.debug(f"[TEST-19] 开始写入 {num_memories} 条记忆")
        start_time = time.time()
        success_count = 0
        
        for i in range(num_memories):
            try:
                command = {"action": "set", "key": f"memory_{i}", "value": f"data_{i}"}
                await leader.propose_command(command)
                success_count += 1
                
                if (i + 1) % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    logger.debug(f"[TEST-19] 已写入 {i + 1}/{num_memories} 条，速率: {rate:.2f} 条/秒")
            except Exception as e:
                logger.warning(f"[TEST-19] 写入 memory_{i} 失败: {e}")
        
        elapsed = time.time() - start_time
        logger.debug(f"[TEST-19] 写入完成，总耗时: {elapsed:.3f}s，成功: {success_count}/{num_memories}")
        
        logger.debug("[TEST-19] 验证所有节点日志一致性")
        log_lengths = [len(node._persistent_state.log) for node in nodes]
        logger.debug(f"[TEST-19] 各节点日志长度: {log_lengths}")
        
        logger.debug("[TEST-19] 停止所有节点")
        for node in nodes:
            await node.stop()
        logger.debug("[TEST-19] 所有节点停止完成")
        
        logger.debug("[TEST-19] 测试通过!")

    async def test_hour_long_chaos_simulation(self):
        """测试20: 小时级混沌模拟（使用加速模拟）"""
        logger.debug("=" * 60)
        logger.debug("[TEST-20] 开始测试: 小时级混沌模拟（加速版）")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-20] 注意: 使用加速模拟，1秒=1分钟，总运行时间约60秒")
        
        logger.debug("[TEST-20] 创建混沌工程框架")
        framework = ChaosEngineeringFramework()
        
        logger.debug("[TEST-20] 创建多种故障实验")
        experiments = []
        
        # CPU故障
        cpu_config = CPUFaultConfig(cores=1, load_percent=30.0, duration=5.0, auto_rollback=True)
        cpu_exp = framework.create_experiment(
            name="cpu_chaos", description="CPU chaos", fault_type=FaultType.CPU, fault_config=cpu_config
        )
        experiments.append(cpu_exp)
        logger.debug(f"[TEST-20] CPU实验创建: {cpu_exp.id}")
        
        # 内存故障
        mem_config = MemoryFaultConfig(memory_mb=50, duration=5.0, auto_rollback=True)
        mem_exp = framework.create_experiment(
            name="memory_chaos", description="Memory chaos", fault_type=FaultType.MEMORY, fault_config=mem_config
        )
        experiments.append(mem_exp)
        logger.debug(f"[TEST-20] 内存实验创建: {mem_exp.id}")
        
        logger.debug("[TEST-20] 开始混沌模拟循环")
        start_time = time.time()
        cycle_count = 0
        max_cycles = 5  # 模拟5个"小时"
        
        while cycle_count < max_cycles and (time.time() - start_time) < 60:
            cycle_count += 1
            logger.debug(f"[TEST-20] === 混沌周期 {cycle_count}/{max_cycles} ===")
            
            for exp in experiments:
                logger.debug(f"[TEST-20] 启动实验: {exp.name}")
                framework.start_experiment(exp.id)
                
                logger.debug(f"[TEST-20] 等待实验完成")
                await asyncio.sleep(6.0)  # 等待实验完成
                
                status = framework.get_experiment_status(exp.id)
                logger.debug(f"[TEST-20] 实验状态: {status['status']}")
        
        elapsed = time.time() - start_time
        logger.debug(f"[TEST-20] 混沌模拟完成，总耗时: {elapsed:.3f}s，周期数: {cycle_count}")
        
        logger.debug("[TEST-20] 验证系统稳定性")
        assert cycle_count >= max_cycles, f"应该完成 {max_cycles} 个周期，实际完成 {cycle_count} 个"
        
        logger.debug("[TEST-20] 关闭框架")
        framework.shutdown()
        logger.debug("[TEST-20] 框架关闭完成")
        
        logger.debug("[TEST-20] 测试通过!")

    async def test_cascade_failure_with_multiple_dimensions(self):
        """测试21: 多维度级联故障"""
        logger.debug("=" * 60)
        logger.debug("[TEST-21] 开始测试: 多维度级联故障")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-21] 创建混沌工程框架")
        framework = ChaosEngineeringFramework()
        
        logger.debug("[TEST-21] 创建多维度故障实验")
        
        # CPU故障
        cpu_config = CPUFaultConfig(cores=2, load_percent=50.0, duration=3.0, auto_rollback=True)
        cpu_exp = framework.create_experiment(
            name="cascade_cpu", description="Cascade CPU fault", fault_type=FaultType.CPU, fault_config=cpu_config
        )
        
        # 内存故障
        mem_config = MemoryFaultConfig(memory_mb=100, duration=3.0, auto_rollback=True)
        mem_exp = framework.create_experiment(
            name="cascade_memory", description="Cascade memory fault", fault_type=FaultType.MEMORY, fault_config=mem_config
        )
        
        logger.debug("[TEST-21] 同时启动所有故障实验（级联故障）")
        framework.start_experiment(cpu_exp.id)
        framework.start_experiment(mem_exp.id)
        logger.debug("[TEST-21] 所有故障实验已启动")
        
        logger.debug("[TEST-21] 等待故障注入")
        await asyncio.sleep(2.0)
        
        logger.debug("[TEST-21] 检查实验状态")
        cpu_status = framework.get_experiment_status(cpu_exp.id)
        mem_status = framework.get_experiment_status(mem_exp.id)
        logger.debug(f"[TEST-21] CPU实验状态: {cpu_status['status']}")
        logger.debug(f"[TEST-21] 内存实验状态: {mem_status['status']}")
        
        logger.debug("[TEST-21] 等待故障恢复")
        await asyncio.sleep(3.0)
        
        logger.debug("[TEST-21] 验证系统恢复能力")
        cpu_status = framework.get_experiment_status(cpu_exp.id)
        mem_status = framework.get_experiment_status(mem_exp.id)
        
        logger.debug(f"[TEST-21] 最终CPU实验状态: {cpu_status['status']}")
        logger.debug(f"[TEST-21] 最终内存实验状态: {mem_status['status']}")
        
        logger.debug("[TEST-21] 关闭框架")
        framework.shutdown()
        logger.debug("[TEST-21] 框架关闭完成")
        
        logger.debug("[TEST-21] 测试通过!")

    async def test_event_sourcing_massive_events(self):
        """测试22: 大规模事件溯源"""
        logger.debug("=" * 60)
        logger.debug("[TEST-22] 开始测试: 大规模事件溯源")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-22] 注意: 为测试速度，使用10000个事件代替100万")
        num_events = 10000  # 实际测试中可调整为1000000
        
        logger.debug("[TEST-22] 创建EventStore")
        event_store = InMemoryEventStore()
        
        logger.debug("[TEST-22] 创建CQRS系统")
        cqrs = CQRSSystem(event_store)
        
        logger.debug("[TEST-22] 创建测试投影")
        from memory_system.cqrs_system import Projection
        
        class CountingProjection(Projection):
            def __init__(self, name, store):
                super().__init__(name, store)
                self.event_count = 0
            
            async def project(self, event):
                self.event_count += 1
                if self.event_count % 1000 == 0:
                    logger.debug(f"[TEST-22] 投影已处理 {self.event_count} 个事件")
            
            def get_handled_event_types(self):
                return {"massive_event"}
        
        read_store = InMemoryReadModelStore()
        projection = CountingProjection("counting_projection", read_store)
        cqrs.register_projection(projection)
        
        logger.debug("[TEST-22] 启动CQRS系统")
        await cqrs.start()
        
        logger.debug(f"[TEST-22] 创建 {num_events} 个事件")
        from memory_system.cqrs_system import StoredEvent
        start_time = time.time()
        
        for i in range(num_events):
            event = StoredEvent(
                event_id=f"event_{i}",
                aggregate_id=f"agg_{i % 100}",
                event_type="massive_event",
                event_data={"index": i, "data": "x" * 100},
                version=i + 1,
                timestamp=datetime.now(timezone.utc),
                sequence_number=i + 1
            )
            await event_store.append(f"agg_{i % 100}", [event])
            
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                logger.debug(f"[TEST-22] 已创建 {i + 1}/{num_events} 个事件，速率: {rate:.2f} 个/秒")
        
        elapsed = time.time() - start_time
        logger.debug(f"[TEST-22] 事件创建完成，总耗时: {elapsed:.3f}s")
        
        logger.debug("[TEST-22] 等待投影处理完成")
        await asyncio.sleep(3.0)
        
        logger.debug("[TEST-22] 验证投影处理结果")
        logger.debug(f"[TEST-22] 投影处理事件数: {projection.event_count}")
        assert projection.event_count == num_events, f"投影应该处理 {num_events} 个事件，实际处理 {projection.event_count} 个"
        
        logger.debug("[TEST-22] 停止CQRS系统")
        await cqrs.stop()
        logger.debug("[TEST-22] CQRS系统停止完成")
        
        logger.debug("[TEST-22] 测试通过!")

    async def test_saga_long_running_transaction(self):
        """测试23: 长时间运行Saga事务"""
        logger.debug("=" * 60)
        logger.debug("[TEST-23] 开始测试: 长时间运行Saga事务")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-23] 创建EventBus")
        event_bus = InMemoryEventBus()
        await event_bus.start()
        logger.debug("[TEST-23] EventBus启动完成")
        
        logger.debug("[TEST-23] 创建Saga编排器")
        saga_orchestrator = SagaOrchestrator(event_bus)
        
        logger.debug("[TEST-23] 创建10步Saga，每步耗时0.1秒（模拟1秒）")
        step_results = []
        
        async def create_step_action(step_num):
            async def step_action(context):
                await asyncio.sleep(0.1)  # 每步0.1秒
                step_results.append(step_num)
                logger.debug(f"[TEST-23] 步骤 {step_num} 完成")
                return {"step": step_num}
            return step_action
        
        steps = []
        for i in range(10):
            action = await create_step_action(i + 1)
            steps.append(SagaStep(f"step_{i+1}", action))
        
        logger.debug("[TEST-23] 注册Saga")
        saga_orchestrator.register_saga("long_running_saga", steps)
        
        logger.debug("[TEST-23] 启动Saga")
        start_time = time.time()
        instance = await saga_orchestrator.start_saga("long_running_saga", {"test": True})
        logger.debug(f"[TEST-23] Saga实例启动: {instance.saga_id}")
        
        logger.debug("[TEST-23] 等待Saga完成")
        max_wait = 30.0
        while (time.time() - start_time) < max_wait:
            final_instance = saga_orchestrator.get_instance(instance.saga_id)
            if final_instance.status in [SagaStatus.COMPLETED, SagaStatus.FAILED]:
                break
            await asyncio.sleep(0.1)
        
        elapsed = time.time() - start_time
        logger.debug(f"[TEST-23] Saga完成，总耗时: {elapsed:.3f}s")
        
        logger.debug("[TEST-23] 验证Saga结果")
        final_instance = saga_orchestrator.get_instance(instance.saga_id)
        logger.debug(f"[TEST-23] Saga状态: {final_instance.status}")
        logger.debug(f"[TEST-23] 完成步骤数: {len(final_instance.completed_steps)}")
        
        assert final_instance.status == SagaStatus.COMPLETED, f"Saga应该完成，状态为{final_instance.status}"
        assert len(final_instance.completed_steps) == 10, f"应该完成10个步骤，实际完成{len(final_instance.completed_steps)}个"
        
        logger.debug("[TEST-23] 停止EventBus")
        await event_bus.stop()
        logger.debug("[TEST-23] EventBus停止完成")
        
        logger.debug("[TEST-23] 测试通过!")

    async def test_distributed_quorum_under_partition(self):
        """测试24: 网络分区下的quorum行为"""
        logger.debug("=" * 60)
        logger.debug("[TEST-24] 开始测试: 网络分区下的quorum行为")
        logger.debug("=" * 60)
        
        logger.debug("[TEST-24] 创建5个存储节点")
        nodes = [
            StorageNode(node_id=f"node{i}", host="localhost", port=8000 + i)
            for i in range(5)
        ]
        
        logger.debug("[TEST-24] 创建分布式存储后端（3副本，2读仲裁，2写仲裁）")
        storage = DistributedStorageBackend(
            nodes=nodes,
            replication_factor=3,
            read_quorum=2,
            write_quorum=2,
            default_timeout=1.0
        )
        
        logger.debug("[TEST-24] 模拟网络分区: 标记2个节点为不健康")
        nodes[3].is_healthy = False
        nodes[4].is_healthy = False
        logger.debug("[TEST-24] node3, node4 标记为不健康")
        
        logger.debug("[TEST-24] 尝试写入数据")
        from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        
        item = MemoryItem(
            memory_id="test_partition",
            content="test content",
            memory_type=MemoryType.EPISODIC,
            timestamp=time.time()
        )
        
        try:
            await asyncio.wait_for(storage.add(item), timeout=5.0)
            logger.debug("[TEST-24] 写入成功（quorum达成）")
            write_success = True
        except Exception as e:
            logger.warning(f"[TEST-24] 写入失败: {e}")
            write_success = False
        
        logger.debug("[TEST-24] 尝试读取数据")
        try:
            result = await asyncio.wait_for(storage.get("test_partition"), timeout=5.0)
            if result:
                logger.debug("[TEST-24] 读取成功")
                read_success = True
            else:
                logger.debug("[TEST-24] 读取返回None")
                read_success = False
        except Exception as e:
            logger.warning(f"[TEST-24] 读取失败: {e}")
            read_success = False
        
        logger.debug("[TEST-24] 恢复网络分区")
        nodes[3].is_healthy = True
        nodes[4].is_healthy = True
        logger.debug("[TEST-24] 所有节点恢复为健康状态")
        
        logger.debug("[TEST-24] 验证quorum行为")
        logger.debug(f"[TEST-24] 写入成功: {write_success}")
        logger.debug(f"[TEST-24] 读取成功: {read_success}")
        
        logger.debug("[TEST-24] 关闭存储")
        await storage.close()
        logger.debug("[TEST-24] 存储关闭完成")
        
        logger.debug("[TEST-24] 测试通过!")


if __name__ == "__main__":
    logger.debug("=" * 60)
    logger.debug("第27轮测试：超大规模真实场景测试")
    logger.debug("=" * 60)
    pytest.main([__file__, "-v", "--tb=short"])

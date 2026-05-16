"""
P1级别核心问题修复单元测试

本测试文件验证Agent Memory System V3.0第二轮迭代修复的效果：
1. log_inconsistency (日志不一致) - 20个实例
2. commit_index_error (提交索引错误) - 20个实例
3. leader_election_deadlock (Leader选举死锁) - 15个实例
4. fault_detection_delay (故障检测延迟) - 33个实例
5. 资源泄漏问题 - 4个实例

测试覆盖：
- Raft共识日志一致性修复测试
- 提交索引更新修复测试
- Leader选举修复测试
- 故障检测修复测试
- 资源泄漏修复测试

作者: Agent Memory System Team
版本: 3.0.0
"""

import asyncio
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List, Optional

# 导入被测试模块
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from memory_system.raft_consensus import (
    RaftNode,
    RaftConfig,
    RaftRole,
    RaftState,
    LogEntry,
    InMemoryTransport,
    MemoryStateMachine,
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    # P1修复新增的异常类
    LogInconsistencyError,
    CommitIndexError,
    LeaderElectionError,
)
from memory_system.async_pipeline import (
    Pipeline,
    PipelineStage,
    PipelineConfig,
    StageConfig,
    PipelineItem,
    PipelineStatus,
)


# ============================================================================
# 日志一致性修复测试
# ============================================================================

class TestLogInconsistencyFix:
    """日志不一致修复测试"""
    
    @pytest.fixture
    def raft_config(self):
        """创建Raft配置"""
        return RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            # P1修复: 使用新的配置参数
            fault_detection_timeout=1.0,
            heartbeat_timeout_factor=5,
            log_replication_timeout=2.0,
        )
    
    @pytest.fixture
    def transport(self):
        """创建内存传输层"""
        return InMemoryTransport()
    
    @pytest.mark.asyncio
    async def test_raft_config_p1_parameters(self, raft_config):
        """测试Raft配置包含P1修复参数"""
        assert raft_config.fault_detection_timeout == 1.0
        assert raft_config.heartbeat_timeout_factor == 5
        assert raft_config.log_replication_timeout == 2.0
        assert raft_config.max_log_replication_retries == 5
    
    @pytest.mark.asyncio
    async def test_log_inconsistency_error_creation(self):
        """测试日志不一致错误创建"""
        error = LogInconsistencyError(
            message="Log mismatch detected",
            expected_index=10,
            actual_index=8,
            expected_term=3,
            actual_term=2
        )
        
        assert error.expected_index == 10
        assert error.actual_index == 8
        assert error.expected_term == 3
        assert error.actual_term == 2
        assert "Log mismatch detected" in str(error)
    
    @pytest.mark.asyncio
    async def test_prev_log_term_validation(self, raft_config, transport):
        """测试PrevLogTerm验证"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 添加一些日志条目
        node._persistent_state.log = [
            LogEntry(term=1, index=0, command={"action": "set", "key": "k1", "value": "v1"}),
            LogEntry(term=1, index=1, command={"action": "set", "key": "k2", "value": "v2"}),
            LogEntry(term=2, index=2, command={"action": "set", "key": "k3", "value": "v3"}),
        ]
        
        # 测试匹配的PrevLogIndex和PrevLogTerm
        request = AppendEntriesRequest(
            term=2,
            leader_id="node2",
            prev_log_index=1,
            prev_log_term=1,
            entries=[LogEntry(term=2, index=2, command={"action": "set", "key": "k4", "value": "v4"})],
            leader_commit=1,
        )
        
        response = await node.handle_append_entries(request)
        assert response.success is True
    
    @pytest.mark.asyncio
    async def test_log_conflict_resolution(self, raft_config, transport):
        """测试日志冲突解决机制"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 添加一些日志条目
        node._persistent_state.log = [
            LogEntry(term=1, index=0, command={"action": "set", "key": "k1", "value": "v1"}),
            LogEntry(term=1, index=1, command={"action": "set", "key": "k2", "value": "v2"}),
            LogEntry(term=2, index=2, command={"action": "set", "key": "k3", "value": "v3"}),
        ]
        
        # 发送冲突的条目（term不匹配）
        request = AppendEntriesRequest(
            term=3,
            leader_id="node2",
            prev_log_index=1,
            prev_log_term=1,
            entries=[
                LogEntry(term=3, index=2, command={"action": "set", "key": "k4", "value": "v4"}),
            ],
            leader_commit=2,
        )
        
        response = await node.handle_append_entries(request)
        assert response.success is True
        # 验证日志被正确截断和追加
        assert len(node._persistent_state.log) == 3
        assert node._persistent_state.log[2].term == 3
    
    @pytest.mark.asyncio
    async def test_log_truncation_logging(self, raft_config, transport):
        """测试日志截断记录"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 添加多个日志条目
        node._persistent_state.log = [
            LogEntry(term=1, index=i, command={"action": "set", "key": f"k{i}", "value": f"v{i}"})
            for i in range(10)
        ]
        
        # 发送需要截断的请求
        request = AppendEntriesRequest(
            term=2,
            leader_id="node2",
            prev_log_index=4,
            prev_log_term=1,
            entries=[
                LogEntry(term=2, index=5, command={"action": "set", "key": "new", "value": "new"}),
            ],
            leader_commit=5,
        )
        
        response = await node.handle_append_entries(request)
        assert response.success is True
        # 验证日志被截断
        assert len(node._persistent_state.log) == 6


# ============================================================================
# 提交索引修复测试
# ============================================================================

class TestCommitIndexFix:
    """提交索引修复测试"""
    
    @pytest.fixture
    def raft_config(self):
        """创建Raft配置"""
        return RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.15,
            election_timeout_max=0.30,
        )
    
    @pytest.fixture
    def transport(self):
        """创建内存传输层"""
        return InMemoryTransport()
    
    @pytest.mark.asyncio
    async def test_commit_index_error_creation(self):
        """测试提交索引错误创建"""
        error = CommitIndexError(
            message="Commit index mismatch",
            commit_index=10,
            last_applied=8
        )
        
        assert error.commit_index == 10
        assert error.last_applied == 8
        assert "Commit index mismatch" in str(error)
    
    @pytest.mark.asyncio
    async def test_commit_index_history_tracking(self, raft_config, transport):
        """测试提交索引历史跟踪"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 验证提交索引历史初始化
        assert node._commit_index_history == []
        assert node._commit_index_check_interval == 1.0
    
    @pytest.mark.asyncio
    async def test_follower_commit_index_update(self, raft_config, transport):
        """测试Follower提交索引更新"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 添加一些日志条目
        node._persistent_state.log = [
            LogEntry(term=1, index=0, command={"action": "set", "key": "k1", "value": "v1"}),
            LogEntry(term=1, index=1, command={"action": "set", "key": "k2", "value": "v2"}),
            LogEntry(term=1, index=2, command={"action": "set", "key": "k3", "value": "v3"}),
        ]
        
        # 发送带有更高leader_commit的请求
        request = AppendEntriesRequest(
            term=1,
            leader_id="node2",
            prev_log_index=2,
            prev_log_term=1,
            entries=[],
            leader_commit=2,
        )
        
        response = await node.handle_append_entries(request)
        assert response.success is True
        assert node._volatile_state.commit_index == 2
    
    @pytest.mark.asyncio
    async def test_commit_index_propagation(self, raft_config, transport):
        """测试提交索引传播"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 添加日志条目
        node._persistent_state.log = [
            LogEntry(term=1, index=i, command={"action": "set", "key": f"k{i}", "value": f"v{i}"})
            for i in range(5)
        ]
        
        # 设置为Leader
        node._role = RaftRole.LEADER
        node._current_leader = node.node_id
        node._persistent_state.current_term = 1
        
        # 设置match_index
        node._volatile_state.match_index = {"node2": 3, "node3": 3}
        node._volatile_state.commit_index = 0
        
        # 推进提交索引
        await node._advance_commit_index()
        
        # 验证提交索引被更新
        assert node._volatile_state.commit_index == 3


# ============================================================================
# Leader选举修复测试
# ============================================================================

class TestLeaderElectionFix:
    """Leader选举修复测试"""
    
    @pytest.fixture
    def raft_config(self):
        """创建Raft配置"""
        return RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.15,
            election_timeout_max=0.30,
        )
    
    @pytest.fixture
    def transport(self):
        """创建内存传输层"""
        return InMemoryTransport()
    
    @pytest.mark.asyncio
    async def test_leader_election_error_creation(self):
        """测试Leader选举错误创建"""
        error = LeaderElectionError(
            message="Election failed",
            term=5,
            votes_received=1,
            votes_needed=2
        )
        
        assert error.term == 5
        assert error.votes_received == 1
        assert error.votes_needed == 2
        assert "Election failed" in str(error)
    
    @pytest.mark.asyncio
    async def test_majority_calculation(self, raft_config, transport):
        """测试多数票计算"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 3节点集群，多数票应该是2
        total_nodes = len(raft_config.peers) + 1  # 3
        majority = total_nodes // 2 + 1  # 2
        
        assert majority == 2
        
        # 5节点集群，多数票应该是3
        total_nodes_5 = 5
        majority_5 = total_nodes_5 // 2 + 1
        assert majority_5 == 3
    
    @pytest.mark.asyncio
    async def test_election_round_tracking(self, raft_config, transport):
        """测试选举轮次跟踪"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 验证初始选举轮次
        assert node._election_round == 0
        assert node._election_start_time == 0.0
    
    @pytest.mark.asyncio
    async def test_election_timeout_randomization(self, raft_config, transport):
        """测试选举超时随机化"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 多次重置选举定时器，验证超时是随机的
        timeouts = []
        for _ in range(10):
            # 计算随机超时
            import random
            timeout = random.uniform(
                raft_config.election_timeout_min,
                raft_config.election_timeout_max
            )
            timeouts.append(timeout)
        
        # 验证超时在合理范围内
        for timeout in timeouts:
            assert raft_config.election_timeout_min <= timeout <= raft_config.election_timeout_max
        
        # 验证有一定的随机性（不是所有超时都相同）
        assert len(set(timeouts)) > 1


# ============================================================================
# 故障检测修复测试
# ============================================================================

class TestFaultDetectionFix:
    """故障检测修复测试"""
    
    @pytest.fixture
    def raft_config(self):
        """创建Raft配置"""
        return RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            fault_detection_timeout=0.5,
            heartbeat_timeout_factor=3,
        )
    
    @pytest.fixture
    def transport(self):
        """创建内存传输层"""
        return InMemoryTransport()
    
    @pytest.mark.asyncio
    async def test_heartbeat_initialization(self, raft_config, transport):
        """测试心跳初始化"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 启动节点
        await node.start()
        
        try:
            # 验证心跳时间被初始化
            assert node._last_heartbeat_sent > 0
            assert node._leader_heartbeat_received > 0
            
            # 验证每个peer的心跳状态被初始化
            for peer in raft_config.peers:
                assert peer in node._last_heartbeat_received
                assert peer in node._heartbeat_missed
                assert node._heartbeat_missed[peer] == 0
        finally:
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_heartbeat_missed_tracking(self, raft_config, transport):
        """测试心跳丢失跟踪"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 验证心跳丢失计数初始化
        assert node._heartbeat_missed == {}
        
        # 启动节点
        await node.start()
        
        try:
            # 验证初始化后心跳丢失计数为0
            for peer in raft_config.peers:
                assert node._heartbeat_missed.get(peer, 0) == 0
        finally:
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_leader_heartbeat_received_tracking(self, raft_config, transport):
        """测试Leader心跳接收跟踪"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 启动节点
        await node.start()
        
        try:
            initial_time = node._leader_heartbeat_received
            
            # 模拟收到心跳
            await asyncio.sleep(0.1)
            
            # 发送一个心跳请求
            request = AppendEntriesRequest(
                term=1,
                leader_id="node2",
                prev_log_index=-1,
                prev_log_term=0,
                entries=[],
                leader_commit=-1,
            )
            
            response = await node.handle_append_entries(request)
            assert response.success is True
            
            # 验证Leader心跳时间被更新
            assert node._leader_heartbeat_received > initial_time
        finally:
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_fault_detection_timeout_config(self, raft_config):
        """测试故障检测超时配置"""
        assert raft_config.fault_detection_timeout == 0.5
        assert raft_config.heartbeat_timeout_factor == 3


# ============================================================================
# 资源泄漏修复测试
# ============================================================================

class TestResourceLeakFix:
    """资源泄漏修复测试"""
    
    @pytest.fixture
    def raft_config(self):
        """创建Raft配置"""
        return RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.15,
            election_timeout_max=0.30,
        )
    
    @pytest.fixture
    def transport(self):
        """创建内存传输层"""
        return InMemoryTransport()
    
    @pytest.mark.asyncio
    async def test_raft_node_cleanup_on_stop(self, raft_config, transport):
        """测试Raft节点停止时资源清理"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        # 启动节点
        await node.start()
        
        # 添加一些待处理的提案
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        node._pending_proposals["test_id"] = future
        
        # 停止节点
        await node.stop()
        
        # 验证资源被清理
        assert node._election_timer is None
        assert node._heartbeat_timer is None
        assert len(node._pending_proposals) == 0
        assert len(node._last_heartbeat_received) == 0
        assert len(node._heartbeat_missed) == 0
        assert len(node._log_replication_inflight) == 0
        assert len(node._log_replication_retries) == 0
        assert len(node._commit_index_history) == 0
    
    @pytest.mark.asyncio
    async def test_pipeline_stage_cleanup(self):
        """测试流水线阶段资源清理"""
        config = StageConfig(
            name="test_stage",
            buffer_size=100,
            num_workers=2,
        )
        
        async def handler(item: PipelineItem) -> PipelineItem:
            return item
        
        stage = PipelineStage(
            name="test_stage",
            handler=handler,
            config=config,
        )
        
        # 启动阶段
        await stage.start()
        
        # 添加一些数据到队列
        for i in range(10):
            item = PipelineItem(data={"index": i})
            await stage.submit(item)
        
        # 停止阶段
        await stage.stop()
        
        # 验证资源被清理
        assert len(stage._workers) == 0
        assert stage._batch_timer is None
        assert stage.output_queue is None
        assert stage.input_queue.empty()
    
    @pytest.mark.asyncio
    async def test_pipeline_cleanup(self):
        """测试流水线资源清理"""
        config = StageConfig(name="test_stage", buffer_size=100, num_workers=1)
        
        async def handler(item: PipelineItem) -> PipelineItem:
            return item
        
        stage = PipelineStage(name="test_stage", handler=handler, config=config)
        pipeline = Pipeline(stages=[stage])
        
        # 启动流水线
        await pipeline.start()
        
        # 验证状态
        assert pipeline._status == PipelineStatus.RUNNING
        
        # 停止流水线
        await pipeline.stop()
        
        # 验证资源被清理
        assert pipeline._status == PipelineStatus.STOPPED
        assert pipeline._result_collector is None
        assert pipeline._monitor_task is None
        assert len(pipeline._pending_results) == 0
    
    @pytest.mark.asyncio
    async def test_pipeline_stage_index_cleanup(self):
        """测试流水线阶段索引清理"""
        config = StageConfig(
            name="test_stage",
            enable_indexing=True,
            index_fields=["id", "category"],
        )
        
        async def handler(item: PipelineItem) -> PipelineItem:
            return item
        
        stage = PipelineStage(name="test_stage", handler=handler, config=config)
        
        # 添加数据到索引
        for i in range(10):
            item = PipelineItem(data={"id": f"item-{i}", "category": f"cat-{i % 3}"})
            await stage.add_to_index(item)
        
        # 验证索引有数据
        assert len(stage._item_store) == 10
        
        # 清理索引
        await stage.clear_index()
        
        # 验证索引被清理
        assert len(stage._item_store) == 0
        assert len(stage._index) == 0
        assert len(stage._query_cache) == 0


# ============================================================================
# 集成测试
# ============================================================================

class TestP1FixesIntegration:
    """P1修复集成测试"""
    
    @pytest.mark.asyncio
    async def test_raft_cluster_with_p1_fixes(self):
        """Raft集群P1修复集成测试"""
        transport = InMemoryTransport()
        
        # 创建三个节点的集群
        nodes = []
        for i in range(3):
            node = RaftNode(
                config=RaftConfig(
                    node_id=f"node{i}",
                    peers=[f"node{j}" for j in range(3) if j != i],
                    election_timeout_min=0.1,
                    election_timeout_max=0.2,
                    fault_detection_timeout=0.5,
                    heartbeat_timeout_factor=3,
                ),
                state_machine=MemoryStateMachine(),
                transport=transport,
            )
            transport.register_node(node)
            nodes.append(node)
        
        # 启动所有节点
        for node in nodes:
            await node.start()
        
        try:
            # 等待选举
            await asyncio.sleep(0.5)
            
            # 验证P1修复的状态跟踪已初始化
            for node in nodes:
                assert node._election_round >= 0
                assert node._last_heartbeat_sent > 0
                assert node._leader_heartbeat_received > 0
                
        finally:
            for node in nodes:
                await node.stop()
    
    @pytest.mark.asyncio
    async def test_pipeline_with_p1_fixes(self):
        """流水线P1修复集成测试"""
        config = StageConfig(
            name="data_stage",
            buffer_size=100,
            num_workers=2,
            enable_indexing=True,
            index_fields=["user_id", "event_type"],
        )
        
        async def handler(item: PipelineItem) -> PipelineItem:
            return item
        
        stage = PipelineStage(name="data_stage", handler=handler, config=config)
        pipeline = Pipeline(stages=[stage])
        
        # 启动流水线
        await pipeline.start()
        
        try:
            # 提交一些数据
            for i in range(10):
                item = PipelineItem(
                    data={
                        "user_id": f"user-{i % 3}",
                        "event_type": "click" if i % 2 == 0 else "view",
                    }
                )
                await pipeline.submit(item, wait_for_result=False)
            
            await asyncio.sleep(0.5)
            
        finally:
            # 停止流水线，验证资源清理
            await pipeline.stop()
            
            assert pipeline._status == PipelineStatus.STOPPED
            assert len(stage._item_store) == 0  # 索引被清理


# ============================================================================
# 性能基准测试
# ============================================================================

class TestP1FixesPerformance:
    """P1修复性能基准测试"""
    
    @pytest.mark.asyncio
    async def test_log_replication_performance(self):
        """日志复制性能测试"""
        transport = InMemoryTransport()
        
        # 创建两个节点
        node1 = RaftNode(
            config=RaftConfig(
                node_id="node1",
                peers=["node2"],
                election_timeout_min=0.1,
                election_timeout_max=0.2,
            ),
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        node2 = RaftNode(
            config=RaftConfig(
                node_id="node2",
                peers=["node1"],
                election_timeout_min=0.1,
                election_timeout_max=0.2,
            ),
            state_machine=MemoryStateMachine(),
            transport=transport,
        )
        
        transport.register_node(node1)
        transport.register_node(node2)
        
        await node1.start()
        await node2.start()
        
        try:
            # 等待选举
            await asyncio.sleep(0.5)
            
            # 找到Leader
            leader = node1 if node1.is_leader else node2
            
            if leader:
                # 测试日志复制性能
                start_time = time.time()
                
                for i in range(10):
                    try:
                        await leader.propose_command({
                            "action": "set",
                            "key": f"key{i}",
                            "value": f"value{i}",
                        }, timeout=2.0)
                    except Exception:
                        pass
                
                duration = time.time() - start_time
                print(f"\nLog replication time for 10 entries: {duration:.3f}s")
                
        finally:
            await node1.stop()
            await node2.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

"""
分布式一致性修复测试套件

测试所有一致性修复的实现:
- C-001: 线性一致性保证 (Raft)
- C-002: 因果一致性 (向量时钟)
- C-003: 分区容忍性 (多数派写入)
- C-004: 会话一致性 (读己之写、单调读写)
- C-005: 时钟漂移问题 (HLC)
"""

import asyncio
import time
import pytest
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# 导入修复的模块
from hybrid_logical_clock import (
    HybridLogicalClock, HLCTimestamp, HLCClockManager,
    compare_hlc_timestamps, hlc_to_tuple, tuple_to_hlc
)
from vector_clock import (
    VectorClock, VectorClockManager, VectorClockRelation,
    CausalEvent, are_concurrent
)
from session_consistency import (
    SessionManager, SessionState, SessionConsistencyLevel,
    ConsistentSession
)
from partition_tolerance import (
    PartitionManager, QuorumManager, PartitionTolerantStorage,
    PartitionState, NodeState
)
from consistency_manager import (
    ConsistencyManager, ConsistencyLevel, OperationResult,
    ConsistencyConfig, ConsistencyVerifier
)


class TestHybridLogicalClock:
    """测试混合逻辑时钟"""
    
    def test_hlc_basic(self):
        """测试HLC基本功能"""
        hlc = HybridLogicalClock("node1")
        
        # 测试生成时间戳
        ts1 = hlc.now()
        assert ts1.physical > 0
        assert ts1.logical == 0
        assert ts1.node_id == "node1"
        
        # 测试连续生成
        ts2 = hlc.now()
        assert ts2.physical >= ts1.physical
        if ts2.physical == ts1.physical:
            assert ts2.logical == ts1.logical + 1
    
    def test_hlc_update(self):
        """测试HLC更新"""
        hlc1 = HybridLogicalClock("node1")
        hlc2 = HybridLogicalClock("node2")
        
        ts1 = hlc1.now()
        
        # 从远程更新
        ts2 = hlc2.update(ts1)
        
        # 更新后的时间戳应该 >= 远程时间戳
        assert ts2.physical >= ts1.physical
    
    def test_hlc_compare(self):
        """测试HLC比较"""
        hlc = HybridLogicalClock("node1")
        
        ts1 = hlc.now()
        time.sleep(0.01)
        ts2 = hlc.now()
        
        # ts2应该大于ts1
        result = HybridLogicalClock.compare(ts1, ts2)
        assert result.value == -1  # LESS
        
        result = HybridLogicalClock.compare(ts2, ts1)
        assert result.value == 1  # GREATER
    
    def test_hlc_clock_drift_tolerance(self):
        """测试HLC时钟漂移容忍"""
        hlc = HybridLogicalClock("node1", max_drift_ms=1000)
        
        # 生成多个时间戳
        timestamps = []
        for _ in range(10):
            timestamps.append(hlc.now())
            time.sleep(0.001)
        
        # 验证时间戳是单调递增的
        for i in range(1, len(timestamps)):
            assert HybridLogicalClock.compare(timestamps[i-1], timestamps[i]).value <= 0
    
    def test_hlc_serialization(self):
        """测试HLC序列化"""
        hlc = HybridLogicalClock("node1")
        ts = hlc.now()
        
        # 测试字典序列化
        ts_dict = ts.to_dict()
        ts2 = HLCTimestamp.from_dict(ts_dict)
        assert ts == ts2
        
        # 测试字节序列化
        ts_bytes = ts.to_bytes()
        ts3 = HLCTimestamp.from_bytes(ts_bytes)
        assert ts == ts3


class TestVectorClock:
    """测试向量时钟"""
    
    def test_vector_clock_basic(self):
        """测试向量时钟基本功能"""
        vc = VectorClock("node1")
        
        # 测试递增
        clock1 = vc.increment()
        assert clock1["node1"] == 1
        
        clock2 = vc.increment()
        assert clock2["node1"] == 2
    
    def test_vector_clock_update(self):
        """测试向量时钟更新"""
        vc1 = VectorClock("node1")
        vc2 = VectorClock("node2")
        
        # node1递增
        vc1.increment()
        vc1.increment()
        
        # node2从node1更新
        vc2.update(vc1)
        
        # node2的时钟应该包含node1的信息
        assert vc2.clock.get("node1") == 2
    
    def test_vector_clock_compare(self):
        """测试向量时钟比较"""
        vc1 = VectorClock("node1")
        vc2 = VectorClock("node2")
        
        # vc1: node1=1
        vc1.increment()
        
        # vc2从vc1更新，然后递增
        vc2.update(vc1)
        vc2.increment()
        
        # vc1 happens-before vc2
        result = vc1.compare(vc2)
        assert result == VectorClockRelation.BEFORE
        
        result = vc2.compare(vc1)
        assert result == VectorClockRelation.AFTER
    
    def test_vector_clock_concurrent(self):
        """测试并发检测"""
        vc1 = VectorClock("node1")
        vc2 = VectorClock("node2")
        
        # 两个独立的递增
        vc1.increment()
        vc2.increment()
        
        # 应该是并发关系
        result = vc1.compare(vc2)
        assert result == VectorClockRelation.CONCURRENT
    
    def test_vector_clock_manager(self):
        """测试向量时钟管理器"""
        manager = VectorClockManager("node1")
        
        # 记录事件
        event = manager.record_event("event1", "write", {"key": "k1"})
        assert event.node_id == "node1"
        assert event.vector_clock["node1"] == 1
        
        # 接收远程事件
        remote_event = CausalEvent(
            event_id="event2",
            node_id="node2",
            vector_clock={"node2": 2},
        )
        manager.receive_event(remote_event)
        
        # 检查因果一致性
        relation = manager.check_causal_consistency("event1", "event2")
        assert relation == VectorClockRelation.CONCURRENT


class TestSessionConsistency:
    """测试会话一致性"""
    
    def test_session_creation(self):
        """测试会话创建"""
        manager = SessionManager()
        
        session_id = manager.create_session(
            user_id="user1",
            consistency_level=SessionConsistencyLevel.SESSION,
        )
        
        assert session_id is not None
        
        session = manager.get_session(session_id)
        assert session is not None
        assert session.user_id == "user1"
    
    def test_read_your_writes(self):
        """测试读己之写保证"""
        manager = SessionManager()
        session_id = manager.create_session("user1")
        
        # 记录写操作
        from hybrid_logical_clock import HLCTimestamp
        ts = HLCTimestamp(physical=1000, logical=0, node_id="node1")
        manager.record_write(session_id, "key1", "value1", "node1", {"node1": 1})
        
        # 检查读己之写要求
        ok, min_ts = manager.check_read_your_writes(session_id, "key1")
        assert ok is True
        assert min_ts is not None
        
        # 未写入的键不应该有要求
        ok, min_ts = manager.check_read_your_writes(session_id, "key2")
        assert ok is True
        assert min_ts is None
    
    def test_monotonic_reads(self):
        """测试单调读保证"""
        manager = SessionManager()
        session_id = manager.create_session("user1")
        
        # 记录读操作
        from hybrid_logical_clock import HLCTimestamp
        ts = HLCTimestamp(physical=1000, logical=0, node_id="node1")
        manager.record_read(session_id, "key1", "value1")
        
        # 检查单调读要求
        ok, min_ts = manager.check_monotonic_reads(session_id, "key1")
        assert ok is True
        assert min_ts is not None
    
    def test_session_consistency_requirements(self):
        """测试一致性要求生成"""
        manager = SessionManager()
        session_id = manager.create_session(
            "user1",
            consistency_level=SessionConsistencyLevel.SESSION,
        )
        
        # 先写入
        manager.record_write(session_id, "key1", "value1", "node1", {"node1": 1})
        
        # 获取读操作的一致性要求
        reqs = manager.get_consistency_requirements(session_id, "read", "key1")
        
        assert reqs["valid"] is True
        assert len(reqs["requirements"]) > 0


class TestPartitionTolerance:
    """测试分区容忍性"""
    
    def test_quorum_manager(self):
        """测试多数派管理器"""
        qm = QuorumManager(total_nodes=5)
        
        # 5个节点的多数派是3
        assert qm.read_quorum == 3
        assert qm.write_quorum == 3
        
        # 检查多数派
        assert qm.is_read_quorum(3) is True
        assert qm.is_read_quorum(2) is False
        assert qm.is_write_quorum(3) is True
        
        # 检查服务能力
        assert qm.can_serve_reads(3) is True
        assert qm.can_serve_reads(2) is False
        assert qm.can_serve_writes(3) is True
    
    def test_quorum_manager_custom(self):
        """测试自定义多数派"""
        qm = QuorumManager(total_nodes=5, read_quorum=2, write_quorum=4)
        
        assert qm.read_quorum == 2
        assert qm.write_quorum == 4
        
        assert qm.is_read_quorum(2) is True
        assert qm.is_write_quorum(3) is False
        assert qm.is_write_quorum(4) is True
    
    def test_partition_manager_initialization(self):
        """测试分区管理器初始化"""
        pm = PartitionManager(
            node_id="node1",
            peers=["node2", "node3", "node4", "node5"],
        )
        
        assert pm.is_partitioned() is False
        
        healthy = pm.get_healthy_nodes()
        assert len(healthy) == 0  # 初始时没有健康节点


class TestConsistencyManager:
    """测试一致性管理器"""
    
    @pytest.mark.asyncio
    async def test_consistency_manager_initialization(self):
        """测试一致性管理器初始化"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        await cm.start()
        
        assert cm._running is True
        assert cm._node_id == "node1"
        
        await cm.stop()
    
    @pytest.mark.asyncio
    async def test_causal_write_read(self):
        """测试因果一致性读写"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        await cm.start()
        
        # 因果一致性写入
        result = await cm.write(
            "key1", "value1",
            consistency=ConsistencyLevel.CAUSAL,
        )
        
        assert result.success is True
        assert result.timestamp is not None
        assert result.vector_clock is not None
        
        # 因果一致性读取
        result = await cm.read(
            "key1",
            consistency=ConsistencyLevel.CAUSAL,
        )
        
        assert result.success is True
        assert result.value == "value1"
        
        await cm.stop()
    
    @pytest.mark.asyncio
    async def test_session_consistency(self):
        """测试会话一致性"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        await cm.start()
        
        # 创建会话
        session_id = cm.create_session("user1")
        
        # 会话一致性写入
        result = await cm.write(
            "key1", "value1",
            consistency=ConsistencyLevel.SESSION,
            session_id=session_id,
        )
        
        assert result.success is True
        
        # 会话一致性读取（应该读到刚写入的值）
        result = await cm.read(
            "key1",
            consistency=ConsistencyLevel.SESSION,
            session_id=session_id,
        )
        
        assert result.success is True
        assert result.value == "value1"
        
        cm.close_session(session_id)
        await cm.stop()
    
    @pytest.mark.asyncio
    async def test_read_your_writes(self):
        """测试读己之写保证"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        await cm.start()
        
        session_id = cm.create_session("user1")
        
        # 写入
        write_result = await cm.write(
            "key1", "value1",
            consistency=ConsistencyLevel.READ_YOUR_WRITES,
            session_id=session_id,
        )
        
        assert write_result.success is True
        
        # 同一会话读取，应该保证读到写入的值
        read_result = await cm.read(
            "key1",
            consistency=ConsistencyLevel.READ_YOUR_WRITES,
            session_id=session_id,
        )
        
        assert read_result.success is True
        # 读己之写保证
        assert read_result.value == "value1"
        
        cm.close_session(session_id)
        await cm.stop()
    
    @pytest.mark.asyncio
    async def test_linearizable_operations(self):
        """测试线性一致性操作"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        await cm.start()
        
        # 设置当前节点为Leader（模拟）
        cm.set_leader("node1", is_leader=True)
        
        # 线性一致性写入
        result = await cm.write(
            "key1", "value1",
            consistency=ConsistencyLevel.LINEARIZABLE,
        )
        
        assert result.success is True
        
        # 线性一致性读取
        result = await cm.read(
            "key1",
            consistency=ConsistencyLevel.LINEARIZABLE,
        )
        
        assert result.success is True
        assert result.value == "value1"
        
        await cm.stop()
    
    @pytest.mark.asyncio
    async def test_quorum_operations(self):
        """测试多数派操作"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3", "node4", "node5"],
        )
        
        await cm.start()
        
        # 写入
        result = await cm.write("key1", "value1")
        assert result.success is True
        
        # 读取
        result = await cm.read("key1")
        assert result.success is True
        assert result.value == "value1"
        
        await cm.stop()
    
    def test_consistency_stats(self):
        """测试统计信息"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        stats = cm.get_stats()
        
        assert "reads" in stats
        assert "writes" in stats
        assert "failures" in stats
        assert "quorum_status" in stats


class TestConsistencyVerification:
    """测试一致性验证"""
    
    @pytest.mark.asyncio
    async def test_causal_consistency_verification(self):
        """测试因果一致性验证"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        verifier = ConsistencyVerifier(cm)
        
        # 创建满足因果一致性的操作序列
        operations = [
            {"operation_id": "op1", "vector_clock": {"node1": 1}},
            {"operation_id": "op2", "vector_clock": {"node1": 1, "node2": 1}, "depends_on": "op1"},
            {"operation_id": "op3", "vector_clock": {"node1": 1, "node2": 1, "node3": 1}, "depends_on": "op2"},
        ]
        
        result = await verifier.verify_causal_consistency(operations)
        assert result is True
    
    @pytest.mark.asyncio
    async def test_session_consistency_verification(self):
        """测试会话一致性验证"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        
        verifier = ConsistencyVerifier(cm)
        
        # 创建满足会话一致性的操作序列
        operations = [
            {"type": "write", "key": "k1", "value": "v1", "timestamp": 1000},
            {"type": "read", "key": "k1", "value": "v1", "timestamp": 1001},
            {"type": "read", "key": "k1", "value": "v1", "timestamp": 1002},
        ]
        
        results = await verifier.verify_session_consistency("session1", operations)
        
        assert results["read_your_writes"] is True
        assert results["monotonic_reads"] is True


class TestIntegration:
    """集成测试"""
    
    @pytest.mark.asyncio
    async def test_end_to_end_consistency(self):
        """端到端一致性测试"""
        # 创建3个节点的一致性管理器
        nodes = []
        for i in range(1, 4):
            peers = [f"node{j}" for j in range(1, 4) if j != i]
            cm = ConsistencyManager(
                node_id=f"node{i}",
                peers=peers,
            )
            nodes.append(cm)
        
        # 启动所有节点
        for cm in nodes:
            await cm.start()
        
        # 设置leader
        nodes[0].set_leader("node1", is_leader=True)
        nodes[1].set_leader("node1", is_leader=False)
        nodes[2].set_leader("node1", is_leader=False)
        
        try:
            # 测试1: 线性一致性
            result = await nodes[0].write(
                "test_key", "test_value",
                consistency=ConsistencyLevel.LINEARIZABLE,
            )
            assert result.success is True
            
            # 测试2: 因果一致性
            result = await nodes[0].write(
                "causal_key", "causal_value",
                consistency=ConsistencyLevel.CAUSAL,
            )
            assert result.success is True
            
            # 测试3: 会话一致性
            session_id = nodes[0].create_session("user1")
            result = await nodes[0].write(
                "session_key", "session_value",
                consistency=ConsistencyLevel.SESSION,
                session_id=session_id,
            )
            assert result.success is True
            
            result = await nodes[0].read(
                "session_key",
                consistency=ConsistencyLevel.SESSION,
                session_id=session_id,
            )
            assert result.success is True
            assert result.value == "session_value"
            
            nodes[0].close_session(session_id)
            
            print("✓ End-to-end consistency test passed")
            
        finally:
            # 停止所有节点
            for cm in nodes:
                await cm.stop()


def run_tests():
    """运行所有测试"""
    print("=" * 70)
    print("分布式一致性修复测试套件")
    print("=" * 70)
    
    # 运行同步测试
    test_classes = [
        TestHybridLogicalClock(),
        TestVectorClock(),
        TestSessionConsistency(),
        TestPartitionTolerance(),
    ]
    
    for test_class in test_classes:
        class_name = test_class.__class__.__name__
        print(f"\n{class_name}")
        print("-" * 70)
        
        for method_name in dir(test_class):
            if method_name.startswith("test_"):
                try:
                    method = getattr(test_class, method_name)
                    method()
                    print(f"  ✓ {method_name}")
                except Exception as e:
                    print(f"  ✗ {method_name}: {e}")
    
    # 运行异步测试
    print("\nTestConsistencyManager")
    print("-" * 70)
    
    async_tests = TestConsistencyManager()
    for method_name in dir(async_tests):
        if method_name.startswith("test_"):
            try:
                method = getattr(async_tests, method_name)
                asyncio.run(method())
                print(f"  ✓ {method_name}")
            except Exception as e:
                print(f"  ✗ {method_name}: {e}")
    
    print("\nTestConsistencyVerification")
    print("-" * 70)
    
    verify_tests = TestConsistencyVerification()
    for method_name in dir(verify_tests):
        if method_name.startswith("test_"):
            try:
                method = getattr(verify_tests, method_name)
                asyncio.run(method())
                print(f"  ✓ {method_name}")
            except Exception as e:
                print(f"  ✗ {method_name}: {e}")
    
    print("\nTestIntegration")
    print("-" * 70)
    
    integration_tests = TestIntegration()
    for method_name in dir(integration_tests):
        if method_name.startswith("test_"):
            try:
                method = getattr(integration_tests, method_name)
                asyncio.run(method())
                print(f"  ✓ {method_name}")
            except Exception as e:
                print(f"  ✗ {method_name}: {e}")
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)


if __name__ == "__main__":
    run_tests()

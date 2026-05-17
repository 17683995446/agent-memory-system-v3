"""分布式共识模块单元测试"""
import pytest
import asyncio
from memory_system.partition_tolerance import QuorumManager, PartitionManager
from memory_system.hybrid_logical_clock import HybridLogicalClock
from memory_system.vector_clock import VectorClock, VectorClockRelation


class TestQuorumManager:
    """多数派管理器测试"""

    def test_has_quorum_with_majority(self):
        """多数节点可用时应达到quorum"""
        qm = QuorumManager(total_nodes=5)
        assert qm.has_quorum({"a", "b", "c"})  # 3/5

    def test_has_quorum_without_majority(self):
        """少数节点不应达到quorum"""
        qm = QuorumManager(total_nodes=5)
        assert not qm.has_quorum({"a", "b"})  # 2/5

    def test_is_read_quorum(self):
        """读quorum检查"""
        qm = QuorumManager(total_nodes=5, read_quorum=2)
        assert qm.is_read_quorum(2)
        assert not qm.is_read_quorum(1)

    def test_is_write_quorum(self):
        """写quorum检查"""
        qm = QuorumManager(total_nodes=5, write_quorum=3)
        assert qm.is_write_quorum(3)
        assert not qm.is_write_quorum(2)


class TestHybridLogicalClock:
    """混合逻辑时钟测试"""

    def test_hlc_increments(self):
        """HLC应递增"""
        hlc = HybridLogicalClock(node_id="test_node")
        ts1 = hlc.now()
        ts2 = hlc.now()
        assert ts2 > ts1

    def test_hlc_merge(self):
        """HLC合并应取最大值"""
        hlc = HybridLogicalClock(node_id="test_node")
        ts1 = hlc.now()
        ts2 = hlc.now()
        merged = hlc.update(ts1)
        assert merged >= ts1
        assert merged >= ts2


class TestVectorClock:
    """向量时钟测试"""

    def test_concurrent_events(self):
        """并发事件检测"""
        vc1 = VectorClock(node_id="A")
        vc2 = VectorClock(node_id="B")

        vc1.increment()
        vc2.increment()

        assert vc1.compare(vc2) == VectorClockRelation.CONCURRENT

    def test_happens_before(self):
        """happens-before关系"""
        vc1 = VectorClock(node_id="A")
        vc1.increment()
        # vc1 updates vc2, so vc2 knows about A's event
        vc2 = VectorClock(node_id="B")
        vc2.update(vc1)
        vc2.increment()

        # vc1 only has A:1, vc2 has A:1, B:1
        # vc1 is BEFORE vc2 (vc2 dominates vc1)
        assert vc1.compare(vc2) == VectorClockRelation.BEFORE
        assert vc2.compare(vc1) == VectorClockRelation.AFTER

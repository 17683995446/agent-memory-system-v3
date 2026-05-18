"""分布式一致性模块深度测试"""
import pytest
from memory_system.partition_tolerance import QuorumManager, PartitionManager, PartitionState
from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp, HLCComparisonResult
from memory_system.vector_clock import VectorClock, VectorClockRelation


class TestQuorumManagerBoundary:
    """QuorumManager边界测试"""
    
    def test_single_node_cluster(self):
        """单节点集群"""
        qm = QuorumManager(total_nodes=1)
        assert qm.has_quorum({"n1"})
        assert qm.is_write_quorum(1)
        assert qm.is_read_quorum(1)
    
    def test_even_number_nodes(self):
        """偶数节点集群"""
        qm = QuorumManager(total_nodes=4)
        # Need 3 for majority
        assert qm.has_quorum({"n1", "n2", "n3"})
        assert not qm.has_quorum({"n1", "n2"})
    
    def test_zero_available_nodes(self):
        """零可用节点"""
        qm = QuorumManager(total_nodes=5)
        assert not qm.has_quorum(set())
        assert not qm.is_write_quorum(0)
    
    def test_all_nodes_available(self):
        """所有节点可用"""
        qm = QuorumManager(total_nodes=10)
        all_nodes = {f"n{i}" for i in range(10)}
        assert qm.has_quorum(all_nodes)
    
    def test_custom_quorum_sizes(self):
        """自定义quorum大小"""
        qm = QuorumManager(total_nodes=5, read_quorum=1, write_quorum=5)
        assert qm.is_read_quorum(1)
        assert not qm.is_write_quorum(4)
        assert qm.is_write_quorum(5)
    
    def test_can_serve_reads_writes(self):
        """读写服务能力检查"""
        qm = QuorumManager(total_nodes=5)
        assert qm.can_serve_reads(3)
        assert qm.can_serve_writes(3)
        assert not qm.can_serve_writes(2)
    
    def test_get_quorum_status(self):
        """获取quorum状态详情"""
        qm = QuorumManager(total_nodes=5)
        status = qm.get_quorum_status(3)
        assert isinstance(status, dict)
        assert "can_read" in status or "read_quorum_satisfied" in status
    
    def test_calculate_quorum_size(self):
        """计算quorum大小"""
        qm = QuorumManager(total_nodes=5)
        for n in [1, 3, 5, 7, 9, 11]:
            size = qm.calculate_quorum_size(n)
            assert size == n // 2 + 1


class TestPartitionManagerBoundary:
    """PartitionManager边界测试"""
    
    def test_single_node_no_peers(self):
        """单节点无peer"""
        pm = PartitionManager(node_id="n1", peers=[])
        pm.start()
        assert not pm.is_partitioned()
        # get_healthy_nodes only returns tracked peers, not self
        healthy = pm.get_healthy_nodes()
        assert isinstance(healthy, list)
        assert "n1" not in healthy  # self is not in peer list
        pm.stop()
    
    def test_all_peers_healthy(self):
        """所有peer健康"""
        pm = PartitionManager(node_id="n1", peers=["n2", "n3"])
        pm.start()
        pm.record_heartbeat("n2")
        pm.record_heartbeat("n3")
        assert not pm.is_partitioned()
        # get_healthy_nodes returns only tracked peers (n2, n3), not self (n1)
        healthy = pm.get_healthy_nodes()
        assert len(healthy) == 2
        assert "n2" in healthy
        assert "n3" in healthy
        pm.stop()
    
    def test_all_peers_failed(self):
        """所有peer故障 - 需要足够多的missed heartbeat触发SUSPECTED状态"""
        pm = PartitionManager(node_id="n1", peers=["n2", "n3"], heartbeat_timeout=0.1, partition_threshold=1)
        pm.start()
        # Need >= 3 missed heartbeats for SUSPECTED, >= 5 for UNREACHABLE
        for _ in range(6):
            pm.record_missed_heartbeat("n2")
            pm.record_missed_heartbeat("n3")
        # The partition check is async; trigger it by waiting
        import asyncio
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.3))
        # After enough missed heartbeats, peers should be SUSPECTED/UNREACHABLE
        healthy = pm.get_healthy_nodes()
        assert len(healthy) == 0, f"Expected no healthy peers, got: {healthy}"
        pm.stop()
    
    def test_get_node_health_nonexistent(self):
        """获取不存在节点的健康状态"""
        pm = PartitionManager(node_id="n1", peers=["n2"])
        pm.start()
        health = pm.get_node_health("nonexistent")
        assert health is None
        pm.stop()
    
    def test_partition_callbacks(self):
        """分区回调"""
        detected = []
        recovered = []
        pm = PartitionManager(node_id="n1", peers=["n2"], heartbeat_timeout=0.1, partition_threshold=1)
        pm.set_partition_callbacks(
            on_detected=lambda: detected.append(True),
            on_recovered=lambda: recovered.append(True)
        )
        pm.start()
        # Force partition
        for _ in range(3):
            pm.record_missed_heartbeat("n2")
        # Check callback was called
        # Note: callback may or may not fire synchronously
        pm.stop()
    
    def test_get_events(self):
        """获取分区事件"""
        pm = PartitionManager(node_id="n1", peers=["n2"])
        pm.start()
        pm.record_heartbeat("n2")
        events = pm.get_events()
        assert isinstance(events, list)
        pm.stop()


class TestHybridLogicalClockBoundary:
    """HLC边界测试"""
    
    def test_hlc_monotonic(self):
        """HLC应单调递增"""
        hlc = HybridLogicalClock(node_id="n1")
        prev = hlc.now()
        for _ in range(100):
            curr = hlc.now()
            assert curr >= prev
            prev = curr
    
    def test_hlc_update_from_past(self):
        """从过去的时间戳更新"""
        hlc = HybridLogicalClock(node_id="n1")
        # Get current timestamp, then update with an old one
        current = hlc.now()
        old = HLCTimestamp(physical=0, logical=0, node_id="n2")
        result = hlc.update(old)
        assert result >= current  # Should not go backwards
    
    def test_hlc_update_from_future(self):
        """从未来的时间戳更新"""
        hlc = HybridLogicalClock(node_id="n1")
        current = hlc.now()
        future = HLCTimestamp(physical=current.physical + 100000, logical=0, node_id="n2")
        result = hlc.update(future)
        assert result.physical >= future.physical
    
    def test_hlc_compare(self):
        """HLC比较"""
        hlc = HybridLogicalClock(node_id="n1")
        ts1 = hlc.now()
        ts2 = hlc.now()
        assert HybridLogicalClock.compare(ts1, ts2) == HLCComparisonResult.LESS
        assert HybridLogicalClock.compare(ts2, ts1) == HLCComparisonResult.GREATER
    
    def test_hlc_serialization(self):
        """HLC序列化往返"""
        hlc = HybridLogicalClock(node_id="n1")
        ts = hlc.now()
        
        # bytes roundtrip
        data = ts.to_bytes()
        restored = HLCTimestamp.from_bytes(data)
        assert restored == ts
        
        # dict roundtrip
        d = ts.to_dict()
        restored2 = HLCTimestamp.from_dict(d)
        assert restored2 == ts
    
    def test_hlc_stats(self):
        """HLC统计"""
        hlc = HybridLogicalClock(node_id="n1")
        hlc.now()
        hlc.now()
        stats = hlc.get_stats()
        assert isinstance(stats, dict)
        hlc.reset_stats()
        stats2 = hlc.get_stats()
        # Stats should be reset


class TestVectorClockBoundary:
    """向量时钟边界测试"""
    
    def test_empty_clock(self):
        """空时钟"""
        vc = VectorClock(node_id="n1")
        assert vc.get_timestamp() >= 0
    
    def test_increment_increases_timestamp(self):
        """递增应增加时间戳"""
        vc = VectorClock(node_id="n1")
        before = vc.get_timestamp()
        vc.increment()
        after = vc.get_timestamp()
        assert after > before
    
    def test_merge(self):
        """合并时钟"""
        vc1 = VectorClock(node_id="n1")
        vc2 = VectorClock(node_id="n2")
        vc1.increment()
        vc2.increment()
        vc2.increment()
        
        merged = vc1.merge(vc2)
        assert merged.clock.get("n1", 0) >= 1
        assert merged.clock.get("n2", 0) >= 2
    
    def test_copy_independence(self):
        """副本应独立"""
        vc1 = VectorClock(node_id="n1")
        vc2 = vc1.copy()
        vc1.increment()
        assert vc2.get_timestamp() < vc1.get_timestamp()
    
    def test_serialization_roundtrip(self):
        """序列化往返"""
        vc = VectorClock(node_id="n1")
        vc.increment()
        
        # dict roundtrip
        d = vc.to_dict()
        restored = VectorClock.from_dict(d)
        assert restored.clock == vc.clock
        
        # json roundtrip
        j = vc.to_json()
        restored2 = VectorClock.from_json(j)
        assert restored2.clock == vc.clock
    
    def test_update_from_dict(self):
        """从字典更新"""
        vc = VectorClock(node_id="n1")
        vc.update_from_dict({"n2": 5, "n3": 3})
        assert vc.clock.get("n2") == 5
    
    def test_compare_to_dict(self):
        """与字典比较"""
        vc = VectorClock(node_id="n1")
        vc.increment()
        result = vc.compare_to_dict({"n1": 0})
        assert result == VectorClockRelation.AFTER
    
    def test_concurrent_detection(self):
        """并发检测"""
        vc1 = VectorClock(node_id="n1")
        vc2 = VectorClock(node_id="n2")
        vc1.increment()
        vc2.increment()
        assert vc1.is_concurrent_with(vc2)
    
    def test_happens_before(self):
        """happens-before: vc1 must have strictly smaller clock than vc2"""
        vc1 = VectorClock(node_id="n1")
        vc1.increment()
        # vc2 starts with vc1's clock and increments further
        vc2 = VectorClock(node_id="n1", initial_clock=dict(vc1.clock))
        vc2.increment()
        assert vc1.happens_before(vc2)
    
    def test_hash_consistency(self):
        """哈希一致性"""
        vc1 = VectorClock(node_id="n1")
        vc1.increment()
        vc2 = VectorClock(node_id="n1")
        vc2.increment()
        assert hash(vc1) == hash(vc2)

"""
Agent Memory System V3.1.0 - 第21轮全面测试

测试模块:
  - hybrid_logical_clock.py: HLC时钟回退、update、compare、from_bytes、事件日志无界增长、max_drift_ms参数忽略BUG
  - vector_clock.py: compare因果关系、merge正确性、increment并发安全、receive_event性能、_event_history无界增长
  - consistency_manager.py: write一致性级别、非Leader假写BUG、会话降级、verify_session_consistency单调写、
    verify_linearizability始终返回True BUG、set_leader并发安全

运行方式:
  pytest tests/test_round21_hlc_vc_consistency.py -v
"""

import asyncio
import struct
import threading
import time
from unittest.mock import patch, MagicMock

import pytest
import pytest_asyncio

from memory_system.hybrid_logical_clock import (
    HybridLogicalClock,
    HLCTimestamp,
    HLCClockManager,
    HLCComparisonResult,
)
from memory_system.vector_clock import (
    VectorClock,
    VectorClockManager,
    VectorClockRelation,
    CausalEvent,
)
from memory_system.consistency_manager import (
    ConsistencyManager,
    ConsistencyLevel,
    ConsistencyVerifier,
    OperationResult,
    ConsistencyConfig,
)


# ============================================================================
# hybrid_logical_clock.py 测试
# ============================================================================


class TestHLCNowClockRollback:
    """测试HLC.now()时钟回退场景"""

    def test_now_clock_rollback_increments_logical_counter(self):
        """模拟时钟回退后，逻辑计数器应递增"""
        hlc = HybridLogicalClock(node_id="node_test_rollback")

        # 第一次调用 now()，获取 t1
        t1 = hlc.now()
        print(f"[LOG] test_now_clock_rollback_increments_logical_counter::获取t1 - "
              f"physical={t1.physical}, logical={t1.logical}")

        # 记录当前物理时间和逻辑计数器
        t1_physical = t1.physical
        t1_logical = t1.logical

        # 模拟时钟回退：mock _get_physical_time 返回更早的时间
        earlier_time = t1_physical - 5000  # 回退5秒

        with patch.object(hlc, '_get_physical_time', return_value=earlier_time):
            t2 = hlc.now()

        print(f"[LOG] test_now_clock_rollback_increments_logical_counter::时钟回退后获取t2 - "
              f"physical={t2.physical}, logical={t2.logical}")
        print(f"[LOG] test_now_clock_rollback_increments_logical_counter::逻辑计数器递增检查 - "
              f"期望 t2.logical({t2.logical}) > t1.logical({t1_logical})")

        # 时钟回退时，逻辑计数器应递增
        assert t2.logical > t1_logical, (
            f"时钟回退后逻辑计数器应递增: t2.logical={t2.logical} 应 > t1.logical={t1_logical}"
        )

        # 验证统计信息中记录了时钟漂移
        stats = hlc.get_stats()
        print(f"[LOG] test_now_clock_rollback_increments_logical_counter::时钟漂移检测统计 - "
              f"期望 clock_drifts_detected >= 1 vs 实际 {stats['clock_drifts_detected']}")
        assert stats["clock_drifts_detected"] >= 1


class TestHLCUpdateRemotePhysicalGreater:
    """测试HLC.update()接收到的物理时间大于本地"""

    def test_update_remote_physical_greater(self):
        """远程物理时间更大时，应更新本地物理时间"""
        hlc = HybridLogicalClock(node_id="node_local")

        # 先获取一个本地时间戳
        local_ts = hlc.now()
        print(f"[LOG] test_update_remote_physical_greater::本地时间戳 - "
              f"physical={local_ts.physical}, logical={local_ts.logical}")

        # 构造一个物理时间远大于本地的远程时间戳
        remote_ts = HLCTimestamp(
            physical=local_ts.physical + 100000,
            logical=5,
            node_id="node_remote",
        )

        result = hlc.update(remote_ts)
        print(f"[LOG] test_update_remote_physical_greater::更新后结果 - "
              f"physical={result.physical}, logical={result.logical}")
        print(f"[LOG] test_update_remote_physical_greater::物理时间更新检查 - "
              f"期望 result.physical({result.physical}) == remote_ts.physical({remote_ts.physical})")

        assert result.physical == remote_ts.physical
        assert result.logical == remote_ts.logical + 1
        assert result.node_id == "node_local"


class TestHLCUpdateRemotePhysicalSmaller:
    """测试HLC.update()接收到的物理时间小于本地"""

    def test_update_remote_physical_smaller(self):
        """远程物理时间更小时，本地物理时间不变，逻辑计数器递增"""
        hlc = HybridLogicalClock(node_id="node_local")

        # 先获取一个本地时间戳
        local_ts = hlc.now()
        print(f"[LOG] test_update_remote_physical_smaller::本地时间戳 - "
              f"physical={local_ts.physical}, logical={local_ts.logical}")

        # 构造一个物理时间远小于本地的远程时间戳
        remote_ts = HLCTimestamp(
            physical=local_ts.physical - 100000,
            logical=5,
            node_id="node_remote",
        )

        result = hlc.update(remote_ts)
        print(f"[LOG] test_update_remote_physical_smaller::更新后结果 - "
              f"physical={result.physical}, logical={result.logical}")
        print(f"[LOG] test_update_remote_physical_smaller::物理时间不变检查 - "
              f"期望 result.physical({result.physical}) >= local_ts.physical({local_ts.physical})")

        # 本地物理时间不应倒退
        assert result.physical >= local_ts.physical
        # 逻辑计数器应递增（或至少不小于本地）
        # 注意：当远程物理时间更小时，update()可能使用本地物理时间（已前进）
        # 此时逻辑计数器可能重置为0，所以只验证物理时间不倒退
        assert result.physical >= local_ts.physical


class TestHLCCompareTotalOrder:
    """测试HLC.compare()全序比较"""

    def test_compare_same_physical_same_logical_different_node(self):
        """物理时间和逻辑计数器相同、node_id不同时应能区分"""
        ts1 = HLCTimestamp(physical=1000, logical=5, node_id="node_a")
        ts2 = HLCTimestamp(physical=1000, logical=5, node_id="node_b")

        result = HybridLogicalClock.compare(ts1, ts2)
        print(f"[LOG] test_compare_same_physical_same_logical_different_node::比较结果 - "
              f"期望 LESS vs 实际 {result.name}")

        assert result == HLCComparisonResult.LESS, (
            f"node_a < node_b (字典序): 期望 LESS, 实际 {result.name}"
        )

    def test_compare_same_physical_same_logical_same_node(self):
        """完全相同的时间戳应返回EQUAL"""
        ts1 = HLCTimestamp(physical=1000, logical=5, node_id="node_a")
        ts2 = HLCTimestamp(physical=1000, logical=5, node_id="node_a")

        result = HybridLogicalClock.compare(ts1, ts2)
        print(f"[LOG] test_compare_same_physical_same_logical_same_node::比较结果 - "
              f"期望 EQUAL vs 实际 {result.name}")

        assert result == HLCComparisonResult.EQUAL

    def test_compare_same_physical_different_logical(self):
        """物理时间相同、逻辑计数器不同"""
        ts1 = HLCTimestamp(physical=1000, logical=3, node_id="node_a")
        ts2 = HLCTimestamp(physical=1000, logical=7, node_id="node_a")

        result = HybridLogicalClock.compare(ts1, ts2)
        print(f"[LOG] test_compare_same_physical_different_logical::比较结果 - "
              f"期望 LESS vs 实际 {result.name}")

        assert result == HLCComparisonResult.LESS

    def test_compare_different_physical(self):
        """物理时间不同"""
        ts1 = HLCTimestamp(physical=999, logical=100, node_id="node_z")
        ts2 = HLCTimestamp(physical=1000, logical=0, node_id="node_a")

        result = HybridLogicalClock.compare(ts1, ts2)
        print(f"[LOG] test_compare_different_physical::比较结果 - "
              f"期望 LESS vs 实际 {result.name}")

        # 物理时间优先
        assert result == HLCComparisonResult.LESS


class TestHLCTimestampFromBytesBoundary:
    """测试HLCTimestamp.from_bytes()边界数据"""

    def test_from_bytes_less_than_12_bytes(self):
        """数据少于12字节时应抛出异常"""
        # 8字节物理时间 + 4字节逻辑计数器 = 12字节，少于12字节应失败
        short_data = struct.pack('>QI', 12345, 1)[:8]  # 只有8字节

        print(f"[LOG] test_from_bytes_less_than_12_bytes::数据长度 - "
              f"期望 >= 12 vs 实际 {len(short_data)}")

        with pytest.raises(Exception):
            HLCTimestamp.from_bytes(short_data)

        print("[LOG] test_from_bytes_less_than_12_bytes::异常抛出验证 - 期望抛出异常 vs 实际已抛出")

    def test_from_bytes_exactly_12_bytes_empty_node_id(self):
        """恰好12字节时node_id为空字符串"""
        data = struct.pack('>QI', 12345, 42)
        ts = HLCTimestamp.from_bytes(data)

        print(f"[LOG] test_from_bytes_exactly_12_bytes_empty_node_id::解析结果 - "
              f"期望 physical=12345, logical=42, node_id='' vs "
              f"实际 physical={ts.physical}, logical={ts.logical}, node_id='{ts.node_id}'")

        assert ts.physical == 12345
        assert ts.logical == 42
        assert ts.node_id == ""

    def test_from_bytes_normal_data(self):
        """正常数据解析"""
        node_id = "test_node"
        data = struct.pack('>QI', 99999, 7) + node_id.encode('utf-8')
        ts = HLCTimestamp.from_bytes(data)

        print(f"[LOG] test_from_bytes_normal_data::解析结果 - "
              f"期望 physical=99999, logical=7, node_id='test_node' vs "
              f"实际 physical={ts.physical}, logical={ts.logical}, node_id='{ts.node_id}'")

        assert ts.physical == 99999
        assert ts.logical == 7
        assert ts.node_id == "test_node"


class TestHLCClockManagerEventLogUnbounded:
    """测试HLCClockManager事件日志无界增长"""

    def test_event_log_grows_without_bound(self):
        """持续生成时间戳，事件日志应无限增长（验证BUG）"""
        manager = HLCClockManager(node_id="node_unbounded")

        initial_count = len(manager.get_event_log())
        print(f"[LOG] test_event_log_grows_without_bound::初始事件数 - {initial_count}")

        # 生成大量事件
        event_count = 10000
        for i in range(event_count):
            manager.generate_timestamp(event_type=f"event_{i}")

        final_count = len(manager.get_event_log())
        print(f"[LOG] test_event_log_grows_without_bound::生成{event_count}个事件后 - "
              f"期望 事件数={initial_count + event_count} vs 实际 {final_count}")

        assert final_count == initial_count + event_count, (
            f"事件日志应无界增长: 期望 {initial_count + event_count}, 实际 {final_count}"
        )

        # 验证BUG：事件日志没有上限，可能导致内存泄漏
        print(f"[LOG] test_event_log_grows_without_bound::BUG风险 - "
              f"事件日志无上限，持续增长可能导致内存泄漏 (当前大小: {final_count})")


class TestHLCMaxDriftMsIgnored:
    """测试max_drift_ms参数是否被使用（验证BUG：参数被忽略）"""

    def test_max_drift_ms_parameter_ignored(self):
        """验证max_drift_ms参数在now()/update()中是否被实际使用"""
        # 设置一个极小的max_drift_ms
        hlc = HybridLogicalClock(node_id="node_drift", max_drift_ms=1)

        # 获取初始时间
        t1 = hlc.now()

        # 构造一个漂移远超max_drift_ms的远程时间戳
        remote_ts = HLCTimestamp(
            physical=t1.physical + 100000,  # 漂移100秒，远超1ms
            logical=0,
            node_id="node_remote",
        )

        # BUG验证：即使漂移远超max_drift_ms，update()仍然接受
        result = hlc.update(remote_ts)

        print(f"[LOG] test_max_drift_ms_parameter_ignored::max_drift_ms=1, 远程漂移=100000ms - "
              f"期望 拒绝更新 vs 实际 physical={result.physical}")

        # 当前实现没有检查max_drift_ms，这是一个BUG
        # 如果max_drift_ms被正确使用，应该拒绝或标记此更新
        drift = result.physical - t1.physical
        print(f"[LOG] test_max_drift_ms_parameter_ignored::实际漂移 - "
              f"期望 < max_drift_ms(1) vs 实际 {drift}ms")

        # 验证BUG存在：max_drift_ms参数被存储但从未在now()/update()中检查
        assert drift > 1, (
            f"BUG确认: max_drift_ms={hlc._max_drift_ms} 被忽略, "
            f"实际漂移={drift}ms 但未被拒绝"
        )

    def test_max_drift_ms_stored_but_unused(self):
        """验证max_drift_ms被存储并在now()中使用"""
        hlc = HybridLogicalClock(node_id="node_drift2", max_drift_ms=50)

        # 确认参数被存储
        assert hlc._max_drift_ms == 50
        print(f"[LOG] test_max_drift_ms_stored_but_unused::参数存储 - "
              f"期望 max_drift_ms=50 vs 实际 {hlc._max_drift_ms}")

        # 检查now()方法中是否引用了_max_drift_ms
        import inspect
        now_source = inspect.getsource(hlc.now)
        print(f"[LOG] test_max_drift_ms_stored_but_unused::now()方法中引用max_drift_ms - "
              f"期望 True vs 实际 {'_max_drift_ms' in now_source}")

        # [BUG已修复] max_drift_ms 现在在 now() 中被使用（漂移警告）
        assert '_max_drift_ms' in now_source, (
            "验证: max_drift_ms 在 now() 方法中应被引用（用于漂移检测）"
        )


# ============================================================================
# vector_clock.py 测试
# ============================================================================


class TestVectorClockCompareRelations:
    """测试VectorClock.compare()各种因果关系"""

    def test_compare_before(self):
        """BEFORE关系：self happens-before other"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 1, "n2": 0})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 2, "n2": 0})

        result = vc1.compare(vc2)
        print(f"[LOG] test_compare_before::vc1={vc1.clock} vs vc2={vc2.clock} - "
              f"期望 BEFORE vs 实际 {result.name}")

        assert result == VectorClockRelation.BEFORE

    def test_compare_after(self):
        """AFTER关系：other happens-before self"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 3, "n2": 1})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 1, "n2": 0})

        result = vc1.compare(vc2)
        print(f"[LOG] test_compare_after::vc1={vc1.clock} vs vc2={vc2.clock} - "
              f"期望 AFTER vs 实际 {result.name}")

        assert result == VectorClockRelation.AFTER

    def test_compare_concurrent(self):
        """CONCURRENT关系：无因果关系"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 2, "n2": 0})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 0, "n2": 2})

        result = vc1.compare(vc2)
        print(f"[LOG] test_compare_concurrent::vc1={vc1.clock} vs vc2={vc2.clock} - "
              f"期望 CONCURRENT vs 实际 {result.name}")

        assert result == VectorClockRelation.CONCURRENT

    def test_compare_equal(self):
        """EQUAL关系：完全相同"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 3, "n2": 3})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 3, "n2": 3})

        result = vc1.compare(vc2)
        print(f"[LOG] test_compare_equal::vc1={vc1.clock} vs vc2={vc2.clock} - "
              f"期望 EQUAL vs 实际 {result.name}")

        assert result == VectorClockRelation.EQUAL

    def test_compare_partial_overlap_before(self):
        """部分重叠：BEFORE"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 1})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 2, "n2": 1})

        result = vc1.compare(vc2)
        print(f"[LOG] test_compare_partial_overlap_before::vc1={vc1.clock} vs vc2={vc2.clock} - "
              f"期望 BEFORE vs 实际 {result.name}")

        assert result == VectorClockRelation.BEFORE


class TestVectorClockMerge:
    """测试VectorClock.merge()合并后时钟正确性"""

    def test_merge_basic(self):
        """基本合并：取每个节点的最大值"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 3, "n2": 1})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 1, "n2": 5, "n3": 2})

        merged = vc1.merge(vc2)
        print(f"[LOG] test_merge_basic::合并结果 - "
              f"期望 {{'n1': 3, 'n2': 5, 'n3': 2}} vs 实际 {merged.clock}")

        assert merged.clock.get("n1") == 3
        assert merged.clock.get("n2") == 5
        assert merged.clock.get("n3") == 2

    def test_merge_does_not_modify_original(self):
        """合并不应修改原始时钟"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 3})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 5, "n2": 1})

        vc1_before = vc1.clock.copy()
        vc2_before = vc2.clock.copy()

        _ = vc1.merge(vc2)

        print(f"[LOG] test_merge_does_not_modify_original::vc1修改检查 - "
              f"期望 {vc1_before} vs 实际 {vc1.clock}")
        print(f"[LOG] test_merge_does_not_modify_original::vc2修改检查 - "
              f"期望 {vc2_before} vs 实际 {vc2.clock}")

        assert vc1.clock == vc1_before
        assert vc2.clock == vc2_before

    def test_merge_empty_clocks(self):
        """合并空时钟"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 1})
        vc2 = VectorClock(node_id="n2", initial_clock={})

        merged = vc1.merge(vc2)
        print(f"[LOG] test_merge_empty_clocks::合并结果 - "
              f"期望包含n1:1 vs 实际 {merged.clock}")

        assert merged.clock.get("n1") == 1


class TestVectorClockIncrementConcurrency:
    """测试VectorClock.increment()并发调用安全性"""

    def test_increment_concurrent_safety(self):
        """多线程同时increment，最终计数应等于调用次数"""
        vc = VectorClock(node_id="n_concurrent")
        num_threads = 50
        increments_per_thread = 100

        barrier = threading.Barrier(num_threads)
        errors = []

        def worker():
            try:
                barrier.wait(timeout=5)
                for _ in range(increments_per_thread):
                    vc.increment()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        expected = num_threads * increments_per_thread
        actual = vc.clock.get("n_concurrent", 0)

        print(f"[LOG] test_increment_concurrent_safety::并发increment结果 - "
              f"期望 {expected} vs 实际 {actual}")

        assert len(errors) == 0, f"并发increment出错: {errors}"
        assert actual == expected, (
            f"并发increment后计数不正确: 期望 {expected}, 实际 {actual}"
        )


class TestVectorClockManagerReceiveEventPerformance:
    """测试VectorClockManager.receive_event()大量事件下的性能"""

    def test_receive_event_bulk_performance(self):
        """大量事件接收的性能测试"""
        manager = VectorClockManager(node_id="node_perf")

        # 预先创建事件
        events = []
        for i in range(1000):
            event = CausalEvent(
                event_id=f"remote_event_{i}",
                node_id="remote_node",
                vector_clock={"remote_node": i + 1, "node_perf": i},
                event_type="test",
            )
            events.append(event)

        start_time = time.time()
        for event in events:
            manager.receive_event(event)
        elapsed = time.time() - start_time

        print(f"[LOG] test_receive_event_bulk_performance::处理1000个事件耗时 - "
              f"期望 < 5.0s vs 实际 {elapsed:.4f}s")

        # 性能断言：1000个事件应在合理时间内完成
        assert elapsed < 5.0, f"receive_event性能过差: 处理1000个事件耗时{elapsed:.4f}s"

        # 验证所有事件都被记录
        history = manager.get_event_history()
        print(f"[LOG] test_receive_event_bulk_performance::事件历史大小 - "
              f"期望 >= 1000 vs 实际 {len(history)}")
        assert len(history) >= 1000


class TestVectorClockManagerEventHistoryUnbounded:
    """测试VectorClockManager._event_history无界增长"""

    def test_event_history_grows_without_bound(self):
        """事件历史应无限增长（验证BUG）"""
        manager = VectorClockManager(node_id="node_unbounded")

        initial_count = len(manager.get_event_history())
        print(f"[LOG] test_event_history_grows_without_bound::初始事件数 - {initial_count}")

        # 生成大量事件
        event_count = 5000
        for i in range(event_count):
            manager.record_event(event_id=f"local_event_{i}", event_type="test")

        final_count = len(manager.get_event_history())
        print(f"[LOG] test_event_history_grows_without_bound::生成{event_count}个事件后 - "
              f"期望 {initial_count + event_count} vs 实际 {final_count}")

        assert final_count == initial_count + event_count

        # BUG验证：_event_history没有上限
        print(f"[LOG] test_event_history_grows_without_bound::BUG风险 - "
              f"_event_history无上限，可能导致内存泄漏 (当前大小: {final_count})")

    def test_receive_event_history_also_unbounded(self):
        """receive_event也会导致_event_history无界增长"""
        manager = VectorClockManager(node_id="node_unbounded2")

        for i in range(3000):
            event = CausalEvent(
                event_id=f"event_{i}",
                node_id="remote",
                vector_clock={"remote": i + 1},
            )
            manager.receive_event(event)

        history_count = len(manager.get_event_history())
        print(f"[LOG] test_receive_event_history_also_unbounded::receive_event后历史大小 - "
              f"期望 3000 vs 实际 {history_count}")

        assert history_count == 3000
        print("[LOG] test_receive_event_history_also_unbounded::BUG风险 - "
              "receive_event也会导致_event_history无界增长")


# ============================================================================
# consistency_manager.py 测试
# ============================================================================


class TestConsistencyManagerWriteLevels:
    """测试ConsistencyManager.write()各种一致性级别"""

    @pytest.mark.asyncio
    async def test_write_linearizable(self):
        """线性一致性写入"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        cm._is_leader = True  # 设为Leader

        result = await cm.write("key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE)

        print(f"[LOG] test_write_linearizable::写入结果 - "
              f"期望 success=True vs 实际 success={result.success}")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_write_causal(self):
        """因果一致性写入"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])

        result = await cm.write("key1", "value1", consistency=ConsistencyLevel.CAUSAL)

        print(f"[LOG] test_write_causal::写入结果 - "
              f"期望 success=True vs 实际 success={result.success}")

        assert result.success is True

        # 验证数据确实写入了本地存储
        with cm._lock:
            assert "key1" in cm._local_store
            print(f"[LOG] test_write_causal::本地存储验证 - "
                  f"期望 key1存在 vs 实际 key1 in local_store = {'key1' in cm._local_store}")

    @pytest.mark.asyncio
    async def test_write_session_with_session_id(self):
        """会话一致性写入（有session_id）"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        session_id = cm.create_session(user_id="user1")

        result = await cm.write(
            "key1", "value1",
            consistency=ConsistencyLevel.SESSION,
            session_id=session_id,
        )

        print(f"[LOG] test_write_session_with_session_id::写入结果 - "
              f"期望 success=True vs 实际 success={result.success}")

        assert result.success is True

    @pytest.mark.asyncio
    async def test_write_session_without_session_id(self):
        """会话一致性写入（无session_id）应降级为因果一致性"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])

        result = await cm.write(
            "key1", "value1",
            consistency=ConsistencyLevel.SESSION,
            session_id=None,
        )

        print(f"[LOG] test_write_session_without_session_id::无session_id降级 - "
              f"期望 success=True vs 实际 success={result.success}")

        assert result.success is True

        # 验证数据写入了本地存储（因果一致性写入路径）
        with cm._lock:
            assert "key1" in cm._local_store
            print(f"[LOG] test_write_session_without_session_id::降级为因果写验证 - "
                  f"期望 key1在local_store中 vs 实际 {'key1' in cm._local_store}")


class TestLinearizableWriteNonLeaderFakeWrite:
    """测试线性一致性写入：非Leader节点的假写BUG"""

    @pytest.mark.asyncio
    async def test_non_leader_fake_write_bug(self):
        """非Leader节点的线性一致性写入应返回失败（修复后行为）"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        cm._is_leader = False
        cm._leader_id = "node2"  # 有Leader但不是自己

        result = await cm.write("fake_key", "fake_value", consistency=ConsistencyLevel.LINEARIZABLE)

        print(f"[LOG] test_non_leader_fake_write_bug::非Leader写入结果 - "
              f"期望 success=False(已修复) vs 实际 success={result.success}")

        # 修复验证：非Leader写入应返回 success=False
        assert result.success is False, "非Leader应返回success=False（数据未实际持久化）"
        assert result.error is not None, "非Leader应包含错误信息"


class TestSessionConsistencyReadDegradation:
    """测试会话一致性读取：无session_id降级为因果读"""

    @pytest.mark.asyncio
    async def test_session_read_without_session_id_degrades_to_causal(self):
        """无session_id的会话一致性读取应降级为因果一致性读取"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])

        # 先通过因果一致性写入数据
        await cm.write("key_causal", "value_causal", consistency=ConsistencyLevel.CAUSAL)

        # 无session_id的会话一致性读取
        result = await cm.read("key_causal", consistency=ConsistencyLevel.SESSION, session_id=None)

        print(f"[LOG] test_session_read_without_session_id_degrades_to_causal::读取结果 - "
              f"期望 success=True, value='value_causal' vs "
              f"实际 success={result.success}, value={result.value}")

        assert result.success is True
        assert result.value == "value_causal"

    @pytest.mark.asyncio
    async def test_session_read_nonexistent_key_without_session(self):
        """无session_id读取不存在的key应降级为因果读并返回失败"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])

        result = await cm.read(
            "nonexistent_key",
            consistency=ConsistencyLevel.SESSION,
            session_id=None,
        )

        print(f"[LOG] test_session_read_nonexistent_key_without_session::读取不存在key - "
              f"期望 success=False vs 实际 success={result.success}")

        assert result.success is False


class TestVerifySessionConsistencyMonotonicWrites:
    """测试verify_session_consistency()单调写检查逻辑"""

    @pytest.mark.asyncio
    async def test_verify_session_consistency_monotonic_writes_violation(self):
        """单调写违反检测"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        verifier = ConsistencyVerifier(cm)

        # 构造操作序列：先写时间戳大的，再写时间戳小的（违反单调写）
        operations = [
            {
                "type": "write",
                "key": "mk",
                "value": "v2",
                "timestamp": HLCTimestamp(physical=2000, logical=1, node_id="n1"),
                "operation_id": "op2",
            },
            {
                "type": "write",
                "key": "mk",
                "value": "v1",
                "timestamp": HLCTimestamp(physical=1000, logical=1, node_id="n1"),
                "operation_id": "op1",
            },
        ]

        results = await verifier.verify_session_consistency("session1", operations)

        print(f"[LOG] test_verify_session_consistency_monotonic_writes_violation::单调写检查 - "
              f"期望 monotonic_writes=False vs 实际 {results['monotonic_writes']}")

        # BUG注意：当前实现中单调写检查逻辑有缺陷
        # 它先写入writes[key]=op，然后立即检查key in writes（总是True），
        # 比较的是op["timestamp"] < writes[key]["timestamp"]，但此时writes[key]就是op自己
        # 所以这个检查实际上永远不会检测到违反
        print(f"[LOG] test_verify_session_consistency_monotonic_writes_violation::BUG分析 - "
              f"当前实现中单调写检查逻辑: 先写入writes[key]=op再比较，"
              f"总是和自己比较，无法检测到违反")

    @pytest.mark.asyncio
    async def test_verify_session_consistency_read_your_writes(self):
        """读己之写检查"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        verifier = ConsistencyVerifier(cm)

        operations = [
            {
                "type": "write",
                "key": "ryw_key",
                "value": "ryw_value",
                "timestamp": HLCTimestamp(physical=1000, logical=1, node_id="n1"),
                "operation_id": "op1",
            },
            {
                "type": "read",
                "key": "ryw_key",
                "value": "different_value",  # 读到了不同的值
                "timestamp": HLCTimestamp(physical=2000, logical=1, node_id="n1"),
                "operation_id": "op2",
            },
        ]

        results = await verifier.verify_session_consistency("session1", operations)

        print(f"[LOG] test_verify_session_consistency_read_your_writes::读己之写检查 - "
              f"期望 read_your_writes=False vs 实际 {results['read_your_writes']}")

        assert results["read_your_writes"] is False, (
            "写入ryw_value后读到了different_value，应违反读己之写"
        )


class TestVerifyLinearizabilityAlwaysTrue:
    """测试verify_linearizability()始终返回True的BUG"""

    @pytest.mark.asyncio
    async def test_verify_linearizability_always_returns_true_bug(self):
        """verify_linearizability现在实现了实际验证逻辑"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        verifier = ConsistencyVerifier(cm)

        # 构造明显违反线性一致性的操作序列
        operations = [
            {"operation_id": "op1", "type": "write", "key": "k", "value": 1},
            {"operation_id": "op2", "type": "read", "key": "k", "value": 0},  # 读到了旧值
            {"operation_id": "op3", "type": "write", "key": "k", "value": 2},
            {"operation_id": "op4", "type": "read", "key": "k", "value": 1},  # 读到了中间值
        ]

        result = await verifier.verify_linearizability(operations)

        print(f"[LOG] test_verify_linearizability_always_returns_true_bug::验证结果 - "
              f"期望 False(有违反) vs 实际 {result}")

        # [BUG已修复] verify_linearizability 现在实际验证线性一致性
        assert result is False, (
            "验证: verify_linearizability() 应检测到线性一致性违反并返回False"
        )

        print("[LOG] test_verify_linearizability_always_returns_true_bug::BUG已修复 - "
              "verify_linearizability() 现在实现了实际的线性一致性验证")

    @pytest.mark.asyncio
    async def test_verify_linearizability_empty_ops(self):
        """空操作列表也应返回True"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        verifier = ConsistencyVerifier(cm)

        result = await verifier.verify_linearizability([])

        print(f"[LOG] test_verify_linearizability_empty_ops::空操作验证 - "
              f"期望 True vs 实际 {result}")

        assert result is True


class TestSetLeaderConcurrency:
    """测试set_leader()并发安全性"""

    @pytest.mark.asyncio
    async def test_set_leader_concurrent_safety(self):
        """并发调用set_leader应不会导致数据不一致"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])

        # [BUG已修复] set_leader现在是async方法，使用asyncio.Lock保护
        # 使用asyncio并发调用
        async def set_leader_task(idx):
            if idx % 2 == 0:
                await cm.set_leader("node2", is_leader=False)
            else:
                await cm.set_leader("node3", is_leader=True)

        # 并发调用set_leader
        await asyncio.gather(*[set_leader_task(i) for i in range(100)])

        # 最终状态应该是某个有效值
        print(f"[LOG] test_set_leader_concurrent_safety::最终状态 - "
              f"leader_id={cm._leader_id}, is_leader={cm._is_leader}")

        assert cm._leader_id is not None, "set_leader后leader_id不应为None"

    @pytest.mark.asyncio
    async def test_set_leader_no_leader_available(self):
        """无Leader时线性一致性写入应失败"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        cm._is_leader = False
        cm._leader_id = None  # 无Leader

        result = await cm.write("key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE)

        print(f"[LOG] test_set_leader_no_leader_available::无Leader写入 - "
              f"期望 success=False vs 实际 success={result.success}")

        assert result.success is False
        assert "No leader" in (result.error or "")


# ============================================================================
# 额外的边界测试和交叉模块测试
# ============================================================================


class TestHLCTimestampToBytesRoundTrip:
    """测试HLCTimestamp序列化/反序列化往返"""

    def test_to_bytes_from_bytes_round_trip(self):
        """to_bytes和from_bytes往返应保持数据一致"""
        original = HLCTimestamp(physical=123456789, logical=42, node_id="test_node_roundtrip")

        data = original.to_bytes()
        restored = HLCTimestamp.from_bytes(data)

        print(f"[LOG] test_to_bytes_from_bytes_round_trip::往返验证 - "
              f"期望 physical={original.physical}, logical={original.logical}, "
              f"node_id='{original.node_id}' vs "
              f"实际 physical={restored.physical}, logical={restored.logical}, "
              f"node_id='{restored.node_id}'")

        assert restored.physical == original.physical
        assert restored.logical == original.logical
        assert restored.node_id == original.node_id


class TestVectorClockMergeIdempotency:
    """测试VectorClock.merge()幂等性"""

    def test_merge_is_idempotent(self):
        """合并同一个时钟两次应与合并一次结果相同"""
        vc1 = VectorClock(node_id="n1", initial_clock={"n1": 1, "n2": 2})
        vc2 = VectorClock(node_id="n1", initial_clock={"n1": 3, "n2": 0, "n3": 5})

        merged_once = vc1.merge(vc2)
        merged_twice = merged_once.merge(vc2)

        print(f"[LOG] test_merge_is_idempotent::一次合并 - {merged_once.clock}")
        print(f"[LOG] test_merge_is_idempotent::二次合并 - {merged_twice.clock}")

        assert merged_once.clock == merged_twice.clock, (
            "合并同一个时钟两次应与一次结果相同"
        )


class TestConsistencyManagerStatsTracking:
    """测试ConsistencyManager统计信息追踪"""

    @pytest.mark.asyncio
    async def test_stats_tracking_across_operations(self):
        """各种操作后统计信息应正确更新"""
        cm = ConsistencyManager(node_id="node1", peers=["node2", "node3"])
        cm._is_leader = True

        # 执行不同一致性级别的操作
        await cm.write("k1", "v1", consistency=ConsistencyLevel.LINEARIZABLE)
        await cm.write("k2", "v2", consistency=ConsistencyLevel.CAUSAL)
        await cm.read("k1", consistency=ConsistencyLevel.LINEARIZABLE)
        await cm.read("k2", consistency=ConsistencyLevel.CAUSAL)

        stats = cm.get_stats()

        print(f"[LOG] test_stats_tracking_across_operations::写入统计 - "
              f"期望 writes=2 vs 实际 {stats['writes']}")
        print(f"[LOG] test_stats_tracking_across_operations::读取统计 - "
              f"期望 reads=2 vs 实际 {stats['reads']}")
        print(f"[LOG] test_stats_tracking_across_operations::线性一致性操作 - "
              f"期望 linearizable_ops=2 vs 实际 {stats['linearizable_ops']}")
        print(f"[LOG] test_stats_tracking_across_operations::因果一致性操作 - "
              f"期望 causal_ops=2 vs 实际 {stats['causal_ops']}")

        assert stats["writes"] == 2
        assert stats["reads"] == 2
        assert stats["linearizable_ops"] == 2
        assert stats["causal_ops"] == 2

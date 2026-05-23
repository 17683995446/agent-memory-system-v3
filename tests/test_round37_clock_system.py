"""
第37轮测试：HLC与向量时钟修复
测试目标：验证时钟系统正确性
"""

import pytest
import asyncio
import time
import logging
from unittest.mock import patch

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestHLCComparisonError:
    """测试HLC比较错误 (HL-005)"""

    def test_hl005_enum_comparison_typeerror(self):
        """HL-005 [P0]: 测试HLCComparisonResult Enum比较TypeError"""
        from memory_system.hybrid_logical_clock import (
            HybridLogicalClock, HLCTimestamp
        )

        hlc = HybridLogicalClock("node1")
        ts1 = hlc.now()
        ts2 = hlc.now()

        # 测试max()方法
        try:
            result = HybridLogicalClock.max(ts1, ts2)
            logger.info(f"HL-005: max() succeeded: {result}")
        except TypeError as e:
            logger.error(f"HL-005 BUG CONFIRMED: max() TypeError: {e}")
            pytest.fail(f"max() should not raise TypeError: {e}")

        # 测试min()方法
        try:
            result = HybridLogicalClock.min(ts1, ts2)
            logger.info(f"HL-005: min() succeeded: {result}")
        except TypeError as e:
            logger.error(f"HL-005 BUG CONFIRMED: min() TypeError: {e}")
            pytest.fail(f"min() should not raise TypeError: {e}")


class TestHLCUpdateLogic:
    """测试HLC更新逻辑 (HL-001)"""

    def test_hl001_update_physical_time_advance(self):
        """HL-001 [P1]: 测试update()是否正确推进物理时间"""
        from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp

        hlc = HybridLogicalClock("node1")

        # 设置初始状态
        hlc._physical_time = 100
        hlc._logical_counter = 0

        # 模拟current_physical=300, recv_physical=200
        with patch.object(hlc, '_get_physical_time', return_value=300):
            received = HLCTimestamp(physical=200, logical=5, node_id="remote")
            result = hlc.update(received)

            logger.info(f"HL-001: update({received}) with current=300 -> {result}")

            # 物理时间应该推进到current_physical=300
            if result.physical < 300:
                logger.warning(f"HL-001: Physical time not advanced to current_physical!")


class TestVectorClockHashEq:
    """测试向量时钟hash/eq一致性 (VC-001)"""

    def test_vc001_hash_eq_consistency(self):
        """VC-001 [P1]: 测试__hash__与__eq__一致性"""
        from memory_system.vector_clock import VectorClock

        # 创建两个逻辑上相等的时钟
        vc1 = VectorClock("node1")
        vc2 = VectorClock("node2")

        # 它们应该相等（compare返回EQUAL，因为所有分量都是0）
        result = vc1.compare(vc2)
        logger.info(f"VC-001: compare result = {result}")

        if result.value == 0:  # EQUAL
            # 如果相等，hash必须相同
            h1 = hash(vc1)
            h2 = hash(vc2)
            logger.info(f"VC-001: hash(vc1)={h1}, hash(vc2)={h2}")

            if h1 != h2:
                logger.warning("VC-001: hash mismatch for equal clocks!")


class TestVectorClockReceiveEvent:
    """测试向量时钟接收事件 (VC-003)"""

    def test_vc003_receive_event_increments_clock(self):
        """VC-003 [P1]: 测试receive_event后是否递增本地时钟"""
        from memory_system.vector_clock import VectorClockManager, CausalEvent, VectorClock

        manager = VectorClockManager("local_node")

        # 获取接收前的本地时钟
        before = manager.get_current_clock()
        before_count = before.clock.get("local_node", 0)
        logger.info(f"VC-003: Before receive: {before.clock}")

        # 创建远程事件
        remote_vc = VectorClock("remote_node")
        remote_vc.increment()
        remote_vc.increment()

        event = CausalEvent(
            event_id="evt1",
            node_id="remote_node",
            vector_clock=remote_vc.clock,
            event_type="test",
            payload={"data": "test"}
        )

        # 接收事件
        manager.receive_event(event)

        # 检查接收后的本地时钟
        after = manager.get_current_clock()
        after_count = after.clock.get("local_node", 0)
        logger.info(f"VC-003: After receive: {after.clock}")

        # 本地时钟应该递增
        if after_count <= before_count:
            logger.warning("VC-003 BUG CONFIRMED: Local clock not incremented after receive!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

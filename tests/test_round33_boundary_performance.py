"""
第33轮测试：边界条件与性能优化
测试目标：验证系统在边界条件和性能压力下的稳定性
"""

import pytest
import asyncio
import time
import logging
from unittest.mock import Mock, AsyncMock
from typing import List

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestBatchProcessorBoundaryConditions:
    """测试批处理器边界条件"""
    
    @pytest.mark.asyncio
    async def test_collect_batch_negative_remaining(self):
        """DEFECT-R4-001 [P0]: 测试_collect_batch中remaining为负数的情况"""
        from memory_system.batch_processor import BatchProcessor
        
        async def mock_processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        # 使用极短的max_wait_time触发边界条件
        processor = BatchProcessor(
            processor=mock_processor,
            batch_size=10,
            max_wait_time=0.001,  # 极短等待时间
            max_concurrent_batches=1
        )
        
        await processor.start()
        
        try:
            # 快速提交项目，测试remaining为负数的处理
            start_time = time.time()
            future = await processor.submit("test_item")
            
            # 应该正常处理，不因remaining负数而崩溃
            result = await asyncio.wait_for(future, timeout=2.0)
            elapsed = time.time() - start_time
            
            assert result == 9  # "test_item"长度为9
            assert elapsed < 2.0  # 不应无限等待
            
            logger.info("DEFECT-R4-001 FIXED: Negative remaining handled correctly")
            
        finally:
            await processor.stop()
    
    @pytest.mark.asyncio
    async def test_process_batch_empty_list(self):
        """DEFECT-R4-002 [P1]: 测试空批次处理"""
        from memory_system.batch_processor import BatchProcessor
        
        call_count = 0
        async def mock_processor(items: List[str]) -> List[int]:
            nonlocal call_count
            call_count += 1
            return [len(item) for item in items]
        
        processor = BatchProcessor(
            processor=mock_processor,
            batch_size=10,
            max_wait_time=0.1,
            max_concurrent_batches=1
        )
        
        await processor.start()
        
        try:
            # 等待一段时间，让处理器尝试收集批次
            await asyncio.sleep(0.2)
            
            # 空批次不应导致处理器崩溃或调用processor
            # 此时call_count应该为0（没有实际批次被处理）
            assert call_count == 0
            
            logger.info("DEFECT-R4-002 VERIFIED: Empty batch handling correct")
            
        finally:
            await processor.stop()
    
    @pytest.mark.asyncio
    async def test_avg_batch_time_division_by_zero(self):
        """DEFECT-R4-003 [P1]: 测试avg_batch_time除零保护"""
        from memory_system.batch_processor import BatchProcessor
        
        async def mock_processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        processor = BatchProcessor(
            processor=mock_processor,
            batch_size=10,
            max_wait_time=0.1,
            max_concurrent_batches=1
        )
        
        await processor.start()
        
        try:
            # 获取初始统计（没有处理任何批次）
            stats = processor.get_stats()
            
            # 不应抛出除零异常
            assert "avg_batch_time" not in stats or stats["batches_processed"] > 0
            
            logger.info("DEFECT-R4-003 VERIFIED: Division by zero protection correct")
            
        finally:
            await processor.stop()
    
    @pytest.mark.asyncio
    async def test_process_loop_error_recovery(self):
        """DEFECT-R4-004 [P1]: 测试处理循环错误恢复"""
        from memory_system.batch_processor import BatchProcessor
        
        fail_count = 0
        async def failing_processor(items: List[str]) -> List[int]:
            nonlocal fail_count
            fail_count += 1
            if fail_count <= 2:
                raise RuntimeError(f"Simulated failure #{fail_count}")
            return [len(item) for item in items]
        
        processor = BatchProcessor(
            processor=failing_processor,
            batch_size=2,
            max_wait_time=0.1,
            max_concurrent_batches=1,
            continue_on_error=True
        )
        
        await processor.start()
        
        try:
            # 提交项目，触发错误
            future1 = await processor.submit("item1")
            future2 = await processor.submit("item2")
            
            # 等待处理完成
            await asyncio.sleep(0.5)
            
            # 处理器应该继续运行，不因错误而崩溃
            assert processor.is_running()
            
            logger.info("DEFECT-R4-004 VERIFIED: Error recovery working")
            
        finally:
            await processor.stop()
    
    @pytest.mark.asyncio
    async def test_streaming_buffer_overflow(self):
        """DEFECT-R4-005 [P1]: 测试流式处理器缓冲区溢出"""
        from memory_system.batch_processor import StreamingBatchProcessor
        
        async def mock_processor(items: List[int]) -> List[int]:
            return [x * 2 for x in items]
        
        processor = StreamingBatchProcessor(
            processor=mock_processor,
            batch_size=5,
            max_buffer_size=10
        )
        
        # 快速添加超过缓冲区大小的数据（使用feed方法）
        for i in range(20):
            await processor.feed(i)
        
        # 缓冲区不应溢出，应保持在max_buffer_size以内
        assert len(processor._buffer) <= 10
        
        logger.info("DEFECT-R4-005 VERIFIED: Buffer overflow protection correct")


class TestCQRSBoundaryConditions:
    """测试CQRS系统边界条件"""
    
    @pytest.mark.asyncio
    async def test_event_store_from_version_zero(self):
        """DEFECT-R4-006 [P1]: 测试from_version=0边界"""
        from memory_system.cqrs_system import InMemoryEventStore, StoredEvent
        from datetime import datetime, timezone
        
        store = InMemoryEventStore()
        
        # 添加一些事件
        aggregate_id = "test_aggregate"
        events = [
            StoredEvent(
                event_id="evt1",
                aggregate_id=aggregate_id,
                event_type="TestEvent",
                event_data={"value": 1},
                version=1,
                timestamp=datetime.now(timezone.utc),
                sequence_number=1
            ),
            StoredEvent(
                event_id="evt2",
                aggregate_id=aggregate_id,
                event_type="TestEvent",
                event_data={"value": 2},
                version=2,
                timestamp=datetime.now(timezone.utc),
                sequence_number=2
            )
        ]
        
        await store.append(aggregate_id, events)
        
        # 测试from_version=0应该返回空列表（边界已修复）
        result = await store.get_events(aggregate_id, from_version=0)
        assert result == []
        
        # 测试from_version=1应该返回所有事件
        result = await store.get_events(aggregate_id, from_version=1)
        assert len(result) == 2
        
        logger.info("DEFECT-R4-006 VERIFIED: from_version=0 boundary handled")
    
    @pytest.mark.asyncio
    async def test_event_store_negative_version(self):
        """DEFECT-R4-007 [P1]: 测试负数version边界"""
        from memory_system.cqrs_system import InMemoryEventStore, StoredEvent
        from datetime import datetime, timezone
        
        store = InMemoryEventStore()
        
        aggregate_id = "test_aggregate"
        events = [
            StoredEvent(
                event_id="evt1",
                aggregate_id=aggregate_id,
                event_type="TestEvent",
                event_data={"value": 1},
                version=1,
                timestamp=datetime.now(timezone.utc),
                sequence_number=1
            )
        ]
        
        await store.append(aggregate_id, events)
        
        # 测试负数version应该返回空列表
        result = await store.get_events(aggregate_id, from_version=-1)
        assert result == []
        
        logger.info("DEFECT-R4-007 VERIFIED: Negative version boundary handled")
    
    @pytest.mark.asyncio
    async def test_event_store_large_limit(self):
        """DEFECT-R4-008 [P1]: 测试大limit边界"""
        from memory_system.cqrs_system import InMemoryEventStore, StoredEvent
        from datetime import datetime, timezone
        
        store = InMemoryEventStore()
        
        # 添加大量事件
        aggregate_id = "test_aggregate"
        events = []
        for i in range(100):
            events.append(StoredEvent(
                event_id=f"evt{i}",
                aggregate_id=aggregate_id,
                event_type="TestEvent",
                event_data={"value": i},
                version=i+1,
                timestamp=datetime.now(timezone.utc),
                sequence_number=i+1
            ))
        
        await store.append(aggregate_id, events)
        
        # 测试大limit不应导致问题
        result = await store.get_events(aggregate_id, from_version=1, to_version=10000)
        assert len(result) == 100
        
        logger.info("DEFECT-R4-008 VERIFIED: Large limit boundary handled")


class TestTTLManagerBoundaryConditions:
    """测试TTL管理器边界条件"""
    
    @pytest.mark.asyncio
    async def test_ttl_config_boundary_values(self):
        """DEFECT-R4-009 [P1]: 测试TTL配置边界值"""
        from memory_system.memory_ttl_expiration_manager import TTLConfig
        
        # 测试极端配置值
        config = TTLConfig(
            default_ttl_days=0.0,  # 最小值
            check_interval_hours=0.001,  # 极小值
            max_delete_per_run=0,  # 零值
            memory_pressure_threshold=0.0,  # 最小阈值
            memory_emergency_threshold=1.0,  # 最大阈值
        )
        
        # 配置应该被接受
        assert config.default_ttl_days == 0.0
        assert config.max_delete_per_run == 0
        
        logger.info("DEFECT-R4-009 VERIFIED: TTL config boundary values handled")
    
    @pytest.mark.asyncio
    async def test_memory_threshold_boundaries(self):
        """DEFECT-R4-010 [P1]: 测试内存阈值边界"""
        from memory_system.memory_ttl_expiration_manager import TTLConfig
        
        config = TTLConfig()
        
        # 验证阈值顺序正确
        assert config.memory_pressure_threshold < config.memory_critical_threshold
        assert config.memory_critical_threshold < config.aggressive_cleanup_threshold
        assert config.aggressive_cleanup_threshold < config.memory_emergency_threshold
        
        # 验证所有阈值在有效范围内
        assert 0.0 <= config.memory_pressure_threshold <= 1.0
        assert 0.0 <= config.memory_emergency_threshold <= 1.0
        
        logger.info("DEFECT-R4-010 VERIFIED: Memory threshold boundaries correct")


class TestPerformanceUnderLoad:
    """测试负载下的性能"""
    
    @pytest.mark.asyncio
    async def test_batch_processor_high_throughput(self):
        """DEFECT-R4-011 [P1]: 测试高吞吐量处理"""
        from memory_system.batch_processor import BatchProcessor
        
        processed_count = 0
        async def mock_processor(items: List[str]) -> List[int]:
            nonlocal processed_count
            processed_count += len(items)
            await asyncio.sleep(0.001)  # 模拟处理延迟
            return [len(item) for item in items]
        
        processor = BatchProcessor(
            processor=mock_processor,
            batch_size=50,
            max_wait_time=0.05,
            max_concurrent_batches=5
        )
        
        await processor.start()
        
        try:
            start_time = time.time()
            
            # 提交大量项目
            futures = []
            for i in range(200):
                future = await processor.submit(f"item_{i}")
                futures.append(future)
            
            # 等待所有完成
            results = await asyncio.gather(*futures, return_exceptions=True)
            elapsed = time.time() - start_time
            
            # 验证处理完成
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            assert success_count == 200
            
            # 验证性能（应在合理时间内完成）
            assert elapsed < 10.0  # 10秒内完成
            
            logger.info(f"DEFECT-R4-011 VERIFIED: High throughput test passed ({success_count} items in {elapsed:.2f}s)")
            
        finally:
            await processor.stop()
    
    @pytest.mark.asyncio
    async def test_batch_processor_memory_stability(self):
        """DEFECT-R4-012 [P1]: 测试内存稳定性"""
        from memory_system.batch_processor import BatchProcessor
        
        async def mock_processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        processor = BatchProcessor(
            processor=mock_processor,
            batch_size=100,
            max_wait_time=0.1,
            max_concurrent_batches=3
        )
        
        await processor.start()
        
        try:
            # 连续处理多批次
            for batch in range(10):
                futures = []
                for i in range(50):
                    future = await processor.submit(f"batch_{batch}_item_{i}")
                    futures.append(future)
                
                await asyncio.gather(*futures)
                
                # 检查统计信息
                stats = processor.get_stats()
                assert stats["queue_size"] <= 100  # 队列不应无限增长
            
            logger.info("DEFECT-R4-012 VERIFIED: Memory stability test passed")
            
        finally:
            await processor.stop()


class TestEdgeCases:
    """测试极端边界情况"""
    
    @pytest.mark.asyncio
    async def test_batch_processor_single_item(self):
        """DEFECT-R4-013 [P1]: 测试单项目处理"""
        from memory_system.batch_processor import BatchProcessor
        
        async def mock_processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        processor = BatchProcessor(
            processor=mock_processor,
            batch_size=100,  # 大batch_size
            max_wait_time=0.1,
            max_concurrent_batches=1
        )
        
        await processor.start()
        
        try:
            # 只提交一个项目
            future = await processor.submit("single")
            result = await asyncio.wait_for(future, timeout=2.0)
            
            assert result == 6  # "single"长度为6
            
            logger.info("DEFECT-R4-013 VERIFIED: Single item processing correct")
            
        finally:
            await processor.stop()
    
    @pytest.mark.asyncio
    async def test_batch_processor_rapid_start_stop(self):
        """DEFECT-R4-014 [P1]: 测试快速启停"""
        from memory_system.batch_processor import BatchProcessor
        
        async def mock_processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        # 快速多次启停
        for i in range(5):
            processor = BatchProcessor(
                processor=mock_processor,
                batch_size=10,
                max_wait_time=0.05,
                max_concurrent_batches=1
            )
            
            await processor.start()
            await asyncio.sleep(0.01)
            await processor.stop()
        
        logger.info("DEFECT-R4-014 VERIFIED: Rapid start/stop test passed")
    
    @pytest.mark.asyncio
    async def test_batch_processor_large_items(self):
        """DEFECT-R4-015 [P1]: 测试大项目处理"""
        from memory_system.batch_processor import BatchProcessor
        
        async def mock_processor(items: List[str]) -> List[int]:
            return [len(item) for item in items]
        
        processor = BatchProcessor(
            processor=mock_processor,
            batch_size=5,
            max_wait_time=0.1,
            max_concurrent_batches=1
        )
        
        await processor.start()
        
        try:
            # 提交大项目
            large_item = "x" * 1000000  # 1MB字符串
            future = await processor.submit(large_item)
            result = await asyncio.wait_for(future, timeout=5.0)
            
            assert result == 1000000
            
            logger.info("DEFECT-R4-015 VERIFIED: Large item processing correct")
            
        finally:
            await processor.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

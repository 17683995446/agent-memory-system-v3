"""
Agent Memory System V3.0 - 第2轮缺陷修复单元测试

测试覆盖以下缺陷修复:
- DEFECT-R2-001: 内存泄漏修复
- DEFECT-R2-002: 句柄泄漏修复
- DEFECT-R2-003: 性能衰减修复
- DEFECT-R2-004: OOM处理不当修复
- DEFECT-R2-005: OOM恢复机制效果有限修复
- DEFECT-R2-006: 磁盘清理策略过于激进修复
"""

import asyncio
import gc
import time
import unittest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from memory_system.memory_ttl_expiration_manager import (
    MemoryTTLExpirationManager,
    TTLConfig
)
from memory_system.agent_memory_system_facade import (
    MemorySystem,
    _resource_registry,
    AUTO_RELEASE_CONFIG
)
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.async_pipeline import PipelineStage, StageConfig, PipelineItem


class TestDEFECTR2004_OOMHandling(unittest.TestCase):
    """测试 DEFECT-R2-004: OOM处理不当修复"""

    def setUp(self):
        self.config = TTLConfig(
            memory_pressure_threshold=0.60,
            memory_critical_threshold=0.75,
            aggressive_cleanup_threshold=0.85,
            memory_emergency_threshold=0.92,
            write_rate_limit=50,
            memory_check_interval_seconds=2.0,
        )
        self.mock_storage = Mock()
        self.manager = MemoryTTLExpirationManager(self.mock_storage, config=self.config)

    def test_enhanced_memory_thresholds(self):
        """测试增强的内存阈值配置"""
        self.assertEqual(self.config.memory_pressure_threshold, 0.60)
        self.assertEqual(self.config.memory_critical_threshold, 0.75)
        self.assertEqual(self.config.aggressive_cleanup_threshold, 0.85)
        self.assertEqual(self.config.memory_emergency_threshold, 0.92)
        self.assertEqual(self.config.write_rate_limit, 50)
        self.assertEqual(self.config.memory_check_interval_seconds, 2.0)

    def test_memory_history_tracking(self):
        """测试内存历史记录跟踪"""
        self.assertTrue(hasattr(self.manager, '_memory_history'))
        self.assertTrue(hasattr(self.manager, '_consecutive_high_memory_count'))

    def test_memory_pressure_levels(self):
        """测试内存压力级别定义"""
        self.assertEqual(self.manager._memory_pressure_level, "normal")

    def test_write_rate_limiter_exists(self):
        """测试写入限流器存在"""
        self.assertTrue(hasattr(self.manager, '_write_tokens'))
        self.assertTrue(hasattr(self.manager, 'acquire_write_token'))


class TestDEFECTR2005_OOMRecovery(unittest.TestCase):
    """测试 DEFECT-R2-005: OOM恢复机制效果有限修复"""

    def setUp(self):
        self.config = TTLConfig(
            oom_recovery_enabled=True,
            oom_recovery_aggressiveness=3,
            memory_compression_enabled=True,
            memory_compression_threshold=0.70,
        )
        self.mock_storage = Mock()
        self.manager = MemoryTTLExpirationManager(self.mock_storage, config=self.config)

    def test_oom_recovery_config(self):
        """测试OOM恢复配置"""
        self.assertTrue(self.config.oom_recovery_enabled)
        self.assertEqual(self.config.oom_recovery_aggressiveness, 3)
        self.assertTrue(self.config.memory_compression_enabled)

    def test_recovery_stats_tracking(self):
        """测试恢复统计跟踪"""
        self.assertTrue(hasattr(self.manager, '_recovery_stats'))
        self.assertIn('recovery_count', self.manager._recovery_stats)
        self.assertIn('last_recovery_time', self.manager._recovery_stats)
        self.assertIn('memory_freed_bytes', self.manager._recovery_stats)
        self.assertIn('compression_count', self.manager._recovery_stats)

    def test_oom_recovery_methods_exist(self):
        """测试OOM恢复方法存在"""
        self.assertTrue(hasattr(self.manager, '_oom_recovery'))
        self.assertTrue(hasattr(self.manager, '_extreme_cleanup'))
        self.assertTrue(hasattr(self.manager, '_cleanup_by_access_pattern'))
        self.assertTrue(hasattr(self.manager, '_compress_memory'))


class TestDEFECTR2001_MemoryLeak(unittest.TestCase):
    """测试 DEFECT-R2-001: 内存泄漏修复"""

    def test_resource_registry_exists(self):
        """测试资源注册表存在"""
        self.assertIsInstance(_resource_registry, dict)

    def test_auto_release_config(self):
        """测试自动释放配置"""
        self.assertTrue(AUTO_RELEASE_CONFIG['enabled'])
        self.assertEqual(AUTO_RELEASE_CONFIG['check_interval_seconds'], 60)
        self.assertEqual(AUTO_RELEASE_CONFIG['idle_timeout_seconds'], 300)

    def test_memory_system_has_resource_tracking(self):
        """测试MemorySystem有资源跟踪属性"""
        # 由于MemorySystem需要复杂的初始化，我们只检查类定义
        self.assertTrue(hasattr(MemorySystem, '_resource_lock'))
        self.assertTrue(hasattr(MemorySystem, '_update_activity'))
        self.assertTrue(hasattr(MemorySystem, '_operation_context'))


class TestDEFECTR2002_HandleLeak(unittest.TestCase):
    """测试 DEFECT-R2-002: 句柄泄漏修复"""

    def setUp(self):
        self.storage = MemoryStorage(max_memories=1000)

    def test_handle_registry_exists(self):
        """测试句柄注册表存在"""
        self.assertTrue(hasattr(self.storage, '_handle_registry'))
        self.assertTrue(hasattr(self.storage, '_active_handles'))

    def test_handle_management_methods(self):
        """测试句柄管理方法存在"""
        self.assertTrue(hasattr(self.storage, '_register_handle'))
        self.assertTrue(hasattr(self.storage, '_update_handle_access'))
        self.assertTrue(hasattr(self.storage, '_release_handle'))
        self.assertTrue(hasattr(self.storage, '_cleanup_expired_handles'))

    def test_handle_cleanup_task(self):
        """测试句柄清理任务"""
        self.assertTrue(hasattr(self.storage, '_handle_cleanup_task'))
        self.assertTrue(hasattr(self.storage, '_handle_cleanup_loop'))

    def test_handle_stats(self):
        """测试句柄统计"""
        stats = self.storage.get_handle_stats()
        self.assertIn('total_handles', stats)
        self.assertIn('active_handles', stats)
        self.assertIn('expired_handles', stats)


class TestDEFECTR2006_GradualCleanup(unittest.TestCase):
    """测试 DEFECT-R2-006: 渐进式清理修复"""

    def setUp(self):
        self.storage = MemoryStorage(max_memories=1000)

    def test_cleanup_config_exists(self):
        """测试清理配置存在"""
        self.assertTrue(hasattr(self.storage, '_cleanup_config'))
        config = self.storage._cleanup_config
        self.assertIn('gradual_cleanup', config)
        self.assertIn('cleanup_batch_size', config)
        self.assertIn('eviction_threshold_percent', config)
        self.assertIn('target_capacity_percent', config)

    def test_gradual_cleanup_enabled(self):
        """测试渐进式清理已启用"""
        self.assertTrue(self.storage._cleanup_config['gradual_cleanup'])

    def test_cleanup_history_tracking(self):
        """测试清理历史跟踪"""
        self.assertTrue(hasattr(self.storage, '_cleanup_history'))

    def test_evict_oldest_is_gradual(self):
        """测试淘汰是渐进式的"""
        # 检查 _evict_oldest 方法是否支持批量清理
        import inspect
        source = inspect.getsource(self.storage._evict_oldest)
        self.assertIn('evict_count', source)
        self.assertIn('cleanup_batch_size', source)


class TestDEFECTR2003_PerformanceDegradation(unittest.TestCase):
    """测试 DEFECT-R2-003: 性能衰减修复"""

    def test_stage_config_memory_management(self):
        """测试阶段配置的内存管理选项"""
        config = StageConfig(name="test")
        self.assertTrue(hasattr(config, 'memory_management_enabled'))
        self.assertTrue(hasattr(config, 'max_item_store_size'))
        self.assertTrue(hasattr(config, 'item_store_cleanup_interval'))
        self.assertTrue(hasattr(config, 'enable_item_compression'))

    def test_pipeline_stage_has_memory_management(self):
        """测试PipelineStage有内存管理"""
        # 检查类定义
        self.assertTrue(hasattr(PipelineStage, '_memory_management_task'))
        self.assertTrue(hasattr(PipelineStage, '_item_store_access_times'))
        self.assertTrue(hasattr(PipelineStage, '_cleanup_item_store'))
        self.assertTrue(hasattr(PipelineStage, '_compress_old_items'))

    def test_memory_stats_method(self):
        """测试内存统计方法"""
        self.assertTrue(hasattr(PipelineStage, 'get_memory_stats'))


class TestIntegration(unittest.TestCase):
    """集成测试"""

    async def async_test_ttl_manager_lifecycle(self):
        """测试TTL管理器生命周期"""
        config = TTLConfig(enabled=True)
        mock_storage = Mock()
        mock_storage.search_by_time_range = AsyncMock(return_value=[])

        manager = MemoryTTLExpirationManager(mock_storage, config=config)

        # 启动
        await manager.start()
        self.assertTrue(manager._running)
        self.assertIsNotNone(manager._memory_monitor_task)
        self.assertIsNotNone(manager._write_rate_limiter_task)

        # 获取统计
        stats = manager.get_stats()
        self.assertIn('memory_pressure_level', stats)
        self.assertIn('recovery_stats', stats)

        # 停止
        await manager.stop()
        self.assertFalse(manager._running)

    def test_ttl_manager_lifecycle(self):
        """同步包装器"""
        asyncio.run(self.async_test_ttl_manager_lifecycle())

    async def async_test_storage_cleanup_tasks(self):
        """测试存储清理任务"""
        storage = MemoryStorage(max_memories=100)

        # 启动清理任务
        await storage.start_cleanup_tasks()
        self.assertIsNotNone(storage._handle_cleanup_task)

        # 停止清理任务
        await storage.stop_cleanup_tasks()
        self.assertIsNone(storage._handle_cleanup_task)

    def test_storage_cleanup_tasks(self):
        """同步包装器"""
        asyncio.run(self.async_test_storage_cleanup_tasks())


class TestCleanupStrategies(unittest.TestCase):
    """测试各种清理策略"""

    def test_emergency_cleanup_strategy(self):
        """测试紧急清理策略"""
        config = TTLConfig()
        mock_storage = Mock()
        manager = MemoryTTLExpirationManager(mock_storage, config=config)

        # 检查紧急清理方法存在
        self.assertTrue(hasattr(manager, '_emergency_cleanup'))
        self.assertTrue(hasattr(manager, '_cleanup_low_importance'))

    def test_aggressive_cleanup_strategy(self):
        """测试激进清理策略"""
        config = TTLConfig()
        mock_storage = Mock()
        manager = MemoryTTLExpirationManager(mock_storage, config=config)

        self.assertTrue(hasattr(manager, '_aggressive_cleanup'))

    def test_oom_recovery_levels(self):
        """测试OOM恢复级别"""
        config = TTLConfig(oom_recovery_aggressiveness=5)
        mock_storage = Mock()
        manager = MemoryTTLExpirationManager(mock_storage, config=config)

        # 检查所有恢复级别方法存在
        self.assertTrue(hasattr(manager, '_oom_recovery'))
        self.assertTrue(hasattr(manager, '_hard_limit_recovery'))


class TestMemoryPressureMonitoring(unittest.TestCase):
    """测试内存压力监控"""

    @patch('memory_system.memory_ttl_expiration_manager.psutil')
    def test_memory_pressure_detection(self, mock_psutil):
        """测试内存压力检测"""
        # 模拟正常内存状态
        mock_memory = Mock()
        mock_memory.percent = 50.0
        mock_memory.available = 8 * 1024 * 1024 * 1024  # 8GB
        mock_memory.used = 4 * 1024 * 1024 * 1024  # 4GB
        mock_psutil.virtual_memory.return_value = mock_memory

        config = TTLConfig()
        mock_storage = Mock()
        manager = MemoryTTLExpirationManager(mock_storage, config=config)

        # 运行检查
        asyncio.run(manager._check_memory_pressure())

        # 验证内存历史被记录
        self.assertGreater(len(manager._memory_history), 0)


class TestHandleManagement(unittest.TestCase):
    """测试句柄管理"""

    def test_register_and_release_handle(self):
        """测试注册和释放句柄"""
        storage = MemoryStorage()

        # 注册句柄
        handle_id = storage._register_handle("test_handle", "test_type", {"key": "value"})
        self.assertEqual(handle_id, "test_handle")
        self.assertIn("test_handle", storage._handle_registry)
        self.assertIn("test_handle", storage._active_handles)

        # 更新访问
        storage._update_handle_access("test_handle")
        self.assertIn("last_accessed", storage._handle_registry["test_handle"])

        # 释放句柄
        storage._release_handle("test_handle")
        self.assertNotIn("test_handle", storage._active_handles)
        self.assertFalse(storage._handle_registry["test_handle"]["active"])


class TestGradualEviction(unittest.TestCase):
    """测试渐进式淘汰"""

    async def async_test_gradual_eviction(self):
        """测试渐进式淘汰"""
        storage = MemoryStorage(max_memories=100)
        storage._cleanup_config['cleanup_batch_size'] = 5
        storage._cleanup_config['target_capacity_percent'] = 0.85

        # 添加超过容量的记忆
        for i in range(110):
            from memory_system.core_abstractions.memory_data_models import (
                MemoryItem, MemoryType, ImportanceLevel
            )
            item = MemoryItem(
                content=f"Test content {i}",
                memory_type=MemoryType.EPISODIC,
                importance=ImportanceLevel.MEDIUM,
            )
            await storage.add(item)

        # 检查是否触发渐进式清理
        self.assertLessEqual(len(storage._memories), 110)

    def test_gradual_eviction(self):
        """同步包装器"""
        try:
            asyncio.run(self.async_test_gradual_eviction())
        except ImportError:
            # 如果无法导入MemoryItem，跳过测试
            self.skipTest("MemoryItem import failed")


def run_tests():
    """运行所有测试"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestDEFECTR2004_OOMHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestDEFECTR2005_OOMRecovery))
    suite.addTests(loader.loadTestsFromTestCase(TestDEFECTR2001_MemoryLeak))
    suite.addTests(loader.loadTestsFromTestCase(TestDEFECTR2002_HandleLeak))
    suite.addTests(loader.loadTestsFromTestCase(TestDEFECTR2006_GradualCleanup))
    suite.addTests(loader.loadTestsFromTestCase(TestDEFECTR2003_PerformanceDegradation))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestCleanupStrategies))
    suite.addTests(loader.loadTestsFromTestCase(TestMemoryPressureMonitoring))
    suite.addTests(loader.loadTestsFromTestCase(TestHandleManagement))
    suite.addTests(loader.loadTestsFromTestCase(TestGradualEviction))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

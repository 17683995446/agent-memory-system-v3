"""
测试 TTL 过期管理器
"""

import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import tempfile
import shutil

from memory_system.memory_ttl_expiration_manager import (
    MemoryTTLExpirationManager,
    TTLConfig,
    ArchiveOnExpire
)
from memory_system.core_abstractions.memory_data_models import MemoryItem, ImportanceLevel, MemoryType


class TestTTLConfig:
    """测试 TTL 配置"""

    def test_ttl_config_defaults(self):
        """测试默认配置"""
        config = TTLConfig()
        
        assert config.enabled is True
        assert config.default_ttl_days == 365.0
        assert config.check_interval_hours == 24.0
        assert config.max_delete_per_run == 1000
        
        # 验证按重要性设置的 TTL
        assert config.ttl_by_importance[5] == 730.0  # CRITICAL: 2年
        assert config.ttl_by_importance[4] == 365.0  # HIGH: 1年
        assert config.ttl_by_importance[3] == 180.0  # MEDIUM: 6个月
        assert config.ttl_by_importance[2] == 30.0   # LOW: 1个月
        assert config.ttl_by_importance[1] == 7.0    # TRIVIAL: 1周

    def test_ttl_config_custom(self):
        """测试自定义配置"""
        custom_ttl = {5: 1000.0, 4: 500.0, 3: 250.0, 2: 100.0, 1: 10.0}
        config = TTLConfig(
            enabled=False,
            default_ttl_days=180.0,
            check_interval_hours=12.0,
            max_delete_per_run=500,
            ttl_by_importance=custom_ttl
        )
        
        assert config.enabled is False
        assert config.default_ttl_days == 180.0
        assert config.check_interval_hours == 12.0
        assert config.max_delete_per_run == 500
        assert config.ttl_by_importance == custom_ttl


class TestMemoryTTLExpirationManager:
    """测试 TTL 过期管理器"""

    @pytest.fixture
    def mock_storage(self):
        """创建模拟存储"""
        storage = MagicMock()
        storage.search_by_time_range = AsyncMock(return_value=[])
        storage.delete = AsyncMock()
        storage.get = AsyncMock(return_value=None)
        return storage

    @pytest.fixture
    def ttl_manager(self, mock_storage):
        """创建 TTL 管理器"""
        config = TTLConfig(enabled=True, check_interval_hours=0.001)  # 很短的间隔用于测试
        return MemoryTTLExpirationManager(mock_storage, config=config)

    def test_is_expired_by_timestamp(self, mock_storage):
        """测试基于时间戳的过期判断"""
        config = TTLConfig(enabled=True, default_ttl_days=1.0)  # 1天TTL
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        now = time.time()
        
        # 创建一个刚好2天前的记忆，使用自定义TTL 1天（应该过期）
        memory = MagicMock()
        memory.timestamp = now - (2 * 24 * 3600)  # 2天前
        memory.metadata = {"ttl_days": 1.0}  # 自定义1天TTL
        memory.importance = ImportanceLevel.MEDIUM
        
        assert manager._is_expired(memory, now) is True
        
        # 创建一个12小时前的记忆，使用自定义TTL 1天（不应该过期）
        memory2 = MagicMock()
        memory2.timestamp = now - (12 * 3600)  # 12小时前
        memory2.metadata = {"ttl_days": 1.0}  # 自定义1天TTL
        memory2.importance = ImportanceLevel.MEDIUM
        assert manager._is_expired(memory2, now) is False

    def test_is_expired_by_importance(self, mock_storage):
        """测试基于重要性的不同TTL"""
        config = TTLConfig(enabled=True)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        now = time.time()
        
        # TRIVIAL (1周TTL) - 10天前应该过期
        memory_trivial = MagicMock()
        memory_trivial.timestamp = now - (10 * 24 * 3600)
        memory_trivial.metadata = {}
        memory_trivial.importance = ImportanceLevel.TRIVIAL
        assert manager._is_expired(memory_trivial, now) is True
        
        # CRITICAL (2年TTL) - 10天前不应该过期
        memory_critical = MagicMock()
        memory_critical.timestamp = now - (10 * 24 * 3600)
        memory_critical.metadata = {}
        memory_critical.importance = ImportanceLevel.CRITICAL
        assert manager._is_expired(memory_critical, now) is False

    @pytest.mark.asyncio
    async def test_cleanup_expired(self, mock_storage, ttl_manager):
        """测试清理过期记忆"""
        now = time.time()
        
        # 创建一些模拟记忆
        expired_memory = MagicMock()
        expired_memory.memory_id = "expired_1"
        expired_memory.timestamp = now - (400 * 24 * 3600)  # 400天前
        expired_memory.metadata = {}
        expired_memory.importance = ImportanceLevel.HIGH  # 1年TTL
        
        valid_memory = MagicMock()
        valid_memory.memory_id = "valid_1"
        valid_memory.timestamp = now - (100 * 24 * 3600)  # 100天前
        valid_memory.metadata = {}
        valid_memory.importance = ImportanceLevel.HIGH
        
        mock_storage.search_by_time_range.return_value = [expired_memory, valid_memory]
        
        deleted_count = await ttl_manager.cleanup_expired()
        
        assert deleted_count == 1
        mock_storage.delete.assert_called_once_with("expired_1")

    @pytest.mark.asyncio
    async def test_cleanup_respects_max_delete(self, mock_storage):
        """测试尊重最大删除数"""
        config = TTLConfig(enabled=True, max_delete_per_run=2)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        now = time.time()
        
        # 创建5个过期记忆
        memories = []
        for i in range(5):
            memory = MagicMock()
            memory.memory_id = f"expired_{i}"
            memory.timestamp = now - (400 * 24 * 3600)
            memory.metadata = {}
            memory.importance = ImportanceLevel.HIGH
            memories.append(memory)
        
        mock_storage.search_by_time_range.return_value = memories
        
        deleted_count = await manager.cleanup_expired()
        
        # 应该只删除2个（max_delete_per_run）
        assert deleted_count == 2
        assert mock_storage.delete.call_count == 2

    def test_get_ttl_for_memory(self, mock_storage):
        """测试获取记忆TTL"""
        config = TTLConfig(enabled=True, default_ttl_days=200.0)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        # 测试自定义TTL
        memory_custom = MagicMock()
        memory_custom.metadata = {"ttl_days": 50.0}
        memory_custom.importance = ImportanceLevel.HIGH
        assert manager._get_ttl_for_memory(memory_custom) == 50.0
        
        # 测试按重要性获取TTL
        memory_high = MagicMock()
        memory_high.metadata = {}
        memory_high.importance = ImportanceLevel.HIGH
        assert manager._get_ttl_for_memory(memory_high) == 365.0
        
        # 测试默认TTL（未知重要性）
        memory_unknown = MagicMock()
        memory_unknown.metadata = {}
        memory_unknown.importance = MagicMock()
        memory_unknown.importance.value = 999  # 未知重要性
        assert manager._get_ttl_for_memory(memory_unknown) == 200.0

    @pytest.mark.asyncio
    async def test_set_ttl(self, mock_storage):
        """测试设置自定义TTL"""
        manager = MemoryTTLExpirationManager(mock_storage)
        
        memory = MagicMock()
        memory.metadata = {}
        mock_storage.get.return_value = memory
        
        result = await manager.set_ttl("memory_1", 30.0)
        
        assert result is True
        assert memory.metadata["ttl_days"] == 30.0

    @pytest.mark.asyncio
    async def test_set_ttl_not_found(self, mock_storage):
        """测试设置TTL时记忆不存在"""
        manager = MemoryTTLExpirationManager(mock_storage)
        mock_storage.get.return_value = None
        
        result = await manager.set_ttl("nonexistent", 30.0)
        
        assert result is False

    @pytest.mark.asyncio
    async def test_get_expiration_info(self, mock_storage):
        """测试获取过期信息"""
        manager = MemoryTTLExpirationManager(mock_storage)
        
        now = time.time()
        memory = MagicMock()
        memory.timestamp = now - (100 * 24 * 3600)  # 100天前
        memory.metadata = {}
        memory.importance = ImportanceLevel.HIGH  # 365天TTL
        
        mock_storage.get.return_value = memory
        
        info = await manager.get_expiration_info("memory_1")
        
        assert info is not None
        assert info["memory_id"] == "memory_1"
        assert info["created_at"] == memory.timestamp
        assert info["ttl_days"] == 365.0
        assert info["is_expired"] is False
        assert info["days_remaining"] > 200  # 还剩200多天

    @pytest.mark.asyncio
    async def test_get_expiration_info_expired(self, mock_storage):
        """测试获取已过期记忆的信息"""
        manager = MemoryTTLExpirationManager(mock_storage)
        
        now = time.time()
        memory = MagicMock()
        memory.timestamp = now - (400 * 24 * 3600)  # 400天前
        memory.metadata = {}
        memory.importance = ImportanceLevel.HIGH  # 365天TTL
        
        mock_storage.get.return_value = memory
        
        info = await manager.get_expiration_info("memory_1")
        
        assert info["is_expired"] is True
        assert info["days_remaining"] == 0

    @pytest.mark.asyncio
    async def test_get_expiration_info_not_found(self, mock_storage):
        """测试获取不存在的记忆的过期信息"""
        manager = MemoryTTLExpirationManager(mock_storage)
        mock_storage.get.return_value = None
        
        info = await manager.get_expiration_info("nonexistent")
        
        assert info is None

    def test_get_stats(self, mock_storage):
        """测试获取统计"""
        manager = MemoryTTLExpirationManager(mock_storage)
        
        stats = manager.get_stats()
        
        assert "total_cleaned" in stats
        assert "last_cleanup_time" in stats
        assert "last_cleanup_count" in stats
        assert "running" in stats
        assert "enabled" in stats
        
        assert stats["total_cleaned"] == 0
        assert stats["running"] is False
        assert stats["enabled"] is True

    @pytest.mark.asyncio
    async def test_preview_expired(self, mock_storage):
        """测试预览过期记忆"""
        manager = MemoryTTLExpirationManager(mock_storage)
        
        now = time.time()
        
        # 创建过期和未过期记忆
        expired = MagicMock()
        expired.memory_id = "expired_1"
        expired.timestamp = now - (400 * 24 * 3600)
        expired.metadata = {}
        expired.importance = ImportanceLevel.HIGH
        
        valid = MagicMock()
        valid.memory_id = "valid_1"
        valid.timestamp = now - (100 * 24 * 3600)
        valid.metadata = {}
        valid.importance = ImportanceLevel.HIGH
        
        mock_storage.search_by_time_range.return_value = [expired, valid]
        
        expired_ids = await manager.preview_expired()
        
        assert len(expired_ids) == 1
        assert "expired_1" in expired_ids
        # 确保没有删除任何记忆
        mock_storage.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_stop(self, mock_storage):
        """测试启动和停止清理任务"""
        config = TTLConfig(enabled=True, check_interval_hours=0.001)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        # 启动
        await manager.start()
        assert manager._running is True
        assert manager._cleanup_task is not None
        
        # 等待一小段时间让循环运行
        await asyncio.sleep(0.01)
        
        # 停止
        await manager.stop()
        assert manager._running is False
        assert manager._cleanup_task is None

    @pytest.mark.asyncio
    async def test_start_disabled(self, mock_storage):
        """测试禁用时启动"""
        config = TTLConfig(enabled=False)
        manager = MemoryTTLExpirationManager(mock_storage, config=config)
        
        await manager.start()
        
        assert manager._running is False
        assert manager._cleanup_task is None

    @pytest.mark.asyncio
    async def test_cleanup_with_callback(self, mock_storage):
        """测试带回调的清理"""
        callback_mock = MagicMock()
        manager = MemoryTTLExpirationManager(mock_storage, on_expire=callback_mock)
        
        now = time.time()
        
        expired_memory = MagicMock()
        expired_memory.memory_id = "expired_1"
        expired_memory.timestamp = now - (400 * 24 * 3600)
        expired_memory.metadata = {}
        expired_memory.importance = ImportanceLevel.HIGH
        
        mock_storage.search_by_time_range.return_value = [expired_memory]
        
        await manager.cleanup_expired()
        
        # 验证回调被调用
        callback_mock.assert_called_once()
        args = callback_mock.call_args[0]
        assert args[0] == "expired_1"


class TestArchiveOnExpire:
    """测试归档处理器"""

    @pytest.fixture
    def temp_dir(self):
        """创建临时目录"""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path)

    @pytest.mark.asyncio
    async def test_archive_callback(self, temp_dir):
        """测试归档回调"""
        archiver = ArchiveOnExpire(archive_dir=temp_dir)
        
        context = {"timestamp": 1234567890.0}
        await archiver("memory_123", context)
        
        # 验证文件被创建
        archive_file = Path(temp_dir) / "memory_123.json"
        assert archive_file.exists()
        
        # 验证内容
        import json
        with open(archive_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        assert data["memory_id"] == "memory_123"
        assert data["archived_at"] == 1234567890.0
        assert data["reason"] == "ttl_expired"

    def test_archive_dir_created(self, temp_dir):
        """测试归档目录自动创建"""
        new_dir = Path(temp_dir) / "new_archive"
        assert not new_dir.exists()
        
        ArchiveOnExpire(archive_dir=str(new_dir))
        
        assert new_dir.exists()

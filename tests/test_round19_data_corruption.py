"""第19轮深度实践 - 数据损坏恢复测试

测试策略:
1. 模拟数据损坏 - 字节翻转、截断、注入
2. 校验和验证 - 检测数据完整性
3. 数据恢复机制 - 从备份/副本恢复
4. 部分损坏处理 - 部分数据可用
5. 损坏检测延迟 - 延迟发现损坏
"""

import asyncio
import os
import random
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDataCorruption:
    """数据损坏测试"""

    def test_corrupted_serialization_detection(self):
        """损坏序列化数据检测"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
            from memory_system.exceptions import SerializationError
        except ImportError:
            pytest.skip("Required modules not available")
            return

        serializer = JSONSerializer()
        data = {"key": "value", "number": 42}

        # 正常序列化
        serialized = serializer.serialize(data)

        # 模拟损坏：修改字节
        corrupted = bytearray(serialized)
        if len(corrupted) > 10:
            corrupted[10] ^= 0xFF  # 翻转字节

        # 应该检测到损坏
        try:
            serializer.deserialize(bytes(corrupted))
            assert False, "Should have detected corruption"
        except SerializationError:
            pass  # 预期行为

    def test_truncated_data_handling(self):
        """截断数据处理"""
        try:
            from memory_system.snapshot_serializer import JSONSerializer
            from memory_system.exceptions import SerializationError
        except ImportError:
            pytest.skip("Required modules not available")
            return

        serializer = JSONSerializer()
        data = {"items": list(range(100))}

        serialized = serializer.serialize(data)

        # 截断数据
        truncated = serialized[:len(serialized)//2]

        # 应该检测到截断
        try:
            serializer.deserialize(truncated)
            assert False, "Should have detected truncation"
        except SerializationError:
            pass

    @pytest.mark.asyncio
    async def test_data_recovery_from_backup(self):
        """从备份恢复数据"""
        try:
            from memory_system.storage_backends.in_memory_storage_backend import InMemoryStorageBackend
            from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
        except ImportError:
            pytest.skip("Required modules not available")
            return

        primary = InMemoryStorageBackend()
        backup = InMemoryStorageBackend()

        # 写入数据到主存储和备份
        for i in range(100):
            item = MemoryItem(
                memory_id=f"mem_{i}",
                content=f"content_{i}",
                memory_type=MemoryType.SHORT_TERM,
                agent_id="test_agent",
            )
            await primary.add(item)
            await backup.add(item)

        # 模拟主存储损坏：删除一半数据
        for i in range(50):
            try:
                await primary.remove(f"mem_{i}")
            except:
                pass

        # 从备份恢复
        recovered = 0
        for i in range(50):
            try:
                backup_item = await backup.get(f"mem_{i}")
                await primary.add(backup_item)
                recovered += 1
            except:
                pass

        print(f"\n数据恢复测试:")
        print(f"  恢复数量: {recovered}")

        assert recovered == 50


class TestChecksumValidation:
    """校验和验证测试"""

    def test_checksum_mismatch_detection(self):
        """校验和不匹配检测"""
        import hashlib

        data = b"important data"
        checksum = hashlib.sha256(data).hexdigest()

        # 验证正确数据
        assert hashlib.sha256(data).hexdigest() == checksum

        # 验证错误数据
        corrupted = b"corrupted data"
        assert hashlib.sha256(corrupted).hexdigest() != checksum


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

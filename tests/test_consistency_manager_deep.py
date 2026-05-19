"""
Consistency Manager 深度测试 - 覆盖 consistency_manager.py 模块 (之前零覆盖)

测试范围:
- ConsistencyLevel, OperationType 枚举
- ConsistencyConfig 数据类
- ConsistencyManager: start, stop, write, read, create_session, close_session, get_stats
- ConsistencyVerifier: verify_linearizability, verify_causal_consistency

发现的 BUG:
- BUG-CM-001: ConsistencyLevel 枚举缺少 MONOTONIC_WRITES 成员。
  consistency_manager.py 第238行引用 ConsistencyLevel.MONOTONIC_WRITES，
  但该值在枚举中不存在，导致 AttributeError 被捕获为通用异常。
  这使得所有写入操作（不仅是 SESSION 级别）都失败，因为 Python 在评估
  `consistency in [SESSION, READ_YOUR_WRITES, MONOTONIC_WRITES]` 时
  会抛出 AttributeError，阻止代码到达 else 分支。
- BUG-CM-002: _write_linearizable 在无 Leader 时返回 OperationResult(success=False)，
  但 ConsistencyManager.write() 的 except 块仅捕获 Exception，而该返回值
  是正常返回（非异常），导致 failures 计数器不递增。
- BUG-CM-003: _read_linearizable 使用 self._storage.read(key) 读取数据，
  而 _write_causal 使用 self._local_store[key] 写入数据。
  两个不同的存储后端导致 CAUSAL 写入的数据无法通过 LINEARIZABLE 读取到。
"""

import sys
sys.path.insert(0, '/workspace/agent_memory_system_v2')

import asyncio
import pytest
from datetime import datetime

from memory_system.consistency_manager import (
    ConsistencyLevel,
    OperationType,
    ConsistencyConfig,
    OperationResult,
    ConsistencyManager,
    ConsistencyVerifier,
)
from memory_system.session_consistency import SessionConsistencyLevel


# ============================================================================
# 枚举和数据类测试
# ============================================================================

class TestEnumsAndDataClasses:
    """枚举和数据类测试"""

    def test_consistency_level_values(self):
        """ConsistencyLevel 枚举值完整性"""
        assert ConsistencyLevel.EVENTUAL.value == "eventual"
        assert ConsistencyLevel.CAUSAL.value == "causal"
        assert ConsistencyLevel.SESSION.value == "session"
        assert ConsistencyLevel.MONOTONIC_READS.value == "monotonic_reads"
        assert ConsistencyLevel.READ_YOUR_WRITES.value == "read_your_writes"
        assert ConsistencyLevel.BOUNDED_STALENESS.value == "bounded_staleness"
        assert ConsistencyLevel.LINEARIZABLE.value == "linearizable"
        assert ConsistencyLevel.STRONG.value == "strong"

    def test_missing_monotonic_writes_enum(self):
        """[BUG-CM-001] ConsistencyLevel 缺少 MONOTONIC_WRITES - 已修复
        consistency_manager.py 第238行引用 ConsistencyLevel.MONOTONIC_WRITES，
        该枚举值已添加。
        """
        # 修复后 MONOTONIC_WRITES 应正常存在
        assert ConsistencyLevel.MONOTONIC_WRITES is not None
        assert ConsistencyLevel.MONOTONIC_WRITES.value == "monotonic_writes"

    def test_operation_type_values(self):
        """OperationType 枚举值完整性"""
        assert OperationType.READ.value == "read"
        assert OperationType.WRITE.value == "write"
        assert OperationType.DELETE.value == "delete"
        assert OperationType.CAS.value == "compare_and_swap"

    def test_consistency_config_defaults(self):
        """ConsistencyConfig 默认值"""
        config = ConsistencyConfig()
        assert config.default_consistency == ConsistencyLevel.LINEARIZABLE
        assert config.session_timeout == 3600.0
        assert config.quorum_read is None
        assert config.quorum_write is None
        assert config.partition_threshold == 3
        assert config.max_retry == 3
        assert config.retry_delay == 0.1

    def test_consistency_config_custom(self):
        """ConsistencyConfig 自定义值"""
        config = ConsistencyConfig(
            default_consistency=ConsistencyLevel.CAUSAL,
            session_timeout=1800.0,
            quorum_read=2,
            quorum_write=3,
        )
        assert config.default_consistency == ConsistencyLevel.CAUSAL
        assert config.session_timeout == 1800.0
        assert config.quorum_read == 2
        assert config.quorum_write == 3

    def test_operation_result_success(self):
        """OperationResult 成功结果"""
        result = OperationResult(success=True, value="hello")
        assert result.success is True
        assert result.value == "hello"
        assert bool(result) is True

    def test_operation_result_failure(self):
        """OperationResult 失败结果"""
        result = OperationResult(success=False, error="something went wrong")
        assert result.success is False
        assert result.error == "something went wrong"
        assert bool(result) is False


# ============================================================================
# ConsistencyManager 基本读写测试
# ============================================================================

class TestConsistencyManagerBasic:
    """ConsistencyManager 基本读写操作测试"""

    @pytest.fixture
    def manager(self):
        """创建并启动一致性管理器"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
            config=ConsistencyConfig(default_consistency=ConsistencyLevel.CAUSAL),
        )
        return cm

    @pytest.mark.asyncio
    async def test_start(self, manager):
        """启动一致性管理器"""
        await manager.start()
        assert manager._running is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_stop(self, manager):
        """停止一致性管理器"""
        await manager.start()
        await manager.stop()
        assert manager._running is False

    @pytest.mark.asyncio
    async def test_basic_write(self, manager):
        """基本写入操作"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        result = await manager.write("key1", "value1", consistency=ConsistencyLevel.CAUSAL)
        assert result.success is True
        assert result.value is None  # 写操作无返回值
        assert result.timestamp is not None
        assert result.vector_clock is not None
        await manager.stop()

    @pytest.mark.asyncio
    async def test_basic_read(self, manager):
        """基本读取操作"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        # 先写入
        await manager.write("key1", "value1", consistency=ConsistencyLevel.CAUSAL)

        # 再读取
        result = await manager.read("key1", consistency=ConsistencyLevel.CAUSAL)
        assert result.success is True
        assert result.value == "value1"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_read_nonexistent_key(self, manager):
        """读取不存在的 key"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        result = await manager.read("nonexistent", consistency=ConsistencyLevel.CAUSAL)
        assert result.success is False
        assert "not found" in result.error.lower()
        await manager.stop()

    @pytest.mark.asyncio
    async def test_write_after_read_consistency(self, manager):
        """写后读一致性 - 写入后立即读取应返回最新值"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        await manager.write("key1", "value1", consistency=ConsistencyLevel.CAUSAL)
        result = await manager.read("key1", consistency=ConsistencyLevel.CAUSAL)
        assert result.value == "value1"

        # 覆盖写入
        await manager.write("key1", "value2", consistency=ConsistencyLevel.CAUSAL)
        result = await manager.read("key1", consistency=ConsistencyLevel.CAUSAL)
        assert result.value == "value2"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_duplicate_key_write(self, manager):
        """重复 key 写入 - 应覆盖旧值"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        await manager.write("key1", "old_value", consistency=ConsistencyLevel.CAUSAL)
        await manager.write("key1", "new_value", consistency=ConsistencyLevel.CAUSAL)

        result = await manager.read("key1", consistency=ConsistencyLevel.CAUSAL)
        assert result.success is True
        assert result.value == "new_value"
        await manager.stop()


# ============================================================================
# 会话管理测试
# ============================================================================

class TestConsistencyManagerSession:
    """会话一致性测试"""

    @pytest.fixture
    def manager(self):
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
            config=ConsistencyConfig(default_consistency=ConsistencyLevel.CAUSAL),
        )
        return cm

    @pytest.mark.asyncio
    async def test_create_session(self, manager):
        """创建会话"""
        await manager.start()
        session_id = manager.create_session("user1")
        assert session_id is not None
        assert len(session_id) > 0
        await manager.stop()

    @pytest.mark.asyncio
    async def test_close_session(self, manager):
        """关闭会话"""
        await manager.start()
        session_id = manager.create_session("user1")
        result = manager.close_session(session_id)
        assert result is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_close_nonexistent_session(self, manager):
        """关闭不存在的会话"""
        await manager.start()
        result = manager.close_session("nonexistent-session-id")
        assert result is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_session_consistency_level(self, manager):
        """创建会话时指定一致性级别"""
        await manager.start()
        session_id = manager.create_session(
            "user1",
            consistency_level=SessionConsistencyLevel.READ_YOUR_WRITES,
        )
        assert session_id is not None
        await manager.stop()

    @pytest.mark.asyncio
    async def test_session_write_and_read(self, manager):
        """[BUG-CM-001] 会话内写后读一致性 - 已修复
        MONOTONIC_WRITES 枚举已添加，SESSION 级别写入正常工作。
        """
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        session_id = manager.create_session("user1")

        # 修复后 SESSION 级别写入应成功
        write_result = await manager.write(
            "session_key", "session_value",
            consistency=ConsistencyLevel.SESSION,
            session_id=session_id,
        )
        assert write_result.success is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_session_without_id_degrades(self, manager):
        """[BUG-CM-001] 无 session_id 时降级为因果一致性 - 已修复
        MONOTONIC_WRITES 枚举已添加，SESSION 级别操作正常工作。
        """
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        # 修复后 SESSION 级别写入应成功
        write_result = await manager.write("key1", "value1", consistency=ConsistencyLevel.SESSION)
        assert write_result.success is True

        # 读取也应成功
        read_result = await manager.read("key1", consistency=ConsistencyLevel.SESSION)
        assert read_result.success is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_session_causal_workaround(self, manager):
        """会话中使用 CAUSAL 一致性作为 workaround"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        session_id = manager.create_session("user1")

        # 使用 CAUSAL 级别绕过 MONOTONIC_WRITES bug
        write_result = await manager.write(
            "session_key", "session_value",
            consistency=ConsistencyLevel.CAUSAL,
            session_id=session_id,
        )
        assert write_result.success is True

        read_result = await manager.read(
            "session_key",
            consistency=ConsistencyLevel.CAUSAL,
            session_id=session_id,
        )
        assert read_result.success is True
        assert read_result.value == "session_value"
        await manager.stop()


# ============================================================================
# 不同一致性级别测试
# ============================================================================

class TestConsistencyLevels:
    """不同一致性级别测试"""

    @pytest.fixture
    def manager(self):
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        return cm

    @pytest.mark.asyncio
    async def test_linearizable_write_as_leader(self, manager):
        """线性一致性写入 - 作为 Leader"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        result = await manager.write(
            "key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE
        )
        assert result.success is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_linearizable_read_as_leader(self, manager):
        """[BUG-CM-003] 线性一致性读取 - 存储后端不一致
        _write_linearizable 使用 self._storage.write() 写入 PartitionTolerantStorage，
        但 _read_linearizable 使用 self._storage.read() 读取。
        而 CAUSAL 写入使用 self._local_store，LINEARIZABLE 读取使用 self._storage，
        两个不同的存储后端导致 CAUSAL 写入的数据无法通过 LINEARIZABLE 读取到。
        """
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        # 使用 LINEARIZABLE 写入到 _storage
        await manager.write("key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE)
        result = await manager.read("key1", consistency=ConsistencyLevel.LINEARIZABLE)
        assert result.success is True
        assert result.value == "value1"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_cross_storage_backend_bug(self, manager):
        """[BUG-CM-003] 跨存储后端读写 - 已修复
        CAUSAL 写入现在同时写入 _local_store 和 _storage，
        LINEARIZABLE 读取从 _storage 可以找到数据。
        """
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        # CAUSAL 写入到 _local_store 和 _storage
        await manager.write("key1", "value1", consistency=ConsistencyLevel.CAUSAL)

        # LINEARIZABLE 从 _storage 读取 - 修复后应能找到
        result = await manager.read("key1", consistency=ConsistencyLevel.LINEARIZABLE)
        assert result.success is True
        assert result.value == "value1"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_linearizable_write_no_leader(self, manager):
        """线性一致性写入 - 无 Leader 时失败"""
        await manager.start()
        # 不设置 leader

        result = await manager.write(
            "key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE
        )
        assert result.success is False
        assert "No leader" in result.error
        await manager.stop()

    @pytest.mark.asyncio
    async def test_linearizable_read_no_leader(self, manager):
        """线性一致性读取 - 无 Leader 时失败"""
        await manager.start()
        # 不设置 leader

        result = await manager.read(
            "key1", consistency=ConsistencyLevel.LINEARIZABLE
        )
        assert result.success is False
        assert "No leader" in result.error
        await manager.stop()

    @pytest.mark.asyncio
    async def test_causal_consistency(self, manager):
        """因果一致性读写"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        result = await manager.write(
            "key1", "value1", consistency=ConsistencyLevel.CAUSAL
        )
        assert result.success is True
        assert result.vector_clock is not None

        read_result = await manager.read(
            "key1", consistency=ConsistencyLevel.CAUSAL
        )
        assert read_result.success is True
        assert read_result.value == "value1"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_eventual_consistency_write(self, manager):
        """[BUG-CM-001] 最终一致性写入 - 已修复
        MONOTONIC_WRITES 枚举已添加，EVENTUAL 级别写入正常工作。
        """
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        result = await manager.write(
            "key1", "value1", consistency=ConsistencyLevel.EVENTUAL
        )
        # 修复后 EVENTUAL 写入应成功
        assert result.success is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_read_your_writes_consistency(self, manager):
        """[BUG-CM-001] 读己之写一致性 - 已修复
        MONOTONIC_WRITES 枚举已添加，READ_YOUR_WRITES 级别操作正常工作。
        """
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        session_id = manager.create_session("user1")
        result = await manager.write(
            "key1", "value1",
            consistency=ConsistencyLevel.READ_YOUR_WRITES,
            session_id=session_id,
        )
        # 修复后 READ_YOUR_WRITES 写入应成功
        assert result.success is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_monotonic_reads_consistency(self, manager):
        """单调读一致性"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        await manager.write("key1", "value1", consistency=ConsistencyLevel.CAUSAL)

        session_id = manager.create_session("user1")
        result = await manager.read(
            "key1",
            consistency=ConsistencyLevel.MONOTONIC_READS,
            session_id=session_id,
        )
        assert result.success is True
        await manager.stop()


# ============================================================================
# Leader 切换和分区测试
# ============================================================================

class TestLeaderAndPartition:
    """Leader 切换和分区状态测试"""

    @pytest.fixture
    def manager(self):
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        return cm

    @pytest.mark.asyncio
    async def test_set_leader(self, manager):
        """设置 Leader"""
        await manager.start()
        manager.set_leader("node2", is_leader=False)
        assert manager._leader_id == "node2"
        assert manager._is_leader is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_leader_switch_write(self, manager):
        """Leader 切换后写入"""
        await manager.start()

        # node1 是 leader
        manager.set_leader("node1", is_leader=True)
        result1 = await manager.write(
            "key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE
        )
        assert result1.success is True

        # 切换到 node2 作为 leader
        manager.set_leader("node2", is_leader=False)
        result2 = await manager.write(
            "key2", "value2", consistency=ConsistencyLevel.LINEARIZABLE
        )
        # node1 不是 leader，转发到 node2 leader
        assert result2.success is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_partition_status(self, manager):
        """分区状态检查"""
        await manager.start()
        status = manager.get_partition_status()
        assert "is_partitioned" in status
        assert "healthy_nodes" in status
        assert "total_nodes" in status
        assert "quorum_status" in status
        assert status["total_nodes"] == 3  # node1 + 2 peers
        await manager.stop()

    @pytest.mark.asyncio
    async def test_get_stats(self, manager):
        """统计信息收集"""
        await manager.start()
        manager.set_leader("node1", is_leader=True)

        # 执行一些操作
        await manager.write("k1", "v1", consistency=ConsistencyLevel.LINEARIZABLE)
        await manager.write("k2", "v2", consistency=ConsistencyLevel.CAUSAL)
        await manager.read("k1", consistency=ConsistencyLevel.LINEARIZABLE)
        await manager.read("k2", consistency=ConsistencyLevel.CAUSAL)

        stats = manager.get_stats()
        assert stats["reads"] == 2
        assert stats["writes"] == 2
        assert stats["failures"] == 0
        assert stats["linearizable_ops"] == 2  # 1 write + 1 read
        assert stats["causal_ops"] == 2  # 1 write + 1 read
        await manager.stop()

    @pytest.mark.asyncio
    async def test_stats_failure_count(self, manager):
        """[BUG-CM-002] 失败计数 - 无 Leader 时操作失败
        _write_linearizable 返回 OperationResult(success=False) 但不抛异常，
        因此 write() 的 except 块不会捕获，failures 计数器不递增。
        """
        await manager.start()
        # 不设置 leader

        result = await manager.write(
            "key1", "value1", consistency=ConsistencyLevel.LINEARIZABLE
        )
        assert result.success is False

        stats = manager.get_stats()
        # BUG-CM-002 已修复: failures 现在正确计数为 1
        assert stats["failures"] == 1
        await manager.stop()


# ============================================================================
# 并发写入测试
# ============================================================================

class TestConcurrentOperations:
    """并发操作测试"""

    @pytest.mark.asyncio
    async def test_concurrent_writes(self):
        """并发写入多个 key"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        await cm.start()
        cm.set_leader("node1", is_leader=True)

        async def write_key(key_num):
            return await cm.write(
                f"key_{key_num}", f"value_{key_num}",
                consistency=ConsistencyLevel.CAUSAL
            )

        # 并发写入 10 个 key
        tasks = [write_key(i) for i in range(10)]
        results = await asyncio.gather(*tasks)

        for i, result in enumerate(results):
            assert result.success is True, f"Write key_{i} failed"

        # 验证所有 key 都可读
        for i in range(10):
            result = await cm.read(f"key_{i}", consistency=ConsistencyLevel.CAUSAL)
            assert result.success is True
            assert result.value == f"value_{i}"

        await cm.stop()

    @pytest.mark.asyncio
    async def test_concurrent_read_write_same_key(self):
        """并发读写同一个 key"""
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        await cm.start()
        cm.set_leader("node1", is_leader=True)

        # 先写入
        await cm.write("shared_key", "initial", consistency=ConsistencyLevel.CAUSAL)

        # 并发读
        async def read_key():
            return await cm.read("shared_key", consistency=ConsistencyLevel.CAUSAL)

        tasks = [read_key() for _ in range(5)]
        results = await asyncio.gather(*tasks)

        for result in results:
            assert result.success is True
            assert result.value == "initial"

        await cm.stop()


# ============================================================================
# ConsistencyVerifier 测试
# ============================================================================

class TestConsistencyVerifier:
    """一致性验证器测试"""

    @pytest.fixture
    def verifier(self):
        cm = ConsistencyManager(
            node_id="node1",
            peers=["node2", "node3"],
        )
        return ConsistencyVerifier(cm)

    @pytest.mark.asyncio
    async def test_verify_linearizability_empty(self, verifier):
        """线性一致性验证 - 空操作列表"""
        result = await verifier.verify_linearizability([])
        assert result is True

    @pytest.mark.asyncio
    async def test_verify_linearizability_with_ops(self, verifier):
        """线性一致性验证 - 有操作"""
        operations = [
            {"type": "write", "key": "k1", "value": "v1", "timestamp": 1},
            {"type": "read", "key": "k1", "value": "v1", "timestamp": 2},
        ]
        result = await verifier.verify_linearizability(operations)
        # 简化实现始终返回 True
        assert result is True

    @pytest.mark.asyncio
    async def test_verify_causal_consistency_empty(self, verifier):
        """因果一致性验证 - 空操作列表"""
        result = await verifier.verify_causal_consistency([])
        assert result is True

    @pytest.mark.asyncio
    async def test_verify_causal_consistency_no_dependencies(self, verifier):
        """因果一致性验证 - 无依赖关系的操作"""
        operations = [
            {
                "operation_id": "op1",
                "type": "write",
                "key": "k1",
                "value": "v1",
                "vector_clock": {"node1": 1},
            },
            {
                "operation_id": "op2",
                "type": "write",
                "key": "k2",
                "value": "v2",
                "vector_clock": {"node1": 1},
            },
        ]
        result = await verifier.verify_causal_consistency(operations)
        assert result is True

    @pytest.mark.asyncio
    async def test_verify_causal_consistency_with_dependency(self, verifier):
        """因果一致性验证 - 有依赖关系的操作"""
        operations = [
            {
                "operation_id": "op1",
                "type": "write",
                "key": "k1",
                "value": "v1",
                "vector_clock": {"node1": 1},
            },
            {
                "operation_id": "op2",
                "type": "read",
                "key": "k1",
                "value": "v1",
                "vector_clock": {"node1": 2},
                "depends_on": "op1",
            },
        ]
        result = await verifier.verify_causal_consistency(operations)
        assert result is True

    @pytest.mark.asyncio
    async def test_verify_session_consistency(self, verifier):
        """会话一致性验证"""
        operations = [
            {"type": "write", "key": "k1", "value": "v1", "timestamp": 1},
            {"type": "read", "key": "k1", "value": "v1", "timestamp": 2},
            {"type": "write", "key": "k1", "value": "v2", "timestamp": 3},
            {"type": "read", "key": "k1", "value": "v2", "timestamp": 4},
        ]
        result = await verifier.verify_session_consistency("session1", operations)
        assert result["read_your_writes"] is True
        assert result["monotonic_reads"] is True
        assert result["monotonic_writes"] is True

    @pytest.mark.asyncio
    async def test_verify_session_monotonic_reads_violation(self, verifier):
        """会话一致性验证 - 单调读违反"""
        operations = [
            {"type": "read", "key": "k1", "value": "v2", "timestamp": 2},
            {"type": "read", "key": "k1", "value": "v1", "timestamp": 1},
        ]
        result = await verifier.verify_session_consistency("session1", operations)
        assert result["monotonic_reads"] is False

    @pytest.mark.asyncio
    async def test_verify_session_read_your_writes_violation(self, verifier):
        """会话一致性验证 - 读己之写违反"""
        operations = [
            {"type": "write", "key": "k1", "value": "v1", "timestamp": 1},
            {"type": "read", "key": "k1", "value": "v_old", "timestamp": 2},
        ]
        result = await verifier.verify_session_consistency("session1", operations)
        assert result["read_your_writes"] is False

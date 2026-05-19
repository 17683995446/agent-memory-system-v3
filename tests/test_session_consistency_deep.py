"""
深度测试: session_consistency.py 模块

覆盖 P1 盲区模块 - 会话一致性系统
测试 SessionConsistencyLevel, SessionState, SessionManager, ConsistentSession
"""

import sys
import time
import threading
import pytest
import asyncio

sys.path.insert(0, '/workspace/agent_memory_system_v2')

from memory_system.session_consistency import (
    SessionConsistencyLevel,
    SessionState,
    SessionManager,
    ConsistentSession,
)
from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp


# ============================================================
# 1. 会话创建
# ============================================================
class TestSessionCreation:

    def test_create_basic_session(self):
        """基本会话创建 - 验证所有字段初始化"""
        manager = SessionManager()
        session = manager.create_session("user_001")

        assert session is not None
        assert session.session_id is not None
        assert len(session.session_id) == 32
        assert session.user_id == "user_001"
        assert session.consistency_level == SessionConsistencyLevel.SESSION
        assert isinstance(session.created_at, float)
        assert isinstance(session.last_activity, float)
        assert len(session.write_set) == 0
        assert len(session.read_set) == 0
        assert len(session.write_timestamps) == 0
        assert len(session.read_timestamps) == 0
        assert session.sticky_node is None
        assert len(session.operation_history) == 0

    def test_create_session_with_custom_consistency_level(self):
        """使用自定义一致性级别创建会话"""
        manager = SessionManager()
        for level in SessionConsistencyLevel:
            session = manager.create_session("user_001", consistency_level=level)
            assert session.consistency_level == level

    def test_create_session_with_sticky_node(self):
        """创建带有粘性节点的会话"""
        manager = SessionManager()
        session = manager.create_session("user_001", sticky_node="node_A")
        assert session.sticky_node == "node_A"

    def test_create_multiple_sessions_same_user(self):
        """同一用户创建多个会话"""
        manager = SessionManager()
        sessions = [manager.create_session("user_001") for _ in range(5)]
        session_ids = {s.session_id for s in sessions}
        assert len(session_ids) == 5, "每个会话应有唯一ID"


# ============================================================
# 2. 会话获取
# ============================================================
class TestSessionRetrieval:

    def test_get_existing_session(self):
        """获取已存在的会话"""
        manager = SessionManager()
        created = manager.create_session("user_001")
        retrieved = manager.get_session(created.session_id)

        assert retrieved is not None
        assert retrieved.session_id == created.session_id
        assert retrieved.user_id == created.user_id

    def test_get_nonexistent_session(self):
        """获取不存在的会话返回 None"""
        manager = SessionManager()
        result = manager.get_session("nonexistent_id")
        assert result is None

    def test_get_expired_session_returns_none(self):
        """获取过期会话返回 None"""
        manager = SessionManager(default_timeout=0.01)
        session = manager.create_session("user_001")
        time.sleep(0.02)
        result = manager.get_session(session.session_id)
        assert result is None


# ============================================================
# 3. 会话关闭
# ============================================================
class TestSessionClose:

    def test_close_existing_session(self):
        """关闭已存在的会话"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        result = manager.close_session(session.session_id)
        assert result is True

    def test_close_nonexistent_session(self):
        """关闭不存在的会话返回 False"""
        manager = SessionManager()
        result = manager.close_session("nonexistent_id")
        assert result is False

    def test_close_session_removes_from_user_sessions(self):
        """关闭会话后从用户会话列表中移除"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        manager.close_session(session.session_id)
        user_sessions = manager.get_user_sessions("user_001")
        assert len(user_sessions) == 0

    def test_double_close_session(self):
        """重复关闭会话第二次返回 False"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        assert manager.close_session(session.session_id) is True
        assert manager.close_session(session.session_id) is False


# ============================================================
# 4. 会话过期检测
# ============================================================
class TestSessionExpiration:

    def test_session_not_expired_initially(self):
        """新创建的会话不应过期"""
        state = SessionState(session_id="test", user_id="user_001")
        assert state.is_expired(timeout_seconds=3600) is False

    def test_session_expired_after_timeout(self):
        """超时后会话应标记为过期"""
        state = SessionState(session_id="test", user_id="user_001")
        state.last_activity = time.time() - 3700
        assert state.is_expired(timeout_seconds=3600) is True

    def test_session_not_expired_within_timeout(self):
        """在超时窗口内会话不应过期"""
        state = SessionState(session_id="test", user_id="user_001")
        state.last_activity = time.time() - 100
        assert state.is_expired(timeout_seconds=3600) is False

    def test_touch_updates_last_activity(self):
        """touch() 应更新 last_activity"""
        state = SessionState(session_id="test", user_id="user_001")
        old_activity = state.last_activity
        time.sleep(0.01)
        state.touch()
        assert state.last_activity > old_activity


# ============================================================
# 5. 读己之写一致性
# ============================================================
class TestReadYourWrites:

    def test_check_ryw_for_unwritten_key(self):
        """未写入的键不应有时间戳要求"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        ok, min_ts = manager.check_read_your_writes(session.session_id, "key_1")
        assert ok is True
        assert min_ts is None

    def test_check_ryw_for_written_key(self):
        """已写入的键应返回写入时间戳"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", {})
        ok, min_ts = manager.check_read_your_writes(session.session_id, "key_1")
        assert ok is True
        assert min_ts is not None
        assert isinstance(min_ts, HLCTimestamp)

    def test_check_ryw_nonexistent_session(self):
        """不存在的会话应返回 False"""
        manager = SessionManager()
        ok, min_ts = manager.check_read_your_writes("nonexistent", "key_1")
        assert ok is False
        assert min_ts is None

    def test_record_write_updates_session_state(self):
        """记录写操作应更新会话状态"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        vc = {"node_A": 1, "node_B": 2}
        ts = manager.record_write(session.session_id, "key_1", "value_1", "node_A", vc)

        assert ts is not None
        assert "key_1" in session.write_set
        assert "key_1" in session.write_timestamps
        assert "key_1" in session.write_vector_clocks
        assert session.write_vector_clocks["key_1"] == vc
        assert len(session.operation_history) == 1
        assert session.operation_history[0]["type"] == "write"


# ============================================================
# 6. 单调读一致性
# ============================================================
class TestMonotonicReads:

    def test_check_monotonic_reads_for_unread_key(self):
        """未读取的键不应有时间戳要求"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        ok, min_ts = manager.check_monotonic_reads(session.session_id, "key_1")
        assert ok is True
        assert min_ts is None

    def test_check_monotonic_reads_for_read_key(self):
        """已读取的键应返回读取时间戳"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        manager.record_read(session.session_id, "key_1", "value_1")
        ok, min_ts = manager.check_monotonic_reads(session.session_id, "key_1")
        assert ok is True
        assert min_ts is not None
        assert isinstance(min_ts, HLCTimestamp)

    def test_monotonic_reads_timestamp_increases(self):
        """后续读取的时间戳应大于等于前一次"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        ts1 = manager.record_read(session.session_id, "key_1", "value_1")
        ts2 = manager.record_read(session.session_id, "key_1", "value_2")
        assert ts2 >= ts1

    def test_record_read_updates_session_state(self):
        """记录读操作应更新会话状态"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        manager.record_read(session.session_id, "key_1", "value_1")

        assert "key_1" in session.read_set
        assert "key_1" in session.read_timestamps
        assert len(session.operation_history) == 1
        assert session.operation_history[0]["type"] == "read"


# ============================================================
# 7. 单调写一致性
# ============================================================
class TestMonotonicWrites:

    def test_check_monotonic_writes_for_unwritten_key(self):
        """未写入的键不应有向量时钟要求"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        ok, min_vc = manager.check_monotonic_writes(session.session_id, "key_1")
        assert ok is True
        assert min_vc is None

    def test_check_monotonic_writes_for_written_key(self):
        """已写入的键应返回向量时钟"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        vc = {"node_A": 1, "node_B": 2}
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", vc)
        ok, min_vc = manager.check_monotonic_writes(session.session_id, "key_1")
        assert ok is True
        assert min_vc is not None
        assert min_vc == vc

    def test_monotonic_writes_nonexistent_session(self):
        """不存在的会话应返回 False"""
        manager = SessionManager()
        ok, min_vc = manager.check_monotonic_writes("nonexistent", "key_1")
        assert ok is False
        assert min_vc is None


# ============================================================
# 8. 粘性路由
# ============================================================
class TestStickyRouting:

    def test_get_sticky_node_default_none(self):
        """默认粘性节点应为 None"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        assert manager.get_sticky_node(session.session_id) is None

    def test_set_and_get_sticky_node(self):
        """设置并获取粘性节点"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        assert manager.set_sticky_node(session.session_id, "node_A") is True
        assert manager.get_sticky_node(session.session_id) == "node_A"

    def test_set_sticky_node_nonexistent_session(self):
        """对不存在的会话设置粘性节点返回 False"""
        manager = SessionManager()
        assert manager.set_sticky_node("nonexistent", "node_A") is False

    def test_get_sticky_node_nonexistent_session(self):
        """对不存在的会话获取粘性节点返回 None"""
        manager = SessionManager()
        assert manager.get_sticky_node("nonexistent") is None

    def test_update_sticky_node(self):
        """更新粘性节点"""
        manager = SessionManager()
        session = manager.create_session("user_001", sticky_node="node_A")
        manager.set_sticky_node(session.session_id, "node_B")
        assert manager.get_sticky_node(session.session_id) == "node_B"


# ============================================================
# 9. 一致性要求获取
# ============================================================
class TestConsistencyRequirements:

    def test_get_requirements_nonexistent_session(self):
        """不存在的会话返回 valid=False"""
        manager = SessionManager()
        reqs = manager.get_consistency_requirements("nonexistent", "read", "key_1")
        assert reqs["valid"] is False

    def test_get_requirements_session_level_read(self):
        """SESSION 级别读操作应包含读己之写和单调读要求"""
        manager = SessionManager()
        session = manager.create_session(
            "user_001",
            consistency_level=SessionConsistencyLevel.SESSION
        )
        # 先写入再读取
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", {})
        manager.record_read(session.session_id, "key_1", "value_1")

        reqs = manager.get_consistency_requirements(session.session_id, "read", "key_1")
        assert reqs["valid"] is True
        assert reqs["consistency_level"] == "session"
        req_types = {r["type"] for r in reqs["requirements"]}
        assert "read_your_writes" in req_types
        assert "monotonic_reads" in req_types

    def test_get_requirements_monotonic_writes(self):
        """SESSION 级别写操作应包含单调写要求"""
        manager = SessionManager()
        session = manager.create_session(
            "user_001",
            consistency_level=SessionConsistencyLevel.SESSION
        )
        # 使用非空向量时钟，否则 check_monotonic_writes 返回的空 dict
        # 会被 `if min_vc:` 判定为 falsy 而跳过
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", {"node_A": 1})

        reqs = manager.get_consistency_requirements(session.session_id, "write", "key_1")
        assert reqs["valid"] is True
        req_types = {r["type"] for r in reqs["requirements"]}
        assert "monotonic_writes" in req_types

    def test_get_requirements_monotonic_writes_empty_vc_bug(self):
        """
        BUG发现: 空向量时钟 {} 被 `if min_vc:` 判定为 falsy，
        导致单调写要求不会被添加到 requirements 中。
        即使 session 中确实写过该键，空 VC 也会被忽略。
        """
        manager = SessionManager()
        session = manager.create_session(
            "user_001",
            consistency_level=SessionConsistencyLevel.SESSION
        )
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", {})

        reqs = manager.get_consistency_requirements(session.session_id, "write", "key_1")
        req_types = {r["type"] for r in reqs["requirements"]}
        # BUG 已修复: 空 dict {} 不再被误判为 falsy，monotonic_writes 正确出现
        assert "monotonic_writes" in req_types

    def test_get_requirements_monotonic_writes_level_only(self):
        """MONOTONIC_WRITES 级别写操作应包含单调写要求"""
        manager = SessionManager()
        session = manager.create_session(
            "user_001",
            consistency_level=SessionConsistencyLevel.MONOTONIC_WRITES
        )
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", {"node_A": 1})

        reqs = manager.get_consistency_requirements(session.session_id, "write", "key_1")
        assert reqs["valid"] is True
        req_types = {r["type"] for r in reqs["requirements"]}
        assert "monotonic_writes" in req_types

    def test_get_requirements_read_your_writes_only(self):
        """READ_YOUR_WRITES 级别不应包含单调读要求"""
        manager = SessionManager()
        session = manager.create_session(
            "user_001",
            consistency_level=SessionConsistencyLevel.READ_YOUR_WRITES
        )
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", {})
        manager.record_read(session.session_id, "key_1", "value_1")

        reqs = manager.get_consistency_requirements(session.session_id, "read", "key_1")
        req_types = {r["type"] for r in reqs["requirements"]}
        assert "read_your_writes" in req_types
        assert "monotonic_reads" not in req_types


# ============================================================
# 10. 过期会话清理
# ============================================================
class TestExpiredSessionCleanup:

    def test_cleanup_expired_sessions(self):
        """清理过期会话应返回正确数量"""
        manager = SessionManager(default_timeout=0.01)
        s1 = manager.create_session("user_001")
        s2 = manager.create_session("user_002")
        time.sleep(0.02)

        # 创建一个新会话（未过期）
        s3 = manager.create_session("user_003")

        cleaned = manager.cleanup_expired_sessions()
        assert cleaned == 2
        assert manager.get_session(s1.session_id) is None
        assert manager.get_session(s2.session_id) is None
        assert manager.get_session(s3.session_id) is not None

    def test_cleanup_no_expired_sessions(self):
        """没有过期会话时返回 0"""
        manager = SessionManager(default_timeout=3600)
        manager.create_session("user_001")
        manager.create_session("user_002")
        cleaned = manager.cleanup_expired_sessions()
        assert cleaned == 0


# ============================================================
# 11. 用户会话查询
# ============================================================
class TestUserSessionQuery:

    def test_get_user_sessions_empty(self):
        """没有会话时返回空列表"""
        manager = SessionManager()
        sessions = manager.get_user_sessions("user_001")
        assert sessions == []

    def test_get_user_sessions_multiple(self):
        """获取用户的所有会话"""
        manager = SessionManager()
        s1 = manager.create_session("user_001")
        s2 = manager.create_session("user_001")
        manager.create_session("user_002")  # 不同用户

        sessions = manager.get_user_sessions("user_001")
        assert len(sessions) == 2
        session_ids = {s.session_id for s in sessions}
        assert s1.session_id in session_ids
        assert s2.session_id in session_ids

    def test_get_user_sessions_after_close(self):
        """关闭会话后不再出现在用户会话列表中"""
        manager = SessionManager()
        s1 = manager.create_session("user_001")
        s2 = manager.create_session("user_001")
        manager.close_session(s1.session_id)

        sessions = manager.get_user_sessions("user_001")
        assert len(sessions) == 1
        assert sessions[0].session_id == s2.session_id


# ============================================================
# 12. 统计信息
# ============================================================
class TestStatistics:

    def test_get_stats_initial(self):
        """初始统计信息"""
        manager = SessionManager()
        stats = manager.get_stats()
        assert stats["total_sessions"] == 0
        assert stats["total_users"] == 0
        assert stats["default_timeout"] == 3600

    def test_get_stats_after_creations(self):
        """创建会话后统计信息更新"""
        manager = SessionManager()
        manager.create_session("user_001")
        manager.create_session("user_001")
        manager.create_session("user_002")

        stats = manager.get_stats()
        assert stats["total_sessions"] == 3
        assert stats["total_users"] == 2

    def test_get_stats_after_close(self):
        """关闭会话后统计信息更新"""
        manager = SessionManager()
        s1 = manager.create_session("user_001")
        manager.create_session("user_002")
        manager.close_session(s1.session_id)

        stats = manager.get_stats()
        assert stats["total_sessions"] == 1


# ============================================================
# 13. 并发会话操作
# ============================================================
class TestConcurrentSessionOperations:

    def test_concurrent_session_creation(self):
        """并发创建会话不应产生冲突"""
        manager = SessionManager()
        results = []
        errors = []

        def create_session(user_id):
            try:
                session = manager.create_session(user_id)
                results.append(session.session_id)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=create_session, args=(f"user_{i}",))
            for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(set(results)) == 20, "所有会话ID应唯一"

    def test_concurrent_read_write_operations(self):
        """并发读写操作不应导致数据损坏"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        errors = []

        def do_writes():
            try:
                for i in range(50):
                    manager.record_write(
                        session.session_id, f"key_{i}", f"value_{i}",
                        "node_A", {"node_A": i}
                    )
            except Exception as e:
                errors.append(e)

        def do_reads():
            try:
                for i in range(50):
                    manager.record_read(session.session_id, f"key_{i}", f"value_{i}")
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=do_writes)
        t2 = threading.Thread(target=do_reads)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0
        assert len(session.write_set) == 50
        assert len(session.read_set) == 50


# ============================================================
# 14. ConsistentSession 读写
# ============================================================
class TestConsistentSession:

    def _make_mock_storage(self):
        """创建模拟存储后端"""
        class MockStorage:
            def __init__(self):
                self.data = {}
                self.write_calls = []
                self.read_calls = []

            async def write(self, key, value, timestamp=None, vector_clock=None):
                self.write_calls.append((key, value, timestamp, vector_clock))
                self.data[key] = value
                return True

            async def read(self, key, min_timestamp=None):
                self.read_calls.append((key, min_timestamp))
                if key in self.data:
                    return (self.data[key], min_timestamp)
                return None

        return MockStorage()

    @pytest.mark.asyncio
    async def test_consistent_session_write(self):
        """ConsistentSession 写入操作"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        storage = self._make_mock_storage()
        cs = ConsistentSession(manager, session.session_id, storage)

        success, ts = await cs.write("key_1", "value_1")
        assert success is True
        assert ts is not None
        assert isinstance(ts, HLCTimestamp)
        assert len(storage.write_calls) == 1
        assert storage.data["key_1"] == "value_1"

    @pytest.mark.asyncio
    async def test_consistent_session_read(self):
        """ConsistentSession 读取操作"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        storage = self._make_mock_storage()
        cs = ConsistentSession(manager, session.session_id, storage)

        # 先写入
        await cs.write("key_1", "value_1")
        # 再读取
        value, ts = await cs.read("key_1")
        assert value == "value_1"
        assert ts is not None

    @pytest.mark.asyncio
    async def test_consistent_session_read_nonexistent_key(self):
        """读取不存在的键返回 None"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        storage = self._make_mock_storage()
        cs = ConsistentSession(manager, session.session_id, storage)

        value, ts = await cs.read("nonexistent_key")
        assert value is None
        assert ts is None

    @pytest.mark.asyncio
    async def test_consistent_session_get_session_id(self):
        """获取会话ID"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        storage = self._make_mock_storage()
        cs = ConsistentSession(manager, session.session_id, storage)

        assert cs.get_session_id() == session.session_id

    @pytest.mark.asyncio
    async def test_consistent_session_close(self):
        """关闭一致性会话"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        storage = self._make_mock_storage()
        cs = ConsistentSession(manager, session.session_id, storage)

        cs.close()
        assert manager.get_session(session.session_id) is None

    @pytest.mark.asyncio
    async def test_consistent_session_write_records_in_manager(self):
        """写入操作应在 SessionManager 中记录"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        storage = self._make_mock_storage()
        cs = ConsistentSession(manager, session.session_id, storage)

        await cs.write("key_1", "value_1")
        refreshed = manager.get_session(session.session_id)
        assert "key_1" in refreshed.write_set


# ============================================================
# 15. 会话状态序列化
# ============================================================
class TestSessionStateSerialization:

    def test_to_dict_basic(self):
        """基本序列化"""
        state = SessionState(session_id="abc123", user_id="user_001")
        d = state.to_dict()

        assert d["session_id"] == "abc123"
        assert d["user_id"] == "user_001"
        assert isinstance(d["created_at"], float)
        assert isinstance(d["last_activity"], float)
        assert isinstance(d["write_set"], list)
        assert isinstance(d["read_set"], list)
        assert isinstance(d["write_timestamps"], dict)
        assert isinstance(d["read_timestamps"], dict)
        assert isinstance(d["write_vector_clocks"], dict)
        assert d["consistency_level"] == "session"
        assert d["operation_count"] == 0

    def test_to_dict_with_operations(self):
        """包含操作记录的序列化"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        manager.record_write(session.session_id, "key_1", "value_1", "node_A", {"node_A": 1})
        manager.record_read(session.session_id, "key_2", "value_2")

        d = session.to_dict()
        assert "key_1" in d["write_set"]
        assert "key_2" in d["read_set"]
        assert d["operation_count"] == 2
        assert d["write_timestamps"]["key_1"]["node_id"] == "session_manager"
        assert d["read_timestamps"]["key_2"]["node_id"] == "session_manager"

    def test_to_dict_with_sticky_node(self):
        """包含粘性节点的序列化"""
        state = SessionState(session_id="abc", user_id="user_001", sticky_node="node_X")
        d = state.to_dict()
        assert d["sticky_node"] == "node_X"

    def test_to_dict_write_timestamps_are_dicts(self):
        """写入时间戳应序列化为字典"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        manager.record_write(session.session_id, "key_1", "val", "node_A", {})

        d = session.to_dict()
        wt = d["write_timestamps"]["key_1"]
        assert isinstance(wt, dict)
        assert "physical" in wt
        assert "logical" in wt
        assert "node_id" in wt


# ============================================================
# 额外边界条件测试
# ============================================================
class TestEdgeCases:

    def test_record_write_nonexistent_session(self):
        """对不存在的会话记录写操作返回 None"""
        manager = SessionManager()
        ts = manager.record_write("nonexistent", "key_1", "value_1", "node_A", {})
        assert ts is None

    def test_record_read_nonexistent_session(self):
        """对不存在的会话记录读操作返回 None"""
        manager = SessionManager()
        ts = manager.record_read("nonexistent", "key_1", "value_1")
        assert ts is None

    def test_multiple_writes_same_key_updates_timestamp(self):
        """对同一键多次写入应更新时间戳"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        ts1 = manager.record_write(session.session_id, "key_1", "v1", "node_A", {})
        ts2 = manager.record_write(session.session_id, "key_1", "v2", "node_A", {})
        assert ts2 > ts1

    def test_session_consistency_level_enum_values(self):
        """验证枚举值"""
        assert SessionConsistencyLevel.READ_YOUR_WRITES.value == "read_your_writes"
        assert SessionConsistencyLevel.MONOTONIC_READS.value == "monotonic_reads"
        assert SessionConsistencyLevel.MONOTONIC_WRITES.value == "monotonic_writes"
        assert SessionConsistencyLevel.WRITES_FOLLOW_READS.value == "writes_follow_reads"
        assert SessionConsistencyLevel.SESSION.value == "session"

    def test_session_state_write_vector_clocks_copy(self):
        """record_write 应复制向量时钟，避免引用共享"""
        manager = SessionManager()
        session = manager.create_session("user_001")
        vc = {"node_A": 1}
        manager.record_write(session.session_id, "key_1", "val", "node_A", vc)
        vc["node_A"] = 999  # 修改原始字典
        assert session.write_vector_clocks["key_1"]["node_A"] == 1

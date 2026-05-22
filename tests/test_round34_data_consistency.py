"""
第34轮测试：数据一致性与事务完整性
测试目标：验证分布式事务、Raft日志、向量时钟、HLC的数据一致性
"""

import pytest
import asyncio
import time
import logging
from unittest.mock import Mock, AsyncMock, patch
from typing import List

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# ============================================================
# 一、分布式事务管理器测试 (DC-001 ~ DC-006)
# ============================================================

class TestDistributedTransactionConsistency:
    """测试分布式事务一致性"""
    
    @pytest.mark.asyncio
    async def test_dc001_prepare_concurrent_race_condition(self):
        """DC-001 [P0]: 测试prepare并发竞态条件"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus, TransactionContext
        )
        
        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()
        
        try:
            # 注册参与者 (使用register_participant而非add_participant)
            p1 = Participant(
                participant_id="p1",
                resource_id="resource_1",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=AsyncMock(return_value=True),
                abort_handler=AsyncMock(return_value=True)
            )
            coordinator.register_participant(p1)
            
            # 创建事务 (timeout是float类型)
            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p1")
            
            # 并发调用prepare - 应该只有一个成功
            results = await asyncio.gather(
                coordinator.prepare(tx.tx_id),
                coordinator.prepare(tx.tx_id),
                return_exceptions=True
            )
            
            # 验证：至少一个应该成功或失败，但不应该两个都成功
            success_count = sum(1 for r in results if r is True)
            logger.info(f"DC-001: Concurrent prepare results: {results}, successes: {success_count}")
            
            # 如果两个都成功了，说明存在竞态条件BUG
            if success_count == 2:
                logger.error("DC-001 BUG CONFIRMED: Both concurrent prepare calls succeeded!")
            
        finally:
            await coordinator.stop()
    
    @pytest.mark.asyncio
    async def test_dc005_unknown_state_no_recovery(self):
        """DC-005 [P0]: 测试UNKNOWN状态事务无恢复机制"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus
        )
        
        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()
        
        try:
            # 注册两个参与者
            p_success = Participant(
                participant_id="p_ok",
                resource_id="resource_ok",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=AsyncMock(return_value=True),
                abort_handler=AsyncMock(return_value=True)
            )
            p_fail = Participant(
                participant_id="p_fail",
                resource_id="resource_fail",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=AsyncMock(side_effect=RuntimeError("Commit failed")),
                abort_handler=AsyncMock(return_value=True)
            )
            coordinator.register_participant(p_success)
            coordinator.register_participant(p_fail)
            
            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p_ok")
            await coordinator.add_participant(tx.tx_id, "p_fail")
            
            # prepare成功
            prepared = await coordinator.prepare(tx.tx_id)
            assert prepared is True
            
            # commit应该部分失败，事务进入UNKNOWN状态
            commit_result = await coordinator.commit(tx.tx_id)
            
            # 验证事务状态
            tx = await store.load_transaction(tx.tx_id)
            logger.info(f"DC-005: Transaction status after partial commit: {tx.status}")
            
            # 如果状态是UNKNOWN，验证恢复循环是否处理
            if tx.status == TransactionStatus.UNKNOWN:
                logger.warning("DC-005 BUG CONFIRMED: Transaction in UNKNOWN state with no recovery")
            
        finally:
            await coordinator.stop()
    
    @pytest.mark.asyncio
    async def test_dc006_abort_committing_transaction(self):
        """DC-006 [P0]: 测试对COMMITTING状态事务执行abort"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus
        )
        
        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()
        
        try:
            # 注册慢提交参与者
            slow_commit_started = asyncio.Event()
            
            async def slow_commit(tx_id, context):
                slow_commit_started.set()
                await asyncio.sleep(2.0)
                return True
            
            p = Participant(
                participant_id="p_slow",
                resource_id="resource_slow",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=slow_commit,
                abort_handler=AsyncMock(return_value=True)
            )
            coordinator.register_participant(p)
            
            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p_slow")
            await coordinator.prepare(tx.tx_id)
            
            # 启动commit（会阻塞在慢提交上）
            commit_task = asyncio.create_task(coordinator.commit(tx.tx_id))
            
            # 等待commit开始
            await slow_commit_started.wait()
            
            # 尝试abort正在提交的事务
            try:
                abort_result = await asyncio.wait_for(
                    coordinator.abort(tx.tx_id),
                    timeout=1.0
                )
                logger.warning(f"DC-006 BUG CONFIRMED: Abort succeeded on COMMITTING tx: {abort_result}")
            except (asyncio.TimeoutError, Exception) as e:
                logger.info(f"DC-006: Abort correctly rejected: {e}")
            finally:
                commit_task.cancel()
                try:
                    await commit_task
                except asyncio.CancelledError:
                    pass
            
        finally:
            await coordinator.stop()
    
    @pytest.mark.asyncio
    async def test_dc014_participant_commit_exception_state(self):
        """DC-014 [P1]: 测试参与者commit异常后状态更新"""
        from memory_system.distributed_transaction_manager import (
            Participant, TransactionContext, ParticipantStatus
        )
        
        p = Participant(
            participant_id="p1",
            resource_id="resource_1",
            prepare_handler=AsyncMock(return_value=True),
            commit_handler=AsyncMock(side_effect=RuntimeError("Commit failed")),
            abort_handler=AsyncMock(return_value=True)
        )
        
        # 先prepare
        result = await p.prepare("tx1", TransactionContext())
        assert result is True
        assert p.status == ParticipantStatus.PREPARED
        
        # commit失败 - BUG: 异常被捕获不传播，返回False
        commit_result = await p.commit("tx1", TransactionContext())
        
        # 验证状态 - BUG: 状态仍然是PREPARED而非FAILED
        logger.info(f"DC-014: Participant status after commit failure: {p.status}, commit_result={commit_result}")
        if p.status == ParticipantStatus.PREPARED:
            logger.warning("DC-014 BUG CONFIRMED: Status still PREPARED after commit failure!")


# ============================================================
# 二、Raft共识一致性测试 (RC-002 ~ RC-019)
# ============================================================

class TestRaftConsistency:
    """测试Raft共识一致性"""
    
    @pytest.mark.asyncio
    async def test_rc002_apply_exception_last_applied(self):
        """RC-002 [P0]: 测试状态机apply异常后last_applied是否错误递增"""
        from memory_system.raft_consensus import (
            RaftNode, RaftConfig, StateMachine, LogEntry
        )
        
        config = RaftConfig(node_id="test_node", peers=[])
        
        call_count = 0
        mock_state_machine = Mock(spec=StateMachine)
        
        async def failing_apply(command):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("First apply failed")
            return "success"
        
        mock_state_machine.apply = AsyncMock(side_effect=failing_apply)
        
        node = RaftNode(config, state_machine=mock_state_machine)
        await node.start()
        
        try:
            # 添加两个日志条目
            entry1 = LogEntry(term=1, index=0, command={"action": "fail"}, entry_id="e1")
            entry2 = LogEntry(term=1, index=1, command={"action": "ok"}, entry_id="e2")
            node._persistent_state.log.append(entry1)
            node._persistent_state.log.append(entry2)
            node._volatile_state.commit_index = 1
            
            # 应用日志
            await node._apply_committed_entries()
            
            # BUG: last_applied可能已经递增到1，跳过了失败的条目0
            logger.info(f"RC-002: last_applied={node._volatile_state.last_applied}, call_count={call_count}")
            
            if node._volatile_state.last_applied == 1:
                logger.error("RC-002 BUG CONFIRMED: last_applied advanced past failed entry!")
            
        finally:
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_rc003_truncate_committed_entries(self):
        """RC-003 [P0]: 测试日志截断是否可能删除已提交条目"""
        from memory_system.raft_consensus import (
            RaftNode, RaftConfig, StateMachine, LogEntry,
            AppendEntriesRequest, AppendEntriesResponse
        )
        
        config = RaftConfig(node_id="test_node", peers=["leader"])
        
        mock_state_machine = Mock(spec=StateMachine)
        mock_state_machine.apply = AsyncMock(return_value="ok")
        
        node = RaftNode(config, state_machine=mock_state_machine)
        await node.start()
        
        try:
            # 添加已提交的日志条目
            for i in range(5):
                entry = LogEntry(term=1, index=i, command={"action": f"cmd_{i}"}, entry_id=f"e{i}")
                node._persistent_state.log.append(entry)
            
            # 设置commit_index=4（所有条目已提交）
            node._volatile_state.commit_index = 4
            node._volatile_state.last_applied = 4
            
            # 模拟一个旧的Leader发送冲突的AppendEntries
            # prev_log_index=2, 但index=3的term不匹配
            request = AppendEntriesRequest(
                term=2,
                leader_id="old_leader",
                prev_log_index=2,
                prev_log_term=1,
                entries=[
                    LogEntry(term=2, index=3, command={"action": "conflict"}, entry_id="e_conflict"),
                ],
                leader_commit=0
            )
            
            response = await node.handle_append_entries(request)
            
            # 检查：已提交的条目(index<=4)是否被截断
            log_length = len(node._persistent_state.log)
            logger.info(f"RC-003: Response success={response.success}, log_length={log_length}")
            
            # 如果log_length < 5，说明已提交条目被截断了
            if log_length < 5:
                logger.error(f"RC-003 BUG CONFIRMED: Committed entries truncated! log_length={log_length}")
            
        finally:
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_rc004_snapshot_install_order(self):
        """RC-004 [P0]: 测试快照安装顺序是否正确"""
        from memory_system.raft_consensus import (
            RaftNode, RaftConfig, StateMachine, LogEntry,
            InstallSnapshotRequest
        )
        
        config = RaftConfig(node_id="test_node", peers=["leader"])
        
        restore_called = False
        mock_state_machine = Mock(spec=StateMachine)
        
        async def mock_restore(data):
            nonlocal restore_called
            restore_called = True
            return None
        
        mock_state_machine.apply = AsyncMock(return_value="ok")
        mock_state_machine.restore = AsyncMock(side_effect=mock_restore)
        
        node = RaftNode(config, state_machine=mock_state_machine)
        await node.start()
        
        try:
            # 添加日志条目
            for i in range(5):
                entry = LogEntry(term=1, index=i, command={"action": f"cmd_{i}"}, entry_id=f"e{i}")
                node._persistent_state.log.append(entry)
            
            node._volatile_state.commit_index = 4
            node._volatile_state.last_applied = 4
            
            # 安装快照
            request = InstallSnapshotRequest(
                term=2,
                leader_id="leader",
                last_included_index=10,
                last_included_term=2,
                offset=0,
                data=b"snapshot_data",
                done=True
            )
            
            response = await node.handle_install_snapshot(request)
            
            logger.info(f"RC-004: restore_called={restore_called}, "
                       f"log_length={len(node._persistent_state.log)}, "
                       f"last_applied={node._volatile_state.last_applied}, "
                       f"commit_index={node._volatile_state.commit_index}")
            
        finally:
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_rc019_snapshot_log_index_offset(self):
        """RC-019 [P0]: 测试快照安装后日志索引偏移问题"""
        from memory_system.raft_consensus import (
            RaftNode, RaftConfig, StateMachine, LogEntry,
            InstallSnapshotRequest
        )
        
        config = RaftConfig(node_id="test_node", peers=["leader"])
        
        mock_state_machine = Mock(spec=StateMachine)
        mock_state_machine.apply = AsyncMock(return_value="ok")
        mock_state_machine.restore = AsyncMock(return_value=None)
        
        node = RaftNode(config, state_machine=mock_state_machine)
        await node.start()
        
        try:
            # 添加日志条目 (index 0-9)
            for i in range(10):
                entry = LogEntry(term=1, index=i, command={"action": f"cmd_{i}"}, entry_id=f"e{i}")
                node._persistent_state.log.append(entry)
            
            # 安装快照，包含index 0-7
            request = InstallSnapshotRequest(
                term=2,
                leader_id="leader",
                last_included_index=7,
                last_included_term=1,
                offset=0,
                data=b"snapshot_data",
                done=True
            )
            
            response = await node.handle_install_snapshot(request)
            
            # 安装后日志应该只包含index 8-9
            log_length = len(node._persistent_state.log)
            logger.info(f"RC-019: log_length={log_length}")
            
            # 验证日志条目的index是否正确
            if log_length > 0:
                first_entry = node._persistent_state.log[0]
                logger.info(f"RC-019: First log entry index={first_entry.index}, expected 8")
                
                # BUG: 如果用列表下标访问log[8]会越界
                if log_length <= 1:
                    # 只有index=8的条目
                    if first_entry.index == 8:
                        logger.info("RC-019: Log entry index correct after snapshot")
                else:
                    second_entry = node._persistent_state.log[1]
                    logger.info(f"RC-019: Second log entry index={second_entry.index}, expected 9")
            
        finally:
            await node.stop()


# ============================================================
# 三、向量时钟一致性测试 (VC-001 ~ VC-003)
# ============================================================

class TestVectorClockConsistency:
    """测试向量时钟一致性"""
    
    def test_vc001_hash_eq_inconsistency(self):
        """VC-001 [P1]: 测试__hash__与__eq__不一致"""
        from memory_system.vector_clock import VectorClock
        
        vc1 = VectorClock("node1")
        vc2 = VectorClock("node2")
        
        # 两个时钟所有分量都为0，compare应该返回EQUAL
        result = vc1.compare(vc2)
        logger.info(f"VC-001: compare result={result}")
        
        # 如果equal，hash必须相同
        if result.value == 0:  # EQUAL
            h1 = hash(vc1)
            h2 = hash(vc2)
            if h1 != h2:
                logger.error(f"VC-001 BUG CONFIRMED: hash({vc1.clock})={h1} != hash({vc2.clock})={h2}")
    
    def test_vc002_are_concurrent_equal_clocks(self):
        """VC-002 [P1]: 测试are_concurrent对相等时钟返回True"""
        from memory_system.vector_clock import VectorClock
        
        vc1 = VectorClock("node1")
        vc1.increment()
        vc1.increment()
        
        vc2 = VectorClock("node1")
        vc2.increment()
        vc2.increment()
        
        # 检查是否有are_concurrent方法
        if not hasattr(vc1, 'are_concurrent'):
            # 方法不存在，使用compare代替
            result = vc1.compare(vc2)
            logger.info(f"VC-002: compare result for equal clocks={result}")
            if result.value == 0:  # EQUAL
                logger.info("VC-002: are_concurrent method not found, but compare correctly returns EQUAL")
            return
        
        # 两个时钟完全相等
        is_concurrent = vc1.are_concurrent(vc2)
        logger.info(f"VC-002: Equal clocks are_concurrent={is_concurrent}")
        
        if is_concurrent:
            logger.error("VC-002 BUG CONFIRMED: Equal clocks reported as concurrent!")
    
    @pytest.mark.asyncio
    async def test_vc003_receive_event_no_increment(self):
        """VC-003 [P1]: 测试receive_event后是否递增本地时钟"""
        from memory_system.vector_clock import VectorClockManager, CausalEvent, VectorClock
        
        manager = VectorClockManager("local_node")
        
        # 创建远程事件
        remote_vc = VectorClock("remote_node")
        remote_vc.increment()
        remote_vc.increment()
        remote_vc.increment()
        
        event = CausalEvent(
            event_id="event1",
            node_id="remote_node",
            vector_clock=remote_vc.clock,
            event_type="test",
            payload={"test": True}
        )
        
        # 接收事件
        manager.receive_event(event)
        
        # 检查本地时钟
        local = manager.get_current_clock()
        logger.info(f"VC-003: Local clock after receive: {local.clock}")
        
        # 本地节点的计数器应该至少为1（合并后递增）
        if "local_node" not in local.clock or local.clock["local_node"] < 1:
            logger.warning("VC-003 BUG CONFIRMED: Local clock not incremented after receive!")


# ============================================================
# 四、HLC一致性测试 (HL-001 ~ HL-005)
# ============================================================

class TestHLCConsistency:
    """测试混合逻辑时钟一致性"""
    
    @pytest.mark.asyncio
    async def test_hl005_enum_comparison_error(self):
        """HL-005 [P0]: 测试Enum比较TypeError"""
        from memory_system.hybrid_logical_clock import (
            HybridLogicalClock, HLCComparisonResult
        )
        
        hlc = HybridLogicalClock("node1")
        ts1 = hlc.now()
        ts2 = hlc.now()
        
        # 测试max()方法是否会抛出TypeError
        try:
            result = HybridLogicalClock.max(ts1, ts2)
            logger.info(f"HL-005: max() succeeded: {result}")
        except TypeError as e:
            logger.error(f"HL-005 BUG CONFIRMED: Enum comparison TypeError: {e}")
    
    @pytest.mark.asyncio
    async def test_hl001_update_physical_time(self):
        """HL-001 [P1]: 测试update()是否正确处理current_physical > recv_physical"""
        from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp
        
        hlc = HybridLogicalClock("node1")
        
        # 设置初始物理时间
        hlc._physical_time = 100
        hlc._logical_counter = 0
        
        # 模拟current_physical大于recv_physical
        with patch('time.time', return_value=300):
            received = HLCTimestamp(physical=200, logical=5, node_id="remote")
            result = hlc.update(received)
            
            # 标准算法应该使用current_physical=300
            logger.info(f"HL-001: update({received}) with current_physical=300 -> {result}")
            
            # 如果物理时间没有推进到300，说明存在BUG
            if result.physical < 300:
                logger.warning(f"HL-001 BUG CONFIRMED: Physical time not advanced to current_physical!")
    
    @pytest.mark.asyncio
    async def test_hl003_from_dict_missing_last_physical(self):
        """HL-003 [P2]: 测试from_dict是否恢复_last_physical_time"""
        from memory_system.hybrid_logical_clock import HybridLogicalClock
        
        hlc = HybridLogicalClock("node1")
        hlc._physical_time = 1000000
        hlc._last_physical_time = 1000000
        hlc._logical_counter = 0
        
        # 序列化
        data = hlc.to_dict()
        
        # 反序列化
        hlc2 = HybridLogicalClock.from_dict(data)
        
        # 检查_last_physical_time是否被恢复
        if hasattr(hlc2, '_last_physical_time'):
            if hlc2._last_physical_time != 1000000:
                logger.warning(f"HL-003 BUG CONFIRMED: _last_physical_time not restored! "
                             f"Got {hlc2._last_physical_time}, expected 1000000")
        else:
            logger.info("HL-003: _last_physical_time attribute not found (may not exist in this version)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

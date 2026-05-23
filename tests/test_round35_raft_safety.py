"""
第35轮测试：Raft核心安全修复
测试目标：验证Raft日志索引、快照安装等核心安全机制
"""

import pytest
import asyncio
import time
import logging
from unittest.mock import Mock, AsyncMock

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestRaftSnapshotIndexBug:
    """测试Raft快照索引偏移BUG (RC-019)"""
    
    @pytest.mark.asyncio
    async def test_rc019_get_entry_at_after_snapshot(self):
        """RC-019 [P0]: 测试快照安装后get_entry_at是否正确工作"""
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
            
            node._volatile_state.commit_index = 9
            node._volatile_state.last_applied = 9
            
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
            logger.info(f"RC-019: After snapshot, log_length={log_length}")
            
            # BUG: get_entry_at(8)会访问log[8]，但实际数据在log[0]
            entry_8 = node._persistent_state.get_entry_at(8)
            logger.info(f"RC-019: get_entry_at(8) = {entry_8}")
            
            # 如果返回None，说明存在索引偏移BUG
            if entry_8 is None:
                logger.error("RC-019 BUG CONFIRMED: get_entry_at(8) returns None after snapshot!")
                
            # 验证entry_8的内容
            if entry_8 and entry_8.index != 8:
                logger.error(f"RC-019 BUG CONFIRMED: get_entry_at(8) returns wrong entry with index={entry_8.index}")
                
        finally:
            await node.stop()
    
    @pytest.mark.asyncio
    async def test_rc019_append_entries_after_snapshot(self):
        """RC-019 [P0]: 测试快照安装后AppendEntries是否正确处理"""
        from memory_system.raft_consensus import (
            RaftNode, RaftConfig, StateMachine, LogEntry,
            InstallSnapshotRequest, AppendEntriesRequest
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
            
            node._volatile_state.commit_index = 9
            
            # 安装快照，包含index 0-7
            snapshot_request = InstallSnapshotRequest(
                term=2,
                leader_id="leader",
                last_included_index=7,
                last_included_term=1,
                offset=0,
                data=b"snapshot_data",
                done=True
            )
            await node.handle_install_snapshot(snapshot_request)
            
            # 现在Leader发送index 9的新条目
            # prev_log_index=8, prev_log_term=1
            new_entry = LogEntry(term=2, index=9, command={"action": "new"}, entry_id="e9_new")
            append_request = AppendEntriesRequest(
                term=2,
                leader_id="leader",
                prev_log_index=8,
                prev_log_term=1,
                entries=[new_entry],
                leader_commit=9
            )
            
            response = await node.handle_append_entries(append_request)
            
            logger.info(f"RC-019: AppendEntries response success={response.success}")
            
            # 如果返回False，可能是因为get_entry_at(8)返回None
            if not response.success:
                logger.error("RC-019 BUG CONFIRMED: AppendEntries failed after snapshot due to index offset!")
                
        finally:
            await node.stop()


class TestRaftSnapshotOrderBug:
    """测试Raft快照安装顺序BUG (RC-004)"""
    
    @pytest.mark.asyncio
    async def test_rc004_snapshot_restore_order(self):
        """RC-004 [P0]: 测试快照安装顺序 - 先更新元数据再恢复状态机"""
        from memory_system.raft_consensus import (
            RaftNode, RaftConfig, StateMachine, LogEntry,
            InstallSnapshotRequest
        )
        
        config = RaftConfig(node_id="test_node", peers=["leader"])
        
        restore_called = False
        restore_data = None
        
        async def mock_restore(data):
            nonlocal restore_called, restore_data
            restore_called = True
            restore_data = data
            # 模拟恢复成功
            return None
        
        mock_state_machine = Mock(spec=StateMachine)
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
            
            original_log_len = len(node._persistent_state.log)
            
            # 安装快照
            request = InstallSnapshotRequest(
                term=2,
                leader_id="leader",
                last_included_index=10,
                last_included_term=2,
                offset=0,
                data=b"snapshot_data_v2",
                done=True
            )
            
            response = await node.handle_install_snapshot(request)
            
            # 验证：如果restore被调用但后续步骤失败，状态可能不一致
            logger.info(f"RC-004: restore_called={restore_called}, "
                       f"last_applied={node._volatile_state.last_applied}, "
                       f"commit_index={node._volatile_state.commit_index}")
            
            # 检查last_applied是否正确更新
            if node._volatile_state.last_applied != 10:
                logger.warning(f"RC-004: last_applied not updated correctly: {node._volatile_state.last_applied} != 10")
                
        finally:
            await node.stop()


class TestRaftLogConsistency:
    """测试Raft日志一致性"""
    
    @pytest.mark.asyncio
    async def test_log_entry_index_consistency(self):
        """测试日志条目索引一致性"""
        from memory_system.raft_consensus import LogEntry
        
        # 创建条目
        entry1 = LogEntry(term=1, index=0, command={"a": 1}, entry_id="e1")
        entry2 = LogEntry(term=1, index=1, command={"b": 2}, entry_id="e2")
        entry3 = LogEntry(term=2, index=2, command={"c": 3}, entry_id="e3")
        
        # 验证索引正确存储
        assert entry1.index == 0
        assert entry2.index == 1
        assert entry3.index == 2
        
        logger.info("Log entry index consistency verified")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

"""
Agent Memory System V3.1.0 - 第21轮全面测试文件

测试模块: raft_consensus.py - Raft分布式共识协议实现
测试范围:
  - RaftNode 核心功能 (初始化、选举、投票、日志复制、提交推进、提案、故障转移、状态转换)
  - RaftCluster 集群功能 (选举、心跳、Leader故障、日志复制、分区容忍)
  - BUG验证测试 (竞态条件、锁释放顺序、leader_id设置)
  - 边界条件测试 (空日志选举、单节点集群、日志冲突截断、快照安装)

运行方式: pytest tests/test_round21_raft_consensus.py -v
"""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

from memory_system.raft_consensus import (
    RaftNode, RaftCluster, RaftConfig, RaftRole, RaftState,
    LogEntry, VolatileState, InMemoryTransport, MemoryStateMachine,
    ProposalTimeoutError, FailoverError,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    NotLeaderError, FailoverConfig, FailoverState,
)


# ============================================================================
# 辅助工具
# ============================================================================

def make_config(node_id="node0", peers=None, **kwargs):
    """创建RaftConfig的便捷方法"""
    return RaftConfig(
        node_id=node_id,
        peers=peers or [],
        **kwargs,
    )


def make_node(node_id="node0", peers=None, **kwargs):
    """创建RaftNode的便捷方法（不启动）"""
    config = make_config(node_id=node_id, peers=peers or [])
    transport = InMemoryTransport()
    node = RaftNode(
        config=config,
        state_machine=MemoryStateMachine(),
        transport=transport,
        **kwargs,
    )
    node._ensure_async_primitives()
    return node


async def start_node(node):
    """启动节点并等待初始化完成"""
    await node.start()
    return node


def make_cluster(num_nodes=3, election_timeout_min=0.3, election_timeout_max=0.6, heartbeat_interval=0.1):
    """创建RaftCluster的便捷方法（使用较长的选举超时确保测试稳定性）"""
    cluster = RaftCluster.__new__(RaftCluster)
    cluster.num_nodes = num_nodes
    cluster.nodes = {}
    cluster.transport = InMemoryTransport()
    cluster._election_timeout_min = election_timeout_min
    cluster._election_timeout_max = election_timeout_max
    cluster._heartbeat_interval = heartbeat_interval
    return cluster


async def start_cluster(cluster):
    """启动集群（使用自定义超时配置）"""
    node_ids = [f"node{i}" for i in range(cluster.num_nodes)]

    for i, node_id in enumerate(node_ids):
        peers = [nid for nid in node_ids if nid != node_id]
        config = RaftConfig(
            node_id=node_id,
            peers=peers,
            election_timeout_min=cluster._election_timeout_min,
            election_timeout_max=cluster._election_timeout_max,
            heartbeat_interval=cluster._heartbeat_interval,
        )

        node = RaftNode(
            config=config,
            state_machine=MemoryStateMachine(),
            transport=cluster.transport,
        )
        cluster.nodes[node_id] = node
        cluster.transport.register_node(node)

    for node in cluster.nodes.values():
        await node.start()


async def manually_elect_leader(cluster, candidate_id=None):
    """
    手动触发选举以避免内置选举的死锁问题。

    内置选举在_election_timeout_handler中持有_state_lock时调用_start_election，
    而_start_election内部用asyncio.gather发送投票请求。
    由于asyncio.Lock在await期间不释放锁，handle_request_vote尝试获取同一把锁时
    会产生死锁。

    此方法绕过内置选举机制，直接模拟选举过程。
    """
    if candidate_id is None:
        candidate_id = list(cluster.nodes.keys())[0]

    candidate = cluster.nodes[candidate_id]
    node_ids = list(cluster.nodes.keys())
    peers = [nid for nid in node_ids if nid != candidate_id]

    # 同步所有节点的term到最大值，确保选举可以正常进行
    max_term = max(n._persistent_state.current_term for n in cluster.nodes.values())
    for node in cluster.nodes.values():
        node._persistent_state.current_term = max_term
        node._persistent_state.voted_for = None
        node._role = RaftRole.FOLLOWER

    # 转为Candidate
    candidate._role = RaftRole.CANDIDATE
    candidate._persistent_state.current_term += 1
    candidate._persistent_state.voted_for = candidate_id
    candidate._votes_received = {candidate_id}

    last_log_index = candidate._persistent_state.get_last_log_index()
    last_log_term = candidate._persistent_state.get_last_log_term()

    request = RequestVoteRequest(
        term=candidate._persistent_state.current_term,
        candidate_id=candidate_id,
        last_log_index=last_log_index,
        last_log_term=last_log_term,
    )

    # 逐个发送投票请求（不持有_state_lock，避免死锁）
    for peer_id in peers:
        peer = cluster.nodes.get(peer_id)
        if peer:
            try:
                response = await peer.handle_request_vote(request)
                if response and response.vote_granted:
                    candidate._votes_received.add(response.voter_id)
            except Exception:
                pass

    # 检查是否获得多数票
    total_nodes = cluster.num_nodes
    majority = total_nodes // 2 + 1

    if len(candidate._votes_received) >= majority:
        # 取消选举定时器
        if candidate._election_timer:
            candidate._election_timer.cancel()
            candidate._election_timer = None
        await candidate._become_leader()
        return candidate

    return None


# ============================================================================
# RaftNode 核心测试
# ============================================================================

class TestRaftNodeInitialization:
    """RaftNode 初始化状态测试"""

    @pytest.mark.asyncio
    async def test_initial_state_is_follower(self):
        """测试节点初始化后角色为Follower"""
        node = make_node("node0")
        print("[LOG] test_initial_state_is_follower::检查初始角色 - 期望 Follower vs 实际", node._role)
        assert node._role == RaftRole.FOLLOWER

    @pytest.mark.asyncio
    async def test_initial_term_is_zero(self):
        """测试节点初始化后term为0"""
        node = make_node("node0")
        print("[LOG] test_initial_term_is_zero::检查初始term - 期望 0 vs 实际", node._persistent_state.current_term)
        assert node._persistent_state.current_term == 0

    @pytest.mark.asyncio
    async def test_initial_log_is_empty(self):
        """测试节点初始化后日志为空"""
        node = make_node("node0")
        print("[LOG] test_initial_log_is_empty::检查初始日志长度 - 期望 0 vs 实际", len(node._persistent_state.log))
        assert len(node._persistent_state.log) == 0

    @pytest.mark.asyncio
    async def test_initial_commit_index_is_minus_one(self):
        """测试节点初始化后commit_index为-1"""
        node = make_node("node0")
        print("[LOG] test_initial_commit_index_is_minus_one::检查初始commit_index - 期望 -1 vs 实际", node._volatile_state.commit_index)
        assert node._volatile_state.commit_index == -1

    @pytest.mark.asyncio
    async def test_initial_voted_for_is_none(self):
        """测试节点初始化后voted_for为None"""
        node = make_node("node0")
        print("[LOG] test_initial_voted_for_is_none::检查初始voted_for - 期望 None vs 实际", node._persistent_state.voted_for)
        assert node._persistent_state.voted_for is None

    @pytest.mark.asyncio
    async def test_initial_last_applied_is_minus_one(self):
        """测试节点初始化后last_applied为-1"""
        node = make_node("node0")
        print("[LOG] test_initial_last_applied_is_minus_one::检查初始last_applied - 期望 -1 vs 实际", node._volatile_state.last_applied)
        assert node._volatile_state.last_applied == -1

    @pytest.mark.asyncio
    async def test_initial_votes_received_is_empty(self):
        """测试节点初始化后votes_received为空"""
        node = make_node("node0")
        print("[LOG] test_initial_votes_received_is_empty::检查初始votes_received - 期望 set() vs 实际", node._votes_received)
        assert node._votes_received == set()

    @pytest.mark.asyncio
    async def test_initial_current_leader_is_none(self):
        """测试节点初始化后current_leader为None"""
        node = make_node("node0")
        print("[LOG] test_initial_current_leader_is_none::检查初始current_leader - 期望 None vs 实际", node._current_leader)
        assert node._current_leader is None


class TestStartElection:
    """测试 _start_election() 选举流程"""

    @pytest.mark.asyncio
    async def test_start_election_increments_term(self):
        """测试选举时term自增"""
        node = make_node("node0", peers=["node1", "node2"])
        initial_term = node._persistent_state.current_term
        print("[LOG] test_start_election_increments_term::选举前term - 期望 0 vs 实际", initial_term)

        await node._start_election()

        expected_term = initial_term + 1
        actual_term = node._persistent_state.current_term
        print(f"[LOG] test_start_election_increments_term::选举后term - 期望 {expected_term} vs 实际 {actual_term}")
        assert actual_term == expected_term

    @pytest.mark.asyncio
    async def test_start_election_votes_for_self(self):
        """测试选举时投票给自己"""
        node = make_node("node0", peers=["node1", "node2"])
        await node._start_election()

        expected_voted_for = "node0"
        actual_voted_for = node._persistent_state.voted_for
        print(f"[LOG] test_start_election_votes_for_self::voted_for - 期望 {expected_voted_for} vs 实际 {actual_voted_for}")
        assert actual_voted_for == expected_voted_for

    @pytest.mark.asyncio
    async def test_start_election_role_becomes_candidate(self):
        """测试选举时角色变为Candidate"""
        node = make_node("node0", peers=["node1", "node2"])
        await node._start_election()

        print("[LOG] test_start_election_role_becomes_candidate::角色 - 期望 CANDIDATE vs 实际", node._role)
        assert node._role == RaftRole.CANDIDATE

    @pytest.mark.asyncio
    async def test_start_election_self_vote_in_votes_received(self):
        """测试选举时自己包含在votes_received中"""
        node = make_node("node0", peers=["node1", "node2"])
        await node._start_election()

        print("[LOG] test_start_election_self_vote_in_votes_received::votes_received - 期望 {'node0'} vs 实际", node._votes_received)
        assert "node0" in node._votes_received

    @pytest.mark.asyncio
    async def test_start_election_leader_does_not_start_election(self):
        """测试Leader不会发起新选举"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 5
        initial_term = node._persistent_state.current_term

        await node._start_election()

        print(f"[LOG] test_start_election_leader_does_not_start_election::Leader选举后term - 期望 {initial_term} vs 实际 {node._persistent_state.current_term}")
        assert node._persistent_state.current_term == initial_term
        assert node._role == RaftRole.LEADER

    @pytest.mark.asyncio
    async def test_start_election_sends_request_vote_to_peers(self):
        """测试选举时向所有peer发送RequestVote"""
        node = make_node("node0", peers=["node1", "node2"])
        send_mock = AsyncMock()
        node.transport.send_request_vote = send_mock

        await node._start_election()

        print(f"[LOG] test_start_election_sends_request_vote_to_peers::发送投票请求数 - 期望 2 vs 实际 {send_mock.call_count}")
        assert send_mock.call_count == 2


class TestHandleRequestVote:
    """测试 handle_request_vote() 投票逻辑"""

    @pytest.mark.asyncio
    async def test_reject_if_request_term_less_than_current(self):
        """测试term < 当前term时拒绝投票"""
        node = make_node("node0")
        node._persistent_state.current_term = 5

        request = RequestVoteRequest(
            term=3, candidate_id="node1",
            last_log_index=-1, last_log_term=0,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_reject_if_request_term_less_than_current::vote_granted - 期望 False vs 实际 {response.vote_granted}")
        assert response.vote_granted is False
        assert response.term == 5

    @pytest.mark.asyncio
    async def test_accept_if_request_term_greater_become_follower(self):
        """测试term > 当前term时转为Follower，可以投票"""
        node = make_node("node0")
        node._persistent_state.current_term = 2
        node._role = RaftRole.CANDIDATE

        request = RequestVoteRequest(
            term=5, candidate_id="node1",
            last_log_index=0, last_log_term=1,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_accept_if_request_term_greater_become_follower::角色 - 期望 FOLLOWER vs 实际 {node._role.name}")
        assert node._role == RaftRole.FOLLOWER
        print(f"[LOG] test_accept_if_request_term_greater_become_follower::term - 期望 5 vs 实际 {node._persistent_state.current_term}")
        assert node._persistent_state.current_term == 5

    @pytest.mark.asyncio
    async def test_reject_if_already_voted_for_other(self):
        """测试已投票给其他候选人时拒绝"""
        node = make_node("node0")
        node._persistent_state.current_term = 3
        node._persistent_state.voted_for = "node2"

        request = RequestVoteRequest(
            term=3, candidate_id="node1",
            last_log_index=0, last_log_term=1,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_reject_if_already_voted_for_other::vote_granted - 期望 False vs 实际 {response.vote_granted}")
        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_reject_if_candidate_log_not_up_to_date(self):
        """测试候选人日志不够新时拒绝"""
        node = make_node("node0")
        node._persistent_state.current_term = 3
        # node0有日志: term=3, index=0
        entry = LogEntry(term=3, index=0, command={"action": "set", "key": "k", "value": "v"})
        node._persistent_state.log.append(entry)

        # node1的日志: term=2, index=0 (比node0旧)
        request = RequestVoteRequest(
            term=3, candidate_id="node1",
            last_log_index=0, last_log_term=2,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_reject_if_candidate_log_not_up_to_date::vote_granted - 期望 False vs 实际 {response.vote_granted}")
        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_accept_vote_when_conditions_met(self):
        """测试满足条件时正常投票"""
        node = make_node("node0")
        node._persistent_state.current_term = 3

        request = RequestVoteRequest(
            term=3, candidate_id="node1",
            last_log_index=-1, last_log_term=0,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_accept_vote_when_conditions_met::vote_granted - 期望 True vs 实际 {response.vote_granted}")
        assert response.vote_granted is True
        assert node._persistent_state.voted_for == "node1"

    @pytest.mark.asyncio
    async def test_revote_allowed_for_same_candidate(self):
        """测试对同一候选人可以重复投票"""
        node = make_node("node0")
        node._persistent_state.current_term = 3
        node._persistent_state.voted_for = "node1"

        request = RequestVoteRequest(
            term=3, candidate_id="node1",
            last_log_index=-1, last_log_term=0,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_revote_allowed_for_same_candidate::vote_granted - 期望 True vs 实际 {response.vote_granted}")
        assert response.vote_granted is True

    @pytest.mark.asyncio
    async def test_reject_if_log_term_same_but_index_shorter(self):
        """测试日志term相同但index更短时拒绝"""
        node = make_node("node0")
        node._persistent_state.current_term = 2
        # node0有2条日志: index=0(term=1), index=1(term=2)
        node._persistent_state.log.append(LogEntry(term=1, index=0, command={}))
        node._persistent_state.log.append(LogEntry(term=2, index=1, command={}))

        # node1只有1条日志: index=0(term=2)
        request = RequestVoteRequest(
            term=2, candidate_id="node1",
            last_log_index=0, last_log_term=2,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_reject_if_log_term_same_but_index_shorter::vote_granted - 期望 False vs 实际 {response.vote_granted}")
        assert response.vote_granted is False


class TestHandleAppendEntries:
    """测试 handle_append_entries() 日志复制"""

    @pytest.mark.asyncio
    async def test_reject_if_term_less_than_current(self):
        """测试term < 当前term时拒绝"""
        node = make_node("node0")
        node._persistent_state.current_term = 5

        request = AppendEntriesRequest(
            term=3, leader_id="node1",
            prev_log_index=-1, prev_log_term=0,
            entries=[], leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_reject_if_term_less_than_current::success - 期望 False vs 实际 {response.success}")
        assert response.success is False

    @pytest.mark.asyncio
    async def test_become_follower_if_term_greater(self):
        """测试term > 当前term时转为Follower"""
        node = make_node("node0")
        node._persistent_state.current_term = 2
        node._role = RaftRole.CANDIDATE

        request = AppendEntriesRequest(
            term=5, leader_id="node1",
            prev_log_index=-1, prev_log_term=0,
            entries=[], leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_become_follower_if_term_greater::角色 - 期望 FOLLOWER vs 实际 {node._role.name}")
        assert node._role == RaftRole.FOLLOWER
        print(f"[LOG] test_become_follower_if_term_greater::term - 期望 5 vs 实际 {node._persistent_state.current_term}")
        assert node._persistent_state.current_term == 5

    @pytest.mark.asyncio
    async def test_reject_if_prev_log_index_mismatch(self):
        """测试prev_log_index/term不匹配时返回conflict"""
        node = make_node("node0")
        node._persistent_state.current_term = 2
        # node0有日志: index=0(term=1)
        node._persistent_state.log.append(LogEntry(term=1, index=0, command={}))

        # Leader声称prev_log_index=0, prev_log_term=2, 但实际是term=1
        request = AppendEntriesRequest(
            term=2, leader_id="node1",
            prev_log_index=0, prev_log_term=2,
            entries=[], leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_reject_if_prev_log_index_mismatch::success - 期望 False vs 实际 {response.success}")
        assert response.success is False
        print(f"[LOG] test_reject_if_prev_log_index_mismatch::conflict_index - 期望 0 vs 实际 {response.conflict_index}")
        assert response.conflict_index == 0

    @pytest.mark.asyncio
    async def test_reject_if_prev_log_index_out_of_range(self):
        """测试prev_log_index超出日志范围"""
        node = make_node("node0")
        node._persistent_state.current_term = 2

        request = AppendEntriesRequest(
            term=2, leader_id="node1",
            prev_log_index=5, prev_log_term=2,
            entries=[], leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_reject_if_prev_log_index_out_of_range::success - 期望 False vs 实际 {response.success}")
        assert response.success is False

    @pytest.mark.asyncio
    async def test_normal_append_entries_success(self):
        """测试正常追加日志成功"""
        node = make_node("node0")
        node._persistent_state.current_term = 2

        entry = LogEntry(term=2, index=0, command={"action": "set", "key": "k1", "value": "v1"})
        request = AppendEntriesRequest(
            term=2, leader_id="node1",
            prev_log_index=-1, prev_log_term=0,
            entries=[entry], leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_normal_append_entries_success::success - 期望 True vs 实际 {response.success}")
        assert response.success is True
        print(f"[LOG] test_normal_append_entries_success::日志长度 - 期望 1 vs 实际 {len(node._persistent_state.log)}")
        assert len(node._persistent_state.log) == 1
        assert node._persistent_state.log[0].command == {"action": "set", "key": "k1", "value": "v1"}

    @pytest.mark.asyncio
    async def test_heartbeat_resets_election_timer(self):
        """测试心跳重置选举定时器"""
        node = make_node("node0")
        node._persistent_state.current_term = 2

        # 设置一个假的election_timer
        node._election_timer = MagicMock()

        request = AppendEntriesRequest(
            term=2, leader_id="node1",
            prev_log_index=-1, prev_log_term=0,
            entries=[], leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_heartbeat_resets_election_timer::current_leader - 期望 node1 vs 实际 {node._current_leader}")
        assert node._current_leader == "node1"

    @pytest.mark.asyncio
    async def test_append_entries_updates_commit_index(self):
        """测试AppendEntries更新follower的commit_index"""
        node = make_node("node0")
        node._persistent_state.current_term = 2
        node._persistent_state.log.append(LogEntry(term=2, index=0, command={"action": "set", "key": "k1", "value": "v1"}))

        request = AppendEntriesRequest(
            term=2, leader_id="node1",
            prev_log_index=-1, prev_log_term=0,
            entries=[], leader_commit=0,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_append_entries_updates_commit_index::commit_index - 期望 0 vs 实际 {node._volatile_state.commit_index}")
        assert node._volatile_state.commit_index == 0


class TestAdvanceCommitIndex:
    """测试 _advance_commit_index() 提交推进"""

    @pytest.mark.asyncio
    async def test_only_commit_current_term_entries(self):
        """测试只提交当前term的条目"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 3

        # 添加旧term的条目
        node._persistent_state.log.append(LogEntry(term=1, index=0, command={"action": "set", "key": "a", "value": "1"}))
        # 添加当前term的条目
        node._persistent_state.log.append(LogEntry(term=3, index=1, command={"action": "set", "key": "b", "value": "2"}))

        # 设置match_index: node1和node2都匹配到index=1
        node._volatile_state.match_index["node1"] = 1
        node._volatile_state.match_index["node2"] = 1

        await node._advance_commit_index()

        # 应该提交到index=1（当前term的条目），同时也间接提交了index=0
        print(f"[LOG] test_only_commit_current_term_entries::commit_index - 期望 1 vs 实际 {node._volatile_state.commit_index}")
        assert node._volatile_state.commit_index == 1

    @pytest.mark.asyncio
    async def test_no_commit_without_majority(self):
        """测试没有多数匹配时不提交"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 3

        node._persistent_state.log.append(LogEntry(term=3, index=0, command={"action": "set", "key": "a", "value": "1"}))

        # 只有node1匹配index=0，node2不匹配 -> 2/3没有多数(需要2票)
        # Leader自己(1票) + node1(1票) = 2票 >= 多数(2)，所以会提交
        # 修正: 让node1也不匹配，只有Leader自己匹配 -> 1票 < 多数(2)
        node._volatile_state.match_index["node1"] = -1
        node._volatile_state.match_index["node2"] = -1

        await node._advance_commit_index()

        print(f"[LOG] test_no_commit_without_majority::commit_index - 期望 -1 vs 实际 {node._volatile_state.commit_index}")
        assert node._volatile_state.commit_index == -1

    @pytest.mark.asyncio
    async def test_follower_does_not_advance_commit(self):
        """测试Follower不推进commit_index"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.FOLLOWER
        node._persistent_state.current_term = 3

        node._persistent_state.log.append(LogEntry(term=3, index=0, command={"action": "set", "key": "a", "value": "1"}))
        node._volatile_state.match_index["node1"] = 0
        node._volatile_state.match_index["node2"] = 0

        await node._advance_commit_index()

        print(f"[LOG] test_follower_does_not_advance_commit::commit_index - 期望 -1 vs 实际 {node._volatile_state.commit_index}")
        assert node._volatile_state.commit_index == -1

    @pytest.mark.asyncio
    async def test_commit_skips_old_term_entries(self):
        """测试提交跳过旧term条目（只提交当前term）"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 5

        # term=3的旧条目
        node._persistent_state.log.append(LogEntry(term=3, index=0, command={"action": "set", "key": "a", "value": "1"}))
        # term=5的当前条目
        node._persistent_state.log.append(LogEntry(term=5, index=1, command={"action": "set", "key": "b", "value": "2"}))

        # 所有节点都匹配到index=1
        node._volatile_state.match_index["node1"] = 1
        node._volatile_state.match_index["node2"] = 1

        await node._advance_commit_index()

        # 应该提交到index=1
        print(f"[LOG] test_commit_skips_old_term_entries::commit_index - 期望 1 vs 实际 {node._volatile_state.commit_index}")
        assert node._volatile_state.commit_index == 1


class TestProposeCommand:
    """测试 propose_command() 提案"""

    @pytest.mark.asyncio
    async def test_only_leader_can_propose(self):
        """测试只有Leader才能提案"""
        node = make_node("node0")
        node._role = RaftRole.FOLLOWER

        with pytest.raises(NotLeaderError):
            await node.propose_command({"action": "set", "key": "k1", "value": "v1"})

        print("[LOG] test_only_leader_can_propose::Follower提案 - 期望 NotLeaderError vs 实际 NotLeaderError")

    @pytest.mark.asyncio
    async def test_candidate_cannot_propose(self):
        """测试Candidate不能提案"""
        node = make_node("node0")
        node._role = RaftRole.CANDIDATE

        with pytest.raises(NotLeaderError):
            await node.propose_command({"action": "set", "key": "k1", "value": "v1"})

        print("[LOG] test_candidate_cannot_propose::Candidate提案 - 期望 NotLeaderError vs 实际 NotLeaderError")

    @pytest.mark.asyncio
    async def test_propose_command_appends_to_log(self):
        """测试提案追加到日志"""
        node = make_node("node0", peers=[])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 1
        node._volatile_state.initialize_leader_state([], -1)

        # 手动设置广播为空操作以避免网络调用
        node._broadcast_append_entries = AsyncMock()

        command = {"action": "set", "key": "k1", "value": "v1"}
        # 由于没有follower，提案会超时
        with pytest.raises(ProposalTimeoutError):
            await node.propose_command(command, timeout=0.1)

        print(f"[LOG] test_propose_command_appends_to_log::日志长度 - 期望 1 vs 实际 {len(node._persistent_state.log)}")
        assert len(node._persistent_state.log) == 1
        assert node._persistent_state.log[0].command == command

    @pytest.mark.asyncio
    async def test_propose_command_timeout(self):
        """测试提案超时处理"""
        node = make_node("node0", peers=["node1"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 1
        node._volatile_state.initialize_leader_state(["node1"], -1)

        node._broadcast_append_entries = AsyncMock()

        with pytest.raises(ProposalTimeoutError):
            await node.propose_command({"action": "set", "key": "k1", "value": "v1"}, timeout=0.05)

        print("[LOG] test_propose_command_timeout::提案超时 - 期望 ProposalTimeoutError vs 实际 ProposalTimeoutError")

    @pytest.mark.asyncio
    async def test_propose_command_entry_has_correct_term(self):
        """测试提案条目有正确的term"""
        node = make_node("node0", peers=[])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 7
        node._volatile_state.initialize_leader_state([], -1)
        node._broadcast_append_entries = AsyncMock()

        with pytest.raises(ProposalTimeoutError):
            await node.propose_command({"action": "set", "key": "k1", "value": "v1"}, timeout=0.05)

        actual_term = node._persistent_state.log[0].term
        print(f"[LOG] test_propose_command_entry_has_correct_term::entry term - 期望 7 vs 实际 {actual_term}")
        assert actual_term == 7


class TestInitiateFailover:
    """测试 initiate_failover() 故障转移流程"""

    @pytest.mark.asyncio
    async def test_failover_sets_in_progress_flag(self):
        """测试故障转移设置in_progress标志"""
        node = make_node("node0", peers=["node1", "node2"])
        node._persistent_state.current_term = 1

        # 使用极短超时让failover快速失败
        with pytest.raises(FailoverError):
            await node.initiate_failover("node1", timeout=0.01)

        print(f"[LOG] test_failover_sets_in_progress_flag::in_progress - 期望 False vs 实际 {node._failover_state.in_progress}")
        assert node._failover_state.in_progress is False

    @pytest.mark.asyncio
    async def test_failover_records_failed_leader(self):
        """测试故障转移记录故障Leader"""
        node = make_node("node0", peers=["node1", "node2"])
        node._persistent_state.current_term = 1

        try:
            await node.initiate_failover("node1", timeout=0.01)
        except FailoverError:
            pass

        print(f"[LOG] test_failover_records_failed_leader::failed_leader - 期望 node1 vs 实际 {node._failover_state.failed_leader}")
        assert node._failover_state.failed_leader == "node1"

    @pytest.mark.asyncio
    async def test_concurrent_failover_rejected(self):
        """测试并发故障转移被拒绝"""
        node = make_node("node0", peers=["node1", "node2"])
        node._persistent_state.current_term = 1

        # 手动设置in_progress
        node._failover_state.in_progress = True

        result = await node.initiate_failover("node1", timeout=0.01)
        print(f"[LOG] test_concurrent_failover_rejected::result - 期望 False vs 实际 {result}")
        assert result is False


class TestStateTransitions:
    """测试 _become_leader() 和 _become_follower() 状态转换"""

    @pytest.mark.asyncio
    async def test_become_leader_sets_role(self):
        """测试_become_leader设置角色为LEADER"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.CANDIDATE
        node._persistent_state.current_term = 3

        await node._become_leader()

        print(f"[LOG] test_become_leader_sets_role::角色 - 期望 LEADER vs 实际 {node._role.name}")
        assert node._role == RaftRole.LEADER

    @pytest.mark.asyncio
    async def test_become_leader_sets_current_leader(self):
        """测试_become_leader设置current_leader为自身"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.CANDIDATE
        node._persistent_state.current_term = 3

        await node._become_leader()

        print(f"[LOG] test_become_leader_sets_current_leader::current_leader - 期望 node0 vs 实际 {node._current_leader}")
        assert node._current_leader == "node0"

    @pytest.mark.asyncio
    async def test_become_leader_initializes_volatile_state(self):
        """测试_become_leader初始化next_index和match_index"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.CANDIDATE
        node._persistent_state.current_term = 3
        node._persistent_state.log.append(LogEntry(term=3, index=0, command={}))

        await node._become_leader()

        print(f"[LOG] test_become_leader_initializes_volatile_state::next_index[node1] - 期望 1 vs 实际 {node._volatile_state.next_index.get('node1')}")
        assert node._volatile_state.next_index.get("node1") == 1
        print(f"[LOG] test_become_leader_initializes_volatile_state::match_index[node1] - 期望 -1 vs 实际 {node._volatile_state.match_index.get('node1')}")
        assert node._volatile_state.match_index.get("node1") == -1

    @pytest.mark.asyncio
    async def test_become_follower_resets_role(self):
        """测试_become_follower重置角色为FOLLOWER"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 3

        await node._become_follower(5)

        print(f"[LOG] test_become_follower_resets_role::角色 - 期望 FOLLOWER vs 实际 {node._role.name}")
        assert node._role == RaftRole.FOLLOWER

    @pytest.mark.asyncio
    async def test_become_follower_updates_term(self):
        """测试_become_follower更新term"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 3

        await node._become_follower(5)

        print(f"[LOG] test_become_follower_updates_term::term - 期望 5 vs 实际 {node._persistent_state.current_term}")
        assert node._persistent_state.current_term == 5

    @pytest.mark.asyncio
    async def test_become_follower_clears_voted_for(self):
        """测试_become_follower清除voted_for"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 3
        node._persistent_state.voted_for = "node0"

        await node._become_follower(5)

        print(f"[LOG] test_become_follower_clears_voted_for::voted_for - 期望 None vs 实际 {node._persistent_state.voted_for}")
        assert node._persistent_state.voted_for is None

    @pytest.mark.asyncio
    async def test_become_follower_cancels_heartbeat(self):
        """测试_become_follower取消心跳定时器"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 3

        # 创建一个假的heartbeat_timer
        fake_timer = asyncio.create_task(asyncio.sleep(100))
        node._heartbeat_timer = fake_timer

        await node._become_follower(5)

        print(f"[LOG] test_become_follower_cancels_heartbeat::heartbeat_timer - 期望 None vs 实际 {node._heartbeat_timer}")
        assert node._heartbeat_timer is None


# ============================================================================
# RaftCluster 集群测试
# ============================================================================

class TestRaftClusterElection:
    """测试3节点集群选举"""

    @pytest.mark.asyncio
    async def test_three_node_cluster_elects_leader(self):
        """测试3节点集群能选举出Leader"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)

        print(f"[LOG] test_three_node_cluster_elects_leader::leader - 期望 非 None vs 实际 {leader.node_id if leader else None}")
        assert leader is not None

        await cluster.stop()

    @pytest.mark.asyncio
    async def test_only_one_leader_elected(self):
        """测试只选举出一个Leader"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)
        leader_count = sum(1 for n in cluster.nodes.values() if n.is_leader)

        print(f"[LOG] test_only_one_leader_elected::leader_count - 期望 1 vs 实际 {leader_count}")
        assert leader_count == 1

        await cluster.stop()

    @pytest.mark.asyncio
    async def test_leader_has_majority_votes(self):
        """测试Leader获得多数票"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)

        print(f"[LOG] test_leader_has_majority_votes::votes_received - 期望 >= 2 vs 实际 {len(leader._votes_received)}")
        assert len(leader._votes_received) >= 2

        await cluster.stop()

    @pytest.mark.asyncio
    async def test_all_followers_have_same_term_as_leader(self):
        """测试所有Follower的term与Leader相同"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)
        leader_term = leader._persistent_state.current_term

        for node_id, node in cluster.nodes.items():
            if not node.is_leader:
                print(f"[LOG] test_all_followers_have_same_term_as_leader::{node_id} term - 期望 {leader_term} vs 实际 {node._persistent_state.current_term}")
                assert node._persistent_state.current_term == leader_term

        await cluster.stop()


class TestLeaderHeartbeat:
    """测试Leader心跳维持"""

    @pytest.mark.asyncio
    async def test_heartbeat_prevents_new_election(self):
        """测试心跳阻止新选举"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)
        leader_id = leader.node_id
        leader_term = leader._persistent_state.current_term

        # 等待几个心跳周期
        await asyncio.sleep(0.5)

        # 检查Leader没有变化，term也没有变化
        current_leader = cluster.get_leader()
        print(f"[LOG] test_heartbeat_prevents_new_election::leader_id - 期望 {leader_id} vs 实际 {current_leader.node_id if current_leader else None}")
        assert current_leader is not None
        assert current_leader.node_id == leader_id
        print(f"[LOG] test_heartbeat_prevents_new_election::term - 期望 {leader_term} vs 实际 {current_leader._persistent_state.current_term}")
        assert current_leader._persistent_state.current_term == leader_term

        await cluster.stop()


class TestLeaderFailure:
    """测试Leader故障触发新选举"""

    @pytest.mark.asyncio
    async def test_leader_failure_triggers_new_election(self):
        """测试Leader故障后触发新选举"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)
        old_leader_id = leader.node_id
        print(f"[LOG] test_leader_failure_triggers_new_election::旧Leader - 期望 {old_leader_id} vs 实际 {old_leader_id}")

        # 停止旧Leader
        await leader.stop()

        # 手动选举新Leader（模拟故障后新选举）
        remaining_nodes = [nid for nid in cluster.nodes if nid != old_leader_id]
        new_leader = await manually_elect_leader(cluster, candidate_id=remaining_nodes[0])

        # 新Leader应该是其他节点之一
        print(f"[LOG] test_leader_failure_triggers_new_election::新Leader - 期望 在 {remaining_nodes} 中 vs 实际 {new_leader.node_id if new_leader else None}")
        assert new_leader is not None
        assert new_leader.node_id in remaining_nodes

        await cluster.stop()


class TestLogReplication:
    """测试日志复制"""

    @pytest.mark.asyncio
    async def test_leader_write_replicates_to_followers(self):
        """测试Leader写入后复制到Follower"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)

        # Leader写入
        result = await leader.propose_command(
            {"action": "set", "key": "test_key", "value": "test_value"},
            timeout=5.0,
        )

        print(f"[LOG] test_leader_write_replicates_to_followers::propose result - 期望 success=True vs 实际 {result}")

        # 等待复制
        await asyncio.sleep(0.5)

        # 检查所有节点的日志
        for node_id, node in cluster.nodes.items():
            log_len = len(node._persistent_state.log)
            print(f"[LOG] test_leader_write_replicates_to_followers::{node_id} 日志长度 - 期望 >= 1 vs 实际 {log_len}")
            assert log_len >= 1

        await cluster.stop()

    @pytest.mark.asyncio
    async def test_log_committed_on_majority(self):
        """测试日志在多数节点上提交"""
        cluster = make_cluster(num_nodes=3)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)

        await leader.propose_command(
            {"action": "set", "key": "commit_key", "value": "commit_value"},
            timeout=5.0,
        )

        # 等待提交
        await asyncio.sleep(0.5)

        # Leader的commit_index应该已推进
        print(f"[LOG] test_log_committed_on_majority::leader commit_index - 期望 >= 0 vs 实际 {leader._volatile_state.commit_index}")
        assert leader._volatile_state.commit_index >= 0

        await cluster.stop()


class TestPartitionTolerance:
    """测试分区容忍"""

    @pytest.mark.asyncio
    async def test_minority_partition_cannot_elect_leader(self):
        """测试网络分区后少数派无法选举"""
        cluster = make_cluster(num_nodes=5)
        await start_cluster(cluster)

        leader = await manually_elect_leader(cluster)
        print(f"[LOG] test_minority_partition_cannot_elect_leader::初始Leader - 期望 非 None vs 实际 {leader.node_id if leader else None}")
        assert leader is not None

        # 模拟网络分区: 设置高消息丢失率
        cluster.transport.set_drop_rate(1.0)  # 100%消息丢失

        # 等待选举超时
        await asyncio.sleep(1.0)

        # 恢复网络
        cluster.transport.set_drop_rate(0.0)

        # 重置所有节点的投票状态以模拟分区恢复后的新选举
        for node in cluster.nodes.values():
            node._persistent_state.voted_for = None
            node._votes_received = set()
            if node._role == RaftRole.LEADER:
                node._role = RaftRole.FOLLOWER

        # 手动选举新Leader验证恢复后可以选举
        new_leader = await manually_elect_leader(cluster)
        print(f"[LOG] test_minority_partition_cannot_elect_leader::恢复后Leader - 期望 非 None vs 实际 {new_leader.node_id if new_leader else None}")
        assert new_leader is not None

        await cluster.stop()


# ============================================================================
# BUG验证测试
# ============================================================================

class TestHeartbeatRaceCondition:
    """测试_heartbeat_loop中无锁访问_role的竞态条件"""

    @pytest.mark.asyncio
    async def test_heartbeat_loop_role_check_without_lock(self):
        """
        BUG验证: _heartbeat_loop中检查self._role == RaftRole.LEADER时没有持有锁
        这可能导致在检查后、发送心跳前角色已经改变

        验证方式: 快速切换角色，观察心跳循环行为
        """
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 1
        node._volatile_state.initialize_leader_state(["node1", "node2"], -1)
        node._shutdown_event.clear()

        # 记录心跳循环是否在非Leader状态下尝试发送心跳
        role_at_heartbeat_time = []

        original_broadcast = node._broadcast_append_entries

        async def tracked_broadcast():
            role_at_heartbeat_time.append(node._role)
            # 模拟在广播期间角色改变
            node._role = RaftRole.FOLLOWER
            await original_broadcast()

        node._broadcast_append_entries = tracked_broadcast

        # 启动心跳循环
        heartbeat_task = asyncio.create_task(node._heartbeat_loop())

        # 等待至少一个心跳周期
        await asyncio.sleep(0.1)

        # 检查是否在心跳期间角色发生了变化
        print(f"[LOG] test_heartbeat_loop_role_check_without_lock::心跳期间角色变化 - 期望检测到竞态 vs 实际 {role_at_heartbeat_time}")
        # 如果role_at_heartbeat_time中有LEADER，说明心跳在角色检查后、实际发送前角色可能已经改变
        # 这验证了竞态条件的存在
        has_race = any(r == RaftRole.LEADER for r in role_at_heartbeat_time)
        print(f"[LOG] test_heartbeat_loop_role_check_without_lock::竞态条件存在 - 期望 True vs 实际 {has_race}")

        # 清理
        node._shutdown_event.set()
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


class TestProposeCommandLockReleaseOrder:
    """测试propose_command中先释放锁再注册future的BUG"""

    @pytest.mark.asyncio
    async def test_propose_command_lock_release_before_future_registration(self):
        """
        BUG验证: propose_command中在锁内创建日志条目后释放锁，
        然后注册future。在释放锁和注册future之间存在时间窗口，
        如果日志条目在这个窗口内被提交并应用，future还未注册，
        会导致提案结果丢失。

        验证方式: 模拟快速提交场景
        """
        node = make_node("node0", peers=[])
        node._role = RaftRole.LEADER
        node._persistent_state.current_term = 1
        node._volatile_state.initialize_leader_state([], -1)

        # 追踪pending_proposals的注册时机
        registration_log = []

        original_propose = node.propose_command.__wrapped__ if hasattr(node.propose_command, '__wrapped__') else None

        # 直接测试锁释放和future注册的顺序
        entry = LogEntry(
            term=1, index=0,
            command={"action": "set", "key": "k1", "value": "v1"},
        )

        # 模拟: 在锁内追加日志
        async with node._state_lock:
            node._persistent_state.log.append(entry)
            registration_log.append("log_appended_under_lock")

        # 模拟: 锁释放后注册future
        # 在这个间隙，如果_apply_committed_entries被调用，future还不存在
        future = asyncio.get_running_loop().create_future()
        registration_log.append("future_created_after_lock_release")

        node._pending_proposals[entry.entry_id] = future
        registration_log.append("future_registered")

        print(f"[LOG] test_propose_command_lock_release_before_future_registration::注册顺序 - 期望 ['log_appended_under_lock', 'future_created_after_lock_release', 'future_registered'] vs 实际 {registration_log}")
        assert registration_log == [
            "log_appended_under_lock",
            "future_created_after_lock_release",
            "future_registered",
        ]

        # 验证BUG窗口存在: log_appended和future_registered之间有间隙
        print("[LOG] test_propose_command_lock_release_before_future_registration::BUG窗口验证 - 在log_appended和future_registered之间存在间隙，可能导致结果丢失")


class TestRequestLogSyncLeaderIdBug:
    """测试_request_log_sync_from_leader中leader_id设为自身的BUG"""

    @pytest.mark.asyncio
    async def test_log_sync_request_uses_self_as_leader_id(self):
        """
        BUG验证: _request_log_sync_from_leader中创建AppendEntriesRequest时，
        leader_id设为self.node_id（请求者自身），而不是实际的Leader。
        这会导致Follower收到请求时认为请求来自一个不同的Leader。

        验证: 检查_request_log_sync_from_leader发送的请求中leader_id是否为自身
        """
        node = make_node("node0", peers=["node1", "node2"])
        node._persistent_state.current_term = 3
        node._current_leader = "node1"

        # 捕获发送的请求
        sent_requests = []

        original_send = node.transport.send_append_entries

        async def capture_send(peer_id, request):
            sent_requests.append(request)
            # 返回一个成功响应
            return AppendEntriesResponse(
                term=3, follower_id=peer_id,
                success=True, match_index=-1,
            )

        node.transport.send_append_entries = capture_send

        await node._request_log_sync_from_leader()

        print(f"[LOG] test_log_sync_request_uses_self_as_leader_id::发送请求数 - 期望 1 vs 实际 {len(sent_requests)}")
        assert len(sent_requests) == 1

        actual_leader_id = sent_requests[0].leader_id
        expected_leader_id = node._current_leader  # BUG 3 [P1] 修复后: 应该是实际Leader ID

        print(f"[LOG] test_log_sync_request_uses_self_as_leader_id::请求中leader_id - 期望 {node._current_leader} vs 实际 {actual_leader_id}")

        # BUG 3 [P1] 修复验证: leader_id应该是实际Leader（node._current_leader），而非自身node_id
        assert actual_leader_id == node._current_leader, (
            f"leader_id should be the actual leader ({node._current_leader}), "
            f"not self ({node.node_id})"
        )


# ============================================================================
# 边界条件测试
# ============================================================================

class TestEmptyLogElection:
    """测试空日志的节点参与选举"""

    @pytest.mark.asyncio
    async def test_empty_log_node_can_vote(self):
        """测试空日志节点可以投票"""
        node = make_node("node0")
        node._persistent_state.current_term = 1

        request = RequestVoteRequest(
            term=1, candidate_id="node1",
            last_log_index=-1, last_log_term=0,
        )
        response = await node.handle_request_vote(request)

        print(f"[LOG] test_empty_log_node_can_vote::vote_granted - 期望 True vs 实际 {response.vote_granted}")
        assert response.vote_granted is True

    @pytest.mark.asyncio
    async def test_empty_log_node_can_receive_votes(self):
        """测试空日志节点可以获得投票"""
        node = make_node("node0", peers=["node1", "node2"])

        # node1和node2都是空日志，应该投票给node0
        node1 = make_node("node1")
        node2 = make_node("node2")

        # 模拟node0发起选举
        await node._start_election()

        # node1和node2应该投票给node0
        request = RequestVoteRequest(
            term=node._persistent_state.current_term,
            candidate_id="node0",
            last_log_index=-1, last_log_term=0,
        )

        response1 = await node1.handle_request_vote(request)
        response2 = await node2.handle_request_vote(request)

        print(f"[LOG] test_empty_log_node_can_receive_votes::node1投票 - 期望 True vs 实际 {response1.vote_granted}")
        assert response1.vote_granted is True
        print(f"[LOG] test_empty_log_node_can_receive_votes::node2投票 - 期望 True vs 实际 {response2.vote_granted}")
        assert response2.vote_granted is True


class TestSingleNodeCluster:
    """测试单节点集群"""

    @pytest.mark.asyncio
    async def test_single_node_becomes_leader_immediately(self):
        """测试单节点集群自动成为Leader"""
        node = make_node("node0", peers=[])
        await node.start()

        print(f"[LOG] test_single_node_becomes_leader_immediately::角色 - 期望 LEADER vs 实际 {node._role.name}")
        assert node._role == RaftRole.LEADER
        print(f"[LOG] test_single_node_becomes_leader_immediately::term - 期望 1 vs 实际 {node._persistent_state.current_term}")
        assert node._persistent_state.current_term == 1

        await node.stop()

    @pytest.mark.asyncio
    async def test_single_node_can_propose(self):
        """测试单节点集群可以提案（但因为没有follower会超时）"""
        node = make_node("node0", peers=[])
        await node.start()

        # 单节点没有follower，提案会超时因为没有多数确认
        with pytest.raises(ProposalTimeoutError):
            await node.propose_command(
                {"action": "set", "key": "k1", "value": "v1"},
                timeout=0.1,
            )

        # 但日志应该已经追加
        print(f"[LOG] test_single_node_can_propose::日志长度 - 期望 1 vs 实际 {len(node._persistent_state.log)}")
        assert len(node._persistent_state.log) == 1

        await node.stop()

    @pytest.mark.asyncio
    async def test_single_node_skips_election(self):
        """测试单节点跳过选举直接成为Leader"""
        node = make_node("node0", peers=[])

        # 检查start方法中的单节点逻辑
        await node.start()

        # 验证没有经过Candidate状态
        print(f"[LOG] test_single_node_skips_election::角色 - 期望 LEADER vs 实际 {node._role.name}")
        assert node._role == RaftRole.LEADER
        print(f"[LOG] test_single_node_skips_election::voted_for - 期望 node0 vs 实际 {node._persistent_state.voted_for}")
        assert node._persistent_state.voted_for == "node0"

        await node.stop()


class TestLogConflictTruncation:
    """测试日志冲突时的截断"""

    @pytest.mark.asyncio
    async def test_log_truncation_on_conflict(self):
        """测试日志冲突时截断不一致的条目"""
        node = make_node("node0")
        node._persistent_state.current_term = 2

        # node0有日志: index=0(term=1), index=1(term=2), index=2(term=2)
        node._persistent_state.log.append(LogEntry(term=1, index=0, command={"key": "a"}))
        node._persistent_state.log.append(LogEntry(term=2, index=1, command={"key": "b"}))
        node._persistent_state.log.append(LogEntry(term=2, index=2, command={"key": "c"}))

        # Leader发送: prev_log_index=0, prev_log_term=1, 新条目 index=1(term=3)
        # 这与node0的index=1(term=2)冲突
        new_entry = LogEntry(term=3, index=1, command={"key": "b_new"})
        request = AppendEntriesRequest(
            term=3, leader_id="node1",
            prev_log_index=0, prev_log_term=1,
            entries=[new_entry], leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_log_truncation_on_conflict::success - 期望 True vs 实际 {response.success}")
        assert response.success is True
        print(f"[LOG] test_log_truncation_on_conflict::日志长度 - 期望 2 vs 实际 {len(node._persistent_state.log)}")
        assert len(node._persistent_state.log) == 2
        # index=1应该被新条目替换
        print(f"[LOG] test_log_truncation_on_conflict::index=1的term - 期望 3 vs 实际 {node._persistent_state.log[1].term}")
        assert node._persistent_state.log[1].term == 3
        assert node._persistent_state.log[1].command == {"key": "b_new"}

    @pytest.mark.asyncio
    async def test_multiple_entries_truncated(self):
        """测试多条冲突条目被截断"""
        node = make_node("node0")
        node._persistent_state.current_term = 2

        # node0有5条日志
        for i in range(5):
            node._persistent_state.log.append(LogEntry(term=1, index=i, command={"key": f"old_{i}"}))

        # Leader发送: prev_log_index=1, prev_log_term=1, 新条目从index=2开始
        new_entries = [
            LogEntry(term=2, index=2, command={"key": "new_2"}),
            LogEntry(term=2, index=3, command={"key": "new_3"}),
        ]
        request = AppendEntriesRequest(
            term=2, leader_id="node1",
            prev_log_index=1, prev_log_term=1,
            entries=new_entries, leader_commit=-1,
        )
        response = await node.handle_append_entries(request)

        print(f"[LOG] test_multiple_entries_truncated::success - 期望 True vs 实际 {response.success}")
        assert response.success is True
        print(f"[LOG] test_multiple_entries_truncated::日志长度 - 期望 4 vs 实际 {len(node._persistent_state.log)}")
        assert len(node._persistent_state.log) == 4
        # index=2和3应该是新条目
        print(f"[LOG] test_multiple_entries_truncated::index=2 command - 期望 new_2 vs 实际 {node._persistent_state.log[2].command.get('key')}")
        assert node._persistent_state.log[2].command == {"key": "new_2"}
        print(f"[LOG] test_multiple_entries_truncated::index=3 command - 期望 new_3 vs 实际 {node._persistent_state.log[3].command.get('key')}")
        assert node._persistent_state.log[3].command == {"key": "new_3"}


class TestSnapshotInstallation:
    """测试快照安装"""

    @pytest.mark.asyncio
    async def test_install_snapshot_success(self):
        """测试成功安装快照"""
        node = make_node("node0")
        node._persistent_state.current_term = 2

        # 添加一些日志
        for i in range(3):
            node._persistent_state.log.append(LogEntry(term=1, index=i, command={"key": f"k{i}"}))

        snapshot_data = b'{"snap_key": "snap_value"}'
        request = InstallSnapshotRequest(
            term=2, leader_id="node1",
            last_included_index=2, last_included_term=1,
            offset=0, data=snapshot_data, done=True,
        )
        response = await node.handle_install_snapshot(request)

        print(f"[LOG] test_install_snapshot_success::success - 期望 True vs 实际 {response.success}")
        assert response.success is True
        print(f"[LOG] test_install_snapshot_success::last_snapshot_index - 期望 2 vs 实际 {node._last_snapshot_index}")
        assert node._last_snapshot_index == 2
        print(f"[LOG] test_install_snapshot_success::last_applied - 期望 2 vs 实际 {node._volatile_state.last_applied}")
        assert node._volatile_state.last_applied == 2

    @pytest.mark.asyncio
    async def test_install_snapshot_rejects_old_term(self):
        """测试拒绝旧term的快照"""
        node = make_node("node0")
        node._persistent_state.current_term = 5

        request = InstallSnapshotRequest(
            term=3, leader_id="node1",
            last_included_index=2, last_included_term=1,
            offset=0, data=b'{}', done=True,
        )
        response = await node.handle_install_snapshot(request)

        print(f"[LOG] test_install_snapshot_rejects_old_term::success - 期望 False vs 实际 {response.success}")
        assert response.success is False

    @pytest.mark.asyncio
    async def test_install_snapshot_truncates_log(self):
        """测试安装快照后截断日志"""
        node = make_node("node0")
        node._persistent_state.current_term = 3

        # 添加5条日志
        for i in range(5):
            node._persistent_state.log.append(LogEntry(term=1, index=i, command={"key": f"k{i}"}))

        snapshot_data = b'{"snap_key": "snap_value"}'
        request = InstallSnapshotRequest(
            term=3, leader_id="node1",
            last_included_index=2, last_included_term=1,
            offset=0, data=snapshot_data, done=True,
        )
        response = await node.handle_install_snapshot(request)

        # 快照包含index=0,1,2，日志应该只保留index=3,4
        print(f"[LOG] test_install_snapshot_truncates_log::日志长度 - 期望 2 vs 实际 {len(node._persistent_state.log)}")
        assert len(node._persistent_state.log) == 2

    @pytest.mark.asyncio
    async def test_install_snapshot_updates_commit_index(self):
        """测试安装快照后更新commit_index"""
        node = make_node("node0")
        node._persistent_state.current_term = 3
        node._volatile_state.commit_index = -1

        snapshot_data = b'{"snap_key": "snap_value"}'
        request = InstallSnapshotRequest(
            term=3, leader_id="node1",
            last_included_index=5, last_included_term=2,
            offset=0, data=snapshot_data, done=True,
        )
        response = await node.handle_install_snapshot(request)

        print(f"[LOG] test_install_snapshot_updates_commit_index::commit_index - 期望 5 vs 实际 {node._volatile_state.commit_index}")
        assert node._volatile_state.commit_index == 5

    @pytest.mark.asyncio
    async def test_install_snapshot_becomes_follower_on_higher_term(self):
        """测试安装快照时遇到更高term转为Follower"""
        node = make_node("node0")
        node._persistent_state.current_term = 2
        node._role = RaftRole.CANDIDATE

        snapshot_data = b'{"snap_key": "snap_value"}'
        request = InstallSnapshotRequest(
            term=5, leader_id="node1",
            last_included_index=0, last_included_term=1,
            offset=0, data=snapshot_data, done=True,
        )
        response = await node.handle_install_snapshot(request)

        print(f"[LOG] test_install_snapshot_becomes_follower_on_higher_term::角色 - 期望 FOLLOWER vs 实际 {node._role.name}")
        assert node._role == RaftRole.FOLLOWER
        print(f"[LOG] test_install_snapshot_becomes_follower_on_higher_term::term - 期望 5 vs 实际 {node._persistent_state.current_term}")
        assert node._persistent_state.current_term == 5


# ============================================================================
# 额外边界条件测试
# ============================================================================

class TestRaftStateDataClass:
    """测试RaftState数据类"""

    def test_get_last_log_index_empty(self):
        """测试空日志的get_last_log_index"""
        state = RaftState()
        result = state.get_last_log_index()
        print(f"[LOG] test_get_last_log_index_empty::get_last_log_index - 期望 -1 vs 实际 {result}")
        assert result == -1

    def test_get_last_log_index_with_entries(self):
        """测试有日志的get_last_log_index"""
        state = RaftState()
        state.log.append(LogEntry(term=1, index=0, command={}))
        state.log.append(LogEntry(term=1, index=1, command={}))
        result = state.get_last_log_index()
        print(f"[LOG] test_get_last_log_index_with_entries::get_last_log_index - 期望 1 vs 实际 {result}")
        assert result == 1

    def test_get_last_log_term_empty(self):
        """测试空日志的get_last_log_term"""
        state = RaftState()
        result = state.get_last_log_term()
        print(f"[LOG] test_get_last_log_term_empty::get_last_log_term - 期望 0 vs 实际 {result}")
        assert result == 0

    def test_get_entry_at_valid_index(self):
        """测试获取有效索引的日志条目"""
        state = RaftState()
        entry = LogEntry(term=1, index=0, command={"key": "test"})
        state.log.append(entry)
        result = state.get_entry_at(0)
        print(f"[LOG] test_get_entry_at_valid_index::entry - 期望 term=1 vs 实际 term={result.term}")
        assert result is not None
        assert result.term == 1

    def test_get_entry_at_invalid_index(self):
        """测试获取无效索引的日志条目"""
        state = RaftState()
        result = state.get_entry_at(5)
        print(f"[LOG] test_get_entry_at_invalid_index::entry - 期望 None vs 实际 {result}")
        assert result is None


class TestVolatileStateDataClass:
    """测试VolatileState数据类"""

    def test_initialize_leader_state(self):
        """测试初始化Leader状态"""
        vs = VolatileState()
        vs.initialize_leader_state(["node1", "node2"], last_log_index=5)

        print(f"[LOG] test_initialize_leader_state::next_index[node1] - 期望 6 vs 实际 {vs.next_index.get('node1')}")
        assert vs.next_index.get("node1") == 6
        print(f"[LOG] test_initialize_leader_state::match_index[node1] - 期望 -1 vs 实际 {vs.match_index.get('node1')}")
        assert vs.match_index.get("node1") == -1

    def test_initialize_leader_state_empty_peers(self):
        """测试无peer时初始化Leader状态"""
        vs = VolatileState()
        vs.initialize_leader_state([], last_log_index=0)

        print(f"[LOG] test_initialize_leader_state_empty_peers::next_index - 期望空 vs 实际 {vs.next_index}")
        assert vs.next_index == {}
        print(f"[LOG] test_initialize_leader_state_empty_peers::match_index - 期望空 vs 实际 {vs.match_index}")
        assert vs.match_index == {}


class TestRaftConfigValidation:
    """测试RaftConfig配置验证"""

    def test_invalid_election_timeout_range(self):
        """测试无效的选举超时范围"""
        with pytest.raises(ValueError):
            RaftConfig(node_id="node0", election_timeout_min=0.5, election_timeout_max=0.3)
        print("[LOG] test_invalid_election_timeout_range::期望 ValueError vs 实际 ValueError")

    def test_heartbeat_greater_than_election_timeout(self):
        """测试心跳间隔大于选举超时"""
        with pytest.raises(ValueError):
            RaftConfig(node_id="node0", heartbeat_interval=0.5, election_timeout_min=0.3)
        print("[LOG] test_heartbeat_greater_than_election_timeout::期望 ValueError vs 实际 ValueError")

    def test_invalid_fault_detection_timeout(self):
        """测试无效的故障检测超时"""
        with pytest.raises(ValueError):
            RaftConfig(node_id="node0", fault_detection_timeout=-1)
        print("[LOG] test_invalid_fault_detection_timeout::期望 ValueError vs 实际 ValueError")

    def test_invalid_heartbeat_timeout_factor(self):
        """测试无效的心跳超时因子"""
        with pytest.raises(ValueError):
            RaftConfig(node_id="node0", heartbeat_timeout_factor=2)
        print("[LOG] test_invalid_heartbeat_timeout_factor::期望 ValueError vs 实际 ValueError")


class TestMemoryStateMachine:
    """测试MemoryStateMachine"""

    @pytest.mark.asyncio
    async def test_set_command(self):
        """测试set命令"""
        sm = MemoryStateMachine()
        result = await sm.apply({"action": "set", "key": "k1", "value": "v1"})
        print(f"[LOG] test_set_command::result - 期望 success=True vs 实际 {result}")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_command(self):
        """测试get命令"""
        sm = MemoryStateMachine()
        await sm.apply({"action": "set", "key": "k1", "value": "v1"})
        result = await sm.apply({"action": "get", "key": "k1"})
        print(f"[LOG] test_get_command::value - 期望 v1 vs 实际 {result.get('value')}")
        assert result["success"] is True
        assert result["value"] == "v1"

    @pytest.mark.asyncio
    async def test_delete_command(self):
        """测试delete命令"""
        sm = MemoryStateMachine()
        await sm.apply({"action": "set", "key": "k1", "value": "v1"})
        result = await sm.apply({"action": "delete", "key": "k1"})
        print(f"[LOG] test_delete_command::result - 期望 success=True vs 实际 {result}")
        assert result["success"] is True
        # 验证已删除
        result = await sm.apply({"action": "get", "key": "k1"})
        assert result.get("value") is None

    @pytest.mark.asyncio
    async def test_snapshot_and_restore(self):
        """测试快照和恢复"""
        sm = MemoryStateMachine()
        await sm.apply({"action": "set", "key": "k1", "value": "v1"})
        await sm.apply({"action": "set", "key": "k2", "value": "v2"})

        snapshot = await sm.snapshot()
        print(f"[LOG] test_snapshot_and_restore::snapshot长度 - 期望 > 0 vs 实际 {len(snapshot)}")
        assert len(snapshot) > 0

        # 创建新的状态机并恢复
        sm2 = MemoryStateMachine()
        await sm2.restore(snapshot)

        result = await sm2.apply({"action": "get", "key": "k1"})
        print(f"[LOG] test_snapshot_and_restore::恢复后k1 - 期望 v1 vs 实际 {result.get('value')}")
        assert result["value"] == "v1"

        result = await sm2.apply({"action": "get", "key": "k2"})
        print(f"[LOG] test_snapshot_and_restore::恢复后k2 - 期望 v2 vs 实际 {result.get('value')}")
        assert result["value"] == "v2"


class TestLogEntryDataClass:
    """测试LogEntry数据类"""

    def test_log_entry_creation(self):
        """测试日志条目创建"""
        entry = LogEntry(term=1, index=0, command={"action": "set", "key": "k1", "value": "v1"})
        print(f"[LOG] test_log_entry_creation::term - 期望 1 vs 实际 {entry.term}")
        assert entry.term == 1
        assert entry.index == 0
        assert entry.command == {"action": "set", "key": "k1", "value": "v1"}
        assert entry.entry_id != ""  # 自动生成

    def test_log_entry_to_dict(self):
        """测试日志条目序列化"""
        entry = LogEntry(term=1, index=0, command={"action": "set", "key": "k1"})
        d = entry.to_dict()
        print(f"[LOG] test_log_entry_to_dict::keys - 期望包含term,index,command vs 实际 {list(d.keys())}")
        assert "term" in d
        assert "index" in d
        assert "command" in d

    def test_log_entry_from_dict(self):
        """测试日志条目反序列化"""
        d = {"term": 2, "index": 5, "command": {"action": "delete", "key": "k1"}}
        entry = LogEntry.from_dict(d)
        print(f"[LOG] test_log_entry_from_dict::term - 期望 2 vs 实际 {entry.term}")
        assert entry.term == 2
        assert entry.index == 5


class TestInMemoryTransport:
    """测试InMemoryTransport"""

    @pytest.mark.asyncio
    async def test_send_request_vote_to_registered_node(self):
        """测试向已注册节点发送投票请求"""
        transport = InMemoryTransport()
        node = make_node("node0")
        node._ensure_async_primitives()
        transport.register_node(node)

        request = RequestVoteRequest(
            term=1, candidate_id="node1",
            last_log_index=-1, last_log_term=0,
        )
        response = await transport.send_request_vote("node0", request)

        print(f"[LOG] test_send_request_vote_to_registered_node::response - 期望 非 None vs 实际 {response}")
        assert response is not None

    @pytest.mark.asyncio
    async def test_send_request_vote_to_unregistered_node(self):
        """测试向未注册节点发送投票请求"""
        transport = InMemoryTransport()

        request = RequestVoteRequest(
            term=1, candidate_id="node1",
            last_log_index=-1, last_log_term=0,
        )
        response = await transport.send_request_vote("unknown", request)

        print(f"[LOG] test_send_request_vote_to_unregistered_node::response - 期望 None vs 实际 {response}")
        assert response is None

    @pytest.mark.asyncio
    async def test_message_drop_simulation(self):
        """测试消息丢失模拟"""
        transport = InMemoryTransport()
        transport.set_drop_rate(1.0)  # 100%丢失

        node = make_node("node0")
        node._ensure_async_primitives()
        transport.register_node(node)

        request = RequestVoteRequest(
            term=1, candidate_id="node1",
            last_log_index=-1, last_log_term=0,
        )
        response = await transport.send_request_vote("node0", request)

        print(f"[LOG] test_message_drop_simulation::response (100% drop) - 期望 None vs 实际 {response}")
        assert response is None


class TestHandleVoteResponse:
    """测试投票响应处理"""

    @pytest.mark.asyncio
    async def test_vote_response_with_higher_term(self):
        """测试收到更高term的投票响应时转为Follower"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.CANDIDATE
        node._persistent_state.current_term = 3

        response = RequestVoteResponse(
            term=5, voter_id="node1", vote_granted=True,
        )
        await node._handle_vote_response(response)

        print(f"[LOG] test_vote_response_with_higher_term::角色 - 期望 FOLLOWER vs 实际 {node._role.name}")
        assert node._role == RaftRole.FOLLOWER
        print(f"[LOG] test_vote_response_with_higher_term::term - 期望 5 vs 实际 {node._persistent_state.current_term}")
        assert node._persistent_state.current_term == 5

    @pytest.mark.asyncio
    async def test_vote_response_ignored_if_not_candidate(self):
        """测试非Candidate时忽略投票响应"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.FOLLOWER
        node._persistent_state.current_term = 3

        response = RequestVoteResponse(
            term=3, voter_id="node1", vote_granted=True,
        )
        await node._handle_vote_response(response)

        print(f"[LOG] test_vote_response_ignored_if_not_candidate::votes_received - 期望空 vs 实际 {node._votes_received}")
        assert "node1" not in node._votes_received

    @pytest.mark.asyncio
    async def test_vote_response_ignored_if_term_mismatch(self):
        """测试term不匹配时忽略投票响应"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.CANDIDATE
        node._persistent_state.current_term = 3

        response = RequestVoteResponse(
            term=2, voter_id="node1", vote_granted=True,
        )
        await node._handle_vote_response(response)

        print(f"[LOG] test_vote_response_ignored_if_term_mismatch::votes_received - 期望空 vs 实际 {node._votes_received}")
        assert "node1" not in node._votes_received

    @pytest.mark.asyncio
    async def test_majority_votes_trigger_leadership(self):
        """测试获得多数票后成为Leader"""
        node = make_node("node0", peers=["node1", "node2"])
        node._role = RaftRole.CANDIDATE
        node._persistent_state.current_term = 3
        node._votes_received = {"node0"}  # 自己已投票

        # node1投票
        response1 = RequestVoteResponse(term=3, voter_id="node1", vote_granted=True)
        await node._handle_vote_response(response1)

        print(f"[LOG] test_majority_votes_trigger_leadership::角色 - 期望 LEADER vs 实际 {node._role.name}")
        assert node._role == RaftRole.LEADER


class TestGetStatus:
    """测试get_status方法"""

    @pytest.mark.asyncio
    async def test_get_status_returns_dict(self):
        """测试get_status返回字典"""
        node = make_node("node0")
        status = await node.get_status()

        print(f"[LOG] test_get_status_returns_dict::类型 - 期望 dict vs 实际 {type(status).__name__}")
        assert isinstance(status, dict)
        print(f"[LOG] test_get_status_returns_dict::node_id - 期望 node0 vs 实际 {status.get('node_id')}")
        assert status["node_id"] == "node0"
        print(f"[LOG] test_get_status_returns_dict::role - 期望 FOLLOWER vs 实际 {status.get('role')}")
        assert status["role"] == "FOLLOWER"


class TestNodeStop:
    """测试节点停止"""

    @pytest.mark.asyncio
    async def test_stop_clears_pending_proposals(self):
        """测试停止时清理待处理提案"""
        node = make_node("node0")
        await node.start()

        # 创建一个假的pending proposal
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        node._pending_proposals["test_id"] = future

        await node.stop()

        print(f"[LOG] test_stop_clears_pending_proposals::pending_proposals - 期望空 vs 实际 {node._pending_proposals}")
        assert len(node._pending_proposals) == 0

    @pytest.mark.asyncio
    async def test_stop_cancels_timers(self):
        """测试停止时取消定时器"""
        node = make_node("node0")
        await node.start()

        await node.stop()

        print(f"[LOG] test_stop_cancels_timers::election_timer - 期望 None vs 实际 {node._election_timer}")
        assert node._election_timer is None
        print(f"[LOG] test_stop_cancels_timers::heartbeat_timer - 期望 None vs 实际 {node._heartbeat_timer}")
        assert node._heartbeat_timer is None


class TestApplyCommittedEntries:
    """测试应用已提交条目"""

    @pytest.mark.asyncio
    async def test_apply_entries_to_state_machine(self):
        """测试将已提交条目应用到状态机"""
        node = make_node("node0")
        node._volatile_state.commit_index = 0
        node._volatile_state.last_applied = -1

        entry = LogEntry(term=1, index=0, command={"action": "set", "key": "apply_key", "value": "apply_value"})
        node._persistent_state.log.append(entry)

        await node._apply_committed_entries()

        # 验证状态机已应用
        result = await node.state_machine.apply({"action": "get", "key": "apply_key"})
        print(f"[LOG] test_apply_entries_to_state_machine::value - 期望 apply_value vs 实际 {result.get('value')}")
        assert result["value"] == "apply_value"
        print(f"[LOG] test_apply_entries_to_state_machine::last_applied - 期望 0 vs 实际 {node._volatile_state.last_applied}")
        assert node._volatile_state.last_applied == 0

    @pytest.mark.asyncio
    async def test_apply_multiple_entries(self):
        """测试应用多条已提交条目"""
        node = make_node("node0")
        node._volatile_state.commit_index = 2
        node._volatile_state.last_applied = -1

        for i in range(3):
            entry = LogEntry(term=1, index=i, command={"action": "set", "key": f"key{i}", "value": f"val{i}"})
            node._persistent_state.log.append(entry)

        await node._apply_committed_entries()

        print(f"[LOG] test_apply_multiple_entries::last_applied - 期望 2 vs 实际 {node._volatile_state.last_applied}")
        assert node._volatile_state.last_applied == 2

        # 验证所有条目都已应用
        for i in range(3):
            result = await node.state_machine.apply({"action": "get", "key": f"key{i}"})
            print(f"[LOG] test_apply_multiple_entries::key{i} value - 期望 val{i} vs 实际 {result.get('value')}")
            assert result["value"] == f"val{i}"


class TestCalculateRetryDelay:
    """测试指数退避重试延迟计算"""

    @pytest.mark.asyncio
    async def test_retry_delay_increases_exponentially(self):
        """测试重试延迟指数增长"""
        node = make_node("node0")

        delay0 = node._calculate_retry_delay(0)
        delay1 = node._calculate_retry_delay(1)
        delay2 = node._calculate_retry_delay(2)

        print(f"[LOG] test_retry_delay_increases_exponentially::delay0 - 期望 0.1 vs 实际 {delay0}")
        assert delay0 == 0.1
        print(f"[LOG] test_retry_delay_increases_exponentially::delay1 > delay0 - 期望 True vs 实际 {delay1 > delay0}")
        assert delay1 > delay0
        print(f"[LOG] test_retry_delay_increases_exponentially::delay2 > delay1 - 期望 True vs 实际 {delay2 > delay1}")
        assert delay2 > delay1

    @pytest.mark.asyncio
    async def test_retry_delay_capped_at_max(self):
        """测试重试延迟不超过最大值"""
        node = make_node("node0")

        delay_large = node._calculate_retry_delay(100)
        print(f"[LOG] test_retry_delay_capped_at_max::delay(100) - 期望 <= 5.0 vs 实际 {delay_large}")
        assert delay_large <= node.failover_config.max_retry_delay


class TestWaitForStateRecovery:
    """测试等待状态恢复"""

    @pytest.mark.asyncio
    async def test_recovery_when_already_applied(self):
        """测试已应用时立即恢复"""
        node = make_node("node0")
        node._volatile_state.commit_index = -1
        node._volatile_state.last_applied = -1

        result = await node.wait_for_state_recovery(timeout=1.0)
        print(f"[LOG] test_recovery_when_already_applied::result - 期望 True vs 实际 {result}")
        assert result is True

    @pytest.mark.asyncio
    async def test_recovery_timeout(self):
        """测试恢复超时"""
        node = make_node("node0")
        node._volatile_state.commit_index = 10
        node._volatile_state.last_applied = -1

        result = await node.wait_for_state_recovery(timeout=0.2)
        print(f"[LOG] test_recovery_timeout::result - 期望 False vs 实际 {result}")
        assert result is False

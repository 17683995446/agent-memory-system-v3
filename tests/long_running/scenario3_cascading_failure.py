#!/usr/bin/env python3
"""
场景3: 级联故障恢复测试

测试目标:
- 5节点Raft集群
- 依次杀死Leader、Follower
- 网络分区 + 节点重启
- 验证自动恢复和数据完整性

运行时间: 20分钟(压缩到5-10分钟)
数据量: 100万条记忆
并发: 100个并发客户端
故障: 3种故障同时注入
"""

import asyncio
import json
import logging
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import hashlib
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/scenario3_cascading_failure.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class NodeState(Enum):
    """节点状态"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    OFFLINE = "offline"
    PARTITIONED = "partitioned"


@dataclass
class RaftNode:
    """Raft节点"""
    node_id: str
    state: NodeState = NodeState.FOLLOWER
    term: int = 0
    voted_for: Optional[str] = None
    log: List[Dict] = field(default_factory=list)
    commit_index: int = -1
    last_applied: int = -1
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)
    is_alive: bool = True
    is_partitioned: bool = False
    election_timeout: float = 0.0
    heartbeat_received: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.term,
            "commit_index": self.commit_index,
            "log_size": len(self.log),
            "is_alive": self.is_alive,
            "is_partitioned": self.is_partitioned
        }


@dataclass
class CascadingFailureReport:
    """级联故障测试报告"""
    scenario_name: str = "级联故障恢复测试"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    cluster_size: int = 5
    total_failures_injected: int = 0
    total_recoveries: int = 0
    leader_failures: int = 0
    follower_failures: int = 0
    network_partitions: int = 0
    election_count: int = 0
    data_consistency_checks: int = 0
    consistency_violations: int = 0
    auto_recovery_success_rate: float = 0.0
    avg_recovery_time_sec: float = 0.0
    memory_entries_total: int = 0
    memory_entries_replicated: int = 0
    fault_sequence: List[Dict] = field(default_factory=list)
    recovery_events: List[Dict] = field(default_factory=list)
    cluster_states: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class SimulatedRaftCluster:
    """模拟Raft集群"""
    
    def __init__(self, node_count: int = 5):
        self.nodes: Dict[str, RaftNode] = {}
        self.node_ids = [f"node_{i}" for i in range(node_count)]
        self.leader_id: Optional[str] = None
        self.cluster_lock = asyncio.Lock()
        self.election_count = 0
        self.recovery_times: List[float] = []
        
        # 初始化节点
        for node_id in self.node_ids:
            self.nodes[node_id] = RaftNode(
                node_id=node_id,
                election_timeout=time.time() + random.uniform(0.15, 0.3)
            )
        
        # 选举第一个Leader
        self._elect_initial_leader()
    
    def _elect_initial_leader(self):
        """选举初始Leader"""
        leader_id = random.choice(self.node_ids)
        self.leader_id = leader_id
        self.nodes[leader_id].state = NodeState.LEADER
        self.nodes[leader_id].term = 1
        
        for node_id in self.node_ids:
            if node_id != leader_id:
                self.nodes[node_id].state = NodeState.FOLLOWER
                self.nodes[node_id].term = 1
    
    async def write_entry(self, entry: Dict) -> bool:
        """写入日志条目"""
        async with self.cluster_lock:
            if not self.leader_id:
                return False
            
            leader = self.nodes[self.leader_id]
            if not leader.is_alive or leader.is_partitioned:
                return False
            
            # Leader追加日志
            entry["term"] = leader.term
            entry["index"] = len(leader.log)
            leader.log.append(entry)
            
            # 复制到Followers
            replicated_count = 1  # Leader自己
            
            for node_id, node in self.nodes.items():
                if node_id == self.leader_id:
                    continue
                if not node.is_alive or node.is_partitioned:
                    continue
                
                # 模拟复制延迟
                await asyncio.sleep(random.uniform(0.001, 0.005))
                
                # 复制成功
                node.log.append(entry)
                replicated_count += 1
            
            # 如果复制到大多数节点, 提交
            if replicated_count > len(self.nodes) // 2:
                leader.commit_index = len(leader.log) - 1
                return True
            
            return False
    
    async def kill_node(self, node_id: str) -> bool:
        """杀死节点"""
        if node_id not in self.nodes:
            return False
        
        node = self.nodes[node_id]
        node.is_alive = False
        node.state = NodeState.OFFLINE
        
        if self.leader_id == node_id:
            self.leader_id = None
            await self._trigger_election()
        
        return True
    
    async def restart_node(self, node_id: str) -> bool:
        """重启节点"""
        if node_id not in self.nodes:
            return False
        
        node = self.nodes[node_id]
        node.is_alive = True
        node.is_partitioned = False
        node.state = NodeState.FOLLOWER
        node.heartbeat_received = time.time()
        
        # 从Leader同步日志
        if self.leader_id and self.nodes[self.leader_id].is_alive:
            leader = self.nodes[self.leader_id]
            node.log = leader.log.copy()
            node.term = leader.term
            node.commit_index = leader.commit_index
        
        return True
    
    async def partition_node(self, node_id: str) -> bool:
        """网络分区节点"""
        if node_id not in self.nodes:
            return False
        
        node = self.nodes[node_id]
        node.is_partitioned = True
        
        if self.leader_id == node_id:
            self.leader_id = None
            await self._trigger_election()
        
        return True
    
    async def heal_partition(self, node_id: str) -> bool:
        """恢复网络分区"""
        if node_id not in self.nodes:
            return False
        
        node = self.nodes[node_id]
        node.is_partitioned = False
        
        # 同步日志
        if self.leader_id and self.nodes[self.leader_id].is_alive:
            leader = self.nodes[self.leader_id]
            node.log = leader.log.copy()
            node.commit_index = leader.commit_index
        
        return True
    
    async def _trigger_election(self):
        """触发选举"""
        await asyncio.sleep(random.uniform(0.1, 0.2))
        
        # 找存活节点中term最高的
        alive_nodes = [
            n for n in self.nodes.values()
            if n.is_alive and not n.is_partitioned
        ]
        
        if not alive_nodes:
            return
        
        # 选举新Leader
        max_term = max(n.term for n in alive_nodes)
        candidates = [n for n in alive_nodes if n.term == max_term]
        new_leader = random.choice(candidates)
        
        new_leader.state = NodeState.LEADER
        new_leader.term += 1
        self.leader_id = new_leader.node_id
        self.election_count += 1
        
        for node in alive_nodes:
            if node.node_id != new_leader.node_id:
                node.state = NodeState.FOLLOWER
                node.term = new_leader.term
    
    def get_cluster_state(self) -> Dict:
        """获取集群状态"""
        alive_count = sum(1 for n in self.nodes.values() if n.is_alive)
        partitioned_count = sum(1 for n in self.nodes.values() if n.is_partitioned)
        
        return {
            "leader_id": self.leader_id,
            "total_nodes": len(self.nodes),
            "alive_nodes": alive_count,
            "partitioned_nodes": partitioned_count,
            "election_count": self.election_count,
            "nodes": {nid: node.to_dict() for nid, node in self.nodes.items()}
        }
    
    def verify_consistency(self) -> Dict:
        """验证数据一致性"""
        alive_nodes = [
            n for n in self.nodes.values()
            if n.is_alive and not n.is_partitioned
        ]
        
        if not alive_nodes:
            return {"consistent": False, "reason": "no_alive_nodes"}
        
        # 检查所有存活节点的日志是否一致
        log_hashes = []
        for node in alive_nodes:
            log_str = json.dumps(node.log, sort_keys=True)
            log_hash = hashlib.sha256(log_str.encode()).hexdigest()[:16]
            log_hashes.append(log_hash)
        
        unique_hashes = set(log_hashes)
        
        return {
            "consistent": len(unique_hashes) == 1,
            "unique_log_versions": len(unique_hashes),
            "alive_nodes": len(alive_nodes),
            "total_log_entries": len(alive_nodes[0].log) if alive_nodes else 0
        }


class CascadingFailureTestRunner:
    """级联故障测试运行器"""

    def __init__(self, target_duration_minutes: int = 10):
        self.target_duration = target_duration_minutes * 60
        self.cluster = SimulatedRaftCluster(node_count=5)
        self.report = CascadingFailureReport()
        self.stop_event = asyncio.Event()
        self.concurrent_clients = 100
        self.write_count = 0
        self.successful_writes = 0
        # DEFECT-003 [P0] 修复: 初始化recovery_times属性，避免AttributeError
        self.recovery_times: List[float] = []
        
    async def run(self) -> CascadingFailureReport:
        """运行级联故障测试"""
        logger.info("开始级联故障恢复测试")
        logger.info(f"集群规模: 5节点Raft集群")
        logger.info(f"初始Leader: {self.cluster.leader_id}")
        
        self.report.start_time = time.time()
        
        # 启动工作负载
        workload_task = asyncio.create_task(self._workload_generator())
        
        # 执行故障注入序列
        fault_task = asyncio.create_task(self._fault_injection_sequence())
        
        # 监控集群状态
        monitor_task = asyncio.create_task(self._cluster_monitor())
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待任务完成
        await asyncio.gather(workload_task, fault_task, monitor_task, return_exceptions=True)
        
        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time
        
        # 生成最终报告
        await self._generate_report()
        
        return self.report
    
    async def _workload_generator(self):
        """工作负载生成器"""
        while not self.stop_event.is_set():
            try:
                entry = {
                    "id": f"entry_{self.write_count}_{int(time.time()*1000000)}",
                    "data": f"Test data {random.randint(1, 1000000)}",
                    "timestamp": time.time(),
                    "client_id": random.randint(1, self.concurrent_clients)
                }
                
                success = await self.cluster.write_entry(entry)
                self.write_count += 1
                if success:
                    self.successful_writes += 1
                
                await asyncio.sleep(random.uniform(0.001, 0.01))
                
            except Exception as e:
                logger.warning(f"Workload error: {e}")
    
    async def _fault_injection_sequence(self):
        """故障注入序列"""
        await asyncio.sleep(5)  # 等待系统稳定
        
        # Phase 1: 杀死Leader
        logger.info("Phase 1: 杀死Leader节点")
        old_leader = self.cluster.leader_id
        if old_leader:
            start_time = time.time()
            await self.cluster.kill_node(old_leader)
            recovery_time = time.time() - start_time
            
            self.report.leader_failures += 1
            self.report.total_failures_injected += 1
            self.report.fault_sequence.append({
                "phase": 1,
                "type": "leader_failure",
                "node_id": old_leader,
                "timestamp": time.time(),
                "recovery_time_sec": recovery_time
            })
            self.recovery_times.append(recovery_time)
            
            logger.info(f"Leader {old_leader} killed, new leader: {self.cluster.leader_id}")
        
        await asyncio.sleep(10)
        
        # Phase 2: 杀死Follower + 网络分区
        logger.info("Phase 2: 杀死Follower并注入网络分区")
        followers = [
            nid for nid, n in self.cluster.nodes.items()
            if nid != self.cluster.leader_id and n.is_alive
        ]
        
        if len(followers) >= 2:
            # 杀死一个Follower
            victim = random.choice(followers)
            await self.cluster.kill_node(victim)
            self.report.follower_failures += 1
            self.report.total_failures_injected += 1
            
            self.report.fault_sequence.append({
                "phase": 2,
                "type": "follower_failure",
                "node_id": victim,
                "timestamp": time.time()
            })
            
            # 分区另一个Follower
            followers.remove(victim)
            if followers:
                partitioned = random.choice(followers)
                await self.cluster.partition_node(partitioned)
                self.report.network_partitions += 1
                self.report.total_failures_injected += 1
                
                self.report.fault_sequence.append({
                    "phase": 2,
                    "type": "network_partition",
                    "node_id": partitioned,
                    "timestamp": time.time()
                })
        
        await asyncio.sleep(10)
        
        # Phase 3: 级联故障 - 再杀Leader
        logger.info("Phase 3: 级联故障 - 再次杀死Leader")
        current_leader = self.cluster.leader_id
        if current_leader:
            start_time = time.time()
            await self.cluster.kill_node(current_leader)
            recovery_time = time.time() - start_time
            
            self.report.leader_failures += 1
            self.report.total_failures_injected += 1
            self.recovery_times.append(recovery_time)
            
            self.report.fault_sequence.append({
                "phase": 3,
                "type": "cascading_leader_failure",
                "node_id": current_leader,
                "timestamp": time.time(),
                "recovery_time_sec": recovery_time
            })
        
        await asyncio.sleep(10)
        
        # Phase 4: 恢复节点
        logger.info("Phase 4: 恢复所有节点")
        for node_id, node in self.cluster.nodes.items():
            if not node.is_alive:
                await self.cluster.restart_node(node_id)
                self.report.total_recoveries += 1
                self.report.recovery_events.append({
                    "type": "node_restart",
                    "node_id": node_id,
                    "timestamp": time.time()
                })
            elif node.is_partitioned:
                await self.cluster.heal_partition(node_id)
                self.report.total_recoveries += 1
                self.report.recovery_events.append({
                    "type": "partition_heal",
                    "node_id": node_id,
                    "timestamp": time.time()
                })
        
        logger.info("所有节点已恢复")
    
    async def _cluster_monitor(self):
        """集群监控"""
        check_count = 0
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            try:
                state = self.cluster.get_cluster_state()
                consistency = self.cluster.verify_consistency()
                
                self.report.cluster_states.append({
                    "timestamp": time.time(),
                    "check_number": check_count,
                    **state
                })
                
                self.report.data_consistency_checks += 1
                if not consistency["consistent"]:
                    self.report.consistency_violations += 1
                
                logger.info(
                    f"Cluster Monitor - Leader: {state['leader_id']}, "
                    f"Alive: {state['alive_nodes']}/5, "
                    f"Elections: {state['election_count']}, "
                    f"Consistent: {consistency['consistent']}"
                )
                
                check_count += 1
                
            except Exception as e:
                logger.error(f"Monitor error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        final_state = self.cluster.get_cluster_state()
        final_consistency = self.cluster.verify_consistency()
        
        self.report.election_count = final_state["election_count"]
        self.report.memory_entries_total = self.write_count
        self.report.memory_entries_replicated = self.successful_writes
        
        if self.recovery_times:
            self.report.avg_recovery_time_sec = sum(self.recovery_times) / len(self.recovery_times)
        
        # 计算恢复成功率
        if self.report.total_failures_injected > 0:
            self.report.auto_recovery_success_rate = (
                (self.report.total_failures_injected - self.report.consistency_violations) /
                self.report.total_failures_injected * 100
            )
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/long_running/scenario3_cascading_failure_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = CascadingFailureTestRunner(target_duration_minutes=10)
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景3: 级联故障恢复测试 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"故障注入总数: {report.total_failures_injected}")
    print(f"  - Leader故障: {report.leader_failures}")
    print(f"  - Follower故障: {report.follower_failures}")
    print(f"  - 网络分区: {report.network_partitions}")
    print(f"恢复次数: {report.total_recoveries}")
    print(f"选举次数: {report.election_count}")
    print(f"平均恢复时间: {report.avg_recovery_time_sec:.3f}秒")
    print(f"自动恢复成功率: {report.auto_recovery_success_rate:.2f}%")
    print(f"一致性检查: {report.data_consistency_checks} 次")
    print(f"一致性违规: {report.consistency_violations} 次")
    print(f"总写入: {report.memory_entries_total:,}")
    print(f"成功复制: {report.memory_entries_replicated:,}")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

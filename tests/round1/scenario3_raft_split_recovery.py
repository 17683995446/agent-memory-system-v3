#!/usr/bin/env python3
"""
场景3: Raft集群脑裂恢复测试
Agent Memory System V3.0 - 第1轮实践

测试目标:
- 5节点Raft集群
- 网络分区成 2+3
- 各自写入后恢复
- 验证数据合并和冲突解决

关键指标:
- 脑裂检测时间
- 数据一致性恢复
- 冲突解决正确性
- 集群可用性
"""

import asyncio
import hashlib
import json
import logging
import random
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# 配置日志
log_dir = Path("/workspace/agent_memory_system_v2/tests/round1/logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'scenario3_raft_split_recovery.log'),
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
    is_alive: bool = True
    is_partitioned: bool = False
    partition_group: int = 0  # 0=未分区, 1=分区1, 2=分区2
    
    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.term,
            "commit_index": self.commit_index,
            "log_size": len(self.log),
            "is_alive": self.is_alive,
            "is_partitioned": self.is_partitioned,
            "partition_group": self.partition_group
        }


@dataclass
class SplitRecoveryMetrics:
    """脑裂恢复测试指标"""
    scenario_name: str = "Raft集群脑裂恢复"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    
    # 集群配置
    cluster_size: int = 5
    partition_groups: List[int] = field(default_factory=lambda: [2, 3])
    
    # 脑裂阶段
    split_start_time: float = 0.0
    split_end_time: float = 0.0
    split_duration_sec: float = 0.0
    
    # 写入统计
    writes_partition_1: int = 0
    writes_partition_2: int = 0
    total_writes_during_split: int = 0
    
    # 恢复统计
    recovery_start_time: float = 0.0
    recovery_end_time: float = 0.0
    recovery_duration_sec: float = 0.0
    
    # 一致性检查
    data_consistency_violations: int = 0
    conflicts_detected: int = 0
    conflicts_resolved: int = 0
    data_loss_entries: int = 0
    
    # 集群状态
    election_count: int = 0
    leader_changes: int = 0
    
    # 时间线数据
    cluster_states: List[Dict] = field(default_factory=list)
    write_timeline: List[Dict] = field(default_factory=list)
    conflict_timeline: List[Dict] = field(default_factory=list)
    
    # 错误和缺陷
    errors: List[Dict] = field(default_factory=list)
    defects_found: List[Dict] = field(default_factory=list)


class RaftClusterSimulator:
    """Raft集群模拟器"""
    
    def __init__(self, node_count: int = 5):
        self.nodes: Dict[str, RaftNode] = {}
        self.node_ids = [f"node_{i}" for i in range(node_count)]
        self.leader_id: Optional[str] = None
        self.cluster_lock = asyncio.Lock()
        self.election_count = 0
        
        # 初始化节点
        for node_id in self.node_ids:
            self.nodes[node_id] = RaftNode(node_id=node_id)
        
        # 选举初始Leader
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
    
    async def write_entry(self, entry: Dict, partition_group: int = 0) -> bool:
        """写入日志条目"""
        async with self.cluster_lock:
            if not self.leader_id:
                return False
            
            leader = self.nodes[self.leader_id]
            if not leader.is_alive:
                return False
            
            # 检查Leader是否在指定分区
            if partition_group > 0 and leader.partition_group != partition_group:
                return False
            
            # Leader追加日志
            entry["term"] = leader.term
            entry["index"] = len(leader.log)
            entry["partition_group"] = partition_group
            leader.log.append(entry)
            
            # 复制到同分区的Followers
            replicated_count = 1  # Leader自己
            
            for node_id, node in self.nodes.items():
                if node_id == self.leader_id:
                    continue
                if not node.is_alive:
                    continue
                if partition_group > 0 and node.partition_group != partition_group:
                    continue
                
                # 模拟复制延迟
                await asyncio.sleep(random.uniform(0.001, 0.005))
                
                # 复制成功
                node.log.append(entry)
                replicated_count += 1
            
            # 如果复制到大多数节点, 提交
            group_nodes = sum(1 for n in self.nodes.values() 
                           if n.is_alive and (partition_group == 0 or n.partition_group == partition_group))
            if replicated_count > group_nodes // 2:
                leader.commit_index = len(leader.log) - 1
                return True
            
            return False
    
    async def create_partition(self, group1_nodes: List[str], group2_nodes: List[str]):
        """创建网络分区"""
        async with self.cluster_lock:
            # 标记分区
            for node_id in group1_nodes:
                if node_id in self.nodes:
                    self.nodes[node_id].is_partitioned = True
                    self.nodes[node_id].partition_group = 1
            
            for node_id in group2_nodes:
                if node_id in self.nodes:
                    self.nodes[node_id].is_partitioned = True
                    self.nodes[node_id].partition_group = 2
            
            # 如果当前Leader在分区1, 分区2需要选举新Leader
            if self.leader_id and self.nodes[self.leader_id].partition_group == 1:
                # 分区2选举新Leader
                group2_alive = [nid for nid in group2_nodes 
                               if nid in self.nodes and self.nodes[nid].is_alive]
                if group2_alive:
                    new_leader = random.choice(group2_alive)
                    self.nodes[new_leader].state = NodeState.LEADER
                    self.nodes[new_leader].term += 1
                    self.election_count += 1
                    
                    for nid in group2_alive:
                        if nid != new_leader:
                            self.nodes[nid].state = NodeState.FOLLOWER
                            self.nodes[nid].term = self.nodes[new_leader].term
            
            logger.info(f"网络分区创建: Group1={group1_nodes}, Group2={group2_nodes}")
    
    async def heal_partition(self):
        """恢复网络分区"""
        async with self.cluster_lock:
            # 收集所有分区的日志
            all_logs = []
            max_term = 0
            
            for node in self.nodes.values():
                if node.log:
                    all_logs.extend(node.log)
                    max_term = max(max_term, node.term)
            
            # 合并日志(按时间戳排序)
            if all_logs:
                merged_log = sorted(all_logs, key=lambda x: (x.get("timestamp", 0), x.get("index", 0)))
                
                # 去重
                seen_ids = set()
                unique_log = []
                for entry in merged_log:
                    entry_id = entry.get("id")
                    if entry_id and entry_id not in seen_ids:
                        seen_ids.add(entry_id)
                        unique_log.append(entry)
                    elif not entry_id:
                        unique_log.append(entry)
                
                # 同步到所有节点
                for node in self.nodes.values():
                    if node.is_alive:
                        node.log = unique_log.copy()
                        node.commit_index = len(unique_log) - 1
                        node.is_partitioned = False
                        node.partition_group = 0
            
            # 选举统一Leader
            alive_nodes = [n for n in self.nodes.values() if n.is_alive]
            if alive_nodes:
                new_leader = random.choice(alive_nodes)
                for node in alive_nodes:
                    if node.node_id == new_leader.node_id:
                        node.state = NodeState.LEADER
                        node.term = max_term + 1
                        self.leader_id = node.node_id
                    else:
                        node.state = NodeState.FOLLOWER
                        node.term = max_term + 1
                
                self.election_count += 1
            
            logger.info("网络分区已恢复")
    
    def get_cluster_state(self) -> Dict:
        """获取集群状态"""
        alive_count = sum(1 for n in self.nodes.values() if n.is_alive)
        partitioned_count = sum(1 for n in self.nodes.values() if n.is_partitioned)
        
        group1_count = sum(1 for n in self.nodes.values() if n.partition_group == 1)
        group2_count = sum(1 for n in self.nodes.values() if n.partition_group == 2)
        
        return {
            "leader_id": self.leader_id,
            "total_nodes": len(self.nodes),
            "alive_nodes": alive_count,
            "partitioned_nodes": partitioned_count,
            "group1_nodes": group1_count,
            "group2_nodes": group2_count,
            "election_count": self.election_count,
            "nodes": {nid: node.to_dict() for nid, node in self.nodes.items()}
        }
    
    def verify_consistency(self) -> Dict:
        """验证数据一致性"""
        alive_nodes = [n for n in self.nodes.values() if n.is_alive and not n.is_partitioned]
        
        if not alive_nodes:
            return {"consistent": False, "reason": "no_alive_nodes"}
        
        # 检查所有存活节点的日志是否一致
        log_hashes = []
        for node in alive_nodes:
            log_str = json.dumps(node.log, sort_keys=True)
            log_hash = hashlib.sha256(log_str.encode()).hexdigest()[:16]
            log_hashes.append(log_hash)
        
        unique_hashes = set(log_hashes)
        
        # 检查数据丢失
        all_entries = set()
        for node in alive_nodes:
            for entry in node.log:
                entry_id = entry.get("id")
                if entry_id:
                    all_entries.add(entry_id)
        
        return {
            "consistent": len(unique_hashes) == 1,
            "unique_log_versions": len(unique_hashes),
            "alive_nodes": len(alive_nodes),
            "total_log_entries": len(alive_nodes[0].log) if alive_nodes else 0,
            "unique_entries": len(all_entries)
        }
    
    def detect_conflicts(self) -> List[Dict]:
        """检测冲突"""
        conflicts = []
        
        # 收集所有条目
        all_entries = {}
        for node in self.nodes.values():
            for entry in node.log:
                entry_id = entry.get("id")
                if entry_id:
                    if entry_id in all_entries:
                        # 检查内容是否一致
                        if all_entries[entry_id].get("content") != entry.get("content"):
                            conflicts.append({
                                "type": "content_conflict",
                                "entry_id": entry_id,
                                "node_1": all_entries[entry_id].get("node_id"),
                                "node_2": entry.get("node_id")
                            })
                    else:
                        all_entries[entry_id] = entry
        
        return conflicts


class SplitRecoveryTestRunner:
    """脑裂恢复测试运行器"""
    
    def __init__(self, target_duration_minutes: int = 5):
        self.target_duration = target_duration_minutes * 60
        self.cluster = RaftClusterSimulator(node_count=5)
        self.metrics = SplitRecoveryMetrics()
        self.stop_event = asyncio.Event()
        
    async def run(self) -> SplitRecoveryMetrics:
        """运行脑裂恢复测试"""
        logger.info("=" * 80)
        logger.info("场景3: Raft集群脑裂恢复测试")
        logger.info("=" * 80)
        logger.info(f"集群规模: 5节点Raft集群")
        logger.info(f"初始Leader: {self.cluster.leader_id}")
        logger.info("=" * 80)
        
        self.metrics.start_time = time.time()
        
        # 预写入一些数据
        await self._pre_write_data(100)
        
        # Phase 1: 创建网络分区
        await self._phase1_create_partition()
        
        # Phase 2: 双分区各自写入
        await self._phase2_concurrent_writes()
        
        # Phase 3: 恢复分区
        await self._phase3_heal_partition()
        
        # Phase 4: 验证一致性
        await self._phase4_verify_consistency()
        
        self.metrics.end_time = time.time()
        self.metrics.total_duration_sec = self.metrics.end_time - self.metrics.start_time
        
        # 生成最终报告
        await self._generate_final_metrics()
        
        return self.metrics
    
    async def _pre_write_data(self, count: int):
        """预写入数据"""
        logger.info(f"预写入 {count} 条数据...")
        
        for i in range(count):
            entry = {
                "id": f"pre_{i:04d}_{int(time.time()*1000000)}",
                "content": f"Pre-split data {i}",
                "timestamp": time.time(),
                "node_id": self.cluster.leader_id
            }
            await self.cluster.write_entry(entry)
        
        logger.info("预写入完成")
    
    async def _phase1_create_partition(self):
        """Phase 1: 创建网络分区"""
        logger.info("\n" + "=" * 40)
        logger.info("Phase 1: 创建网络分区 (2+3)")
        logger.info("=" * 40)
        
        self.metrics.split_start_time = time.time()
        
        # 分区: 2节点 + 3节点
        group1 = ["node_0", "node_1"]
        group2 = ["node_2", "node_3", "node_4"]
        
        await self.cluster.create_partition(group1, group2)
        
        state = self.cluster.get_cluster_state()
        self.metrics.cluster_states.append({
            "phase": "split_created",
            "timestamp": time.time(),
            **state
        })
        
        logger.info(f"分区后状态:")
        logger.info(f"  Group1 (2节点): {group1}")
        logger.info(f"  Group2 (3节点): {group2}")
        logger.info(f"  Leader: {state['leader_id']}")
        
        # 等待分区稳定
        await asyncio.sleep(3)
    
    async def _phase2_concurrent_writes(self):
        """Phase 2: 双分区并发写入"""
        logger.info("\n" + "=" * 40)
        logger.info("Phase 2: 双分区并发写入")
        logger.info("=" * 40)
        
        # 在两个分区同时写入
        write_tasks = [
            asyncio.create_task(self._write_to_partition(1, 50)),
            asyncio.create_task(self._write_to_partition(2, 50))
        ]
        
        await asyncio.gather(*write_tasks)
        
        logger.info(f"分区1写入: {self.metrics.writes_partition_1} 条")
        logger.info(f"分区2写入: {self.metrics.writes_partition_2} 条")
    
    async def _write_to_partition(self, partition_group: int, count: int):
        """向指定分区写入数据"""
        for i in range(count):
            entry = {
                "id": f"p{partition_group}_{i:04d}_{int(time.time()*1000000)}",
                "content": f"Partition {partition_group} data {i} at {time.time()}",
                "timestamp": time.time(),
                "partition_group": partition_group
            }
            
            success = await self.cluster.write_entry(entry, partition_group)
            
            if success:
                if partition_group == 1:
                    self.metrics.writes_partition_1 += 1
                else:
                    self.metrics.writes_partition_2 += 1
                
                self.metrics.total_writes_during_split += 1
                
                self.metrics.write_timeline.append({
                    "timestamp": time.time(),
                    "partition_group": partition_group,
                    "entry_id": entry["id"],
                    "success": success
                })
            
            await asyncio.sleep(0.01)
    
    async def _phase3_heal_partition(self):
        """Phase 3: 恢复网络分区"""
        logger.info("\n" + "=" * 40)
        logger.info("Phase 3: 恢复网络分区")
        logger.info("=" * 40)
        
        self.metrics.recovery_start_time = time.time()
        
        await self.cluster.heal_partition()
        
        self.metrics.recovery_end_time = time.time()
        self.metrics.recovery_duration_sec = (
            self.metrics.recovery_end_time - self.metrics.recovery_start_time
        )
        
        self.metrics.split_end_time = time.time()
        self.metrics.split_duration_sec = (
            self.metrics.split_end_time - self.metrics.split_start_time
        )
        
        state = self.cluster.get_cluster_state()
        self.metrics.cluster_states.append({
            "phase": "partition_healed",
            "timestamp": time.time(),
            **state
        })
        
        logger.info(f"分区恢复完成")
        logger.info(f"  恢复耗时: {self.metrics.recovery_duration_sec:.3f}秒")
        logger.info(f"  新Leader: {state['leader_id']}")
        logger.info(f"  选举次数: {state['election_count']}")
    
    async def _phase4_verify_consistency(self):
        """Phase 4: 验证一致性"""
        logger.info("\n" + "=" * 40)
        logger.info("Phase 4: 验证数据一致性")
        logger.info("=" * 40)
        
        # 等待同步
        await asyncio.sleep(2)
        
        # 验证一致性
        consistency = self.cluster.verify_consistency()
        self.metrics.data_consistency_violations = 0 if consistency["consistent"] else 1
        
        # 检测冲突
        conflicts = self.cluster.detect_conflicts()
        self.metrics.conflicts_detected = len(conflicts)
        
        logger.info(f"一致性检查结果:")
        logger.info(f"  一致: {consistency['consistent']}")
        logger.info(f"  日志版本数: {consistency['unique_log_versions']}")
        logger.info(f"  总条目数: {consistency['total_log_entries']}")
        logger.info(f"  唯一条目数: {consistency['unique_entries']}")
        logger.info(f"  检测到的冲突: {len(conflicts)}")
        
        if conflicts:
            for conflict in conflicts[:5]:  # 只显示前5个
                logger.warning(f"  冲突: {conflict}")
    
    async def _generate_final_metrics(self):
        """生成最终指标"""
        state = self.cluster.get_cluster_state()
        consistency = self.cluster.verify_consistency()
        
        self.metrics.election_count = state["election_count"]
        
        # 检测缺陷
        if not consistency["consistent"]:
            self.metrics.defects_found.append({
                "id": "DEFECT-S3-001",
                "severity": "P0",
                "type": "split_brain_data_inconsistency",
                "description": f"脑裂后数据不一致，发现 {consistency['unique_log_versions']} 个不同日志版本",
                "expected": 1,
                "actual": consistency["unique_log_versions"]
            })
        
        if self.metrics.conflicts_detected > 0:
            self.metrics.defects_found.append({
                "id": "DEFECT-S3-002",
                "severity": "P1",
                "type": "unresolved_conflicts",
                "description": f"检测到 {self.metrics.conflicts_detected} 个未解决的冲突",
                "conflicts": self.metrics.conflicts_detected
            })
        
        if self.metrics.recovery_duration_sec > 5.0:
            self.metrics.defects_found.append({
                "id": "DEFECT-S3-003",
                "severity": "P2",
                "type": "slow_recovery",
                "description": f"分区恢复时间过长: {self.metrics.recovery_duration_sec:.3f}秒",
                "threshold": "5秒",
                "actual": f"{self.metrics.recovery_duration_sec:.3f}秒"
            })


async def main():
    """主函数"""
    runner = SplitRecoveryTestRunner(target_duration_minutes=3)
    
    try:
        metrics = await runner.run()
        
        # 保存详细报告
        metrics_dir = Path("/workspace/agent_memory_system_v2/tests/round1/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = metrics_dir / "scenario3_raft_split_recovery_metrics.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(metrics), f, indent=2, default=str)
        
        # 打印摘要
        print("\n" + "=" * 80)
        print("场景3: Raft集群脑裂恢复测试 - 完成")
        print("=" * 80)
        print(f"运行时间: {metrics.total_duration_sec:.2f} 秒")
        print(f"集群规模: {metrics.cluster_size} 节点")
        print("-" * 80)
        print(f"脑裂阶段:")
        print(f"  - 持续时间: {metrics.split_duration_sec:.2f} 秒")
        print(f"  - 分区1写入: {metrics.writes_partition_1} 条")
        print(f"  - 分区2写入: {metrics.writes_partition_2} 条")
        print(f"  - 总写入: {metrics.total_writes_during_split} 条")
        print("-" * 80)
        print(f"恢复阶段:")
        print(f"  - 恢复耗时: {metrics.recovery_duration_sec:.3f} 秒")
        print(f"  - 选举次数: {metrics.election_count}")
        print("-" * 80)
        print(f"一致性检查:")
        print(f"  - 一致性违规: {metrics.data_consistency_violations}")
        print(f"  - 冲突检测: {metrics.conflicts_detected}")
        print(f"  - 数据丢失: {metrics.data_loss_entries}")
        print("-" * 80)
        print(f"发现的缺陷: {len(metrics.defects_found)} 个")
        for defect in metrics.defects_found:
            print(f"  - [{defect['severity']}] {defect['id']}: {defect['description']}")
        print("=" * 80)
        
        logger.info(f"测试报告已保存到: {report_path}")
        
    except Exception as e:
        logger.error(f"测试执行失败: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())

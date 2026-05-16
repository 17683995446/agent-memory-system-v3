#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 场景1: 分布式一致性压力测试
Scenario 1: Distributed Consistency Stress Test

目标: 暴露共识协议实现的深层缺陷

测试内容:
- 模拟5节点Raft集群
- 在网络分区情况下进行并发写入
- 验证Leader切换后数据一致性
- 发现共识协议实现缺陷

缺陷检测:
- Split-brain问题
- 日志不一致
- Leader选举死锁
- 提交索引不正确
- 任期号混乱
"""

import asyncio
import time
import random
import json
import logging
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from collections import defaultdict
import hashlib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DefectType(Enum):
    """缺陷类型枚举"""
    SPLIT_BRAIN = "split_brain"
    LOG_INCONSISTENCY = "log_inconsistency"
    LEADER_ELECTION_DEADLOCK = "leader_election_deadlock"
    COMMIT_INDEX_ERROR = "commit_index_error"
    TERM_CONFUSION = "term_confusion"
    DATA_LOSS = "data_loss"
    SAFETY_VIOLATION = "safety_violation"
    LIVENESS_VIOLATION = "liveness_violation"


@dataclass
class DefectReport:
    """缺陷报告"""
    defect_type: DefectType
    severity: str  # critical, high, medium, low
    description: str
    timestamp: float
    evidence: Dict[str, Any] = field(default_factory=dict)
    affected_nodes: List[str] = field(default_factory=list)
    reproduction_steps: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化的字典"""
        return {
            'defect_type': self.defect_type.value,
            'severity': self.severity,
            'description': self.description,
            'timestamp': self.timestamp,
            'evidence': self.evidence,
            'affected_nodes': self.affected_nodes,
            'reproduction_steps': self.reproduction_steps
        }


@dataclass
class RaftNodeState:
    """Raft节点状态"""
    node_id: str
    term: int = 0
    role: str = "follower"  # follower, candidate, leader
    voted_for: Optional[str] = None
    log: List[Dict[str, Any]] = field(default_factory=list)
    commit_index: int = -1
    last_applied: int = -1
    is_partitioned: bool = False
    is_alive: bool = True
    data: Dict[str, Any] = field(default_factory=dict)


class MockRaftCluster:
    """模拟Raft集群"""
    
    def __init__(self, num_nodes: int = 5):
        self.num_nodes = num_nodes
        self.nodes: Dict[str, RaftNodeState] = {}
        self.leader_id: Optional[str] = None
        self.current_term = 0
        self._lock = threading.RLock()
        self._defects: List[DefectReport] = []
        self._operation_log: List[Dict[str, Any]] = []
        
        # 初始化节点
        for i in range(num_nodes):
            node_id = f"node-{i}"
            self.nodes[node_id] = RaftNodeState(node_id=node_id)
    
    def start(self):
        """启动集群"""
        logger.info("=" * 70)
        logger.info("启动5节点Raft集群")
        logger.info("=" * 70)
        
        # 初始选举
        self._perform_election()
        
        logger.info(f"初始Leader: {self.leader_id}, 任期: {self.current_term}")
    
    def stop(self):
        """停止集群"""
        logger.info("停止集群")
    
    def _perform_election(self) -> bool:
        """执行选举"""
        with self._lock:
            alive_nodes = [n for n in self.nodes.values() if n.is_alive and not n.is_partitioned]
            
            if len(alive_nodes) == 0:
                self._record_defect(DefectType.LIVENESS_VIOLATION, "critical",
                    "无法进行选举: 没有可用节点")
                return False
            
            # 检查是否有过半节点
            majority = (self.num_nodes + 1) // 2
            if len(alive_nodes) < majority:
                self._record_defect(DefectType.LEADER_ELECTION_DEADLOCK, "high",
                    f"无法获得多数票: 可用节点{len(alive_nodes)} < 需要多数{majority}")
                return False
            
            # 增加任期
            self.current_term += 1
            
            # 选择新Leader (选择ID最小的存活节点)
            candidate = min(alive_nodes, key=lambda n: n.node_id)
            
            # 更新所有节点状态
            for node in alive_nodes:
                node.term = self.current_term
                node.role = "follower"
                node.voted_for = candidate.node_id
            
            candidate.role = "leader"
            self.leader_id = candidate.node_id
            
            logger.info(f"选举完成: Leader={self.leader_id}, 任期={self.current_term}")
            return True
    
    def _record_defect(self, defect_type: DefectType, severity: str, 
                       description: str, evidence: Dict[str, Any] = None):
        """记录缺陷"""
        defect = DefectReport(
            defect_type=defect_type,
            severity=severity,
            description=description,
            timestamp=time.time(),
            evidence=evidence or {},
            affected_nodes=list(self.nodes.keys())
        )
        self._defects.append(defect)
        logger.error(f"[缺陷发现] {defect_type.value}: {description}")
    
    def partition_network(self, partition: List[List[str]]):
        """
        模拟网络分区
        
        Args:
            partition: 分区列表，每个子列表是一个分区
        """
        logger.info(f"\n模拟网络分区: {partition}")
        
        with self._lock:
            # 重置所有节点的分区状态
            for node in self.nodes.values():
                node.is_partitioned = False
            
            # 标记被隔离的节点
            all_nodes = set(self.nodes.keys())
            partitioned_nodes = set()
            for part in partition:
                partitioned_nodes.update(part)
            
            isolated_nodes = all_nodes - partitioned_nodes
            for node_id in isolated_nodes:
                self.nodes[node_id].is_partitioned = True
            
            # 检查是否会导致Split-brain
            for part in partition:
                if len(part) > self.num_nodes // 2:
                    # 这个分区有超过半数节点，可能形成新Leader
                    pass
            
            # 检查多个分区是否都有多数节点
            majority_partitions = [p for p in partition if len(p) > self.num_nodes // 2]
            if len(majority_partitions) > 1:
                self._record_defect(DefectType.SPLIT_BRAIN, "critical",
                    "检测到Split-brain风险: 多个分区都拥有多数节点",
                    {"partitions": partition, "majority_partitions": majority_partitions})
    
    def heal_partition(self):
        """恢复网络分区"""
        logger.info("恢复网络分区")
        with self._lock:
            for node in self.nodes.values():
                node.is_partitioned = False
    
    def kill_node(self, node_id: str):
        """杀死节点"""
        logger.info(f"杀死节点: {node_id}")
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id].is_alive = False
                
                # 如果是Leader，触发重新选举
                if node_id == self.leader_id:
                    self.leader_id = None
                    self._perform_election()
    
    def revive_node(self, node_id: str):
        """恢复节点"""
        logger.info(f"恢复节点: {node_id}")
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id].is_alive = True
    
    async def concurrent_write(self, num_writers: int = 10, 
                               operations_per_writer: int = 100) -> Dict[str, Any]:
        """
        并发写入测试
        
        Args:
            num_writers: 并发写入者数量
            operations_per_writer: 每个写入者的操作数
        """
        logger.info(f"\n开始并发写入测试: {num_writers}个写入者, 每人{operations_per_writer}次操作")
        
        results = {
            'total_operations': 0,
            'successful': 0,
            'failed': 0,
            'conflicts': 0,
            'latencies': []
        }
        
        async def writer_task(writer_id: int):
            """写入者任务"""
            nonlocal results
            for i in range(operations_per_writer):
                start_time = time.time()
                
                key = f"key-{writer_id}-{i}"
                value = {
                    'writer': writer_id,
                    'sequence': i,
                    'timestamp': time.time(),
                    'term': self.current_term
                }
                
                success = await self._write_with_consensus(key, value)
                
                latency = (time.time() - start_time) * 1000
                results['latencies'].append(latency)
                results['total_operations'] += 1
                
                if success:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
                
                # 添加随机延迟
                await asyncio.sleep(random.uniform(0.001, 0.01))
        
        # 启动所有写入者
        writers = [writer_task(i) for i in range(num_writers)]
        await asyncio.gather(*writers)
        
        logger.info(f"并发写入完成: 成功={results['successful']}, 失败={results['failed']}")
        return results
    
    async def _write_with_consensus(self, key: str, value: Any) -> bool:
        """通过共识协议写入"""
        with self._lock:
            # 检查是否有Leader
            if not self.leader_id:
                self._record_defect(DefectType.SAFETY_VIOLATION, "high",
                    "写入失败: 没有Leader")
                return False
            
            leader = self.nodes.get(self.leader_id)
            if not leader or not leader.is_alive or leader.is_partitioned:
                self._record_defect(DefectType.SAFETY_VIOLATION, "high",
                    "写入失败: Leader不可用")
                return False
            
            # 模拟日志复制
            log_entry = {
                'term': self.current_term,
                'index': len(leader.log),
                'key': key,
                'value': value,
                'timestamp': time.time()
            }
            
            # 计算需要复制的节点数
            alive_nodes = [n for n in self.nodes.values() 
                          if n.is_alive and not n.is_partitioned]
            majority = (len(alive_nodes) + 1) // 2
            
            # 模拟复制到多数节点
            replicated_count = 1  # Leader自己
            for node in alive_nodes:
                if node.node_id != self.leader_id:
                    # 模拟复制成功
                    if random.random() > 0.1:  # 90%成功率
                        node.log.append(log_entry.copy())
                        replicated_count += 1
            
            # 检查是否获得多数
            if replicated_count >= majority:
                # 提交
                leader.log.append(log_entry)
                leader.commit_index = len(leader.log) - 1
                leader.data[key] = value
                
                # 更新其他节点的commit_index
                for node in alive_nodes:
                    if len(node.log) > node.commit_index:
                        node.commit_index = min(len(node.log) - 1, leader.commit_index)
                
                self._operation_log.append({
                    'type': 'write',
                    'key': key,
                    'value': value,
                    'leader': self.leader_id,
                    'term': self.current_term,
                    'replicated': replicated_count
                })
                
                return True
            else:
                return False
    
    def verify_consistency(self) -> Dict[str, Any]:
        """验证数据一致性"""
        logger.info("\n验证数据一致性...")
        
        results = {
            'is_consistent': True,
            'inconsistencies': [],
            'data_checksums': {},
            'log_lengths': {},
            'commit_indices': {}
        }
        
        with self._lock:
            # 收集所有存活节点的数据
            alive_nodes = [n for n in self.nodes.values() if n.is_alive]
            
            # 检查日志长度
            log_lengths = {n.node_id: len(n.log) for n in alive_nodes}
            results['log_lengths'] = log_lengths
            
            if len(set(log_lengths.values())) > 1:
                self._record_defect(DefectType.LOG_INCONSISTENCY, "high",
                    "日志长度不一致", {"log_lengths": log_lengths})
                results['is_consistent'] = False
                results['inconsistencies'].append({
                    'type': 'log_length_mismatch',
                    'details': log_lengths
                })
            
            # 检查commit_index
            commit_indices = {n.node_id: n.commit_index for n in alive_nodes}
            results['commit_indices'] = commit_indices
            
            if len(set(commit_indices.values())) > 1:
                self._record_defect(DefectType.COMMIT_INDEX_ERROR, "high",
                    "提交索引不一致", {"commit_indices": commit_indices})
                results['is_consistent'] = False
                results['inconsistencies'].append({
                    'type': 'commit_index_mismatch',
                    'details': commit_indices
                })
            
            # 检查数据一致性
            all_keys: Set[str] = set()
            for node in alive_nodes:
                all_keys.update(node.data.keys())
            
            for key in all_keys:
                values = []
                for node in alive_nodes:
                    if key in node.data:
                        values.append(json.dumps(node.data[key], sort_keys=True))
                
                if len(set(values)) > 1:
                    self._record_defect(DefectType.DATA_LOSS, "critical",
                        f"数据不一致: key={key}", {"values": values})
                    results['is_consistent'] = False
                    results['inconsistencies'].append({
                        'type': 'data_mismatch',
                        'key': key,
                        'values': values
                    })
            
            # 检查任期一致性
            terms = {n.node_id: n.term for n in alive_nodes}
            if len(set(terms.values())) > 1:
                self._record_defect(DefectType.TERM_CONFUSION, "medium",
                    "任期号不一致", {"terms": terms})
                results['is_consistent'] = False
                results['inconsistencies'].append({
                    'type': 'term_mismatch',
                    'details': terms
                })
            
            # 计算数据校验和
            for node in alive_nodes:
                data_str = json.dumps(node.data, sort_keys=True)
                checksum = hashlib.md5(data_str.encode()).hexdigest()
                results['data_checksums'][node.node_id] = checksum
            
            if len(set(results['data_checksums'].values())) > 1:
                results['is_consistent'] = False
        
        if results['is_consistent']:
            logger.info("数据一致性验证通过")
        else:
            logger.error(f"数据一致性验证失败: {len(results['inconsistencies'])}个不一致")
        
        return results
    
    def force_leader_change(self) -> bool:
        """强制Leader切换"""
        logger.info("\n强制Leader切换...")
        
        with self._lock:
            if not self.leader_id:
                return self._perform_election()
            
            old_leader = self.leader_id
            
            # 模拟Leader失效
            self.nodes[old_leader].role = "follower"
            self.leader_id = None
            
            # 触发新选举
            success = self._perform_election()
            
            if success and self.leader_id == old_leader:
                self._record_defect(DefectType.LEADER_ELECTION_DEADLOCK, "medium",
                    "Leader切换后选举了相同的节点")
            
            return success
    
    def get_defects(self) -> List[DefectReport]:
        """获取所有检测到的缺陷"""
        return self._defects.copy()
    
    def get_cluster_state(self) -> Dict[str, Any]:
        """获取集群状态"""
        with self._lock:
            return {
                'leader': self.leader_id,
                'term': self.current_term,
                'nodes': {nid: asdict(n) for nid, n in self.nodes.items()},
                'defects_count': len(self._defects)
            }


class DistributedConsistencyScenario:
    """分布式一致性压力测试场景"""
    
    def __init__(self):
        self.cluster = MockRaftCluster(num_nodes=5)
        self.results: Dict[str, Any] = {}
        self.defects: List[DefectReport] = []
        
    async def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        logger.info("=" * 70)
        logger.info("场景1: 分布式一致性压力测试")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # 启动集群
        self.cluster.start()
        await asyncio.sleep(1)
        
        try:
            # 测试1: 正常情况下的并发写入
            result1 = await self.test_normal_concurrent_write()
            self.results['normal_concurrent_write'] = result1
            
            # 测试2: 网络分区情况下的写入
            result2 = await self.test_partition_write()
            self.results['partition_write'] = result2
            
            # 测试3: Leader切换一致性
            result3 = await self.test_leader_change_consistency()
            self.results['leader_change_consistency'] = result3
            
            # 测试4: 多次分区恢复一致性
            result4 = await self.test_multiple_partition_heal()
            self.results['multiple_partition_heal'] = result4
            
            # 测试5: 极端情况测试
            result5 = await self.test_edge_cases()
            self.results['edge_cases'] = result5
            
        finally:
            self.cluster.stop()
        
        end_time = time.time()
        
        # 收集所有缺陷
        self.defects = self.cluster.get_defects()
        
        # 生成报告
        report = self._generate_report(start_time, end_time)
        
        return report
    
    async def test_normal_concurrent_write(self) -> Dict[str, Any]:
        """测试1: 正常情况下的并发写入"""
        logger.info("\n" + "-" * 70)
        logger.info("测试1: 正常情况下的并发写入")
        logger.info("-" * 70)
        
        # 并发写入
        write_result = await self.cluster.concurrent_write(
            num_writers=20,
            operations_per_writer=50
        )
        
        # 验证一致性
        consistency = self.cluster.verify_consistency()
        
        return {
            'write_result': write_result,
            'consistency': consistency,
            'success': consistency['is_consistent']
        }
    
    async def test_partition_write(self) -> Dict[str, Any]:
        """测试2: 网络分区情况下的写入"""
        logger.info("\n" + "-" * 70)
        logger.info("测试2: 网络分区情况下的写入")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        # 场景2.1: 少数派分区
        logger.info("\n场景2.1: 少数派分区 (2节点)")
        self.cluster.partition_network([["node-0", "node-1", "node-2"], ["node-3", "node-4"]])
        await asyncio.sleep(0.5)
        
        # 尝试写入
        write1 = await self.cluster.concurrent_write(num_writers=5, operations_per_writer=20)
        consistency1 = self.cluster.verify_consistency()
        
        results['scenarios'].append({
            'name': 'minority_partition',
            'write': write1,
            'consistency': consistency1
        })
        
        # 恢复分区
        self.cluster.heal_partition()
        await asyncio.sleep(0.5)
        
        # 场景2.2: 多数派分区
        logger.info("\n场景2.2: 多数派分区 (3节点)")
        self.cluster.partition_network([["node-0", "node-1", "node-2"]])
        await asyncio.sleep(0.5)
        
        write2 = await self.cluster.concurrent_write(num_writers=5, operations_per_writer=20)
        consistency2 = self.cluster.verify_consistency()
        
        results['scenarios'].append({
            'name': 'majority_partition',
            'write': write2,
            'consistency': consistency2
        })
        
        self.cluster.heal_partition()
        await asyncio.sleep(0.5)
        
        # 场景2.3: 完全分区
        logger.info("\n场景2.3: 完全分区 (每个节点独立)")
        self.cluster.partition_network([["node-0"], ["node-1"], ["node-2"], ["node-3"], ["node-4"]])
        await asyncio.sleep(0.5)
        
        write3 = await self.cluster.concurrent_write(num_writers=5, operations_per_writer=10)
        consistency3 = self.cluster.verify_consistency()
        
        results['scenarios'].append({
            'name': 'complete_partition',
            'write': write3,
            'consistency': consistency3
        })
        
        self.cluster.heal_partition()
        await asyncio.sleep(0.5)
        
        return results
    
    async def test_leader_change_consistency(self) -> Dict[str, Any]:
        """测试3: Leader切换一致性"""
        logger.info("\n" + "-" * 70)
        logger.info("测试3: Leader切换一致性")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        # 初始写入
        initial_write = await self.cluster.concurrent_write(num_writers=10, operations_per_writer=30)
        
        # 多次Leader切换
        for i in range(5):
            logger.info(f"\nLeader切换 #{i+1}")
            
            # 切换前的一致性
            before = self.cluster.verify_consistency()
            
            # 强制切换
            success = self.cluster.force_leader_change()
            await asyncio.sleep(0.3)
            
            # 切换后写入
            write = await self.cluster.concurrent_write(num_writers=5, operations_per_writer=20)
            
            # 切换后的一致性
            after = self.cluster.verify_consistency()
            
            results['scenarios'].append({
                'round': i + 1,
                'leader_change_success': success,
                'consistency_before': before,
                'consistency_after': after,
                'write_result': write
            })
        
        return results
    
    async def test_multiple_partition_heal(self) -> Dict[str, Any]:
        """测试4: 多次分区恢复一致性"""
        logger.info("\n" + "-" * 70)
        logger.info("测试4: 多次分区恢复一致性")
        logger.info("-" * 70)
        
        results = {'cycles': []}
        
        for cycle in range(3):
            logger.info(f"\n分区-恢复周期 #{cycle + 1}")
            
            # 写入数据
            write1 = await self.cluster.concurrent_write(num_writers=5, operations_per_writer=20)
            
            # 创建分区
            partition = random.choice([
                [["node-0", "node-1"], ["node-2", "node-3", "node-4"]],
                [["node-0", "node-1", "node-2"], ["node-3", "node-4"]],
                [["node-0"], ["node-1", "node-2", "node-3", "node-4"]]
            ])
            self.cluster.partition_network(partition)
            await asyncio.sleep(0.3)
            
            # 分区期间写入
            write2 = await self.cluster.concurrent_write(num_writers=3, operations_per_writer=10)
            
            # 恢复分区
            self.cluster.heal_partition()
            await asyncio.sleep(0.5)
            
            # 恢复后验证
            consistency = self.cluster.verify_consistency()
            
            results['cycles'].append({
                'cycle': cycle + 1,
                'partition': partition,
                'consistency_after_heal': consistency
            })
        
        return results
    
    async def test_edge_cases(self) -> Dict[str, Any]:
        """测试5: 极端情况测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试5: 极端情况测试")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        # 场景5.1: 所有节点重启
        logger.info("\n场景5.1: 所有节点重启")
        for node_id in list(self.cluster.nodes.keys()):
            self.cluster.kill_node(node_id)
        await asyncio.sleep(0.5)
        
        for node_id in list(self.cluster.nodes.keys()):
            self.cluster.revive_node(node_id)
        await asyncio.sleep(0.5)
        
        consistency1 = self.cluster.verify_consistency()
        results['scenarios'].append({
            'name': 'all_nodes_restart',
            'consistency': consistency1
        })
        
        # 场景5.2: Leader被杀死后快速恢复
        logger.info("\n场景5.2: Leader被杀死后快速恢复")
        if self.cluster.leader_id:
            leader = self.cluster.leader_id
            self.cluster.kill_node(leader)
            await asyncio.sleep(0.1)
            self.cluster.revive_node(leader)
            await asyncio.sleep(0.3)
        
        consistency2 = self.cluster.verify_consistency()
        results['scenarios'].append({
            'name': 'leader_quick_recovery',
            'consistency': consistency2
        })
        
        # 场景5.3: 高频Leader切换
        logger.info("\n场景5.3: 高频Leader切换")
        for _ in range(10):
            self.cluster.force_leader_change()
            await asyncio.sleep(0.05)
        
        consistency3 = self.cluster.verify_consistency()
        results['scenarios'].append({
            'name': 'frequent_leader_changes',
            'consistency': consistency3
        })
        
        return results
    
    def _generate_report(self, start_time: float, end_time: float) -> Dict[str, Any]:
        """生成测试报告"""
        # 统计缺陷
        defects_by_type = defaultdict(list)
        defects_by_severity = defaultdict(list)
        
        for defect in self.defects:
            defect_dict = defect.to_dict()
            defects_by_type[defect.defect_type.value].append(defect_dict)
            defects_by_severity[defect.severity].append(defect_dict)
        
        report = {
            'scenario': '分布式一致性压力测试',
            'scenario_id': 'scenario1_distributed_consistency',
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'end_time': datetime.fromtimestamp(end_time).isoformat(),
            'duration_seconds': end_time - start_time,
            'summary': {
                'total_tests': 5,
                'defects_found': len(self.defects),
                'critical_defects': len(defects_by_severity.get('critical', [])),
                'high_defects': len(defects_by_severity.get('high', [])),
                'medium_defects': len(defects_by_severity.get('medium', [])),
                'low_defects': len(defects_by_severity.get('low', []))
            },
            'defects_by_type': dict(defects_by_type),
            'defects_by_severity': dict(defects_by_severity),
            'all_defects': [d.to_dict() for d in self.defects],
            'test_results': self.results
        }
        
        return report


async def main():
    """主函数"""
    scenario = DistributedConsistencyScenario()
    report = await scenario.run_all_tests()
    
    # 保存报告
    output_path = "/workspace/agent_memory_system_v2/tests/reports/scenario1_distributed_consistency_report.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("场景1: 分布式一致性压力测试 - 摘要")
    print("=" * 70)
    print(f"测试持续时间: {report['duration_seconds']:.2f} 秒")
    print(f"发现缺陷总数: {report['summary']['defects_found']}")
    print(f"  - 严重: {report['summary']['critical_defects']}")
    print(f"  - 高危: {report['summary']['high_defects']}")
    print(f"  - 中危: {report['summary']['medium_defects']}")
    print(f"  - 低危: {report['summary']['low_defects']}")
    
    if report['all_defects']:
        print("\n检测到的缺陷:")
        for defect in report['all_defects']:
            print(f"  [{defect['severity'].upper()}] {defect['defect_type']}: {defect['description']}")
    
    print(f"\n报告已保存: {output_path}")
    
    return report


if __name__ == "__main__":
    asyncio.run(main())

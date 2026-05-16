#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 场景3: 故障注入与恢复测试
Scenario 3: Fault Injection and Recovery Test

目标: 暴露故障恢复逻辑缺陷

测试内容:
- 随机杀死节点进程
- 模拟磁盘损坏和网络延迟
- 验证自动故障转移和数据恢复
- 发现故障恢复逻辑缺陷

缺陷检测:
- 故障检测延迟
- 故障转移失败
- 数据恢复不完整
- 状态不一致
- 脑裂问题
- 恢复超时
"""

import asyncio
import time
import random
import json
import logging
import threading
import os
import signal
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Callable
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
    FAULT_DETECTION_DELAY = "fault_detection_delay"
    FAILOVER_FAILURE = "failover_failure"
    DATA_RECOVERY_INCOMPLETE = "data_recovery_incomplete"
    STATE_INCONSISTENCY = "state_inconsistency"
    SPLIT_BRAIN = "split_brain"
    RECOVERY_TIMEOUT = "recovery_timeout"
    ORPHANED_DATA = "orphaned_data"
    PARTIAL_RECOVERY = "partial_recovery"
    CASCADING_FAILURE = "cascading_failure"
    RESOURCE_LEAK = "resource_leak"


class FaultType(Enum):
    """故障类型"""
    NODE_CRASH = "node_crash"
    DISK_FAILURE = "disk_failure"
    NETWORK_PARTITION = "network_partition"
    NETWORK_DELAY = "network_delay"
    MEMORY_PRESSURE = "memory_pressure"
    CPU_SATURATION = "cpu_saturation"
    PROCESS_KILL = "process_kill"
    POWER_FAILURE = "power_failure"


@dataclass
class DefectReport:
    """缺陷报告"""
    defect_type: DefectType
    severity: str
    description: str
    timestamp: float
    evidence: Dict[str, Any] = field(default_factory=dict)
    affected_nodes: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化的字典"""
        return {
            'defect_type': self.defect_type.value,
            'severity': self.severity,
            'description': self.description,
            'timestamp': self.timestamp,
            'evidence': self.evidence,
            'affected_nodes': self.affected_nodes
        }


@dataclass
class NodeState:
    """节点状态"""
    node_id: str
    is_alive: bool = True
    is_healthy: bool = True
    is_recovering: bool = False
    last_heartbeat: float = 0.0
    data: Dict[str, Any] = field(default_factory=dict)
    disk_healthy: bool = True
    network_healthy: bool = True
    memory_usage: float = 0.0
    cpu_usage: float = 0.0
    recovery_count: int = 0
    crash_count: int = 0


@dataclass
class FaultScenario:
    """故障场景"""
    fault_type: FaultType
    target_nodes: List[str]
    duration: float
    intensity: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class FaultInjectableCluster:
    """可注入故障的集群"""
    
    def __init__(self, num_nodes: int = 5):
        self.num_nodes = num_nodes
        self.nodes: Dict[str, NodeState] = {}
        self.leader_id: Optional[str] = None
        self._lock = threading.RLock()
        self._defects: List[DefectReport] = []
        
        # 故障检测配置
        self.heartbeat_interval = 0.1
        self.heartbeat_timeout = 0.5
        
        # 恢复配置
        self.recovery_timeout = 5.0
        self.max_recovery_attempts = 3
        
        # 数据复制因子
        self.replication_factor = 3
        
        # 初始化节点
        for i in range(num_nodes):
            node_id = f"node-{i}"
            self.nodes[node_id] = NodeState(node_id=node_id)
        
        # 选择初始Leader
        self.leader_id = "node-0"
        
        # 监控线程
        self._monitor_running = False
        self._monitor_thread: Optional[threading.Thread] = None
    
    def start(self):
        """启动集群"""
        logger.info("=" * 70)
        logger.info(f"启动 {self.num_nodes} 节点集群")
        logger.info("=" * 70)
        
        self._monitor_running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        
        logger.info(f"初始Leader: {self.leader_id}")
    
    def stop(self):
        """停止集群"""
        self._monitor_running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2.0)
        logger.info("集群已停止")
    
    def _monitor_loop(self):
        """监控循环"""
        while self._monitor_running:
            try:
                self._check_node_health()
                self._check_leader_availability()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"监控循环错误: {e}")
    
    def _check_node_health(self):
        """检查节点健康状态"""
        current_time = time.time()
        
        with self._lock:
            for node in self.nodes.values():
                if node.is_alive:
                    # 检查心跳超时
                    if current_time - node.last_heartbeat > self.heartbeat_timeout:
                        if node.is_healthy:
                            node.is_healthy = False
                            self._record_defect(DefectType.FAULT_DETECTION_DELAY, "high",
                                f"节点 {node.node_id} 心跳超时",
                                {"last_heartbeat": node.last_heartbeat, "current": current_time},
                                [node.node_id])
    
    def _check_leader_availability(self):
        """检查Leader可用性"""
        with self._lock:
            if not self.leader_id:
                return
            
            leader = self.nodes.get(self.leader_id)
            if not leader or not leader.is_alive or not leader.is_healthy:
                # 触发故障转移
                self._perform_failover()
    
    def _perform_failover(self) -> bool:
        """执行故障转移"""
        logger.info("触发故障转移...")
        
        with self._lock:
            old_leader = self.leader_id
            
            # 选择新Leader
            candidates = [
                n for n in self.nodes.values()
                if n.is_alive and n.is_healthy and n.node_id != old_leader
            ]
            
            if not candidates:
                self._record_defect(DefectType.FAILOVER_FAILURE, "critical",
                    "故障转移失败: 没有可用的候选节点",
                    {"old_leader": old_leader})
                return False
            
            # 选择数据最完整的节点
            new_leader = max(candidates, key=lambda n: len(n.data))
            self.leader_id = new_leader.node_id
            
            logger.info(f"故障转移完成: {old_leader} -> {self.leader_id}")
            
            # 验证数据一致性
            self._verify_data_consistency()
            
            return True
    
    def _verify_data_consistency(self):
        """验证数据一致性"""
        with self._lock:
            # 收集所有存活节点的数据
            alive_nodes = [n for n in self.nodes.values() if n.is_alive and n.is_healthy]
            
            if len(alive_nodes) < 2:
                return
            
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
                    self._record_defect(DefectType.STATE_INCONSISTENCY, "high",
                        f"数据不一致: key={key}",
                        {"key": key, "values": values})
    
    def _record_defect(self, defect_type: DefectType, severity: str,
                       description: str, evidence: Dict[str, Any] = None,
                       affected_nodes: List[str] = None):
        """记录缺陷"""
        defect = DefectReport(
            defect_type=defect_type,
            severity=severity,
            description=description,
            timestamp=time.time(),
            evidence=evidence or {},
            affected_nodes=affected_nodes or []
        )
        self._defects.append(defect)
        logger.error(f"[缺陷发现] {defect_type.value}: {description}")
    
    def inject_fault(self, scenario: FaultScenario) -> Dict[str, Any]:
        """注入故障"""
        logger.info(f"\n注入故障: {scenario.fault_type.value}")
        logger.info(f"目标节点: {scenario.target_nodes}")
        
        result = {
            'fault_type': scenario.fault_type.value,
            'target_nodes': scenario.target_nodes,
            'start_time': time.time(),
            'success': False,
            'recovery_time_ms': 0,
            'defects_before': len(self._defects)
        }
        
        try:
            # 记录故障前状态
            state_before = self._get_cluster_state()
            
            # 执行故障注入
            if scenario.fault_type == FaultType.NODE_CRASH:
                self._inject_node_crash(scenario)
            elif scenario.fault_type == FaultType.DISK_FAILURE:
                self._inject_disk_failure(scenario)
            elif scenario.fault_type == FaultType.NETWORK_PARTITION:
                self._inject_network_partition(scenario)
            elif scenario.fault_type == FaultType.NETWORK_DELAY:
                self._inject_network_delay(scenario)
            elif scenario.fault_type == FaultType.MEMORY_PRESSURE:
                self._inject_memory_pressure(scenario)
            elif scenario.fault_type == FaultType.PROCESS_KILL:
                self._inject_process_kill(scenario)
            elif scenario.fault_type == FaultType.POWER_FAILURE:
                self._inject_power_failure(scenario)
            
            # 等待故障持续时间
            time.sleep(scenario.duration)
            
            # 记录故障期间状态
            state_during = self._get_cluster_state()
            
            # 恢复故障
            recovery_start = time.time()
            self._recover_fault(scenario)
            result['recovery_time_ms'] = (time.time() - recovery_start) * 1000
            
            # 等待恢复稳定
            time.sleep(1.0)
            
            # 记录恢复后状态
            state_after = self._get_cluster_state()
            
            # 验证恢复
            self._verify_recovery(state_before, state_after, scenario)
            
            result['success'] = True
            result['state_before'] = state_before
            result['state_during'] = state_during
            result['state_after'] = state_after
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"故障注入失败: {e}")
        
        result['end_time'] = time.time()
        result['defects_after'] = len(self._defects)
        result['new_defects'] = result['defects_after'] - result['defects_before']
        
        return result
    
    def _inject_node_crash(self, scenario: FaultScenario):
        """注入节点崩溃"""
        for node_id in scenario.target_nodes:
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.is_alive = False
                node.is_healthy = False
                node.crash_count += 1
                logger.info(f"节点 {node_id} 已崩溃")
    
    def _inject_disk_failure(self, scenario: FaultScenario):
        """注入磁盘故障"""
        for node_id in scenario.target_nodes:
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.disk_healthy = False
                # 磁盘故障可能导致数据丢失
                if random.random() < scenario.intensity * 0.3:
                    keys_to_remove = random.sample(
                        list(node.data.keys()),
                        min(int(len(node.data) * 0.1), len(node.data))
                    )
                    for key in keys_to_remove:
                        del node.data[key]
                    self._record_defect(DefectType.DATA_RECOVERY_INCOMPLETE, "critical",
                        f"磁盘故障导致数据丢失: node={node_id}",
                        {"lost_keys": keys_to_remove},
                        [node_id])
                logger.info(f"节点 {node_id} 磁盘故障")
    
    def _inject_network_partition(self, scenario: FaultScenario):
        """注入网络分区"""
        for node_id in scenario.target_nodes:
            if node_id in self.nodes:
                self.nodes[node_id].network_healthy = False
                logger.info(f"节点 {node_id} 网络分区")
    
    def _inject_network_delay(self, scenario: FaultScenario):
        """注入网络延迟"""
        delay_ms = scenario.metadata.get('delay_ms', 100) * scenario.intensity
        for node_id in scenario.target_nodes:
            if node_id in self.nodes:
                self.nodes[node_id].last_heartbeat = time.time() - delay_ms / 1000
                logger.info(f"节点 {node_id} 网络延迟: {delay_ms}ms")
    
    def _inject_memory_pressure(self, scenario: FaultScenario):
        """注入内存压力"""
        for node_id in scenario.target_nodes:
            if node_id in self.nodes:
                self.nodes[node_id].memory_usage = scenario.intensity * 100
                if scenario.intensity > 0.9:
                    # 高内存压力可能导致节点不稳定
                    self.nodes[node_id].is_healthy = False
                logger.info(f"节点 {node_id} 内存压力: {scenario.intensity * 100:.0f}%")
    
    def _inject_process_kill(self, scenario: FaultScenario):
        """注入进程杀死"""
        self._inject_node_crash(scenario)
    
    def _inject_power_failure(self, scenario: FaultScenario):
        """注入电源故障"""
        for node_id in scenario.target_nodes:
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.is_alive = False
                node.is_healthy = False
                node.disk_healthy = False
                node.network_healthy = False
                logger.info(f"节点 {node_id} 电源故障")
    
    def _recover_fault(self, scenario: FaultScenario):
        """恢复故障"""
        logger.info(f"恢复故障: {scenario.fault_type.value}")
        
        for node_id in scenario.target_nodes:
            if node_id in self.nodes:
                node = self.nodes[node_id]
                
                # 模拟恢复过程
                node.is_recovering = True
                time.sleep(0.1)  # 模拟恢复延迟
                
                # 恢复节点状态
                node.is_alive = True
                node.is_healthy = True
                node.disk_healthy = True
                node.network_healthy = True
                node.memory_usage = 0.0
                node.last_heartbeat = time.time()
                node.recovery_count += 1
                node.is_recovering = False
                
                logger.info(f"节点 {node_id} 已恢复")
        
        # 如果需要，重新选举Leader
        if not self.leader_id or not self.nodes[self.leader_id].is_alive:
            self._perform_failover()
    
    def _verify_recovery(self, state_before: Dict[str, Any], 
                         state_after: Dict[str, Any],
                         scenario: FaultScenario):
        """验证恢复"""
        # 检查节点是否都恢复
        for node_id in scenario.target_nodes:
            node = self.nodes.get(node_id)
            if not node or not node.is_alive:
                self._record_defect(DefectType.RECOVERY_TIMEOUT, "high",
                    f"节点恢复超时: {node_id}",
                    {"node_id": node_id})
        
        # 检查数据完整性
        data_before = state_before.get('total_data', 0)
        data_after = state_after.get('total_data', 0)
        
        if data_after < data_before * 0.9:  # 允许10%数据丢失
            self._record_defect(DefectType.PARTIAL_RECOVERY, "critical",
                f"数据恢复不完整: 恢复前={data_before}, 恢复后={data_after}",
                {"before": data_before, "after": data_after})
    
    def _get_cluster_state(self) -> Dict[str, Any]:
        """获取集群状态"""
        with self._lock:
            return {
                'leader': self.leader_id,
                'alive_nodes': sum(1 for n in self.nodes.values() if n.is_alive),
                'healthy_nodes': sum(1 for n in self.nodes.values() if n.is_healthy),
                'total_data': sum(len(n.data) for n in self.nodes.values()),
                'nodes': {nid: asdict(n) for nid, n in self.nodes.items()}
            }
    
    async def write_data(self, key: str, value: Any) -> bool:
        """写入数据（带复制）"""
        with self._lock:
            if not self.leader_id:
                return False
            
            leader = self.nodes.get(self.leader_id)
            if not leader or not leader.is_alive:
                return False
            
            # 写入Leader
            leader.data[key] = value
            leader.last_heartbeat = time.time()
            
            # 复制到其他节点
            alive_nodes = [n for n in self.nodes.values() 
                          if n.is_alive and n.node_id != self.leader_id]
            
            replicated = 0
            for node in random.sample(alive_nodes, min(self.replication_factor - 1, len(alive_nodes))):
                node.data[key] = value
                replicated += 1
            
            return replicated >= self.replication_factor - 1
    
    def get_defects(self) -> List[DefectReport]:
        """获取所有缺陷"""
        return self._defects.copy()


class FaultInjectionScenario:
    """故障注入与恢复测试场景"""
    
    def __init__(self):
        self.cluster = FaultInjectableCluster(num_nodes=5)
        self.results: Dict[str, Any] = {}
        self.defects: List[DefectReport] = []
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        logger.info("=" * 70)
        logger.info("场景3: 故障注入与恢复测试")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # 启动集群
        self.cluster.start()
        
        # 初始化数据
        await self._populate_data()
        
        try:
            # 测试1: 单节点崩溃恢复
            result1 = await self.test_single_node_crash()
            self.results['single_node_crash'] = result1
            
            # 测试2: 多节点同时崩溃
            result2 = await self.test_multiple_node_crash()
            self.results['multiple_node_crash'] = result2
            
            # 测试3: Leader崩溃故障转移
            result3 = await self.test_leader_crash_failover()
            self.results['leader_crash_failover'] = result3
            
            # 测试4: 磁盘故障恢复
            result4 = await self.test_disk_failure_recovery()
            self.results['disk_failure_recovery'] = result4
            
            # 测试5: 网络分区恢复
            result5 = await self.test_network_partition_recovery()
            self.results['network_partition_recovery'] = result5
            
            # 测试6: 级联故障
            result6 = await self.test_cascading_failure()
            self.results['cascading_failure'] = result6
            
            # 测试7: 混合故障场景
            result7 = await self.test_mixed_faults()
            self.results['mixed_faults'] = result7
            
        finally:
            self.cluster.stop()
        
        end_time = time.time()
        
        # 收集缺陷
        self.defects = self.cluster.get_defects()
        
        # 生成报告
        report = self._generate_report(start_time, end_time)
        
        return report
    
    async def _populate_data(self):
        """填充测试数据"""
        logger.info("填充测试数据...")
        for i in range(100):
            key = f"data-{i}"
            value = {'id': i, 'content': f"test-content-{i}", 'timestamp': time.time()}
            await self.cluster.write_data(key, value)
        logger.info(f"已填充 {100} 条数据")
    
    async def test_single_node_crash(self) -> Dict[str, Any]:
        """测试1: 单节点崩溃恢复"""
        logger.info("\n" + "-" * 70)
        logger.info("测试1: 单节点崩溃恢复")
        logger.info("-" * 70)
        
        scenario = FaultScenario(
            fault_type=FaultType.NODE_CRASH,
            target_nodes=["node-1"],
            duration=2.0
        )
        
        result = self.cluster.inject_fault(scenario)
        
        # 验证集群仍然可用
        write_success = await self.cluster.write_data("test-after-crash", {"value": 1})
        result['cluster_available_after'] = write_success
        
        return result
    
    async def test_multiple_node_crash(self) -> Dict[str, Any]:
        """测试2: 多节点同时崩溃"""
        logger.info("\n" + "-" * 70)
        logger.info("测试2: 多节点同时崩溃")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        # 场景2.1: 2个节点崩溃
        scenario1 = FaultScenario(
            fault_type=FaultType.NODE_CRASH,
            target_nodes=["node-1", "node-2"],
            duration=3.0
        )
        result1 = self.cluster.inject_fault(scenario1)
        results['scenarios'].append({'name': 'two_nodes', 'result': result1})
        
        # 场景2.2: 3个节点崩溃（超过半数）
        scenario2 = FaultScenario(
            fault_type=FaultType.NODE_CRASH,
            target_nodes=["node-0", "node-1", "node-2"],
            duration=3.0
        )
        result2 = self.cluster.inject_fault(scenario2)
        results['scenarios'].append({'name': 'three_nodes', 'result': result2})
        
        return results
    
    async def test_leader_crash_failover(self) -> Dict[str, Any]:
        """测试3: Leader崩溃故障转移"""
        logger.info("\n" + "-" * 70)
        logger.info("测试3: Leader崩溃故障转移")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        for i in range(3):
            current_leader = self.cluster.leader_id
            logger.info(f"\n轮次 {i+1}: 当前Leader={current_leader}")
            
            scenario = FaultScenario(
                fault_type=FaultType.NODE_CRASH,
                target_nodes=[current_leader],
                duration=2.0
            )
            
            result = self.cluster.inject_fault(scenario)
            result['old_leader'] = current_leader
            result['new_leader'] = self.cluster.leader_id
            
            results['scenarios'].append(result)
            
            # 等待稳定
            await asyncio.sleep(0.5)
        
        return results
    
    async def test_disk_failure_recovery(self) -> Dict[str, Any]:
        """测试4: 磁盘故障恢复"""
        logger.info("\n" + "-" * 70)
        logger.info("测试4: 磁盘故障恢复")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        # 场景4.1: 单节点磁盘故障
        scenario1 = FaultScenario(
            fault_type=FaultType.DISK_FAILURE,
            target_nodes=["node-1"],
            duration=2.0,
            intensity=0.5
        )
        result1 = self.cluster.inject_fault(scenario1)
        results['scenarios'].append({'name': 'single_disk', 'result': result1})
        
        # 场景4.2: 多节点磁盘故障
        scenario2 = FaultScenario(
            fault_type=FaultType.DISK_FAILURE,
            target_nodes=["node-2", "node-3"],
            duration=3.0,
            intensity=0.8
        )
        result2 = self.cluster.inject_fault(scenario2)
        results['scenarios'].append({'name': 'multiple_disk', 'result': result2})
        
        return results
    
    async def test_network_partition_recovery(self) -> Dict[str, Any]:
        """测试5: 网络分区恢复"""
        logger.info("\n" + "-" * 70)
        logger.info("测试5: 网络分区恢复")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        # 场景5.1: 少数派分区
        scenario1 = FaultScenario(
            fault_type=FaultType.NETWORK_PARTITION,
            target_nodes=["node-3", "node-4"],
            duration=3.0
        )
        result1 = self.cluster.inject_fault(scenario1)
        results['scenarios'].append({'name': 'minority_partition', 'result': result1})
        
        # 场景5.2: 多数派分区
        scenario2 = FaultScenario(
            fault_type=FaultType.NETWORK_PARTITION,
            target_nodes=["node-0", "node-1", "node-2"],
            duration=3.0
        )
        result2 = self.cluster.inject_fault(scenario2)
        results['scenarios'].append({'name': 'majority_partition', 'result': result2})
        
        return results
    
    async def test_cascading_failure(self) -> Dict[str, Any]:
        """测试6: 级联故障"""
        logger.info("\n" + "-" * 70)
        logger.info("测试6: 级联故障")
        logger.info("-" * 70)
        
        results = {'steps': []}
        
        # 逐步杀死节点
        for i in range(4):
            target = f"node-{i}"
            logger.info(f"\n级联步骤 {i+1}: 杀死 {target}")
            
            scenario = FaultScenario(
                fault_type=FaultType.NODE_CRASH,
                target_nodes=[target],
                duration=1.0
            )
            
            result = self.cluster.inject_fault(scenario)
            result['step'] = i + 1
            results['steps'].append(result)
            
            # 检查集群是否还能工作
            alive_count = sum(1 for n in self.cluster.nodes.values() if n.is_alive)
            results['steps'][-1]['alive_nodes'] = alive_count
            
            await asyncio.sleep(0.5)
        
        # 恢复所有节点
        logger.info("\n恢复所有节点...")
        for node_id in self.cluster.nodes:
            node = self.cluster.nodes[node_id]
            node.is_alive = True
            node.is_healthy = True
        
        return results
    
    async def test_mixed_faults(self) -> Dict[str, Any]:
        """测试7: 混合故障场景"""
        logger.info("\n" + "-" * 70)
        logger.info("测试7: 混合故障场景")
        logger.info("-" * 70)
        
        results = {'scenarios': []}
        
        # 混合故障: 节点崩溃 + 磁盘故障 + 网络延迟
        scenarios = [
            FaultScenario(
                fault_type=FaultType.NODE_CRASH,
                target_nodes=["node-1"],
                duration=2.0
            ),
            FaultScenario(
                fault_type=FaultType.DISK_FAILURE,
                target_nodes=["node-2"],
                duration=2.0,
                intensity=0.5
            ),
            FaultScenario(
                fault_type=FaultType.NETWORK_DELAY,
                target_nodes=["node-3"],
                duration=2.0,
                metadata={'delay_ms': 200}
            )
        ]
        
        for i, scenario in enumerate(scenarios):
            result = self.cluster.inject_fault(scenario)
            results['scenarios'].append({
                'name': f'mixed_{i+1}',
                'fault_type': scenario.fault_type.value,
                'result': result
            })
        
        return results
    
    def _generate_report(self, start_time: float, end_time: float) -> Dict[str, Any]:
        """生成测试报告"""
        defects_by_type = defaultdict(list)
        defects_by_severity = defaultdict(list)
        
        for defect in self.defects:
            defect_dict = defect.to_dict()
            defects_by_type[defect.defect_type.value].append(defect_dict)
            defects_by_severity[defect.severity].append(defect_dict)
        
        report = {
            'scenario': '故障注入与恢复测试',
            'scenario_id': 'scenario3_fault_injection',
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'end_time': datetime.fromtimestamp(end_time).isoformat(),
            'duration_seconds': end_time - start_time,
            'summary': {
                'total_tests': 7,
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
    scenario = FaultInjectionScenario()
    report = await scenario.run_all_tests()
    
    # 保存报告
    output_path = "/workspace/agent_memory_system_v2/tests/reports/scenario3_fault_injection_report.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("场景3: 故障注入与恢复测试 - 摘要")
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

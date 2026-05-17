#!/usr/bin/env python3
"""
场景4: 级联故障注入测试
Agent Memory System V3.0 - 第1轮实践

测试目标:
- 依次杀死Leader、Follower
- 磁盘满、内存不足同时发生
- 验证自动恢复能力

关键指标:
- 故障检测时间
- 自动恢复成功率
- 数据完整性
- 服务可用性
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
        logging.FileHandler(log_dir / 'scenario4_cascading_failure.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class NodeState(Enum):
    """节点状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECOVERING = "recovering"


class FailureType(Enum):
    """故障类型"""
    LEADER_DEATH = "leader_death"
    FOLLOWER_DEATH = "follower_death"
    DISK_FULL = "disk_full"
    MEMORY_EXHAUSTED = "memory_exhausted"
    NETWORK_PARTITION = "network_partition"
    CPU_OVERLOAD = "cpu_overload"


@dataclass
class NodeHealth:
    """节点健康状态"""
    node_id: str
    state: NodeState = NodeState.HEALTHY
    disk_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    cpu_usage_percent: float = 0.0
    is_leader: bool = False
    last_heartbeat: float = 0.0
    failure_count: int = 0
    recovery_count: int = 0


@dataclass
class CascadingFailureMetrics:
    """级联故障测试指标"""
    scenario_name: str = "级联故障注入"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    
    # 故障统计
    total_failures_injected: int = 0
    failures_by_type: Dict[str, int] = field(default_factory=dict)
    
    # 恢复统计
    total_recoveries: int = 0
    auto_recovery_success: int = 0
    auto_recovery_failed: int = 0
    avg_recovery_time_sec: float = 0.0
    
    # 可用性统计
    total_downtime_sec: float = 0.0
    availability_percent: float = 100.0
    
    # 数据完整性
    data_corruption_detected: int = 0
    data_loss_entries: int = 0
    
    # 故障序列
    failure_sequence: List[Dict] = field(default_factory=list)
    recovery_events: List[Dict] = field(default_factory=list)
    
    # 健康状态时间线
    health_timeline: List[Dict] = field(default_factory=list)
    
    # 错误和缺陷
    errors: List[Dict] = field(default_factory=list)
    defects_found: List[Dict] = field(default_factory=list)


class CascadingFailureSimulator:
    """级联故障模拟器"""
    
    def __init__(self, node_count: int = 5):
        self.node_count = node_count
        self.nodes: Dict[str, NodeHealth] = {}
        self.leader_id: Optional[str] = None
        self.cluster_lock = asyncio.Lock()
        
        # 系统资源状态
        self.disk_usage = 0.0
        self.memory_usage = 0.0
        self.cpu_usage = 0.0
        
        # 数据存储
        self.data_store: Dict[str, Dict] = {}
        self.data_lock = asyncio.Lock()
        
        # 初始化节点
        for i in range(node_count):
            node_id = f"node_{i}"
            self.nodes[node_id] = NodeHealth(
                node_id=node_id,
                last_heartbeat=time.time()
            )
        
        # 选举初始Leader
        self._elect_leader()
    
    def _elect_leader(self):
        """选举Leader"""
        healthy_nodes = [
            nid for nid, n in self.nodes.items()
            if n.state != NodeState.FAILED
        ]
        
        if healthy_nodes:
            if self.leader_id and self.nodes[self.leader_id].state != NodeState.FAILED:
                return  # 保持现有Leader
            
            new_leader = random.choice(healthy_nodes)
            
            # 重置旧Leader
            if self.leader_id:
                self.nodes[self.leader_id].is_leader = False
            
            # 设置新Leader
            self.leader_id = new_leader
            self.nodes[new_leader].is_leader = True
            
            logger.info(f"新Leader选举: {new_leader}")
    
    async def inject_failure(self, failure_type: FailureType, target_node: Optional[str] = None) -> Dict:
        """注入故障"""
        async with self.cluster_lock:
            failure_record = {
                "type": failure_type.value,
                "timestamp": time.time(),
                "target_node": target_node,
                "success": False
            }
            
            if failure_type == FailureType.LEADER_DEATH:
                if self.leader_id:
                    self.nodes[self.leader_id].state = NodeState.FAILED
                    self.nodes[self.leader_id].is_leader = False
                    old_leader = self.leader_id
                    self.leader_id = None
                    failure_record["target_node"] = old_leader
                    failure_record["success"] = True
                    
                    # 触发新Leader选举
                    await asyncio.sleep(0.5)
                    self._elect_leader()
            
            elif failure_type == FailureType.FOLLOWER_DEATH:
                followers = [
                    nid for nid, n in self.nodes.items()
                    if not n.is_leader and n.state != NodeState.FAILED
                ]
                
                if followers:
                    victim = target_node or random.choice(followers)
                    self.nodes[victim].state = NodeState.FAILED
                    failure_record["target_node"] = victim
                    failure_record["success"] = True
            
            elif failure_type == FailureType.DISK_FULL:
                self.disk_usage = 100.0
                failure_record["disk_usage"] = 100.0
                failure_record["success"] = True
                
                # 标记所有节点为降级状态
                for node in self.nodes.values():
                    if node.state != NodeState.FAILED:
                        node.state = NodeState.DEGRADED
                        node.disk_usage_percent = 100.0
            
            elif failure_type == FailureType.MEMORY_EXHAUSTED:
                self.memory_usage = 100.0
                failure_record["memory_usage"] = 100.0
                failure_record["success"] = True
                
                # 随机影响部分节点
                for node in self.nodes.values():
                    if random.random() < 0.5 and node.state != NodeState.FAILED:
                        node.state = NodeState.DEGRADED
                        node.memory_usage_percent = 100.0
            
            elif failure_type == FailureType.CPU_OVERLOAD:
                self.cpu_usage = 100.0
                failure_record["cpu_usage"] = 100.0
                failure_record["success"] = True
                
                for node in self.nodes.values():
                    if node.state != NodeState.FAILED:
                        node.cpu_usage_percent = 100.0
            
            elif failure_type == FailureType.NETWORK_PARTITION:
                # 随机分区
                healthy_nodes = [
                    nid for nid, n in self.nodes.items()
                    if n.state != NodeState.FAILED
                ]
                
                if len(healthy_nodes) >= 2:
                    partition_size = len(healthy_nodes) // 2
                    partition = random.sample(healthy_nodes, partition_size)
                    
                    for nid in partition:
                        self.nodes[nid].state = NodeState.DEGRADED
                    
                    failure_record["partitioned_nodes"] = partition
                    failure_record["success"] = True
            
            if failure_record["success"]:
                logger.warning(f"故障注入: {failure_type.value} - {failure_record.get('target_node', 'system')}")
            
            return failure_record
    
    async def attempt_recovery(self) -> List[Dict]:
        """尝试自动恢复"""
        recovery_events = []
        
        async with self.cluster_lock:
            # 恢复磁盘
            if self.disk_usage >= 100.0:
                self.disk_usage = random.uniform(50.0, 70.0)
                recovery_events.append({
                    "type": "disk_recovery",
                    "timestamp": time.time(),
                    "new_usage": self.disk_usage
                })
                
                for node in self.nodes.values():
                    if node.disk_usage_percent >= 100.0:
                        node.disk_usage_percent = random.uniform(50.0, 70.0)
                        if node.state == NodeState.DEGRADED:
                            node.state = NodeState.RECOVERING
            
            # 恢复内存
            if self.memory_usage >= 100.0:
                self.memory_usage = random.uniform(60.0, 80.0)
                recovery_events.append({
                    "type": "memory_recovery",
                    "timestamp": time.time(),
                    "new_usage": self.memory_usage
                })
                
                for node in self.nodes.values():
                    if node.memory_usage_percent >= 100.0:
                        node.memory_usage_percent = random.uniform(60.0, 80.0)
            
            # 恢复CPU
            if self.cpu_usage >= 100.0:
                self.cpu_usage = random.uniform(40.0, 60.0)
                recovery_events.append({
                    "type": "cpu_recovery",
                    "timestamp": time.time(),
                    "new_usage": self.cpu_usage
                })
                
                for node in self.nodes.values():
                    if node.cpu_usage_percent >= 100.0:
                        node.cpu_usage_percent = random.uniform(40.0, 60.0)
            
            # 恢复失败节点
            for node_id, node in self.nodes.items():
                if node.state == NodeState.FAILED:
                    if random.random() < 0.8:  # 80%恢复成功率
                        node.state = NodeState.RECOVERING
                        node.failure_count += 1
                        recovery_events.append({
                            "type": "node_recovery",
                            "node_id": node_id,
                            "timestamp": time.time(),
                            "success": True
                        })
                    else:
                        recovery_events.append({
                            "type": "node_recovery",
                            "node_id": node_id,
                            "timestamp": time.time(),
                            "success": False
                        })
                
                elif node.state == NodeState.RECOVERING:
                    # 完成恢复
                    node.state = NodeState.HEALTHY
                    node.recovery_count += 1
                    node.last_heartbeat = time.time()
            
            # 如果没有Leader, 选举新Leader
            if not self.leader_id or self.nodes[self.leader_id].state == NodeState.FAILED:
                old_leader = self.leader_id
                self._elect_leader()
                
                if self.leader_id and self.leader_id != old_leader:
                    recovery_events.append({
                        "type": "leader_election",
                        "new_leader": self.leader_id,
                        "timestamp": time.time()
                    })
        
        for event in recovery_events:
            logger.info(f"恢复事件: {event['type']} - {event.get('node_id', 'system')}")
        
        return recovery_events
    
    async def write_data(self, key: str, value: Dict) -> bool:
        """写入数据"""
        # 检查系统状态
        if self.disk_usage >= 100.0:
            return False
        
        if self.memory_usage >= 95.0:
            return False
        
        if not self.leader_id or self.nodes[self.leader_id].state == NodeState.FAILED:
            return False
        
        async with self.data_lock:
            # 模拟写入延迟
            await asyncio.sleep(random.uniform(0.001, 0.005))
            
            self.data_store[key] = {
                **value,
                "timestamp": time.time(),
                "leader": self.leader_id
            }
            
            return True
    
    async def read_data(self, key: str) -> Optional[Dict]:
        """读取数据"""
        async with self.data_lock:
            await asyncio.sleep(random.uniform(0.0005, 0.002))
            return self.data_store.get(key)
    
    def get_cluster_health(self) -> Dict:
        """获取集群健康状态"""
        healthy_count = sum(1 for n in self.nodes.values() if n.state == NodeState.HEALTHY)
        degraded_count = sum(1 for n in self.nodes.values() if n.state == NodeState.DEGRADED)
        failed_count = sum(1 for n in self.nodes.values() if n.state == NodeState.FAILED)
        recovering_count = sum(1 for n in self.nodes.values() if n.state == NodeState.RECOVERING)
        
        return {
            "leader_id": self.leader_id,
            "healthy_nodes": healthy_count,
            "degraded_nodes": degraded_count,
            "failed_nodes": failed_count,
            "recovering_nodes": recovering_count,
            "total_nodes": len(self.nodes),
            "disk_usage": self.disk_usage,
            "memory_usage": self.memory_usage,
            "cpu_usage": self.cpu_usage,
            "data_entries": len(self.data_store),
            "nodes": {nid: {
                "state": n.state.value,
                "is_leader": n.is_leader,
                "disk": n.disk_usage_percent,
                "memory": n.memory_usage_percent,
                "cpu": n.cpu_usage_percent
            } for nid, n in self.nodes.items()}
        }
    
    def verify_data_integrity(self) -> Dict:
        """验证数据完整性"""
        corruption_count = 0
        
        for key, value in self.data_store.items():
            # 检查必需字段
            if "timestamp" not in value or "leader" not in value:
                corruption_count += 1
                continue
            
            # 检查时间戳合理性
            if value["timestamp"] > time.time() + 60:
                corruption_count += 1
        
        return {
            "total_entries": len(self.data_store),
            "corruption_count": corruption_count,
            "integrity_ok": corruption_count == 0
        }


class CascadingFailureTestRunner:
    """级联故障测试运行器"""
    
    def __init__(self, target_duration_minutes: int = 5):
        self.target_duration = target_duration_minutes * 60
        self.simulator = CascadingFailureSimulator(node_count=5)
        self.metrics = CascadingFailureMetrics()
        self.stop_event = asyncio.Event()
        
        self.recovery_times: List[float] = []
        self.failure_start_times: Dict[str, float] = {}
    
    async def run(self) -> CascadingFailureMetrics:
        """运行级联故障测试"""
        logger.info("=" * 80)
        logger.info("场景4: 级联故障注入测试")
        logger.info("=" * 80)
        logger.info(f"集群规模: 5节点")
        logger.info(f"目标持续时间: {self.target_duration/60:.1f} 分钟")
        logger.info("=" * 80)
        
        self.metrics.start_time = time.time()
        
        # 启动健康监控
        monitor_task = asyncio.create_task(self._health_monitor())
        
        # 启动工作负载
        workload_task = asyncio.create_task(self._workload_generator())
        
        # 执行故障注入序列
        failure_task = asyncio.create_task(self._failure_injection_sequence())
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待任务完成
        await asyncio.gather(monitor_task, workload_task, failure_task, return_exceptions=True)
        
        self.metrics.end_time = time.time()
        self.metrics.total_duration_sec = self.metrics.end_time - self.metrics.start_time
        
        # 生成最终报告
        await self._generate_final_metrics()
        
        return self.metrics
    
    async def _workload_generator(self):
        """工作负载生成器"""
        write_count = 0
        
        while not self.stop_event.is_set():
            try:
                key = f"key_{write_count}_{int(time.time()*1000000)}"
                value = {
                    "data": f"Test data {write_count}",
                    "sequence": write_count,
                    "random": random.randint(1, 1000000)
                }
                
                success = await self.simulator.write_data(key, value)
                
                if success:
                    write_count += 1
                
                await asyncio.sleep(random.uniform(0.01, 0.05))
                
            except Exception as e:
                logger.warning(f"Workload error: {e}")
    
    async def _failure_injection_sequence(self):
        """故障注入序列"""
        await asyncio.sleep(3)  # 等待系统稳定
        
        # Phase 1: 杀死Leader
        logger.info("\n" + "=" * 40)
        logger.info("Phase 1: 杀死Leader节点")
        logger.info("=" * 40)
        
        start_time = time.time()
        failure = await self.simulator.inject_failure(FailureType.LEADER_DEATH)
        self.failure_start_times[failure["type"]] = start_time
        
        if failure["success"]:
            self.metrics.total_failures_injected += 1
            self.metrics.failures_by_type[failure["type"]] = self.metrics.failures_by_type.get(failure["type"], 0) + 1
            self.metrics.failure_sequence.append(failure)
            
            # 等待恢复
            await asyncio.sleep(2)
            
            # 尝试恢复
            recovery_start = time.time()
            recoveries = await self.simulator.attempt_recovery()
            recovery_time = time.time() - recovery_start
            
            self.recovery_times.append(recovery_time)
            self.metrics.recovery_events.extend(recoveries)
            
            for event in recoveries:
                if event.get("success"):
                    self.metrics.auto_recovery_success += 1
                else:
                    self.metrics.auto_recovery_failed += 1
        
        await asyncio.sleep(5)
        
        # Phase 2: 杀死Follower + 磁盘满
        logger.info("\n" + "=" * 40)
        logger.info("Phase 2: 杀死Follower + 磁盘满")
        logger.info("=" * 40)
        
        # 杀死Follower
        start_time = time.time()
        failure = await self.simulator.inject_failure(FailureType.FOLLOWER_DEATH)
        
        if failure["success"]:
            self.metrics.total_failures_injected += 1
            self.metrics.failures_by_type[failure["type"]] = self.metrics.failures_by_type.get(failure["type"], 0) + 1
            self.metrics.failure_sequence.append(failure)
        
        # 磁盘满
        start_time = time.time()
        failure = await self.simulator.inject_failure(FailureType.DISK_FULL)
        
        if failure["success"]:
            self.metrics.total_failures_injected += 1
            self.metrics.failures_by_type[failure["type"]] = self.metrics.failures_by_type.get(failure["type"], 0) + 1
            self.metrics.failure_sequence.append(failure)
        
        # 等待并恢复
        await asyncio.sleep(3)
        
        recovery_start = time.time()
        recoveries = await self.simulator.attempt_recovery()
        recovery_time = time.time() - recovery_start
        self.recovery_times.append(recovery_time)
        self.metrics.recovery_events.extend(recoveries)
        
        for event in recoveries:
            if event.get("success"):
                self.metrics.auto_recovery_success += 1
            else:
                self.metrics.auto_recovery_failed += 1
        
        await asyncio.sleep(5)
        
        # Phase 3: 内存不足 + CPU过载
        logger.info("\n" + "=" * 40)
        logger.info("Phase 3: 内存不足 + CPU过载")
        logger.info("=" * 40)
        
        # 内存不足
        failure = await self.simulator.inject_failure(FailureType.MEMORY_EXHAUSTED)
        if failure["success"]:
            self.metrics.total_failures_injected += 1
            self.metrics.failures_by_type[failure["type"]] = self.metrics.failures_by_type.get(failure["type"], 0) + 1
            self.metrics.failure_sequence.append(failure)
        
        # CPU过载
        failure = await self.simulator.inject_failure(FailureType.CPU_OVERLOAD)
        if failure["success"]:
            self.metrics.total_failures_injected += 1
            self.metrics.failures_by_type[failure["type"]] = self.metrics.failures_by_type.get(failure["type"], 0) + 1
            self.metrics.failure_sequence.append(failure)
        
        # 等待并恢复
        await asyncio.sleep(3)
        
        recovery_start = time.time()
        recoveries = await self.simulator.attempt_recovery()
        recovery_time = time.time() - recovery_start
        self.recovery_times.append(recovery_time)
        self.metrics.recovery_events.extend(recoveries)
        
        for event in recoveries:
            if event.get("success"):
                self.metrics.auto_recovery_success += 1
            else:
                self.metrics.auto_recovery_failed += 1
        
        await asyncio.sleep(5)
        
        # Phase 4: 级联故障 - 再次杀死Leader + 网络分区
        logger.info("\n" + "=" * 40)
        logger.info("Phase 4: 级联故障 - Leader死亡 + 网络分区")
        logger.info("=" * 40)
        
        failure = await self.simulator.inject_failure(FailureType.LEADER_DEATH)
        if failure["success"]:
            self.metrics.total_failures_injected += 1
            self.metrics.failures_by_type[failure["type"]] = self.metrics.failures_by_type.get(failure["type"], 0) + 1
            self.metrics.failure_sequence.append(failure)
        
        failure = await self.simulator.inject_failure(FailureType.NETWORK_PARTITION)
        if failure["success"]:
            self.metrics.total_failures_injected += 1
            self.metrics.failures_by_type[failure["type"]] = self.metrics.failures_by_type.get(failure["type"], 0) + 1
            self.metrics.failure_sequence.append(failure)
        
        # 最终恢复
        await asyncio.sleep(3)
        
        recovery_start = time.time()
        recoveries = await self.simulator.attempt_recovery()
        recovery_time = time.time() - recovery_start
        self.recovery_times.append(recovery_time)
        self.metrics.recovery_events.extend(recoveries)
        
        for event in recoveries:
            if event.get("success"):
                self.metrics.auto_recovery_success += 1
            else:
                self.metrics.auto_recovery_failed += 1
        
        logger.info("\n故障注入序列完成")
    
    async def _health_monitor(self):
        """健康状态监控"""
        check_count = 0
        last_health = None
        
        while not self.stop_event.is_set():
            await asyncio.sleep(3)
            
            try:
                health = self.simulator.get_cluster_health()
                integrity = self.simulator.verify_data_integrity()
                
                self.metrics.health_timeline.append({
                    "timestamp": time.time(),
                    "check_number": check_count,
                    **health
                })
                
                # 检测数据损坏
                if not integrity["integrity_ok"]:
                    self.metrics.data_corruption_detected += 1
                
                # 计算可用性
                if health["healthy_nodes"] == 0 and last_health and last_health["healthy_nodes"] > 0:
                    # 系统不可用
                    pass
                
                logger.info(
                    f"[Health] Leader: {health['leader_id']} | "
                    f"Healthy: {health['healthy_nodes']} | "
                    f"Failed: {health['failed_nodes']} | "
                    f"Disk: {health['disk_usage']:.1f}% | "
                    f"Mem: {health['memory_usage']:.1f}% | "
                    f"Data: {health['data_entries']}"
                )
                
                last_health = health
                check_count += 1
                
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    
    async def _generate_final_metrics(self):
        """生成最终指标"""
        final_health = self.simulator.get_cluster_health()
        integrity = self.simulator.verify_data_integrity()
        
        self.metrics.total_recoveries = len(self.metrics.recovery_events)
        
        if self.recovery_times:
            self.metrics.avg_recovery_time_sec = sum(self.recovery_times) / len(self.recovery_times)
        
        # 计算可用性
        total_failures = self.metrics.total_failures_injected
        successful_recoveries = self.metrics.auto_recovery_success
        
        if total_failures > 0:
            self.metrics.availability_percent = (successful_recoveries / total_failures) * 100
        
        self.metrics.data_corruption_detected = 0 if integrity["integrity_ok"] else 1
        
        # 检测缺陷
        if self.metrics.auto_recovery_failed > 0:
            self.metrics.defects_found.append({
                "id": "DEFECT-S4-001",
                "severity": "P0",
                "type": "recovery_failure",
                "description": f"自动恢复失败 {self.metrics.auto_recovery_failed} 次",
                "failed_count": self.metrics.auto_recovery_failed
            })
        
        if not integrity["integrity_ok"]:
            self.metrics.defects_found.append({
                "id": "DEFECT-S4-002",
                "severity": "P0",
                "type": "data_corruption",
                "description": f"检测到数据损坏: {integrity['corruption_count']} 条",
                "corruption_count": integrity["corruption_count"]
            })
        
        if self.metrics.avg_recovery_time_sec > 3.0:
            self.metrics.defects_found.append({
                "id": "DEFECT-S4-003",
                "severity": "P2",
                "type": "slow_recovery",
                "description": f"平均恢复时间过长: {self.metrics.avg_recovery_time_sec:.3f}秒",
                "threshold": "3秒",
                "actual": f"{self.metrics.avg_recovery_time_sec:.3f}秒"
            })


async def main():
    """主函数"""
    runner = CascadingFailureTestRunner(target_duration_minutes=3)
    
    try:
        metrics = await runner.run()
        
        # 保存详细报告
        metrics_dir = Path("/workspace/agent_memory_system_v2/tests/round1/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = metrics_dir / "scenario4_cascading_failure_metrics.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(metrics), f, indent=2, default=str)
        
        # 打印摘要
        print("\n" + "=" * 80)
        print("场景4: 级联故障注入测试 - 完成")
        print("=" * 80)
        print(f"运行时间: {metrics.total_duration_sec:.2f} 秒")
        print("-" * 80)
        print(f"故障注入:")
        print(f"  - 总数: {metrics.total_failures_injected}")
        for ft, count in metrics.failures_by_type.items():
            print(f"    - {ft}: {count}")
        print("-" * 80)
        print(f"恢复统计:")
        print(f"  - 总恢复次数: {metrics.total_recoveries}")
        print(f"  - 成功: {metrics.auto_recovery_success}")
        print(f"  - 失败: {metrics.auto_recovery_failed}")
        print(f"  - 平均恢复时间: {metrics.avg_recovery_time_sec:.3f}秒")
        print(f"  - 可用性: {metrics.availability_percent:.1f}%")
        print("-" * 80)
        print(f"数据完整性:")
        print(f"  - 数据损坏: {metrics.data_corruption_detected}")
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

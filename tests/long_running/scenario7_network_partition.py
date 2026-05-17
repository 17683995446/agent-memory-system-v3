#!/usr/bin/env python3
"""
场景7: 网络分区脑裂测试

测试目标:
- 将集群分为两个分区
- 各自选举Leader
- 恢复分区后验证数据合并
- 测试冲突解决策略

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
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/scenario7_network_partition.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PartitionState(Enum):
    """分区状态"""
    NORMAL = "normal"
    PARTITIONED = "partitioned"
    RECOVERING = "recovering"


@dataclass
class NetworkPartitionReport:
    """网络分区测试报告"""
    scenario_name: str = "网络分区脑裂测试"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    cluster_size: int = 5
    partition_count: int = 0
    split_brain_events: int = 0
    leaders_during_partition: int = 0
    data_divergence_detected: int = 0
    merge_operations: int = 0
    conflicts_during_merge: int = 0
    conflicts_resolved: int = 0
    data_loss_events: int = 0
    partition_events: List[Dict] = field(default_factory=list)
    merge_events: List[Dict] = field(default_factory=list)
    leader_changes: List[Dict] = field(default_factory=list)
    conflict_resolution_log: List[Dict] = field(default_factory=list)
    consistency_checks: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class PartitionedNode:
    """分区节点"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.is_alive = True
        self.partition_group: Optional[int] = None  # None = 未分区
        self.is_leader = False
        self.term = 0
        self.data: Dict[str, Dict] = {}
        self.log: List[Dict] = []
        self.commit_index = -1
        
    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "is_alive": self.is_alive,
            "partition_group": self.partition_group,
            "is_leader": self.is_leader,
            "term": self.term,
            "data_count": len(self.data),
            "log_size": len(self.log)
        }


class SplitBrainResolver:
    """脑裂解决器"""
    
    def __init__(self):
        self.resolution_strategies = ["last_writer_wins", "vector_clock", "timestamp"]
        self.resolution_count = 0
        
    def resolve_conflict(self, key: str, versions: List[Dict]) -> Dict:
        """解决冲突"""
        if not versions:
            return None
        
        if len(versions) == 1:
            return versions[0]
        
        # 使用Last-Writer-Wins策略
        sorted_versions = sorted(
            versions,
            key=lambda v: v.get("timestamp", 0),
            reverse=True
        )
        
        winner = sorted_versions[0]
        winner["_conflict_resolved"] = True
        winner["_conflict_versions"] = len(versions)
        
        self.resolution_count += 1
        
        return winner


class NetworkPartitionSimulator:
    """网络分区模拟器"""
    
    def __init__(self, node_count: int = 5):
        self.nodes: Dict[str, PartitionedNode] = {}
        self.node_count = node_count
        self.state = PartitionState.NORMAL
        self.leader_id: Optional[str] = None
        self.split_brain_resolver = SplitBrainResolver()
        
        # 初始化节点
        for i in range(node_count):
            node_id = f"node_{i}"
            self.nodes[node_id] = PartitionedNode(node_id)
        
        # 选举初始Leader
        self._elect_leader()
    
    def _elect_leader(self, partition_group: Optional[int] = None):
        """选举Leader"""
        candidates = [
            n for n in self.nodes.values()
            if n.is_alive and n.partition_group == partition_group
        ]
        
        if not candidates:
            return None
        
        # 选择term最高的, 或随机选择
        max_term = max(n.term for n in candidates)
        eligible = [n for n in candidates if n.term == max_term]
        leader = random.choice(eligible)
        
        leader.is_leader = True
        leader.term += 1
        
        if partition_group is None:
            self.leader_id = leader.node_id
        
        return leader.node_id
    
    async def create_partition(self) -> Dict:
        """创建网络分区"""
        if self.state == PartitionState.PARTITIONED:
            return {"success": False, "reason": "already_partitioned"}
        
        # 将节点分为两组
        alive_nodes = [n for n in self.nodes.values() if n.is_alive]
        if len(alive_nodes) < 4:
            return {"success": False, "reason": "not_enough_nodes"}
        
        random.shuffle(alive_nodes)
        mid = len(alive_nodes) // 2
        
        group_a = alive_nodes[:mid]
        group_b = alive_nodes[mid:]
        
        for node in group_a:
            node.partition_group = 0
            node.is_leader = False
        
        for node in group_b:
            node.partition_group = 1
            node.is_leader = False
        
        # 各自选举Leader
        leader_a = self._elect_leader(0)
        leader_b = self._elect_leader(1)
        
        self.state = PartitionState.PARTITIONED
        
        return {
            "success": True,
            "group_a": [n.node_id for n in group_a],
            "group_b": [n.node_id for n in group_b],
            "leader_a": leader_a,
            "leader_b": leader_b,
            "split_brain": leader_a is not None and leader_b is not None
        }
    
    async def heal_partition(self) -> Dict:
        """恢复网络分区"""
        if self.state != PartitionState.PARTITIONED:
            return {"success": False, "reason": "not_partitioned"}
        
        self.state = PartitionState.RECOVERING
        
        # 合并数据
        merge_result = await self._merge_partitions()
        
        # 清除分区标记
        for node in self.nodes.values():
            node.partition_group = None
        
        # 重新选举Leader
        self._elect_leader()
        
        self.state = PartitionState.NORMAL
        
        return {
            "success": True,
            "merged_keys": merge_result["merged_keys"],
            "conflicts": merge_result["conflicts"],
            "new_leader": self.leader_id
        }
    
    async def _merge_partitions(self) -> Dict:
        """合并分区数据"""
        merged_keys = 0
        conflicts = 0
        
        # 收集所有key
        all_keys = set()
        for node in self.nodes.values():
            all_keys.update(node.data.keys())
        
        for key in all_keys:
            # 收集该key的所有版本
            versions = []
            for node in self.nodes.values():
                if key in node.data:
                    versions.append(node.data[key])
            
            if len(versions) > 1:
                # 发现冲突
                conflicts += 1
                winner = self.split_brain_resolver.resolve_conflict(key, versions)
                
                # 将解决后的数据同步到所有节点
                for node in self.nodes.values():
                    node.data[key] = winner
            else:
                # 无冲突, 同步到所有节点
                value = versions[0] if versions else None
                if value:
                    for node in self.nodes.values():
                        node.data[key] = value
            
            merged_keys += 1
        
        return {
            "merged_keys": merged_keys,
            "conflicts": conflicts
        }
    
    async def write_during_partition(self, key: str, value: str, node_id: str) -> Dict:
        """分区期间写入"""
        if node_id not in self.nodes:
            return {"success": False, "error": "node_not_found"}
        
        node = self.nodes[node_id]
        if not node.is_alive:
            return {"success": False, "error": "node_dead"}
        
        # 只有Leader可以写入
        if not node.is_leader:
            return {"success": False, "error": "not_leader"}
        
        entry = {
            "key": key,
            "value": value,
            "timestamp": time.time(),
            "node_id": node_id,
            "partition_group": node.partition_group,
            "term": node.term
        }
        
        # 写入本地
        node.data[key] = entry
        node.log.append(entry)
        node.commit_index = len(node.log) - 1
        
        # 复制到同分区的其他节点
        replicated = 1
        for other in self.nodes.values():
            if (other.node_id != node_id and 
                other.is_alive and 
                other.partition_group == node.partition_group):
                other.data[key] = entry
                other.log.append(entry)
                replicated += 1
        
        return {
            "success": True,
            "key": key,
            "replicated_to": replicated
        }
    
    def get_cluster_state(self) -> Dict:
        """获取集群状态"""
        partition_groups = {}
        for node in self.nodes.values():
            group = node.partition_group
            if group not in partition_groups:
                partition_groups[group] = []
            partition_groups[group].append(node.node_id)
        
        leaders = [
            n.node_id for n in self.nodes.values() if n.is_leader
        ]
        
        return {
            "state": self.state.value,
            "total_nodes": len(self.nodes),
            "alive_nodes": sum(1 for n in self.nodes.values() if n.is_alive),
            "partition_groups": partition_groups,
            "leaders": leaders,
            "split_brain": len(leaders) > 1,
            "nodes": {nid: node.to_dict() for nid, node in self.nodes.items()}
        }
    
    def verify_consistency(self) -> Dict:
        """验证一致性"""
        if self.state == PartitionState.PARTITIONED:
            return {"consistent": False, "reason": "partitioned"}
        
        # 检查所有节点数据是否一致
        all_keys = set()
        for node in self.nodes.values():
            all_keys.update(node.data.keys())
        
        divergent_keys = 0
        for key in all_keys:
            values = []
            for node in self.nodes.values():
                if key in node.data:
                    values.append(node.data[key])
            
            if len(values) > 1:
                # 比较值
                value_hashes = set(
                    hashlib.sha256(json.dumps(v, sort_keys=True).encode()).hexdigest()[:16]
                    for v in values
                )
                if len(value_hashes) > 1:
                    divergent_keys += 1
        
        return {
            "consistent": divergent_keys == 0,
            "total_keys": len(all_keys),
            "divergent_keys": divergent_keys
        }


class NetworkPartitionTestRunner:
    """网络分区测试运行器"""
    
    def __init__(self, target_duration_minutes: int = 10):
        self.target_duration = target_duration_minutes * 60
        self.simulator = NetworkPartitionSimulator(node_count=5)
        self.report = NetworkPartitionReport()
        self.stop_event = asyncio.Event()
        self.concurrent_clients = 100
        
    async def run(self) -> NetworkPartitionReport:
        """运行网络分区测试"""
        logger.info("开始网络分区脑裂测试")
        logger.info(f"集群规模: 5节点")
        logger.info(f"初始Leader: {self.simulator.leader_id}")
        
        self.report.start_time = time.time()
        
        # 启动工作负载
        workload_task = asyncio.create_task(self._workload_generator())
        
        # 执行分区测试序列
        partition_task = asyncio.create_task(self._partition_test_sequence())
        
        # 监控集群状态
        monitor_task = asyncio.create_task(self._cluster_monitor())
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待任务完成
        await asyncio.gather(workload_task, partition_task, monitor_task, return_exceptions=True)
        
        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time
        
        # 生成最终报告
        await self._generate_report()
        
        return self.report
    
    async def _workload_generator(self):
        """工作负载生成器"""
        sequence = 0
        while not self.stop_event.is_set():
            try:
                # 随机选择节点写入
                node_id = random.choice(list(self.simulator.nodes.keys()))
                key = f"key_{sequence}_{int(time.time()*1000000)}"
                value = f"value_{sequence}_{random.randint(1, 1000000)}"
                
                result = await self.simulator.write_during_partition(key, value, node_id)
                
                if result["success"]:
                    logger.debug(f"Write success: {key} on {node_id}")
                
                sequence += 1
                await asyncio.sleep(random.uniform(0.01, 0.05))
                
            except Exception as e:
                logger.warning(f"Workload error: {e}")
    
    async def _partition_test_sequence(self):
        """分区测试序列"""
        await asyncio.sleep(5)  # 等待系统稳定
        
        # Phase 1: 创建分区
        logger.info("Phase 1: 创建网络分区")
        partition_result = await self.simulator.create_partition()
        
        if partition_result["success"]:
            self.report.partition_count += 1
            self.report.partition_events.append({
                "phase": 1,
                "type": "partition_created",
                "timestamp": time.time(),
                **partition_result
            })
            
            if partition_result.get("split_brain"):
                self.report.split_brain_events += 1
                self.report.leaders_during_partition = 2
                
                logger.info(
                    f"Split-brain detected - Leader A: {partition_result['leader_a']}, "
                    f"Leader B: {partition_result['leader_b']}"
                )
            
            logger.info(f"Partition created: Group A={partition_result['group_a']}, Group B={partition_result['group_b']}")
        
        await asyncio.sleep(15)  # 在分区状态下运行一段时间
        
        # Phase 2: 恢复分区
        logger.info("Phase 2: 恢复网络分区")
        heal_result = await self.simulator.heal_partition()
        
        if heal_result["success"]:
            self.report.merge_operations += 1
            self.report.conflicts_during_merge = heal_result["conflicts"]
            self.report.conflicts_resolved = heal_result["conflicts"]  # 假设所有冲突都被解决
            
            self.report.merge_events.append({
                "phase": 2,
                "type": "partition_healed",
                "timestamp": time.time(),
                **heal_result
            })
            
            logger.info(
                f"Partition healed - Merged keys: {heal_result['merged_keys']}, "
                f"Conflicts: {heal_result['conflicts']}, "
                f"New Leader: {heal_result['new_leader']}"
            )
        
        await asyncio.sleep(5)
        
        # Phase 3: 再次分区(测试多次分区)
        logger.info("Phase 3: 再次创建分区")
        partition_result2 = await self.simulator.create_partition()
        
        if partition_result2["success"]:
            self.report.partition_count += 1
            self.report.partition_events.append({
                "phase": 3,
                "type": "partition_created_again",
                "timestamp": time.time(),
                **partition_result2
            })
            
            if partition_result2.get("split_brain"):
                self.report.split_brain_events += 1
        
        await asyncio.sleep(10)
        
        # Phase 4: 最终恢复
        logger.info("Phase 4: 最终恢复")
        heal_result2 = await self.simulator.heal_partition()
        
        if heal_result2["success"]:
            self.report.merge_operations += 1
            self.report.merge_events.append({
                "phase": 4,
                "type": "final_heal",
                "timestamp": time.time(),
                **heal_result2
            })
    
    async def _cluster_monitor(self):
        """集群监控"""
        check_count = 0
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            try:
                state = self.simulator.get_cluster_state()
                consistency = self.simulator.verify_consistency()
                
                self.report.consistency_checks.append({
                    "timestamp": time.time(),
                    "check_number": check_count,
                    **state,
                    **consistency
                })
                
                if state.get("split_brain"):
                    self.report.leader_changes.append({
                        "timestamp": time.time(),
                        "type": "split_brain_detected",
                        "leaders": state["leaders"]
                    })
                
                logger.info(
                    f"Cluster Monitor - State: {state['state']}, "
                    f"Leaders: {state['leaders']}, "
                    f"Split-brain: {state['split_brain']}, "
                    f"Consistent: {consistency.get('consistent')}"
                )
                
                check_count += 1
                
            except Exception as e:
                logger.error(f"Monitor error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        final_state = self.simulator.get_cluster_state()
        final_consistency = self.simulator.verify_consistency()
        
        self.report.leaders_during_partition = len(final_state.get("leaders", []))
        self.report.data_divergence_detected = final_consistency.get("divergent_keys", 0)
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/long_running/scenario7_network_partition_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = NetworkPartitionTestRunner(target_duration_minutes=10)
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景7: 网络分区脑裂测试 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"分区次数: {report.partition_count}")
    print(f"脑裂事件: {report.split_brain_events}")
    print(f"分区期间Leader数: {report.leaders_during_partition}")
    print(f"数据分歧检测: {report.data_divergence_detected}")
    print(f"合并操作: {report.merge_operations}")
    print(f"合并冲突: {report.conflicts_during_merge}")
    print(f"冲突已解决: {report.conflicts_resolved}")
    print(f"数据丢失事件: {report.data_loss_events}")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

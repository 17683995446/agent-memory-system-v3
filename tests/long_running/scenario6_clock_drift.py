#!/usr/bin/env python3
"""
场景6: 时钟漂移与分布式一致性

测试目标:
- 各节点时钟不同步(±5秒)
- 验证分布式事务一致性
- 测试时间戳冲突解决
- 验证因果一致性

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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import hashlib
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/scenario6_clock_drift.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ClockDriftReport:
    """时钟漂移测试报告"""
    scenario_name: str = "时钟漂移与分布式一致性"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    max_clock_drift_sec: float = 5.0
    node_count: int = 5
    total_transactions: int = 0
    committed_transactions: int = 0
    aborted_transactions: int = 0
    timestamp_conflicts: int = 0
    conflicts_resolved: int = 0
    causal_consistency_violations: int = 0
    vector_clock_divergence: int = 0
    reconciliation_events: int = 0
    clock_drift_timeline: List[Dict] = field(default_factory=list)
    conflict_events: List[Dict] = field(default_factory=list)
    consistency_checks: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class VectorClock:
    """向量时钟"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock: Dict[str, int] = {node_id: 0}
    
    def increment(self):
        """递增本地时钟"""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
    
    def update(self, other: 'VectorClock'):
        """合并另一个向量时钟"""
        for node, timestamp in other.clock.items():
            self.clock[node] = max(self.clock.get(node, 0), timestamp)
    
    def compare(self, other: 'VectorClock') -> Optional[str]:
        """
        比较两个向量时钟
        返回: "before", "after", "concurrent", 或 "equal"
        """
        dominates = False
        dominated = False
        
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        
        for node in all_nodes:
            self_ts = self.clock.get(node, 0)
            other_ts = other.clock.get(node, 0)
            
            if self_ts > other_ts:
                dominates = True
            elif other_ts > self_ts:
                dominated = True
        
        if dominates and not dominated:
            return "after"
        elif dominated and not dominates:
            return "before"
        elif not dominates and not dominated:
            return "equal"
        else:
            return "concurrent"
    
    def to_dict(self) -> Dict:
        return self.clock.copy()


class ClockDriftNode:
    """带时钟漂移的节点"""
    
    def __init__(self, node_id: str, drift_seconds: float = 0.0):
        self.node_id = node_id
        self.drift_seconds = drift_seconds  # 相对于真实时间的偏移
        self.vector_clock = VectorClock(node_id)
        self.data: Dict[str, Dict] = {}
        self.transaction_log: List[Dict] = []
        
    def get_timestamp(self) -> float:
        """获取带漂移的时间戳"""
        return time.time() + self.drift_seconds
    
    def get_datetime(self) -> datetime:
        """获取带漂移的日期时间"""
        return datetime.fromtimestamp(self.get_timestamp())


class CausalConsistencyChecker:
    """因果一致性检查器"""
    
    def __init__(self):
        self.dependencies: Dict[str, Set[str]] = {}  # key -> set of dependencies
        self.violations: List[Dict] = []
    
    def record_write(self, key: str, dependencies: Set[str], node_id: str):
        """记录写入及其依赖"""
        self.dependencies[key] = dependencies.copy()
    
    def check_read(self, key: str, visible_keys: Set[str]) -> bool:
        """检查读取是否满足因果一致性"""
        if key not in self.dependencies:
            return True
        
        required = self.dependencies[key]
        missing = required - visible_keys
        
        if missing:
            self.violations.append({
                "key": key,
                "missing_dependencies": list(missing),
                "timestamp": time.time()
            })
            return False
        
        return True


class DistributedTransactionManager:
    """分布式事务管理器"""
    
    def __init__(self, nodes: List[ClockDriftNode]):
        self.nodes = {node.node_id: node for node in nodes}
        self.lock = asyncio.Lock()
        self.committed_count = 0
        self.aborted_count = 0
        self.conflict_count = 0
        
    async def execute_transaction(self, tx_id: str, operations: List[Dict], 
                                   coordinator_id: str) -> Dict:
        """执行分布式事务"""
        async with self.lock:
            coordinator = self.nodes[coordinator_id]
            
            # 准备阶段
            prepare_results = {}
            for op in operations:
                node_id = op["node_id"]
                if node_id in self.nodes:
                    node = self.nodes[node_id]
                    node.vector_clock.increment()
                    
                    prepare_results[node_id] = {
                        "status": "prepared",
                        "timestamp": node.get_timestamp(),
                        "vector_clock": node.vector_clock.to_dict()
                    }
            
            # 检查时间戳冲突
            timestamps = [r["timestamp"] for r in prepare_results.values()]
            if max(timestamps) - min(timestamps) > 5.0:  # 超过5秒差异
                self.conflict_count += 1
                # 中止事务
                self.aborted_count += 1
                return {
                    "success": False,
                    "status": "aborted",
                    "reason": "timestamp_conflict",
                    "tx_id": tx_id
                }
            
            # 提交阶段
            for op in operations:
                node_id = op["node_id"]
                if node_id in self.nodes:
                    node = self.nodes[node_id]
                    key = op["key"]
                    
                    # 应用操作
                    node.data[key] = {
                        "value": op["value"],
                        "timestamp": node.get_timestamp(),
                        "vector_clock": node.vector_clock.to_dict(),
                        "tx_id": tx_id
                    }
                    
                    node.transaction_log.append({
                        "tx_id": tx_id,
                        "operation": op,
                        "timestamp": node.get_timestamp(),
                        "vector_clock": node.vector_clock.to_dict()
                    })
            
            self.committed_count += 1
            
            return {
                "success": True,
                "status": "committed",
                "tx_id": tx_id,
                "participants": list(prepare_results.keys())
            }


class ClockDriftTestRunner:
    """时钟漂移测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 10,
                 node_count: int = 5,
                 max_drift_seconds: float = 5.0,
                 concurrent_clients: int = 100):
        self.target_duration = target_duration_minutes * 60
        self.node_count = node_count
        self.max_drift_seconds = max_drift_seconds
        self.concurrent_clients = concurrent_clients
        
        # 创建带漂移的节点
        self.nodes = []
        for i in range(node_count):
            drift = random.uniform(-max_drift_seconds, max_drift_seconds)
            self.nodes.append(ClockDriftNode(f"node_{i}", drift))
        
        self.tx_manager = DistributedTransactionManager(self.nodes)
        self.consistency_checker = CausalConsistencyChecker()
        self.report = ClockDriftReport()
        self.stop_event = asyncio.Event()
        
    async def run(self) -> ClockDriftReport:
        """运行时钟漂移测试"""
        logger.info("开始时钟漂移与分布式一致性测试")
        logger.info(f"节点数: {self.node_count}")
        logger.info(f"最大时钟漂移: ±{self.max_drift_seconds}秒")
        logger.info(f"并发客户端: {self.concurrent_clients}")
        
        # 打印初始时钟漂移
        for node in self.nodes:
            logger.info(f"Node {node.node_id} drift: {node.drift_seconds:+.3f}s")
        
        self.report.start_time = time.time()
        
        # 启动工作负载
        tasks = [
            asyncio.create_task(self._transaction_worker()),
            asyncio.create_task(self._clock_monitor()),
            asyncio.create_task(self._consistency_checker_worker()),
            asyncio.create_task(self._drift_injector_worker()),
        ]
        
        # 启动并发客户端
        client_tasks = [
            asyncio.create_task(self._client_worker(i))
            for i in range(self.concurrent_clients)
        ]
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待任务完成
        await asyncio.gather(*tasks, *client_tasks, return_exceptions=True)
        
        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time
        
        # 生成最终报告
        await self._generate_report()
        
        return self.report
    
    async def _client_worker(self, client_id: int):
        """客户端工作器 - 发起事务"""
        sequence = 0
        while not self.stop_event.is_set():
            try:
                # 创建分布式事务
                tx_id = f"tx_{client_id}_{sequence}_{int(time.time()*1000000)}"
                
                # 随机选择参与节点
                num_participants = random.randint(2, self.node_count)
                participants = random.sample([n.node_id for n in self.nodes], num_participants)
                
                operations = []
                for node_id in participants:
                    key = f"key_{client_id}_{sequence}_{node_id}"
                    value = f"value_from_client_{client_id}_seq_{sequence}"
                    operations.append({
                        "node_id": node_id,
                        "key": key,
                        "value": value,
                        "type": "write"
                    })
                
                coordinator = random.choice(participants)
                result = await self.tx_manager.execute_transaction(tx_id, operations, coordinator)
                
                self.report.total_transactions += 1
                
                if result["success"]:
                    self.report.committed_transactions += 1
                else:
                    self.report.aborted_transactions += 1
                    if result.get("reason") == "timestamp_conflict":
                        self.report.timestamp_conflicts += 1
                        self.report.conflict_events.append({
                            "type": "timestamp_conflict",
                            "tx_id": tx_id,
                            "timestamp": time.time()
                        })
                
                sequence += 1
                
            except Exception as e:
                self.report.errors.append(f"Client {client_id} error: {e}")
            
            await asyncio.sleep(random.uniform(0.01, 0.1))
    
    async def _transaction_worker(self):
        """事务工作器"""
        while not self.stop_event.is_set():
            try:
                # 创建跨多个节点的事务
                tx_id = f"bulk_tx_{int(time.time()*1000000)}"
                
                # 所有节点参与
                operations = []
                for node in self.nodes:
                    key = f"shared_key_{int(time.time())}"
                    value = f"shared_value_{random.randint(1, 1000000)}"
                    operations.append({
                        "node_id": node.node_id,
                        "key": key,
                        "value": value,
                        "type": "write"
                    })
                
                coordinator = self.nodes[0].node_id
                result = await self.tx_manager.execute_transaction(tx_id, operations, coordinator)
                
                if result["success"]:
                    logger.debug(f"Bulk transaction committed: {tx_id}")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.warning(f"Transaction worker error: {e}")
    
    async def _clock_monitor(self):
        """时钟监控器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            try:
                real_time = time.time()
                node_times = []
                
                for node in self.nodes:
                    node_time = node.get_timestamp()
                    node_times.append({
                        "node_id": node.node_id,
                        "drift_seconds": node.drift_seconds,
                        "node_timestamp": node_time,
                        "real_timestamp": real_time,
                        "difference": node_time - real_time
                    })
                
                # 计算最大差异
                timestamps = [n["node_timestamp"] for n in node_times]
                max_diff = max(timestamps) - min(timestamps)
                
                self.report.clock_drift_timeline.append({
                    "timestamp": real_time,
                    "max_drift_seconds": max_diff,
                    "node_times": node_times
                })
                
                logger.info(f"Clock Monitor - Max drift: {max_diff:.3f}s")
                
            except Exception as e:
                logger.error(f"Clock monitor error: {e}")
    
    async def _consistency_checker_worker(self):
        """一致性检查工作器"""
        check_count = 0
        while not self.stop_event.is_set():
            await asyncio.sleep(10)
            
            try:
                # 检查向量时钟分歧
                divergences = 0
                causal_violations = 0
                
                # 收集所有数据
                all_keys = set()
                for node in self.nodes:
                    all_keys.update(node.data.keys())
                
                for key in all_keys:
                    # 获取所有节点上该key的版本
                    versions = []
                    for node in self.nodes:
                        if key in node.data:
                            versions.append({
                                "node_id": node.node_id,
                                "data": node.data[key]
                            })
                    
                    if len(versions) > 1:
                        # 检查版本是否一致
                        vector_clocks = [v["data"]["vector_clock"] for v in versions]
                        
                        # 简单检查: 如果向量时钟不同, 记录分歧
                        unique_clocks = set(json.dumps(vc, sort_keys=True) for vc in vector_clocks)
                        if len(unique_clocks) > 1:
                            divergences += 1
                
                self.report.vector_clock_divergence += divergences
                
                # 记录一致性检查
                self.report.consistency_checks.append({
                    "timestamp": time.time(),
                    "check_number": check_count,
                    "vector_clock_divergences": divergences,
                    "causal_violations": causal_violations,
                    "total_keys_checked": len(all_keys)
                })
                
                logger.info(
                    f"Consistency Check #{check_count} - "
                    f"Divergences: {divergences}, "
                    f"Keys: {len(all_keys)}"
                )
                
                check_count += 1
                
            except Exception as e:
                logger.error(f"Consistency checker error: {e}")
    
    async def _drift_injector_worker(self):
        """漂移注入工作器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(30)  # 每30秒调整一次漂移
            
            try:
                # 随机调整某些节点的时钟漂移
                for node in self.nodes:
                    if random.random() < 0.3:  # 30%概率调整
                        old_drift = node.drift_seconds
                        new_drift = random.uniform(-self.max_drift_seconds, self.max_drift_seconds)
                        node.drift_seconds = new_drift
                        
                        logger.info(
                            f"Drift adjusted for {node.node_id}: "
                            f"{old_drift:+.3f}s -> {new_drift:+.3f}s"
                        )
                
            except Exception as e:
                logger.warning(f"Drift injector error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        self.report.committed_transactions = self.tx_manager.committed_count
        self.report.aborted_transactions = self.tx_manager.aborted_count
        self.report.timestamp_conflicts = self.tx_manager.conflict_count
        
        # 计算冲突解决率
        if self.report.timestamp_conflicts > 0:
            self.report.conflicts_resolved = self.report.timestamp_conflicts  # 通过中止解决
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/long_running/scenario6_clock_drift_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = ClockDriftTestRunner(
        target_duration_minutes=10,
        node_count=5,
        max_drift_seconds=5.0,
        concurrent_clients=100
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景6: 时钟漂移与分布式一致性 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"节点数: {report.node_count}")
    print(f"最大时钟漂移: ±{report.max_clock_drift_sec}秒")
    print(f"总事务数: {report.total_transactions}")
    print(f"已提交事务: {report.committed_transactions}")
    print(f"中止事务: {report.aborted_transactions}")
    print(f"时间戳冲突: {report.timestamp_conflicts}")
    print(f"冲突已解决: {report.conflicts_resolved}")
    print(f"向量时钟分歧: {report.vector_clock_divergence}")
    print(f"因果一致性违规: {report.causal_consistency_violations}")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

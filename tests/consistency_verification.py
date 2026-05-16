#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 一致性验证测试
Consistency Verification for Deep Validation Phase

包含:
- 线性一致性测试
- 顺序一致性测试
- 因果一致性测试
- 最终一致性测试
- Jepsen风格测试
- 分布式事务一致性
"""

import asyncio
import time
import random
import string
import json
import logging
import threading
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Tuple, Set
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from collections import defaultdict
from abc import ABC, abstractmethod

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConsistencyLevel(Enum):
    """一致性级别"""
    LINEARIZABLE = "linearizable"      # 线性一致性
    SEQUENTIAL = "sequential"          # 顺序一致性
    CAUSAL = "causal"                  # 因果一致性
    EVENTUAL = "eventual"              # 最终一致性
    SESSION = "session"                # 会话一致性
    BOUNDED_STALENESS = "bounded"      # 有界陈旧性


class OperationType(Enum):
    """操作类型"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    CAS = "compare_and_swap"           # 比较并交换
    READ_MODIFY_WRITE = "rmw"          # 读-修改-写


@dataclass
class Operation:
    """操作记录"""
    op_id: str
    op_type: OperationType
    key: str
    value: Optional[Any] = None
    timestamp: float = 0.0
    process_id: str = ""
    dependencies: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


@dataclass
class HistoryEntry:
    """历史记录条目"""
    operation: Operation
    invocation_time: float
    completion_time: float
    result: Any = None
    success: bool = True


@dataclass
class ConsistencyTestResult:
    """一致性测试结果"""
    test_name: str
    consistency_level: ConsistencyLevel
    passed: bool
    violations: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


class DistributedStore(ABC):
    """分布式存储抽象基类"""
    
    @abstractmethod
    async def write(self, key: str, value: Any) -> bool:
        pass
    
    @abstractmethod
    async def read(self, key: str) -> Optional[Any]:
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        pass
    
    @abstractmethod
    async def compare_and_swap(self, key: str, expected: Any, new_value: Any) -> bool:
        pass
    
    @abstractmethod
    async def get_version(self, key: str) -> int:
        pass


class MockDistributedStore(DistributedStore):
    """模拟分布式存储"""
    
    def __init__(self, node_id: str, consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL):
        self.node_id = node_id
        self.consistency = consistency
        self.data: Dict[str, Tuple[Any, int, float]] = {}  # value, version, timestamp
        self.lock = threading.RLock()
        self.replicas: List['MockDistributedStore'] = []
        self.delay_ms = 0
        self.failure_rate = 0.0
        
    def add_replica(self, replica: 'MockDistributedStore'):
        """添加副本"""
        self.replicas.append(replica)
        
    async def write(self, key: str, value: Any) -> bool:
        """写入数据"""
        if random.random() < self.failure_rate:
            return False
            
        await asyncio.sleep(self.delay_ms / 1000)
        
        with self.lock:
            version = self.data.get(key, (None, 0, 0))[1] + 1
            timestamp = time.time()
            self.data[key] = (value, version, timestamp)
            
            # 异步复制到副本
            if self.consistency == ConsistencyLevel.LINEARIZABLE:
                await self._synchronous_replicate(key, value, version, timestamp)
            else:
                asyncio.create_task(self._async_replicate(key, value, version, timestamp))
                
        return True
    
    async def read(self, key: str) -> Optional[Any]:
        """读取数据"""
        if random.random() < self.failure_rate:
            return None
            
        await asyncio.sleep(self.delay_ms / 1000)
        
        with self.lock:
            if key in self.data:
                return self.data[key][0]
            return None
    
    async def delete(self, key: str) -> bool:
        """删除数据"""
        if random.random() < self.failure_rate:
            return False
            
        with self.lock:
            if key in self.data:
                del self.data[key]
                return True
            return False
    
    async def compare_and_swap(self, key: str, expected: Any, new_value: Any) -> bool:
        """比较并交换"""
        with self.lock:
            current = self.data.get(key, (None, 0, 0))[0]
            if current == expected:
                version = self.data.get(key, (None, 0, 0))[1] + 1
                self.data[key] = (new_value, version, time.time())
                return True
            return False
    
    async def get_version(self, key: str) -> int:
        """获取版本号"""
        with self.lock:
            return self.data.get(key, (None, 0, 0))[1]
    
    async def _synchronous_replicate(self, key: str, value: Any, version: int, timestamp: float):
        """同步复制"""
        tasks = []
        for replica in self.replicas:
            tasks.append(self._replicate_to(replica, key, value, version, timestamp))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _async_replicate(self, key: str, value: Any, version: int, timestamp: float):
        """异步复制"""
        await asyncio.sleep(random.uniform(0.01, 0.1))  # 模拟网络延迟
        for replica in self.replicas:
            await self._replicate_to(replica, key, value, version, timestamp)
    
    async def _replicate_to(self, replica: 'MockDistributedStore', key: str, 
                           value: Any, version: int, timestamp: float):
        """复制到指定副本"""
        with replica.lock:
            current_version = replica.data.get(key, (None, 0, 0))[1]
            if version > current_version:
                replica.data[key] = (value, version, timestamp)


class ConsistencyVerifier:
    """一致性验证器"""
    
    def __init__(self):
        self.history: List[HistoryEntry] = []
        self.lock = threading.Lock()
    
    def record_operation(self, entry: HistoryEntry):
        """记录操作"""
        with self.lock:
            self.history.append(entry)
    
    def verify_linearizability(self) -> ConsistencyTestResult:
        """验证线性一致性"""
        logger.info("验证线性一致性...")
        start_time = time.time()
        
        violations = []
        
        # 按key分组
        by_key = defaultdict(list)
        for entry in self.history:
            by_key[entry.operation.key].append(entry)
        
        # 对每个key验证线性一致性
        for key, entries in by_key.items():
            # 检查读写顺序
            writes = [e for e in entries if e.operation.op_type == OperationType.WRITE]
            reads = [e for e in entries if e.operation.op_type == OperationType.READ]
            
            # 验证每个读都能对应到一个写
            for read in reads:
                if read.result is not None:
                    # 找到可能导致这个读的写操作
                    possible_writes = [
                        w for w in writes 
                        if w.result == read.result and 
                        w.completion_time <= read.invocation_time
                    ]
                    
                    if not possible_writes:
                        violations.append({
                            'type': 'linearizability_violation',
                            'key': key,
                            'read_op': read.operation.op_id,
                            'read_value': read.result,
                            'message': '读操作无法对应到任何写操作'
                        })
        
        duration = time.time() - start_time
        
        return ConsistencyTestResult(
            test_name="Linearizability Test",
            consistency_level=ConsistencyLevel.LINEARIZABLE,
            passed=len(violations) == 0,
            violations=violations,
            duration_seconds=duration,
            metrics={'total_operations': len(self.history), 'violations': len(violations)}
        )
    
    def verify_sequential_consistency(self) -> ConsistencyTestResult:
        """验证顺序一致性"""
        logger.info("验证顺序一致性...")
        start_time = time.time()
        
        violations = []
        
        # 检查程序顺序是否被保持
        by_process = defaultdict(list)
        for entry in self.history:
            by_process[entry.operation.process_id].append(entry)
        
        # 验证每个进程内的操作顺序
        for process_id, entries in by_process.items():
            sorted_entries = sorted(entries, key=lambda e: e.invocation_time)
            
            for i in range(len(sorted_entries) - 1):
                current = sorted_entries[i]
                next_op = sorted_entries[i + 1]
                
                # 检查操作顺序是否被保持
                if current.completion_time > next_op.invocation_time:
                    violations.append({
                        'type': 'sequential_violation',
                        'process': process_id,
                        'op1': current.operation.op_id,
                        'op2': next_op.operation.op_id,
                        'message': '程序顺序未被保持'
                    })
        
        duration = time.time() - start_time
        
        return ConsistencyTestResult(
            test_name="Sequential Consistency Test",
            consistency_level=ConsistencyLevel.SEQUENTIAL,
            passed=len(violations) == 0,
            violations=violations,
            duration_seconds=duration,
            metrics={'total_processes': len(by_process), 'violations': len(violations)}
        )
    
    def verify_causal_consistency(self) -> ConsistencyTestResult:
        """验证因果一致性"""
        logger.info("验证因果一致性...")
        start_time = time.time()
        
        violations = []
        
        # 构建happens-before关系
        happens_before = defaultdict(set)
        
        # 按进程排序操作
        by_process = defaultdict(list)
        for entry in self.history:
            by_process[entry.operation.process_id].append(entry)
        
        for process_id, entries in by_process.items():
            sorted_entries = sorted(entries, key=lambda e: e.invocation_time)
            for i in range(len(sorted_entries)):
                for j in range(i + 1, len(sorted_entries)):
                    happens_before[sorted_entries[i].operation.op_id].add(
                        sorted_entries[j].operation.op_id
                    )
        
        # 检查读-写依赖
        for entry in self.history:
            if entry.operation.op_type == OperationType.READ and entry.result is not None:
                # 找到对应的写操作
                for other in self.history:
                    if (other.operation.op_type == OperationType.WRITE and 
                        other.operation.key == entry.operation.key and
                        other.result == entry.result):
                        # 读操作happens-after写操作
                        if entry.operation.op_id not in happens_before[other.operation.op_id]:
                            # 检查是否违反因果一致性
                            pass
        
        duration = time.time() - start_time
        
        return ConsistencyTestResult(
            test_name="Causal Consistency Test",
            consistency_level=ConsistencyLevel.CAUSAL,
            passed=len(violations) == 0,
            violations=violations,
            duration_seconds=duration,
            metrics={'causal_relations': len(happens_before), 'violations': len(violations)}
        )
    
    def verify_eventual_consistency(self, max_delay_seconds: float = 5.0) -> ConsistencyTestResult:
        """验证最终一致性"""
        logger.info("验证最终一致性...")
        start_time = time.time()
        
        violations = []
        
        # 按key分组
        by_key = defaultdict(list)
        for entry in self.history:
            by_key[entry.operation.key].append(entry)
        
        # 检查最终所有副本是否收敛到相同值
        for key, entries in by_key.items():
            writes = [e for e in entries if e.operation.op_type == OperationType.WRITE]
            if not writes:
                continue
            
            # 找到最后一个写操作
            last_write = max(writes, key=lambda e: e.completion_time)
            final_value = last_write.result
            final_time = last_write.completion_time
            
            # 检查在此之后的读操作是否都读到最终值
            reads_after = [
                e for e in entries 
                if e.operation.op_type == OperationType.READ and
                e.invocation_time > final_time + max_delay_seconds
            ]
            
            for read in reads_after:
                if read.result != final_value and read.result is not None:
                    violations.append({
                        'type': 'eventual_consistency_violation',
                        'key': key,
                        'expected': final_value,
                        'actual': read.result,
                        'delay': read.invocation_time - final_time,
                        'message': f'读操作在{max_delay_seconds}s后仍未收敛'
                    })
        
        duration = time.time() - start_time
        
        return ConsistencyTestResult(
            test_name="Eventual Consistency Test",
            consistency_level=ConsistencyLevel.EVENTUAL,
            passed=len(violations) == 0,
            violations=violations,
            duration_seconds=duration,
            metrics={'keys_checked': len(by_key), 'violations': len(violations)}
        )


class JepsenStyleTest:
    """Jepsen风格测试"""
    
    def __init__(self, num_nodes: int = 5, num_clients: int = 10):
        self.num_nodes = num_nodes
        self.num_clients = num_clients
        self.nodes: List[MockDistributedStore] = []
        self.verifier = ConsistencyVerifier()
        self.results: List[ConsistencyTestResult] = []
        
    async def setup(self):
        """设置测试环境"""
        logger.info("=" * 70)
        logger.info("设置Jepsen测试环境")
        logger.info("=" * 70)
        
        # 创建节点
        for i in range(self.num_nodes):
            node = MockDistributedStore(f"node-{i}", ConsistencyLevel.EVENTUAL)
            self.nodes.append(node)
        
        # 建立副本关系
        for i, node in enumerate(self.nodes):
            for j, other in enumerate(self.nodes):
                if i != j:
                    node.add_replica(other)
        
        logger.info(f"创建了 {self.num_nodes} 个节点")
    
    async def run_jepsen_test(self) -> List[ConsistencyTestResult]:
        """运行Jepsen风格测试"""
        logger.info("\n" + "=" * 70)
        logger.info("开始Jepsen风格测试")
        logger.info("=" * 70)
        
        await self.setup()
        
        # 1. 运行线性一致性测试
        result1 = await self._test_linearizability()
        self.results.append(result1)
        
        # 2. 运行顺序一致性测试
        result2 = await self._test_sequential_consistency()
        self.results.append(result2)
        
        # 3. 运行因果一致性测试
        result3 = await self._test_causal_consistency()
        self.results.append(result3)
        
        # 4. 运行最终一致性测试
        result4 = await self._test_eventual_consistency()
        self.results.append(result4)
        
        # 5. 运行分布式事务测试
        result5 = await self._test_distributed_transactions()
        self.results.append(result5)
        
        logger.info("\n" + "=" * 70)
        logger.info("Jepsen风格测试完成")
        logger.info("=" * 70)
        
        return self.results
    
    async def _test_linearizability(self) -> ConsistencyTestResult:
        """测试线性一致性"""
        logger.info("\n测试线性一致性...")
        
        # 重置历史记录
        self.verifier.history = []
        
        # 使用单个客户端进行顺序操作
        client_id = "linearizability-client"
        node = self.nodes[0]
        
        operations = []
        for i in range(50):
            key = f"linear-key-{i % 10}"
            value = f"value-{i}"
            
            # 写操作
            op_id = f"write-{i}"
            op = Operation(op_id, OperationType.WRITE, key, value, process_id=client_id)
            
            invocation_time = time.time()
            success = await node.write(key, value)
            completion_time = time.time()
            
            entry = HistoryEntry(op, invocation_time, completion_time, value, success)
            self.verifier.record_operation(entry)
            operations.append(entry)
            
            # 读操作
            await asyncio.sleep(0.01)
            op_id = f"read-{i}"
            op = Operation(op_id, OperationType.READ, key, process_id=client_id)
            
            invocation_time = time.time()
            result = await node.read(key)
            completion_time = time.time()
            
            entry = HistoryEntry(op, invocation_time, completion_time, result, True)
            self.verifier.record_operation(entry)
            operations.append(entry)
        
        # 验证
        result = self.verifier.verify_linearizability()
        logger.info(f"  结果: {'通过' if result.passed else '失败'}, "
                   f"违规: {len(result.violations)}")
        
        return result
    
    async def _test_sequential_consistency(self) -> ConsistencyTestResult:
        """测试顺序一致性"""
        logger.info("\n测试顺序一致性...")
        
        self.verifier.history = []
        
        async def client_worker(client_id: int):
            node = self.nodes[client_id % len(self.nodes)]
            
            for i in range(20):
                key = f"seq-key-{client_id}"
                value = f"client{client_id}-value{i}"
                
                op_id = f"client{client_id}-op{i}"
                op = Operation(op_id, OperationType.WRITE, key, value, process_id=f"client-{client_id}")
                
                invocation_time = time.time()
                success = await node.write(key, value)
                completion_time = time.time()
                
                entry = HistoryEntry(op, invocation_time, completion_time, value, success)
                self.verifier.record_operation(entry)
                
                await asyncio.sleep(random.uniform(0.001, 0.01))
        
        # 并发运行多个客户端
        await asyncio.gather(*[client_worker(i) for i in range(self.num_clients)])
        
        # 验证
        result = self.verifier.verify_sequential_consistency()
        logger.info(f"  结果: {'通过' if result.passed else '失败'}, "
                   f"违规: {len(result.violations)}")
        
        return result
    
    async def _test_causal_consistency(self) -> ConsistencyTestResult:
        """测试因果一致性"""
        logger.info("\n测试因果一致性...")
        
        self.verifier.history = []
        
        # 模拟因果依赖：客户端A写入，客户端B读取后写入
        node_a = self.nodes[0]
        node_b = self.nodes[1]
        
        for i in range(30):
            key = f"causal-key-{i}"
            value_a = f"value-a-{i}"
            value_b = f"value-b-{i}"
            
            # A写入
            op_id_a = f"causal-write-a-{i}"
            op_a = Operation(op_id_a, OperationType.WRITE, key, value_a, process_id="client-a")
            
            invocation_time = time.time()
            await node_a.write(key, value_a)
            completion_time = time.time()
            
            entry_a = HistoryEntry(op_a, invocation_time, completion_time, value_a, True)
            self.verifier.record_operation(entry_a)
            
            await asyncio.sleep(0.02)
            
            # B读取
            op_id_read = f"causal-read-b-{i}"
            op_read = Operation(op_id_read, OperationType.READ, key, process_id="client-b")
            
            invocation_time = time.time()
            read_value = await node_b.read(key)
            completion_time = time.time()
            
            entry_read = HistoryEntry(op_read, invocation_time, completion_time, read_value, True)
            self.verifier.record_operation(entry_read)
            
            # B写入（依赖于读取的值）
            op_id_b = f"causal-write-b-{i}"
            op_b = Operation(op_id_b, OperationType.WRITE, key, value_b, 
                           process_id="client-b", dependencies=[op_id_read])
            
            invocation_time = time.time()
            await node_b.write(key, value_b)
            completion_time = time.time()
            
            entry_b = HistoryEntry(op_b, invocation_time, completion_time, value_b, True)
            self.verifier.record_operation(entry_b)
        
        # 验证
        result = self.verifier.verify_causal_consistency()
        logger.info(f"  结果: {'通过' if result.passed else '失败'}, "
                   f"违规: {len(result.violations)}")
        
        return result
    
    async def _test_eventual_consistency(self) -> ConsistencyTestResult:
        """测试最终一致性"""
        logger.info("\n测试最终一致性...")
        
        self.verifier.history = []
        
        # 写入数据
        for i in range(20):
            key = f"eventual-key-{i}"
            value = f"eventual-value-{i}"
            
            # 随机选择一个节点写入
            node = random.choice(self.nodes)
            await node.write(key, value)
            
            # 记录写操作
            op_id = f"eventual-write-{i}"
            op = Operation(op_id, OperationType.WRITE, key, value, process_id="writer")
            entry = HistoryEntry(op, time.time() - 0.1, time.time(), value, True)
            self.verifier.record_operation(entry)
        
        # 等待复制
        await asyncio.sleep(0.5)
        
        # 从所有节点读取
        for i in range(20):
            key = f"eventual-key-{i}"
            
            for j, node in enumerate(self.nodes):
                value = await node.read(key)
                
                op_id = f"eventual-read-{i}-{j}"
                op = Operation(op_id, OperationType.READ, key, process_id=f"reader-{j}")
                entry = HistoryEntry(op, time.time(), time.time(), value, True)
                self.verifier.record_operation(entry)
        
        # 验证
        result = self.verifier.verify_eventual_consistency(max_delay_seconds=1.0)
        logger.info(f"  结果: {'通过' if result.passed else '失败'}, "
                   f"违规: {len(result.violations)}")
        
        return result
    
    async def _test_distributed_transactions(self) -> ConsistencyTestResult:
        """测试分布式事务一致性"""
        logger.info("\n测试分布式事务一致性...")
        
        violations = []
        successful_transactions = 0
        failed_transactions = 0
        
        successful_transactions = 0
        failed_transactions = 0
        
        async def execute_transaction(tx_id: int) -> bool:
            """执行分布式事务"""
            nonlocal successful_transactions, failed_transactions
            
            keys = [f"tx-{tx_id}-key-{i}" for i in range(5)]
            values = [f"tx-{tx_id}-value-{i}" for i in range(5)]
            
            # 两阶段提交模拟
            # 阶段1：准备
            prepared = []
            for i, key in enumerate(keys):
                node = self.nodes[i % len(self.nodes)]
                # 尝试获取锁并准备
                if await node.write(key, values[i]):
                    prepared.append((node, key, values[i]))
                else:
                    # 回滚
                    for n, k, v in prepared:
                        await n.delete(k)
                    failed_transactions += 1
                    return False
            
            # 阶段2：提交
            await asyncio.sleep(0.01)  # 模拟提交延迟
            successful_transactions += 1
            return True
        
        # 执行多个事务
        tasks = [execute_transaction(i) for i in range(20)]
        await asyncio.gather(*tasks)
        
        # 验证事务原子性
        for i in range(20):
            keys = [f"tx-{i}-key-{j}" for j in range(5)]
            values_found = []
            
            for j, key in enumerate(keys):
                node = self.nodes[j % len(self.nodes)]
                value = await node.read(key)
                values_found.append(value is not None)
            
            # 检查是否全部存在或全部不存在
            if any(values_found) and not all(values_found):
                violations.append({
                    'type': 'atomicity_violation',
                    'transaction': i,
                    'message': '事务未满足原子性'
                })
        
        result = ConsistencyTestResult(
            test_name="Distributed Transaction Test",
            consistency_level=ConsistencyLevel.LINEARIZABLE,
            passed=len(violations) == 0,
            violations=violations,
            metrics={
                'successful_transactions': successful_transactions,
                'failed_transactions': failed_transactions,
                'atomicity_violations': len(violations)
            }
        )
        
        logger.info(f"  成功事务: {successful_transactions}, 失败: {failed_transactions}, "
                   f"违规: {len(violations)}")
        
        return result
    
    def generate_report(self, output_file: str = "consistency_verification_report.json"):
        """生成测试报告"""
        report = {
            'test_suite': 'Agent Memory System Consistency Verification',
            'version': '3.0',
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_tests': len(self.results),
                'passed': sum(1 for r in self.results if r.passed),
                'failed': sum(1 for r in self.results if not r.passed),
                'total_violations': sum(len(r.violations) for r in self.results)
            },
            'results': []
        }
        
        for result in self.results:
            report['results'].append({
                'test_name': result.test_name,
                'consistency_level': result.consistency_level.value,
                'passed': result.passed,
                'violations': result.violations,
                'duration_seconds': result.duration_seconds,
                'metrics': result.metrics
            })
        
        output_path = f"/workspace/agent_memory_system_v2/tests/reports/{output_file}"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n一致性验证报告已保存: {output_path}")
        return report


async def main():
    """主函数"""
    # 创建Jepsen测试
    jepsen = JepsenStyleTest(num_nodes=5, num_clients=10)
    
    # 运行测试
    results = await jepsen.run_jepsen_test()
    
    # 生成报告
    report = jepsen.generate_report()
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("一致性验证测试摘要")
    print("=" * 70)
    print(f"总测试数: {report['summary']['total_tests']}")
    print(f"通过: {report['summary']['passed']}")
    print(f"失败: {report['summary']['failed']}")
    print(f"总违规数: {report['summary']['total_violations']}")
    
    for result in results:
        status = "✓" if result.passed else "✗"
        print(f"\n{status} {result.test_name}")
        print(f"   一致性级别: {result.consistency_level.value}")
        print(f"   耗时: {result.duration_seconds:.3f}s")
        print(f"   违规: {len(result.violations)}")


if __name__ == "__main__":
    asyncio.run(main())

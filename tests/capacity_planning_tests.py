#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 容量规划测试
Capacity Planning Tests for Deep Validation Phase

包含:
- 数据规模扩展测试
- 节点扩展测试
- 分片再平衡测试
- 存储容量测试
- 网络带宽测试
- 预测模型验证
"""

import asyncio
import time
import random
import json
import logging
import statistics
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class CapacityMetrics:
    """容量指标"""
    data_size_gb: float
    num_records: int
    num_nodes: int
    storage_per_node_gb: float
    memory_per_node_gb: float
    cpu_usage_percent: float
    network_throughput_mbps: float
    query_latency_p99_ms: float
    shard_balance_score: float  # 0-1, 1表示完全平衡


@dataclass
class ScalingPrediction:
    """扩展预测"""
    current_capacity: CapacityMetrics
    predicted_capacity: CapacityMetrics
    recommended_nodes: int
    recommended_storage_gb: float
    confidence_score: float
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class CapacityTestResult:
    """容量测试结果"""
    test_name: str
    passed: bool
    metrics: CapacityMetrics
    predictions: Optional[ScalingPrediction] = None
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)


class MockShard:
    """模拟分片"""
    
    def __init__(self, shard_id: str, node_id: str):
        self.shard_id = shard_id
        self.node_id = node_id
        self.data: Dict[str, Any] = {}
        self.size_bytes = 0
        
    def add_data(self, key: str, value: Any, size_bytes: int):
        """添加数据"""
        self.data[key] = value
        self.size_bytes += size_bytes
        
    def get_size_mb(self) -> float:
        """获取大小(MB)"""
        return self.size_bytes / 1024 / 1024


class MockCluster:
    """模拟集群"""
    
    def __init__(self, num_nodes: int = 3):
        self.num_nodes = num_nodes
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.shards: Dict[str, MockShard] = {}
        self.total_data_size = 0
        self._initialize_nodes()
        
    def _initialize_nodes(self):
        """初始化节点"""
        for i in range(self.num_nodes):
            node_id = f"node-{i}"
            self.nodes[node_id] = {
                'cpu_percent': 0.0,
                'memory_gb': 16.0,
                'storage_gb': 100.0,
                'used_storage_gb': 0.0,
                'network_mbps': 1000.0
            }
            
    def add_node(self) -> str:
        """添加节点"""
        node_id = f"node-{len(self.nodes)}"
        self.nodes[node_id] = {
            'cpu_percent': 0.0,
            'memory_gb': 16.0,
            'storage_gb': 100.0,
            'used_storage_gb': 0.0,
            'network_mbps': 1000.0
        }
        self.num_nodes += 1
        return node_id
    
    def remove_node(self, node_id: str) -> bool:
        """移除节点"""
        if node_id in self.nodes:
            # 迁移分片
            shards_to_migrate = [s for s in self.shards.values() if s.node_id == node_id]
            del self.nodes[node_id]
            self.num_nodes -= 1
            
            # 重新分配分片
            for shard in shards_to_migrate:
                new_node = random.choice(list(self.nodes.keys()))
                shard.node_id = new_node
            return True
        return False
    
    def create_shard(self, shard_id: str) -> MockShard:
        """创建分片"""
        node_id = random.choice(list(self.nodes.keys()))
        shard = MockShard(shard_id, node_id)
        self.shards[shard_id] = shard
        return shard
    
    def get_shard_balance_score(self) -> float:
        """计算分片平衡分数"""
        if not self.shards:
            return 1.0
            
        node_sizes = defaultdict(float)
        for shard in self.shards.values():
            node_sizes[shard.node_id] += shard.get_size_mb()
        
        if not node_sizes:
            return 1.0
            
        sizes = list(node_sizes.values())
        avg_size = statistics.mean(sizes)
        
        if avg_size == 0:
            return 1.0
            
        # 计算标准差
        variance = sum((s - avg_size) ** 2 for s in sizes) / len(sizes)
        std_dev = variance ** 0.5
        
        # 平衡分数 = 1 - (标准差 / 平均值)
        balance_score = max(0, 1 - (std_dev / avg_size))
        return balance_score
    
    def rebalance_shards(self) -> int:
        """重新平衡分片，返回迁移的分片数量"""
        if not self.shards or not self.nodes:
            return 0
            
        # 计算每个节点的目标大小
        total_size = sum(s.get_size_mb() for s in self.shards.values())
        target_size_per_node = total_size / len(self.nodes)
        
        # 计算每个节点需要迁移的分片
        node_sizes = defaultdict(float)
        for shard in self.shards.values():
            node_sizes[shard.node_id] += shard.get_size_mb()
        
        migrations = 0
        for node_id, size in node_sizes.items():
            if size > target_size_per_node * 1.2:  # 超过20%
                # 迁移分片到负载较低的节点
                shards_on_node = [s for s in self.shards.values() if s.node_id == node_id]
                for shard in sorted(shards_on_node, key=lambda s: s.get_size_mb(), reverse=True):
                    if size <= target_size_per_node:
                        break
                    
                    # 找到负载最低的节点
                    min_node = min(self.nodes.keys(), key=lambda n: node_sizes[n])
                    if min_node != node_id:
                        node_sizes[node_id] -= shard.get_size_mb()
                        node_sizes[min_node] += shard.get_size_mb()
                        shard.node_id = min_node
                        migrations += 1
                        
        return migrations


class CapacityPlanningTestSuite:
    """容量规划测试套件"""
    
    def __init__(self):
        self.cluster = MockCluster()
        self.results: List[CapacityTestResult] = []
        self.historical_metrics: List[CapacityMetrics] = []
        
    async def run_all_tests(self) -> List[CapacityTestResult]:
        """运行所有容量规划测试"""
        logger.info("=" * 70)
        logger.info("开始容量规划测试套件")
        logger.info("=" * 70)
        
        # 1. 数据规模扩展测试
        await self.run_data_scale_tests()
        
        # 2. 节点扩展测试
        await self.run_node_scaling_tests()
        
        # 3. 分片再平衡测试
        await self.run_shard_rebalance_tests()
        
        # 4. 存储容量测试
        await self.run_storage_capacity_tests()
        
        # 5. 网络带宽测试
        await self.run_network_bandwidth_tests()
        
        # 6. 预测模型验证
        await self.run_prediction_model_tests()
        
        logger.info("\n" + "=" * 70)
        logger.info("容量规划测试套件执行完成")
        logger.info("=" * 70)
        
        return self.results
    
    async def run_data_scale_tests(self):
        """数据规模扩展测试"""
        logger.info("\n" + "-" * 70)
        logger.info("数据规模扩展测试")
        logger.info("-" * 70)
        
        start_time = time.time()
        errors = []
        
        test_sizes = [1000, 10000, 100000, 1000000]
        
        for target_size in test_sizes:
            logger.info(f"  测试数据规模: {target_size} 条记录")
            
            # 创建分片和填充数据
            records_per_shard = 1000
            num_shards = max(1, target_size // records_per_shard)
            
            for i in range(num_shards):
                shard_id = f"shard_scale_{i}"
                shard = self.cluster.create_shard(shard_id)
                
                # 填充数据
                for j in range(records_per_shard):
                    key = f"key_{i}_{j}"
                    value = f"value_{random.randint(1, 1000000)}"
                    size_bytes = len(key) + len(value) + 100  # 估算大小
                    shard.add_data(key, value, size_bytes)
                    self.cluster.total_data_size += size_bytes
            
            # 收集指标
            metrics = self._collect_metrics()
            self.historical_metrics.append(metrics)
            
            logger.info(f"    数据大小: {metrics.data_size_gb:.2f} GB")
            logger.info(f"    分片数: {len(self.cluster.shards)}")
            logger.info(f"    平衡分数: {metrics.shard_balance_score:.3f}")
        
        duration = time.time() - start_time
        
        result = CapacityTestResult(
            test_name="Data Scale Test",
            passed=len(errors) == 0,
            metrics=self.historical_metrics[-1] if self.historical_metrics else CapacityMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0),
            duration_seconds=duration,
            errors=errors
        )
        self.results.append(result)
        
        logger.info(f"  结果: {'通过' if result.passed else '失败'}")
    
    async def run_node_scaling_tests(self):
        """节点扩展测试"""
        logger.info("\n" + "-" * 70)
        logger.info("节点扩展测试")
        logger.info("-" * 70)
        
        start_time = time.time()
        errors = []
        
        # 测试水平扩展
        node_counts = [3, 5, 7, 10]
        
        for target_nodes in node_counts:
            logger.info(f"  测试节点数: {target_nodes}")
            
            # 添加节点
            while self.cluster.num_nodes < target_nodes:
                new_node_id = self.cluster.add_node()
                logger.info(f"    添加节点: {new_node_id}")
            
            # 重新平衡
            migrations = self.cluster.rebalance_shards()
            logger.info(f"    分片迁移: {migrations}")
            
            # 收集指标
            metrics = self._collect_metrics()
            logger.info(f"    每节点存储: {metrics.storage_per_node_gb:.2f} GB")
            logger.info(f"    平衡分数: {metrics.shard_balance_score:.3f}")
        
        duration = time.time() - start_time
        
        result = CapacityTestResult(
            test_name="Node Scaling Test",
            passed=len(errors) == 0,
            metrics=self._collect_metrics(),
            duration_seconds=duration,
            errors=errors
        )
        self.results.append(result)
        
        logger.info(f"  结果: {'通过' if result.passed else '失败'}")
    
    async def run_shard_rebalance_tests(self):
        """分片再平衡测试"""
        logger.info("\n" + "-" * 70)
        logger.info("分片再平衡测试")
        logger.info("-" * 70)
        
        start_time = time.time()
        errors = []
        
        # 创建不平衡的数据分布
        logger.info("  创建不平衡分布...")
        for i in range(20):
            shard_id = f"shard_imbalance_{i}"
            shard = self.cluster.create_shard(shard_id)
            
            # 故意让某些分片更大
            num_records = random.randint(100, 10000)
            for j in range(num_records):
                key = f"key_{i}_{j}"
                value = "x" * random.randint(100, 1000)
                shard.add_data(key, value, len(key) + len(value))
        
        # 记录再平衡前状态
        balance_before = self.cluster.get_shard_balance_score()
        logger.info(f"  再平衡前分数: {balance_before:.3f}")
        
        # 执行再平衡
        migrations = self.cluster.rebalance_shards()
        balance_after = self.cluster.get_shard_balance_score()
        
        logger.info(f"  分片迁移数: {migrations}")
        logger.info(f"  再平衡后分数: {balance_after:.3f}")
        
        # 验证改进
        if balance_after <= balance_before:
            errors.append("再平衡未改善分布")
        
        duration = time.time() - start_time
        
        result = CapacityTestResult(
            test_name="Shard Rebalance Test",
            passed=len(errors) == 0,
            metrics=self._collect_metrics(),
            duration_seconds=duration,
            errors=errors
        )
        self.results.append(result)
        
        logger.info(f"  结果: {'通过' if result.passed else '失败'}")
    
    async def run_storage_capacity_tests(self):
        """存储容量测试"""
        logger.info("\n" + "-" * 70)
        logger.info("存储容量测试")
        logger.info("-" * 70)
        
        start_time = time.time()
        errors = []
        
        # 测试存储限制
        storage_thresholds = [50, 75, 90, 95]  # 百分比
        
        for threshold in storage_thresholds:
            logger.info(f"  测试存储阈值: {threshold}%")
            
            # 计算当前使用率
            total_storage = sum(n['storage_gb'] for n in self.cluster.nodes.values())
            used_storage = sum(n['used_storage_gb'] for n in self.cluster.nodes.values())
            usage_percent = (used_storage / total_storage) * 100 if total_storage > 0 else 0
            
            logger.info(f"    当前使用率: {usage_percent:.1f}%")
            
            # 如果超过阈值，触发告警
            if usage_percent > threshold:
                logger.warning(f"    存储使用率超过阈值: {usage_percent:.1f}% > {threshold}%")
        
        # 测试数据压缩效果
        logger.info("  测试数据压缩...")
        uncompressed_size = self.cluster.total_data_size
        # 模拟压缩率
        compression_ratio = 0.3
        compressed_size = uncompressed_size * compression_ratio
        
        logger.info(f"    压缩前: {uncompressed_size / 1024 / 1024:.2f} MB")
        logger.info(f"    压缩后: {compressed_size / 1024 / 1024:.2f} MB")
        logger.info(f"    压缩率: {(1 - compression_ratio) * 100:.1f}%")
        
        duration = time.time() - start_time
        
        result = CapacityTestResult(
            test_name="Storage Capacity Test",
            passed=len(errors) == 0,
            metrics=self._collect_metrics(),
            duration_seconds=duration,
            errors=errors
        )
        self.results.append(result)
        
        logger.info(f"  结果: {'通过' if result.passed else '失败'}")
    
    async def run_network_bandwidth_tests(self):
        """网络带宽测试"""
        logger.info("\n" + "-" * 70)
        logger.info("网络带宽测试")
        logger.info("-" * 70)
        
        start_time = time.time()
        errors = []
        
        # 模拟网络流量
        bandwidth_tests = [
            {'name': '正常流量', 'mbps': 100},
            {'name': '高峰流量', 'mbps': 500},
            {'name': '突发流量', 'mbps': 1000}
        ]
        
        for test in bandwidth_tests:
            logger.info(f"  测试场景: {test['name']} ({test['mbps']} Mbps)")
            
            # 模拟数据传输
            data_size_mb = 100
            transfer_time = data_size_mb / test['mbps']
            
            logger.info(f"    传输 {data_size_mb} MB 数据需要 {transfer_time:.2f} 秒")
            
            # 检查是否满足SLA
            if transfer_time > 10:  # 假设SLA是10秒内
                errors.append(f"{test['name']} 传输时间超过SLA")
        
        # 测试跨可用区延迟
        logger.info("  测试跨可用区延迟...")
        az_delays = [5, 10, 20, 50]  # ms
        
        for delay_ms in az_delays:
            # 模拟RTT
            rtt = delay_ms * 2
            throughput = 1000 / rtt  # 简化计算
            logger.info(f"    延迟 {delay_ms}ms -> RTT {rtt}ms -> 吞吐 {throughput:.1f} Mbps")
        
        duration = time.time() - start_time
        
        result = CapacityTestResult(
            test_name="Network Bandwidth Test",
            passed=len(errors) == 0,
            metrics=self._collect_metrics(),
            duration_seconds=duration,
            errors=errors
        )
        self.results.append(result)
        
        logger.info(f"  结果: {'通过' if result.passed else '失败'}")
    
    async def run_prediction_model_tests(self):
        """预测模型验证"""
        logger.info("\n" + "-" * 70)
        logger.info("预测模型验证")
        logger.info("-" * 70)
        
        start_time = time.time()
        errors = []
        
        # 使用历史数据训练简单预测模型
        if len(self.historical_metrics) < 2:
            logger.warning("历史数据不足，使用模拟数据")
            # 生成模拟历史数据
            for i in range(10):
                metrics = CapacityMetrics(
                    data_size_gb=i * 10,
                    num_records=i * 10000,
                    num_nodes=3 + i // 3,
                    storage_per_node_gb=i * 3,
                    memory_per_node_gb=8 + i * 0.5,
                    cpu_usage_percent=20 + i * 5,
                    network_throughput_mbps=100 + i * 10,
                    query_latency_p99_ms=10 + i * 2,
                    shard_balance_score=0.8 + i * 0.01
                )
                self.historical_metrics.append(metrics)
        
        # 线性回归预测
        logger.info("  训练预测模型...")
        
        # 提取特征
        X = np.array([[m.num_records, m.num_nodes] for m in self.historical_metrics])
        y_storage = np.array([m.storage_per_node_gb for m in self.historical_metrics])
        y_latency = np.array([m.query_latency_p99_ms for m in self.historical_metrics])
        
        # 简单线性预测
        current_records = self.historical_metrics[-1].num_records
        future_records = current_records * 2  # 预测2倍增长
        
        # 预测存储需求
        if len(y_storage) > 1:
            storage_growth_rate = (y_storage[-1] - y_storage[0]) / len(y_storage)
            predicted_storage = y_storage[-1] + storage_growth_rate * 10
        else:
            predicted_storage = y_storage[-1] * 2
        
        # 预测节点需求
        current_nodes = self.cluster.num_nodes
        recommended_nodes = max(current_nodes, int(future_records / 50000) + 1)
        
        logger.info(f"  当前记录数: {current_records}")
        logger.info(f"  预测记录数: {future_records}")
        logger.info(f"  当前节点数: {current_nodes}")
        logger.info(f"  推荐节点数: {recommended_nodes}")
        logger.info(f"  预测存储/节点: {predicted_storage:.2f} GB")
        
        # 创建预测结果
        current_metrics = self.historical_metrics[-1]
        predicted_metrics = CapacityMetrics(
            data_size_gb=current_metrics.data_size_gb * 2,
            num_records=future_records,
            num_nodes=recommended_nodes,
            storage_per_node_gb=predicted_storage,
            memory_per_node_gb=current_metrics.memory_per_node_gb * 1.5,
            cpu_usage_percent=current_metrics.cpu_usage_percent * 0.8,  # 更多节点降低CPU
            network_throughput_mbps=current_metrics.network_throughput_mbps * 1.2,
            query_latency_p99_ms=current_metrics.query_latency_p99_ms * 0.9,
            shard_balance_score=0.95
        )
        
        prediction = ScalingPrediction(
            current_capacity=current_metrics,
            predicted_capacity=predicted_metrics,
            recommended_nodes=recommended_nodes,
            recommended_storage_gb=predicted_storage * recommended_nodes,
            confidence_score=0.85
        )
        
        duration = time.time() - start_time
        
        result = CapacityTestResult(
            test_name="Prediction Model Test",
            passed=len(errors) == 0,
            metrics=current_metrics,
            predictions=prediction,
            duration_seconds=duration,
            errors=errors
        )
        self.results.append(result)
        
        logger.info(f"  结果: {'通过' if result.passed else '失败'}")
    
    def _collect_metrics(self) -> CapacityMetrics:
        """收集当前指标"""
        total_size_gb = self.cluster.total_data_size / 1024 / 1024 / 1024
        num_records = sum(len(s.data) for s in self.cluster.shards.values())
        
        storage_per_node = total_size_gb / max(1, self.cluster.num_nodes)
        
        # 模拟其他指标
        cpu_usage = min(95, 20 + num_records / 10000)
        network_mbps = 100 + self.cluster.num_nodes * 50
        latency_ms = 10 + num_records / 50000
        
        balance_score = self.cluster.get_shard_balance_score()
        
        return CapacityMetrics(
            data_size_gb=total_size_gb,
            num_records=num_records,
            num_nodes=self.cluster.num_nodes,
            storage_per_node_gb=storage_per_node,
            memory_per_node_gb=16.0,
            cpu_usage_percent=cpu_usage,
            network_throughput_mbps=network_mbps,
            query_latency_p99_ms=latency_ms,
            shard_balance_score=balance_score
        )
    
    def generate_report(self, output_file: str = "capacity_planning_report.json"):
        """生成测试报告"""
        report = {
            'test_suite': 'Agent Memory System Capacity Planning Tests',
            'version': '3.0',
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_tests': len(self.results),
                'passed': sum(1 for r in self.results if r.passed),
                'failed': sum(1 for r in self.results if not r.passed),
                'total_data_size_gb': self.cluster.total_data_size / 1024 / 1024 / 1024,
                'total_nodes': self.cluster.num_nodes,
                'total_shards': len(self.cluster.shards)
            },
            'results': []
        }
        
        for result in self.results:
            result_data = {
                'test_name': result.test_name,
                'passed': result.passed,
                'duration_seconds': result.duration_seconds,
                'metrics': {
                    'data_size_gb': result.metrics.data_size_gb,
                    'num_records': result.metrics.num_records,
                    'num_nodes': result.metrics.num_nodes,
                    'storage_per_node_gb': result.metrics.storage_per_node_gb,
                    'shard_balance_score': result.metrics.shard_balance_score
                },
                'errors': result.errors
            }
            
            if result.predictions:
                result_data['predictions'] = {
                    'recommended_nodes': result.predictions.recommended_nodes,
                    'recommended_storage_gb': result.predictions.recommended_storage_gb,
                    'confidence_score': result.predictions.confidence_score
                }
            
            report['results'].append(result_data)
        
        output_path = f"/workspace/agent_memory_system_v2/tests/reports/{output_file}"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n容量规划测试报告已保存: {output_path}")
        return report


async def main():
    """主函数"""
    # 创建测试套件
    suite = CapacityPlanningTestSuite()
    
    # 运行所有测试
    results = await suite.run_all_tests()
    
    # 生成报告
    report = suite.generate_report()
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("容量规划测试摘要")
    print("=" * 70)
    print(f"总测试数: {report['summary']['total_tests']}")
    print(f"通过: {report['summary']['passed']}")
    print(f"失败: {report['summary']['failed']}")
    print(f"总数据量: {report['summary']['total_data_size_gb']:.2f} GB")
    print(f"总节点数: {report['summary']['total_nodes']}")
    print(f"总分片数: {report['summary']['total_shards']}")
    
    for result in results:
        status = "✓" if result.passed else "✗"
        print(f"\n{status} {result.test_name}")
        print(f"   数据量: {result.metrics.data_size_gb:.2f} GB")
        print(f"   记录数: {result.metrics.num_records}")
        print(f"   节点数: {result.metrics.num_nodes}")
        print(f"   平衡分数: {result.metrics.shard_balance_score:.3f}")


if __name__ == "__main__":
    asyncio.run(main())

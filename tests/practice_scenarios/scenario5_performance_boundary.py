#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 场景5: 性能瓶颈和边界测试
Scenario 5: Performance Bottleneck and Boundary Test

目标: 发现性能瓶颈和边界条件问题

测试内容:
- 插入100万条记忆数据
- 测试索引构建和查询性能
- 验证分片再平衡效率
- 发现性能瓶颈和边界条件问题

缺陷检测:
- 查询性能退化
- 索引构建瓶颈
- 分片不均衡
- 内存溢出
- 超时问题
- 吞吐量下降
"""

import asyncio
import time
import random
import json
import logging
import threading
import math
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from collections import defaultdict
import heapq

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DefectType(Enum):
    """缺陷类型枚举"""
    QUERY_DEGRADATION = "query_degradation"
    INDEX_BUILD_BOTTLENECK = "index_build_bottleneck"
    SHARD_IMBALANCE = "shard_imbalance"
    MEMORY_OVERFLOW = "memory_overflow"
    TIMEOUT = "timeout"
    THROUGHPUT_DROP = "throughput_drop"
    HOTSPOT = "hotspot"
    LOCK_CONTENTION = "lock_contention"
    GC_PRESSURE = "gc_pressure"
    SLOW_OPERATION = "slow_operation"


@dataclass
class DefectReport:
    """缺陷报告"""
    defect_type: DefectType
    severity: str
    description: str
    timestamp: float
    evidence: Dict[str, Any] = field(default_factory=dict)
    performance_impact: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化的字典"""
        return {
            'defect_type': self.defect_type.value,
            'severity': self.severity,
            'description': self.description,
            'timestamp': self.timestamp,
            'evidence': self.evidence,
            'performance_impact': self.performance_impact
        }


@dataclass
class PerformanceMetrics:
    """性能指标"""
    timestamp: float
    operation: str
    latency_ms: float
    throughput_ops: float
    memory_used_mb: float
    cpu_percent: float
    queue_size: int
    active_threads: int


class ShardedMemoryStore:
    """分片记忆存储"""
    
    def __init__(self, num_shards: int = 10):
        self.num_shards = num_shards
        self.shards: Dict[int, Dict[str, Any]] = {i: {} for i in range(num_shards)}
        self.shard_stats: Dict[int, Dict[str, int]] = {
            i: {'reads': 0, 'writes': 0, 'size': 0} for i in range(num_shards)
        }
        
        # 索引
        self.primary_index: Dict[str, int] = {}  # key -> shard_id
        self.secondary_indices: Dict[str, Dict[Any, Set[str]]] = {}  # 索引名 -> 值 -> 键集合
        
        # 性能追踪
        self._operation_log: List[Dict[str, Any]] = []
        self._slow_operations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()
        
        # 热点追踪
        self._access_count: Dict[str, int] = defaultdict(int)
        
        # 慢操作阈值
        self.slow_threshold_ms = 100.0
    
    def _get_shard_id(self, key: str) -> int:
        """计算分片ID"""
        return hash(key) % self.num_shards
    
    def write(self, key: str, value: Any, build_index: bool = True) -> Tuple[bool, float]:
        """
        写入数据
        
        Returns:
            (success, latency_ms)
        """
        start = time.time()
        
        with self._lock:
            shard_id = self._get_shard_id(key)
            
            # 写入分片
            self.shards[shard_id][key] = value
            self.shard_stats[shard_id]['writes'] += 1
            self.shard_stats[shard_id]['size'] = len(self.shards[shard_id])
            
            # 更新主索引
            self.primary_index[key] = shard_id
            
            # 构建二级索引
            if build_index:
                self._build_secondary_indices(key, value)
            
            # 记录访问
            self._access_count[key] += 1
            
            latency = (time.time() - start) * 1000
            
            # 记录操作
            self._operation_log.append({
                'type': 'write',
                'key': key,
                'shard': shard_id,
                'latency_ms': latency,
                'timestamp': time.time()
            })
            
            # 检查慢操作
            if latency > self.slow_threshold_ms:
                self._slow_operations.append({
                    'type': 'write',
                    'key': key,
                    'latency_ms': latency,
                    'timestamp': time.time()
                })
            
            return True, latency
    
    def read(self, key: str) -> Tuple[Optional[Any], float]:
        """
        读取数据
        
        Returns:
            (value, latency_ms)
        """
        start = time.time()
        
        with self._lock:
            shard_id = self._get_shard_id(key)
            
            value = self.shards[shard_id].get(key)
            self.shard_stats[shard_id]['reads'] += 1
            
            # 记录访问
            self._access_count[key] += 1
            
            latency = (time.time() - start) * 1000
            
            self._operation_log.append({
                'type': 'read',
                'key': key,
                'shard': shard_id,
                'latency_ms': latency,
                'timestamp': time.time()
            })
            
            if latency > self.slow_threshold_ms:
                self._slow_operations.append({
                    'type': 'read',
                    'key': key,
                    'latency_ms': latency,
                    'timestamp': time.time()
                })
            
            return value, latency
    
    def query(self, filter_func: callable = None, limit: int = 100) -> Tuple[List[Any], float]:
        """
        查询数据
        
        Returns:
            (results, latency_ms)
        """
        start = time.time()
        
        results = []
        scanned = 0
        
        with self._lock:
            for shard_id, shard in self.shards.items():
                for key, value in shard.items():
                    scanned += 1
                    if filter_func is None or filter_func(key, value):
                        results.append(value)
                        if len(results) >= limit:
                            break
                if len(results) >= limit:
                    break
        
        latency = (time.time() - start) * 1000
        
        self._operation_log.append({
            'type': 'query',
            'scanned': scanned,
            'results': len(results),
            'latency_ms': latency,
            'timestamp': time.time()
        })
        
        if latency > self.slow_threshold_ms * 10:  # 查询阈值更高
            self._slow_operations.append({
                'type': 'query',
                'scanned': scanned,
                'results': len(results),
                'latency_ms': latency,
                'timestamp': time.time()
            })
        
        return results, latency
    
    def _build_secondary_indices(self, key: str, value: Any):
        """构建二级索引"""
        if not isinstance(value, dict):
            return
        
        for field_name, field_value in value.items():
            if field_name not in self.secondary_indices:
                self.secondary_indices[field_name] = defaultdict(set)
            
            # 限制索引值的大小，并确保值是可哈希的
            try:
                if len(str(field_value)) < 100:
                    # 对于列表等不可哈希类型，转换为字符串
                    if isinstance(field_value, (list, dict)):
                        field_value = str(field_value)
                    self.secondary_indices[field_name][field_value].add(key)
            except TypeError:
                pass  # 跳过不可哈希的值
    
    def get_hotspots(self, threshold: int = 100) -> List[Tuple[str, int]]:
        """获取热点键"""
        return [
            (key, count) for key, count in self._access_count.items()
            if count > threshold
        ]
    
    def get_shard_balance(self) -> Dict[str, Any]:
        """获取分片均衡情况"""
        sizes = [stats['size'] for stats in self.shard_stats.values()]
        
        if not sizes or sum(sizes) == 0:
            return {'balanced': True, 'imbalance_ratio': 0.0}
        
        avg_size = sum(sizes) / len(sizes)
        max_size = max(sizes)
        min_size = min(sizes)
        
        imbalance_ratio = (max_size - min_size) / avg_size if avg_size > 0 else 0
        
        return {
            'balanced': imbalance_ratio < 0.2,
            'imbalance_ratio': imbalance_ratio,
            'avg_size': avg_size,
            'max_size': max_size,
            'min_size': min_size,
            'sizes': sizes
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        total_size = sum(len(shard) for shard in self.shards.values())
        
        return {
            'total_items': total_size,
            'num_shards': self.num_shards,
            'primary_index_size': len(self.primary_index),
            'secondary_indices_count': len(self.secondary_indices),
            'slow_operations_count': len(self._slow_operations),
            'shard_balance': self.get_shard_balance(),
            'shard_stats': self.shard_stats.copy()
        }


class PerformanceBoundaryScenario:
    """性能瓶颈和边界测试场景"""
    
    def __init__(self, target_items: int = 1000000):
        self.target_items = target_items
        self.store = ShardedMemoryStore(num_shards=20)
        self.results: Dict[str, Any] = {}
        self.defects: List[DefectReport] = []
        
        # 性能阈值
        self.write_latency_threshold = 10.0  # ms
        self.read_latency_threshold = 5.0  # ms
        self.query_latency_threshold = 1000.0  # ms
        self.throughput_drop_threshold = 0.5  # 50%下降
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        logger.info("=" * 70)
        logger.info("场景5: 性能瓶颈和边界测试")
        logger.info(f"目标数据量: {self.target_items:,} 条")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # 测试1: 大规模数据插入
        result1 = await self.test_large_scale_insert()
        self.results['large_scale_insert'] = result1
        
        # 测试2: 索引构建性能
        result2 = await self.test_index_building()
        self.results['index_building'] = result2
        
        # 测试3: 查询性能测试
        result3 = await self.test_query_performance()
        self.results['query_performance'] = result3
        
        # 测试4: 分片均衡测试
        result4 = await self.test_shard_balance()
        self.results['shard_balance'] = result4
        
        # 测试5: 边界条件测试
        result5 = await self.test_boundary_conditions()
        self.results['boundary_conditions'] = result5
        
        # 测试6: 性能退化测试
        result6 = await self.test_performance_degradation()
        self.results['performance_degradation'] = result6
        
        end_time = time.time()
        
        # 生成报告
        report = self._generate_report(start_time, end_time)
        
        return report
    
    def _record_defect(self, defect_type: DefectType, severity: str,
                       description: str, evidence: Dict[str, Any] = None,
                       performance_impact: float = 0.0):
        """记录缺陷"""
        defect = DefectReport(
            defect_type=defect_type,
            severity=severity,
            description=description,
            timestamp=time.time(),
            evidence=evidence or {},
            performance_impact=performance_impact
        )
        self.defects.append(defect)
        logger.error(f"[缺陷发现] {defect_type.value}: {description}")
    
    async def test_large_scale_insert(self) -> Dict[str, Any]:
        """测试1: 大规模数据插入"""
        logger.info("\n" + "-" * 70)
        logger.info("测试1: 大规模数据插入")
        logger.info("-" * 70)
        
        results = {
            'total_inserted': 0,
            'batch_metrics': [],
            'throughput_history': []
        }
        
        # 批量插入
        batch_size = 10000
        num_batches = self.target_items // batch_size
        
        # 限制实际插入数量（用于测试）
        actual_batches = min(num_batches, 100)  # 最多100批次
        
        total_start = time.time()
        prev_throughput = 0
        
        for batch_idx in range(actual_batches):
            batch_start = time.time()
            batch_latencies = []
            
            for i in range(batch_size):
                key = f"item-{batch_idx * batch_size + i}"
                value = {
                    'id': batch_idx * batch_size + i,
                    'batch': batch_idx,
                    'data': random.random(),
                    'tags': [f"tag-{random.randint(1, 100)}" for _ in range(3)],
                    'timestamp': time.time()
                }
                
                success, latency = self.store.write(key, value)
                batch_latencies.append(latency)
            
            batch_time = time.time() - batch_start
            batch_throughput = batch_size / batch_time
            
            results['throughput_history'].append(batch_throughput)
            results['total_inserted'] += batch_size
            
            # 记录批次指标
            batch_metrics = {
                'batch': batch_idx + 1,
                'items': batch_size,
                'time_ms': batch_time * 1000,
                'throughput': batch_throughput,
                'avg_latency_ms': sum(batch_latencies) / len(batch_latencies),
                'p99_latency_ms': sorted(batch_latencies)[int(len(batch_latencies) * 0.99)]
            }
            results['batch_metrics'].append(batch_metrics)
            
            # 检测吞吐量下降
            if prev_throughput > 0:
                drop_ratio = (prev_throughput - batch_throughput) / prev_throughput
                if drop_ratio > self.throughput_drop_threshold:
                    self._record_defect(DefectType.THROUGHPUT_DROP, "high",
                        f"吞吐量下降 {drop_ratio*100:.1f}%: {prev_throughput:.0f} -> {batch_throughput:.0f} ops/s",
                        {"prev": prev_throughput, "current": batch_throughput, "drop_ratio": drop_ratio},
                        drop_ratio)
            
            prev_throughput = batch_throughput
            
            # 定期报告
            if (batch_idx + 1) % 10 == 0:
                logger.info(f"已插入 {results['total_inserted']:,} 条, "
                           f"吞吐量: {batch_throughput:.0f} ops/s")
            
            await asyncio.sleep(0.01)  # 让出控制权
        
        total_time = time.time() - total_start
        results['total_time_seconds'] = total_time
        results['avg_throughput'] = results['total_inserted'] / total_time
        
        logger.info(f"\n插入完成: {results['total_inserted']:,} 条, "
                   f"总时间: {total_time:.2f}s, "
                   f"平均吞吐量: {results['avg_throughput']:.0f} ops/s")
        
        return results
    
    async def test_index_building(self) -> Dict[str, Any]:
        """测试2: 索引构建性能"""
        logger.info("\n" + "-" * 70)
        logger.info("测试2: 索引构建性能")
        logger.info("-" * 70)
        
        results = {
            'index_sizes': {},
            'build_times': {}
        }
        
        # 测试不同索引大小
        test_sizes = [1000, 10000, 100000]
        
        for size in test_sizes:
            logger.info(f"\n测试索引大小: {size:,}")
            
            # 创建新存储
            test_store = ShardedMemoryStore(num_shards=10)
            
            start = time.time()
            
            # 插入数据并构建索引
            for i in range(size):
                key = f"idx-test-{i}"
                value = {
                    'id': i,
                    'category': f"cat-{i % 100}",
                    'value': random.random()
                }
                test_store.write(key, value)
            
            build_time = time.time() - start
            
            results['index_sizes'][size] = {
                'build_time_ms': build_time * 1000,
                'items_per_second': size / build_time,
                'primary_index_size': len(test_store.primary_index),
                'secondary_indices': len(test_store.secondary_indices)
            }
            
            logger.info(f"  构建时间: {build_time*1000:.2f}ms, "
                       f"速度: {size/build_time:.0f} items/s")
            
            # 检测索引构建瓶颈
            if size / build_time < 10000:  # 低于10000 items/s
                self._record_defect(DefectType.INDEX_BUILD_BOTTLENECK, "medium",
                    f"索引构建速度慢: {size/build_time:.0f} items/s",
                    {"size": size, "build_time_ms": build_time * 1000})
        
        return results
    
    async def test_query_performance(self) -> Dict[str, Any]:
        """测试3: 查询性能测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试3: 查询性能测试")
        logger.info("-" * 70)
        
        results = {
            'query_types': {},
            'latency_distribution': []
        }
        
        # 获取当前数据量
        stats = self.store.get_stats()
        total_items = stats['total_items']
        
        # 测试不同查询类型
        query_tests = [
            ('point_lookup', lambda: self._test_point_lookup()),
            ('range_query', lambda: self._test_range_query()),
            ('filter_query', lambda: self._test_filter_query()),
            ('full_scan', lambda: self._test_full_scan())
        ]
        
        for query_name, query_func in query_tests:
            logger.info(f"\n测试查询类型: {query_name}")
            
            query_results = await query_func()
            results['query_types'][query_name] = query_results
            
            # 检测查询性能问题
            if query_results['avg_latency_ms'] > self.query_latency_threshold:
                self._record_defect(DefectType.QUERY_DEGRADATION, "high",
                    f"{query_name} 查询延迟过高: {query_results['avg_latency_ms']:.2f}ms",
                    {"query_type": query_name, "avg_latency": query_results['avg_latency_ms']},
                    query_results['avg_latency_ms'] / self.query_latency_threshold)
        
        return results
    
    async def _test_point_lookup(self) -> Dict[str, Any]:
        """测试点查询"""
        latencies = []
        num_queries = 1000
        
        for _ in range(num_queries):
            # 随机选择一个键
            if self.store.primary_index:
                key = random.choice(list(self.store.primary_index.keys()))
                _, latency = self.store.read(key)
                latencies.append(latency)
        
        if not latencies:
            return {'avg_latency_ms': 0, 'p99_latency_ms': 0}
        
        return {
            'num_queries': num_queries,
            'avg_latency_ms': sum(latencies) / len(latencies),
            'p99_latency_ms': sorted(latencies)[int(len(latencies) * 0.99)],
            'max_latency_ms': max(latencies)
        }
    
    async def _test_range_query(self) -> Dict[str, Any]:
        """测试范围查询"""
        latencies = []
        num_queries = 100
        
        for _ in range(num_queries):
            start_id = random.randint(0, max(0, self.store.get_stats()['total_items'] - 100))
            
            query_start = time.time()
            results, _ = self.store.query(
                filter_func=lambda k, v: isinstance(v, dict) and 
                          start_id <= v.get('id', 0) < start_id + 100,
                limit=100
            )
            latencies.append((time.time() - query_start) * 1000)
        
        if not latencies:
            return {'avg_latency_ms': 0, 'p99_latency_ms': 0}
        
        return {
            'num_queries': num_queries,
            'avg_latency_ms': sum(latencies) / len(latencies),
            'p99_latency_ms': sorted(latencies)[int(len(latencies) * 0.99)],
            'max_latency_ms': max(latencies)
        }
    
    async def _test_filter_query(self) -> Dict[str, Any]:
        """测试过滤查询"""
        latencies = []
        num_queries = 100
        
        for _ in range(num_queries):
            target_batch = random.randint(0, 100)
            
            query_start = time.time()
            results, _ = self.store.query(
                filter_func=lambda k, v: isinstance(v, dict) and v.get('batch') == target_batch,
                limit=1000
            )
            latencies.append((time.time() - query_start) * 1000)
        
        if not latencies:
            return {'avg_latency_ms': 0, 'p99_latency_ms': 0}
        
        return {
            'num_queries': num_queries,
            'avg_latency_ms': sum(latencies) / len(latencies),
            'p99_latency_ms': sorted(latencies)[int(len(latencies) * 0.99)],
            'max_latency_ms': max(latencies)
        }
    
    async def _test_full_scan(self) -> Dict[str, Any]:
        """测试全表扫描"""
        query_start = time.time()
        results, latency = self.store.query(limit=1000000)
        
        return {
            'num_queries': 1,
            'avg_latency_ms': latency,
            'p99_latency_ms': latency,
            'max_latency_ms': latency,
            'results_count': len(results)
        }
    
    async def test_shard_balance(self) -> Dict[str, Any]:
        """测试4: 分片均衡测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试4: 分片均衡测试")
        logger.info("-" * 70)
        
        results = {
            'initial_balance': {},
            'after_operations': {},
            'rebalance_metrics': {}
        }
        
        # 获取当前分片状态
        balance = self.store.get_shard_balance()
        results['initial_balance'] = balance
        
        logger.info(f"初始分片均衡: 不均衡比率={balance['imbalance_ratio']:.2f}")
        
        # 检测分片不均衡
        if balance['imbalance_ratio'] > 0.3:
            self._record_defect(DefectType.SHARD_IMBALANCE, "medium",
                f"分片不均衡: 比率={balance['imbalance_ratio']:.2f}",
                {"imbalance_ratio": balance['imbalance_ratio'], "sizes": balance['sizes']},
                balance['imbalance_ratio'])
        
        # 模拟热点访问
        logger.info("\n模拟热点访问...")
        hot_key = "hotspot-key"
        for _ in range(10000):
            self.store.write(hot_key, {'value': random.random()})
        
        # 检测热点
        hotspots = self.store.get_hotspots(threshold=1000)
        results['hotspots'] = [(k, c) for k, c in hotspots[:10]]
        
        if hotspots:
            self._record_defect(DefectType.HOTSPOT, "medium",
                f"检测到热点: {hotspots[0][0]} 访问次数={hotspots[0][1]}",
                {"hotspot": hotspots[0][0], "access_count": hotspots[0][1]})
        
        # 再次检查分片均衡
        balance_after = self.store.get_shard_balance()
        results['after_operations'] = balance_after
        
        return results
    
    async def test_boundary_conditions(self) -> Dict[str, Any]:
        """测试5: 边界条件测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试5: 边界条件测试")
        logger.info("-" * 70)
        
        results = {
            'tests': []
        }
        
        # 测试5.1: 空键处理
        logger.info("\n测试5.1: 空键处理")
        try:
            success, latency = self.store.write("", {"test": "empty_key"})
            results['tests'].append({
                'name': 'empty_key',
                'success': success,
                'latency_ms': latency
            })
        except Exception as e:
            results['tests'].append({
                'name': 'empty_key',
                'error': str(e)
            })
        
        # 测试5.2: 超大值
        logger.info("\n测试5.2: 超大值")
        large_value = {'data': ['x' * 1000 for _ in range(1000)]}
        start = time.time()
        success, latency = self.store.write("large-value-key", large_value)
        results['tests'].append({
            'name': 'large_value',
            'success': success,
            'latency_ms': latency,
            'value_size_kb': len(str(large_value)) / 1024
        })
        
        if latency > 100:  # 超过100ms
            self._record_defect(DefectType.SLOW_OPERATION, "medium",
                f"大值写入慢: {latency:.2f}ms",
                {"latency_ms": latency, "value_size_kb": len(str(large_value)) / 1024})
        
        # 测试5.3: 并发写入同一键
        logger.info("\n测试5.3: 并发写入同一键")
        concurrent_writes = 100
        latencies = []
        
        async def concurrent_write():
            success, latency = self.store.write("concurrent-key", {'value': random.random()})
            return latency
        
        tasks = [concurrent_write() for _ in range(concurrent_writes)]
        latencies = await asyncio.gather(*tasks)
        
        results['tests'].append({
            'name': 'concurrent_writes',
            'count': concurrent_writes,
            'avg_latency_ms': sum(latencies) / len(latencies),
            'max_latency_ms': max(latencies)
        })
        
        # 检测锁竞争
        if max(latencies) > sum(latencies) / len(latencies) * 10:
            self._record_defect(DefectType.LOCK_CONTENTION, "high",
                f"检测到锁竞争: 平均延迟={sum(latencies)/len(latencies):.2f}ms, "
                f"最大延迟={max(latencies):.2f}ms",
                {"avg_latency": sum(latencies)/len(latencies), "max_latency": max(latencies)})
        
        # 测试5.4: 边界索引访问
        logger.info("\n测试5.4: 边界索引访问")
        # 尝试访问不存在的键
        for _ in range(1000):
            value, latency = self.store.read(f"non-existent-{random.randint(0, 1000000)}")
        
        results['tests'].append({
            'name': 'non_existent_keys',
            'count': 1000
        })
        
        return results
    
    async def test_performance_degradation(self) -> Dict[str, Any]:
        """测试6: 性能退化测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试6: 性能退化测试")
        logger.info("-" * 70)
        
        results = {
            'measurements': [],
            'degradation_detected': False
        }
        
        # 在不同数据量下测量性能
        measurement_points = [10000, 50000, 100000, 200000, 500000]
        
        for target_items in measurement_points:
            if self.store.get_stats()['total_items'] < target_items:
                # 需要插入更多数据
                current = self.store.get_stats()['total_items']
                needed = target_items - current
                
                logger.info(f"\n扩展数据到 {target_items:,} 条...")
                
                for i in range(needed):
                    key = f"degrad-test-{current + i}"
                    value = {'id': current + i, 'data': random.random()}
                    self.store.write(key, value)
            
            # 测量当前性能
            logger.info(f"\n测量性能 @ {target_items:,} 条数据")
            
            # 写入性能
            write_latencies = []
            for _ in range(1000):
                key = f"perf-test-{random.randint(0, 1000000)}"
                _, latency = self.store.write(key, {'data': random.random()})
                write_latencies.append(latency)
            
            # 读取性能
            read_latencies = []
            for _ in range(1000):
                if self.store.primary_index:
                    key = random.choice(list(self.store.primary_index.keys()))
                    _, latency = self.store.read(key)
                    read_latencies.append(latency)
            
            measurement = {
                'items': target_items,
                'write_avg_ms': sum(write_latencies) / len(write_latencies),
                'write_p99_ms': sorted(write_latencies)[int(len(write_latencies) * 0.99)],
                'read_avg_ms': sum(read_latencies) / len(read_latencies) if read_latencies else 0,
                'read_p99_ms': sorted(read_latencies)[int(len(read_latencies) * 0.99)] if read_latencies else 0
            }
            results['measurements'].append(measurement)
            
            logger.info(f"  写入: avg={measurement['write_avg_ms']:.2f}ms, "
                       f"p99={measurement['write_p99_ms']:.2f}ms")
            logger.info(f"  读取: avg={measurement['read_avg_ms']:.2f}ms, "
                       f"p99={measurement['read_p99_ms']:.2f}ms")
        
        # 检测性能退化趋势
        if len(results['measurements']) >= 2:
            first = results['measurements'][0]
            last = results['measurements'][-1]
            
            write_degradation = last['write_avg_ms'] / first['write_avg_ms'] if first['write_avg_ms'] > 0 else 1
            read_degradation = last['read_avg_ms'] / first['read_avg_ms'] if first['read_avg_ms'] > 0 else 1
            
            results['write_degradation_factor'] = write_degradation
            results['read_degradation_factor'] = read_degradation
            
            if write_degradation > 5:  # 性能下降超过5倍
                results['degradation_detected'] = True
                self._record_defect(DefectType.QUERY_DEGRADATION, "critical",
                    f"写入性能严重退化: {write_degradation:.1f}x",
                    {"first_avg": first['write_avg_ms'], "last_avg": last['write_avg_ms']},
                    write_degradation)
            
            if read_degradation > 5:
                results['degradation_detected'] = True
                self._record_defect(DefectType.QUERY_DEGRADATION, "critical",
                    f"读取性能严重退化: {read_degradation:.1f}x",
                    {"first_avg": first['read_avg_ms'], "last_avg": last['read_avg_ms']},
                    read_degradation)
        
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
            'scenario': '性能瓶颈和边界测试',
            'scenario_id': 'scenario5_performance_boundary',
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'end_time': datetime.fromtimestamp(end_time).isoformat(),
            'duration_seconds': end_time - start_time,
            'config': {
                'target_items': self.target_items
            },
            'summary': {
                'total_tests': 6,
                'defects_found': len(self.defects),
                'critical_defects': len(defects_by_severity.get('critical', [])),
                'high_defects': len(defects_by_severity.get('high', [])),
                'medium_defects': len(defects_by_severity.get('medium', [])),
                'low_defects': len(defects_by_severity.get('low', []))
            },
            'defects_by_type': dict(defects_by_type),
            'defects_by_severity': dict(defects_by_severity),
            'all_defects': [d.to_dict() for d in self.defects],
            'test_results': self.results,
            'store_stats': self.store.get_stats()
        }
        
        return report


async def main():
    """主函数 - 10000条数据快速测试版本"""
    # 修改为10000条数据快速测试版本
    scenario = PerformanceBoundaryScenario(target_items=10000)
    report = await scenario.run_all_tests()
    
    # 保存报告
    output_path = "/workspace/agent_memory_system_v2/tests/reports/scenario5_performance_boundary_report.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("场景5: 性能瓶颈和边界测试 - 摘要")
    print("=" * 70)
    print(f"测试持续时间: {report['duration_seconds']:.2f} 秒")
    print(f"目标数据量: {report['config']['target_items']:,} 条")
    print(f"实际数据量: {report['store_stats']['total_items']:,} 条")
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

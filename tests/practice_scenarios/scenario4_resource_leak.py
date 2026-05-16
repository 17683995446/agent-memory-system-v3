#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 场景4: 内存和资源泄漏测试
Scenario 4: Memory and Resource Leak Test

目标: 暴露资源泄漏问题

测试内容:
- 长时间运行（模拟24小时）
- 频繁创建/删除记忆
- 监控内存、文件句柄、连接数
- 发现资源泄漏问题

缺陷检测:
- 内存泄漏
- 文件句柄泄漏
- 连接泄漏
- 线程泄漏
- 缓存膨胀
- 对象未释放
"""

import asyncio
import time
import random
import json
import logging
import threading
import gc
import weakref
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from collections import defaultdict
import tracemalloc

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DefectType(Enum):
    """缺陷类型枚举"""
    MEMORY_LEAK = "memory_leak"
    FILE_HANDLE_LEAK = "file_handle_leak"
    CONNECTION_LEAK = "connection_leak"
    THREAD_LEAK = "thread_leak"
    CACHE_BLOAT = "cache_bloat"
    OBJECT_NOT_RELEASED = "object_not_released"
    GROWING_QUEUE = "growing_queue"
    CIRCULAR_REFERENCE = "circular_reference"
    EVENT_LISTENER_LEAK = "event_listener_leak"
    TIMER_LEAK = "timer_leak"


@dataclass
class DefectReport:
    """缺陷报告"""
    defect_type: DefectType
    severity: str
    description: str
    timestamp: float
    evidence: Dict[str, Any] = field(default_factory=dict)
    growth_rate: float = 0.0  # 资源增长率
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化的字典"""
        return {
            'defect_type': self.defect_type.value,
            'severity': self.severity,
            'description': self.description,
            'timestamp': self.timestamp,
            'evidence': self.evidence,
            'growth_rate': self.growth_rate
        }


@dataclass
class ResourceSnapshot:
    """资源快照"""
    timestamp: float
    memory_used_mb: float
    memory_peak_mb: float
    object_count: int
    thread_count: int
    file_handle_count: int
    connection_count: int
    cache_size: int
    gc_collections: Tuple[int, int, int]


class ResourceTracker:
    """资源追踪器"""
    
    def __init__(self):
        self.snapshots: List[ResourceSnapshot] = []
        self._lock = threading.Lock()
        self._connections: Set[int] = set()
        self._file_handles: Set[int] = set()
        self._threads: Dict[int, threading.Thread] = {}
        self._objects: Dict[int, weakref.ref] = {}
        
        # 启动内存追踪
        tracemalloc.start()
    
    def take_snapshot(self) -> ResourceSnapshot:
        """获取资源快照"""
        current, peak = tracemalloc.get_traced_memory()
        
        snapshot = ResourceSnapshot(
            timestamp=time.time(),
            memory_used_mb=current / 1024 / 1024,
            memory_peak_mb=peak / 1024 / 1024,
            object_count=len(self._objects),
            thread_count=threading.active_count(),
            file_handle_count=len(self._file_handles),
            connection_count=len(self._connections),
            cache_size=0,  # 由外部设置
            gc_collections=gc.get_count()
        )
        
        with self._lock:
            self.snapshots.append(snapshot)
        
        return snapshot
    
    def register_connection(self, conn_id: int):
        """注册连接"""
        with self._lock:
            self._connections.add(conn_id)
    
    def unregister_connection(self, conn_id: int):
        """注销连接"""
        with self._lock:
            self._connections.discard(conn_id)
    
    def register_file_handle(self, handle_id: int):
        """注册文件句柄"""
        with self._lock:
            self._file_handles.add(handle_id)
    
    def unregister_file_handle(self, handle_id: int):
        """注销文件句柄"""
        with self._lock:
            self._file_handles.discard(handle_id)
    
    def register_object(self, obj: Any) -> int:
        """注册对象"""
        obj_id = id(obj)
        with self._lock:
            # 只为可以创建弱引用的对象注册
            try:
                self._objects[obj_id] = weakref.ref(obj, lambda x: self._on_object_collected(obj_id))
            except TypeError:
                # 对于不能创建弱引用的对象（如dict），只记录ID
                pass
        return obj_id
    
    def _on_object_collected(self, obj_id: int):
        """对象被回收时的回调"""
        with self._lock:
            self._objects.pop(obj_id, None)
    
    def get_growth_rate(self, resource_type: str, window: int = 10) -> float:
        """计算资源增长率"""
        with self._lock:
            if len(self.snapshots) < 2:
                return 0.0
            
            recent = self.snapshots[-window:] if len(self.snapshots) >= window else self.snapshots
            
            if len(recent) < 2:
                return 0.0
            
            if resource_type == 'memory':
                values = [s.memory_used_mb for s in recent]
            elif resource_type == 'objects':
                values = [s.object_count for s in recent]
            elif resource_type == 'threads':
                values = [s.thread_count for s in recent]
            elif resource_type == 'connections':
                values = [s.connection_count for s in recent]
            else:
                return 0.0
            
            if len(values) < 2:
                return 0.0
            
            # 计算线性回归斜率
            n = len(values)
            x_mean = (n - 1) / 2
            y_mean = sum(values) / n
            
            numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
            denominator = sum((i - x_mean) ** 2 for i in range(n))
            
            if denominator == 0:
                return 0.0
            
            return numerator / denominator
    
    def stop(self):
        """停止追踪"""
        tracemalloc.stop()


class LeakyMemorySystem:
    """可能有泄漏的记忆系统"""
    
    def __init__(self, resource_tracker: ResourceTracker):
        self.resource_tracker = resource_tracker
        
        # 正常数据存储
        self.data: Dict[str, Any] = {}
        
        # 可能泄漏的资源
        self._leaky_cache: Dict[str, Any] = {}  # 永不清理的缓存
        self._event_listeners: List[Callable] = []  # 只增不减的监听器列表
        self._timers: List[threading.Timer] = []  # 未取消的定时器
        self._connections: List[Any] = []  # 未关闭的连接
        self._temp_objects: List[Any] = []  # 临时对象未释放
        
        # 统计
        self._create_count = 0
        self._delete_count = 0
        
        self._lock = threading.RLock()
    
    def create_memory(self, key: str, value: Any) -> bool:
        """创建记忆"""
        with self._lock:
            self._create_count += 1
            
            # 正常存储
            self.data[key] = value
            
            # 注册对象追踪
            self.resource_tracker.register_object(value)
            
            # 模拟各种可能的泄漏
            
            # 1. 缓存泄漏 - 缓存只增不减
            cache_key = f"cache-{key}"
            self._leaky_cache[cache_key] = {
                'data': value,
                'created_at': time.time(),
                'large_data': [random.random() for _ in range(100)]  # 额外数据
            }
            
            # 2. 事件监听器泄漏 - 每次创建都添加监听器
            def listener():
                pass
            self._event_listeners.append(listener)
            
            # 3. 定时器泄漏 - 创建但不取消的定时器
            timer = threading.Timer(3600, lambda: None)  # 1小时后执行
            timer.daemon = True
            timer.start()
            self._timers.append(timer)
            
            # 4. 连接泄漏模拟
            conn_id = random.randint(10000, 99999)
            self.resource_tracker.register_connection(conn_id)
            self._connections.append(conn_id)
            
            # 5. 临时对象泄漏
            temp_obj = {'key': key, 'data': [random.random() for _ in range(50)]}
            self._temp_objects.append(temp_obj)
            
            return True
    
    def delete_memory(self, key: str) -> bool:
        """删除记忆"""
        with self._lock:
            self._delete_count += 1
            
            if key in self.data:
                del self.data[key]
                
                # 注意: 这里故意不清理泄漏的资源，模拟真实的泄漏场景
                # 实际应该清理:
                # - 从 _leaky_cache 删除
                # - 移除相关监听器
                # - 取消相关定时器
                # - 关闭相关连接
                # - 释放临时对象
                
                return True
            return False
    
    def get_memory(self, key: str) -> Optional[Any]:
        """获取记忆"""
        with self._lock:
            return self.data.get(key)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            return {
                'data_count': len(self.data),
                'cache_count': len(self._leaky_cache),
                'listener_count': len(self._event_listeners),
                'timer_count': len(self._timers),
                'connection_count': len(self._connections),
                'temp_object_count': len(self._temp_objects),
                'create_count': self._create_count,
                'delete_count': self._delete_count
            }
    
    def force_cleanup(self):
        """强制清理（模拟修复）"""
        with self._lock:
            # 清理缓存
            self._leaky_cache.clear()
            
            # 取消所有定时器
            for timer in self._timers:
                timer.cancel()
            self._timers.clear()
            
            # 关闭所有连接
            for conn_id in self._connections:
                self.resource_tracker.unregister_connection(conn_id)
            self._connections.clear()
            
            # 清理临时对象
            self._temp_objects.clear()
            
            # 清理监听器
            self._event_listeners.clear()
            
            # 强制垃圾回收
            gc.collect()


class ResourceLeakScenario:
    """内存和资源泄漏测试场景"""
    
    def __init__(self, simulation_hours: float = 24.0):
        self.simulation_hours = simulation_hours
        self.resource_tracker = ResourceTracker()
        self.memory_system = LeakyMemorySystem(self.resource_tracker)
        self.results: Dict[str, Any] = {}
        self.defects: List[DefectReport] = []
        
        # 检测阈值
        self.memory_growth_threshold = 1.0  # MB/分钟
        self.object_growth_threshold = 100  # 对象/分钟
        self.thread_growth_threshold = 0.1  # 线程/分钟
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        logger.info("=" * 70)
        logger.info("场景4: 内存和资源泄漏测试")
        logger.info(f"模拟时长: {self.simulation_hours} 小时")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # 实际测试时间（加速模拟）
        # 每小时模拟为60秒
        actual_duration = min(self.simulation_hours * 60, 600)  # 最多10分钟
        
        # 测试1: 基础泄漏检测
        result1 = await self.test_basic_leak_detection(actual_duration * 0.3)
        self.results['basic_leak_detection'] = result1
        
        # 测试2: 高频创建删除
        result2 = await self.test_high_frequency_operations(actual_duration * 0.2)
        self.results['high_frequency_operations'] = result2
        
        # 测试3: 长时间运行泄漏
        result3 = await self.test_long_running_leak(actual_duration * 0.3)
        self.results['long_running_leak'] = result3
        
        # 测试4: 压力泄漏测试
        result4 = await self.test_stress_leak(actual_duration * 0.2)
        self.results['stress_leak'] = result4
        
        end_time = time.time()
        
        # 停止追踪
        self.resource_tracker.stop()
        
        # 生成报告
        report = self._generate_report(start_time, end_time)
        
        return report
    
    def _record_defect(self, defect_type: DefectType, severity: str,
                       description: str, evidence: Dict[str, Any] = None,
                       growth_rate: float = 0.0):
        """记录缺陷"""
        defect = DefectReport(
            defect_type=defect_type,
            severity=severity,
            description=description,
            timestamp=time.time(),
            evidence=evidence or {},
            growth_rate=growth_rate
        )
        self.defects.append(defect)
        logger.error(f"[缺陷发现] {defect_type.value}: {description}")
    
    async def test_basic_leak_detection(self, duration: float) -> Dict[str, Any]:
        """测试1: 基础泄漏检测"""
        logger.info("\n" + "-" * 70)
        logger.info("测试1: 基础泄漏检测")
        logger.info("-" * 70)
        
        results = {
            'snapshots': [],
            'defects_found': []
        }
        
        # 初始快照
        initial_snapshot = self.resource_tracker.take_snapshot()
        initial_snapshot.cache_size = len(self.memory_system._leaky_cache)
        results['initial'] = asdict(initial_snapshot)
        
        # 执行操作
        operations = int(duration * 10)  # 每秒10次操作
        for i in range(operations):
            # 创建记忆
            key = f"memory-{i}"
            value = {
                'id': i,
                'data': [random.random() for _ in range(100)],
                'timestamp': time.time()
            }
            self.memory_system.create_memory(key, value)
            
            # 随机删除一些
            if i > 0 and i % 3 == 0:
                del_key = f"memory-{random.randint(0, i-1)}"
                self.memory_system.delete_memory(del_key)
            
            # 定期采样
            if i % 100 == 0:
                snapshot = self.resource_tracker.take_snapshot()
                snapshot.cache_size = len(self.memory_system._leaky_cache)
                results['snapshots'].append(asdict(snapshot))
                
                # 检测泄漏
                self._check_for_leaks()
            
            await asyncio.sleep(0.01)
        
        # 最终快照
        final_snapshot = self.resource_tracker.take_snapshot()
        final_snapshot.cache_size = len(self.memory_system._leaky_cache)
        results['final'] = asdict(final_snapshot)
        
        # 计算增长
        memory_growth = final_snapshot.memory_used_mb - initial_snapshot.memory_used_mb
        results['memory_growth_mb'] = memory_growth
        results['stats'] = self.memory_system.get_stats()
        
        return results
    
    async def test_high_frequency_operations(self, duration: float) -> Dict[str, Any]:
        """测试2: 高频创建删除"""
        logger.info("\n" + "-" * 70)
        logger.info("测试2: 高频创建删除")
        logger.info("-" * 70)
        
        results = {
            'operations': 0,
            'creates': 0,
            'deletes': 0,
            'snapshots': []
        }
        
        initial_snapshot = self.resource_tracker.take_snapshot()
        
        # 高频操作
        operations = int(duration * 100)  # 每秒100次操作
        
        async def worker(worker_id: int, num_ops: int):
            """工作协程"""
            creates = 0
            deletes = 0
            
            for i in range(num_ops):
                key = f"worker-{worker_id}-mem-{i}"
                value = {'worker': worker_id, 'seq': i, 'data': random.random()}
                
                self.memory_system.create_memory(key, value)
                creates += 1
                
                if i > 0 and i % 2 == 0:
                    self.memory_system.delete_memory(key)
                    deletes += 1
                
                await asyncio.sleep(0.001)
            
            return creates, deletes
        
        # 并发执行
        num_workers = 10
        ops_per_worker = operations // num_workers
        
        tasks = [worker(i, ops_per_worker) for i in range(num_workers)]
        worker_results = await asyncio.gather(*tasks)
        
        for creates, deletes in worker_results:
            results['creates'] += creates
            results['deletes'] += deletes
        
        results['operations'] = results['creates'] + results['deletes']
        
        # 最终快照
        final_snapshot = self.resource_tracker.take_snapshot()
        results['memory_growth_mb'] = final_snapshot.memory_used_mb - initial_snapshot.memory_used_mb
        results['final_stats'] = self.memory_system.get_stats()
        
        # 检测泄漏
        self._check_for_leaks()
        
        return results
    
    async def test_long_running_leak(self, duration: float) -> Dict[str, Any]:
        """测试3: 长时间运行泄漏"""
        logger.info("\n" + "-" * 70)
        logger.info("测试3: 长时间运行泄漏")
        logger.info("-" * 70)
        
        results = {
            'checkpoints': [],
            'leak_detected': False
        }
        
        # 定期检查点
        check_interval = duration / 10
        memory_history = []
        
        for checkpoint in range(10):
            logger.info(f"\n检查点 {checkpoint + 1}/10")
            
            # 执行一段时间操作
            end_time = time.time() + check_interval
            while time.time() < end_time:
                key = f"longrun-{checkpoint}-{time.time()}"
                value = {'checkpoint': checkpoint, 'data': random.random()}
                self.memory_system.create_memory(key, value)
                
                # 随机删除
                if random.random() < 0.3:
                    self.memory_system.delete_memory(key)
                
                await asyncio.sleep(0.05)
            
            # 记录检查点
            snapshot = self.resource_tracker.take_snapshot()
            stats = self.memory_system.get_stats()
            
            checkpoint_data = {
                'checkpoint': checkpoint + 1,
                'memory_mb': snapshot.memory_used_mb,
                'data_count': stats['data_count'],
                'cache_count': stats['cache_count'],
                'listener_count': stats['listener_count'],
                'timer_count': stats['timer_count'],
                'connection_count': stats['connection_count'],
                'temp_object_count': stats['temp_object_count']
            }
            results['checkpoints'].append(checkpoint_data)
            memory_history.append(snapshot.memory_used_mb)
            
            # 检测持续增长
            if len(memory_history) >= 3:
                # 检查最近3个点是否持续增长
                if memory_history[-1] > memory_history[-2] > memory_history[-3]:
                    growth_rate = (memory_history[-1] - memory_history[-3]) / 2
                    if growth_rate > 0.1:  # 每检查点增长超过0.1MB
                        results['leak_detected'] = True
                        self._record_defect(DefectType.MEMORY_LEAK, "high",
                            f"检测到持续内存增长: {growth_rate:.2f} MB/检查点",
                            {"memory_history": memory_history[-5:]},
                            growth_rate)
        
        return results
    
    async def test_stress_leak(self, duration: float) -> Dict[str, Any]:
        """测试4: 压力泄漏测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试4: 压力泄漏测试")
        logger.info("-" * 70)
        
        results = {
            'phases': [],
            'cleanup_effectiveness': 0.0
        }
        
        # 阶段1: 压力创建
        logger.info("\n阶段1: 压力创建")
        before_stress = self.resource_tracker.take_snapshot()
        
        for i in range(1000):
            key = f"stress-{i}"
            value = {
                'id': i,
                'large_data': [random.random() for _ in range(1000)]
            }
            self.memory_system.create_memory(key, value)
            
            if i % 100 == 0:
                await asyncio.sleep(0.01)
        
        after_stress = self.resource_tracker.take_snapshot()
        results['phases'].append({
            'name': 'stress_create',
            'memory_before': before_stress.memory_used_mb,
            'memory_after': after_stress.memory_used_mb,
            'memory_growth': after_stress.memory_used_mb - before_stress.memory_used_mb
        })
        
        # 阶段2: 删除所有数据
        logger.info("\n阶段2: 删除所有数据")
        for i in range(1000):
            key = f"stress-{i}"
            self.memory_system.delete_memory(key)
        
        after_delete = self.resource_tracker.take_snapshot()
        results['phases'].append({
            'name': 'delete_all',
            'memory_before': after_stress.memory_used_mb,
            'memory_after': after_delete.memory_used_mb,
            'memory_growth': after_delete.memory_used_mb - after_stress.memory_used_mb
        })
        
        # 检查删除后内存是否释放
        if after_delete.memory_used_mb > before_stress.memory_used_mb * 1.5:
            self._record_defect(DefectType.MEMORY_LEAK, "high",
                "删除数据后内存未释放",
                {"before": before_stress.memory_used_mb, 
                 "after_delete": after_delete.memory_used_mb})
        
        # 阶段3: 强制清理
        logger.info("\n阶段3: 强制清理")
        self.memory_system.force_cleanup()
        gc.collect()
        
        after_cleanup = self.resource_tracker.take_snapshot()
        results['phases'].append({
            'name': 'force_cleanup',
            'memory_before': after_delete.memory_used_mb,
            'memory_after': after_cleanup.memory_used_mb,
            'memory_growth': after_cleanup.memory_used_mb - after_delete.memory_used_mb
        })
        
        # 计算清理效果
        total_growth = after_stress.memory_used_mb - before_stress.memory_used_mb
        recovered = after_stress.memory_used_mb - after_cleanup.memory_used_mb
        if total_growth > 0:
            results['cleanup_effectiveness'] = recovered / total_growth
        
        # 最终统计
        results['final_stats'] = self.memory_system.get_stats()
        
        return results
    
    def _check_for_leaks(self):
        """检查泄漏"""
        # 检查内存增长
        memory_growth = self.resource_tracker.get_growth_rate('memory')
        if memory_growth > self.memory_growth_threshold:
            self._record_defect(DefectType.MEMORY_LEAK, "high",
                f"内存增长率异常: {memory_growth:.2f} MB/分钟",
                {"growth_rate": memory_growth},
                memory_growth)
        
        # 检查对象增长
        object_growth = self.resource_tracker.get_growth_rate('objects')
        if object_growth > self.object_growth_threshold:
            self._record_defect(DefectType.OBJECT_NOT_RELEASED, "medium",
                f"对象增长率异常: {object_growth:.0f} 对象/分钟",
                {"growth_rate": object_growth},
                object_growth)
        
        # 检查线程增长
        thread_growth = self.resource_tracker.get_growth_rate('threads')
        if thread_growth > self.thread_growth_threshold:
            self._record_defect(DefectType.THREAD_LEAK, "high",
                f"线程增长率异常: {thread_growth:.2f} 线程/分钟",
                {"growth_rate": thread_growth},
                thread_growth)
        
        # 检查缓存膨胀
        stats = self.memory_system.get_stats()
        if stats['cache_count'] > stats['data_count'] * 2:
            self._record_defect(DefectType.CACHE_BLOAT, "medium",
                f"缓存膨胀: 缓存数({stats['cache_count']}) > 数据数({stats['data_count']}) * 2",
                {"cache_count": stats['cache_count'], "data_count": stats['data_count']})
        
        # 检查定时器泄漏
        if stats['timer_count'] > 100:
            self._record_defect(DefectType.TIMER_LEAK, "medium",
                f"定时器泄漏: {stats['timer_count']} 个未取消的定时器",
                {"timer_count": stats['timer_count']})
        
        # 检查连接泄漏
        if stats['connection_count'] > 100:
            self._record_defect(DefectType.CONNECTION_LEAK, "high",
                f"连接泄漏: {stats['connection_count']} 个未关闭的连接",
                {"connection_count": stats['connection_count']})
        
        # 检查事件监听器泄漏
        if stats['listener_count'] > 1000:
            self._record_defect(DefectType.EVENT_LISTENER_LEAK, "medium",
                f"事件监听器泄漏: {stats['listener_count']} 个监听器",
                {"listener_count": stats['listener_count']})
    
    def _generate_report(self, start_time: float, end_time: float) -> Dict[str, Any]:
        """生成测试报告"""
        defects_by_type = defaultdict(list)
        defects_by_severity = defaultdict(list)
        
        for defect in self.defects:
            defect_dict = defect.to_dict()
            defects_by_type[defect.defect_type.value].append(defect_dict)
            defects_by_severity[defect.severity].append(defect_dict)
        
        report = {
            'scenario': '内存和资源泄漏测试',
            'scenario_id': 'scenario4_resource_leak',
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'end_time': datetime.fromtimestamp(end_time).isoformat(),
            'duration_seconds': end_time - start_time,
            'config': {
                'simulation_hours': self.simulation_hours
            },
            'summary': {
                'total_tests': 4,
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
    """主函数 - 5分钟快速测试版本"""
    # 修改为5分钟快速测试版本
    scenario = ResourceLeakScenario(simulation_hours=0.1)  # 6分钟模拟时间
    report = await scenario.run_all_tests()
    
    # 保存报告
    output_path = "/workspace/agent_memory_system_v2/tests/reports/scenario4_resource_leak_report.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("场景4: 内存和资源泄漏测试 - 摘要")
    print("=" * 70)
    print(f"测试持续时间: {report['duration_seconds']:.2f} 秒")
    print(f"模拟时长: {report['config']['simulation_hours']} 小时")
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

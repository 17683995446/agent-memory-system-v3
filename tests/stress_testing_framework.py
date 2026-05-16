#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 压力测试框架
Stress Testing Framework for Deep Validation Phase

包含:
- 高并发负载生成器
- 渐进式压力测试
- 脉冲式压力测试
- Soak测试
- 资源饱和度测试
- 性能拐点识别
"""

import asyncio
import time
import random
import statistics
import psutil
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from collections import defaultdict
import json
import logging
import signal
import sys
from contextlib import contextmanager
import threading
import queue

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class StressTestConfig:
    """压力测试配置"""
    # 基础配置
    test_duration: int = 300  # 测试持续时间(秒)
    warmup_duration: int = 30  # 预热时间(秒)
    cooldown_duration: int = 30  # 冷却时间(秒)
    
    # 并发配置
    min_concurrency: int = 10
    max_concurrency: int = 1000
    concurrency_step: int = 50
    
    # 负载配置
    requests_per_second: float = 100.0
    request_timeout: float = 30.0
    
    # 脉冲测试配置
    pulse_duration: int = 60  # 脉冲持续时间
    pulse_intensity: float = 3.0  # 脉冲强度倍数
    pulse_interval: int = 120  # 脉冲间隔
    
    # Soak测试配置
    soak_duration_hours: int = 24  # Soak测试持续时间
    
    # 资源监控配置
    resource_check_interval: float = 1.0
    cpu_threshold: float = 90.0
    memory_threshold: float = 90.0
    disk_threshold: float = 90.0
    
    # 报告配置
    report_interval: int = 60
    output_dir: str = "./reports"


@dataclass
class LoadMetrics:
    """负载指标"""
    timestamp: float
    concurrency: int
    requests_sent: int
    requests_success: int
    requests_failed: int
    latency_ms: List[float]
    throughput_rps: float
    error_rate: float
    cpu_percent: float
    memory_percent: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_io_sent_mb: float
    network_io_recv_mb: float


@dataclass
class StressTestResult:
    """压力测试结果"""
    test_name: str
    test_type: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    config: StressTestConfig
    metrics_history: List[LoadMetrics] = field(default_factory=list)
    breaking_point: Optional[Dict[str, Any]] = None
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)


class ResourceMonitor:
    """资源监控器"""
    
    def __init__(self, check_interval: float = 1.0):
        self.check_interval = check_interval
        self.running = False
        self.metrics_history: List[Dict[str, Any]] = []
        self.monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
    def start(self):
        """启动资源监控"""
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("资源监控器已启动")
        
    def stop(self):
        """停止资源监控"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        logger.info("资源监控器已停止")
        
    def _monitor_loop(self):
        """监控循环"""
        while self.running:
            try:
                metrics = self._collect_metrics()
                with self._lock:
                    self.metrics_history.append(metrics)
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"资源监控错误: {e}")
                
    def _collect_metrics(self) -> Dict[str, Any]:
        """收集系统资源指标"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk_io = psutil.disk_io_counters()
        net_io = psutil.net_io_counters()
        
        # 计算磁盘IO速率 (MB/s)
        disk_read_mb = disk_io.read_bytes / 1024 / 1024 if disk_io else 0
        disk_write_mb = disk_io.write_bytes / 1024 / 1024 if disk_io else 0
        
        # 计算网络IO速率 (MB/s)
        net_sent_mb = net_io.bytes_sent / 1024 / 1024 if net_io else 0
        net_recv_mb = net_io.bytes_recv / 1024 / 1024 if net_io else 0
        
        return {
            'timestamp': time.time(),
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'memory_used_gb': memory.used / 1024 / 1024 / 1024,
            'memory_available_gb': memory.available / 1024 / 1024 / 1024,
            'disk_read_mb': disk_read_mb,
            'disk_write_mb': disk_write_mb,
            'network_sent_mb': net_sent_mb,
            'network_recv_mb': net_recv_mb,
            'load_avg': psutil.getloadavg() if hasattr(psutil, 'getloadavg') else (0, 0, 0)
        }
        
    def get_latest_metrics(self) -> Optional[Dict[str, Any]]:
        """获取最新指标"""
        with self._lock:
            return self.metrics_history[-1] if self.metrics_history else None
            
    def get_metrics_history(self) -> List[Dict[str, Any]]:
        """获取历史指标"""
        with self._lock:
            return self.metrics_history.copy()


class LoadGenerator:
    """负载生成器"""
    
    def __init__(self, config: StressTestConfig):
        self.config = config
        self.running = False
        self.stats = {
            'requests_sent': 0,
            'requests_success': 0,
            'requests_failed': 0,
            'latencies': [],
            'errors': defaultdict(int)
        }
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        
    async def generate_load(
        self,
        operation: Callable,
        concurrency: int,
        duration: int,
        target_rps: Optional[float] = None
    ) -> Dict[str, Any]:
        """生成负载"""
        self.running = True
        self._stop_event.clear()
        
        # 重置统计
        with self._lock:
            self.stats = {
                'requests_sent': 0,
                'requests_success': 0,
                'requests_failed': 0,
                'latencies': [],
                'errors': defaultdict(int)
            }
        
        start_time = time.time()
        end_time = start_time + duration
        
        # 创建任务队列
        task_queue = asyncio.Queue(maxsize=concurrency * 2)
        
        # 启动工作协程
        workers = [
            asyncio.create_task(self._worker(task_queue, operation))
            for _ in range(concurrency)
        ]
        
        # 启动负载注入器
        injector = asyncio.create_task(
            self._load_injector(task_queue, target_rps or self.config.requests_per_second, end_time)
        )
        
        # 等待测试完成
        try:
            await asyncio.wait_for(injector, timeout=duration + 10)
        except asyncio.TimeoutError:
            logger.warning("负载注入器超时")
        
        # 停止工作协程
        self.running = False
        self._stop_event.set()
        
        # 清空队列
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        
        # 等待工作协程完成
        await asyncio.gather(*workers, return_exceptions=True)
        
        # 返回统计结果
        with self._lock:
            return {
                'requests_sent': self.stats['requests_sent'],
                'requests_success': self.stats['requests_success'],
                'requests_failed': self.stats['requests_failed'],
                'latencies': self.stats['latencies'].copy(),
                'errors': dict(self.stats['errors']),
                'duration': time.time() - start_time
            }
    
    async def _worker(self, queue: asyncio.Queue, operation: Callable):
        """工作协程"""
        while self.running and not self._stop_event.is_set():
            try:
                # 等待任务，带超时
                task = await asyncio.wait_for(queue.get(), timeout=1.0)
                
                # 执行任务
                start = time.time()
                try:
                    await operation(task)
                    latency = (time.time() - start) * 1000  # 转换为毫秒
                    
                    with self._lock:
                        self.stats['requests_success'] += 1
                        self.stats['latencies'].append(latency)
                except Exception as e:
                    with self._lock:
                        self.stats['requests_failed'] += 1
                        self.stats['errors'][str(type(e).__name__)] += 1
                
                with self._lock:
                    self.stats['requests_sent'] += 1
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"工作协程错误: {e}")
    
    async def _load_injector(self, queue: asyncio.Queue, target_rps: float, end_time: float):
        """负载注入器"""
        interval = 1.0 / target_rps if target_rps > 0 else 0.01
        
        while time.time() < end_time and self.running:
            try:
                # 生成任务
                task = self._generate_task()
                
                # 尝试放入队列
                try:
                    queue.put_nowait(task)
                except asyncio.QueueFull:
                    # 队列满，等待
                    await asyncio.sleep(0.001)
                    continue
                
                # 控制速率
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"负载注入错误: {e}")
    
    def _generate_task(self) -> Dict[str, Any]:
        """生成测试任务"""
        return {
            'id': random.randint(1, 1000000),
            'type': random.choice(['read', 'write', 'search', 'delete']),
            'priority': random.randint(1, 5),
            'timestamp': time.time()
        }
    
    def stop(self):
        """停止负载生成"""
        self.running = False
        self._stop_event.set()


class StressTestingFramework:
    """压力测试框架"""
    
    def __init__(self, config: Optional[StressTestConfig] = None):
        self.config = config or StressTestConfig()
        self.resource_monitor = ResourceMonitor(self.config.resource_check_interval)
        self.load_generator = LoadGenerator(self.config)
        self.results: List[StressTestResult] = []
        
    async def run_all_tests(self) -> List[StressTestResult]:
        """运行所有压力测试"""
        logger.info("=" * 60)
        logger.info("开始执行压力测试套件")
        logger.info("=" * 60)
        
        # 1. 渐进式压力测试
        result1 = await self.run_progressive_stress_test()
        self.results.append(result1)
        
        # 2. 脉冲式压力测试
        result2 = await self.run_pulse_stress_test()
        self.results.append(result2)
        
        # 3. Soak测试
        result3 = await self.run_soak_test()
        self.results.append(result3)
        
        # 4. 资源饱和度测试
        result4 = await self.run_resource_saturation_test()
        self.results.append(result4)
        
        # 5. 性能拐点识别
        result5 = await self.run_breaking_point_detection()
        self.results.append(result5)
        
        logger.info("=" * 60)
        logger.info("压力测试套件执行完成")
        logger.info("=" * 60)
        
        return self.results
    
    async def run_progressive_stress_test(self) -> StressTestResult:
        """渐进式压力测试"""
        logger.info("\n" + "=" * 60)
        logger.info("开始渐进式压力测试")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        metrics_history = []
        
        # 启动资源监控
        self.resource_monitor.start()
        
        # 模拟操作
        async def mock_operation(task: Dict[str, Any]):
            # 模拟处理延迟
            delay = random.uniform(0.001, 0.01)
            await asyncio.sleep(delay)
        
        # 逐步增加并发
        current_concurrency = self.config.min_concurrency
        step = 0
        
        while current_concurrency <= self.config.max_concurrency:
            logger.info(f"\n步骤 {step + 1}: 并发数 = {current_concurrency}")
            
            # 预热
            logger.info(f"预热 {self.config.warmup_duration} 秒...")
            await self.load_generator.generate_load(
                mock_operation,
                current_concurrency,
                self.config.warmup_duration,
                self.config.requests_per_second
            )
            
            # 正式测试
            logger.info(f"测试 {self.config.test_duration} 秒...")
            result = await self.load_generator.generate_load(
                mock_operation,
                current_concurrency,
                self.config.test_duration,
                self.config.requests_per_second
            )
            
            # 收集指标
            metrics = self._create_metrics(result, current_concurrency)
            metrics_history.append(metrics)
            
            # 检查是否达到资源阈值
            latest_resource = self.resource_monitor.get_latest_metrics()
            if latest_resource:
                if (latest_resource['cpu_percent'] > self.config.cpu_threshold or
                    latest_resource['memory_percent'] > self.config.memory_threshold):
                    logger.warning(f"达到资源阈值! CPU: {latest_resource['cpu_percent']:.1f}%, "
                                 f"内存: {latest_resource['memory_percent']:.1f}%")
                    break
            
            # 增加并发
            current_concurrency += self.config.concurrency_step
            step += 1
            
            # 冷却
            logger.info(f"冷却 {self.config.cooldown_duration} 秒...")
            await asyncio.sleep(self.config.cooldown_duration)
        
        # 停止资源监控
        self.resource_monitor.stop()
        
        end_time = datetime.now()
        
        # 生成结果
        result = StressTestResult(
            test_name="渐进式压力测试",
            test_type="progressive",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(end_time - start_time).total_seconds(),
            config=self.config,
            metrics_history=metrics_history,
            summary=self._generate_summary(metrics_history)
        )
        
        logger.info(f"\n渐进式压力测试完成")
        logger.info(f"持续时间: {result.duration_seconds:.1f} 秒")
        logger.info(f"最大并发: {max([m.concurrency for m in metrics_history])}")
        
        return result
    
    async def run_pulse_stress_test(self) -> StressTestResult:
        """脉冲式压力测试"""
        logger.info("\n" + "=" * 60)
        logger.info("开始脉冲式压力测试")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        metrics_history = []
        
        # 启动资源监控
        self.resource_monitor.start()
        
        async def mock_operation(task: Dict[str, Any]):
            delay = random.uniform(0.001, 0.01)
            await asyncio.sleep(delay)
        
        # 基础负载
        base_concurrency = self.config.min_concurrency
        pulse_concurrency = int(base_concurrency * self.config.pulse_intensity)
        
        # 测试周期数
        num_cycles = self.config.test_duration // (self.config.pulse_duration + self.config.pulse_interval)
        
        for cycle in range(num_cycles):
            logger.info(f"\n周期 {cycle + 1}/{num_cycles}")
            
            # 基础负载阶段
            logger.info(f"基础负载: 并发={base_concurrency}, 持续={self.config.pulse_interval}秒")
            result = await self.load_generator.generate_load(
                mock_operation,
                base_concurrency,
                self.config.pulse_interval,
                self.config.requests_per_second
            )
            metrics = self._create_metrics(result, base_concurrency, is_pulse=False)
            metrics_history.append(metrics)
            
            # 脉冲负载阶段
            logger.info(f"脉冲负载: 并发={pulse_concurrency}, 持续={self.config.pulse_duration}秒")
            result = await self.load_generator.generate_load(
                mock_operation,
                pulse_concurrency,
                self.config.pulse_duration,
                self.config.requests_per_second * self.config.pulse_intensity
            )
            metrics = self._create_metrics(result, pulse_concurrency, is_pulse=True)
            metrics_history.append(metrics)
        
        # 停止资源监控
        self.resource_monitor.stop()
        
        end_time = datetime.now()
        
        result = StressTestResult(
            test_name="脉冲式压力测试",
            test_type="pulse",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(end_time - start_time).total_seconds(),
            config=self.config,
            metrics_history=metrics_history,
            summary=self._generate_summary(metrics_history)
        )
        
        logger.info(f"\n脉冲式压力测试完成")
        
        return result
    
    async def run_soak_test(self) -> StressTestResult:
        """Soak测试（长时间稳定性测试）"""
        logger.info("\n" + "=" * 60)
        logger.info("开始Soak测试")
        logger.info(f"计划持续时间: {self.config.soak_duration_hours} 小时")
        logger.info("=" * 60)
        
        # 使用较短的测试时间用于演示
        soak_duration = min(300, self.config.soak_duration_hours * 3600)  # 最多5分钟演示
        
        start_time = datetime.now()
        metrics_history = []
        anomalies = []
        
        # 启动资源监控
        self.resource_monitor.start()
        
        async def mock_operation(task: Dict[str, Any]):
            # 模拟内存泄漏场景
            if random.random() < 0.001:  # 0.1% 概率模拟慢操作
                await asyncio.sleep(1.0)
            else:
                await asyncio.sleep(random.uniform(0.001, 0.01))
        
        concurrency = self.config.min_concurrency * 2
        check_interval = 60  # 每分钟检查一次
        elapsed = 0
        
        while elapsed < soak_duration:
            logger.info(f"\nSoak测试进度: {elapsed}/{soak_duration} 秒")
            
            # 运行一个检查周期
            result = await self.load_generator.generate_load(
                mock_operation,
                concurrency,
                check_interval,
                self.config.requests_per_second
            )
            
            metrics = self._create_metrics(result, concurrency)
            metrics_history.append(metrics)
            
            # 检测异常
            if metrics.error_rate > 0.01:  # 错误率超过1%
                anomalies.append({
                    'timestamp': time.time(),
                    'type': 'high_error_rate',
                    'value': metrics.error_rate,
                    'message': f'错误率异常: {metrics.error_rate:.2%}'
                })
            
            # 检查内存增长趋势
            resource_metrics = self.resource_monitor.get_latest_metrics()
            if resource_metrics and resource_metrics['memory_percent'] > 80:
                anomalies.append({
                    'timestamp': time.time(),
                    'type': 'high_memory_usage',
                    'value': resource_metrics['memory_percent'],
                    'message': f'内存使用率过高: {resource_metrics["memory_percent"]:.1f}%'
                })
            
            elapsed += check_interval
        
        # 停止资源监控
        self.resource_monitor.stop()
        
        end_time = datetime.now()
        
        result = StressTestResult(
            test_name="Soak测试",
            test_type="soak",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(end_time - start_time).total_seconds(),
            config=self.config,
            metrics_history=metrics_history,
            anomalies=anomalies,
            summary=self._generate_summary(metrics_history)
        )
        
        logger.info(f"\nSoak测试完成")
        logger.info(f"发现异常数: {len(anomalies)}")
        
        return result
    
    async def run_resource_saturation_test(self) -> StressTestResult:
        """资源饱和度测试"""
        logger.info("\n" + "=" * 60)
        logger.info("开始资源饱和度测试")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        metrics_history = []
        
        # 启动资源监控
        self.resource_monitor.start()
        
        async def cpu_intensive_operation(task: Dict[str, Any]):
            # CPU密集型操作
            start = time.time()
            while time.time() - start < 0.01:
                _ = sum(i ** 2 for i in range(1000))
            await asyncio.sleep(0.001)
        
        async def memory_intensive_operation(task: Dict[str, Any]):
            # 内存密集型操作
            data = [random.random() for _ in range(10000)]
            await asyncio.sleep(0.01)
            del data
        
        async def io_intensive_operation(task: Dict[str, Any]):
            # IO密集型操作
            await asyncio.sleep(random.uniform(0.05, 0.1))
        
        operations = [
            ('cpu', cpu_intensive_operation),
            ('memory', memory_intensive_operation),
            ('io', io_intensive_operation)
        ]
        
        for resource_type, operation in operations:
            logger.info(f"\n测试资源类型: {resource_type.upper()}")
            
            concurrency = self.config.min_concurrency
            while concurrency <= self.config.max_concurrency:
                logger.info(f"  并发: {concurrency}")
                
                result = await self.load_generator.generate_load(
                    operation,
                    concurrency,
                    60,  # 每个级别测试60秒
                    self.config.requests_per_second
                )
                
                metrics = self._create_metrics(result, concurrency)
                metrics_history.append(metrics)
                
                # 检查资源饱和度
                resource_metrics = self.resource_monitor.get_latest_metrics()
                if resource_metrics:
                    if resource_type == 'cpu' and resource_metrics['cpu_percent'] > 95:
                        logger.warning(f"CPU达到饱和: {resource_metrics['cpu_percent']:.1f}%")
                        break
                    elif resource_type == 'memory' and resource_metrics['memory_percent'] > 95:
                        logger.warning(f"内存达到饱和: {resource_metrics['memory_percent']:.1f}%")
                        break
                
                concurrency += self.config.concurrency_step
        
        # 停止资源监控
        self.resource_monitor.stop()
        
        end_time = datetime.now()
        
        result = StressTestResult(
            test_name="资源饱和度测试",
            test_type="saturation",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(end_time - start_time).total_seconds(),
            config=self.config,
            metrics_history=metrics_history,
            summary=self._generate_summary(metrics_history)
        )
        
        logger.info(f"\n资源饱和度测试完成")
        
        return result
    
    async def run_breaking_point_detection(self) -> StressTestResult:
        """性能拐点识别"""
        logger.info("\n" + "=" * 60)
        logger.info("开始性能拐点识别")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        metrics_history = []
        breaking_point = None
        
        # 启动资源监控
        self.resource_monitor.start()
        
        async def mock_operation(task: Dict[str, Any]):
            await asyncio.sleep(random.uniform(0.001, 0.01))
        
        concurrency = self.config.min_concurrency
        previous_throughput = 0
        previous_latency_p99 = 0
        
        while concurrency <= self.config.max_concurrency:
            logger.info(f"\n测试并发: {concurrency}")
            
            result = await self.load_generator.generate_load(
                mock_operation,
                concurrency,
                60,
                self.config.requests_per_second
            )
            
            metrics = self._create_metrics(result, concurrency)
            metrics_history.append(metrics)
            
            # 计算P99延迟
            latencies = sorted(result['latencies'])
            p99_latency = latencies[int(len(latencies) * 0.99)] if latencies else 0
            
            # 检测拐点
            throughput_change = (metrics.throughput_rps - previous_throughput) / previous_throughput if previous_throughput > 0 else 1
            latency_change = (p99_latency - previous_latency_p99) / previous_latency_p99 if previous_latency_p99 > 0 else 0
            
            logger.info(f"  吞吐量: {metrics.throughput_rps:.1f} RPS (变化: {throughput_change:+.1%})")
            logger.info(f"  P99延迟: {p99_latency:.2f} ms (变化: {latency_change:+.1%})")
            logger.info(f"  错误率: {metrics.error_rate:.2%}")
            
            # 拐点判定条件
            if (throughput_change < 0.1 or  # 吞吐量增长小于10%
                latency_change > 0.5 or      # P99延迟增长超过50%
                metrics.error_rate > 0.05):  # 错误率超过5%
                
                if breaking_point is None:
                    breaking_point = {
                        'concurrency': concurrency,
                        'throughput_rps': metrics.throughput_rps,
                        'p99_latency_ms': p99_latency,
                        'error_rate': metrics.error_rate,
                        'timestamp': time.time()
                    }
                    logger.warning(f"\n*** 发现性能拐点! 并发={concurrency} ***")
                    logger.warning(f"  吞吐量: {metrics.throughput_rps:.1f} RPS")
                    logger.warning(f"  P99延迟: {p99_latency:.2f} ms")
                    logger.warning(f"  错误率: {metrics.error_rate:.2%}")
                    break
            
            previous_throughput = metrics.throughput_rps
            previous_latency_p99 = p99_latency
            
            concurrency += self.config.concurrency_step
        
        # 停止资源监控
        self.resource_monitor.stop()
        
        end_time = datetime.now()
        
        result = StressTestResult(
            test_name="性能拐点识别",
            test_type="breaking_point",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(end_time - start_time).total_seconds(),
            config=self.config,
            metrics_history=metrics_history,
            breaking_point=breaking_point,
            summary=self._generate_summary(metrics_history)
        )
        
        logger.info(f"\n性能拐点识别完成")
        if breaking_point:
            logger.info(f"性能拐点: 并发={breaking_point['concurrency']}")
        else:
            logger.info("未找到性能拐点")
        
        return result
    
    def _create_metrics(
        self,
        result: Dict[str, Any],
        concurrency: int,
        is_pulse: bool = False
    ) -> LoadMetrics:
        """创建负载指标"""
        latencies = result.get('latencies', [])
        
        # 获取资源指标
        resource_metrics = self.resource_monitor.get_latest_metrics() or {}
        
        total_requests = result['requests_sent']
        success_requests = result['requests_success']
        
        return LoadMetrics(
            timestamp=time.time(),
            concurrency=concurrency,
            requests_sent=total_requests,
            requests_success=success_requests,
            requests_failed=result['requests_failed'],
            latency_ms=latencies,
            throughput_rps=success_requests / result['duration'] if result['duration'] > 0 else 0,
            error_rate=(result['requests_failed'] / total_requests) if total_requests > 0 else 0,
            cpu_percent=resource_metrics.get('cpu_percent', 0),
            memory_percent=resource_metrics.get('memory_percent', 0),
            disk_io_read_mb=resource_metrics.get('disk_read_mb', 0),
            disk_io_write_mb=resource_metrics.get('disk_write_mb', 0),
            network_io_sent_mb=resource_metrics.get('network_sent_mb', 0),
            network_io_recv_mb=resource_metrics.get('network_recv_mb', 0)
        )
    
    def _generate_summary(self, metrics_history: List[LoadMetrics]) -> Dict[str, Any]:
        """生成测试摘要"""
        if not metrics_history:
            return {}
        
        all_latencies = []
        for m in metrics_history:
            all_latencies.extend(m.latency_ms)
        
        if all_latencies:
            all_latencies.sort()
            p50 = all_latencies[int(len(all_latencies) * 0.50)]
            p95 = all_latencies[int(len(all_latencies) * 0.95)]
            p99 = all_latencies[int(len(all_latencies) * 0.99)]
        else:
            p50 = p95 = p99 = 0
        
        max_throughput = max(m.throughput_rps for m in metrics_history)
        avg_error_rate = statistics.mean(m.error_rate for m in metrics_history)
        max_concurrency = max(m.concurrency for m in metrics_history)
        
        return {
            'total_requests': sum(m.requests_sent for m in metrics_history),
            'total_success': sum(m.requests_success for m in metrics_history),
            'total_failed': sum(m.requests_failed for m in metrics_history),
            'max_throughput_rps': max_throughput,
            'avg_error_rate': avg_error_rate,
            'max_concurrency': max_concurrency,
            'latency_p50_ms': p50,
            'latency_p95_ms': p95,
            'latency_p99_ms': p99,
            'avg_cpu_percent': statistics.mean(m.cpu_percent for m in metrics_history),
            'max_cpu_percent': max(m.cpu_percent for m in metrics_history),
            'avg_memory_percent': statistics.mean(m.memory_percent for m in metrics_history),
            'max_memory_percent': max(m.memory_percent for m in metrics_history)
        }
    
    def generate_report(self, output_file: str = "stress_test_report.json"):
        """生成测试报告"""
        report = {
            'test_suite': 'Agent Memory System Stress Testing',
            'version': '3.0',
            'generated_at': datetime.now().isoformat(),
            'config': {
                'min_concurrency': self.config.min_concurrency,
                'max_concurrency': self.config.max_concurrency,
                'test_duration': self.config.test_duration
            },
            'results': []
        }
        
        for result in self.results:
            report['results'].append({
                'test_name': result.test_name,
                'test_type': result.test_type,
                'duration_seconds': result.duration_seconds,
                'summary': result.summary,
                'breaking_point': result.breaking_point,
                'anomalies_count': len(result.anomalies)
            })
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n测试报告已保存: {output_file}")
        return report


async def main():
    """主函数"""
    # 创建配置
    config = StressTestConfig(
        test_duration=60,  # 每个测试60秒
        warmup_duration=10,
        cooldown_duration=10,
        min_concurrency=10,
        max_concurrency=200,
        concurrency_step=50,
        soak_duration_hours=1  # 演示用1小时
    )
    
    # 创建测试框架
    framework = StressTestingFramework(config)
    
    # 运行所有测试
    results = await framework.run_all_tests()
    
    # 生成报告
    report = framework.generate_report("/workspace/agent_memory_system_v2/tests/reports/stress_test_report.json")
    
    # 打印摘要
    print("\n" + "=" * 60)
    print("压力测试摘要")
    print("=" * 60)
    for result in results:
        print(f"\n{result.test_name}:")
        print(f"  持续时间: {result.duration_seconds:.1f} 秒")
        print(f"  最大吞吐量: {result.summary.get('max_throughput_rps', 0):.1f} RPS")
        print(f"  P99延迟: {result.summary.get('latency_p99_ms', 0):.2f} ms")
        print(f"  平均错误率: {result.summary.get('avg_error_rate', 0):.2%}")
        if result.breaking_point:
            print(f"  性能拐点: 并发={result.breaking_point['concurrency']}")


if __name__ == "__main__":
    # 设置信号处理
    def signal_handler(sig, frame):
        logger.info("接收到中断信号，正在停止...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 运行测试
    asyncio.run(main())

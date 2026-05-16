#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 故障注入测试套件
Fault Injection Test Suite for Deep Validation Phase

包含:
- 网络分区测试
- 节点故障测试
- 磁盘故障测试
- 内存压力测试
- 时钟偏移测试
- 级联故障测试
"""

import asyncio
import time
import random
import string
import json
import logging
import threading
import socket
import subprocess
import os
import signal
import tempfile
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Tuple, Set
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FaultType(Enum):
    """故障类型枚举"""
    NETWORK_PARTITION = "network_partition"
    NODE_FAILURE = "node_failure"
    DISK_FAILURE = "disk_failure"
    MEMORY_PRESSURE = "memory_pressure"
    CLOCK_SKEW = "clock_skew"
    CASCADE_FAILURE = "cascade_failure"
    CPU_SATURATION = "cpu_saturation"
    PACKET_LOSS = "packet_loss"
    LATENCY_SPIKE = "latency_spike"


@dataclass
class FaultScenario:
    """故障场景配置"""
    fault_type: FaultType
    target_nodes: List[str]
    duration_seconds: float
    intensity: float  # 0.0 - 1.0
    recovery_time_seconds: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FaultInjectionResult:
    """故障注入测试结果"""
    scenario: FaultScenario
    start_time: datetime
    end_time: datetime
    success: bool
    error_message: Optional[str] = None
    metrics_before: Dict[str, Any] = field(default_factory=dict)
    metrics_during: Dict[str, Any] = field(default_factory=dict)
    metrics_after: Dict[str, Any] = field(default_factory=dict)
    recovery_time_ms: float = 0.0
    data_loss_count: int = 0
    inconsistency_count: int = 0


@dataclass
class NodeState:
    """节点状态"""
    node_id: str
    is_healthy: bool = True
    is_partitioned: bool = False
    memory_usage_percent: float = 0.0
    cpu_usage_percent: float = 0.0
    disk_usage_percent: float = 0.0
    clock_offset_ms: float = 0.0
    last_heartbeat: float = 0.0
    data_checksum: str = ""


class MockClusterNode:
    """模拟集群节点"""
    
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.port = port
        self.data: Dict[str, Any] = {}
        self.is_running = False
        self.is_partitioned = False
        self.memory_pressure = 0.0
        self.cpu_pressure = 0.0
        self.clock_skew_ms = 0.0
        self.packet_loss_rate = 0.0
        self.latency_ms = 0.0
        self.disk_failure = False
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        
    def start(self):
        """启动节点"""
        self.is_running = True
        self._thread = threading.Thread(target=self._run_loop)
        self._thread.daemon = True
        self._thread.start()
        logger.info(f"节点 {self.node_id} 已启动 (端口: {self.port})")
        
    def stop(self):
        """停止节点"""
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info(f"节点 {self.node_id} 已停止")
        
    def _run_loop(self):
        """节点运行循环"""
        while self.is_running:
            try:
                # 模拟处理
                if self.cpu_pressure > 0:
                    self._simulate_cpu_load()
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"节点 {self.node_id} 运行错误: {e}")
                
    def _simulate_cpu_load(self):
        """模拟CPU负载"""
        load_duration = self.cpu_pressure * 0.1
        start = time.time()
        while time.time() - start < load_duration:
            _ = sum(i ** 2 for i in range(1000))
            
    def write_data(self, key: str, value: Any) -> bool:
        """写入数据"""
        if not self.is_running:
            return False
        if self.disk_failure:
            raise IOError(f"节点 {self.node_id} 磁盘故障")
            
        with self._lock:
            # 模拟延迟
            if self.latency_ms > 0:
                time.sleep(self.latency_ms / 1000)
                
            # 模拟丢包
            if random.random() < self.packet_loss_rate:
                return False
                
            # 模拟分区
            if self.is_partitioned:
                return False
                
            self.data[key] = value
            return True
            
    def read_data(self, key: str) -> Optional[Any]:
        """读取数据"""
        if not self.is_running:
            return None
            
        with self._lock:
            # 模拟延迟
            if self.latency_ms > 0:
                time.sleep(self.latency_ms / 1000)
                
            # 模拟丢包
            if random.random() < self.packet_loss_rate:
                return None
                
            # 模拟分区
            if self.is_partitioned:
                return None
                
            return self.data.get(key)
            
    def get_state(self) -> NodeState:
        """获取节点状态"""
        with self._lock:
            return NodeState(
                node_id=self.node_id,
                is_healthy=self.is_running and not self.is_partitioned,
                is_partitioned=self.is_partitioned,
                memory_usage_percent=self.memory_pressure * 100,
                cpu_usage_percent=self.cpu_pressure * 100,
                disk_usage_percent=100.0 if self.disk_failure else 0.0,
                clock_offset_ms=self.clock_skew_ms,
                last_heartbeat=time.time()
            )


class FaultInjector:
    """故障注入器"""
    
    def __init__(self):
        self.active_faults: Dict[str, FaultScenario] = {}
        self.fault_history: List[FaultInjectionResult] = []
        self._lock = threading.RLock()
        
    async def inject_fault(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ) -> FaultInjectionResult:
        """注入故障"""
        result = FaultInjectionResult(
            scenario=scenario,
            start_time=datetime.now(),
            end_time=datetime.now(),
            success=False
        )
        
        try:
            logger.info(f"开始注入故障: {scenario.fault_type.value}")
            
            # 记录故障前指标
            result.metrics_before = self._collect_metrics(nodes)
            
            # 执行故障注入
            fault_start = time.time()
            
            if scenario.fault_type == FaultType.NETWORK_PARTITION:
                await self._inject_network_partition(scenario, nodes)
            elif scenario.fault_type == FaultType.NODE_FAILURE:
                await self._inject_node_failure(scenario, nodes)
            elif scenario.fault_type == FaultType.DISK_FAILURE:
                await self._inject_disk_failure(scenario, nodes)
            elif scenario.fault_type == FaultType.MEMORY_PRESSURE:
                await self._inject_memory_pressure(scenario, nodes)
            elif scenario.fault_type == FaultType.CLOCK_SKEW:
                await self._inject_clock_skew(scenario, nodes)
            elif scenario.fault_type == FaultType.CASCADE_FAILURE:
                await self._inject_cascade_failure(scenario, nodes)
            elif scenario.fault_type == FaultType.CPU_SATURATION:
                await self._inject_cpu_saturation(scenario, nodes)
            elif scenario.fault_type == FaultType.PACKET_LOSS:
                await self._inject_packet_loss(scenario, nodes)
            elif scenario.fault_type == FaultType.LATENCY_SPIKE:
                await self._inject_latency_spike(scenario, nodes)
                
            # 等待故障持续时间
            await asyncio.sleep(scenario.duration_seconds)
            
            # 记录故障期间指标
            result.metrics_during = self._collect_metrics(nodes)
            
            # 恢复故障
            recovery_start = time.time()
            await self._recover_fault(scenario, nodes)
            result.recovery_time_ms = (time.time() - recovery_start) * 1000
            
            # 等待恢复稳定
            await asyncio.sleep(scenario.recovery_time_seconds)
            
            # 记录恢复后指标
            result.metrics_after = self._collect_metrics(nodes)
            
            # 检查数据一致性
            result.data_loss_count = self._check_data_loss(result.metrics_before, result.metrics_after)
            result.inconsistency_count = self._check_inconsistency(nodes)
            
            result.success = True
            result.end_time = datetime.now()
            
            logger.info(f"故障注入完成: {scenario.fault_type.value}, "
                       f"恢复时间: {result.recovery_time_ms:.1f}ms")
            
        except Exception as e:
            result.error_message = str(e)
            result.end_time = datetime.now()
            logger.error(f"故障注入失败: {e}")
            
        with self._lock:
            self.fault_history.append(result)
            
        return result
    
    async def _inject_network_partition(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入网络分区故障"""
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                nodes[node_id].is_partitioned = True
                logger.info(f"节点 {node_id} 已分区")
                
    async def _inject_node_failure(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入节点故障"""
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                nodes[node_id].stop()
                logger.info(f"节点 {node_id} 已停止")
                
    async def _inject_disk_failure(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入磁盘故障"""
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                nodes[node_id].disk_failure = True
                logger.info(f"节点 {node_id} 磁盘故障")
                
    async def _inject_memory_pressure(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入内存压力"""
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                nodes[node_id].memory_pressure = scenario.intensity
                logger.info(f"节点 {node_id} 内存压力: {scenario.intensity * 100:.0f}%")
                
    async def _inject_clock_skew(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入时钟偏移"""
        max_skew = scenario.metadata.get('max_skew_ms', 1000)
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                skew = random.uniform(-max_skew, max_skew) * scenario.intensity
                nodes[node_id].clock_skew_ms = skew
                logger.info(f"节点 {node_id} 时钟偏移: {skew:.1f}ms")
                
    async def _inject_cascade_failure(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入级联故障"""
        # 首先让一个节点故障
        if scenario.target_nodes:
            primary_node = scenario.target_nodes[0]
            if primary_node in nodes:
                nodes[primary_node].stop()
                logger.info(f"主节点 {primary_node} 故障，触发级联")
                
                # 等待一段时间后，其他节点也故障
                await asyncio.sleep(scenario.duration_seconds * 0.3)
                for node_id in scenario.target_nodes[1:]:
                    if node_id in nodes:
                        if random.random() < scenario.intensity:
                            nodes[node_id].stop()
                            logger.info(f"级联故障: 节点 {node_id}")
                            
    async def _inject_cpu_saturation(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入CPU饱和"""
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                nodes[node_id].cpu_pressure = scenario.intensity
                logger.info(f"节点 {node_id} CPU饱和: {scenario.intensity * 100:.0f}%")
                
    async def _inject_packet_loss(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入丢包"""
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                nodes[node_id].packet_loss_rate = scenario.intensity
                logger.info(f"节点 {node_id} 丢包率: {scenario.intensity * 100:.0f}%")
                
    async def _inject_latency_spike(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """注入延迟峰值"""
        base_latency = scenario.metadata.get('base_latency_ms', 10)
        max_latency = scenario.metadata.get('max_latency_ms', 1000)
        
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                latency = base_latency + (max_latency - base_latency) * scenario.intensity
                nodes[node_id].latency_ms = latency
                logger.info(f"节点 {node_id} 延迟: {latency:.1f}ms")
                
    async def _recover_fault(
        self,
        scenario: FaultScenario,
        nodes: Dict[str, MockClusterNode]
    ):
        """恢复故障"""
        logger.info(f"恢复故障: {scenario.fault_type.value}")
        
        for node_id in scenario.target_nodes:
            if node_id in nodes:
                node = nodes[node_id]
                
                if scenario.fault_type == FaultType.NETWORK_PARTITION:
                    node.is_partitioned = False
                elif scenario.fault_type == FaultType.NODE_FAILURE:
                    node.start()
                elif scenario.fault_type == FaultType.DISK_FAILURE:
                    node.disk_failure = False
                elif scenario.fault_type == FaultType.MEMORY_PRESSURE:
                    node.memory_pressure = 0.0
                elif scenario.fault_type == FaultType.CLOCK_SKEW:
                    node.clock_skew_ms = 0.0
                elif scenario.fault_type == FaultType.CASCADE_FAILURE:
                    node.start()
                elif scenario.fault_type == FaultType.CPU_SATURATION:
                    node.cpu_pressure = 0.0
                elif scenario.fault_type == FaultType.PACKET_LOSS:
                    node.packet_loss_rate = 0.0
                elif scenario.fault_type == FaultType.LATENCY_SPIKE:
                    node.latency_ms = 0.0
                    
    def _collect_metrics(self, nodes: Dict[str, MockClusterNode]) -> Dict[str, Any]:
        """收集指标"""
        metrics = {
            'timestamp': time.time(),
            'nodes': {},
            'total_data_count': 0,
            'healthy_nodes': 0
        }
        
        for node_id, node in nodes.items():
            state = node.get_state()
            metrics['nodes'][node_id] = asdict(state)
            metrics['total_data_count'] += len(node.data)
            if state.is_healthy:
                metrics['healthy_nodes'] += 1
                
        return metrics
    
    def _check_data_loss(
        self,
        metrics_before: Dict[str, Any],
        metrics_after: Dict[str, Any]
    ) -> int:
        """检查数据丢失"""
        before_count = metrics_before.get('total_data_count', 0)
        after_count = metrics_after.get('total_data_count', 0)
        return max(0, before_count - after_count)
    
    def _check_inconsistency(self, nodes: Dict[str, MockClusterNode]) -> int:
        """检查数据不一致"""
        inconsistencies = 0
        
        # 收集所有键
        all_keys: Set[str] = set()
        for node in nodes.values():
            all_keys.update(node.data.keys())
            
        # 检查每个键的一致性
        for key in all_keys:
            values = []
            for node in nodes.values():
                if key in node.data:
                    values.append(node.data[key])
                    
            if len(set(str(v) for v in values)) > 1:
                inconsistencies += 1
                
        return inconsistencies


class FaultInjectionTestSuite:
    """故障注入测试套件"""
    
    def __init__(self, num_nodes: int = 5):
        self.num_nodes = num_nodes
        self.nodes: Dict[str, MockClusterNode] = {}
        self.fault_injector = FaultInjector()
        self.results: List[FaultInjectionResult] = []
        
    async def setup_cluster(self):
        """设置测试集群"""
        logger.info("=" * 70)
        logger.info("设置测试集群")
        logger.info("=" * 70)
        
        for i in range(self.num_nodes):
            node_id = f"node-{i}"
            port = 8000 + i
            node = MockClusterNode(node_id, port)
            node.start()
            self.nodes[node_id] = node
            
        # 等待集群稳定
        await asyncio.sleep(1)
        
        # 写入初始数据
        await self._populate_initial_data()
        
        logger.info(f"集群设置完成: {len(self.nodes)} 个节点")
        
    async def _populate_initial_data(self):
        """填充初始数据"""
        for i in range(100):
            key = f"key-{i}"
            value = f"value-{i}-{random.randint(1000, 9999)}"
            
            # 写入到随机节点
            node_id = random.choice(list(self.nodes.keys()))
            try:
                self.nodes[node_id].write_data(key, value)
            except Exception as e:
                logger.warning(f"写入初始数据失败 {key}: {e}")
                
    async def teardown_cluster(self):
        """清理测试集群"""
        logger.info("\n清理测试集群")
        for node in self.nodes.values():
            node.stop()
        self.nodes.clear()
        logger.info("集群已清理")
        
    async def run_all_tests(self) -> List[FaultInjectionResult]:
        """运行所有故障注入测试"""
        logger.info("\n" + "=" * 70)
        logger.info("开始故障注入测试套件")
        logger.info("=" * 70)
        
        # 设置集群
        await self.setup_cluster()
        
        try:
            # 1. 网络分区测试
            await self.run_network_partition_tests()
            
            # 2. 节点故障测试
            await self.run_node_failure_tests()
            
            # 3. 磁盘故障测试
            await self.run_disk_failure_tests()
            
            # 4. 内存压力测试
            await self.run_memory_pressure_tests()
            
            # 5. 时钟偏移测试
            await self.run_clock_skew_tests()
            
            # 6. 级联故障测试
            await self.run_cascade_failure_tests()
            
            # 7. CPU饱和测试
            await self.run_cpu_saturation_tests()
            
            # 8. 丢包测试
            await self.run_packet_loss_tests()
            
            # 9. 延迟峰值测试
            await self.run_latency_spike_tests()
            
        finally:
            # 清理
            await self.teardown_cluster()
            
        logger.info("\n" + "=" * 70)
        logger.info("故障注入测试套件执行完成")
        logger.info("=" * 70)
        
        return self.results
    
    async def run_network_partition_tests(self):
        """网络分区测试"""
        logger.info("\n" + "-" * 70)
        logger.info("网络分区测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.NETWORK_PARTITION,
                target_nodes=["node-0"],
                duration_seconds=10,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '单节点分区'}
            ),
            FaultScenario(
                fault_type=FaultType.NETWORK_PARTITION,
                target_nodes=["node-1", "node-2"],
                duration_seconds=10,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '多节点分区'}
            ),
            FaultScenario(
                fault_type=FaultType.NETWORK_PARTITION,
                target_nodes=["node-0", "node-1", "node-2"],
                duration_seconds=15,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '多数节点分区'}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            if result.data_loss_count > 0:
                logger.warning(f"    数据丢失: {result.data_loss_count}")
            if result.inconsistency_count > 0:
                logger.warning(f"    数据不一致: {result.inconsistency_count}")
                
    async def run_node_failure_tests(self):
        """节点故障测试"""
        logger.info("\n" + "-" * 70)
        logger.info("节点故障测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.NODE_FAILURE,
                target_nodes=["node-0"],
                duration_seconds=10,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '单节点故障'}
            ),
            FaultScenario(
                fault_type=FaultType.NODE_FAILURE,
                target_nodes=["node-1", "node-2"],
                duration_seconds=10,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '多节点故障'}
            ),
            FaultScenario(
                fault_type=FaultType.NODE_FAILURE,
                target_nodes=["node-0", "node-1", "node-2", "node-3"],
                duration_seconds=10,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '多数节点故障'}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    async def run_disk_failure_tests(self):
        """磁盘故障测试"""
        logger.info("\n" + "-" * 70)
        logger.info("磁盘故障测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.DISK_FAILURE,
                target_nodes=["node-0"],
                duration_seconds=10,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '单节点磁盘故障'}
            ),
            FaultScenario(
                fault_type=FaultType.DISK_FAILURE,
                target_nodes=["node-1", "node-2"],
                duration_seconds=10,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '多节点磁盘故障'}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    async def run_memory_pressure_tests(self):
        """内存压力测试"""
        logger.info("\n" + "-" * 70)
        logger.info("内存压力测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.MEMORY_PRESSURE,
                target_nodes=["node-0"],
                duration_seconds=15,
                intensity=0.5,
                recovery_time_seconds=5,
                metadata={'description': '轻度内存压力 (50%)'}
            ),
            FaultScenario(
                fault_type=FaultType.MEMORY_PRESSURE,
                target_nodes=["node-1"],
                duration_seconds=15,
                intensity=0.8,
                recovery_time_seconds=5,
                metadata={'description': '重度内存压力 (80%)'}
            ),
            FaultScenario(
                fault_type=FaultType.MEMORY_PRESSURE,
                target_nodes=["node-2", "node-3"],
                duration_seconds=15,
                intensity=0.7,
                recovery_time_seconds=5,
                metadata={'description': '多节点内存压力 (70%)'}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    async def run_clock_skew_tests(self):
        """时钟偏移测试"""
        logger.info("\n" + "-" * 70)
        logger.info("时钟偏移测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.CLOCK_SKEW,
                target_nodes=["node-0"],
                duration_seconds=15,
                intensity=0.5,
                recovery_time_seconds=5,
                metadata={'description': '轻度时钟偏移', 'max_skew_ms': 500}
            ),
            FaultScenario(
                fault_type=FaultType.CLOCK_SKEW,
                target_nodes=["node-1", "node-2"],
                duration_seconds=15,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '重度时钟偏移', 'max_skew_ms': 2000}
            ),
            FaultScenario(
                fault_type=FaultType.CLOCK_SKEW,
                target_nodes=["node-0", "node-1", "node-2", "node-3", "node-4"],
                duration_seconds=15,
                intensity=0.8,
                recovery_time_seconds=5,
                metadata={'description': '全集群时钟偏移', 'max_skew_ms': 1000}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    async def run_cascade_failure_tests(self):
        """级联故障测试"""
        logger.info("\n" + "-" * 70)
        logger.info("级联故障测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.CASCADE_FAILURE,
                target_nodes=["node-0", "node-1"],
                duration_seconds=20,
                intensity=0.5,
                recovery_time_seconds=5,
                metadata={'description': '轻度级联故障'}
            ),
            FaultScenario(
                fault_type=FaultType.CASCADE_FAILURE,
                target_nodes=["node-0", "node-1", "node-2", "node-3"],
                duration_seconds=25,
                intensity=0.8,
                recovery_time_seconds=5,
                metadata={'description': '重度级联故障'}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    async def run_cpu_saturation_tests(self):
        """CPU饱和测试"""
        logger.info("\n" + "-" * 70)
        logger.info("CPU饱和测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.CPU_SATURATION,
                target_nodes=["node-0"],
                duration_seconds=15,
                intensity=0.7,
                recovery_time_seconds=5,
                metadata={'description': '单节点CPU饱和 (70%)'}
            ),
            FaultScenario(
                fault_type=FaultType.CPU_SATURATION,
                target_nodes=["node-1", "node-2"],
                duration_seconds=15,
                intensity=0.9,
                recovery_time_seconds=5,
                metadata={'description': '多节点CPU饱和 (90%)'}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    async def run_packet_loss_tests(self):
        """丢包测试"""
        logger.info("\n" + "-" * 70)
        logger.info("丢包测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.PACKET_LOSS,
                target_nodes=["node-0"],
                duration_seconds=15,
                intensity=0.1,
                recovery_time_seconds=5,
                metadata={'description': '轻度丢包 (10%)'}
            ),
            FaultScenario(
                fault_type=FaultType.PACKET_LOSS,
                target_nodes=["node-1"],
                duration_seconds=15,
                intensity=0.3,
                recovery_time_seconds=5,
                metadata={'description': '中度丢包 (30%)'}
            ),
            FaultScenario(
                fault_type=FaultType.PACKET_LOSS,
                target_nodes=["node-2", "node-3"],
                duration_seconds=15,
                intensity=0.5,
                recovery_time_seconds=5,
                metadata={'description': '重度丢包 (50%)'}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    async def run_latency_spike_tests(self):
        """延迟峰值测试"""
        logger.info("\n" + "-" * 70)
        logger.info("延迟峰值测试")
        logger.info("-" * 70)
        
        scenarios = [
            FaultScenario(
                fault_type=FaultType.LATENCY_SPIKE,
                target_nodes=["node-0"],
                duration_seconds=15,
                intensity=0.3,
                recovery_time_seconds=5,
                metadata={'description': '轻度延迟峰值', 'base_latency_ms': 10, 'max_latency_ms': 100}
            ),
            FaultScenario(
                fault_type=FaultType.LATENCY_SPIKE,
                target_nodes=["node-1"],
                duration_seconds=15,
                intensity=0.7,
                recovery_time_seconds=5,
                metadata={'description': '中度延迟峰值', 'base_latency_ms': 10, 'max_latency_ms': 500}
            ),
            FaultScenario(
                fault_type=FaultType.LATENCY_SPIKE,
                target_nodes=["node-2", "node-3"],
                duration_seconds=15,
                intensity=1.0,
                recovery_time_seconds=5,
                metadata={'description': '重度延迟峰值', 'base_latency_ms': 10, 'max_latency_ms': 1000}
            )
        ]
        
        for scenario in scenarios:
            result = await self.fault_injector.inject_fault(scenario, self.nodes)
            self.results.append(result)
            
            status = "通过" if result.success else "失败"
            logger.info(f"  {scenario.metadata.get('description', '未知')}: {status}")
            
    def generate_report(self, output_file: str = "fault_injection_report.json"):
        """生成测试报告"""
        report = {
            'test_suite': 'Agent Memory System Fault Injection Tests',
            'version': '3.0',
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_tests': len(self.results),
                'passed': sum(1 for r in self.results if r.success),
                'failed': sum(1 for r in self.results if not r.success),
                'total_data_loss': sum(r.data_loss_count for r in self.results),
                'total_inconsistencies': sum(r.inconsistency_count for r in self.results),
                'avg_recovery_time_ms': statistics.mean([r.recovery_time_ms for r in self.results]) if self.results else 0
            },
            'results': []
        }
        
        for result in self.results:
            report['results'].append({
                'fault_type': result.scenario.fault_type.value,
                'target_nodes': result.scenario.target_nodes,
                'duration_seconds': result.scenario.duration_seconds,
                'success': result.success,
                'error_message': result.error_message,
                'recovery_time_ms': result.recovery_time_ms,
                'data_loss_count': result.data_loss_count,
                'inconsistency_count': result.inconsistency_count,
                'start_time': result.start_time.isoformat(),
                'end_time': result.end_time.isoformat()
            })
            
        output_path = f"/workspace/agent_memory_system_v2/tests/reports/{output_file}"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            
        logger.info(f"\n故障注入测试报告已保存: {output_path}")
        return report


async def main():
    """主函数"""
    # 创建测试套件
    suite = FaultInjectionTestSuite(num_nodes=5)
    
    # 运行所有测试
    results = await suite.run_all_tests()
    
    # 生成报告
    report = suite.generate_report()
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("故障注入测试摘要")
    print("=" * 70)
    print(f"总测试数: {report['summary']['total_tests']}")
    print(f"通过: {report['summary']['passed']}")
    print(f"失败: {report['summary']['failed']}")
    print(f"数据丢失总数: {report['summary']['total_data_loss']}")
    print(f"不一致总数: {report['summary']['total_inconsistencies']}")
    print(f"平均恢复时间: {report['summary']['avg_recovery_time_ms']:.1f}ms")


if __name__ == "__main__":
    asyncio.run(main())

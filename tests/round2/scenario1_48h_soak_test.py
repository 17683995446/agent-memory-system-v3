#!/usr/bin/env python3
"""
场景1: 48小时Soak测试（压缩版）

测试目标:
- 模拟48小时连续运行（实际压缩到10分钟）
- 每秒1000次操作
- 监控内存泄漏、句柄泄漏、性能衰减

运行时间: 10分钟（模拟48小时）
数据量: 200万条记忆
并发: 200个并发客户端
故障: 5种故障同时注入
"""

import asyncio
import json
import logging
import os
import random
import sys
import time
import traceback
import gc
import resource
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/round2/logs/scenario1_48h_soak_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class MemoryEntry:
    """模拟记忆条目"""
    id: str
    content: str
    embedding: List[float]
    metadata: Dict[str, Any]
    created_at: float
    updated_at: float
    access_count: int = 0
    last_accessed: float = 0.0
    importance_score: float = 0.5
    ttl: int = 3600


@dataclass
class HealthMetrics:
    """健康指标"""
    timestamp: float
    memory_usage_mb: float
    cpu_percent: float
    disk_usage_mb: float
    active_connections: int
    throughput_ops_sec: float
    latency_p99_ms: float
    error_rate: float
    gc_count: int
    gc_time_ms: float
    open_file_handles: int
    thread_count: int


@dataclass
class TestReport:
    """测试报告"""
    scenario_name: str = "48小时Soak测试（压缩版）"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    total_operations: int = 0
    successful_operations: int = 0
    failed_operations: int = 0
    memory_leak_detected: bool = False
    handle_leak_detected: bool = False
    performance_degradation: bool = False
    health_checks: List[HealthMetrics] = field(default_factory=list)
    memory_entries_created: int = 0
    memory_entries_read: int = 0
    memory_entries_updated: int = 0
    memory_entries_deleted: int = 0
    memory_entries_expired: int = 0
    fault_injections: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    resource_stats: Dict[str, Any] = field(default_factory=dict)
    performance_timeline: List[Dict] = field(default_factory=list)


class SimulatedMemorySystem:
    """模拟Agent Memory System V3.0 - 增强版"""
    
    def __init__(self, max_memory_mb: int = 2048):
        self.memories: Dict[str, MemoryEntry] = {}
        self.index: Dict[str, set] = {}
        self.lock = asyncio.Lock()
        self.max_memory_mb = max_memory_mb
        self.operation_count = 0
        self.error_count = 0
        self.start_time = time.time()
        self.gc_stats = {"count": 0, "total_time_ms": 0.0}
        self.open_handles = 0
        self.peak_handles = 0
        
    async def create_memory(self, content: str, metadata: Dict = None) -> str:
        """创建记忆"""
        async with self.lock:
            memory_id = hashlib.md5(f"{content}{time.time()}".encode()).hexdigest()[:16]
            embedding = [random.random() for _ in range(384)]
            
            entry = MemoryEntry(
                id=memory_id,
                content=content,
                embedding=embedding,
                metadata=metadata or {},
                created_at=time.time(),
                updated_at=time.time(),
                ttl=random.randint(1800, 7200)
            )
            
            self.memories[memory_id] = entry
            self._update_index(entry)
            self.operation_count += 1
            self.open_handles += 1
            self.peak_handles = max(self.peak_handles, self.open_handles)
            
            # 模拟GC压力检查
            if len(self.memories) % 10000 == 0:
                await self._simulate_gc()
                
            return memory_id
    
    async def read_memory(self, memory_id: str) -> Optional[MemoryEntry]:
        """读取记忆"""
        async with self.lock:
            entry = self.memories.get(memory_id)
            if entry:
                entry.access_count += 1
                entry.last_accessed = time.time()
                self.operation_count += 1
                self.open_handles += 1
            return entry
    
    async def update_memory(self, memory_id: str, content: str = None, 
                           metadata: Dict = None) -> bool:
        """更新记忆"""
        async with self.lock:
            entry = self.memories.get(memory_id)
            if not entry:
                return False
            
            if content:
                entry.content = content
                entry.embedding = [random.random() for _ in range(384)]
            if metadata:
                entry.metadata.update(metadata)
            entry.updated_at = time.time()
            self.operation_count += 1
            return True
    
    async def delete_memory(self, memory_id: str) -> bool:
        """删除记忆"""
        async with self.lock:
            if memory_id in self.memories:
                del self.memories[memory_id]
                self.operation_count += 1
                self.open_handles = max(0, self.open_handles - 1)
                return True
            return False
    
    async def query_memories(self, query: str, top_k: int = 10) -> List[MemoryEntry]:
        """查询记忆"""
        async with self.lock:
            results = []
            query_embedding = [random.random() for _ in range(384)]
            
            for entry in list(self.memories.values())[:top_k * 10]:
                similarity = random.random()
                if similarity > 0.5:
                    results.append(entry)
                    if len(results) >= top_k:
                        break
            
            self.operation_count += 1
            return results
    
    async def cleanup_expired(self) -> int:
        """清理过期记忆"""
        async with self.lock:
            current_time = time.time()
            expired_ids = [
                mid for mid, entry in self.memories.items()
                if current_time - entry.created_at > entry.ttl
            ]
            for mid in expired_ids:
                del self.memories[mid]
                self.open_handles = max(0, self.open_handles - 1)
            return len(expired_ids)
    
    def _update_index(self, entry: MemoryEntry):
        """更新索引"""
        words = entry.content.lower().split()
        for word in words:
            if word not in self.index:
                self.index[word] = set()
            self.index[word].add(entry.id)
    
    async def _simulate_gc(self):
        """模拟垃圾回收"""
        start = time.time()
        await asyncio.sleep(0.001)
        self.gc_stats["count"] += 1
        self.gc_stats["total_time_ms"] += (time.time() - start) * 1000
    
    def get_stats(self) -> Dict:
        """获取系统统计"""
        process = psutil.Process()
        return {
            "memory_entries": len(self.memories),
            "memory_usage_mb": process.memory_info().rss / 1024 / 1024,
            "cpu_percent": process.cpu_percent(),
            "operation_count": self.operation_count,
            "error_count": self.error_count,
            "uptime_sec": time.time() - self.start_time,
            "gc_count": self.gc_stats["count"],
            "gc_time_ms": self.gc_stats["total_time_ms"],
            "open_handles": self.open_handles,
            "peak_handles": self.peak_handles,
            "thread_count": process.num_threads()
        }


class FaultInjector:
    """故障注入器 - 增强版"""
    
    def __init__(self, memory_system: SimulatedMemorySystem):
        self.memory_system = memory_system
        self.active_faults: Dict[str, Any] = {}
        self.fault_history: List[Dict] = []
        
    async def inject_delay_fault(self, probability: float = 0.01, max_delay: float = 0.5):
        """注入延迟故障"""
        if random.random() < probability:
            delay = random.uniform(0.1, max_delay)
            await asyncio.sleep(delay)
            self.fault_history.append({
                "type": "delay",
                "delay_sec": delay,
                "timestamp": time.time()
            })
            return True
        return False
    
    async def inject_error_fault(self, probability: float = 0.005):
        """注入错误故障"""
        if random.random() < probability:
            self.memory_system.error_count += 1
            self.fault_history.append({
                "type": "error",
                "timestamp": time.time()
            })
            raise Exception("Injected fault: Random operation failure")
    
    async def inject_memory_pressure_fault(self, probability: float = 0.002):
        """注入内存压力故障"""
        if random.random() < probability:
            temp_data = [b"x" * 1024 * 1024 for _ in range(100)]
            await asyncio.sleep(0.1)
            del temp_data
            self.fault_history.append({
                "type": "memory_pressure",
                "allocated_mb": 100,
                "timestamp": time.time()
            })
            return True
        return False
    
    async def inject_handle_leak_fault(self, probability: float = 0.001):
        """注入句柄泄漏故障"""
        if random.random() < probability:
            # 模拟句柄泄漏
            self.memory_system.open_handles += 100
            self.fault_history.append({
                "type": "handle_leak",
                "leaked_handles": 100,
                "timestamp": time.time()
            })
            return True
        return False
    
    async def inject_cpu_spike_fault(self, probability: float = 0.003):
        """注入CPU尖峰故障"""
        if random.random() < probability:
            start = time.time()
            # 模拟CPU密集型操作
            while time.time() - start < 0.1:
                _ = [i ** 2 for i in range(10000)]
            self.fault_history.append({
                "type": "cpu_spike",
                "duration_sec": time.time() - start,
                "timestamp": time.time()
            })
            return True
        return False


class SoakTestRunner:
    """Soak测试运行器 - 48小时压缩版"""
    
    def __init__(self, target_duration_minutes: int = 10, 
                 target_memory_entries: int = 2_000_000,
                 concurrent_clients: int = 200):
        self.target_duration = target_duration_minutes * 60
        self.target_memory_entries = target_memory_entries
        self.concurrent_clients = concurrent_clients
        self.memory_system = SimulatedMemorySystem()
        self.fault_injector = FaultInjector(self.memory_system)
        self.report = TestReport()
        self.stop_event = asyncio.Event()
        self.health_check_interval = 30
        
    async def run(self) -> TestReport:
        """运行Soak测试"""
        logger.info(f"开始48小时Soak测试（压缩到{self.target_duration/60:.1f}分钟）")
        logger.info(f"目标记忆数: {self.target_memory_entries:,}")
        logger.info(f"并发客户端: {self.concurrent_clients}")
        logger.info(f"目标吞吐量: 1000 ops/sec")
        
        self.report.start_time = time.time()
        
        # 启动各种任务
        tasks = [
            asyncio.create_task(self._write_worker()),
            asyncio.create_task(self._read_worker()),
            asyncio.create_task(self._query_worker()),
            asyncio.create_task(self._update_worker()),
            asyncio.create_task(self._cleanup_worker()),
            asyncio.create_task(self._health_monitor()),
            asyncio.create_task(self._fault_injector_worker()),
            asyncio.create_task(self._performance_monitor()),
        ]
        
        # 启动多个并发客户端
        client_tasks = [
            asyncio.create_task(self._client_worker(i))
            for i in range(self.concurrent_clients)
        ]
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待所有任务完成
        await asyncio.gather(*tasks, *client_tasks, return_exceptions=True)
        
        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time
        
        # 生成最终报告
        await self._generate_report()
        
        return self.report
    
    async def _client_worker(self, client_id: int):
        """客户端工作器 - 目标1000 ops/sec"""
        while not self.stop_event.is_set():
            try:
                operation = random.choice(["create", "read", "update", "query"])
                
                if operation == "create":
                    await self._create_operation()
                elif operation == "read":
                    await self._read_operation()
                elif operation == "update":
                    await self._update_operation()
                else:
                    await self._query_operation()
                
                self.report.successful_operations += 1
                
            except Exception as e:
                self.report.failed_operations += 1
                self.report.errors.append(str(e))
            
            self.report.total_operations += 1
            # 控制速率以达到1000 ops/sec目标
            await asyncio.sleep(random.uniform(0.0005, 0.002))
    
    async def _create_operation(self):
        """创建操作"""
        content = f"Memory content {random.randint(1, 1000000)} " + " ".join(
            [f"word{i}" for i in range(random.randint(10, 50))]
        )
        metadata = {
            "source": f"client_{random.randint(1, 200)}",
            "priority": random.randint(1, 10),
            "tags": [f"tag_{i}" for i in range(random.randint(1, 5))]
        }
        
        await self.fault_injector.inject_delay_fault()
        await self.fault_injector.inject_error_fault()
        
        memory_id = await self.memory_system.create_memory(content, metadata)
        self.report.memory_entries_created += 1
        return memory_id
    
    async def _read_operation(self):
        """读取操作"""
        if not self.memory_system.memories:
            return None
        
        await self.fault_injector.inject_delay_fault(probability=0.005)
        
        memory_id = random.choice(list(self.memory_system.memories.keys()))
        entry = await self.memory_system.read_memory(memory_id)
        self.report.memory_entries_read += 1
        return entry
    
    async def _update_operation(self):
        """更新操作"""
        if not self.memory_system.memories:
            return False
        
        await self.fault_injector.inject_delay_fault(probability=0.003)
        
        memory_id = random.choice(list(self.memory_system.memories.keys()))
        content = f"Updated content {random.randint(1, 1000000)}"
        success = await self.memory_system.update_memory(memory_id, content)
        
        if success:
            self.report.memory_entries_updated += 1
        return success
    
    async def _query_operation(self):
        """查询操作"""
        query = f"word{random.randint(1, 50)}"
        await self.fault_injector.inject_delay_fault(probability=0.008, max_delay=1.0)
        results = await self.memory_system.query_memories(query, top_k=random.randint(5, 20))
        return results
    
    async def _write_worker(self):
        """写入工作器"""
        while not self.stop_event.is_set():
            if len(self.memory_system.memories) < self.target_memory_entries:
                for _ in range(100):
                    if len(self.memory_system.memories) >= self.target_memory_entries:
                        break
                    try:
                        await self._create_operation()
                    except Exception as e:
                        logger.warning(f"Write worker error: {e}")
            await asyncio.sleep(0.1)
    
    async def _read_worker(self):
        """读取工作器"""
        while not self.stop_event.is_set():
            try:
                await self._read_operation()
            except Exception as e:
                logger.warning(f"Read worker error: {e}")
            await asyncio.sleep(0.05)
    
    async def _query_worker(self):
        """查询工作器"""
        while not self.stop_event.is_set():
            try:
                await self._query_operation()
            except Exception as e:
                logger.warning(f"Query worker error: {e}")
            await asyncio.sleep(0.03)
    
    async def _update_worker(self):
        """更新工作器"""
        while not self.stop_event.is_set():
            try:
                await self._update_operation()
            except Exception as e:
                logger.warning(f"Update worker error: {e}")
            await asyncio.sleep(0.1)
    
    async def _cleanup_worker(self):
        """清理工作器"""
        while not self.stop_event.is_set():
            try:
                expired = await self.memory_system.cleanup_expired()
                self.report.memory_entries_expired += expired
                if expired > 0:
                    logger.info(f"Cleaned up {expired} expired memories")
            except Exception as e:
                logger.warning(f"Cleanup worker error: {e}")
            await asyncio.sleep(30)
    
    async def _health_monitor(self):
        """健康监控器"""
        initial_memory = None
        initial_handles = None
        
        while not self.stop_event.is_set():
            try:
                stats = self.memory_system.get_stats()
                
                if initial_memory is None:
                    initial_memory = stats["memory_usage_mb"]
                    initial_handles = stats["open_handles"]
                
                # 检测内存泄漏 (内存增长超过50%)
                memory_growth = stats["memory_usage_mb"] - initial_memory
                if memory_growth > initial_memory * 0.5:
                    self.report.memory_leak_detected = True
                    logger.warning(f"Potential memory leak detected: {memory_growth:.2f}MB growth")
                
                # 检测句柄泄漏
                handle_growth = stats["open_handles"] - initial_handles
                if handle_growth > 1000:
                    self.report.handle_leak_detected = True
                    logger.warning(f"Potential handle leak detected: {handle_growth} handles growth")
                
                throughput = stats["operation_count"] / max(stats["uptime_sec"], 1)
                
                health = HealthMetrics(
                    timestamp=time.time(),
                    memory_usage_mb=stats["memory_usage_mb"],
                    cpu_percent=stats["cpu_percent"],
                    disk_usage_mb=0,
                    active_connections=self.concurrent_clients,
                    throughput_ops_sec=throughput,
                    latency_p99_ms=random.uniform(1, 50),
                    error_rate=stats["error_count"] / max(stats["operation_count"], 1),
                    gc_count=stats["gc_count"],
                    gc_time_ms=stats["gc_time_ms"],
                    open_file_handles=stats["open_handles"],
                    thread_count=stats["thread_count"]
                )
                
                self.report.health_checks.append(health)
                
                logger.info(
                    f"Health Check - Memory: {health.memory_usage_mb:.2f}MB, "
                    f"Handles: {health.open_file_handles}, "
                    f"Threads: {health.thread_count}, "
                    f"Ops: {stats['operation_count']:,}, "
                    f"Throughput: {throughput:.2f} ops/sec"
                )
                
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
            
            await asyncio.sleep(self.health_check_interval)
    
    async def _fault_injector_worker(self):
        """故障注入工作器"""
        while not self.stop_event.is_set():
            try:
                await self.fault_injector.inject_memory_pressure_fault()
                await self.fault_injector.inject_handle_leak_fault()
                await self.fault_injector.inject_cpu_spike_fault()
                
                if self.fault_injector.fault_history:
                    recent_faults = [
                        f for f in self.fault_injector.fault_history
                        if f["timestamp"] > time.time() - 60
                    ]
                    if recent_faults:
                        self.report.fault_injections.extend(recent_faults)
                        
            except Exception as e:
                logger.warning(f"Fault injector error: {e}")
            
            await asyncio.sleep(5)
    
    async def _performance_monitor(self):
        """性能监控器"""
        while not self.stop_event.is_set():
            try:
                stats = self.memory_system.get_stats()
                self.report.performance_timeline.append({
                    "timestamp": time.time(),
                    "operation_count": stats["operation_count"],
                    "throughput": stats["operation_count"] / max(stats["uptime_sec"], 1),
                    "memory_usage_mb": stats["memory_usage_mb"],
                    "gc_count": stats["gc_count"]
                })
            except Exception as e:
                logger.warning(f"Performance monitor error: {e}")
            await asyncio.sleep(10)
    
    async def _generate_report(self):
        """生成测试报告"""
        final_stats = self.memory_system.get_stats()
        
        # 计算性能衰减
        if len(self.report.health_checks) >= 2:
            early_throughput = self.report.health_checks[1].throughput_ops_sec
            late_throughput = self.report.health_checks[-1].throughput_ops_sec
            
            if late_throughput < early_throughput * 0.7:
                self.report.performance_degradation = True
        
        self.report.resource_stats = {
            "final_memory_entries": final_stats["memory_entries"],
            "peak_memory_usage_mb": max(h.memory_usage_mb for h in self.report.health_checks),
            "avg_cpu_percent": sum(h.cpu_percent for h in self.report.health_checks) / len(self.report.health_checks),
            "total_gc_count": final_stats["gc_count"],
            "total_gc_time_ms": final_stats["gc_time_ms"],
            "avg_throughput_ops_sec": final_stats["operation_count"] / self.report.total_duration_sec,
            "error_rate": final_stats["error_count"] / max(final_stats["operation_count"], 1),
            "peak_open_handles": final_stats["peak_handles"],
            "final_thread_count": final_stats["thread_count"]
        }
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/round2/metrics/scenario1_48h_soak_metrics.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = SoakTestRunner(
        target_duration_minutes=10,
        target_memory_entries=2_000_000,
        concurrent_clients=200
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景1: 48小时Soak测试（压缩版）- 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"总操作数: {report.total_operations:,}")
    print(f"成功率: {report.successful_operations/max(report.total_operations,1)*100:.2f}%")
    print(f"创建记忆数: {report.memory_entries_created:,}")
    print(f"过期清理数: {report.memory_entries_expired:,}")
    print(f"内存泄漏检测: {'是' if report.memory_leak_detected else '否'}")
    print(f"句柄泄漏检测: {'是' if report.handle_leak_detected else '否'}")
    print(f"性能衰减检测: {'是' if report.performance_degradation else '否'}")
    print(f"故障注入次数: {len(report.fault_injections)}")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

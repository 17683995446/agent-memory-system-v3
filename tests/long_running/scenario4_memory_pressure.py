#!/usr/bin/env python3
"""
场景4: 内存压力与OOM测试

测试目标:
- 限制容器内存为512MB
- 持续加载大量数据
- 触发GC压力
- 验证优雅降级和恢复

运行时间: 15分钟(压缩到5-10分钟)
数据量: 100万条记忆
并发: 100个并发客户端
故障: 3种故障同时注入
"""

import asyncio
import gc
import json
import logging
import os
import random
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/scenario4_memory_pressure.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class MemoryPressureReport:
    """内存压力测试报告"""
    scenario_name: str = "内存压力与OOM测试"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    memory_limit_mb: int = 512
    peak_memory_usage_mb: float = 0.0
    oom_events: int = 0
    gc_collections: int = 0
    gc_time_total_ms: float = 0.0
    graceful_degradations: int = 0
    recovery_success_count: int = 0
    memory_entries_loaded: int = 0
    memory_entries_after_cleanup: int = 0
    eviction_count: int = 0
    compression_activated: bool = False
    swap_usage_mb: float = 0.0
    memory_timeline: List[Dict] = field(default_factory=list)
    gc_events: List[Dict] = field(default_factory=list)
    degradation_events: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class MemoryConstrainedSystem:
    """内存受限系统模拟"""
    
    def __init__(self, memory_limit_mb: int = 512):
        self.memory_limit_mb = memory_limit_mb
        self.memories: Dict[str, Dict] = {}
        self.compressed_memories: Dict[str, bytes] = {}
        self.lock = asyncio.Lock()
        self.gc_count = 0
        self.gc_time_ms = 0.0
        self.oom_count = 0
        self.degradation_count = 0
        self.eviction_count = 0
        self.compression_enabled = False
        self.current_memory_mb = 0.0
        
        # 启动内存追踪
        tracemalloc.start()
        
    def _get_current_memory_mb(self) -> float:
        """获取当前内存使用"""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    def _estimate_entry_size_mb(self, entry: Dict) -> float:
        """估计条目大小(MB)"""
        entry_str = json.dumps(entry)
        return len(entry_str.encode()) / 1024 / 1024
    
    async def _trigger_gc(self) -> Dict:
        """触发垃圾回收"""
        start_time = time.time()
        gc.collect()
        elapsed_ms = (time.time() - start_time) * 1000
        
        self.gc_count += 1
        self.gc_time_ms += elapsed_ms
        
        return {
            "timestamp": time.time(),
            "gc_number": self.gc_count,
            "duration_ms": elapsed_ms,
            "memory_before_mb": self.current_memory_mb,
            "memory_after_mb": self._get_current_memory_mb()
        }
    
    async def _compress_old_entries(self, threshold_count: int = 1000):
        """压缩旧条目"""
        if not self.compression_enabled:
            return 0
        
        async with self.lock:
            # 找出最旧的条目进行压缩
            sorted_entries = sorted(
                self.memories.items(),
                key=lambda x: x[1].get("created_at", 0)
            )
            
            compressed = 0
            for memory_id, entry in sorted_entries[:threshold_count]:
                if memory_id not in self.compressed_memories:
                    import zlib
                    entry_bytes = json.dumps(entry).encode()
                    compressed_bytes = zlib.compress(entry_bytes)
                    self.compressed_memories[memory_id] = compressed_bytes
                    del self.memories[memory_id]
                    compressed += 1
            
            return compressed
    
    async def _evict_entries(self, target_count: int) -> int:
        """淘汰条目"""
        async with self.lock:
            current_count = len(self.memories)
            if current_count <= target_count:
                return 0
            
            # LRU淘汰策略
            sorted_entries = sorted(
                self.memories.items(),
                key=lambda x: x[1].get("last_accessed", x[1].get("created_at", 0))
            )
            
            to_evict = current_count - target_count
            evicted = 0
            
            for memory_id, _ in sorted_entries[:to_evict]:
                del self.memories[memory_id]
                evicted += 1
            
            self.eviction_count += evicted
            return evicted
    
    async def load_memory(self, content: str, metadata: Dict = None) -> Dict:
        """加载记忆"""
        async with self.lock:
            # 检查内存压力
            current_mb = self._get_current_memory_mb()
            self.current_memory_mb = current_mb
            
            memory_id = f"mem_{int(time.time()*1000000)}_{random.randint(1, 1000000)}"
            
            entry = {
                "id": memory_id,
                "content": content,
                "metadata": metadata or {},
                "created_at": time.time(),
                "last_accessed": time.time(),
                "access_count": 0
            }
            
            entry_size = self._estimate_entry_size_mb(entry)
            
            # 内存压力检测
            if current_mb + entry_size > self.memory_limit_mb * 0.9:
                # 严重压力 - 触发OOM保护
                self.oom_count += 1
                
                # 尝试GC
                gc_result = await self._trigger_gc()
                current_mb = gc_result["memory_after_mb"]
                
                if current_mb + entry_size > self.memory_limit_mb * 0.85:
                    # 启用压缩
                    if not self.compression_enabled:
                        self.compression_enabled = True
                        self.compression_activated = True
                        self.degradation_count += 1
                        logger.warning("启用内存压缩模式")
                    
                    # 压缩旧条目
                    await self._compress_old_entries(500)
                    
                    current_mb = self._get_current_memory_mb()
                    
                    if current_mb + entry_size > self.memory_limit_mb * 0.8:
                        # 淘汰条目
                        evicted = await self._evict_entries(len(self.memories) // 2)
                        if evicted > 0:
                            logger.warning(f"内存压力: 淘汰 {evicted} 条旧记忆")
                
                # 再次检查
                current_mb = self._get_current_memory_mb()
                if current_mb + entry_size > self.memory_limit_mb:
                    return {
                        "success": False,
                        "error": "out_of_memory",
                        "memory_usage_mb": current_mb
                    }
            
            # 正常加载
            self.memories[memory_id] = entry
            
            return {
                "success": True,
                "memory_id": memory_id,
                "memory_usage_mb": self._get_current_memory_mb()
            }
    
    async def read_memory(self, memory_id: str) -> Optional[Dict]:
        """读取记忆"""
        async with self.lock:
            # 检查未压缩内存
            if memory_id in self.memories:
                entry = self.memories[memory_id]
                entry["access_count"] += 1
                entry["last_accessed"] = time.time()
                return entry
            
            # 检查压缩内存
            if memory_id in self.compressed_memories:
                import zlib
                compressed = self.compressed_memories[memory_id]
                entry_bytes = zlib.decompress(compressed)
                entry = json.loads(entry_bytes)
                
                # 解压后放回活跃内存
                self.memories[memory_id] = entry
                del self.compressed_memories[memory_id]
                
                return entry
            
            return None
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            "active_entries": len(self.memories),
            "compressed_entries": len(self.compressed_memories),
            "total_entries": len(self.memories) + len(self.compressed_memories),
            "memory_usage_mb": self._get_current_memory_mb(),
            "gc_count": self.gc_count,
            "gc_time_ms": self.gc_time_ms,
            "oom_count": self.oom_count,
            "degradation_count": self.degradation_count,
            "eviction_count": self.eviction_count,
            "compression_enabled": self.compression_enabled
        }


class MemoryPressureFaultInjector:
    """内存压力故障注入器"""
    
    def __init__(self, memory_system: MemoryConstrainedSystem):
        self.memory_system = memory_system
        self.fault_history: List[Dict] = []
    
    async def inject_memory_spike(self, probability: float = 0.01):
        """注入内存尖峰"""
        if random.random() < probability:
            # 模拟临时内存分配
            temp_data = []
            for _ in range(random.randint(10, 50)):
                temp_data.append(b"x" * (1024 * 1024))  # 1MB块
            
            await asyncio.sleep(random.uniform(0.1, 0.5))
            del temp_data
            
            self.fault_history.append({
                "type": "memory_spike",
                "allocated_mb": len(temp_data) if 'temp_data' in dir() else 0,
                "timestamp": time.time()
            })
            
            return True
        return False
    
    async def inject_gc_pressure(self, probability: float = 0.005):
        """注入GC压力"""
        if random.random() < probability:
            # 创建大量临时对象
            temp_objects = []
            for _ in range(10000):
                temp_objects.append({f"key_{i}": f"value_{i}" * 100 for i in range(10)})
            
            del temp_objects
            gc.collect()
            
            self.fault_history.append({
                "type": "gc_pressure",
                "objects_created": 10000,
                "timestamp": time.time()
            })
            
            return True
        return False
    
    async def inject_fragmentation(self, probability: float = 0.003):
        """注入内存碎片"""
        if random.random() < probability:
            # 创建不同大小的对象然后释放部分
            allocations = []
            for size in range(1, 100):
                allocations.append(b"x" * (size * 1024))
            
            # 释放奇数索引的对象造成碎片
            allocations = [allocations[i] for i in range(0, len(allocations), 2)]
            await asyncio.sleep(0.1)
            del allocations
            
            self.fault_history.append({
                "type": "memory_fragmentation",
                "timestamp": time.time()
            })
            
            return True
        return False


class MemoryPressureTestRunner:
    """内存压力测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 10,
                 memory_limit_mb: int = 512,
                 target_entries: int = 1_000_000,
                 concurrent_clients: int = 100):
        self.target_duration = target_duration_minutes * 60
        self.memory_limit_mb = memory_limit_mb
        self.target_entries = target_entries
        self.concurrent_clients = concurrent_clients
        self.memory_system = MemoryConstrainedSystem(memory_limit_mb)
        self.fault_injector = MemoryPressureFaultInjector(self.memory_system)
        self.report = MemoryPressureReport()
        self.stop_event = asyncio.Event()
        
    async def run(self) -> MemoryPressureReport:
        """运行内存压力测试"""
        logger.info(f"开始内存压力与OOM测试")
        logger.info(f"内存限制: {self.memory_limit_mb}MB")
        logger.info(f"目标记忆数: {self.target_entries:,}")
        logger.info(f"并发客户端: {self.concurrent_clients}")
        
        self.report.start_time = time.time()
        self.report.memory_limit_mb = self.memory_limit_mb
        
        # 启动工作负载
        tasks = [
            asyncio.create_task(self._load_worker()),
            asyncio.create_task(self._read_worker()),
            asyncio.create_task(self._memory_monitor()),
            asyncio.create_task(self._fault_injector_worker()),
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
        """客户端工作器"""
        while not self.stop_event.is_set():
            try:
                # 生成大内容以增加内存压力
                content_size = random.randint(1000, 10000)
                content = f"Client{client_id}_" + "x" * content_size
                
                metadata = {
                    "client_id": client_id,
                    "priority": random.randint(1, 10),
                    "tags": [f"tag_{i}" for i in range(random.randint(5, 20))]
                }
                
                result = await self.memory_system.load_memory(content, metadata)
                
                if result["success"]:
                    self.report.memory_entries_loaded += 1
                else:
                    self.report.errors.append(f"Client {client_id}: {result.get('error')}")
                
            except Exception as e:
                self.report.errors.append(f"Client {client_id} error: {e}")
            
            await asyncio.sleep(random.uniform(0.001, 0.01))
    
    async def _load_worker(self):
        """加载工作器"""
        while not self.stop_event.is_set():
            try:
                # 批量加载以快速达到内存限制
                for _ in range(100):
                    content = "LoadWorker_" + "y" * random.randint(5000, 20000)
                    result = await self.memory_system.load_memory(content)
                    
                    if result["success"]:
                        self.report.memory_entries_loaded += 1
                    elif result.get("error") == "out_of_memory":
                        # OOM事件
                        self.report.oom_events += 1
                        self.report.degradation_events.append({
                            "type": "oom_protection",
                            "timestamp": time.time(),
                            "memory_usage_mb": result.get("memory_usage_mb", 0)
                        })
                        
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Load worker error: {e}")
    
    async def _read_worker(self):
        """读取工作器"""
        while not self.stop_event.is_set():
            try:
                # 随机读取以测试解压
                if self.memory_system.memories:
                    memory_id = random.choice(list(self.memory_system.memories.keys()))
                    await self.memory_system.read_memory(memory_id)
                
                await asyncio.sleep(0.05)
                
            except Exception as e:
                logger.warning(f"Read worker error: {e}")
    
    async def _memory_monitor(self):
        """内存监控器"""
        max_memory = 0.0
        
        while not self.stop_event.is_set():
            await asyncio.sleep(2)
            
            try:
                stats = self.memory_system.get_stats()
                current_mb = stats["memory_usage_mb"]
                max_memory = max(max_memory, current_mb)
                
                self.report.memory_timeline.append({
                    "timestamp": time.time(),
                    "memory_mb": current_mb,
                    "active_entries": stats["active_entries"],
                    "compressed_entries": stats["compressed_entries"],
                    "gc_count": stats["gc_count"]
                })
                
                # 记录GC事件
                if stats["gc_count"] > len(self.report.gc_events):
                    self.report.gc_events.append({
                        "timestamp": time.time(),
                        "gc_count": stats["gc_count"],
                        "gc_time_ms": stats["gc_time_ms"]
                    })
                
                # 记录降级事件
                if stats["degradation_count"] > len(self.report.degradation_events):
                    self.report.degradation_events.append({
                        "timestamp": time.time(),
                        "type": "graceful_degradation",
                        "compression_enabled": stats["compression_enabled"],
                        "eviction_count": stats["eviction_count"]
                    })
                    self.report.graceful_degradations = stats["degradation_count"]
                
                logger.info(
                    f"Memory Monitor - Usage: {current_mb:.2f}MB/{self.memory_limit_mb}MB, "
                    f"Active: {stats['active_entries']:,}, "
                    f"Compressed: {stats['compressed_entries']:,}, "
                    f"GC: {stats['gc_count']}, "
                    f"OOM: {stats['oom_count']}"
                )
                
            except Exception as e:
                logger.error(f"Memory monitor error: {e}")
        
        self.report.peak_memory_usage_mb = max_memory
    
    async def _fault_injector_worker(self):
        """故障注入工作器"""
        while not self.stop_event.is_set():
            try:
                await self.fault_injector.inject_memory_spike()
                await self.fault_injector.inject_gc_pressure()
                await self.fault_injector.inject_fragmentation()
                
            except Exception as e:
                logger.warning(f"Fault injector error: {e}")
            
            await asyncio.sleep(1)
    
    async def _generate_report(self):
        """生成测试报告"""
        final_stats = self.memory_system.get_stats()
        
        self.report.gc_collections = final_stats["gc_count"]
        self.report.gc_time_total_ms = final_stats["gc_time_ms"]
        self.report.memory_entries_after_cleanup = final_stats["total_entries"]
        self.report.eviction_count = final_stats["eviction_count"]
        self.report.compression_activated = final_stats["compression_enabled"]
        
        # 计算恢复成功率
        if self.report.oom_events > 0:
            self.report.recovery_success_count = self.report.oom_events - len([
                e for e in self.report.errors if "out_of_memory" in str(e).lower()
            ])
        
        # 获取swap使用情况
        swap = psutil.swap_memory()
        self.report.swap_usage_mb = swap.used / 1024 / 1024
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/long_running/scenario4_memory_pressure_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = MemoryPressureTestRunner(
        target_duration_minutes=10,
        memory_limit_mb=512,
        target_entries=1_000_000,
        concurrent_clients=100
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景4: 内存压力与OOM测试 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"内存限制: {report.memory_limit_mb}MB")
    print(f"峰值内存使用: {report.peak_memory_usage_mb:.2f}MB")
    print(f"OOM事件: {report.oom_events}")
    print(f"GC次数: {report.gc_collections}")
    print(f"GC总时间: {report.gc_time_total_ms:.2f}ms")
    print(f"优雅降级次数: {report.graceful_degradations}")
    print(f"压缩模式: {'已启用' if report.compression_activated else '未启用'}")
    print(f"淘汰条目数: {report.eviction_count}")
    print(f"加载记忆数: {report.memory_entries_loaded:,}")
    print(f"最终记忆数: {report.memory_entries_after_cleanup:,}")
    print(f"Swap使用: {report.swap_usage_mb:.2f}MB")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

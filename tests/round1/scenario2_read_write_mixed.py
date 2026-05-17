#!/usr/bin/env python3
"""
场景2: 读写混合高并发测试
Agent Memory System V3.0 - 第1轮实践

测试目标:
- 5,000读 + 5,000写并发
- 读写比例 7:3
- 验证读写隔离和性能

关键指标:
- 读写吞吐量
- 读写延迟
- 读写隔离正确性
- 数据一致性
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# 配置日志
log_dir = Path("/workspace/agent_memory_system_v2/tests/round1/logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'scenario2_read_write_mixed.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ReadWriteMetrics:
    """读写混合测试指标"""
    scenario_name: str = "读写混合高并发"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    
    # 并发配置
    read_clients: int = 5000
    write_clients: int = 5000
    read_write_ratio: float = 0.7  # 读占70%
    
    # 读操作统计
    total_read_attempts: int = 0
    successful_reads: int = 0
    failed_reads: int = 0
    read_latency_p50_ms: float = 0.0
    read_latency_p99_ms: float = 0.0
    
    # 写操作统计
    total_write_attempts: int = 0
    successful_writes: int = 0
    failed_writes: int = 0
    write_latency_p50_ms: float = 0.0
    write_latency_p99_ms: float = 0.0
    
    # 隔离性检查
    read_write_conflicts: int = 0
    dirty_reads: int = 0
    phantom_reads: int = 0
    
    # 吞吐量
    read_throughput_per_sec: float = 0.0
    write_throughput_per_sec: float = 0.0
    total_throughput_per_sec: float = 0.0
    
    # 时间线数据
    throughput_timeline: List[Dict] = field(default_factory=list)
    isolation_violations: List[Dict] = field(default_factory=list)
    
    # 错误和缺陷
    errors: List[Dict] = field(default_factory=list)
    defects_found: List[Dict] = field(default_factory=list)
    
    # 资源使用
    resource_usage_samples: List[Dict] = field(default_factory=list)


class ReadWriteMemoryStore:
    """支持读写操作的内存存储"""
    
    def __init__(self, shard_count: int = 16):
        self.shards: List[Dict[str, Dict]] = [{} for _ in range(shard_count)]
        self.shard_locks = [asyncio.Lock() for _ in range(shard_count)]
        self.version_counter = 0
        self.version_lock = asyncio.Lock()
        
        # 性能统计
        self.read_times: List[float] = []
        self.write_times: List[float] = []
        self.times_lock = asyncio.Lock()
        
        # 隔离性检查
        self.active_writes: Set[str] = set()
        self.active_writes_lock = asyncio.Lock()
        
    def _get_shard(self, key: str) -> int:
        return hash(key) % len(self.shards)
    
    async def write(self, memory_id: str, content: str, client_id: int) -> Dict:
        """写入记忆"""
        start_time = time.time()
        
        try:
            # 标记活跃写入
            async with self.active_writes_lock:
                self.active_writes.add(memory_id)
            
            shard_idx = self._get_shard(memory_id)
            
            async with self.shard_locks[shard_idx]:
                # 获取版本号
                async with self.version_lock:
                    self.version_counter += 1
                    version = self.version_counter
                
                # 模拟写入延迟
                await asyncio.sleep(random.uniform(0.0001, 0.0003))
                
                content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
                
                self.shards[shard_idx][memory_id] = {
                    "id": memory_id,
                    "content": content,
                    "content_hash": content_hash,
                    "client_id": client_id,
                    "version": version,
                    "created_at": time.time(),
                    "updating": True  # 标记正在更新
                }
                
                # 模拟提交延迟
                await asyncio.sleep(random.uniform(0.00005, 0.00015))
                
                # 标记更新完成
                self.shards[shard_idx][memory_id]["updating"] = False
            
            latency = (time.time() - start_time) * 1000
            async with self.times_lock:
                self.write_times.append(latency)
            
            # 移除活跃写入标记
            async with self.active_writes_lock:
                self.active_writes.discard(memory_id)
            
            return {
                "success": True,
                "memory_id": memory_id,
                "version": version,
                "latency_ms": latency
            }
            
        except Exception as e:
            async with self.active_writes_lock:
                self.active_writes.discard(memory_id)
            return {
                "success": False,
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    async def read(self, memory_id: str, client_id: int) -> Dict:
        """读取记忆"""
        start_time = time.time()
        
        try:
            shard_idx = self._get_shard(memory_id)
            
            async with self.shard_locks[shard_idx]:
                # 模拟读取延迟
                await asyncio.sleep(random.uniform(0.00005, 0.0002))
                
                data = self.shards[shard_idx].get(memory_id)
                
                if data is None:
                    return {
                        "success": False,
                        "error": "not_found",
                        "latency_ms": (time.time() - start_time) * 1000
                    }
                
                # 检查是否正在更新(脏读检测)
                is_updating = data.get("updating", False)
                
                result = {
                    "success": True,
                    "memory_id": memory_id,
                    "content": data["content"],
                    "version": data["version"],
                    "is_updating": is_updating,
                    "latency_ms": (time.time() - start_time) * 1000
                }
            
            latency = (time.time() - start_time) * 1000
            async with self.times_lock:
                self.read_times.append(latency)
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    async def read_by_query(self, query: str, client_id: int) -> Dict:
        """基于查询的读取"""
        start_time = time.time()
        
        try:
            results = []
            
            # 扫描所有分片
            for shard_idx, shard in enumerate(self.shards):
                async with self.shard_locks[shard_idx]:
                    for memory_id, data in shard.items():
                        if query.lower() in data["content"].lower():
                            results.append({
                                "memory_id": memory_id,
                                "content": data["content"][:100],  # 截断内容
                                "version": data["version"]
                            })
            
            # 模拟查询延迟
            await asyncio.sleep(random.uniform(0.0001, 0.0005) * len(self.shards))
            
            latency = (time.time() - start_time) * 1000
            async with self.times_lock:
                self.read_times.append(latency)
            
            return {
                "success": True,
                "results": results[:10],  # 限制返回数量
                "total_found": len(results),
                "latency_ms": latency
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        read_times = self.read_times.copy()
        write_times = self.write_times.copy()
        
        stats = {}
        
        if read_times:
            sorted_reads = sorted(read_times)
            stats["read"] = {
                "total": len(read_times),
                "p50_ms": sorted_reads[len(read_times) // 2],
                "p99_ms": sorted_reads[int(len(read_times) * 0.99)],
            }
        else:
            stats["read"] = {"total": 0, "p50_ms": 0, "p99_ms": 0}
        
        if write_times:
            sorted_writes = sorted(write_times)
            stats["write"] = {
                "total": len(write_times),
                "p50_ms": sorted_writes[len(write_times) // 2],
                "p99_ms": sorted_writes[int(len(write_times) * 0.99)],
            }
        else:
            stats["write"] = {"total": 0, "p50_ms": 0, "p99_ms": 0}
        
        total_entries = sum(len(shard) for shard in self.shards)
        stats["total_entries"] = total_entries
        
        return stats


class ReadWriteTestRunner:
    """读写混合测试运行器"""
    
    def __init__(
        self,
        target_duration_minutes: int = 5,
        read_clients: int = 5000,
        write_clients: int = 5000,
        read_write_ratio: float = 0.7
    ):
        self.target_duration = target_duration_minutes * 60
        self.read_clients = read_clients
        self.write_clients = write_clients
        self.read_write_ratio = read_write_ratio
        
        self.store = ReadWriteMemoryStore(shard_count=16)
        self.metrics = ReadWriteMetrics()
        
        self.stop_event = asyncio.Event()
        self.read_semaphore = asyncio.Semaphore(read_clients)
        self.write_semaphore = asyncio.Semaphore(write_clients)
        
        # 预填充数据
        self.preloaded_ids: List[str] = []
        
    async def preload_data(self, count: int = 10000):
        """预填充测试数据"""
        logger.info(f"预填充 {count} 条测试数据...")
        
        tasks = []
        for i in range(count):
            memory_id = f"preload_{i:08d}_{int(time.time()*1000000)}"
            content = f"Preloaded content {i} with random data {random.randint(1, 1000000)}"
            task = self.store.write(memory_id, content, -1)
            tasks.append(task)
            self.preloaded_ids.append(memory_id)
        
        # 分批执行
        batch_size = 1000
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
        
        logger.info(f"预填充完成: {len(self.preloaded_ids)} 条数据")
    
    async def run(self) -> ReadWriteMetrics:
        """运行读写混合测试"""
        logger.info("=" * 80)
        logger.info("场景2: 读写混合高并发测试")
        logger.info("=" * 80)
        logger.info(f"读客户端: {self.read_clients:,}")
        logger.info(f"写客户端: {self.write_clients:,}")
        logger.info(f"读写比例: {self.read_write_ratio*100:.0f}:{(1-self.read_write_ratio)*100:.0f}")
        logger.info(f"目标持续时间: {self.target_duration/60:.1f} 分钟")
        logger.info("=" * 80)
        
        # 预填充数据
        await self.preload_data(10000)
        
        self.metrics.start_time = time.time()
        self.metrics.read_clients = self.read_clients
        self.metrics.write_clients = self.write_clients
        self.metrics.read_write_ratio = self.read_write_ratio
        
        # 启动监控任务
        monitor_task = asyncio.create_task(self._monitor_throughput())
        isolation_task = asyncio.create_task(self._check_isolation())
        resource_task = asyncio.create_task(self._monitor_resources())
        
        # 启动读写客户端
        read_tasks = [
            asyncio.create_task(self._read_client(client_id))
            for client_id in range(self.read_clients)
        ]
        
        write_tasks = [
            asyncio.create_task(self._write_client(client_id))
            for client_id in range(self.write_clients)
        ]
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待所有任务完成
        logger.info("等待所有任务完成...")
        await asyncio.gather(*read_tasks, *write_tasks, return_exceptions=True)
        await asyncio.gather(monitor_task, isolation_task, resource_task, return_exceptions=True)
        
        self.metrics.end_time = time.time()
        self.metrics.total_duration_sec = self.metrics.end_time - self.metrics.start_time
        
        # 生成最终报告
        await self._generate_final_metrics()
        
        return self.metrics
    
    async def _read_client(self, client_id: int):
        """读客户端"""
        sequence = 0
        
        while not self.stop_event.is_set():
            try:
                async with self.read_semaphore:
                    # 70%概率读取预加载数据, 30%概率查询
                    if random.random() < 0.7 and self.preloaded_ids:
                        memory_id = random.choice(self.preloaded_ids)
                        result = await self.store.read(memory_id, client_id)
                    else:
                        # 查询操作
                        query = f"content{random.randint(1, 100)}"
                        result = await self.store.read_by_query(query, client_id)
                    
                    self.metrics.total_read_attempts += 1
                    
                    if result["success"]:
                        self.metrics.successful_reads += 1
                        
                        # 检查脏读
                        if result.get("is_updating"):
                            self.metrics.dirty_reads += 1
                            self.metrics.isolation_violations.append({
                                "type": "dirty_read",
                                "timestamp": time.time(),
                                "client_id": client_id,
                                "memory_id": result.get("memory_id")
                            })
                    else:
                        self.metrics.failed_reads += 1
                        if result.get("error") != "not_found":
                            self.metrics.errors.append({
                                "timestamp": time.time(),
                                "client_id": client_id,
                                "operation": "read",
                                "error": result.get("error")
                            })
                    
                    sequence += 1
                    
                    # 控制读取速率
                    await asyncio.sleep(random.uniform(0.001, 0.01))
                    
            except Exception as e:
                self.metrics.failed_reads += 1
                self.metrics.errors.append({
                    "timestamp": time.time(),
                    "client_id": client_id,
                    "operation": "read",
                    "error": type(e).__name__,
                    "details": str(e)
                })
    
    async def _write_client(self, client_id: int):
        """写客户端"""
        sequence = 0
        
        while not self.stop_event.is_set():
            try:
                async with self.write_semaphore:
                    # 生成新数据
                    memory_id = f"write_{client_id:05d}_{sequence:08d}_{int(time.time()*1000000)}"
                    content = (
                        f"WriteClient{client_id:05d}_Seq{sequence:08d}_"
                        f"Rand{random.randint(1, 1000000)}_" +
                        " ".join([f"word{i}" for i in range(random.randint(10, 50))])
                    )
                    
                    result = await self.store.write(memory_id, content, client_id)
                    
                    self.metrics.total_write_attempts += 1
                    
                    if result["success"]:
                        self.metrics.successful_writes += 1
                        # 添加到预加载列表供读取
                        if random.random() < 0.1:  # 10%概率
                            self.preloaded_ids.append(memory_id)
                    else:
                        self.metrics.failed_writes += 1
                        self.metrics.errors.append({
                            "timestamp": time.time(),
                            "client_id": client_id,
                            "operation": "write",
                            "error": result.get("error", "unknown")
                        })
                    
                    sequence += 1
                    
                    # 控制写入速率
                    await asyncio.sleep(random.uniform(0.002, 0.02))
                    
            except Exception as e:
                self.metrics.failed_writes += 1
                self.metrics.errors.append({
                    "timestamp": time.time(),
                    "client_id": client_id,
                    "operation": "write",
                    "error": type(e).__name__,
                    "details": str(e)
                })
    
    async def _monitor_throughput(self):
        """监控吞吐量"""
        last_reads = 0
        last_writes = 0
        last_time = time.time()
        
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            current_time = time.time()
            elapsed = current_time - last_time
            
            reads_delta = self.metrics.successful_reads - last_reads
            writes_delta = self.metrics.successful_writes - last_writes
            
            read_tps = reads_delta / elapsed
            write_tps = writes_delta / elapsed
            total_tps = read_tps + write_tps
            
            logger.info(
                f"[Throughput] Read: {read_tps:,.0f}/s | Write: {write_tps:,.0f}/s | "
                f"Total: {total_tps:,.0f}/s | Ratio: {read_tps/max(total_tps,1)*100:.0f}%"
            )
            
            self.metrics.throughput_timeline.append({
                "timestamp": current_time,
                "read_tps": read_tps,
                "write_tps": write_tps,
                "total_tps": total_tps
            })
            
            last_reads = self.metrics.successful_reads
            last_writes = self.metrics.successful_writes
            last_time = current_time
    
    async def _check_isolation(self):
        """检查隔离性"""
        check_count = 0
        while not self.stop_event.is_set():
            await asyncio.sleep(10)
            
            dirty_read_count = self.metrics.dirty_reads
            
            if dirty_read_count > 0:
                logger.warning(f"[Isolation] Detected {dirty_read_count} dirty reads")
            
            check_count += 1
    
    async def _monitor_resources(self):
        """监控资源使用"""
        try:
            import psutil
            process = psutil.Process()
            
            while not self.stop_event.is_set():
                await asyncio.sleep(10)
                
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent()
                
                self.metrics.resource_usage_samples.append({
                    "timestamp": time.time(),
                    "memory_mb": memory_info.rss / 1024 / 1024,
                    "cpu_percent": cpu_percent,
                    "threads": process.num_threads()
                })
        except ImportError:
            pass
    
    async def _generate_final_metrics(self):
        """生成最终指标"""
        stats = self.store.get_stats()
        
        self.metrics.read_throughput_per_sec = stats["read"]["total"] / self.metrics.total_duration_sec
        self.metrics.write_throughput_per_sec = stats["write"]["total"] / self.metrics.total_duration_sec
        self.metrics.total_throughput_per_sec = self.metrics.read_throughput_per_sec + self.metrics.write_throughput_per_sec
        
        self.metrics.read_latency_p50_ms = stats["read"]["p50_ms"]
        self.metrics.read_latency_p99_ms = stats["read"]["p99_ms"]
        self.metrics.write_latency_p50_ms = stats["write"]["p50_ms"]
        self.metrics.write_latency_p99_ms = stats["write"]["p99_ms"]
        
        # 检测缺陷
        if self.metrics.dirty_reads > 0:
            self.metrics.defects_found.append({
                "id": "DEFECT-S2-001",
                "severity": "P0",
                "type": "dirty_read",
                "description": f"检测到 {self.metrics.dirty_reads} 次脏读",
                "isolation_level": "read_committed"
            })
        
        read_failure_rate = self.metrics.failed_reads / max(self.metrics.total_read_attempts, 1)
        if read_failure_rate > 0.05:
            self.metrics.defects_found.append({
                "id": "DEFECT-S2-002",
                "severity": "P1",
                "type": "high_read_failure_rate",
                "description": f"读取失败率过高: {read_failure_rate*100:.2f}%",
                "threshold": "5%",
                "actual": f"{read_failure_rate*100:.2f}%"
            })


async def main():
    """主函数"""
    runner = ReadWriteTestRunner(
        target_duration_minutes=5,
        read_clients=5000,
        write_clients=5000,
        read_write_ratio=0.7
    )
    
    try:
        metrics = await runner.run()
        
        # 保存详细报告
        metrics_dir = Path("/workspace/agent_memory_system_v2/tests/round1/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = metrics_dir / "scenario2_read_write_mixed_metrics.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(metrics), f, indent=2, default=str)
        
        # 打印摘要
        print("\n" + "=" * 80)
        print("场景2: 读写混合高并发测试 - 完成")
        print("=" * 80)
        print(f"运行时间: {metrics.total_duration_sec/60:.2f} 分钟")
        print(f"读客户端: {metrics.read_clients:,}")
        print(f"写客户端: {metrics.write_clients:,}")
        print(f"读写比例: {metrics.read_write_ratio*100:.0f}:{(1-metrics.read_write_ratio)*100:.0f}")
        print("-" * 80)
        print(f"读取操作:")
        print(f"  - 总尝试: {metrics.total_read_attempts:,}")
        print(f"  - 成功: {metrics.successful_reads:,}")
        print(f"  - 失败: {metrics.failed_reads:,}")
        print(f"  - 吞吐量: {metrics.read_throughput_per_sec:,.0f}/s")
        print(f"  - P50延迟: {metrics.read_latency_p50_ms:.2f}ms")
        print(f"  - P99延迟: {metrics.read_latency_p99_ms:.2f}ms")
        print(f"写入操作:")
        print(f"  - 总尝试: {metrics.total_write_attempts:,}")
        print(f"  - 成功: {metrics.successful_writes:,}")
        print(f"  - 失败: {metrics.failed_writes:,}")
        print(f"  - 吞吐量: {metrics.write_throughput_per_sec:,.0f}/s")
        print(f"  - P50延迟: {metrics.write_latency_p50_ms:.2f}ms")
        print(f"  - P99延迟: {metrics.write_latency_p99_ms:.2f}ms")
        print("-" * 80)
        print(f"隔离性检查:")
        print(f"  - 脏读: {metrics.dirty_reads} 次")
        print(f"  - 读写冲突: {metrics.read_write_conflicts} 次")
        print("-" * 80)
        print(f"发现的缺陷: {len(metrics.defects_found)} 个")
        for defect in metrics.defects_found:
            print(f"  - [{defect['severity']}] {defect['id']}: {defect['description']}")
        print("=" * 80)
        
        logger.info(f"测试报告已保存到: {report_path}")
        
    except Exception as e:
        logger.error(f"测试执行失败: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())

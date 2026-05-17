#!/usr/bin/env python3
"""
场景1: 万级并发写入风暴测试
Agent Memory System V3.0 - 第1轮实践

测试目标:
- 10,000个并发客户端
- 每秒50,000次写入操作
- 持续运行30分钟(压缩到5分钟)
- 验证数据一致性和去重

关键指标:
- 写入吞吐量
- 数据一致性
- 去重准确率
- 系统资源使用
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
from concurrent.futures import ThreadPoolExecutor
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
        logging.FileHandler(log_dir / 'scenario1_write_storm_10k.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class WriteStormMetrics:
    """写入风暴测试指标"""
    scenario_name: str = "万级并发写入风暴"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    
    # 并发配置
    concurrent_clients: int = 10000
    target_writes_per_sec: int = 50000
    actual_writes_per_sec: float = 0.0
    
    # 写入统计
    total_write_attempts: int = 0
    successful_writes: int = 0
    failed_writes: int = 0
    timeout_writes: int = 0
    
    # 去重统计
    duplicate_detected: int = 0
    deduplicated_writes: int = 0
    unique_content_hashes: int = 0
    
    # 一致性检查
    data_consistency_violations: int = 0
    consistency_check_count: int = 0
    
    # 延迟指标(ms)
    write_latency_min_ms: float = 0.0
    write_latency_p50_ms: float = 0.0
    write_latency_p95_ms: float = 0.0
    write_latency_p99_ms: float = 0.0
    write_latency_max_ms: float = 0.0
    
    # 内存统计
    memory_entries_final: int = 0
    memory_entries_expected: int = 0
    
    # 时间线数据
    throughput_timeline: List[Dict] = field(default_factory=list)
    error_timeline: List[Dict] = field(default_factory=list)
    consistency_timeline: List[Dict] = field(default_factory=list)
    
    # 错误和缺陷
    errors: List[Dict] = field(default_factory=list)
    defects_found: List[Dict] = field(default_factory=list)
    
    # 资源使用
    resource_usage_samples: List[Dict] = field(default_factory=list)
    
    # 故障注入
    fault_injections: List[Dict] = field(default_factory=list)


class DistributedMemoryStore:
    """分布式内存存储模拟"""
    
    def __init__(self, shard_count: int = 10):
        self.shards: List[Dict[str, Dict]] = [{} for _ in range(shard_count)]
        self.shard_locks = [asyncio.Lock() for _ in range(shard_count)]
        self.content_hashes: Set[str] = set()
        self.hash_lock = asyncio.Lock()
        self.write_times: List[float] = []
        self.times_lock = asyncio.Lock()
        
    def _get_shard(self, key: str) -> int:
        """获取键对应的分片"""
        return hash(key) % len(self.shards)
    
    async def write(self, memory_id: str, content: str, client_id: int) -> Dict:
        """写入记忆"""
        start_time = time.time()
        
        try:
            # 计算内容哈希
            content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
            
            # 检查重复
            async with self.hash_lock:
                if content_hash in self.content_hashes:
                    return {
                        "success": False,
                        "deduplicated": True,
                        "reason": "duplicate_content",
                        "hash": content_hash,
                        "latency_ms": (time.time() - start_time) * 1000
                    }
                self.content_hashes.add(content_hash)
            
            # 选择分片
            shard_idx = self._get_shard(memory_id)
            
            # 写入分片
            async with self.shard_locks[shard_idx]:
                # 模拟写入延迟
                await asyncio.sleep(random.uniform(0.0001, 0.0005))
                
                self.shards[shard_idx][memory_id] = {
                    "id": memory_id,
                    "content": content,
                    "content_hash": content_hash,
                    "client_id": client_id,
                    "created_at": time.time(),
                    "shard": shard_idx
                }
            
            latency = (time.time() - start_time) * 1000
            async with self.times_lock:
                self.write_times.append(latency)
            
            return {
                "success": True,
                "memory_id": memory_id,
                "hash": content_hash,
                "latency_ms": latency,
                "shard": shard_idx
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    def get_total_entries(self) -> int:
        """获取总条目数"""
        return sum(len(shard) for shard in self.shards)
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        times = self.write_times.copy()
        
        if times:
            sorted_times = sorted(times)
            return {
                "total_writes": len(times),
                "latency_min_ms": min(times),
                "latency_p50_ms": sorted_times[len(times) // 2],
                "latency_p95_ms": sorted_times[int(len(times) * 0.95)],
                "latency_p99_ms": sorted_times[int(len(times) * 0.99)],
                "latency_max_ms": max(times),
                "total_entries": self.get_total_entries(),
                "unique_hashes": len(self.content_hashes)
            }
        return {
            "total_writes": 0,
            "latency_min_ms": 0,
            "latency_p50_ms": 0,
            "latency_p95_ms": 0,
            "latency_p99_ms": 0,
            "latency_max_ms": 0,
            "total_entries": 0,
            "unique_hashes": 0
        }
    
    def verify_consistency(self) -> Dict:
        """验证数据一致性"""
        violations = 0
        
        for shard_idx, shard in enumerate(self.shards):
            for memory_id, data in shard.items():
                # 验证ID格式
                parts = memory_id.split("_")
                if len(parts) != 3:
                    violations += 1
                    continue
                
                # 验证哈希
                expected_hash = hashlib.sha256(data["content"].encode()).hexdigest()[:16]
                if data["content_hash"] != expected_hash:
                    violations += 1
        
        return {
            "total_entries": self.get_total_entries(),
            "violations": violations,
            "consistent": violations == 0
        }


class FaultInjector:
    """故障注入器"""
    
    def __init__(self):
        self.fault_history: List[Dict] = []
        self._lock = asyncio.Lock()
    
    async def inject_random_delay(self, probability: float = 0.01):
        """注入随机延迟"""
        if random.random() < probability:
            delay = random.uniform(0.001, 0.05)
            await asyncio.sleep(delay)
            async with self._lock:
                self.fault_history.append({
                    "type": "random_delay",
                    "delay_ms": delay * 1000,
                    "timestamp": time.time()
                })
            return True
        return False
    
    async def inject_write_timeout(self, probability: float = 0.002):
        """注入写入超时"""
        if random.random() < probability:
            async with self._lock:
                self.fault_history.append({
                    "type": "write_timeout",
                    "timestamp": time.time()
                })
            await asyncio.sleep(0.1)
            raise TimeoutError("Injected write timeout")
    
    async def inject_connection_error(self, probability: float = 0.001):
        """注入连接错误"""
        if random.random() < probability:
            async with self._lock:
                self.fault_history.append({
                    "type": "connection_error",
                    "timestamp": time.time()
                })
            raise ConnectionError("Injected connection error")


class WriteStormTestRunner:
    """写入风暴测试运行器"""
    
    def __init__(
        self,
        target_duration_minutes: int = 5,
        concurrent_clients: int = 10000,
        target_writes_per_sec: int = 50000
    ):
        self.target_duration = target_duration_minutes * 60
        self.concurrent_clients = concurrent_clients
        self.target_writes_per_sec = target_writes_per_sec
        
        self.store = DistributedMemoryStore(shard_count=16)
        self.fault_injector = FaultInjector()
        self.metrics = WriteStormMetrics()
        
        self.stop_event = asyncio.Event()
        self.write_semaphore = asyncio.Semaphore(concurrent_clients)
        
        # 缺陷记录
        self.defects: List[Dict] = []
        
    def generate_content(self, client_id: int, sequence: int) -> str:
        """生成测试内容"""
        # 80%概率生成唯一内容, 20%概率生成可能重复的内容
        if random.random() < 0.8:
            return (
                f"Client{client_id:05d}_Seq{sequence:08d}_"
                f"Rand{random.randint(1, 100000000)}_"
                f"Time{time.time():.6f}_" +
                " ".join([f"word{i}" for i in range(random.randint(20, 100))])
            )
        else:
            # 可能重复的内容
            template_id = random.randint(1, 500)
            return (
                f"Template{template_id:04d}_SharedContent_"
                f"CommonData_" +
                " ".join([f"common{i}" for i in range(random.randint(10, 50))])
            )
    
    async def run(self) -> WriteStormMetrics:
        """运行写入风暴测试"""
        logger.info("=" * 80)
        logger.info("场景1: 万级并发写入风暴测试")
        logger.info("=" * 80)
        logger.info(f"并发客户端: {self.concurrent_clients:,}")
        logger.info(f"目标写入速率: {self.target_writes_per_sec:,} 次/秒")
        logger.info(f"目标持续时间: {self.target_duration/60:.1f} 分钟")
        logger.info("=" * 80)
        
        self.metrics.start_time = time.time()
        self.metrics.concurrent_clients = self.concurrent_clients
        self.metrics.target_writes_per_sec = self.target_writes_per_sec
        
        # 启动监控任务
        monitor_task = asyncio.create_task(self._monitor_throughput())
        consistency_task = asyncio.create_task(self._periodic_consistency_check())
        resource_task = asyncio.create_task(self._monitor_resources())
        
        # 启动写入客户端
        client_tasks = [
            asyncio.create_task(self._write_client(client_id))
            for client_id in range(self.concurrent_clients)
        ]
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待所有任务完成
        logger.info("等待所有任务完成...")
        await asyncio.gather(*client_tasks, return_exceptions=True)
        await asyncio.gather(monitor_task, consistency_task, resource_task, return_exceptions=True)
        
        self.metrics.end_time = time.time()
        self.metrics.total_duration_sec = self.metrics.end_time - self.metrics.start_time
        
        # 生成最终报告
        await self._generate_final_metrics()
        
        return self.metrics
    
    async def _write_client(self, client_id: int):
        """写入客户端"""
        sequence = 0
        writes_in_current_second = 0
        second_start = time.time()
        
        while not self.stop_event.is_set():
            try:
                async with self.write_semaphore:
                    # 生成内容
                    content = self.generate_content(client_id, sequence)
                    memory_id = f"{client_id:05d}_{sequence:08d}_{int(time.time()*1000000)}"
                    
                    # 注入故障
                    await self.fault_injector.inject_random_delay()
                    await self.fault_injector.inject_write_timeout()
                    await self.fault_injector.inject_connection_error()
                    
                    # 执行写入
                    result = await self.store.write(memory_id, content, client_id)
                    
                    self.metrics.total_write_attempts += 1
                    
                    if result["success"]:
                        self.metrics.successful_writes += 1
                    elif result.get("deduplicated"):
                        self.metrics.duplicate_detected += 1
                        self.metrics.deduplicated_writes += 1
                    else:
                        self.metrics.failed_writes += 1
                        self.metrics.errors.append({
                            "timestamp": time.time(),
                            "client_id": client_id,
                            "error": result.get("error", "unknown")
                        })
                    
                    sequence += 1
                    writes_in_current_second += 1
                    
                    # 记录吞吐量
                    current_time = time.time()
                    elapsed = current_time - second_start
                    
                    if elapsed >= 1.0:
                        self.metrics.throughput_timeline.append({
                            "timestamp": current_time,
                            "writes_per_sec": writes_in_current_second,
                            "client_id": client_id
                        })
                        writes_in_current_second = 0
                        second_start = current_time
                    
                    # 速率控制
                    target_per_client = self.target_writes_per_sec / self.concurrent_clients
                    if target_per_client > 0:
                        await asyncio.sleep(1.0 / target_per_client)
                    
            except TimeoutError as e:
                self.metrics.timeout_writes += 1
                self.metrics.failed_writes += 1
                self.metrics.errors.append({
                    "timestamp": time.time(),
                    "client_id": client_id,
                    "error": "timeout",
                    "details": str(e)
                })
            except Exception as e:
                self.metrics.failed_writes += 1
                self.metrics.errors.append({
                    "timestamp": time.time(),
                    "client_id": client_id,
                    "error": type(e).__name__,
                    "details": str(e)
                })
    
    async def _monitor_throughput(self):
        """监控吞吐量"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            stats = self.store.get_stats()
            elapsed = time.time() - self.metrics.start_time
            actual_tps = stats["total_writes"] / max(elapsed, 1)
            
            logger.info(
                f"[Throughput] Writes: {stats['total_writes']:,} | "
                f"Actual: {actual_tps:,.0f}/s | Target: {self.target_writes_per_sec:,}/s | "
                f"P50: {stats['latency_p50_ms']:.2f}ms | P99: {stats['latency_p99_ms']:.2f}ms | "
                f"Deduplicated: {self.metrics.deduplicated_writes:,}"
            )
    
    async def _periodic_consistency_check(self):
        """定期一致性检查"""
        check_count = 0
        while not self.stop_event.is_set():
            await asyncio.sleep(15)  # 每15秒检查一次
            
            consistency = self.store.verify_consistency()
            self.metrics.consistency_check_count += 1
            
            self.metrics.consistency_timeline.append({
                "timestamp": time.time(),
                "check_number": check_count,
                **consistency
            })
            
            if consistency["violations"] > 0:
                self.metrics.data_consistency_violations += consistency["violations"]
                defect = {
                    "id": f"DEFECT-S1-{check_count:04d}",
                    "severity": "P0",
                    "type": "data_consistency_violation",
                    "description": f"检测到数据一致性违规: {consistency['violations']} 次",
                    "timestamp": time.time(),
                    "details": consistency
                }
                self.metrics.defects_found.append(defect)
                logger.error(f"[CRITICAL] 数据一致性违规: {consistency['violations']} 次")
            
            logger.info(
                f"[Consistency] Check #{check_count} | "
                f"Entries: {consistency['total_entries']:,} | "
                f"Violations: {consistency['violations']} | "
                f"Consistent: {consistency['consistent']}"
            )
            
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
                    "threads": process.num_threads(),
                    "open_files": len(process.open_files())
                })
        except ImportError:
            logger.warning("psutil not available, resource monitoring disabled")
    
    async def _generate_final_metrics(self):
        """生成最终指标"""
        stats = self.store.get_stats()
        
        self.metrics.actual_writes_per_sec = stats["total_writes"] / self.metrics.total_duration_sec
        self.metrics.memory_entries_final = stats["total_entries"]
        self.metrics.unique_content_hashes = stats["unique_hashes"]
        
        self.metrics.write_latency_min_ms = stats["latency_min_ms"]
        self.metrics.write_latency_p50_ms = stats["latency_p50_ms"]
        self.metrics.write_latency_p95_ms = stats["latency_p95_ms"]
        self.metrics.write_latency_p99_ms = stats["latency_p99_ms"]
        self.metrics.write_latency_max_ms = stats["latency_max_ms"]
        
        self.metrics.fault_injections = self.fault_injector.fault_history
        
        # 检测缺陷
        if self.metrics.failed_writes > self.metrics.total_write_attempts * 0.01:
            self.metrics.defects_found.append({
                "id": "DEFECT-S1-001",
                "severity": "P1",
                "type": "high_failure_rate",
                "description": f"写入失败率过高: {self.metrics.failed_writes/self.metrics.total_write_attempts*100:.2f}%",
                "threshold": "1%",
                "actual": f"{self.metrics.failed_writes/self.metrics.total_write_attempts*100:.2f}%"
            })
        
        if self.metrics.actual_writes_per_sec < self.target_writes_per_sec * 0.8:
            self.metrics.defects_found.append({
                "id": "DEFECT-S1-002",
                "severity": "P2",
                "type": "throughput_degradation",
                "description": f"吞吐量低于目标: {self.metrics.actual_writes_per_sec:,.0f}/s vs {self.target_writes_per_sec:,}/s",
                "target": self.target_writes_per_sec,
                "actual": self.metrics.actual_writes_per_sec
            })
        
        if self.metrics.write_latency_p99_ms > 100:
            self.metrics.defects_found.append({
                "id": "DEFECT-S1-003",
                "severity": "P2",
                "type": "high_latency",
                "description": f"P99延迟过高: {self.metrics.write_latency_p99_ms:.2f}ms",
                "threshold": "100ms",
                "actual": f"{self.metrics.write_latency_p99_ms:.2f}ms"
            })


async def main():
    """主函数"""
    # 配置: 5分钟模拟30分钟, 10000并发, 50000 TPS
    runner = WriteStormTestRunner(
        target_duration_minutes=5,
        concurrent_clients=10000,
        target_writes_per_sec=50000
    )
    
    try:
        metrics = await runner.run()
        
        # 保存详细报告
        metrics_dir = Path("/workspace/agent_memory_system_v2/tests/round1/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = metrics_dir / "scenario1_write_storm_10k_metrics.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(metrics), f, indent=2, default=str)
        
        # 打印摘要
        print("\n" + "=" * 80)
        print("场景1: 万级并发写入风暴测试 - 完成")
        print("=" * 80)
        print(f"运行时间: {metrics.total_duration_sec/60:.2f} 分钟")
        print(f"并发客户端: {metrics.concurrent_clients:,}")
        print(f"目标TPS: {metrics.target_writes_per_sec:,}")
        print(f"实际TPS: {metrics.actual_writes_per_sec:,.2f}")
        print(f"吞吐量达成率: {metrics.actual_writes_per_sec/metrics.target_writes_per_sec*100:.1f}%")
        print("-" * 80)
        print(f"总写入尝试: {metrics.total_write_attempts:,}")
        print(f"成功写入: {metrics.successful_writes:,}")
        print(f"去重写入: {metrics.deduplicated_writes:,}")
        print(f"失败写入: {metrics.failed_writes:,}")
        print(f"写入失败率: {metrics.failed_writes/max(metrics.total_write_attempts,1)*100:.3f}%")
        print("-" * 80)
        print(f"最终记忆数: {metrics.memory_entries_final:,}")
        print(f"唯一内容哈希: {metrics.unique_content_hashes:,}")
        print(f"一致性违规: {metrics.data_consistency_violations}")
        print("-" * 80)
        print(f"P50延迟: {metrics.write_latency_p50_ms:.2f}ms")
        print(f"P95延迟: {metrics.write_latency_p95_ms:.2f}ms")
        print(f"P99延迟: {metrics.write_latency_p99_ms:.2f}ms")
        print(f"最大延迟: {metrics.write_latency_max_ms:.2f}ms")
        print("-" * 80)
        print(f"故障注入: {len(metrics.fault_injections)} 次")
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

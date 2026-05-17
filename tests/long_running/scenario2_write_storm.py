#!/usr/bin/env python3
"""
场景2: 极端并发写入风暴

测试目标:
- 1000个并发客户端同时写入
- 每秒10000次写入操作
- 持续30分钟
- 验证数据一致性和去重

运行时间: 30分钟(压缩到5-10分钟)
数据量: 100万条记忆
并发: 1000个并发客户端
故障: 3种故障同时注入
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/scenario2_write_storm.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class WriteStormReport:
    """写入风暴测试报告"""
    scenario_name: str = "极端并发写入风暴"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    target_writes_per_sec: int = 10000
    actual_writes_per_sec: float = 0.0
    total_write_attempts: int = 0
    successful_writes: int = 0
    failed_writes: int = 0
    duplicate_detected: int = 0
    deduplicated_writes: int = 0
    concurrent_clients: int = 1000
    target_duration_minutes: int = 30
    data_consistency_violations: int = 0
    memory_entries_final: int = 0
    unique_content_hashes: int = 0
    write_latency_p50_ms: float = 0.0
    write_latency_p99_ms: float = 0.0
    throughput_timeline: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    fault_injections: List[Dict] = field(default_factory=list)
    consistency_checks: List[Dict] = field(default_factory=list)
    resource_usage: List[Dict] = field(default_factory=list)


class DeduplicationEngine:
    """去重引擎"""
    
    def __init__(self):
        self.content_hashes: Set[str] = set()
        self.similarity_threshold = 0.95
        self.dedup_stats = {"checked": 0, "duplicates": 0}
        
    def check_duplicate(self, content: str) -> Tuple[bool, str]:
        """检查是否重复"""
        self.dedup_stats["checked"] += 1
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
        
        if content_hash in self.content_hashes:
            self.dedup_stats["duplicates"] += 1
            return True, content_hash
        
        self.content_hashes.add(content_hash)
        return False, content_hash
    
    def check_similarity_duplicate(self, embedding: List[float]) -> bool:
        """基于相似度的去重检查(简化版)"""
        # 模拟相似度检查
        return random.random() < 0.001  # 0.1%概率发现相似内容


class WriteStormMemorySystem:
    """写入风暴测试用的内存系统"""
    
    def __init__(self):
        self.memories: Dict[str, Dict] = {}
        self.write_lock = asyncio.Lock()
        self.dedup_engine = DeduplicationEngine()
        self.write_times: List[float] = []
        self.write_count = 0
        self.error_count = 0
        self.consistency_violations = 0
        
    async def write_with_dedup(self, content: str, client_id: int) -> Dict:
        """带去重的写入"""
        start_time = time.time()
        
        async with self.write_lock:
            # 检查重复
            is_dup, content_hash = self.dedup_engine.check_duplicate(content)
            
            if is_dup:
                return {
                    "success": False,
                    "deduplicated": True,
                    "reason": "duplicate_content",
                    "hash": content_hash,
                    "latency_ms": (time.time() - start_time) * 1000
                }
            
            # 生成记忆ID
            memory_id = f"{client_id}_{content_hash}_{int(time.time()*1000000)}"
            
            # 模拟写入延迟
            await asyncio.sleep(random.uniform(0.0001, 0.001))
            
            # 写入数据
            self.memories[memory_id] = {
                "id": memory_id,
                "content": content,
                "content_hash": content_hash,
                "client_id": client_id,
                "created_at": time.time(),
                "version": 1
            }
            
            self.write_count += 1
            latency = (time.time() - start_time) * 1000
            self.write_times.append(latency)
            
            return {
                "success": True,
                "memory_id": memory_id,
                "hash": content_hash,
                "latency_ms": latency
            }
    
    async def batch_write(self, contents: List[str], client_id: int) -> List[Dict]:
        """批量写入"""
        results = []
        for content in contents:
            result = await self.write_with_dedup(content, client_id)
            results.append(result)
        return results
    
    def verify_consistency(self) -> Dict:
        """验证数据一致性"""
        violations = 0
        unique_hashes = set()
        
        for memory_id, data in self.memories.items():
            # 验证ID格式
            parts = memory_id.split("_")
            if len(parts) != 3:
                violations += 1
                continue
            
            # 验证内容哈希
            expected_hash = hashlib.sha256(data["content"].encode()).hexdigest()[:16]
            if data["content_hash"] != expected_hash:
                violations += 1
            
            unique_hashes.add(data["content_hash"])
        
        self.consistency_violations = violations
        
        return {
            "total_entries": len(self.memories),
            "unique_hashes": len(unique_hashes),
            "violations": violations,
            "consistent": violations == 0
        }
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        if self.write_times:
            sorted_times = sorted(self.write_times)
            p50 = sorted_times[len(sorted_times) // 2]
            p99 = sorted_times[int(len(sorted_times) * 0.99)]
        else:
            p50 = p99 = 0
        
        return {
            "total_writes": self.write_count,
            "total_errors": self.error_count,
            "unique_entries": len(self.memories),
            "dedup_checked": self.dedup_engine.dedup_stats["checked"],
            "duplicates_found": self.dedup_engine.dedup_stats["duplicates"],
            "latency_p50_ms": p50,
            "latency_p99_ms": p99,
            "consistency_violations": self.consistency_violations
        }


class WriteStormFaultInjector:
    """写入风暴故障注入器"""
    
    def __init__(self):
        self.fault_history: List[Dict] = []
        self.active_faults: Dict[str, bool] = {
            "network_delay": False,
            "write_timeout": False,
            "partial_failure": False
        }
    
    async def inject_network_delay(self, probability: float = 0.02):
        """注入网络延迟"""
        if random.random() < probability:
            delay = random.uniform(0.01, 0.1)
            await asyncio.sleep(delay)
            self.fault_history.append({
                "type": "network_delay",
                "delay_ms": delay * 1000,
                "timestamp": time.time()
            })
            return True
        return False
    
    async def inject_write_timeout(self, probability: float = 0.005):
        """注入写入超时"""
        if random.random() < probability:
            self.fault_history.append({
                "type": "write_timeout",
                "timestamp": time.time()
            })
            raise TimeoutError("Write operation timed out (injected fault)")
    
    async def inject_partial_failure(self, probability: float = 0.003):
        """注入部分写入失败"""
        if random.random() < probability:
            self.fault_history.append({
                "type": "partial_failure",
                "timestamp": time.time()
            })
            raise Exception("Partial write failure (injected fault)")


class WriteStormTestRunner:
    """写入风暴测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 10,
                 concurrent_clients: int = 1000,
                 target_writes_per_sec: int = 10000):
        self.target_duration = target_duration_minutes * 60
        self.concurrent_clients = concurrent_clients
        self.target_writes_per_sec = target_writes_per_sec
        self.memory_system = WriteStormMemorySystem()
        self.fault_injector = WriteStormFaultInjector()
        self.report = WriteStormReport()
        self.stop_event = asyncio.Event()
        self.write_semaphore = asyncio.Semaphore(concurrent_clients)
        
    def generate_content(self, client_id: int, sequence: int) -> str:
        """生成测试内容"""
        # 70%概率生成唯一内容, 30%概率生成可能重复的内容
        if random.random() < 0.7:
            # 唯一内容
            return f"Client{client_id}_Seq{sequence}_Unique{random.randint(1, 10000000)}_" + \
                   " ".join([f"word{i}" for i in range(random.randint(20, 100))])
        else:
            # 可能重复的内容 (使用模板)
            template_id = random.randint(1, 100)
            return f"Template{template_id}_CommonContent_" + \
                   " ".join([f"common{i}" for i in range(random.randint(10, 50))])
    
    async def run(self) -> WriteStormReport:
        """运行写入风暴测试"""
        logger.info(f"开始极端并发写入风暴测试")
        logger.info(f"并发客户端: {self.concurrent_clients}")
        logger.info(f"目标写入速率: {self.target_writes_per_sec:,} 次/秒")
        logger.info(f"目标持续时间: {self.target_duration/60:.1f} 分钟")
        
        self.report.start_time = time.time()
        self.report.concurrent_clients = self.concurrent_clients
        self.report.target_writes_per_sec = self.target_writes_per_sec
        
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
        await asyncio.gather(*client_tasks, return_exceptions=True)
        await asyncio.gather(monitor_task, consistency_task, resource_task, return_exceptions=True)
        
        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time
        
        # 生成最终报告
        await self._generate_report()
        
        return self.report
    
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
                    
                    # 注入故障
                    await self.fault_injector.inject_network_delay()
                    await self.fault_injector.inject_write_timeout()
                    await self.fault_injector.inject_partial_failure()
                    
                    # 执行写入
                    result = await self.memory_system.write_with_dedup(content, client_id)
                    
                    self.report.total_write_attempts += 1
                    
                    if result["success"]:
                        self.report.successful_writes += 1
                    elif result.get("deduplicated"):
                        self.report.duplicate_detected += 1
                        self.report.deduplicated_writes += 1
                    else:
                        self.report.failed_writes += 1
                    
                    sequence += 1
                    writes_in_current_second += 1
                    
                    # 控制写入速率
                    current_time = time.time()
                    elapsed = current_time - second_start
                    
                    if elapsed >= 1.0:
                        # 记录吞吐量
                        self.report.throughput_timeline.append({
                            "timestamp": current_time,
                            "writes_per_sec": writes_in_current_second,
                            "client_id": client_id
                        })
                        writes_in_current_second = 0
                        second_start = current_time
                    
                    # 速率控制: 每个客户端的目标速率
                    target_per_client = self.target_writes_per_sec / self.concurrent_clients
                    if target_per_client > 0:
                        await asyncio.sleep(1.0 / target_per_client)
                    
            except TimeoutError as e:
                self.report.failed_writes += 1
                self.report.errors.append(f"Client {client_id} timeout: {e}")
            except Exception as e:
                self.report.failed_writes += 1
                self.report.errors.append(f"Client {client_id} error: {e}")
    
    async def _monitor_throughput(self):
        """监控吞吐量"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            stats = self.memory_system.get_stats()
            elapsed = time.time() - self.report.start_time
            actual_tps = stats["total_writes"] / max(elapsed, 1)
            
            logger.info(
                f"Throughput Monitor - "
                f"Writes: {stats['total_writes']:,}, "
                f"Actual TPS: {actual_tps:.2f}, "
                f"Target TPS: {self.target_writes_per_sec:,}, "
                f"Deduplicated: {stats['duplicates_found']:,}"
            )
    
    async def _periodic_consistency_check(self):
        """定期一致性检查"""
        check_count = 0
        while not self.stop_event.is_set():
            await asyncio.sleep(30)  # 每30秒检查一次
            
            consistency = self.memory_system.verify_consistency()
            self.report.consistency_checks.append({
                "timestamp": time.time(),
                "check_number": check_count,
                **consistency
            })
            
            self.report.data_consistency_violations = consistency["violations"]
            
            logger.info(
                f"Consistency Check #{check_count} - "
                f"Entries: {consistency['total_entries']:,}, "
                f"Violations: {consistency['violations']}, "
                f"Consistent: {consistency['consistent']}"
            )
            
            check_count += 1
    
    async def _monitor_resources(self):
        """监控资源使用"""
        process = psutil.Process()
        
        while not self.stop_event.is_set():
            await asyncio.sleep(10)
            
            memory_info = process.memory_info()
            cpu_percent = process.cpu_percent()
            
            self.report.resource_usage.append({
                "timestamp": time.time(),
                "memory_mb": memory_info.rss / 1024 / 1024,
                "cpu_percent": cpu_percent,
                "threads": process.num_threads(),
                "open_files": len(process.open_files())
            })
    
    async def _generate_report(self):
        """生成测试报告"""
        final_stats = self.memory_system.get_stats()
        
        self.report.actual_writes_per_sec = final_stats["total_writes"] / self.report.total_duration_sec
        self.report.memory_entries_final = final_stats["unique_entries"]
        self.report.unique_content_hashes = len(self.memory_system.dedup_engine.content_hashes)
        self.report.write_latency_p50_ms = final_stats["latency_p50_ms"]
        self.report.write_latency_p99_ms = final_stats["latency_p99_ms"]
        self.report.data_consistency_violations = final_stats["consistency_violations"]
        self.report.fault_injections = self.fault_injector.fault_history
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/long_running/scenario2_write_storm_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    # 配置: 10分钟模拟30分钟, 1000并发, 10000 TPS
    runner = WriteStormTestRunner(
        target_duration_minutes=10,
        concurrent_clients=1000,
        target_writes_per_sec=10000
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景2: 极端并发写入风暴 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"并发客户端: {report.concurrent_clients}")
    print(f"目标TPS: {report.target_writes_per_sec:,}")
    print(f"实际TPS: {report.actual_writes_per_sec:.2f}")
    print(f"总写入尝试: {report.total_write_attempts:,}")
    print(f"成功写入: {report.successful_writes:,}")
    print(f"去重写入: {report.deduplicated_writes:,}")
    print(f"失败写入: {report.failed_writes:,}")
    print(f"最终记忆数: {report.memory_entries_final:,}")
    print(f"P50延迟: {report.write_latency_p50_ms:.2f}ms")
    print(f"P99延迟: {report.write_latency_p99_ms:.2f}ms")
    print(f"一致性违规: {report.data_consistency_violations}")
    print(f"故障注入: {len(report.fault_injections)} 次")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
场景3: 磁盘压力测试

测试目标:
- 模拟磁盘满（100%使用率）
- 验证写入失败处理
- 测试清理后恢复

运行时间: 8分钟
磁盘限制: 512MB
并发: 100个并发客户端
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import sys
import time
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
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/round2/logs/scenario3_disk_pressure.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DiskPressureReport:
    """磁盘压力测试报告"""
    scenario_name: str = "磁盘压力测试"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    disk_full_events: int = 0
    write_failures: int = 0
    write_success_after_retry: int = 0
    cleanup_operations: int = 0
    recovery_success_count: int = 0
    total_data_written_mb: float = 0.0
    total_data_read_mb: float = 0.0
    disk_usage_timeline: List[Dict] = field(default_factory=list)
    write_failure_events: List[Dict] = field(default_factory=list)
    recovery_events: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class SimulatedDiskStorage:
    """模拟磁盘存储 - 带压力处理"""
    
    def __init__(self, disk_size_mb: int = 512, disk_path: str = "/tmp/test_disk_pressure"):
        self.disk_size_mb = disk_size_mb
        self.disk_path = Path(disk_path)
        self.disk_path.mkdir(parents=True, exist_ok=True)
        
        self.data_store: Dict[str, Dict] = {}
        self.checksums: Dict[str, str] = {}
        self.used_space_mb = 0.0
        self.is_full = False
        self.write_failure_count = 0
        self.write_retry_count = 0
        
        self.write_count = 0
        self.read_count = 0
        self.cleanup_count = 0
        
        self.lock = asyncio.Lock()
        
    def _calculate_checksum(self, data: Dict) -> str:
        """计算数据校验和"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def _estimate_size_mb(self, data: Dict) -> float:
        """估计数据大小"""
        return len(json.dumps(data).encode()) / 1024 / 1024
    
    async def write(self, key: str, data: Dict, retry_count: int = 3) -> Dict:
        """写入数据 - 带重试机制"""
        async with self.lock:
            # 检查磁盘满
            if self.is_full:
                self.write_failure_count += 1
                return {
                    "success": False,
                    "error": "disk_full",
                    "used_space_mb": self.used_space_mb,
                    "retry_recommended": True
                }
            
            size_mb = self._estimate_size_mb(data)
            
            # 检查空间
            if self.used_space_mb + size_mb > self.disk_size_mb:
                self.is_full = True
                self.write_failure_count += 1
                return {
                    "success": False,
                    "error": "disk_full",
                    "used_space_mb": self.used_space_mb,
                    "retry_recommended": True
                }
            
            # 计算校验和
            checksum = self._calculate_checksum(data)
            
            # 写入数据
            self.data_store[key] = data.copy()
            self.checksums[key] = checksum
            self.used_space_mb += size_mb
            self.write_count += 1
            
            return {
                "success": True,
                "key": key,
                "size_mb": size_mb,
                "checksum": checksum
            }
    
    async def write_with_retry(self, key: str, data: Dict, max_retries: int = 3) -> Dict:
        """带重试的写入"""
        for attempt in range(max_retries):
            result = await self.write(key, data)
            
            if result["success"]:
                if attempt > 0:
                    self.write_retry_count += 1
                return result
            
            if result.get("error") == "disk_full" and attempt < max_retries - 1:
                # 等待并尝试清理
                await asyncio.sleep(0.5 * (attempt + 1))
                
                # 尝试清理旧数据
                if attempt == 1:
                    await self._emergency_cleanup()
        
        return result
    
    async def _emergency_cleanup(self) -> int:
        """紧急清理"""
        async with self.lock:
            if not self.data_store:
                return 0
            
            # 删除最旧的20%数据
            sorted_entries = sorted(
                self.data_store.items(),
                key=lambda x: x[1].get("timestamp", 0)
            )
            
            to_delete = len(sorted_entries) // 5
            freed_space = 0.0
            
            for key, data in sorted_entries[:to_delete]:
                size_mb = self._estimate_size_mb(data)
                del self.data_store[key]
                del self.checksums[key]
                self.used_space_mb -= size_mb
                freed_space += size_mb
            
            self.is_full = False
            self.cleanup_count += 1
            
            logger.info(f"Emergency cleanup freed {freed_space:.2f}MB")
            return to_delete
    
    async def cleanup_old_data(self, retention_percent: float = 0.7) -> Dict:
        """清理旧数据"""
        async with self.lock:
            if not self.data_store:
                return {"cleaned": 0, "freed_mb": 0}
            
            target_count = int(len(self.data_store) * retention_percent)
            sorted_entries = sorted(
                self.data_store.items(),
                key=lambda x: x[1].get("timestamp", 0)
            )
            
            to_delete = len(self.data_store) - target_count
            freed_space = 0.0
            
            for key, data in sorted_entries[:to_delete]:
                size_mb = self._estimate_size_mb(data)
                del self.data_store[key]
                del self.checksums[key]
                self.used_space_mb -= size_mb
                freed_space += size_mb
            
            self.is_full = False
            self.cleanup_count += 1
            
            return {
                "cleaned": to_delete,
                "freed_mb": freed_space,
                "remaining": len(self.data_store)
            }
    
    async def read(self, key: str) -> Dict:
        """读取数据"""
        async with self.lock:
            if key not in self.data_store:
                return {"success": False, "error": "key_not_found"}
            
            data = self.data_store[key]
            self.read_count += 1
            
            return {
                "success": True,
                "key": key,
                "data": data
            }
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            "total_keys": len(self.data_store),
            "used_space_mb": self.used_space_mb,
            "free_space_mb": self.disk_size_mb - self.used_space_mb,
            "usage_percent": (self.used_space_mb / self.disk_size_mb) * 100,
            "is_full": self.is_full,
            "write_count": self.write_count,
            "read_count": self.read_count,
            "write_failure_count": self.write_failure_count,
            "write_retry_count": self.write_retry_count,
            "cleanup_count": self.cleanup_count
        }


class DiskPressureFaultInjector:
    """磁盘压力故障注入器"""
    
    def __init__(self, storage: SimulatedDiskStorage):
        self.storage = storage
        self.fault_history: List[Dict] = []
    
    async def inject_disk_full(self, probability: float = 0.01):
        """注入磁盘满故障"""
        if random.random() < probability:
            self.storage.is_full = True
            self.fault_history.append({
                "type": "disk_full",
                "timestamp": time.time(),
                "used_space_mb": self.storage.used_space_mb
            })
            logger.warning(f"磁盘满故障注入 - 已使用: {self.storage.used_space_mb:.2f}MB")
            return True
        return False
    
    async def inject_slow_write(self, probability: float = 0.02):
        """注入慢写入故障"""
        if random.random() < probability:
            delay = random.uniform(0.1, 0.5)
            await asyncio.sleep(delay)
            self.fault_history.append({
                "type": "slow_write",
                "delay_sec": delay,
                "timestamp": time.time()
            })
            return True
        return False
    
    async def inject_write_error(self, probability: float = 0.005):
        """注入写入错误"""
        if random.random() < probability:
            self.fault_history.append({
                "type": "write_error",
                "timestamp": time.time()
            })
            raise Exception("Injected write error")


class DiskPressureTestRunner:
    """磁盘压力测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 8,
                 disk_size_mb: int = 512,
                 concurrent_clients: int = 100):
        self.target_duration = target_duration_minutes * 60
        self.disk_size_mb = disk_size_mb
        self.concurrent_clients = concurrent_clients
        
        self.storage = SimulatedDiskStorage(disk_size_mb, "/tmp/test_disk_pressure_round2")
        self.fault_injector = DiskPressureFaultInjector(self.storage)
        
        self.report = DiskPressureReport()
        self.stop_event = asyncio.Event()
        
    async def run(self) -> DiskPressureReport:
        """运行磁盘压力测试"""
        logger.info("开始磁盘压力测试")
        logger.info(f"磁盘大小: {self.disk_size_mb}MB")
        logger.info(f"并发客户端: {self.concurrent_clients}")

        self.report.start_time = time.time()

        # 启动工作负载
        tasks = [
            asyncio.create_task(self._write_worker()),
            asyncio.create_task(self._read_worker()),
            asyncio.create_task(self._disk_monitor()),
            asyncio.create_task(self._fault_injector_worker()),
            asyncio.create_task(self._cleanup_worker()),
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
        sequence = 0
        while not self.stop_event.is_set():
            try:
                key = f"client_{client_id}_seq_{sequence}_{int(time.time()*1000000)}"
                data = {
                    "client_id": client_id,
                    "sequence": sequence,
                    "content": f"Data from client {client_id} sequence {sequence} " + "x" * random.randint(100, 1000),
                    "timestamp": time.time(),
                    "metadata": {
                        "priority": random.randint(1, 10),
                        "tags": [f"tag_{i}" for i in range(random.randint(1, 5))]
                    }
                }
                
                # 使用带重试的写入
                result = await self.storage.write_with_retry(key, data)
                
                if result["success"]:
                    self.report.total_data_written_mb += result["size_mb"]
                    if result.get("retry_success"):
                        self.report.write_success_after_retry += 1
                elif result.get("error") == "disk_full":
                    self.report.disk_full_events += 1
                    self.report.write_failures += 1
                    self.report.write_failure_events.append({
                        "timestamp": time.time(),
                        "type": "disk_full",
                        "client_id": client_id
                    })
                
                sequence += 1
                
            except Exception as e:
                self.report.errors.append(f"Client {client_id} error: {e}")
            
            await asyncio.sleep(random.uniform(0.001, 0.01))
    
    async def _write_worker(self):
        """写入工作器"""
        while not self.stop_event.is_set():
            try:
                for _ in range(50):
                    key = f"bulk_{int(time.time()*1000000)}_{random.randint(1, 1000000)}"
                    data = {
                        "type": "bulk_write",
                        "content": "Bulk data " + "y" * random.randint(500, 2000),
                        "timestamp": time.time()
                    }
                    
                    result = await self.storage.write_with_retry(key, data)
                    if result["success"]:
                        self.report.total_data_written_mb += result["size_mb"]
                    elif result.get("error") == "disk_full":
                        self.report.write_failures += 1
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Write worker error: {e}")
    
    async def _read_worker(self):
        """读取工作器"""
        while not self.stop_event.is_set():
            try:
                if self.storage.data_store:
                    key = random.choice(list(self.storage.data_store.keys()))
                    result = await self.storage.read(key)
                    
                    if result["success"]:
                        self.report.total_data_read_mb += self.storage._estimate_size_mb(result["data"])
                
                await asyncio.sleep(0.05)
                
            except Exception as e:
                logger.warning(f"Read worker error: {e}")
    
    async def _disk_monitor(self):
        """磁盘监控器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            try:
                stats = self.storage.get_stats()
                
                self.report.disk_usage_timeline.append({
                    "timestamp": time.time(),
                    "used_space_mb": stats["used_space_mb"],
                    "free_space_mb": stats["free_space_mb"],
                    "usage_percent": stats["usage_percent"],
                    "is_full": stats["is_full"],
                    "total_keys": stats["total_keys"]
                })
                
                logger.info(
                    f"Disk Monitor - Used: {stats['used_space_mb']:.2f}MB "
                    f"({stats['usage_percent']:.1f}%), "
                    f"Keys: {stats['total_keys']:,}, "
                    f"Full: {stats['is_full']}, "
                    f"Write Failures: {stats['write_failure_count']}"
                )
                
            except Exception as e:
                logger.error(f"Disk monitor error: {e}")
    
    async def _fault_injector_worker(self):
        """故障注入工作器"""
        fault_cycle = 0
        while not self.stop_event.is_set():
            try:
                if await self.fault_injector.inject_disk_full():
                    pass
                
                await self.fault_injector.inject_slow_write()
                
                fault_cycle += 1
                # 每30秒尝试清理
                if fault_cycle % 6 == 0 and self.storage.is_full:
                    result = await self.storage.cleanup_old_data()
                    if result["cleaned"] > 0:
                        self.report.cleanup_operations += 1
                        self.report.recovery_success_count += 1
                        self.report.recovery_events.append({
                            "timestamp": time.time(),
                            "type": "cleanup_recovery",
                            "cleaned_keys": result["cleaned"],
                            "freed_mb": result["freed_mb"]
                        })
                        logger.info(f"Cleanup recovery: freed {result['freed_mb']:.2f}MB")
                
            except Exception as e:
                logger.warning(f"Fault injector error: {e}")
            
            await asyncio.sleep(5)
    
    async def _cleanup_worker(self):
        """定期清理工作器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(30)
            
            try:
                stats = self.storage.get_stats()
                
                # 如果磁盘使用超过85%，主动清理
                if stats["usage_percent"] > 85:
                    result = await self.storage.cleanup_old_data(retention_percent=0.6)
                    
                    if result["cleaned"] > 0:
                        self.report.cleanup_operations += 1
                        self.report.recovery_events.append({
                            "timestamp": time.time(),
                            "type": "scheduled_cleanup",
                            "cleaned_keys": result["cleaned"],
                            "freed_mb": result["freed_mb"]
                        })
                        
                        logger.info(f"Scheduled cleanup: freed {result['freed_mb']:.2f}MB")
                        
            except Exception as e:
                logger.warning(f"Cleanup worker error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        final_stats = self.storage.get_stats()
        
        self.report.total_data_written_mb = final_stats["used_space_mb"]
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/round2/metrics/scenario3_disk_pressure_metrics.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = DiskPressureTestRunner(
        target_duration_minutes=8,
        disk_size_mb=512,
        concurrent_clients=100
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景3: 磁盘压力测试 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"磁盘满事件: {report.disk_full_events}")
    print(f"写入失败: {report.write_failures}")
    print(f"重试成功: {report.write_success_after_retry}")
    print(f"清理操作: {report.cleanup_operations}")
    print(f"恢复成功: {report.recovery_success_count}")
    print(f"总写入数据: {report.total_data_written_mb:.2f}MB")
    print(f"总读取数据: {report.total_data_read_mb:.2f}MB")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

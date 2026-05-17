#!/usr/bin/env python3
"""
场景5: 磁盘故障与数据损坏

测试目标:
- 模拟磁盘满、磁盘损坏
- 随机数据损坏注入
- 验证数据校验和恢复
- 测试备份恢复流程

运行时间: 20分钟(压缩到5-10分钟)
数据量: 100万条记忆
并发: 100个并发客户端
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
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/scenario5_disk_failure.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DiskFailureReport:
    """磁盘故障测试报告"""
    scenario_name: str = "磁盘故障与数据损坏"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    disk_full_events: int = 0
    disk_corruption_events: int = 0
    checksum_failures: int = 0
    data_corruptions_detected: int = 0
    data_corruptions_repaired: int = 0
    backup_operations: int = 0
    restore_operations: int = 0
    recovery_success_rate: float = 0.0
    total_data_written_mb: float = 0.0
    total_data_read_mb: float = 0.0
    disk_usage_timeline: List[Dict] = field(default_factory=list)
    corruption_events: List[Dict] = field(default_factory=list)
    recovery_events: List[Dict] = field(default_factory=list)
    checksum_verifications: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class SimulatedDiskStorage:
    """模拟磁盘存储"""
    
    def __init__(self, disk_size_mb: int = 1024, disk_path: str = "/tmp/test_disk"):
        self.disk_size_mb = disk_size_mb
        self.disk_path = Path(disk_path)
        self.disk_path.mkdir(parents=True, exist_ok=True)
        
        self.data_store: Dict[str, Dict] = {}
        self.checksums: Dict[str, str] = {}
        self.used_space_mb = 0.0
        self.is_full = False
        self.corrupted_sectors: Set[str] = set()
        
        self.write_count = 0
        self.read_count = 0
        self.checksum_fail_count = 0
        self.repair_count = 0
        
        self.lock = asyncio.Lock()
        
    def _calculate_checksum(self, data: Dict) -> str:
        """计算数据校验和"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def _estimate_size_mb(self, data: Dict) -> float:
        """估计数据大小"""
        return len(json.dumps(data).encode()) / 1024 / 1024
    
    async def write(self, key: str, data: Dict) -> Dict:
        """写入数据"""
        async with self.lock:
            # 检查磁盘满
            if self.is_full:
                return {
                    "success": False,
                    "error": "disk_full",
                    "used_space_mb": self.used_space_mb
                }
            
            size_mb = self._estimate_size_mb(data)
            
            # 检查空间
            if self.used_space_mb + size_mb > self.disk_size_mb * 0.95:
                self.is_full = True
                return {
                    "success": False,
                    "error": "disk_full",
                    "used_space_mb": self.used_space_mb
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
    
    async def read(self, key: str) -> Dict:
        """读取数据"""
        async with self.lock:
            if key not in self.data_store:
                return {"success": False, "error": "key_not_found"}
            
            # 检查扇区是否损坏
            if key in self.corrupted_sectors:
                # 模拟数据损坏
                corrupted_data = self.data_store[key].copy()
                corrupted_data["_corrupted"] = True
                corrupted_data["content"] = "CORRUPTED_DATA_" + str(random.randint(1, 1000000))
                
                self.read_count += 1
                return {
                    "success": False,
                    "error": "data_corruption",
                    "key": key,
                    "data": corrupted_data
                }
            
            data = self.data_store[key]
            
            # 验证校验和
            current_checksum = self._calculate_checksum(data)
            stored_checksum = self.checksums.get(key)
            
            if stored_checksum and current_checksum != stored_checksum:
                self.checksum_fail_count += 1
                return {
                    "success": False,
                    "error": "checksum_mismatch",
                    "key": key
                }
            
            self.read_count += 1
            return {
                "success": True,
                "key": key,
                "data": data
            }
    
    async def verify_and_repair(self, backup_store: 'SimulatedDiskStorage') -> Dict:
        """验证并修复数据"""
        async with self.lock:
            repaired = 0
            failed = 0
            
            for key in list(self.data_store.keys()):
                # 检查校验和
                data = self.data_store[key]
                current_checksum = self._calculate_checksum(data)
                stored_checksum = self.checksums.get(key)
                
                if stored_checksum and current_checksum != stored_checksum:
                    # 尝试从备份恢复
                    backup_result = await backup_store.read(key)
                    if backup_result["success"]:
                        self.data_store[key] = backup_result["data"]
                        self.checksums[key] = backup_result.get("checksum", self._calculate_checksum(backup_result["data"]))
                        repaired += 1
                        self.repair_count += 1
                    else:
                        failed += 1
            
            return {
                "repaired": repaired,
                "failed": failed,
                "total_checked": len(self.data_store)
            }
    
    async def create_backup(self, backup_store: 'SimulatedDiskStorage') -> Dict:
        """创建备份"""
        backup_count = 0
        backup_size = 0.0
        
        for key, data in self.data_store.items():
            result = await backup_store.write(key, data)
            if result["success"]:
                backup_count += 1
                backup_size += result["size_mb"]
        
        return {
            "backed_up_keys": backup_count,
            "backup_size_mb": backup_size
        }
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            "total_keys": len(self.data_store),
            "used_space_mb": self.used_space_mb,
            "free_space_mb": self.disk_size_mb - self.used_space_mb,
            "usage_percent": (self.used_space_mb / self.disk_size_mb) * 100,
            "is_full": self.is_full,
            "corrupted_sectors": len(self.corrupted_sectors),
            "write_count": self.write_count,
            "read_count": self.read_count,
            "checksum_failures": self.checksum_fail_count,
            "repairs": self.repair_count
        }


class DiskFaultInjector:
    """磁盘故障注入器"""
    
    def __init__(self, primary_storage: SimulatedDiskStorage, backup_storage: SimulatedDiskStorage):
        self.primary = primary_storage
        self.backup = backup_storage
        self.fault_history: List[Dict] = []
    
    async def inject_disk_full(self, probability: float = 0.005):
        """注入磁盘满故障"""
        if random.random() < probability:
            self.primary.is_full = True
            self.fault_history.append({
                "type": "disk_full",
                "timestamp": time.time(),
                "used_space_mb": self.primary.used_space_mb
            })
            logger.warning(f"磁盘满故障注入 - 已使用: {self.primary.used_space_mb:.2f}MB")
            return True
        return False
    
    async def inject_data_corruption(self, probability: float = 0.01):
        """注入数据损坏"""
        if random.random() < probability:
            if self.primary.data_store:
                # 随机选择键进行损坏
                key = random.choice(list(self.primary.data_store.keys()))
                self.primary.corrupted_sectors.add(key)
                
                # 损坏数据
                original_data = self.primary.data_store[key]
                corrupted_data = original_data.copy()
                corrupted_data["content"] = "CORRUPTED_" + str(random.randint(1, 1000000))
                self.primary.data_store[key] = corrupted_data
                
                self.fault_history.append({
                    "type": "data_corruption",
                    "key": key,
                    "timestamp": time.time()
                })
                logger.warning(f"数据损坏注入 - Key: {key}")
                return True
        return False
    
    async def inject_checksum_mismatch(self, probability: float = 0.008):
        """注入校验和不匹配"""
        if random.random() < probability:
            if self.primary.checksums:
                key = random.choice(list(self.primary.checksums.keys()))
                # 修改校验和使其不匹配
                self.primary.checksums[key] = "INVALID_CHECKSUM_" + str(random.randint(1, 1000000))
                
                self.fault_history.append({
                    "type": "checksum_mismatch",
                    "key": key,
                    "timestamp": time.time()
                })
                return True
        return False
    
    async def clear_disk_full(self):
        """清除磁盘满状态"""
        if self.primary.is_full:
            # 模拟清理磁盘
            self.primary.used_space_mb *= 0.7  # 清理30%空间
            self.primary.is_full = False
            logger.info("磁盘空间已清理")


class DiskFailureTestRunner:
    """磁盘故障测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 10,
                 disk_size_mb: int = 1024,
                 target_entries: int = 1_000_000,
                 concurrent_clients: int = 100):
        self.target_duration = target_duration_minutes * 60
        self.disk_size_mb = disk_size_mb
        self.target_entries = target_entries
        self.concurrent_clients = concurrent_clients
        
        # 创建主存储和备份存储
        self.primary_storage = SimulatedDiskStorage(disk_size_mb, "/tmp/test_disk_primary")
        self.backup_storage = SimulatedDiskStorage(disk_size_mb * 2, "/tmp/test_disk_backup")
        self.fault_injector = DiskFaultInjector(self.primary_storage, self.backup_storage)
        
        self.report = DiskFailureReport()
        self.stop_event = asyncio.Event()
        
    async def run(self) -> DiskFailureReport:
        """运行磁盘故障测试 - DEFECT-009 [P2] 修复: 添加超时机制"""
        logger.info("开始磁盘故障与数据损坏测试")
        logger.info(f"磁盘大小: {self.disk_size_mb}MB")
        logger.info(f"目标记忆数: {self.target_entries:,}")
        logger.info(f"并发客户端: {self.concurrent_clients}")

        self.report.start_time = time.time()

        # DEFECT-009 [P2] 修复: 添加测试超时机制
        test_timeout = self.target_duration + 60  # 额外60秒缓冲

        try:
            # 启动工作负载
            tasks = [
                asyncio.create_task(self._write_worker()),
                asyncio.create_task(self._read_worker()),
                asyncio.create_task(self._disk_monitor()),
                asyncio.create_task(self._fault_injector_worker()),
                asyncio.create_task(self._backup_worker()),
                asyncio.create_task(self._repair_worker()),
            ]

            # 启动并发客户端
            client_tasks = [
                asyncio.create_task(self._client_worker(i))
                for i in range(self.concurrent_clients)
            ]

            # DEFECT-009 [P2] 修复: 使用wait_for添加超时
            try:
                await asyncio.wait_for(
                    self._run_test_loop(tasks, client_tasks),
                    timeout=test_timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"测试超时 ({test_timeout}秒)，强制停止")
                self.report.errors.append(f"Test timeout after {test_timeout} seconds")

            # 等待任务完成或超时
            await asyncio.wait(
                tasks + client_tasks,
                timeout=30,  # 给30秒清理时间
                return_when=asyncio.ALL_COMPLETED
            )

        except Exception as e:
            logger.error(f"测试执行异常: {e}")
            self.report.errors.append(f"Test execution error: {e}")

        finally:
            self.stop_event.set()

        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time

        # 生成最终报告
        await self._generate_report()

        return self.report

    async def _run_test_loop(self, tasks: list, client_tasks: list):
        """DEFECT-009 [P2] 修复: 测试主循环"""
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
    
    async def _client_worker(self, client_id: int):
        """客户端工作器"""
        sequence = 0
        while not self.stop_event.is_set():
            try:
                # 写入操作
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
                
                result = await self.primary_storage.write(key, data)
                
                if result["success"]:
                    self.report.total_data_written_mb += result["size_mb"]
                elif result.get("error") == "disk_full":
                    self.report.disk_full_events += 1
                
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
                    
                    result = await self.primary_storage.write(key, data)
                    if result["success"]:
                        self.report.total_data_written_mb += result["size_mb"]
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Write worker error: {e}")
    
    async def _read_worker(self):
        """读取工作器"""
        while not self.stop_event.is_set():
            try:
                if self.primary_storage.data_store:
                    key = random.choice(list(self.primary_storage.data_store.keys()))
                    result = await self.primary_storage.read(key)
                    
                    if result["success"]:
                        self.report.total_data_read_mb += self.primary_storage._estimate_size_mb(result["data"])
                    elif result.get("error") == "data_corruption":
                        self.report.data_corruptions_detected += 1
                        self.report.corruption_events.append({
                            "type": "data_corruption_detected",
                            "key": key,
                            "timestamp": time.time()
                        })
                    elif result.get("error") == "checksum_mismatch":
                        self.report.checksum_failures += 1
                
                await asyncio.sleep(0.05)
                
            except Exception as e:
                logger.warning(f"Read worker error: {e}")
    
    async def _disk_monitor(self):
        """磁盘监控器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            try:
                stats = self.primary_storage.get_stats()
                
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
                    f"Corrupted: {stats['corrupted_sectors']}"
                )
                
            except Exception as e:
                logger.error(f"Disk monitor error: {e}")
    
    async def _fault_injector_worker(self):
        """故障注入工作器"""
        fault_cycle = 0
        while not self.stop_event.is_set():
            try:
                # 注入故障
                if await self.fault_injector.inject_disk_full():
                    self.report.disk_full_events += 1
                
                if await self.fault_injector.inject_data_corruption():
                    self.report.disk_corruption_events += 1
                
                if await self.fault_injector.inject_checksum_mismatch():
                    pass  # 校验和错误会在读取时记录
                
                # 每30秒清理一次磁盘满状态
                fault_cycle += 1
                if fault_cycle % 6 == 0:
                    await self.fault_injector.clear_disk_full()
                
            except Exception as e:
                logger.warning(f"Fault injector error: {e}")
            
            await asyncio.sleep(5)
    
    async def _backup_worker(self):
        """备份工作器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(30)  # 每30秒备份一次
            
            try:
                result = await self.primary_storage.create_backup(self.backup_storage)
                self.report.backup_operations += 1
                
                logger.info(
                    f"Backup completed - Keys: {result['backed_up_keys']:,}, "
                    f"Size: {result['backup_size_mb']:.2f}MB"
                )
                
            except Exception as e:
                logger.warning(f"Backup error: {e}")
    
    async def _repair_worker(self):
        """修复工作器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(20)  # 每20秒尝试修复
            
            try:
                result = await self.primary_storage.verify_and_repair(self.backup_storage)
                
                if result["repaired"] > 0:
                    self.report.data_corruptions_repaired += result["repaired"]
                    self.report.recovery_events.append({
                        "type": "data_repair",
                        "repaired_keys": result["repaired"],
                        "failed_keys": result["failed"],
                        "timestamp": time.time()
                    })
                    
                    logger.info(
                        f"Repair completed - Repaired: {result['repaired']}, "
                        f"Failed: {result['failed']}"
                    )
                
            except Exception as e:
                logger.warning(f"Repair error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        final_stats = self.primary_storage.get_stats()
        
        self.report.total_data_written_mb = final_stats["used_space_mb"]
        self.report.recovery_success_rate = (
            (self.report.data_corruptions_repaired / max(self.report.data_corruptions_detected, 1)) * 100
        )
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/long_running/scenario5_disk_failure_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = DiskFailureTestRunner(
        target_duration_minutes=10,
        disk_size_mb=1024,
        target_entries=1_000_000,
        concurrent_clients=100
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景5: 磁盘故障与数据损坏 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"磁盘满事件: {report.disk_full_events}")
    print(f"数据损坏事件: {report.disk_corruption_events}")
    print(f"校验和失败: {report.checksum_failures}")
    print(f"损坏检测: {report.data_corruptions_detected}")
    print(f"损坏修复: {report.data_corruptions_repaired}")
    print(f"备份操作: {report.backup_operations}")
    print(f"恢复成功率: {report.recovery_success_rate:.2f}%")
    print(f"总写入数据: {report.total_data_written_mb:.2f}MB")
    print(f"总读取数据: {report.total_data_read_mb:.2f}MB")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

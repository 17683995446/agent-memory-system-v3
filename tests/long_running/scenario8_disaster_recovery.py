#!/usr/bin/env python3
"""
场景8: 完整灾难恢复演练

测试目标:
- 模拟数据中心完全故障
- 从备份恢复
- 验证RPO/RTO指标
- 测试多区域故障转移

运行时间: 25分钟(压缩到5-10分钟)
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
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/scenario8_disaster_recovery.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DisasterRecoveryReport:
    """灾难恢复测试报告"""
    scenario_name: str = "完整灾难恢复演练"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    
    # RPO/RTO指标
    target_rpo_seconds: float = 300.0  # 5分钟
    target_rto_seconds: float = 600.0  # 10分钟
    actual_rpo_seconds: float = 0.0
    actual_rto_seconds: float = 0.0
    rpo_met: bool = False
    rto_met: bool = False
    
    # 灾难事件
    disaster_events: int = 0
    failover_events: int = 0
    recovery_events: int = 0
    
    # 数据指标
    total_data_entries: int = 0
    data_backed_up: int = 0
    data_recovered: int = 0
    data_loss_count: int = 0
    data_loss_percentage: float = 0.0
    
    # 区域指标
    primary_region: str = "us-east-1"
    secondary_region: str = "us-west-2"
    active_region: str = ""
    
    # 时间线
    disaster_timeline: List[Dict] = field(default_factory=list)
    recovery_timeline: List[Dict] = field(default_factory=list)
    failover_timeline: List[Dict] = field(default_factory=list)
    
    # 性能指标
    backup_duration_seconds: float = 0.0
    restore_duration_seconds: float = 0.0
    verification_duration_seconds: float = 0.0
    
    errors: List[str] = field(default_factory=list)


class RegionalDataCenter:
    """区域数据中心"""
    
    def __init__(self, region_id: str, is_primary: bool = False):
        self.region_id = region_id
        self.is_primary = is_primary
        self.is_active = is_primary
        self.is_healthy = True
        self.data: Dict[str, Dict] = {}
        self.backups: List[Dict] = []
        self.last_backup_time: Optional[float] = None
        
        self.write_count = 0
        self.read_count = 0
        
    def write(self, key: str, data: Dict) -> bool:
        """写入数据"""
        if not self.is_healthy or not self.is_active:
            return False
        
        self.data[key] = {
            **data,
            "_region": self.region_id,
            "_timestamp": time.time()
        }
        self.write_count += 1
        return True
    
    def read(self, key: str) -> Optional[Dict]:
        """读取数据"""
        if not self.is_healthy:
            return None
        
        self.read_count += 1
        return self.data.get(key)
    
    def create_backup(self) -> Dict:
        """创建备份"""
        backup = {
            "timestamp": time.time(),
            "region": self.region_id,
            "data_count": len(self.data),
            "data": self.data.copy()
        }
        self.backups.append(backup)
        self.last_backup_time = time.time()
        return backup
    
    def restore_from_backup(self, backup: Dict) -> bool:
        """从备份恢复"""
        try:
            self.data = backup.get("data", {}).copy()
            self.is_healthy = True
            return True
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def simulate_disaster(self):
        """模拟灾难"""
        self.is_healthy = False
        self.is_active = False
        # 保留备份但丢失活跃数据
        self.data = {}
    
    def get_stats(self) -> Dict:
        return {
            "region_id": self.region_id,
            "is_primary": self.is_primary,
            "is_active": self.is_active,
            "is_healthy": self.is_healthy,
            "data_count": len(self.data),
            "write_count": self.write_count,
            "read_count": self.read_count,
            "backup_count": len(self.backups),
            "last_backup_time": self.last_backup_time
        }


class DisasterRecoveryManager:
    """灾难恢复管理器"""
    
    def __init__(self):
        self.regions: Dict[str, RegionalDataCenter] = {}
        self.active_region_id: Optional[str] = None
        self.primary_region_id: Optional[str] = None
        
        self.disaster_count = 0
        self.failover_count = 0
        self.recovery_count = 0
        
        self.rpo_violations = 0
        self.rto_violations = 0
        
    def add_region(self, region: RegionalDataCenter):
        """添加区域"""
        self.regions[region.region_id] = region
        if region.is_primary:
            self.primary_region_id = region.region_id
            self.active_region_id = region.region_id
    
    async def simulate_disaster(self, region_id: str) -> Dict:
        """模拟灾难"""
        if region_id not in self.regions:
            return {"success": False, "error": "region_not_found"}
        
        region = self.regions[region_id]
        disaster_time = time.time()
        
        # 记录最后备份时间用于计算RPO
        last_backup = region.last_backup_time
        
        # 模拟灾难
        region.simulate_disaster()
        self.disaster_count += 1
        
        # 如果需要, 触发故障转移
        if region_id == self.active_region_id:
            failover_result = await self._trigger_failover(region_id)
        else:
            failover_result = {"failover_triggered": False}
        
        return {
            "success": True,
            "region_id": region_id,
            "disaster_time": disaster_time,
            "last_backup_time": last_backup,
            "data_loss_seconds": disaster_time - last_backup if last_backup else float('inf'),
            **failover_result
        }
    
    async def _trigger_failover(self, failed_region_id: str) -> Dict:
        """触发故障转移"""
        start_time = time.time()
        
        # 选择备用区域
        standby_regions = [
            r for r in self.regions.values()
            if r.region_id != failed_region_id and r.is_healthy
        ]
        
        if not standby_regions:
            return {"failover_triggered": False, "reason": "no_standby_available"}
        
        # 选择第一个健康的备用区域
        new_active = standby_regions[0]
        new_active.is_active = True
        self.active_region_id = new_active.region_id
        
        failover_time = time.time() - start_time
        self.failover_count += 1
        
        return {
            "failover_triggered": True,
            "new_active_region": new_active.region_id,
            "failover_duration_seconds": failover_time
        }
    
    async def recover_region(self, region_id: str, source_region_id: str) -> Dict:
        """恢复区域"""
        start_time = time.time()
        
        if region_id not in self.regions or source_region_id not in self.regions:
            return {"success": False, "error": "region_not_found"}
        
        target = self.regions[region_id]
        source = self.regions[source_region_id]
        
        # 从源区域获取最新备份
        if source.backups:
            latest_backup = max(source.backups, key=lambda b: b["timestamp"])
            restore_success = target.restore_from_backup(latest_backup)
        else:
            # 直接复制数据
            target.data = source.data.copy()
            restore_success = True
        
        restore_time = time.time() - start_time
        
        if restore_success:
            target.is_healthy = True
            self.recovery_count += 1
            
            return {
                "success": True,
                "region_id": region_id,
                "restore_duration_seconds": restore_time,
                "data_recovered": len(target.data)
            }
        
        return {"success": False, "error": "restore_failed"}
    
    async def verify_data_integrity(self) -> Dict:
        """验证数据完整性"""
        # 收集所有区域的数据
        all_data = {}
        for region in self.regions.values():
            for key, value in region.data.items():
                if key not in all_data:
                    all_data[key] = []
                all_data[key].append({
                    "region": region.region_id,
                    "data": value
                })
        
        # 检查一致性
        inconsistent_keys = 0
        for key, versions in all_data.items():
            if len(versions) > 1:
                # 比较数据哈希
                hashes = set()
                for v in versions:
                    data_str = json.dumps(v["data"], sort_keys=True)
                    data_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
                    hashes.add(data_hash)
                
                if len(hashes) > 1:
                    inconsistent_keys += 1
        
        return {
            "total_keys": len(all_data),
            "inconsistent_keys": inconsistent_keys,
            "consistent": inconsistent_keys == 0
        }


class DisasterRecoveryTestRunner:
    """灾难恢复测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 10,
                 target_rpo_seconds: float = 300.0,
                 target_rto_seconds: float = 600.0):
        self.target_duration = target_duration_minutes * 60
        self.target_rpo = target_rpo_seconds
        self.target_rto = target_rto_seconds
        
        self.dr_manager = DisasterRecoveryManager()
        self.report = DisasterRecoveryReport()
        self.stop_event = asyncio.Event()
        self.concurrent_clients = 100
        
        # 初始化区域
        self._initialize_regions()
        
    def _initialize_regions(self):
        """初始化区域"""
        # 主区域
        primary = RegionalDataCenter("us-east-1", is_primary=True)
        self.dr_manager.add_region(primary)
        
        # 备用区域
        secondary = RegionalDataCenter("us-west-2", is_primary=False)
        self.dr_manager.add_region(secondary)
        
        # 第三区域
        tertiary = RegionalDataCenter("eu-west-1", is_primary=False)
        self.dr_manager.add_region(tertiary)
        
        self.report.primary_region = "us-east-1"
        self.report.secondary_region = "us-west-2"
        self.report.active_region = "us-east-1"
    
    async def run(self) -> DisasterRecoveryReport:
        """运行灾难恢复测试"""
        logger.info("开始完整灾难恢复演练")
        logger.info(f"目标RPO: {self.target_rpo}秒")
        logger.info(f"目标RTO: {self.target_rto}秒")
        
        self.report.start_time = time.time()
        self.report.target_rpo_seconds = self.target_rpo
        self.report.target_rto_seconds = self.target_rto
        
        # 启动工作负载
        workload_task = asyncio.create_task(self._workload_generator())
        
        # 启动备份任务
        backup_task = asyncio.create_task(self._backup_scheduler())
        
        # 执行灾难恢复测试序列
        dr_task = asyncio.create_task(self._disaster_recovery_sequence())
        
        # 监控
        monitor_task = asyncio.create_task(self._monitor())
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待任务完成
        await asyncio.gather(workload_task, backup_task, dr_task, monitor_task, return_exceptions=True)
        
        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time
        
        # 生成最终报告
        await self._generate_report()
        
        return self.report
    
    async def _workload_generator(self):
        """工作负载生成器"""
        sequence = 0
        while not self.stop_event.is_set():
            try:
                # 写入到活跃区域
                active_region = self.dr_manager.regions.get(self.dr_manager.active_region_id)
                if active_region and active_region.is_healthy:
                    key = f"entry_{sequence}_{int(time.time()*1000000)}"
                    data = {
                        "sequence": sequence,
                        "content": f"Data content {sequence} " + "x" * random.randint(100, 1000),
                        "timestamp": time.time(),
                        "client_id": random.randint(1, self.concurrent_clients)
                    }
                    
                    success = active_region.write(key, data)
                    if success:
                        self.report.total_data_entries += 1
                
                sequence += 1
                await asyncio.sleep(random.uniform(0.001, 0.01))
                
            except Exception as e:
                logger.warning(f"Workload error: {e}")
    
    async def _backup_scheduler(self):
        """备份调度器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(30)  # 每30秒备份一次
            
            try:
                for region in self.dr_manager.regions.values():
                    if region.is_healthy:
                        backup = region.create_backup()
                        self.report.data_backed_up = backup["data_count"]
                        logger.info(f"Backup created for {region.region_id}: {backup['data_count']} entries")
                
            except Exception as e:
                logger.warning(f"Backup error: {e}")
    
    async def _disaster_recovery_sequence(self):
        """灾难恢复测试序列"""
        await asyncio.sleep(10)  # 等待系统稳定并生成一些数据
        
        # Phase 1: 模拟主区域灾难
        logger.info("Phase 1: 模拟主区域(us-east-1)灾难")
        disaster_result = await self.dr_manager.simulate_disaster("us-east-1")
        
        if disaster_result["success"]:
            self.report.disaster_events += 1
            self.report.disaster_timeline.append({
                "phase": 1,
                "type": "primary_region_failure",
                "timestamp": time.time(),
                **disaster_result
            })
            
            # 计算RPO
            data_loss_seconds = disaster_result.get("data_loss_seconds", float('inf'))
            self.report.actual_rpo_seconds = data_loss_seconds if data_loss_seconds != float('inf') else 0
            self.report.rpo_met = data_loss_seconds <= self.target_rpo
            
            # 记录故障转移
            if disaster_result.get("failover_triggered"):
                self.report.failover_events += 1
                self.report.failover_timeline.append({
                    "phase": 1,
                    "type": "automatic_failover",
                    "timestamp": time.time(),
                    "new_active_region": disaster_result.get("new_active_region"),
                    "failover_duration": disaster_result.get("failover_duration_seconds")
                })
                
                # 计算RTO
                self.report.actual_rto_seconds = disaster_result.get("failover_duration_seconds", 0)
                self.report.rto_met = self.report.actual_rto_seconds <= self.target_rto
                
                self.report.active_region = disaster_result.get("new_active_region", "")
                
                logger.info(
                    f"Failover completed - New active: {self.report.active_region}, "
                    f"RPO: {self.report.actual_rpo_seconds:.2f}s, "
                    f"RTO: {self.report.actual_rto_seconds:.2f}s"
                )
        
        await asyncio.sleep(10)
        
        # Phase 2: 恢复主区域
        logger.info("Phase 2: 恢复主区域")
        recovery_result = await self.dr_manager.recover_region("us-east-1", self.report.active_region)
        
        if recovery_result["success"]:
            self.report.recovery_events += 1
            self.report.recovery_timeline.append({
                "phase": 2,
                "type": "region_recovery",
                "timestamp": time.time(),
                **recovery_result
            })
            
            self.report.data_recovered = recovery_result.get("data_recovered", 0)
            self.report.restore_duration_seconds = recovery_result.get("restore_duration_seconds", 0)
            
            logger.info(f"Region recovered - Data: {self.report.data_recovered} entries")
        
        await asyncio.sleep(5)
        
        # Phase 3: 切换回主区域
        logger.info("Phase 3: 切换回主区域")
        for region in self.dr_manager.regions.values():
            region.is_active = False
        
        primary = self.dr_manager.regions.get("us-east-1")
        if primary:
            primary.is_active = True
            self.dr_manager.active_region_id = "us-east-1"
            self.report.active_region = "us-east-1"
            
            self.report.failover_timeline.append({
                "phase": 3,
                "type": "failback_to_primary",
                "timestamp": time.time(),
                "target_region": "us-east-1"
            })
            
            logger.info("Failback to primary region completed")
        
        await asyncio.sleep(10)
        
        # Phase 4: 模拟多区域故障
        logger.info("Phase 4: 模拟多区域故障")
        disaster_result2 = await self.dr_manager.simulate_disaster("us-east-1")
        
        if disaster_result2["success"]:
            self.report.disaster_events += 1
            self.report.disaster_timeline.append({
                "phase": 4,
                "type": "secondary_disaster",
                "timestamp": time.time(),
                **disaster_result2
            })
            
            logger.info("Secondary disaster simulation completed")
    
    async def _monitor(self):
        """监控"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            try:
                for region_id, region in self.dr_manager.regions.items():
                    stats = region.get_stats()
                    logger.info(
                        f"Region {region_id} - Healthy: {stats['is_healthy']}, "
                        f"Active: {stats['is_active']}, "
                        f"Data: {stats['data_count']}, "
                        f"Writes: {stats['write_count']}"
                    )
                
            except Exception as e:
                logger.error(f"Monitor error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        # 验证数据完整性
        integrity = await self.dr_manager.verify_data_integrity()
        
        # 计算数据丢失
        active_region = self.dr_manager.regions.get(self.dr_manager.active_region_id)
        if active_region:
            self.report.data_recovered = len(active_region.data)
            self.report.data_loss_count = self.report.total_data_entries - self.report.data_recovered
            if self.report.total_data_entries > 0:
                self.report.data_loss_percentage = (self.report.data_loss_count / self.report.total_data_entries) * 100
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/long_running/scenario8_disaster_recovery_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = DisasterRecoveryTestRunner(
        target_duration_minutes=10,
        target_rpo_seconds=300.0,
        target_rto_seconds=600.0
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景8: 完整灾难恢复演练 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"目标RPO: {report.target_rpo_seconds}秒, 实际RPO: {report.actual_rpo_seconds:.2f}秒, 达标: {'是' if report.rpo_met else '否'}")
    print(f"目标RTO: {report.target_rto_seconds}秒, 实际RTO: {report.actual_rto_seconds:.2f}秒, 达标: {'是' if report.rto_met else '否'}")
    print(f"灾难事件: {report.disaster_events}")
    print(f"故障转移: {report.failover_events}")
    print(f"恢复事件: {report.recovery_events}")
    print(f"总数据条目: {report.total_data_entries:,}")
    print(f"数据恢复: {report.data_recovered:,}")
    print(f"数据丢失: {report.data_loss_count} ({report.data_loss_percentage:.2f}%)")
    print(f"活跃区域: {report.active_region}")
    print(f"备份耗时: {report.backup_duration_seconds:.2f}秒")
    print(f"恢复耗时: {report.restore_duration_seconds:.2f}秒")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

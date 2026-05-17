#!/usr/bin/env python3
"""
场景5: 分布式事务压力测试
Agent Memory System V3.0 - 第1轮实践

测试目标:
- 1000个并发事务
- 跨分片操作
- 验证ACID特性

关键指标:
- 事务吞吐量
- 事务成功率
- ACID合规性
- 死锁检测
"""

import asyncio
import hashlib
import json
import logging
import random
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# 配置日志
log_dir = Path("/workspace/agent_memory_system_v2/tests/round1/logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'scenario5_distributed_transaction.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TransactionStatus(Enum):
    """事务状态"""
    ACTIVE = auto()
    PREPARING = auto()
    PREPARED = auto()
    COMMITTING = auto()
    COMMITTED = auto()
    ABORTING = auto()
    ABORTED = auto()
    TIMEOUT = auto()


class TransactionIsolation(Enum):
    """事务隔离级别"""
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"


@dataclass
class Transaction:
    """事务"""
    tx_id: str
    status: TransactionStatus = TransactionStatus.ACTIVE
    start_time: float = 0.0
    end_time: float = 0.0
    operations: List[Dict] = field(default_factory=list)
    locks: Set[str] = field(default_factory=set)
    isolation_level: TransactionIsolation = TransactionIsolation.READ_COMMITTED


@dataclass
class DistributedTransactionMetrics:
    """分布式事务测试指标"""
    scenario_name: str = "分布式事务压力"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    
    # 并发配置
    concurrent_transactions: int = 1000
    isolation_level: str = "read_committed"
    
    # 事务统计
    total_transactions: int = 0
    committed_transactions: int = 0
    aborted_transactions: int = 0
    timeout_transactions: int = 0
    
    # 操作统计
    total_operations: int = 0
    read_operations: int = 0
    write_operations: int = 0
    cross_shard_operations: int = 0
    
    # 性能指标
    transactions_per_sec: float = 0.0
    avg_latency_ms: float = 0.0
    p50_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    
    # ACID检查
    atomicity_violations: int = 0
    consistency_violations: int = 0
    isolation_violations: int = 0
    durability_violations: int = 0
    
    # 死锁和冲突
    deadlocks_detected: int = 0
    lock_conflicts: int = 0
    lock_timeouts: int = 0
    
    # 时间线数据
    transaction_timeline: List[Dict] = field(default_factory=list)
    acid_violations: List[Dict] = field(default_factory=list)
    
    # 错误和缺陷
    errors: List[Dict] = field(default_factory=list)
    defects_found: List[Dict] = field(default_factory=list)


class ShardedDataStore:
    """分片数据存储"""
    
    def __init__(self, shard_count: int = 8):
        self.shard_count = shard_count
        self.shards: List[Dict[str, Dict]] = [{} for _ in range(shard_count)]
        self.shard_locks = [asyncio.Lock() for _ in range(shard_count)]
        
        # 行级锁管理
        self.row_locks: Dict[str, asyncio.Lock] = {}
        self.lock_owners: Dict[str, str] = {}  # key -> tx_id
        self.global_lock = asyncio.Lock()
        
        # 事务日志
        self.transaction_log: List[Dict] = []
        self.log_lock = asyncio.Lock()
    
    def _get_shard(self, key: str) -> int:
        """获取键对应的分片"""
        return hash(key) % self.shard_count
    
    async def acquire_lock(self, key: str, tx_id: str, timeout: float = 5.0) -> bool:
        """获取行级锁"""
        async with self.global_lock:
            if key in self.lock_owners:
                if self.lock_owners[key] == tx_id:
                    return True  # 已持有锁
                return False  # 被其他事务持有
            
            self.lock_owners[key] = tx_id
            return True
    
    async def release_lock(self, key: str, tx_id: str) -> bool:
        """释放行级锁"""
        async with self.global_lock:
            if key in self.lock_owners and self.lock_owners[key] == tx_id:
                del self.lock_owners[key]
                return True
            return False
    
    async def read(self, key: str, tx_id: str) -> Dict:
        """读取数据"""
        shard_idx = self._get_shard(key)
        
        async with self.shard_locks[shard_idx]:
            data = self.shards[shard_idx].get(key)
            
            if data is None:
                return {"success": False, "error": "not_found"}
            
            # 检查是否有未提交的写入
            if data.get("uncommitted") and data.get("tx_id") != tx_id:
                return {"success": False, "error": "write_conflict"}
            
            return {
                "success": True,
                "key": key,
                "value": data.get("value"),
                "version": data.get("version", 0)
            }
    
    async def write(self, key: str, value: Any, tx_id: str) -> Dict:
        """写入数据(未提交)"""
        shard_idx = self._get_shard(key)
        
        # 获取锁
        if not await self.acquire_lock(key, tx_id):
            return {"success": False, "error": "lock_conflict"}
        
        async with self.shard_locks[shard_idx]:
            current = self.shards[shard_idx].get(key, {})
            
            # 标记未提交状态
            self.shards[shard_idx][key] = {
                "value": value,
                "version": current.get("version", 0) + 1,
                "uncommitted": True,
                "tx_id": tx_id,
                "previous_value": current.get("value")
            }
            
            return {
                "success": True,
                "key": key,
                "version": self.shards[shard_idx][key]["version"]
            }
    
    async def commit(self, tx_id: str, keys: List[str]) -> bool:
        """提交事务"""
        async with self.log_lock:
            self.transaction_log.append({
                "tx_id": tx_id,
                "action": "commit",
                "keys": keys,
                "timestamp": time.time()
            })
        
        for key in keys:
            shard_idx = self._get_shard(key)
            
            async with self.shard_locks[shard_idx]:
                data = self.shards[shard_idx].get(key)
                
                if data and data.get("tx_id") == tx_id:
                    data["uncommitted"] = False
                    data["committed_at"] = time.time()
            
            # 释放锁
            await self.release_lock(key, tx_id)
        
        return True
    
    async def rollback(self, tx_id: str, keys: List[str]) -> bool:
        """回滚事务"""
        async with self.log_lock:
            self.transaction_log.append({
                "tx_id": tx_id,
                "action": "rollback",
                "keys": keys,
                "timestamp": time.time()
            })
        
        for key in keys:
            shard_idx = self._get_shard(key)
            
            async with self.shard_locks[shard_idx]:
                data = self.shards[shard_idx].get(key)
                
                if data and data.get("tx_id") == tx_id:
                    # 恢复旧值
                    if "previous_value" in data:
                        data["value"] = data["previous_value"]
                        data["version"] -= 1
                    
                    if data["version"] <= 0:
                        del self.shards[shard_idx][key]
                    else:
                        data["uncommitted"] = False
                        del data["tx_id"]
            
            # 释放锁
            await self.release_lock(key, tx_id)
        
        return True
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        total_entries = sum(len(shard) for shard in self.shards)
        uncommitted_count = sum(
            sum(1 for d in shard.values() if d.get("uncommitted"))
            for shard in self.shards
        )
        
        return {
            "total_entries": total_entries,
            "uncommitted_entries": uncommitted_count,
            "active_locks": len(self.lock_owners),
            "transaction_log_size": len(self.transaction_log)
        }


class DistributedTransactionManager:
    """分布式事务管理器"""
    
    def __init__(self, store: ShardedDataStore):
        self.store = store
        self.active_transactions: Dict[str, Transaction] = {}
        self.tx_lock = asyncio.Lock()
        self.tx_counter = 0
    
    async def begin_transaction(self, isolation_level: TransactionIsolation = TransactionIsolation.READ_COMMITTED) -> Transaction:
        """开始事务"""
        async with self.tx_lock:
            self.tx_counter += 1
            tx_id = f"tx_{self.tx_counter}_{int(time.time()*1000000)}"
        
        tx = Transaction(
            tx_id=tx_id,
            status=TransactionStatus.ACTIVE,
            start_time=time.time(),
            isolation_level=isolation_level
        )
        
        async with self.tx_lock:
            self.active_transactions[tx_id] = tx
        
        return tx
    
    async def execute_operation(self, tx: Transaction, operation: str, key: str, value: Any = None) -> Dict:
        """执行事务操作"""
        if tx.status != TransactionStatus.ACTIVE:
            return {"success": False, "error": "transaction_not_active"}
        
        # 记录操作
        tx.operations.append({
            "type": operation,
            "key": key,
            "value": value,
            "timestamp": time.time()
        })
        
        if operation == "read":
            return await self.store.read(key, tx.tx_id)
        
        elif operation == "write":
            result = await self.store.write(key, value, tx.tx_id)
            if result["success"]:
                tx.locks.add(key)
            return result
        
        return {"success": False, "error": "unknown_operation"}
    
    async def commit_transaction(self, tx: Transaction, timeout: float = 10.0) -> bool:
        """提交事务"""
        if tx.status != TransactionStatus.ACTIVE:
            return False
        
        tx.status = TransactionStatus.COMMITTING
        
        # 2PC: 准备阶段
        tx.status = TransactionStatus.PREPARING
        
        # 检查所有锁是否仍持有
        for key in tx.locks:
            if not await self.store.acquire_lock(key, tx.tx_id):
                # 锁丢失, 需要回滚
                await self.rollback_transaction(tx)
                return False
        
        tx.status = TransactionStatus.PREPARED
        
        # 2PC: 提交阶段
        tx.status = TransactionStatus.COMMITTING
        
        keys = [op["key"] for op in tx.operations if op["type"] == "write"]
        success = await self.store.commit(tx.tx_id, keys)
        
        if success:
            tx.status = TransactionStatus.COMMITTED
        else:
            tx.status = TransactionStatus.ABORTED
        
        tx.end_time = time.time()
        
        async with self.tx_lock:
            if tx.tx_id in self.active_transactions:
                del self.active_transactions[tx.tx_id]
        
        return success
    
    async def rollback_transaction(self, tx: Transaction) -> bool:
        """回滚事务"""
        if tx.status not in [TransactionStatus.ACTIVE, TransactionStatus.PREPARING]:
            return False
        
        tx.status = TransactionStatus.ABORTING
        
        keys = [op["key"] for op in tx.operations if op["type"] == "write"]
        success = await self.store.rollback(tx.tx_id, keys)
        
        tx.status = TransactionStatus.ABORTED
        tx.end_time = time.time()
        
        async with self.tx_lock:
            if tx.tx_id in self.active_transactions:
                del self.active_transactions[tx.tx_id]
        
        return success


class DistributedTransactionTestRunner:
    """分布式事务测试运行器"""
    
    def __init__(
        self,
        target_duration_minutes: int = 5,
        concurrent_transactions: int = 1000
    ):
        self.target_duration = target_duration_minutes * 60
        self.concurrent_transactions = concurrent_transactions
        
        self.store = ShardedDataStore(shard_count=8)
        self.tx_manager = DistributedTransactionManager(self.store)
        self.metrics = DistributedTransactionMetrics()
        
        self.stop_event = asyncio.Event()
        self.tx_semaphore = asyncio.Semaphore(concurrent_transactions)
        
        self.latencies: List[float] = []
    
    async def run(self) -> DistributedTransactionMetrics:
        """运行分布式事务测试"""
        logger.info("=" * 80)
        logger.info("场景5: 分布式事务压力测试")
        logger.info("=" * 80)
        logger.info(f"并发事务: {self.concurrent_transactions:,}")
        logger.info(f"隔离级别: {self.metrics.isolation_level}")
        logger.info(f"目标持续时间: {self.target_duration/60:.1f} 分钟")
        logger.info("=" * 80)
        
        self.metrics.start_time = time.time()
        self.metrics.concurrent_transactions = self.concurrent_transactions
        
        # 启动监控任务
        monitor_task = asyncio.create_task(self._monitor_stats())
        
        # 启动事务客户端
        tx_tasks = [
            asyncio.create_task(self._transaction_worker(i))
            for i in range(self.concurrent_transactions)
        ]
        
        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 等待所有任务完成
        await asyncio.gather(*tx_tasks, return_exceptions=True)
        await asyncio.gather(monitor_task, return_exceptions=True)
        
        self.metrics.end_time = time.time()
        self.metrics.total_duration_sec = self.metrics.end_time - self.metrics.start_time
        
        # 生成最终报告
        await self._generate_final_metrics()
        
        return self.metrics
    
    async def _transaction_worker(self, worker_id: int):
        """事务工作器"""
        sequence = 0
        
        while not self.stop_event.is_set():
            async with self.tx_semaphore:
                start_time = time.time()
                
                try:
                    # 开始事务
                    tx = await self.tx_manager.begin_transaction()
                    
                    # 生成操作
                    operations = self._generate_operations(worker_id, sequence)
                    cross_shard = False
                    shards_accessed = set()
                    
                    for op in operations:
                        key = op["key"]
                        shard = self.store._get_shard(key)
                        shards_accessed.add(shard)
                        
                        if len(shards_accessed) > 1:
                            cross_shard = True
                        
                        result = await self.tx_manager.execute_operation(
                            tx, op["type"], key, op.get("value")
                        )
                        
                        if not result["success"]:
                            # 操作失败, 回滚
                            if result.get("error") == "lock_conflict":
                                self.metrics.lock_conflicts += 1
                            
                            await self.tx_manager.rollback_transaction(tx)
                            self.metrics.aborted_transactions += 1
                            break
                    else:
                        # 所有操作成功, 提交
                        success = await self.tx_manager.commit_transaction(tx)
                        
                        if success:
                            self.metrics.committed_transactions += 1
                        else:
                            self.metrics.aborted_transactions += 1
                    
                    latency = (time.time() - start_time) * 1000
                    self.latencies.append(latency)
                    
                    self.metrics.total_transactions += 1
                    self.metrics.total_operations += len(operations)
                    
                    if cross_shard:
                        self.metrics.cross_shard_operations += 1
                    
                    self.metrics.transaction_timeline.append({
                        "timestamp": time.time(),
                        "tx_id": tx.tx_id,
                        "status": tx.status.value,
                        "operations": len(operations),
                        "cross_shard": cross_shard,
                        "latency_ms": latency
                    })
                    
                    sequence += 1
                    
                    # 控制事务速率
                    await asyncio.sleep(random.uniform(0.01, 0.05))
                    
                except asyncio.TimeoutError:
                    self.metrics.timeout_transactions += 1
                    self.metrics.aborted_transactions += 1
                except Exception as e:
                    self.metrics.errors.append({
                        "timestamp": time.time(),
                        "worker_id": worker_id,
                        "error": str(e)
                    })
    
    def _generate_operations(self, worker_id: int, sequence: int) -> List[Dict]:
        """生成事务操作"""
        operations = []
        
        # 2-5个操作
        op_count = random.randint(2, 5)
        
        for i in range(op_count):
            # 生成跨分片的key
            key = f"key_w{worker_id:04d}_s{sequence:06d}_i{i}_{random.randint(1, 1000)}"
            
            if random.random() < 0.3:  # 30%读取
                operations.append({"type": "read", "key": key})
            else:  # 70%写入
                value = f"value_{worker_id}_{sequence}_{i}_{random.randint(1, 1000000)}"
                operations.append({"type": "write", "key": key, "value": value})
        
        return operations
    
    async def _monitor_stats(self):
        """监控统计"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            stats = self.store.get_stats()
            
            logger.info(
                f"[Stats] TX: {self.metrics.total_transactions} | "
                f"Committed: {self.metrics.committed_transactions} | "
                f"Aborted: {self.metrics.aborted_transactions} | "
                f"Entries: {stats['total_entries']} | "
                f"Locks: {stats['active_locks']} | "
                f"Conflicts: {self.metrics.lock_conflicts}"
            )
    
    async def _generate_final_metrics(self):
        """生成最终指标"""
        stats = self.store.get_stats()
        
        self.metrics.transactions_per_sec = self.metrics.total_transactions / self.metrics.total_duration_sec
        
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            self.metrics.avg_latency_ms = sum(self.latencies) / len(self.latencies)
            self.metrics.p50_latency_ms = sorted_latencies[len(sorted_latencies) // 2]
            self.metrics.p99_latency_ms = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        
        # 检测缺陷
        commit_rate = self.metrics.committed_transactions / max(self.metrics.total_transactions, 1)
        
        if commit_rate < 0.9:
            self.metrics.defects_found.append({
                "id": "DEFECT-S5-001",
                "severity": "P1",
                "type": "low_commit_rate",
                "description": f"事务提交率过低: {commit_rate*100:.1f}%",
                "threshold": "90%",
                "actual": f"{commit_rate*100:.1f}%"
            })
        
        if self.metrics.lock_conflicts > self.metrics.total_transactions * 0.1:
            self.metrics.defects_found.append({
                "id": "DEFECT-S5-002",
                "severity": "P2",
                "type": "high_lock_contention",
                "description": f"锁冲突率过高: {self.metrics.lock_conflicts} 次",
                "conflict_rate": self.metrics.lock_conflicts / max(self.metrics.total_transactions, 1)
            })
        
        if self.metrics.timeout_transactions > self.metrics.total_transactions * 0.05:
            self.metrics.defects_found.append({
                "id": "DEFECT-S5-003",
                "severity": "P2",
                "type": "high_timeout_rate",
                "description": f"事务超时率过高: {self.metrics.timeout_transactions} 次",
                "timeout_rate": self.metrics.timeout_transactions / max(self.metrics.total_transactions, 1)
            })


async def main():
    """主函数"""
    runner = DistributedTransactionTestRunner(
        target_duration_minutes=3,
        concurrent_transactions=1000
    )
    
    try:
        metrics = await runner.run()
        
        # 保存详细报告
        metrics_dir = Path("/workspace/agent_memory_system_v2/tests/round1/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = metrics_dir / "scenario5_distributed_transaction_metrics.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(metrics), f, indent=2, default=str)
        
        # 打印摘要
        print("\n" + "=" * 80)
        print("场景5: 分布式事务压力测试 - 完成")
        print("=" * 80)
        print(f"运行时间: {metrics.total_duration_sec:.2f} 秒")
        print(f"并发事务: {metrics.concurrent_transactions:,}")
        print("-" * 80)
        print(f"事务统计:")
        print(f"  - 总数: {metrics.total_transactions:,}")
        print(f"  - 已提交: {metrics.committed_transactions:,}")
        print(f"  - 已回滚: {metrics.aborted_transactions:,}")
        print(f"  - 超时: {metrics.timeout_transactions:,}")
        print(f"  - 提交率: {metrics.committed_transactions/max(metrics.total_transactions,1)*100:.1f}%")
        print(f"  - TPS: {metrics.transactions_per_sec:.2f}")
        print("-" * 80)
        print(f"操作统计:")
        print(f"  - 总操作: {metrics.total_operations:,}")
        print(f"  - 跨分片: {metrics.cross_shard_operations:,}")
        print("-" * 80)
        print(f"性能指标:")
        print(f"  - 平均延迟: {metrics.avg_latency_ms:.2f}ms")
        print(f"  - P50延迟: {metrics.p50_latency_ms:.2f}ms")
        print(f"  - P99延迟: {metrics.p99_latency_ms:.2f}ms")
        print("-" * 80)
        print(f"并发控制:")
        print(f"  - 锁冲突: {metrics.lock_conflicts:,}")
        print(f"  - 死锁: {metrics.deadlocks_detected}")
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

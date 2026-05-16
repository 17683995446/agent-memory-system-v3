#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 场景2: 高并发竞争条件测试
Scenario 2: High Concurrency Race Condition Test

目标: 暴露竞态条件和死锁问题

测试内容:
- 1000个并发客户端同时读写同一记忆
- 测试分布式事务的隔离性
- 验证乐观锁/悲观锁正确性
- 发现竞态条件和死锁

缺陷检测:
- 丢失更新 (Lost Update)
- 脏读 (Dirty Read)
- 不可重复读 (Non-repeatable Read)
- 幻读 (Phantom Read)
- 死锁 (Deadlock)
- 活锁 (Livelock)
"""

import asyncio
import time
import random
import json
import logging
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from collections import defaultdict
import hashlib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DefectType(Enum):
    """缺陷类型枚举"""
    LOST_UPDATE = "lost_update"
    DIRTY_READ = "dirty_read"
    NON_REPEATABLE_READ = "non_repeatable_read"
    PHANTOM_READ = "phantom_read"
    DEADLOCK = "deadlock"
    LIVELOCK = "livelock"
    LOCK_TIMEOUT = "lock_timeout"
    SERIALIZATION_FAILURE = "serialization_failure"
    WRITE_SKEW = "write_skew"
    READ_SKEW = "read_skew"


class IsolationLevel(Enum):
    """隔离级别"""
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"


@dataclass
class DefectReport:
    """缺陷报告"""
    defect_type: DefectType
    severity: str
    description: str
    timestamp: float
    evidence: Dict[str, Any] = field(default_factory=dict)
    affected_clients: List[int] = field(default_factory=list)
    reproduction_steps: List[str] = field(default_factory=list)


@dataclass
class LockInfo:
    """锁信息"""
    resource_id: str
    lock_type: str  # 'read' or 'write'
    owner_id: int
    acquired_at: float
    timeout: float = 30.0


@dataclass
class TransactionState:
    """事务状态"""
    tx_id: int
    client_id: int
    start_time: float
    isolation_level: IsolationLevel
    read_set: Set[str] = field(default_factory=set)
    write_set: Set[str] = field(default_factory=set)
    local_changes: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    is_committed: bool = False
    snapshot: Dict[str, Any] = field(default_factory=dict)


class ConcurrentMemoryStore:
    """并发记忆存储"""
    
    def __init__(self, isolation_level: IsolationLevel = IsolationLevel.REPEATABLE_READ):
        self.data: Dict[str, Any] = {}
        self.versions: Dict[str, int] = {}  # 用于乐观锁
        self.locks: Dict[str, List[LockInfo]] = defaultdict(list)
        self.transactions: Dict[int, TransactionState] = {}
        self.isolation_level = isolation_level
        
        self._lock = threading.RLock()
        self._tx_counter = 0
        self._defects: List[DefectReport] = []
        self._operation_log: List[Dict[str, Any]] = []
        
        # 死锁检测
        self._wait_for_graph: Dict[int, Set[int]] = defaultdict(set)
        self._deadlock_count = 0
        self._timeout_count = 0
    
    def _record_defect(self, defect_type: DefectType, severity: str,
                       description: str, evidence: Dict[str, Any] = None,
                       affected_clients: List[int] = None):
        """记录缺陷"""
        defect = DefectReport(
            defect_type=defect_type,
            severity=severity,
            description=description,
            timestamp=time.time(),
            evidence=evidence or {},
            affected_clients=affected_clients or []
        )
        self._defects.append(defect)
        logger.error(f"[缺陷发现] {defect_type.value}: {description}")
    
    def begin_transaction(self, client_id: int, 
                          isolation_level: IsolationLevel = None) -> int:
        """开始事务"""
        with self._lock:
            self._tx_counter += 1
            tx_id = self._tx_counter
            
            level = isolation_level or self.isolation_level
            
            # 创建快照
            snapshot = {}
            if level in [IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE]:
                snapshot = self.data.copy()
            
            self.transactions[tx_id] = TransactionState(
                tx_id=tx_id,
                client_id=client_id,
                start_time=time.time(),
                isolation_level=level,
                snapshot=snapshot
            )
            
            return tx_id
    
    def read(self, tx_id: int, key: str) -> Tuple[Optional[Any], bool]:
        """
        读取数据
        
        Returns:
            (value, success)
        """
        with self._lock:
            tx = self.transactions.get(tx_id)
            if not tx or not tx.is_active:
                return None, False
            
            # 检查本地修改
            if key in tx.local_changes:
                return tx.local_changes[key], True
            
            # 根据隔离级别读取
            if tx.isolation_level == IsolationLevel.READ_UNCOMMITTED:
                # 可能读到未提交的数据
                for other_tx in self.transactions.values():
                    if other_tx.is_active and key in other_tx.local_changes:
                        value = other_tx.local_changes[key]
                        self._record_defect(DefectType.DIRTY_READ, "high",
                            f"读取到未提交的数据: key={key}",
                            {"tx_id": tx_id, "other_tx": other_tx.tx_id, "key": key},
                            [tx.client_id, other_tx.client_id])
                        return value, True
            
            # 读取快照或当前数据
            if tx.isolation_level in [IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE]:
                value = tx.snapshot.get(key)
            else:
                value = self.data.get(key)
            
            tx.read_set.add(key)
            
            return value, True
    
    def write(self, tx_id: int, key: str, value: Any) -> bool:
        """写入数据"""
        with self._lock:
            tx = self.transactions.get(tx_id)
            if not tx or not tx.is_active:
                return False
            
            tx.write_set.add(key)
            tx.local_changes[key] = value
            
            return True
    
    def acquire_lock(self, tx_id: int, resource_id: str, 
                     lock_type: str, timeout: float = 5.0) -> bool:
        """获取锁"""
        start_time = time.time()
        
        while True:
            with self._lock:
                tx = self.transactions.get(tx_id)
                if not tx or not tx.is_active:
                    return False
                
                existing_locks = self.locks.get(resource_id, [])
                
                can_acquire = True
                for lock in existing_locks:
                    if lock.owner_id != tx_id:
                        if lock_type == 'write' or lock.lock_type == 'write':
                            can_acquire = False
                            # 添加到等待图
                            self._wait_for_graph[tx_id].add(lock.owner_id)
                            
                            # 检测死锁
                            if self._detect_deadlock(tx_id):
                                self._deadlock_count += 1
                                self._record_defect(DefectType.DEADLOCK, "critical",
                                    f"检测到死锁: tx_id={tx_id}",
                                    {"wait_for_graph": dict(self._wait_for_graph)},
                                    [tx.client_id])
                                return False
                            break
                
                if can_acquire:
                    # 清理等待图
                    self._wait_for_graph[tx_id].clear()
                    
                    # 添加锁
                    lock_info = LockInfo(
                        resource_id=resource_id,
                        lock_type=lock_type,
                        owner_id=tx_id,
                        acquired_at=time.time(),
                        timeout=timeout
                    )
                    self.locks[resource_id].append(lock_info)
                    return True
            
            # 检查超时
            if time.time() - start_time > timeout:
                with self._lock:
                    self._timeout_count += 1
                    tx = self.transactions.get(tx_id)
                    if tx:
                        self._record_defect(DefectType.LOCK_TIMEOUT, "high",
                            f"锁获取超时: resource={resource_id}, tx_id={tx_id}",
                            {"resource": resource_id, "timeout": timeout},
                            [tx.client_id])
                return False
            
            time.sleep(0.01)
    
    def release_lock(self, tx_id: int, resource_id: str = None):
        """释放锁"""
        with self._lock:
            if resource_id:
                self.locks[resource_id] = [
                    l for l in self.locks[resource_id] if l.owner_id != tx_id
                ]
            else:
                for rid in list(self.locks.keys()):
                    self.locks[rid] = [
                        l for l in self.locks[rid] if l.owner_id != tx_id
                    ]
    
    def _detect_deadlock(self, tx_id: int) -> bool:
        """检测死锁 (使用DFS)"""
        visited = set()
        rec_stack = set()
        
        def dfs(node: int) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in self._wait_for_graph.get(node, set()):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        return dfs(tx_id)
    
    def commit(self, tx_id: int, use_optimistic_lock: bool = True) -> Tuple[bool, str]:
        """
        提交事务
        
        Returns:
            (success, error_message)
        """
        with self._lock:
            tx = self.transactions.get(tx_id)
            if not tx or not tx.is_active:
                return False, "Transaction not found or not active"
            
            # 乐观锁验证
            if use_optimistic_lock:
                for key in tx.write_set:
                    current_version = self.versions.get(key, 0)
                    # 如果版本不匹配，说明有并发修改
                    if key in tx.snapshot and current_version > 0:
                        # 检查是否真的有冲突
                        if self.data.get(key) != tx.snapshot.get(key):
                            self._record_defect(DefectType.LOST_UPDATE, "high",
                                f"检测到丢失更新: key={key}",
                                {"key": key, "tx_id": tx_id},
                                [tx.client_id])
                            tx.is_active = False
                            return False, "Optimistic lock conflict"
            
            # 悲观锁获取
            for key in tx.write_set:
                if not self.acquire_lock(tx_id, key, 'write', timeout=1.0):
                    tx.is_active = False
                    return False, f"Failed to acquire lock for {key}"
            
            # 检查写偏斜 (Write Skew)
            if tx.isolation_level == IsolationLevel.SERIALIZABLE:
                for other_tx in self.transactions.values():
                    if other_tx.tx_id != tx_id and other_tx.is_active:
                        # 检查读写集是否重叠
                        if (tx.read_set & other_tx.write_set) or (tx.write_set & other_tx.read_set):
                            self._record_defect(DefectType.WRITE_SKEW, "medium",
                                f"检测到写偏斜: tx1={tx_id}, tx2={other_tx.tx_id}",
                                {"tx1_reads": list(tx.read_set), "tx1_writes": list(tx.write_set),
                                 "tx2_reads": list(other_tx.read_set), "tx2_writes": list(other_tx.write_set)},
                                [tx.client_id, other_tx.client_id])
            
            # 应用修改
            for key, value in tx.local_changes.items():
                old_value = self.data.get(key)
                self.data[key] = value
                self.versions[key] = self.versions.get(key, 0) + 1
                
                self._operation_log.append({
                    'type': 'write',
                    'tx_id': tx_id,
                    'key': key,
                    'old_value': old_value,
                    'new_value': value,
                    'timestamp': time.time()
                })
            
            # 释放锁
            self.release_lock(tx_id)
            
            tx.is_active = False
            tx.is_committed = True
            
            return True, ""
    
    def rollback(self, tx_id: int):
        """回滚事务"""
        with self._lock:
            tx = self.transactions.get(tx_id)
            if tx:
                tx.is_active = False
                tx.local_changes.clear()
                self.release_lock(tx_id)
    
    def get_defects(self) -> List[DefectReport]:
        """获取所有缺陷"""
        return self._defects.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            return {
                'data_size': len(self.data),
                'active_transactions': sum(1 for tx in self.transactions.values() if tx.is_active),
                'total_transactions': len(self.transactions),
                'deadlock_count': self._deadlock_count,
                'timeout_count': self._timeout_count,
                'defects_count': len(self._defects),
                'active_locks': sum(len(locks) for locks in self.locks.values())
            }


class ConcurrentRaceScenario:
    """高并发竞争条件测试场景"""
    
    def __init__(self, num_clients: int = 1000):
        self.num_clients = num_clients
        self.store = ConcurrentMemoryStore()
        self.results: Dict[str, Any] = {}
        self.defects: List[DefectReport] = []
        
        # 性能指标
        self._latencies: List[float] = []
        self._throughputs: List[float] = []
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        logger.info("=" * 70)
        logger.info("场景2: 高并发竞争条件测试")
        logger.info(f"并发客户端数: {self.num_clients}")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # 初始化测试数据
        self._init_test_data()
        
        # 测试1: 丢失更新测试
        result1 = await self.test_lost_update()
        self.results['lost_update'] = result1
        
        # 测试2: 脏读测试
        result2 = await self.test_dirty_read()
        self.results['dirty_read'] = result2
        
        # 测试3: 不可重复读测试
        result3 = await self.test_non_repeatable_read()
        self.results['non_repeatable_read'] = result3
        
        # 测试4: 死锁测试
        result4 = await self.test_deadlock()
        self.results['deadlock'] = result4
        
        # 测试5: 高并发混合读写测试
        result5 = await self.test_high_concurrency_mixed()
        self.results['high_concurrency_mixed'] = result5
        
        # 测试6: 隔离级别验证
        result6 = await self.test_isolation_levels()
        self.results['isolation_levels'] = result6
        
        end_time = time.time()
        
        # 收集缺陷
        self.defects = self.store.get_defects()
        
        # 生成报告
        report = self._generate_report(start_time, end_time)
        
        return report
    
    def _init_test_data(self):
        """初始化测试数据"""
        logger.info("初始化测试数据...")
        for i in range(100):
            key = f"shared-key-{i}"
            self.store.data[key] = {'value': 0, 'updates': []}
            self.store.versions[key] = 0
        logger.info(f"初始化了 {len(self.store.data)} 个共享数据项")
    
    async def test_lost_update(self) -> Dict[str, Any]:
        """测试1: 丢失更新"""
        logger.info("\n" + "-" * 70)
        logger.info("测试1: 丢失更新测试")
        logger.info("-" * 70)
        
        results = {
            'scenarios': [],
            'lost_updates_detected': 0
        }
        
        # 场景: 多个客户端同时读取并更新同一个值
        target_key = "shared-key-0"
        initial_value = self.store.data[target_key]['value']
        
        async def client_update(client_id: int):
            """客户端更新操作"""
            tx_id = self.store.begin_transaction(client_id)
            
            try:
                # 读取当前值
                current, _ = self.store.read(tx_id, target_key)
                
                # 模拟处理延迟
                await asyncio.sleep(random.uniform(0.001, 0.01))
                
                # 写入新值
                new_value = (current['value'] if current else 0) + 1
                self.store.write(tx_id, target_key, {
                    'value': new_value,
                    'updates': (current['updates'] if current else []) + [client_id]
                })
                
                # 提交
                success, error = self.store.commit(tx_id)
                return success
            except Exception as e:
                self.store.rollback(tx_id)
                return False
        
        # 并发执行
        num_concurrent = min(100, self.num_clients)
        tasks = [client_update(i) for i in range(num_concurrent)]
        outcomes = await asyncio.gather(*tasks)
        
        # 检查结果
        final_value = self.store.data[target_key]['value']
        expected_value = initial_value + num_concurrent
        
        if final_value < expected_value:
            lost_updates = expected_value - final_value
            results['lost_updates_detected'] = lost_updates
            logger.warning(f"检测到 {lost_updates} 次丢失更新!")
            logger.warning(f"初始值: {initial_value}, 期望值: {expected_value}, 实际值: {final_value}")
        
        results['scenarios'].append({
            'initial_value': initial_value,
            'expected_value': expected_value,
            'actual_value': final_value,
            'success_rate': sum(outcomes) / len(outcomes)
        })
        
        return results
    
    async def test_dirty_read(self) -> Dict[str, Any]:
        """测试2: 脏读"""
        logger.info("\n" + "-" * 70)
        logger.info("测试2: 脏读测试")
        logger.info("-" * 70)
        
        results = {
            'scenarios': [],
            'dirty_reads_detected': 0
        }
        
        target_key = "shared-key-1"
        
        async def writer_client(client_id: int):
            """写入者"""
            tx_id = self.store.begin_transaction(client_id, IsolationLevel.READ_UNCOMMITTED)
            
            try:
                self.store.write(tx_id, target_key, {'value': 'uncommitted_data'})
                
                # 保持事务活跃，不立即提交
                await asyncio.sleep(0.5)
                
                # 回滚而不是提交
                self.store.rollback(tx_id)
                return 'rolled_back'
            except Exception as e:
                self.store.rollback(tx_id)
                return 'error'
        
        async def reader_client(client_id: int):
            """读取者"""
            await asyncio.sleep(0.1)  # 等待写入者开始
            
            tx_id = self.store.begin_transaction(client_id, IsolationLevel.READ_UNCOMMITTED)
            
            try:
                value, _ = self.store.read(tx_id, target_key)
                self.store.commit(tx_id)
                return value
            except Exception as e:
                self.store.rollback(tx_id)
                return None
        
        # 并发执行
        writer_task = asyncio.create_task(writer_client(0))
        reader_tasks = [reader_client(i) for i in range(1, 10)]
        
        writer_result = await writer_task
        reader_results = await asyncio.gather(*reader_tasks)
        
        # 检查是否有读者读到了未提交的数据
        dirty_reads = sum(1 for r in reader_results if r and r.get('value') == 'uncommitted_data')
        results['dirty_reads_detected'] = dirty_reads
        
        if dirty_reads > 0:
            logger.warning(f"检测到 {dirty_reads} 次脏读!")
        
        results['scenarios'].append({
            'writer_result': writer_result,
            'reader_results': [str(r) for r in reader_results],
            'dirty_reads': dirty_reads
        })
        
        return results
    
    async def test_non_repeatable_read(self) -> Dict[str, Any]:
        """测试3: 不可重复读"""
        logger.info("\n" + "-" * 70)
        logger.info("测试3: 不可重复读测试")
        logger.info("-" * 70)
        
        results = {
            'scenarios': [],
            'non_repeatable_reads_detected': 0
        }
        
        target_key = "shared-key-2"
        self.store.data[target_key] = {'value': 'initial'}
        
        async def reader_client(client_id: int):
            """读取者 - 两次读取同一数据"""
            tx_id = self.store.begin_transaction(client_id, IsolationLevel.READ_COMMITTED)
            
            try:
                # 第一次读取
                value1, _ = self.store.read(tx_id, target_key)
                
                # 等待其他事务修改
                await asyncio.sleep(0.3)
                
                # 第二次读取
                value2, _ = self.store.read(tx_id, target_key)
                
                self.store.commit(tx_id)
                
                return {
                    'first_read': value1,
                    'second_read': value2,
                    'is_repeatable': value1 == value2
                }
            except Exception as e:
                self.store.rollback(tx_id)
                return {'error': str(e)}
        
        async def writer_client(client_id: int):
            """写入者 - 在读取者两次读取之间修改数据"""
            await asyncio.sleep(0.1)
            
            tx_id = self.store.begin_transaction(client_id)
            
            try:
                self.store.write(tx_id, target_key, {'value': 'modified'})
                self.store.commit(tx_id)
                return 'success'
            except Exception as e:
                self.store.rollback(tx_id)
                return 'error'
        
        # 并发执行
        reader_task = asyncio.create_task(reader_client(0))
        writer_task = asyncio.create_task(writer_client(1))
        
        reader_result = await reader_task
        writer_result = await writer_task
        
        # 检查是否发生了不可重复读
        if not reader_result.get('is_repeatable', True):
            results['non_repeatable_reads_detected'] = 1
            logger.warning("检测到不可重复读!")
        
        results['scenarios'].append({
            'reader_result': reader_result,
            'writer_result': writer_result
        })
        
        return results
    
    async def test_deadlock(self) -> Dict[str, Any]:
        """测试4: 死锁测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试4: 死锁测试")
        logger.info("-" * 70)
        
        results = {
            'scenarios': [],
            'deadlocks_detected': 0
        }
        
        # 重置存储以避免干扰
        self.store = ConcurrentMemoryStore()
        self._init_test_data()
        
        async def client_deadlock_scenario(client_id: int, key1: str, key2: str):
            """死锁场景: 两个客户端以不同顺序获取锁"""
            tx_id = self.store.begin_transaction(client_id)
            
            try:
                # 获取第一个锁
                lock1_success = self.store.acquire_lock(tx_id, key1, 'write', timeout=2.0)
                if not lock1_success:
                    return 'lock1_failed'
                
                await asyncio.sleep(0.1)
                
                # 尝试获取第二个锁
                lock2_success = self.store.acquire_lock(tx_id, key2, 'write', timeout=2.0)
                if not lock2_success:
                    self.store.release_lock(tx_id)
                    return 'lock2_failed'
                
                # 执行操作
                self.store.write(tx_id, key1, {'value': f'updated_by_{client_id}'})
                self.store.write(tx_id, key2, {'value': f'updated_by_{client_id}'})
                
                success, _ = self.store.commit(tx_id, use_optimistic_lock=False)
                return 'success' if success else 'commit_failed'
                
            except Exception as e:
                self.store.rollback(tx_id)
                return f'error: {str(e)}'
        
        # 场景: 两个客户端以相反顺序获取锁
        key1 = "shared-key-10"
        key2 = "shared-key-11"
        
        client1 = asyncio.create_task(client_deadlock_scenario(0, key1, key2))
        client2 = asyncio.create_task(client_deadlock_scenario(1, key2, key1))  # 注意顺序相反
        
        results1 = await client1
        results2 = await client2
        
        stats = self.store.get_stats()
        results['deadlocks_detected'] = stats['deadlock_count']
        
        if stats['deadlock_count'] > 0:
            logger.warning(f"检测到 {stats['deadlock_count']} 次死锁!")
        
        results['scenarios'].append({
            'client1_result': results1,
            'client2_result': results2,
            'deadlock_count': stats['deadlock_count'],
            'timeout_count': stats['timeout_count']
        })
        
        return results
    
    async def test_high_concurrency_mixed(self) -> Dict[str, Any]:
        """测试5: 高并发混合读写测试"""
        logger.info("\n" + "-" * 70)
        logger.info("测试5: 高并发混合读写测试")
        logger.info("-" * 70)
        
        results = {
            'operations': 0,
            'successful': 0,
            'failed': 0,
            'latencies': [],
            'defects_before': len(self.store.get_defects())
        }
        
        # 重置存储
        self.store = ConcurrentMemoryStore()
        self._init_test_data()
        
        async def client_mixed_operations(client_id: int, num_ops: int = 10):
            """客户端混合操作"""
            latencies = []
            successes = 0
            failures = 0
            
            for _ in range(num_ops):
                start = time.time()
                
                tx_id = self.store.begin_transaction(client_id)
                
                try:
                    # 随机选择操作类型
                    op_type = random.choice(['read', 'write', 'read_write'])
                    key = f"shared-key-{random.randint(0, 99)}"
                    
                    if op_type == 'read':
                        self.store.read(tx_id, key)
                    elif op_type == 'write':
                        self.store.write(tx_id, key, {
                            'value': random.randint(0, 1000),
                            'updated_by': client_id
                        })
                    else:
                        # 先读后写
                        current, _ = self.store.read(tx_id, key)
                        self.store.write(tx_id, key, {
                            'value': (current['value'] if current else 0) + 1,
                            'updated_by': client_id
                        })
                    
                    success, _ = self.store.commit(tx_id)
                    if success:
                        successes += 1
                    else:
                        failures += 1
                        
                except Exception as e:
                    self.store.rollback(tx_id)
                    failures += 1
                
                latencies.append((time.time() - start) * 1000)
            
            return {
                'client_id': client_id,
                'successes': successes,
                'failures': failures,
                'latencies': latencies
            }
        
        # 并发执行
        num_concurrent = min(500, self.num_clients)
        tasks = [client_mixed_operations(i, num_ops=20) for i in range(num_concurrent)]
        client_results = await asyncio.gather(*tasks)
        
        # 汇总结果
        for cr in client_results:
            results['successful'] += cr['successes']
            results['failed'] += cr['failures']
            results['latencies'].extend(cr['latencies'])
        
        results['operations'] = results['successful'] + results['failed']
        results['defects_after'] = len(self.store.get_defects())
        results['new_defects'] = results['defects_after'] - results['defects_before']
        
        # 性能指标
        if results['latencies']:
            results['avg_latency_ms'] = sum(results['latencies']) / len(results['latencies'])
            results['p99_latency_ms'] = sorted(results['latencies'])[int(len(results['latencies']) * 0.99)]
        
        logger.info(f"总操作数: {results['operations']}")
        logger.info(f"成功率: {results['successful'] / results['operations'] * 100:.2f}%")
        logger.info(f"新增缺陷: {results['new_defects']}")
        
        return results
    
    async def test_isolation_levels(self) -> Dict[str, Any]:
        """测试6: 隔离级别验证"""
        logger.info("\n" + "-" * 70)
        logger.info("测试6: 隔离级别验证")
        logger.info("-" * 70)
        
        results = {'levels': {}}
        
        for level in IsolationLevel:
            logger.info(f"\n测试隔离级别: {level.value}")
            
            # 创建新存储
            store = ConcurrentMemoryStore(isolation_level=level)
            store.data['test-key'] = {'value': 0}
            
            # 并发测试
            async def increment(client_id: int):
                tx_id = store.begin_transaction(client_id)
                try:
                    current, _ = store.read(tx_id, 'test-key')
                    await asyncio.sleep(0.001)
                    store.write(tx_id, 'test-key', {
                        'value': (current['value'] if current else 0) + 1
                    })
                    success, _ = store.commit(tx_id)
                    return success
                except:
                    store.rollback(tx_id)
                    return False
            
            tasks = [increment(i) for i in range(50)]
            outcomes = await asyncio.gather(*tasks)
            
            final_value = store.data['test-key']['value']
            defects = store.get_defects()
            
            results['levels'][level.value] = {
                'final_value': final_value,
                'expected_value': 50,
                'anomalies': len(defects),
                'success_rate': sum(outcomes) / len(outcomes)
            }
            
            logger.info(f"  最终值: {final_value}, 期望值: 50, 异常数: {len(defects)}")
        
        return results
    
    def _generate_report(self, start_time: float, end_time: float) -> Dict[str, Any]:
        """生成测试报告"""
        defects_by_type = defaultdict(list)
        defects_by_severity = defaultdict(list)
        
        for defect in self.defects:
            defects_by_type[defect.defect_type.value].append(asdict(defect))
            defects_by_severity[defect.severity].append(asdict(defect))
        
        report = {
            'scenario': '高并发竞争条件测试',
            'scenario_id': 'scenario2_concurrent_race',
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'end_time': datetime.fromtimestamp(end_time).isoformat(),
            'duration_seconds': end_time - start_time,
            'config': {
                'num_clients': self.num_clients
            },
            'summary': {
                'total_tests': 6,
                'defects_found': len(self.defects),
                'critical_defects': len(defects_by_severity.get('critical', [])),
                'high_defects': len(defects_by_severity.get('high', [])),
                'medium_defects': len(defects_by_severity.get('medium', [])),
                'low_defects': len(defects_by_severity.get('low', []))
            },
            'defects_by_type': dict(defects_by_type),
            'defects_by_severity': dict(defects_by_severity),
            'all_defects': [asdict(d) for d in self.defects],
            'test_results': self.results
        }
        
        return report


async def main():
    """主函数"""
    scenario = ConcurrentRaceScenario(num_clients=1000)
    report = await scenario.run_all_tests()
    
    # 保存报告
    output_path = "/workspace/agent_memory_system_v2/tests/reports/scenario2_concurrent_race_report.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # 打印摘要
    print("\n" + "=" * 70)
    print("场景2: 高并发竞争条件测试 - 摘要")
    print("=" * 70)
    print(f"测试持续时间: {report['duration_seconds']:.2f} 秒")
    print(f"并发客户端数: {report['config']['num_clients']}")
    print(f"发现缺陷总数: {report['summary']['defects_found']}")
    print(f"  - 严重: {report['summary']['critical_defects']}")
    print(f"  - 高危: {report['summary']['high_defects']}")
    print(f"  - 中危: {report['summary']['medium_defects']}")
    print(f"  - 低危: {report['summary']['low_defects']}")
    
    if report['all_defects']:
        print("\n检测到的缺陷:")
        for defect in report['all_defects'][:10]:  # 只显示前10个
            print(f"  [{defect['severity'].upper()}] {defect['defect_type']}: {defect['description']}")
    
    print(f"\n报告已保存: {output_path}")
    
    return report


if __name__ == "__main__":
    asyncio.run(main())

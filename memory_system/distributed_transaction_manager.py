"""
分布式事务管理器模块

本模块实现了分布式事务管理，支持2PC(两阶段提交)协议和Saga模式(补偿事务)，
提供事务超时和回滚、事务状态追踪等功能。专为Agent Memory System的分布式操作设计。

核心特性:
- 2PC (两阶段提交)协议实现
- Saga模式(补偿事务)支持
- 事务超时和自动回滚
- 事务状态追踪和持久化
- 参与者管理
- 幂等性保证
- 事务协调器高可用

示例用法:
    >>> from distributed_transaction_manager import (
    ...     DistributedTransactionManager,
    ...     Transaction,
    ...     TwoPhaseCommitCoordinator,
    ...     SagaOrchestrator
    ... )
    >>> 
    >>> # 使用2PC
    >>> coordinator = TwoPhaseCommitCoordinator()
    >>> tx = await coordinator.begin_transaction()
    >>> await coordinator.prepare_participant(tx.tx_id, participant1)
    >>> await coordinator.prepare_participant(tx.tx_id, participant2)
    >>> result = await coordinator.commit(tx.tx_id)
    >>> 
    >>> # 使用Saga
    >>> saga = SagaOrchestrator()
    >>> saga.add_step("reserve_inventory", reserve_inventory_action, release_inventory_compensation)
    >>> saga.add_step("process_payment", process_payment_action, refund_payment_compensation)
    >>> result = await saga.execute()
"""

import asyncio
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Protocol,
    Set,
    TypeVar,
    Union,
)

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


logger = logging.getLogger(__name__)


class TransactionStatus(Enum):
    """事务状态枚举"""
    ACTIVE = auto()           # 活动状态
    PREPARING = auto()        # 准备中(2PC)
    PREPARED = auto()         # 已准备(2PC)
    COMMITTING = auto()       # 提交中
    COMMITTED = auto()        # 已提交
    ABORTING = auto()         # 中止中
    ABORTED = auto()          # 已中止
    SAGA_COMPENSATING = auto()  # Saga补偿中
    SAGA_COMPENSATED = auto()   # Saga已补偿
    TIMEOUT = auto()          # 超时
    UNKNOWN = auto()          # 未知


class ParticipantStatus(Enum):
    """参与者状态枚举"""
    ACTIVE = auto()
    PREPARED = auto()
    COMMITTED = auto()
    ABORTED = auto()
    FAILED = auto()


class TransactionError(Exception):
    """事务错误基类"""
    pass


class TransactionNotFoundError(TransactionError):
    """事务未找到错误"""
    pass


class TransactionTimeoutError(TransactionError):
    """事务超时错误"""
    pass


class ParticipantError(TransactionError):
    """参与者错误"""
    pass


class SagaCompensationError(TransactionError):
    """Saga补偿错误"""
    pass


@dataclass
class TransactionContext:
    """
    事务上下文
    
    存储事务执行过程中的上下文信息
    """
    data: Dict[str, Any] = field(default_factory=dict)
    
    def set(self, key: str, value: Any):
        """设置上下文数据"""
        self.data[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取上下文数据"""
        return self.data.get(key, default)
    
    def update(self, other: Dict[str, Any]):
        """更新上下文"""
        self.data.update(other)


@dataclass
class Transaction:
    """
    事务定义
    
    Attributes:
        tx_id: 事务唯一标识
        status: 事务状态
        created_at: 创建时间
        timeout_at: 超时时间
        context: 事务上下文
        metadata: 元数据
        parent_tx_id: 父事务ID(支持嵌套事务)
    """
    tx_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: TransactionStatus = TransactionStatus.ACTIVE
    created_at: float = field(default_factory=time.time)
    timeout_at: Optional[float] = None
    context: TransactionContext = field(default_factory=TransactionContext)
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_tx_id: Optional[str] = None
    
    # 内部状态
    _participants: Set[str] = field(default_factory=set, init=False)
    _prepared_participants: Set[str] = field(default_factory=set, init=False)
    _completed_operations: List[Dict[str, Any]] = field(default_factory=list, init=False)
    
    def is_timed_out(self) -> bool:
        """检查是否超时"""
        if self.timeout_at is None:
            return False
        return time.time() > self.timeout_at
    
    def add_participant(self, participant_id: str):
        """添加参与者"""
        self._participants.add(participant_id)
    
    def mark_prepared(self, participant_id: str):
        """标记参与者已准备"""
        self._prepared_participants.add(participant_id)
    
    def all_prepared(self) -> bool:
        """检查是否所有参与者都已准备"""
        return self._participants == self._prepared_participants
    
    def record_operation(self, operation: Dict[str, Any]):
        """记录操作(用于Saga补偿)"""
        self._completed_operations.append(operation)
    
    def get_completed_operations(self) -> List[Dict[str, Any]]:
        """获取已完成的操作列表"""
        return list(reversed(self._completed_operations))


@dataclass
class Participant:
    """
    事务参与者
    
    参与2PC或Saga的参与者定义
    """
    participant_id: str
    resource_id: str
    prepare_handler: Optional[Callable[[str, TransactionContext], Coroutine[Any, Any, bool]]] = None
    commit_handler: Optional[Callable[[str, TransactionContext], Coroutine[Any, Any, bool]]] = None
    abort_handler: Optional[Callable[[str, TransactionContext], Coroutine[Any, Any, bool]]] = None
    status: ParticipantStatus = ParticipantStatus.ACTIVE
    
    async def prepare(self, tx_id: str, context: TransactionContext) -> bool:
        """准备阶段"""
        if self.prepare_handler:
            try:
                result = await self.prepare_handler(tx_id, context)
                if result:
                    self.status = ParticipantStatus.PREPARED
                return result
            except Exception as e:
                logger.error(f"Participant {self.participant_id} prepare failed: {e}")
                self.status = ParticipantStatus.FAILED
                return False
        return True
    
    async def commit(self, tx_id: str, context: TransactionContext) -> bool:
        """提交
        
        DEFECT-DC-014 [P1] 修复: commit异常后更新参与者状态为FAILED
        """
        if self.commit_handler:
            try:
                result = await self.commit_handler(tx_id, context)
                if result:
                    self.status = ParticipantStatus.COMMITTED
                else:
                    self.status = ParticipantStatus.FAILED
                return result
            except Exception as e:
                logger.error(f"Participant {self.participant_id} commit failed: {e}")
                # DEFECT-DC-014: 更新状态为FAILED，避免恢复逻辑误认为仍可重试
                self.status = ParticipantStatus.FAILED
                return False
        self.status = ParticipantStatus.COMMITTED
        return True
    
    async def abort(self, tx_id: str, context: TransactionContext) -> bool:
        """中止"""
        if self.abort_handler:
            try:
                result = await self.abort_handler(tx_id, context)
                if result:
                    self.status = ParticipantStatus.ABORTED
                return result
            except Exception as e:
                logger.error(f"Participant {self.participant_id} abort failed: {e}")
                return False
        self.status = ParticipantStatus.ABORTED
        return True


@dataclass
class SagaStep:
    """
    Saga步骤
    
    Attributes:
        step_id: 步骤ID
        action: 执行动作
        compensation: 补偿动作
        max_retries: 最大重试次数
    """
    step_id: str
    action: Callable[[TransactionContext], Coroutine[Any, Any, Any]]
    compensation: Callable[[TransactionContext, Any], Coroutine[Any, Any, Any]]
    max_retries: int = 3
    
    async def execute(self, context: TransactionContext) -> Any:
        """执行步骤 - 使用tenacity重试机制替代手动循环"""
        from tenacity import RetryError as TenacityRetryError
        retry_decorator = retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=0.1, min=0.1, max=10.0),
            reraise=True,
        )
        retry_fn = retry_decorator(self.action)
        try:
            return await retry_fn(context)
        except TenacityRetryError as e:
            # tenacity包装了原始异常，提取最后的原因
            last_error = e.last_attempt.exception() if e.last_attempt and e.last_attempt.exception() else e
            raise last_error
    
    async def compensate(self, context: TransactionContext, action_result: Any):
        """执行补偿"""
        try:
            await self.compensation(context, action_result)
        except Exception as e:
            logger.error(f"Compensation for step {self.step_id} failed: {e}")
            raise SagaCompensationError(f"Compensation failed for step {self.step_id}")


class TransactionStore(ABC):
    """
    事务存储抽象基类
    
    用于持久化事务状态
    """
    
    @abstractmethod
    async def save_transaction(self, transaction: Transaction) -> bool:
        """保存事务"""
        pass
    
    @abstractmethod
    async def load_transaction(self, tx_id: str) -> Optional[Transaction]:
        """加载事务"""
        pass
    
    @abstractmethod
    async def update_transaction_status(
        self, tx_id: str, status: TransactionStatus
    ) -> bool:
        """更新事务状态"""
        pass
    
    @abstractmethod
    async def get_active_transactions(self) -> List[Transaction]:
        """获取活动事务"""
        pass


class InMemoryTransactionStore(TransactionStore):
    """内存事务存储实现"""
    
    def __init__(self):
        self._transactions: Dict[str, Transaction] = {}
        self._lock = asyncio.Lock()
    
    async def save_transaction(self, transaction: Transaction) -> bool:
        """保存事务"""
        async with self._lock:
            self._transactions[transaction.tx_id] = transaction
        return True
    
    async def load_transaction(self, tx_id: str) -> Optional[Transaction]:
        """加载事务"""
        return self._transactions.get(tx_id)
    
    async def update_transaction_status(
        self, tx_id: str, status: TransactionStatus
    ) -> bool:
        """更新事务状态"""
        async with self._lock:
            if tx_id in self._transactions:
                self._transactions[tx_id].status = status
                return True
        return False
    
    async def get_active_transactions(self) -> List[Transaction]:
        """获取活动事务"""
        # DEFECT-DC-005 [P0]: UNKNOWN状态也应被视为活动状态
        active_statuses = {
            TransactionStatus.ACTIVE,
            TransactionStatus.PREPARING,
            TransactionStatus.PREPARED,
            TransactionStatus.COMMITTING,
            TransactionStatus.SAGA_COMPENSATING,
            TransactionStatus.UNKNOWN,
        }
        return [
            tx for tx in self._transactions.values()
            if tx.status in active_statuses
        ]


class TwoPhaseCommitCoordinator:
    """
    2PC协调器

    实现两阶段提交协议，确保分布式事务的原子性

    DEFECT-S5-001 [P1] 修复:
        - 优化事务并发控制机制
        - 添加并行提交优化
        - 添加事务提交超时控制
        - 优化锁粒度
    """

    def __init__(
        self,
        store: Optional[TransactionStore] = None,
        default_timeout: float = 30.0,
        parallel_commit: bool = True,
        commit_timeout: float = 10.0,
        max_concurrent_commits: int = 10,
    ):
        self.store = store or InMemoryTransactionStore()
        self.default_timeout = default_timeout
        self._participants: Dict[str, Participant] = {}
        # DEFECT-S5-001 [P1] 修复: 使用更细粒度的锁
        self._tx_locks: Dict[str, asyncio.Lock] = {}  # 每个事务一个锁
        self._global_lock = asyncio.Lock()  # 全局锁仅用于关键操作
        self._recovery_task: Optional[asyncio.Task] = None
        self._running = False

        # DEFECT-S5-001 [P1] 修复: 并行提交配置
        self._parallel_commit = parallel_commit
        self._commit_timeout = commit_timeout
        self._max_concurrent_commits = max_concurrent_commits
        self._commit_semaphore = asyncio.Semaphore(max_concurrent_commits)

        # 性能统计
        self._stats = {
            "total_commits": 0,
            "successful_commits": 0,
            "failed_commits": 0,
            "commit_timeouts": 0,
            "avg_commit_time": 0.0,
        }
        self._stats_lock = asyncio.Lock()
    
    async def start(self):
        """启动协调器"""
        self._running = True
        self._recovery_task = asyncio.create_task(self._recovery_loop())
        logger.info("2PC Coordinator started")
    
    async def stop(self):
        """停止协调器"""
        self._running = False
        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
        logger.info("2PC Coordinator stopped")
    
    async def _recovery_loop(self):
        """恢复循环 - 处理未完成的事务"""
        while self._running:
            try:
                await asyncio.sleep(10)  # 每10秒检查一次
                
                # 获取所有活动事务
                active_txs = await self.store.get_active_transactions()

                # 清理已完成事务的锁
                active_tx_ids = {tx.tx_id for tx in active_txs}
                stale_ids = set(self._tx_locks.keys()) - active_tx_ids
                for tx_id in stale_ids:
                    self._tx_locks.pop(tx_id, None)

                for tx in active_txs:
                    # DA-013 [P1] 修复: 使用per-transaction锁保护恢复操作
                    lock = self._tx_locks.setdefault(tx.tx_id, asyncio.Lock())
                    async with lock:
                        if tx.is_timed_out():
                            logger.warning(f"Transaction {tx.tx_id} timed out, aborting")
                            await self.abort(tx.tx_id)
                        elif tx.status == TransactionStatus.PREPARED:
                            logger.info(f"Resuming commit for transaction {tx.tx_id}")
                            await self._do_commit(tx)
                        elif tx.status == TransactionStatus.PREPARING:
                            logger.info(f"Aborting incomplete transaction {tx.tx_id}")
                            await self.abort(tx.tx_id)
                        # DEFECT-DC-005 [P0]: 处理UNKNOWN状态事务
                        elif tx.status == TransactionStatus.UNKNOWN:
                            logger.warning(
                                f"Transaction {tx.tx_id} in UNKNOWN state, "
                                f"requires manual intervention"
                            )
                            # 记录告警，保持UNKNOWN状态等待人工处理
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Recovery loop error: {e}")
    
    def register_participant(self, participant: Participant):
        """注册参与者"""
        self._participants[participant.participant_id] = participant
    
    async def begin_transaction(
        self,
        timeout: Optional[float] = None,
        parent_tx_id: Optional[str] = None,
    ) -> Transaction:
        """
        开始事务
        
        Args:
            timeout: 超时时间(秒)
            parent_tx_id: 父事务ID
            
        Returns:
            新事务
        """
        timeout = timeout or self.default_timeout
        
        tx = Transaction(
            timeout_at=time.time() + timeout,
            parent_tx_id=parent_tx_id,
        )
        
        await self.store.save_transaction(tx)
        logger.info(f"Transaction {tx.tx_id} started")
        
        return tx
    
    async def add_participant(self, tx_id: str, participant_id: str) -> bool:
        """
        添加参与者到事务
        
        Args:
            tx_id: 事务ID
            participant_id: 参与者ID
            
        Returns:
            是否成功
        """
        tx = await self.store.load_transaction(tx_id)
        if not tx:
            raise TransactionNotFoundError(f"Transaction {tx_id} not found")
        
        if tx.status != TransactionStatus.ACTIVE:
            raise TransactionError(f"Transaction {tx_id} is not active")
        
        if participant_id not in self._participants:
            raise ParticipantError(f"Participant {participant_id} not registered")
        
        tx.add_participant(participant_id)
        await self.store.save_transaction(tx)
        
        return True
    
    async def prepare(self, tx_id: str) -> bool:
        """
        准备阶段(Phase 1)

        DEFECT-S5-001 [P1] 修复:
            - 优化并发控制
            - 添加超时控制
            - 使用信号量限制并发

        Args:
            tx_id: 事务ID

        Returns:
            是否所有参与者都准备成功
        """
        # DEFECT-DC-001 [P0]: 使用per-transaction锁保护并发安全
        lock = self._tx_locks.setdefault(tx_id, asyncio.Lock())
        async with lock:
            tx = await self.store.load_transaction(tx_id)
            if not tx:
                raise TransactionNotFoundError(f"Transaction {tx_id} not found")

            if tx.is_timed_out():
                tx.status = TransactionStatus.TIMEOUT
                await self.store.save_transaction(tx)
                raise TransactionTimeoutError(f"Transaction {tx_id} timed out")

            if tx.status != TransactionStatus.ACTIVE:
                raise TransactionError(f"Cannot prepare transaction {tx_id} in status {tx.status}")

            tx.status = TransactionStatus.PREPARING
            await self.store.save_transaction(tx)

        logger.info(f"Transaction {tx_id} entering prepare phase")

        # DEFECT-S5-001 [P1] 修复: 并行向所有参与者发送准备请求，带并发控制
        prepare_tasks = []
        for participant_id in tx._participants:
            participant = self._participants.get(participant_id)
            if participant:
                # 使用信号量限制并发
                task = self._prepare_participant_with_semaphore(tx_id, participant, tx.context)
                prepare_tasks.append(task)

        if prepare_tasks:
            # DEFECT-S5-001 [P1] 修复: 使用 gather 并设置超时
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*prepare_tasks, return_exceptions=True),
                    timeout=self._commit_timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"Transaction {tx_id} prepare phase timed out")
                await self.abort(tx_id)
                return False

            all_prepared = all(
                r is True for r in results if not isinstance(r, Exception)
            )

            if all_prepared:
                tx.status = TransactionStatus.PREPARED
                await self.store.save_transaction(tx)
                logger.info(f"Transaction {tx_id} prepared successfully")
                return True
            else:
                # 有参与者准备失败，中止
                logger.warning(f"Transaction {tx_id} prepare failed, aborting")
                await self.abort(tx_id)
                return False

        return True

    async def _prepare_participant_with_semaphore(
        self,
        tx_id: str,
        participant: Participant,
        context: TransactionContext,
    ) -> bool:
        """
        使用信号量限制并发的准备操作

        DEFECT-S5-001 [P1] 修复: 添加并发控制
        """
        async with self._commit_semaphore:
            return await self._prepare_participant(tx_id, participant, context)
    
    async def _prepare_participant(
        self,
        tx_id: str,
        participant: Participant,
        context: TransactionContext,
    ) -> bool:
        """准备单个参与者"""
        try:
            result = await participant.prepare(tx_id, context)
            if result:
                tx = await self.store.load_transaction(tx_id)
                if tx:
                    tx.mark_prepared(participant.participant_id)
                    await self.store.save_transaction(tx)
            return result
        except Exception as e:
            logger.error(f"Prepare failed for participant {participant.participant_id}: {e}")
            return False
    
    async def commit(self, tx_id: str) -> bool:
        """
        提交事务(Phase 2)
        
        Args:
            tx_id: 事务ID
            
        Returns:
            是否提交成功
        """
        # DEFECT-DC-001 [P0]: 使用per-transaction锁保护并发安全
        lock = self._tx_locks.setdefault(tx_id, asyncio.Lock())
        async with lock:
            tx = await self.store.load_transaction(tx_id)
            if not tx:
                raise TransactionNotFoundError(f"Transaction {tx_id} not found")

            if tx.status != TransactionStatus.PREPARED:
                raise TransactionError(
                    f"Transaction {tx_id} cannot be committed in status {tx.status}"
                )

            return await self._do_commit(tx)
    
    async def _do_commit(self, tx: Transaction) -> bool:
        """
        执行提交

        DEFECT-R3-S4-001 [P0] 修复:
            - 确保所有参与者在提交前状态一致
            - 验证所有参与者都已准备好
            - 添加参与者状态同步检查
        DEFECT-S5-001 [P1] 修复:
            - 优化并发提交
            - 添加超时控制
            - 添加性能统计
        """
        import time
        commit_start = time.time()

        # DEFECT-R3-S4-001 [P0] 修复: 在提交前验证所有参与者状态一致
        # 重新加载事务以获取最新状态
        tx = await self.store.load_transaction(tx.tx_id)
        if not tx:
            logger.error(f"Transaction {tx.tx_id} not found during commit")
            return False

        # 验证事务状态为 PREPARED
        if tx.status != TransactionStatus.PREPARED:
            logger.error(f"Transaction {tx.tx_id} not in PREPARED state (current: {tx.status})")
            return False

        # 验证所有参与者都已准备好
        if not tx.all_prepared():
            unprepared = tx._participants - tx._prepared_participants
            logger.error(f"Transaction {tx.tx_id} has unprepared participants: {unprepared}")
            return False

        # 验证所有参与者状态为 PREPARED
        for participant_id in tx._participants:
            participant = self._participants.get(participant_id)
            if participant and participant.status != ParticipantStatus.PREPARED:
                logger.error(f"Participant {participant_id} not in PREPARED state (current: {participant.status})")
                return False

        tx.status = TransactionStatus.COMMITTING
        await self.store.save_transaction(tx)

        logger.info(f"Transaction {tx.tx_id} entering commit phase")

        # DEFECT-R3-S4-001 [P0] 修复: 确保所有参与者提交成功
        commit_tasks = []
        participant_ids = []
        for participant_id in tx._participants:
            participant = self._participants.get(participant_id)
            if participant:
                task = self._commit_participant_with_semaphore(participant, tx.tx_id, tx.context)
                commit_tasks.append(task)
                participant_ids.append(participant_id)

        success = True
        committed_participants = []
        if commit_tasks:
            try:
                # DEFECT-S5-001 [P1] 修复: 带超时的并发提交
                results = await asyncio.wait_for(
                    asyncio.gather(*commit_tasks, return_exceptions=True),
                    timeout=self._commit_timeout
                )

                # 检查每个参与者的提交结果
                for i, result in enumerate(results):
                    participant_id = participant_ids[i]
                    if isinstance(result, Exception):
                        logger.error(f"Transaction {tx.tx_id} participant {participant_id} commit failed with exception: {result}")
                        success = False
                    elif result is False:
                        logger.error(f"Transaction {tx.tx_id} participant {participant_id} commit returned False")
                        success = False
                    else:
                        committed_participants.append(participant_id)

                if not success:
                    # DEFECT-R3-S4-001 [P0] 修复: 如果有任何参与者提交失败，记录不一致状态
                    logger.error(f"Transaction {tx.tx_id} commit had failures. Committed: {committed_participants}")
                    # 注意：2PC中提交阶段失败需要人工干预，但我们需要记录哪些参与者已提交

            except asyncio.TimeoutError:
                logger.error(f"Transaction {tx.tx_id} commit phase timed out")
                success = False
                async with self._stats_lock:
                    self._stats["commit_timeouts"] += 1

        # DEFECT-R3-S4-001 [P0] 修复: 只有当所有参与者都成功提交时才标记为 COMMITTED
        if success and len(committed_participants) == len(commit_tasks):
            tx.status = TransactionStatus.COMMITTED
        else:
            tx.status = TransactionStatus.UNKNOWN  # 部分提交失败标记为UNKNOWN而非ABORTED
            tx.metadata["committed_participants"] = committed_participants
            tx.metadata["inconsistent"] = True
            success = False

        await self.store.save_transaction(tx)

        # DEFECT-S5-001 [P1] 修复: 更新性能统计
        commit_duration = time.time() - commit_start
        await self._update_commit_stats(success, commit_duration)

        logger.info(f"Transaction {tx.tx_id} {'committed' if success else 'failed'} in {commit_duration:.3f}s")
        return success

    async def _commit_participant_with_semaphore(
        self,
        participant: Participant,
        tx_id: str,
        context: TransactionContext,
    ) -> bool:
        """
        使用信号量限制并发的提交操作

        DEFECT-S5-001 [P1] 修复: 添加并发控制
        """
        async with self._commit_semaphore:
            return await participant.commit(tx_id, context)

    async def _update_commit_stats(self, success: bool, duration: float) -> None:
        """
        更新提交统计信息

        DEFECT-S5-001 [P1] 修复: 性能监控
        """
        async with self._stats_lock:
            self._stats["total_commits"] += 1
            if success:
                self._stats["successful_commits"] += 1
            else:
                self._stats["failed_commits"] += 1

            # 更新平均提交时间
            total = self._stats["total_commits"]
            old_avg = self._stats["avg_commit_time"]
            self._stats["avg_commit_time"] = (old_avg * (total - 1) + duration) / total

    def get_commit_stats(self) -> Dict[str, Any]:
        """
        获取提交统计信息

        DEFECT-S5-001 [P1] 修复: 提供提交性能监控

        Returns:
            提交统计信息
        """
        total = self._stats["total_commits"]
        successful = self._stats["successful_commits"]
        commit_rate = (successful / total * 100) if total > 0 else 0.0

        return {
            "total_commits": total,
            "successful_commits": successful,
            "failed_commits": self._stats["failed_commits"],
            "commit_timeouts": self._stats["commit_timeouts"],
            "commit_rate_percent": round(commit_rate, 2),
            "avg_commit_time_ms": round(self._stats["avg_commit_time"] * 1000, 2),
            "parallel_commit_enabled": self._parallel_commit,
            "max_concurrent_commits": self._max_concurrent_commits,
            "commit_timeout": self._commit_timeout,
        }
    
    async def abort(self, tx_id: str) -> bool:
        """
        中止事务
        
        Args:
            tx_id: 事务ID
            
        Returns:
            是否中止成功
        """
        # DEFECT-DC-001 [P0]: 使用per-transaction锁保护并发安全
        lock = self._tx_locks.setdefault(tx_id, asyncio.Lock())
        async with lock:
            tx = await self.store.load_transaction(tx_id)
            if not tx:
                raise TransactionNotFoundError(f"Transaction {tx_id} not found")

            if tx.status in (TransactionStatus.COMMITTED, TransactionStatus.ABORTED):
                return True

            # DEFECT-DC-006 [P0]: COMMITTING状态不能被简单中止
            if tx.status == TransactionStatus.COMMITTING:
                logger.warning(
                    f"Cannot abort transaction {tx_id} in COMMITTING state, "
                    f"marking as UNKNOWN for manual recovery"
                )
                tx.status = TransactionStatus.UNKNOWN
                await self.store.save_transaction(tx)
                return False

            terminal_states = {
                TransactionStatus.COMMITTED,
                TransactionStatus.ABORTED,
                TransactionStatus.SAGA_COMPENSATED,
                TransactionStatus.TIMEOUT,
            }
            if tx.status in terminal_states:
                return True

            tx.status = TransactionStatus.ABORTING
            await self.store.save_transaction(tx)

            logger.info(f"Transaction {tx_id} aborting")

            # 向所有参与者发送中止请求
            abort_tasks = []
            for participant_id in tx._participants:
                participant = self._participants.get(participant_id)
                if participant:
                    task = participant.abort(tx_id, tx.context)
                    abort_tasks.append(task)

            if abort_tasks:
                await asyncio.gather(*abort_tasks, return_exceptions=True)

            tx.status = TransactionStatus.ABORTED
            await self.store.save_transaction(tx)

            logger.info(f"Transaction {tx_id} aborted")
            return True
    
    async def get_transaction_status(self, tx_id: str) -> Optional[TransactionStatus]:
        """获取事务状态"""
        tx = await self.store.load_transaction(tx_id)
        return tx.status if tx else None


class SagaOrchestrator:
    """
    Saga编排器
    
    实现Saga模式，通过补偿事务处理长事务
    """
    
    def __init__(
        self,
        store: Optional[TransactionStore] = None,
        parallel_execution: bool = False,
    ):
        self.store = store or InMemoryTransactionStore()
        self.parallel_execution = parallel_execution
        self._steps: List[SagaStep] = []
        self._on_failure: Optional[Callable[[Exception], Coroutine[Any, Any, None]]] = None
    
    def add_step(
        self,
        step_id: str,
        action: Callable[[TransactionContext], Coroutine[Any, Any, Any]],
        compensation: Callable[[TransactionContext, Any], Coroutine[Any, Any, Any]],
        max_retries: int = 3,
    ) -> "SagaOrchestrator":
        """
        添加Saga步骤
        
        Args:
            step_id: 步骤ID
            action: 执行动作
            compensation: 补偿动作
            max_retries: 最大重试次数
            
        Returns:
            self，支持链式调用
        """
        step = SagaStep(
            step_id=step_id,
            action=action,
            compensation=compensation,
            max_retries=max_retries,
        )
        self._steps.append(step)
        return self
    
    def on_failure(
        self,
        handler: Callable[[Exception], Coroutine[Any, Any, None]]
    ) -> "SagaOrchestrator":
        """
        设置失败处理器
        
        Args:
            handler: 失败时的回调函数
            
        Returns:
            self，支持链式调用
        """
        self._on_failure = handler
        return self
    
    async def execute(
        self,
        context: Optional[TransactionContext] = None,
        timeout: Optional[float] = None,
    ) -> Transaction:
        """
        执行Saga
        
        Args:
            context: 事务上下文
            timeout: 超时时间
            
        Returns:
            事务对象
        """
        context = context or TransactionContext()
        
        tx = Transaction(
            timeout_at=time.time() + timeout if timeout else None,
        )
        tx.status = TransactionStatus.ACTIVE
        await self.store.save_transaction(tx)
        
        logger.info(f"Saga {tx.tx_id} started with {len(self._steps)} steps")
        
        completed_steps: List[tuple] = []  # (step, result)
        
        try:
            for step in self._steps:
                if tx.is_timed_out():
                    raise TransactionTimeoutError(f"Saga {tx.tx_id} timed out")
                
                logger.info(f"Saga {tx.tx_id} executing step {step.step_id}")
                
                result = await step.execute(context)
                completed_steps.append((step, result))
                
                # 记录操作
                tx.record_operation({
                    "step_id": step.step_id,
                    "result": result,
                })
                await self.store.save_transaction(tx)
            
            tx.status = TransactionStatus.COMMITTED
            await self.store.save_transaction(tx)
            
            logger.info(f"Saga {tx.tx_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Saga {tx.tx_id} failed at step: {e}")
            
            tx.status = TransactionStatus.SAGA_COMPENSATING
            await self.store.save_transaction(tx)
            
            # 执行补偿
            await self._compensate(completed_steps, context)
            
            tx.status = TransactionStatus.SAGA_COMPENSATED
            await self.store.save_transaction(tx)
            
            if self._on_failure:
                await self._on_failure(e)
            
            raise SagaCompensationError(f"Saga failed and was compensated: {e}")
        
        return tx
    
    async def _compensate(
        self,
        completed_steps: List[tuple],
        context: TransactionContext,
    ):
        """执行补偿"""
        logger.info(f"Starting compensation for {len(completed_steps)} steps")
        
        for step, result in reversed(completed_steps):
            try:
                logger.info(f"Compensating step {step.step_id}")
                await step.compensate(context, result)
            except Exception as e:
                logger.error(f"Compensation failed for step {step.step_id}: {e}")
                # 继续补偿其他步骤


class DistributedTransactionManager:
    """
    分布式事务管理器
    
    统一管理2PC和Saga事务
    """
    
    def __init__(
        self,
        store: Optional[TransactionStore] = None,
        default_timeout: float = 30.0,
    ):
        self.store = store or InMemoryTransactionStore()
        self.default_timeout = default_timeout
        
        # 2PC协调器
        self._2pc = TwoPhaseCommitCoordinator(store=self.store, default_timeout=default_timeout)
        
        # 活跃事务
        self._active_transactions: Dict[str, Transaction] = {}
        self._lock = asyncio.Lock()
    
    async def start(self):
        """启动管理器"""
        await self._2pc.start()
        logger.info("DistributedTransactionManager started")
    
    async def stop(self):
        """停止管理器"""
        await self._2pc.stop()
        logger.info("DistributedTransactionManager stopped")
    
    async def begin_2pc_transaction(
        self,
        timeout: Optional[float] = None,
    ) -> Transaction:
        """
        开始2PC事务
        
        Args:
            timeout: 超时时间
            
        Returns:
            事务对象
        """
        return await self._2pc.begin_transaction(timeout)
    
    def register_2pc_participant(self, participant: Participant):
        """注册2PC参与者"""
        self._2pc.register_participant(participant)
    
    async def add_participant_to_2pc(self, tx_id: str, participant_id: str) -> bool:
        """添加参与者到2PC事务"""
        return await self._2pc.add_participant(tx_id, participant_id)
    
    async def prepare_2pc(self, tx_id: str) -> bool:
        """准备2PC事务"""
        return await self._2pc.prepare(tx_id)
    
    async def commit_2pc(self, tx_id: str) -> bool:
        """提交2PC事务"""
        return await self._2pc.commit(tx_id)
    
    async def abort_2pc(self, tx_id: str) -> bool:
        """中止2PC事务"""
        return await self._2pc.abort(tx_id)
    
    def create_saga(self) -> SagaOrchestrator:
        """
        创建Saga编排器
        
        Returns:
            Saga编排器
        """
        return SagaOrchestrator(store=self.store)
    
    async def execute_saga(
        self,
        saga: SagaOrchestrator,
        context: Optional[TransactionContext] = None,
        timeout: Optional[float] = None,
    ) -> Transaction:
        """
        执行Saga
        
        Args:
            saga: Saga编排器
            context: 事务上下文
            timeout: 超时时间
            
        Returns:
            事务对象
        """
        return await saga.execute(context, timeout)
    
    async def get_transaction_status(self, tx_id: str) -> Optional[TransactionStatus]:
        """获取事务状态"""
        tx = await self.store.load_transaction(tx_id)
        return tx.status if tx else None
    
    async def get_transaction(self, tx_id: str) -> Optional[Transaction]:
        """获取事务"""
        return await self.store.load_transaction(tx_id)


# 测试代码
if __name__ == "__main__":
    async def test_2pc():
        """测试2PC"""
        logging.basicConfig(level=logging.INFO)
        
        manager = DistributedTransactionManager()
        await manager.start()
        
        # 创建模拟参与者
        async def prepare_handler(tx_id: str, context: TransactionContext) -> bool:
            logger.info(f"Participant preparing for tx {tx_id}")
            return True
        
        async def commit_handler(tx_id: str, context: TransactionContext) -> bool:
            logger.info(f"Participant committing tx {tx_id}")
            return True
        
        async def abort_handler(tx_id: str, context: TransactionContext) -> bool:
            logger.info(f"Participant aborting tx {tx_id}")
            return True
        
        # 注册参与者
        for i in range(3):
            participant = Participant(
                participant_id=f"participant_{i}",
                resource_id=f"resource_{i}",
                prepare_handler=prepare_handler,
                commit_handler=commit_handler,
                abort_handler=abort_handler,
            )
            manager.register_2pc_participant(participant)
        
        # 开始事务
        tx = await manager.begin_2pc_transaction(timeout=10.0)
        
        # 添加参与者
        for i in range(3):
            await manager.add_participant_to_2pc(tx.tx_id, f"participant_{i}")
        
        # 准备阶段
        prepared = await manager.prepare_2pc(tx.tx_id)
        print(f"Prepared: {prepared}")
        
        # 提交
        if prepared:
            committed = await manager.commit_2pc(tx.tx_id)
            print(f"Committed: {committed}")
        
        await manager.stop()
    
    async def test_saga():
        """测试Saga"""
        logging.basicConfig(level=logging.INFO)
        
        manager = DistributedTransactionManager()
        await manager.start()
        
        # 创建Saga步骤
        async def reserve_inventory(context: TransactionContext) -> Any:
            logger.info("Reserving inventory")
            context.set("inventory_reserved", True)
            return {"item_id": "item_1", "quantity": 1}
        
        async def release_inventory(context: TransactionContext, result: Any):
            logger.info(f"Releasing inventory: {result}")
            context.set("inventory_reserved", False)
        
        async def process_payment(context: TransactionContext) -> Any:
            logger.info("Processing payment")
            # 模拟失败
            if context.get("should_fail"):
                raise Exception("Payment failed")
            context.set("payment_processed", True)
            return {"payment_id": "pay_123", "amount": 100}
        
        async def refund_payment(context: TransactionContext, result: Any):
            logger.info(f"Refunding payment: {result}")
            context.set("payment_processed", False)
        
        async def ship_order(context: TransactionContext) -> Any:
            logger.info("Shipping order")
            context.set("order_shipped", True)
            return {"tracking_id": "track_456"}
        
        async def cancel_shipment(context: TransactionContext, result: Any):
            logger.info(f"Cancelling shipment: {result}")
            context.set("order_shipped", False)
        
        # 创建Saga
        saga = manager.create_saga()
        saga.add_step("reserve_inventory", reserve_inventory, release_inventory)
        saga.add_step("process_payment", process_payment, refund_payment)
        saga.add_step("ship_order", ship_order, cancel_shipment)
        
        # 测试成功场景
        print("\n=== Testing successful Saga ===")
        context = TransactionContext()
        try:
            tx = await manager.execute_saga(saga, context)
            print(f"Saga completed: {tx.status}")
        except Exception as e:
            print(f"Saga failed: {e}")
        
        # 测试失败场景
        print("\n=== Testing failing Saga ===")
        saga2 = manager.create_saga()
        saga2.add_step("reserve_inventory", reserve_inventory, release_inventory)
        saga2.add_step("process_payment", process_payment, refund_payment)
        saga2.add_step("ship_order", ship_order, cancel_shipment)
        
        context2 = TransactionContext()
        context2.set("should_fail", True)
        try:
            tx = await manager.execute_saga(saga2, context2)
            print(f"Saga completed: {tx.status}")
        except Exception as e:
            print(f"Saga failed as expected: {e}")
        
        await manager.stop()
    
    # 运行测试
    print("Testing 2PC...")
    asyncio.run(test_2pc())
    
    print("\nTesting Saga...")
    asyncio.run(test_saga())

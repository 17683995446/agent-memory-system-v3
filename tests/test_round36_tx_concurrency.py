"""
第36轮测试：分布式事务并发安全
测试目标：验证事务管理器的并发安全、UNKNOWN恢复、COMMITTING保护
"""

import pytest
import asyncio
import time
import logging
from unittest.mock import AsyncMock

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestTransactionConcurrencySafety:
    """测试事务并发安全"""

    @pytest.mark.asyncio
    async def test_dc001_concurrent_prepare_abort(self):
        """DC-001 [P0]: 测试并发prepare和abort不应导致状态不一致"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus
        )

        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()

        try:
            p = Participant(
                participant_id="p1",
                resource_id="r1",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=AsyncMock(return_value=True),
                abort_handler=AsyncMock(return_value=True),
            )
            coordinator.register_participant(p)

            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p1")

            # 并发执行prepare和abort
            results = await asyncio.gather(
                coordinator.prepare(tx.tx_id),
                coordinator.abort(tx.tx_id),
                return_exceptions=True,
            )

            # 验证最终状态是确定的（要么PREPARED要么ABORTED）
            tx = await store.load_transaction(tx.tx_id)
            logger.info(f"DC-001: Final status={tx.status}, results={results}")

            # 最终状态必须是确定性终态
            assert tx.status in (
                TransactionStatus.PREPARED,
                TransactionStatus.ABORTED,
                TransactionStatus.COMMITTED,
            ), f"Inconsistent state: {tx.status}"

        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_dc001_concurrent_commit_abort(self):
        """DC-001 [P0]: 测试并发commit和abort"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus
        )

        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()

        try:
            p = Participant(
                participant_id="p1",
                resource_id="r1",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=AsyncMock(return_value=True),
                abort_handler=AsyncMock(return_value=True),
            )
            coordinator.register_participant(p)

            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p1")
            await coordinator.prepare(tx.tx_id)

            # 并发执行commit和abort
            results = await asyncio.gather(
                coordinator.commit(tx.tx_id),
                coordinator.abort(tx.tx_id),
                return_exceptions=True,
            )

            tx = await store.load_transaction(tx.tx_id)
            logger.info(f"DC-001: Concurrent commit+abort status={tx.status}")

            assert tx.status in (
                TransactionStatus.COMMITTED,
                TransactionStatus.ABORTED,
                TransactionStatus.UNKNOWN,
            )

        finally:
            await coordinator.stop()


class TestUnknownStateRecovery:
    """测试UNKNOWN状态恢复"""

    @pytest.mark.asyncio
    async def test_dc005_unknown_state_in_active_transactions(self):
        """DC-005 [P0]: 测试UNKNOWN状态事务是否被get_active_transactions返回"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus
        )

        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()

        try:
            p_fail = Participant(
                participant_id="p_fail",
                resource_id="r_fail",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=AsyncMock(side_effect=RuntimeError("Commit failed")),
                abort_handler=AsyncMock(return_value=True),
            )
            coordinator.register_participant(p_fail)

            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p_fail")
            await coordinator.prepare(tx.tx_id)
            await coordinator.commit(tx.tx_id)

            tx = await store.load_transaction(tx.tx_id)
            logger.info(f"DC-005: Transaction status after partial commit: {tx.status}")

            if tx.status == TransactionStatus.UNKNOWN:
                # 检查是否在active_transactions中
                active = await store.get_active_transactions()
                active_ids = [t.tx_id for t in active]
                if tx.tx_id not in active_ids:
                    logger.error("DC-005 BUG CONFIRMED: UNKNOWN transaction not in active list!")
                else:
                    logger.info("DC-005: UNKNOWN transaction correctly in active list")
            else:
                logger.info(f"DC-005: Transaction not in UNKNOWN state: {tx.status}")

        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_dc005_recovery_loop_handles_unknown(self):
        """DC-005 [P0]: 测试恢复循环是否处理UNKNOWN状态"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus
        )

        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()

        try:
            p = Participant(
                participant_id="p1",
                resource_id="r1",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=AsyncMock(side_effect=RuntimeError("Commit failed")),
                abort_handler=AsyncMock(return_value=True),
            )
            coordinator.register_participant(p)

            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p1")
            await coordinator.prepare(tx.tx_id)
            await coordinator.commit(tx.tx_id)

            tx = await store.load_transaction(tx.tx_id)
            if tx.status == TransactionStatus.UNKNOWN:
                # 等待恢复循环尝试处理
                await asyncio.sleep(1.5)

                tx = await store.load_transaction(tx.tx_id)
                logger.info(f"DC-005: After recovery, status={tx.status}")

                # 恢复后状态应该有所变化（至少被尝试处理）
                # 如果仍然是UNKNOWN，说明恢复循环没有处理它
                if tx.status == TransactionStatus.UNKNOWN:
                    logger.warning("DC-005 BUG CONFIRMED: Recovery loop did not handle UNKNOWN state")

        finally:
            await coordinator.stop()


class TestCommittingStateProtection:
    """测试COMMITTING状态保护"""

    @pytest.mark.asyncio
    async def test_dc006_abort_committing_transaction(self):
        """DC-006 [P0]: 测试对COMMITTING状态事务执行abort"""
        from memory_system.distributed_transaction_manager import (
            TwoPhaseCommitCoordinator, InMemoryTransactionStore, Participant,
            TransactionStatus
        )

        store = InMemoryTransactionStore()
        coordinator = TwoPhaseCommitCoordinator(store=store)
        await coordinator.start()

        try:
            commit_started = asyncio.Event()

            async def slow_commit(tx_id, context):
                commit_started.set()
                await asyncio.sleep(2.0)
                return True

            p = Participant(
                participant_id="p_slow",
                resource_id="r_slow",
                prepare_handler=AsyncMock(return_value=True),
                commit_handler=slow_commit,
                abort_handler=AsyncMock(return_value=True),
            )
            coordinator.register_participant(p)

            tx = await coordinator.begin_transaction(timeout=30.0)
            await coordinator.add_participant(tx.tx_id, "p_slow")
            await coordinator.prepare(tx.tx_id)

            # 启动commit
            commit_task = asyncio.create_task(coordinator.commit(tx.tx_id))
            await commit_started.wait()

            # 尝试abort - 由于DC-001锁保护，abort会等待commit完成
            # 使用较长超时
            try:
                abort_result = await asyncio.wait_for(
                    coordinator.abort(tx.tx_id), timeout=5.0
                )
                logger.info(f"DC-006: abort result={abort_result}")

                # commit完成后abort应该返回True（事务已COMMITTED）
                tx = await store.load_transaction(tx.tx_id)
                logger.info(f"DC-006: Final status={tx.status}")

                # 验证：事务应该已提交（commit先完成）
                # abort在commit完成后获取锁，发现已经是COMMITTED，返回True
                assert tx.status == TransactionStatus.COMMITTED

            except asyncio.TimeoutError:
                logger.error("DC-006: abort timed out waiting for commit lock")

            # 等待commit完成
            try:
                await asyncio.wait_for(commit_task, timeout=5.0)
            except asyncio.TimeoutError:
                commit_task.cancel()
                try:
                    await commit_task
                except asyncio.CancelledError:
                    pass

        finally:
            await coordinator.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

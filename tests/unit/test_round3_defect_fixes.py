"""
第3轮缺陷修复单元测试

测试以下缺陷修复:
- R3-S4-001: 2PC事务一致性失败
- R3-S5-003: 权限绕过漏洞
- R3-S5-004: ABAC策略绕过漏洞
"""

import asyncio
import pytest
import sys
import os
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from memory_system.distributed_transaction_manager import (
    TwoPhaseCommitCoordinator,
    Participant,
    TransactionContext,
    TransactionStatus,
    ParticipantStatus,
)
from memory_system.security_hardening import (
    RBACManager,
    ABACManager,
    Permission,
    Role,
)


class TestTwoPhaseCommitConsistency:
    """测试2PC事务一致性修复 (R3-S4-001)"""

    @pytest.fixture
    async def coordinator(self):
        """创建2PC协调器"""
        coord = TwoPhaseCommitCoordinator()
        await coord.start()
        yield coord
        await coord.stop()

    @pytest.mark.asyncio
    async def test_all_participants_committed_consistently(self):
        """测试所有参与者一致提交"""
        coordinator = TwoPhaseCommitCoordinator()
        await coordinator.start()

        try:
            # 创建参与者
            results = {"prepared": [], "committed": []}

            async def prepare_handler(tx_id, ctx):
                results["prepared"].append(tx_id)
                return True

            async def commit_handler(tx_id, ctx):
                results["committed"].append(tx_id)
                return True

            async def abort_handler(tx_id, ctx):
                return True

            # 注册3个参与者
            for i in range(3):
                participant = Participant(
                    participant_id=f"p{i}",
                    resource_id=f"r{i}",
                    prepare_handler=prepare_handler,
                    commit_handler=commit_handler,
                    abort_handler=abort_handler,
                )
                coordinator.register_participant(participant)

            # 开始事务
            tx = await coordinator.begin_transaction(timeout=10.0)

            # 添加参与者
            for i in range(3):
                await coordinator.add_participant(tx.tx_id, f"p{i}")

            # 准备阶段
            prepared = await coordinator.prepare(tx.tx_id)
            assert prepared is True, "准备阶段应该成功"

            # 验证所有参与者都已准备好
            tx = await coordinator.store.load_transaction(tx.tx_id)
            assert tx.all_prepared() is True, "所有参与者应该都已准备好"

            # 提交阶段
            committed = await coordinator.commit(tx.tx_id)
            assert committed is True, "提交应该成功"

            # 验证所有参与者都已提交
            assert len(results["committed"]) == 3, f"所有3个参与者都应该提交，实际只有 {len(results['committed'])}"

            # 验证事务状态
            status = await coordinator.get_transaction_status(tx.tx_id)
            assert status == TransactionStatus.COMMITTED, f"事务状态应该是COMMITTED，实际是 {status}"

        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_commit_fails_if_not_all_prepared(self):
        """测试如果并非所有参与者都准备好，提交应该失败"""
        coordinator = TwoPhaseCommitCoordinator()
        await coordinator.start()

        try:
            # 创建一个会失败的参与者
            async def failing_prepare(tx_id, ctx):
                return False

            async def commit_handler(tx_id, ctx):
                return True

            async def abort_handler(tx_id, ctx):
                return True

            # 注册参与者
            coordinator.register_participant(Participant(
                participant_id="p1",
                resource_id="r1",
                prepare_handler=failing_prepare,
                commit_handler=commit_handler,
                abort_handler=abort_handler,
            ))

            coordinator.register_participant(Participant(
                participant_id="p2",
                resource_id="r2",
                prepare_handler=lambda tx, ctx: True,
                commit_handler=commit_handler,
                abort_handler=abort_handler,
            ))

            # 开始事务
            tx = await coordinator.begin_transaction(timeout=10.0)
            await coordinator.add_participant(tx.tx_id, "p1")
            await coordinator.add_participant(tx.tx_id, "p2")

            # 准备阶段应该失败
            prepared = await coordinator.prepare(tx.tx_id)
            assert prepared is False, "准备阶段应该失败"

            # 验证事务状态为ABORTED
            status = await coordinator.get_transaction_status(tx.tx_id)
            assert status == TransactionStatus.ABORTED, f"事务状态应该是ABORTED，实际是 {status}"

        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_commit_fails_if_participant_commit_fails(self):
        """测试如果任何参与者提交失败，整个事务应该失败"""
        coordinator = TwoPhaseCommitCoordinator()
        await coordinator.start()

        try:
            committed_count = [0]

            async def prepare_handler(tx_id, ctx):
                return True

            async def commit_handler(tx_id, ctx):
                committed_count[0] += 1
                return True

            async def failing_commit(tx_id, ctx):
                return False

            async def abort_handler(tx_id, ctx):
                return True

            # 注册参与者 - 其中一个提交会失败
            coordinator.register_participant(Participant(
                participant_id="p1",
                resource_id="r1",
                prepare_handler=prepare_handler,
                commit_handler=commit_handler,
                abort_handler=abort_handler,
            ))

            coordinator.register_participant(Participant(
                participant_id="p2",
                resource_id="r2",
                prepare_handler=prepare_handler,
                commit_handler=failing_commit,
                abort_handler=abort_handler,
            ))

            # 开始事务
            tx = await coordinator.begin_transaction(timeout=10.0)
            await coordinator.add_participant(tx.tx_id, "p1")
            await coordinator.add_participant(tx.tx_id, "p2")

            # 准备阶段
            prepared = await coordinator.prepare(tx.tx_id)
            assert prepared is True, "准备阶段应该成功"

            # 提交阶段应该失败
            committed = await coordinator.commit(tx.tx_id)
            assert committed is False, "提交应该失败因为p2提交失败"

            # 验证事务状态
            status = await coordinator.get_transaction_status(tx.tx_id)
            assert status == TransactionStatus.ABORTED, f"事务状态应该是ABORTED，实际是 {status}"

        finally:
            await coordinator.stop()


class TestRBACPermissionBypassFix:
    """测试RBAC权限绕过漏洞修复 (R3-S5-003)"""

    def test_permission_check_strict_role_enforcement(self):
        """测试权限检查严格按角色配置执行"""
        rbac = RBACManager()

        # 创建不同角色的用户
        guest_user = rbac.create_user("guest_user", [Role.GUEST])
        normal_user = rbac.create_user("normal_user", [Role.USER])
        moderator_user = rbac.create_user("moderator_user", [Role.MODERATOR])
        admin_user = rbac.create_user("admin_user", [Role.ADMIN])

        # 测试GUEST权限 - 只有READ
        assert rbac.check_permission(guest_user.user_id, Permission.READ) is True
        assert rbac.check_permission(guest_user.user_id, Permission.WRITE) is False
        assert rbac.check_permission(guest_user.user_id, Permission.DELETE) is False
        assert rbac.check_permission(guest_user.user_id, Permission.EXECUTE) is False
        assert rbac.check_permission(guest_user.user_id, Permission.ADMIN) is False

        # 测试USER权限 - READ + WRITE
        assert rbac.check_permission(normal_user.user_id, Permission.READ) is True
        assert rbac.check_permission(normal_user.user_id, Permission.WRITE) is True
        assert rbac.check_permission(normal_user.user_id, Permission.DELETE) is False
        assert rbac.check_permission(normal_user.user_id, Permission.EXECUTE) is False

        # 测试MODERATOR权限 - READ + WRITE + DELETE
        assert rbac.check_permission(moderator_user.user_id, Permission.READ) is True
        assert rbac.check_permission(moderator_user.user_id, Permission.WRITE) is True
        assert rbac.check_permission(moderator_user.user_id, Permission.DELETE) is True
        assert rbac.check_permission(moderator_user.user_id, Permission.EXECUTE) is False

        # 测试ADMIN权限 - 所有权限
        assert rbac.check_permission(admin_user.user_id, Permission.READ) is True
        assert rbac.check_permission(admin_user.user_id, Permission.WRITE) is True
        assert rbac.check_permission(admin_user.user_id, Permission.DELETE) is True
        assert rbac.check_permission(admin_user.user_id, Permission.EXECUTE) is True
        assert rbac.check_permission(admin_user.user_id, Permission.ADMIN) is True

    def test_resource_specific_permissions(self):
        """测试资源特定权限优先于默认权限"""
        rbac = RBACManager()

        # 创建用户
        user = rbac.create_user("test_user", [Role.USER])

        # 默认情况下USER有READ和WRITE权限
        assert rbac.check_permission(user.user_id, Permission.READ, "resource1") is True
        assert rbac.check_permission(user.user_id, Permission.WRITE, "resource1") is True

        # 设置资源特定权限 - 只允许READ
        rbac.set_resource_permission("resource1", Role.USER, [Permission.READ])

        # 现在WRITE应该被拒绝
        assert rbac.check_permission(user.user_id, Permission.READ, "resource1") is True
        assert rbac.check_permission(user.user_id, Permission.WRITE, "resource1") is False

    def test_inactive_user_denied(self):
        """测试非活动用户被拒绝访问"""
        rbac = RBACManager()

        # 创建用户
        user = rbac.create_user("test_user", [Role.ADMIN])

        # 初始状态应该可以访问
        assert rbac.check_permission(user.user_id, Permission.ADMIN) is True

        # 禁用用户
        with rbac._lock:
            rbac._users[user.user_id].is_active = False

        # 现在应该被拒绝
        assert rbac.check_permission(user.user_id, Permission.ADMIN) is False

    def test_multiple_roles_permission_accumulation(self):
        """测试多角色权限累积"""
        rbac = RBACManager()

        # 创建有多个角色的用户
        user = rbac.create_user("multi_role_user", [Role.USER])
        rbac.assign_role(user.user_id, Role.MODERATOR)

        # USER有READ+WRITE，MODERATOR有READ+WRITE+DELETE
        # 合并后应该有READ+WRITE+DELETE
        assert rbac.check_permission(user.user_id, Permission.READ) is True
        assert rbac.check_permission(user.user_id, Permission.WRITE) is True
        assert rbac.check_permission(user.user_id, Permission.DELETE) is True
        assert rbac.check_permission(user.user_id, Permission.EXECUTE) is False


class TestABACPolicyBypassFix:
    """测试ABAC策略绕过漏洞修复 (R3-S5-004)"""

    def test_abac_allow_policy(self):
        """测试ABAC允许策略正确执行"""
        abac = ABACManager()

        # 添加允许策略
        abac.add_policy(
            name="allow_finance_read",
            subject_attrs={"department": "finance"},
            resource_attrs={"type": "report"},
            action="read",
            effect="allow"
        )

        # 测试合规访问
        subject = {"department": "finance", "user_id": "user1"}
        resource = {"type": "report", "id": "doc1"}
        allowed, policy = abac.evaluate(subject, resource, "read")

        assert allowed is True, "财务部门用户应该被允许读取报告"
        assert policy == "allow_finance_read", f"应该匹配allow_finance_read策略，实际是 {policy}"

    def test_abac_deny_policy_priority(self):
        """测试ABAC拒绝策略优先于允许策略"""
        abac = ABACManager()

        # 添加允许策略
        abac.add_policy(
            name="allow_all_read",
            subject_attrs={},
            resource_attrs={},
            action="read",
            effect="allow"
        )

        # 添加拒绝策略 (更具体的规则)
        abac.add_policy(
            name="deny_guest_sensitive",
            subject_attrs={"role": "guest"},
            resource_attrs={"sensitivity": "high"},
            action="read",
            effect="deny"
        )

        # 测试普通用户访问 - 应该允许
        subject1 = {"role": "user", "user_id": "user1"}
        resource1 = {"sensitivity": "high", "id": "doc1"}
        allowed1, policy1 = abac.evaluate(subject1, resource1, "read")
        assert allowed1 is True, "普通用户应该被允许访问"

        # 测试访客访问敏感资源 - 应该拒绝
        subject2 = {"role": "guest", "user_id": "guest1"}
        resource2 = {"sensitivity": "high", "id": "doc2"}
        allowed2, policy2 = abac.evaluate(subject2, resource2, "read")
        assert allowed2 is False, "访客应该被拒绝访问敏感资源"
        assert policy2 == "deny_guest_sensitive", f"应该匹配deny_guest_sensitive策略，实际是 {policy2}"

    def test_abac_condition_evaluation(self):
        """测试ABAC条件评估"""
        abac = ABACManager()

        # 添加带条件的策略 (仅工作时间允许)
        abac.add_policy(
            name="business_hours_only",
            subject_attrs={"department": "finance"},
            resource_attrs={"type": "sensitive"},
            action="read",
            condition=lambda s, r, e: 9 <= datetime.utcnow().hour < 17,
            effect="allow"
        )

        subject = {"department": "finance", "user_id": "user1"}
        resource = {"type": "sensitive", "id": "doc1"}

        # 评估策略
        allowed, policy = abac.evaluate(subject, resource, "read")

        # 根据当前时间判断结果
        current_hour = datetime.utcnow().hour
        if 9 <= current_hour < 17:
            assert allowed is True, "工作时间应该允许访问"
        else:
            assert allowed is False, "非工作时间应该拒绝访问"

    def test_abac_default_deny(self):
        """测试ABAC默认拒绝"""
        abac = ABACManager()

        # 添加特定策略
        abac.add_policy(
            name="allow_admin_only",
            subject_attrs={"role": "admin"},
            resource_attrs={},
            action="delete",
            effect="allow"
        )

        # 测试未授权访问
        subject = {"role": "user", "user_id": "user1"}
        resource = {"id": "doc1"}
        allowed, policy = abac.evaluate(subject, resource, "delete")

        assert allowed is False, "非管理员应该被拒绝删除权限"
        assert policy is None, "没有匹配策略时policy应该为None"

    def test_abac_action_wildcard(self):
        """测试ABAC动作通配符"""
        abac = ABACManager()

        # 添加通配符策略
        abac.add_policy(
            name="super_admin_all",
            subject_attrs={"role": "super_admin"},
            resource_attrs={},
            action="*",
            effect="allow"
        )

        subject = {"role": "super_admin", "user_id": "admin1"}
        resource = {"id": "doc1"}

        # 测试各种动作
        for action in ["read", "write", "delete", "execute", "admin"]:
            allowed, policy = abac.evaluate(subject, resource, action)
            assert allowed is True, f"超级管理员应该被允许执行 {action}"

    def test_abac_multiple_matching_policies(self):
        """测试多个匹配策略的处理"""
        abac = ABACManager()

        # 添加多个策略
        abac.add_policy(
            name="allow_department_access",
            subject_attrs={"department": "engineering"},
            resource_attrs={},
            action="read",
            effect="allow"
        )

        abac.add_policy(
            name="allow_role_access",
            subject_attrs={"role": "developer"},
            resource_attrs={},
            action="read",
            effect="allow"
        )

        abac.add_policy(
            name="deny_restricted_docs",
            subject_attrs={},
            resource_attrs={"restricted": True},
            action="read",
            effect="deny"
        )

        # 测试匹配多个允许策略
        subject = {"department": "engineering", "role": "developer", "user_id": "user1"}
        resource = {"id": "doc1", "restricted": False}
        allowed, policy = abac.evaluate(subject, resource, "read")
        assert allowed is True, "应该允许访问"

        # 测试同时匹配允许和拒绝策略
        resource_restricted = {"id": "doc2", "restricted": True}
        allowed2, policy2 = abac.evaluate(subject, resource_restricted, "read")
        assert allowed2 is False, "应该拒绝访问受限文档"
        assert policy2 == "deny_restricted_docs"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

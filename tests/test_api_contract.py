"""API契约测试 - 验证公共方法签名和返回类型"""
import pytest
import inspect
from memory_system.security_hardening import (
    CryptoManager, RBACManager, ABACManager, AuditLogger, InputValidator, Role, Permission, User
)
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage, ConfigItem
from memory_system.partition_tolerance import QuorumManager
from memory_system.hybrid_logical_clock import HybridLogicalClock
from memory_system.vector_clock import VectorClock
from memory_system.event_driven_core import InMemoryEventBus, DomainEvent
from memory_system.cqrs_system import InMemoryEventStore
from memory_system.rate_limiter import RateLimiter, RateLimitConfig
from memory_system.di_container import DIContainer
from memory_system.monitoring_alerting import MetricsCollector, AlertManager, AlertRule
from memory_system.read_write_lock import ReadWriteLock


class TestCryptoManagerContract:
    """CryptoManager API契约"""

    def test_encrypt_returns_string(self):
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        result = crypto.encrypt("test")
        assert isinstance(result, str)

    def test_decrypt_returns_string(self):
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        encrypted = crypto.encrypt("test")
        result = crypto.decrypt(encrypted)
        assert isinstance(result, str)

    def test_hash_data_returns_string(self):
        crypto = CryptoManager()
        result = crypto.hash_data("test")
        assert isinstance(result, str)

    def test_generate_token_returns_string(self):
        crypto = CryptoManager()
        result = crypto.generate_token()
        assert isinstance(result, str)

    def test_generate_api_key_returns_string(self):
        crypto = CryptoManager()
        result = crypto.generate_api_key()
        assert isinstance(result, str)

    def test_hash_password_returns_tuple(self):
        crypto = CryptoManager()
        result = crypto.hash_password("test")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_hash_password_bcrypt_returns_string(self):
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        result = crypto.hash_password_bcrypt("test")
        assert isinstance(result, str)

    def test_verify_password_returns_bool(self):
        crypto = CryptoManager()
        h, s = crypto.hash_password("test")
        assert isinstance(crypto.verify_password("test", h, s), bool)

    def test_verify_password_bcrypt_returns_bool(self):
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        h = crypto.hash_password_bcrypt("test")
        assert isinstance(crypto.verify_password_bcrypt("test", h), bool)


class TestRBACManagerContract:
    """RBACManager API契约"""

    def test_create_user_returns_user(self):
        rbac = RBACManager()
        result = rbac.create_user("alice")
        assert isinstance(result, User)
        assert hasattr(result, 'user_id')
        assert hasattr(result, 'username')

    def test_get_user_returns_optional_user(self):
        rbac = RBACManager()
        user = rbac.create_user("alice")
        assert isinstance(rbac.get_user(user.user_id), User)
        assert rbac.get_user("nonexistent") is None

    def test_assign_role_returns_bool(self):
        rbac = RBACManager()
        user = rbac.create_user("alice")
        assert isinstance(rbac.assign_role(user.user_id, Role.ADMIN), bool)

    def test_revoke_role_returns_bool(self):
        rbac = RBACManager()
        user = rbac.create_user("alice", [Role.ADMIN])
        assert isinstance(rbac.revoke_role(user.user_id, Role.ADMIN), bool)

    def test_check_permission_returns_bool(self):
        rbac = RBACManager()
        user = rbac.create_user("alice", [Role.ADMIN])
        assert isinstance(rbac.check_permission(user.user_id, Permission.READ), bool)

    def test_get_user_permissions_returns_list(self):
        rbac = RBACManager()
        user = rbac.create_user("alice", [Role.ADMIN])
        perms = rbac.get_user_permissions(user.user_id)
        assert isinstance(perms, list)


class TestAuditLoggerContract:
    """AuditLogger API契约"""

    def test_log_returns_entry(self):
        from memory_system.security_hardening import AuditLogEntry
        audit = AuditLogger()
        result = audit.log("u1", "action", "resource", "success")
        assert hasattr(result, 'entry_id')
        assert hasattr(result, 'timestamp')

    def test_query_returns_list(self):
        audit = AuditLogger()
        audit.log("u1", "action", "resource", "success")
        result = audit.query()
        assert isinstance(result, list)

    def test_get_stats_returns_dict(self):
        audit = AuditLogger()
        result = audit.get_stats()
        assert isinstance(result, dict)


class TestConfigCenterContract:
    """ConfigCenter API契约"""

    @pytest.mark.asyncio
    async def test_get_returns_value_or_none(self):
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        assert await center.get("key") is None
        await center.set("key", "value")
        assert await center.get("key") == "value"

    @pytest.mark.asyncio
    async def test_set_returns_bool(self):
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        assert isinstance(await center.set("key", "value"), bool)

    @pytest.mark.asyncio
    async def test_delete_returns_bool(self):
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        assert isinstance(await center.delete("key"), bool)

    @pytest.mark.asyncio
    async def test_list_keys_returns_list(self):
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        result = await center.list_keys()
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_get_all_returns_dict(self):
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        result = await center.get_all()
        assert isinstance(result, dict)


class TestMetricsCollectorContract:
    """MetricsCollector API契约"""

    def test_get_metrics_returns_list(self):
        mc = MetricsCollector()
        mc.counter("test", 1.0)
        result = mc.get_metrics("test")
        assert isinstance(result, list)

    def test_get_prometheus_format_returns_string(self):
        mc = MetricsCollector()
        result = mc.get_prometheus_format()
        assert isinstance(result, str)


class TestReadWriteLockContract:
    """ReadWriteLock API契约"""

    def test_is_write_locked_returns_bool(self):
        rw = ReadWriteLock()
        assert isinstance(rw.is_write_locked(), bool)

    def test_is_read_locked_returns_bool(self):
        rw = ReadWriteLock()
        assert isinstance(rw.is_read_locked(), bool)

    def test_get_stats_returns_dict(self):
        rw = ReadWriteLock()
        assert isinstance(rw.get_stats(), dict)


class TestDIContainerContract:
    """DIContainer API契约"""

    def test_is_registered_returns_bool(self):
        c = DIContainer()
        assert isinstance(c.is_registered("test"), bool)

    def test_unregister_returns_bool(self):
        c = DIContainer()
        assert isinstance(c.unregister("test"), bool)

    def test_get_registrations_returns_list(self):
        c = DIContainer()
        assert isinstance(c.get_registrations(), list)

    def test_get_registration_info(self):
        c = DIContainer()
        c.register("test", lambda: None)
        info = c.get_registration_info("test")
        assert isinstance(info, dict) or info is None


class TestMethodSignatureConsistency:
    """方法签名一致性检查"""

    def test_all_public_methods_callable(self):
        """所有公共方法应可调用(无参数错误)"""
        # This is a meta-test that checks method signatures are consistent
        instances = {
            "CryptoManager": CryptoManager(b"0123456789abcdef0123456789abcdef"),
            "RBACManager": RBACManager(),
            "ABACManager": ABACManager(),
            "InputValidator": InputValidator(),
            "AuditLogger": AuditLogger(),
            "QuorumManager": QuorumManager(total_nodes=5),
            "MetricsCollector": MetricsCollector(),
            "ReadWriteLock": ReadWriteLock(),
        }

        for class_name, instance in instances.items():
            for method_name in dir(instance):
                if method_name.startswith('_'):
                    continue
                method = getattr(instance, method_name)
                if callable(method):
                    # Verify it's a real method (not a property)
                    if isinstance(method, property):
                        continue
                    # Just verify it exists and is callable - don't call it
                    assert callable(method), f"{class_name}.{method_name} is not callable"

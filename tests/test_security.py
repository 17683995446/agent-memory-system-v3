"""安全模块单元测试"""
import pytest
import warnings
from memory_system.security_hardening import (
    SecurityHardeningModule, Permission, CryptoManager,
    RBACManager, ABACManager, AuditLogger, InputValidator, Role, User
)


class TestCryptoManager:
    """加密管理器测试"""

    def setup_method(self):
        self.crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")  # 32 bytes

    def test_encrypt_decrypt_roundtrip(self):
        """测试AES-GCM加密解密往返"""
        plaintext = "Hello, Agent Memory System! 你好世界"
        encrypted = self.crypto.encrypt(plaintext)
        decrypted = self.crypto.decrypt(encrypted)
        assert decrypted == plaintext

    def test_encrypt_produces_different_ciphertext(self):
        """同一明文两次加密产生不同密文(nonce随机)"""
        plaintext = "test data"
        ct1 = self.crypto.encrypt(plaintext)
        ct2 = self.crypto.encrypt(plaintext)
        assert ct1 != ct2

    def test_decrypt_tampered_data_raises(self):
        """解密被篡改的数据应抛出异常"""
        encrypted = self.crypto.encrypt("secret")
        tampered = encrypted[:-4] + "XXXX"
        with pytest.raises(Exception):
            self.crypto.decrypt(tampered)

    def test_simple_encrypt_deprecation_warning(self):
        """simple_encrypt应发出弃用警告"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.crypto.simple_encrypt("test")
            assert len(w) >= 1
            assert any(issubclass(warning.category, DeprecationWarning) for warning in w)

    def test_hash_data_sha256(self):
        """SHA-256哈希测试"""
        result = self.crypto.hash_data("test", "sha256")
        assert isinstance(result, str)
        assert len(result) == 64

    def test_hash_data_md5_deprecation(self):
        """MD5哈希应发出弃用警告"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.crypto.hash_data("test", "md5")
            assert any(issubclass(warning.category, DeprecationWarning) for warning in w)

    def test_hash_password_bcrypt(self):
        """bcrypt密码哈希测试"""
        hashed = self.crypto.hash_password_bcrypt("my_password")
        assert hashed != "my_password"
        assert self.crypto.verify_password_bcrypt("my_password", hashed)
        assert not self.crypto.verify_password_bcrypt("wrong_password", hashed)


class TestRBACManager:
    """RBAC权限管理测试"""

    def setup_method(self):
        self.rbac = RBACManager()
        self.user = self.rbac.create_user("Alice", [Role.ADMIN])
        self.user_id = self.user.user_id

    def test_check_permission_admin(self):
        """管理员应拥有所有权限"""
        assert self.rbac.check_permission(self.user_id, Permission.READ)
        assert self.rbac.check_permission(self.user_id, Permission.WRITE)
        assert self.rbac.check_permission(self.user_id, Permission.ADMIN)

    def test_check_permission_nonexistent_user(self):
        """不存在的用户应返回False"""
        assert not self.rbac.check_permission("nonexistent", Permission.READ)

    def test_check_permission_inactive_user(self):
        """非活跃用户应返回False"""
        self.user.is_active = False
        assert not self.rbac.check_permission(self.user_id, Permission.READ)

    def test_revoke_role(self):
        """撤销角色后权限应失效"""
        self.rbac.revoke_role(self.user_id, Role.ADMIN)
        self.rbac.assign_role(self.user_id, Role.USER)
        assert not self.rbac.check_permission(self.user_id, Permission.ADMIN)
        assert self.rbac.check_permission(self.user_id, Permission.READ)


class TestInputValidator:
    """输入验证器测试"""

    def setup_method(self):
        self.validator = InputValidator()

    def test_validate_email_valid(self):
        """有效邮箱验证"""
        assert self.validator.validate_email("user@example.com")
        assert self.validator.validate_email("test.user+tag@domain.co.uk")

    def test_validate_email_invalid(self):
        """无效邮箱验证"""
        assert not self.validator.validate_email("invalid")
        assert not self.validator.validate_email("@no-user.com")

    def test_detect_xss(self):
        """XSS检测"""
        is_threat, threats = self.validator.check_xss("<script>alert('xss')</script>")
        assert is_threat
        assert len(threats) > 0

    def test_detect_sql_injection(self):
        """SQL注入检测"""
        is_threat, threats = self.validator.check_sql_injection("'; DROP TABLE users; --")
        assert is_threat

    def test_escape_sql_deprecation(self):
        """escape_sql应发出弃用警告"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.validator.escape_sql("test")
            assert any(issubclass(warning.category, DeprecationWarning) for warning in w)


class TestAuditLogger:
    """审计日志测试"""

    def test_log_and_query(self):
        """记录和查询审计日志"""
        audit = AuditLogger()
        entry = audit.log(
            user_id="user1",
            action="login",
            resource="auth",
            result="success"
        )
        assert entry.user_id == "user1"
        assert entry.action == "login"

        results = audit.query(user_id="user1")
        assert len(results) >= 1

    def test_handler_notification(self):
        """处理器通知测试"""
        audit = AuditLogger()
        received = []
        audit.add_handler(lambda e: received.append(e))
        audit.log(user_id="u1", action="test", resource="r", result="ok")
        assert len(received) == 1

"""安全模块深度测试 - 边界条件/异常路径/并发安全"""
import pytest
import warnings
import threading
import time
from datetime import datetime
from memory_system.security_hardening import (
    CryptoManager, RBACManager, ABACManager, AuditLogger,
    InputValidator, Permission, Role, User
)


class TestCryptoManagerBoundary:
    """加密管理器边界测试"""
    
    def test_encrypt_empty_string(self):
        """加密空字符串"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        encrypted = crypto.encrypt("")
        assert crypto.decrypt(encrypted) == ""
    
    def test_encrypt_very_long_string(self):
        """加密超长字符串(1MB)"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        long_data = "A" * 1_000_000
        encrypted = crypto.encrypt(long_data)
        assert crypto.decrypt(encrypted) == long_data
    
    def test_encrypt_unicode_variety(self):
        """加密各种Unicode字符"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        test_strings = [
            "中文测试",
            "日本語テスト",
            "한국어테스트",
            "🎉🚀💻",
            "مرحبا",
            "Привет мир",
            "a" * 10000,
        ]
        for s in test_strings:
            encrypted = crypto.encrypt(s)
            assert crypto.decrypt(encrypted) == s, f"Failed for: {repr(s[:20])}"
    
    def test_encrypt_short_key_raises(self):
        """短密钥应抛出异常"""
        crypto = CryptoManager(b"short")  # Only 5 bytes
        with pytest.raises(RuntimeError):
            crypto.encrypt("test")
    
    def test_decrypt_invalid_base64_raises(self):
        """无效Base64应抛出异常"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        with pytest.raises(Exception):
            crypto.decrypt("not-valid-base64!!!")
    
    def test_decrypt_empty_string_raises(self):
        """解密空字符串应抛出异常"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        with pytest.raises(Exception):
            crypto.decrypt("")
    
    def test_decrypt_too_short_data_raises(self):
        """过短数据(少于nonce长度12字节)应抛出异常"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        with pytest.raises(Exception):
            crypto.decrypt("YWJj")  # Only 4 bytes base64
    
    def test_hash_data_unsupported_algorithm(self):
        """不支持的哈希算法应抛出异常"""
        crypto = CryptoManager()
        with pytest.raises(ValueError):
            crypto.hash_data("test", "unsupported_algo")
    
    def test_generate_token_length(self):
        """生成的token长度应正确 (token_urlsafe返回base64编码字符串)"""
        crypto = CryptoManager()
        for length in [16, 32, 64, 128]:
            token = crypto.generate_token(length)
            # token_urlsafe(n) returns base64-encoded string of ~n bytes,
            # so the actual string length is approximately ceil(n * 4/3)
            assert len(token) >= length
            assert len(token) < length * 2
    
    def test_generate_api_key_format(self):
        """API key应以ak_开头"""
        crypto = CryptoManager()
        key = crypto.generate_api_key()
        assert key.startswith("ak_")
        assert len(key) > 10
    
    def test_hash_password_returns_tuple(self):
        """hash_password应返回(hash, salt)元组"""
        crypto = CryptoManager()
        result = crypto.hash_password("test123")
        assert isinstance(result, tuple)
        assert len(result) == 2
        hash_val, salt = result
        assert isinstance(hash_val, str)
        assert isinstance(salt, bytes)
    
    def test_verify_password_wrong_password(self):
        """错误密码应返回False"""
        crypto = CryptoManager()
        hash_val, salt = crypto.hash_password("correct_password")
        assert not crypto.verify_password("wrong_password", hash_val, salt)
    
    def test_verify_password_correct_password(self):
        """正确密码应返回True"""
        crypto = CryptoManager()
        hash_val, salt = crypto.hash_password("my_password")
        assert crypto.verify_password("my_password", hash_val, salt)
    
    def test_verify_password_wrong_salt(self):
        """错误salt应返回False"""
        crypto = CryptoManager()
        hash_val, salt = crypto.hash_password("password")
        assert not crypto.verify_password("password", hash_val, b"wrong_salt_value")


class TestRBACManagerBoundary:
    """RBAC边界测试"""
    
    def setup_method(self):
        self.rbac = RBACManager()
    
    def test_create_user_duplicate_username(self):
        """重复用户名应返回不同user_id"""
        user1 = self.rbac.create_user("alice")
        user2 = self.rbac.create_user("alice")
        assert user1.user_id != user2.user_id
    
    def test_get_nonexistent_user(self):
        """获取不存在的用户应返回None"""
        assert self.rbac.get_user("nonexistent") is None
    
    def test_assign_role_nonexistent_user(self):
        """给不存在的用户分配角色应返回False"""
        assert not self.rbac.assign_role("nonexistent", Role.ADMIN)
    
    def test_revoke_role_nonexistent_user(self):
        """从不存在的用户撤销角色应返回False"""
        assert not self.rbac.revoke_role("nonexistent", Role.ADMIN)
    
    def test_assign_role_not_in_list(self):
        """分配不在列表中的角色应正常工作"""
        user = self.rbac.create_user("user1")
        # Assign a role not in initial list
        result = self.rbac.assign_role(user.user_id, Role.SUPER_ADMIN)
        assert result is True
        assert self.rbac.check_permission(user.user_id, Permission.ADMIN)
    
    def test_check_permission_after_revoke_all(self):
        """撤销所有角色后应无权限"""
        user = self.rbac.create_user("user1", [Role.ADMIN, Role.USER])
        self.rbac.revoke_role(user.user_id, Role.ADMIN)
        self.rbac.revoke_role(user.user_id, Role.USER)
        assert not self.rbac.check_permission(user.user_id, Permission.READ)
    
    def test_set_resource_permission_and_check(self):
        """资源级权限设置和检查"""
        user = self.rbac.create_user("user1", [Role.USER])
        self.rbac.set_resource_permission("secret_doc", Role.USER, [Permission.READ])
        # USER can read secret_doc
        assert self.rbac.check_permission(user.user_id, Permission.READ, resource="secret_doc")
        # USER cannot write secret_doc (only READ granted)
        assert not self.rbac.check_permission(user.user_id, Permission.WRITE, resource="secret_doc")
    
    def test_get_user_permissions(self):
        """获取用户权限列表"""
        user = self.rbac.create_user("user1", [Role.ADMIN])
        perms = self.rbac.get_user_permissions(user.user_id)
        assert isinstance(perms, list)
        assert Permission.ADMIN in perms
        assert Permission.READ in perms
    
    def test_concurrent_permission_checks(self):
        """并发权限检查应线程安全"""
        user = self.rbac.create_user("user1", [Role.ADMIN])
        errors = []
        
        def check_perms():
            try:
                for _ in range(100):
                    self.rbac.check_permission(user.user_id, Permission.READ)
                    self.rbac.check_permission(user.user_id, Permission.WRITE)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=check_perms) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0, f"Thread safety errors: {errors}"


class TestABACManagerBoundary:
    """ABAC边界测试"""
    
    def test_evaluate_no_policies(self):
        """无策略时应拒绝"""
        abac = ABACManager()
        result, reason = abac.evaluate(
            subject={"role": "user"},
            resource={"type": "document"},
            action="read"
        )
        assert result is False
    
    def test_evaluate_with_matching_policy(self):
        """匹配策略应允许"""
        abac = ABACManager()
        abac.add_policy(
            name="allow_admin_read",
            subject_attrs={"role": "admin"},
            resource_attrs={"type": "document"},
            action="read",
            effect="allow"
        )
        result, reason = abac.evaluate(
            subject={"role": "admin"},
            resource={"type": "document"},
            action="read"
        )
        assert result is True
    
    def test_evaluate_with_deny_policy(self):
        """拒绝策略应拒绝"""
        abac = ABACManager()
        abac.add_policy(
            name="deny_user_delete",
            subject_attrs={"role": "user"},
            resource_attrs={"type": "document"},
            action="delete",
            effect="deny"
        )
        result, reason = abac.evaluate(
            subject={"role": "user"},
            resource={"type": "document"},
            action="delete"
        )
        assert result is False
    
    def test_evaluate_with_custom_condition(self):
        """自定义条件函数"""
        abac = ABACManager()
        abac.add_policy(
            name="business_hours_only",
            subject_attrs={"department": "finance"},
            resource_attrs={"type": "report"},
            action="view",
            condition=lambda s, r, e: 9 <= e.get("hour", 0) <= 17,
            effect="allow"
        )
        # During business hours
        result, _ = abac.evaluate(
            subject={"department": "finance"},
            resource={"type": "report"},
            action="view",
            environment={"hour": 10}
        )
        assert result is True
        # Outside business hours
        result, _ = abac.evaluate(
            subject={"department": "finance"},
            resource={"type": "report"},
            action="view",
            environment={"hour": 20}
        )
        assert result is False


class TestInputValidatorBoundary:
    """输入验证边界测试"""
    
    def setup_method(self):
        self.validator = InputValidator()
    
    def test_validate_email_edge_cases(self):
        """邮箱验证边界情况"""
        valid = [
            "a@b.co",
            "user+tag@domain.com",
            "user.name@sub.domain.co.uk",
            "123@456.com",
        ]
        invalid = [
            "",
            " ",
            "@",
            "a@",
            "@b.com",
            "a b@c.com",
            "a@b@c.com",
        ]
        for email in valid:
            assert self.validator.validate_email(email), f"Should be valid: {email}"
        for email in invalid:
            assert not self.validator.validate_email(email), f"Should be invalid: {email}"
    
    def test_validate_url_edge_cases(self):
        """URL验证边界情况"""
        assert self.validator.validate_url("https://example.com")
        assert self.validator.validate_url("http://localhost:8080/path")
        assert not self.validator.validate_url("")
        assert not self.validator.validate_url("not a url")
    
    def test_sanitize_string_max_length(self):
        """字符串截断"""
        long_str = "A" * 2000
        result = self.validator.sanitize_string(long_str, max_length=100)
        assert len(result) == 100
    
    def test_sanitize_string_empty(self):
        """空字符串处理"""
        assert self.validator.sanitize_string("") == ""
    
    def test_validate_json_edge_cases(self):
        """JSON验证边界"""
        valid, data = self.validator.validate_json('{"key": "value"}')
        assert valid is True
        assert data["key"] == "value"
        
        valid, data = self.validator.validate_json("invalid")
        assert valid is False
        assert data is None
        
        valid, data = self.validator.validate_json("")
        assert valid is False
    
    def test_validate_uuid(self):
        """UUID验证"""
        import uuid
        assert self.validator.validate_uuid(str(uuid.uuid4()))
        assert not self.validator.validate_uuid("not-a-uuid")
        assert not self.validator.validate_uuid("")
    
    def test_check_xss_edge_cases(self):
        """XSS检测边界"""
        # Normal text should not be flagged
        is_threat, threats = self.validator.check_xss("Hello World")
        assert not is_threat
        
        # Various XSS patterns
        xss_patterns = [
            '<script>alert("xss")</script>',
            '<img src=x onerror=alert(1)>',
            '<svg onload=alert(1)>',
            'javascript:alert(1)',
            '<iframe src="evil.com">',
        ]
        for pattern in xss_patterns:
            is_threat, threats = self.validator.check_xss(pattern)
            assert is_threat, f"Should detect XSS in: {pattern}"
    
    def test_check_command_injection(self):
        """命令注入检测"""
        is_threat, threats = self.validator.check_command_injection("normal text")
        assert not is_threat
        
        injection_patterns = [
            "; rm -rf /",
            "| cat /etc/passwd",
            "$(whoami)",
            "`id`",
            "& ping -c 100 localhost",
        ]
        for pattern in injection_patterns:
            is_threat, threats = self.validator.check_command_injection(pattern)
            assert is_threat, f"Should detect injection in: {pattern}"


class TestAuditLoggerBoundary:
    """审计日志边界测试"""
    
    def test_log_with_none_user_id(self):
        """None user_id应正常记录"""
        audit = AuditLogger()
        entry = audit.log(None, "action", "resource", "success")
        assert entry.user_id is None
    
    def test_log_with_empty_details(self):
        """空details应正常记录"""
        audit = AuditLogger()
        entry = audit.log("u1", "action", "resource", "success", details={})
        assert entry.details == {}
    
    def test_query_with_no_filters(self):
        """无过滤条件查询"""
        audit = AuditLogger()
        for i in range(5):
            audit.log(f"u{i}", f"action_{i}", f"resource_{i}", "success")
        results = audit.query()
        assert len(results) == 5
    
    def test_query_with_limit(self):
        """限制查询数量"""
        audit = AuditLogger()
        for i in range(20):
            audit.log("u1", "action", "resource", "success")
        results = audit.query(limit=5)
        assert len(results) == 5
    
    def test_max_entries_limit(self):
        """最大条目限制"""
        audit = AuditLogger(max_entries=10)
        for i in range(20):
            audit.log("u1", "action", "resource", "success")
        stats = audit.get_stats()
        assert stats["total_entries"] <= 10
    
    def test_get_stats(self):
        """获取统计信息"""
        audit = AuditLogger()
        audit.log("u1", "login", "auth", "success")
        audit.log("u1", "login", "auth", "failure")
        stats = audit.get_stats()
        assert "total_entries" in stats
    
    def test_concurrent_logging(self):
        """并发日志记录应线程安全"""
        audit = AuditLogger()
        errors = []
        
        def log_entries():
            try:
                for i in range(50):
                    audit.log(f"u{i%5}", "action", "resource", "success")
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=log_entries) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0, f"Concurrent logging errors: {errors}"

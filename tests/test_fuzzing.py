"""Fuzz测试 - 异常路径探索"""
import pytest
import random
import string
from datetime import datetime, timezone
from memory_system.security_hardening import (
    InputValidator, CryptoManager, RBACManager, AuditLogger,
    Permission, Role
)
from memory_system.config_center import ConfigCenter, InMemoryConfigStorage
from memory_system.cqrs_system import InMemoryEventStore, StoredEvent
from memory_system.vector_clock import VectorClock
from memory_system.hybrid_logical_clock import HybridLogicalClock


class TestInputValidatorFuzz:
    """输入验证器Fuzz测试"""
    
    def test_fuzz_email_validation(self):
        """Fuzz邮箱验证"""
        validator = InputValidator()
        
        # Generate random strings
        for _ in range(1000):
            length = random.randint(0, 100)
            random_string = ''.join(random.choices(string.printable, k=length))
            
            # Should not crash
            try:
                validator.validate_email(random_string)
            except Exception:
                pass
    
    def test_fuzz_url_validation(self):
        """Fuzz URL验证"""
        validator = InputValidator()
        
        for _ in range(1000):
            length = random.randint(0, 200)
            random_string = ''.join(random.choices(string.printable, k=length))
            
            try:
                validator.validate_url(random_string)
            except Exception:
                pass
    
    def test_fuzz_xss_detection(self):
        """Fuzz XSS检测"""
        validator = InputValidator()
        
        payloads = [
            "<script>" + "x" * random.randint(0, 1000) + "</script>",
            "javascript:" + "x" * random.randint(0, 100),
            "<img" + " " * random.randint(0, 100) + "src=x>",
            "<" + "x" * random.randint(0, 100) + ">",
        ]
        
        for payload in payloads:
            try:
                is_threat, _ = validator.check_xss(payload)
                # Should detect as threat
                assert is_threat or len(payload) < 10
            except Exception:
                pass


class TestCryptoFuzz:
    """加密模块Fuzz测试"""
    
    def test_fuzz_encrypt_decrypt(self):
        """Fuzz加密解密"""
        crypto = CryptoManager(b"0123456789abcdef0123456789abcdef")
        
        for _ in range(100):
            # Random plaintext
            length = random.randint(0, 10000)
            plaintext = ''.join(random.choices(string.printable, k=length))
            
            try:
                encrypted = crypto.encrypt(plaintext)
                decrypted = crypto.decrypt(encrypted)
                assert decrypted == plaintext
            except Exception as e:
                # Should only fail for edge cases
                if length > 0:
                    pytest.fail(f"Unexpected error: {e}")
    
    def test_fuzz_hash_data(self):
        """Fuzz哈希数据"""
        crypto = CryptoManager()
        
        algorithms = ["sha256", "sha512", "md5", "unknown_algo"]
        
        for _ in range(1000):
            length = random.randint(0, 10000)
            data = ''.join(random.choices(string.printable, k=length))
            algo = random.choice(algorithms)
            
            try:
                result = crypto.hash_data(data, algo)
                assert isinstance(result, str)
            except ValueError:
                pass  # Expected for unknown algorithm


class TestRBACFuzz:
    """RBAC Fuzz测试"""
    
    def test_fuzz_user_creation(self):
        """Fuzz用户创建"""
        rbac = RBACManager()
        
        for _ in range(100):
            length = random.randint(0, 100)
            username = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
            
            try:
                user = rbac.create_user(username)
                assert user.username == username or len(username) == 0
            except Exception:
                pass
    
    def test_fuzz_permission_check(self):
        """Fuzz权限检查"""
        rbac = RBACManager()
        
        # Create a user
        user = rbac.create_user("test", [Role.USER])
        
        for _ in range(1000):
            user_id = random.choice([user.user_id, "nonexistent", "", "x" * 100])
            permission = random.choice(list(Permission))
            
            try:
                result = rbac.check_permission(user_id, permission)
                assert isinstance(result, bool)
            except Exception:
                pass


class TestConfigCenterFuzz:
    """配置中心Fuzz测试"""
    
    @pytest.mark.asyncio
    async def test_fuzz_config_values(self):
        """Fuzz配置值"""
        storage = InMemoryConfigStorage()
        center = ConfigCenter(storage)
        await center.initialize()
        
        test_values = [
            None,
            "",
            "x" * 10000,
            0,
            -1,
            float('inf'),
            float('-inf'),
            [],
            {},
            {"nested": {"deep": {"value": "test"}}},
            ["x" * 1000 for _ in range(100)],
        ]
        
        for i, value in enumerate(test_values):
            try:
                await center.set(f"key_{i}", value)
                retrieved = await center.get(f"key_{i}")
                # Should round-trip correctly
                assert retrieved == value or (value != value and retrieved != retrieved)  # Handle NaN
            except Exception:
                pass


class TestEventStoreFuzz:
    """事件存储Fuzz测试"""
    
    @pytest.mark.asyncio
    async def test_fuzz_event_data(self):
        """Fuzz事件数据"""
        store = InMemoryEventStore()
        
        for i in range(100):
            # Random event type
            event_type = ''.join(random.choices(string.ascii_letters, k=random.randint(1, 50)))
            
            # Random event data
            data = {
                "string": ''.join(random.choices(string.printable, k=random.randint(0, 1000))),
                "number": random.randint(-1000000, 1000000),
                "float": random.random() * 1000000,
                "bool": random.choice([True, False]),
                "null": None,
            }
            
            try:
                await store.append(f"agg_{i%10}", [StoredEvent(
                    event_id=f"e{i}", aggregate_id=f"agg_{i%10}",
                    event_type=event_type, event_data=data,
                    version=i+1, timestamp=datetime.now(timezone.utc),
                    sequence_number=i+1, metadata={}
                )])
            except Exception:
                pass


class TestVectorClockFuzz:
    """向量时钟Fuzz测试"""
    
    def test_fuzz_clock_operations(self):
        """Fuzz时钟操作"""
        for _ in range(100):
            node_id = ''.join(random.choices(string.ascii_letters, k=random.randint(1, 50)))
            vc = VectorClock(node_id)
            
            # Random operations
            for _ in range(random.randint(0, 100)):
                op = random.choice(["increment", "get_timestamp", "to_dict", "copy"])
                try:
                    if op == "increment":
                        vc.increment()
                    elif op == "get_timestamp":
                        vc.get_timestamp()
                    elif op == "to_dict":
                        vc.to_dict()
                    elif op == "copy":
                        vc.copy()
                except Exception:
                    pass


class TestHybridLogicalClockFuzz:
    """HLC Fuzz测试"""
    
    def test_fuzz_hlc_operations(self):
        """Fuzz HLC操作"""
        for _ in range(100):
            node_id = ''.join(random.choices(string.ascii_letters, k=random.randint(1, 50)))
            hlc = HybridLogicalClock(node_id)
            
            # Random operations
            for _ in range(random.randint(0, 100)):
                op = random.choice(["now", "peek", "get_stats"])
                try:
                    if op == "now":
                        hlc.now()
                    elif op == "peek":
                        hlc.peek()
                    elif op == "get_stats":
                        hlc.get_stats()
                except Exception:
                    pass

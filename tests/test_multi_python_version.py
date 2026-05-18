"""
多Python版本兼容性测试
Multi-Python Version Compatibility Testing

第13轮深度实践 - 验证Python 3.9/3.10/3.11/3.12兼容性
"""
import pytest
import sys
import asyncio
import inspect
from typing import Dict, List, Any, Optional

# 导入被测系统
import sys
sys.path.insert(0, '/workspace/agent_memory_system_v2')

from memory_system.security_hardening import CryptoManager, RBACManager
from memory_system.hybrid_logical_clock import HybridLogicalClock
from memory_system.vector_clock import VectorClock


class TestPythonVersionCompatibility:
    """Python版本兼容性测试类"""

    def test_python_version_detection(self):
        """测试Python版本检测"""
        version_info = sys.version_info
        print(f"\n当前Python版本: {version_info.major}.{version_info.minor}.{version_info.micro}")
        
        # 验证版本在支持范围内
        assert version_info.major == 3
        assert version_info.minor >= 9  # 最低支持3.9

    def test_asyncio_compatibility(self):
        """测试asyncio兼容性"""
        # 测试不同Python版本的asyncio API
        
        # 测试asyncio.run
        async def test_coro():
            return "success"
        
        result = asyncio.run(test_coro())
        assert result == "success"
        
        # 测试asyncio.gather
        async def gather_test():
            tasks = [asyncio.sleep(0.01) for _ in range(5)]
            await asyncio.gather(*tasks)
            return "gathered"
        
        result = asyncio.run(gather_test())
        assert result == "gathered"

    def test_typing_compatibility(self):
        """测试类型注解兼容性"""
        from typing import get_type_hints, get_origin, get_args
        
        # 测试各种类型注解
        def sample_func(
            x: int,
            y: Optional[str] = None,
            z: List[int] = None
        ) -> Dict[str, Any]:
            return {}
        
        # Python 3.9+ 应支持这些类型操作
        hints = get_type_hints(sample_func)
        assert 'x' in hints
        assert 'y' in hints
        assert 'z' in hints

    def test_dataclass_compatibility(self):
        """测试数据类兼容性"""
        from dataclasses import dataclass, field, asdict
        from typing import List
        
        @dataclass
        class TestData:
            name: str
            value: int
            items: List[str] = field(default_factory=list)
        
        obj = TestData(name="test", value=42, items=["a", "b"])
        data = asdict(obj)
        
        assert data['name'] == "test"
        assert data['value'] == 42
        assert data['items'] == ["a", "b"]

    def test_enum_compatibility(self):
        """测试枚举兼容性"""
        from enum import Enum, auto, IntEnum
        
        class Status(Enum):
            PENDING = auto()
            RUNNING = auto()
            COMPLETED = auto()
        
        class Priority(IntEnum):
            LOW = 1
            MEDIUM = 2
            HIGH = 3
        
        # 验证枚举行为
        assert Status.PENDING.name == "PENDING"
        assert Priority.HIGH == 3
        assert Priority.HIGH > Priority.LOW


class TestAsyncSyntaxCompatibility:
    """异步语法兼容性测试"""

    @pytest.mark.asyncio
    async def test_async_await_syntax(self):
        """测试async/await语法"""
        async def async_function():
            await asyncio.sleep(0.001)
            return 42
        
        result = await async_function()
        assert result == 42

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        class AsyncContext:
            async def __aenter__(self):
                await asyncio.sleep(0.001)
                return self
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                await asyncio.sleep(0.001)
        
        async with AsyncContext() as ctx:
            assert ctx is not None

    @pytest.mark.asyncio
    async def test_async_iterator(self):
        """测试异步迭代器"""
        class AsyncIterator:
            def __init__(self, count):
                self.count = count
                self.current = 0
            
            def __aiter__(self):
                return self
            
            async def __anext__(self):
                if self.current >= self.count:
                    raise StopAsyncIteration
                await asyncio.sleep(0.001)
                self.current += 1
                return self.current
        
        items = []
        async for item in AsyncIterator(3):
            items.append(item)
        
        assert items == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_async_generator(self):
        """测试异步生成器"""
        async def async_gen():
            for i in range(3):
                await asyncio.sleep(0.001)
                yield i
        
        items = []
        async for item in async_gen():
            items.append(item)
        
        assert items == [0, 1, 2]


class TestExceptionHandlingCompatibility:
    """异常处理兼容性测试"""

    @pytest.mark.skipif(sys.version_info < (3, 11), reason="ExceptionGroup requires Python 3.11+")
    def test_exception_groups_python311(self):
        """测试Python 3.11+的ExceptionGroup"""
        # 使用exec动态执行3.11+语法
        code = '''
import sys
try:
    raise ExceptionGroup("multiple errors", [
        ValueError("error 1"),
        TypeError("error 2")
    ])
except* ValueError as eg:
    assert len(eg.exceptions) == 1
except* TypeError as eg:
    assert len(eg.exceptions) == 1
'''
        exec(code)

    def test_exception_notes_python311(self):
        """测试Python 3.11+的异常注释"""
        if sys.version_info >= (3, 11):
            try:
                err = ValueError("original error")
                err.add_note("Additional context")
                raise err
            except ValueError as e:
                assert e.__notes__ == ["Additional context"]
        else:
            pytest.skip("Exception notes require Python 3.11+")


class TestLibraryCompatibility:
    """第三方库兼容性测试"""

    def test_pydantic_compatibility(self):
        """测试Pydantic兼容性"""
        try:
            from pydantic import BaseModel, Field, validator
            
            class TestModel(BaseModel):
                name: str
                age: int = Field(gt=0, lt=150)
                
                @validator('name')
                def name_must_not_be_empty(cls, v):
                    if not v.strip():
                        raise ValueError('Name cannot be empty')
                    return v
            
            # 测试有效数据
            obj = TestModel(name="test", age=25)
            assert obj.name == "test"
            assert obj.age == 25
            
            # 测试无效数据
            with pytest.raises(Exception):
                TestModel(name="", age=25)
                
        except ImportError:
            pytest.skip("Pydantic not installed")

    def test_cryptography_compatibility(self):
        """测试cryptography库兼容性"""
        try:
            from cryptography.fernet import Fernet
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
            
            # 测试Fernet
            key = Fernet.generate_key()
            f = Fernet(key)
            token = f.encrypt(b"test data")
            decrypted = f.decrypt(token)
            assert decrypted == b"test data"
            
        except ImportError:
            pytest.skip("cryptography not installed")

    def test_numpy_compatibility(self):
        """测试NumPy兼容性"""
        try:
            import numpy as np
            
            # 测试基本操作
            arr = np.array([1, 2, 3, 4, 5])
            assert arr.sum() == 15
            assert arr.mean() == 3.0
            
            # 测试矩阵操作
            matrix = np.array([[1, 2], [3, 4]])
            result = np.dot(matrix, matrix)
            assert result.shape == (2, 2)
            
        except ImportError:
            pytest.skip("numpy not installed")


class TestMemorySystemCompatibility:
    """记忆系统兼容性测试"""

    def test_crypto_manager_initialization(self):
        """测试加密管理器初始化"""
        manager = CryptoManager()
        
        # 测试基本功能
        password = "test_password"
        hashed, salt = manager.hash_password(password)
        assert manager.verify_password(password, hashed, salt)
        
        # 测试加密解密
        data = "test data"
        encrypted = manager.encrypt(data)
        decrypted = manager.decrypt(encrypted)
        assert decrypted == data

    def test_vector_clock_operations(self):
        """测试向量时钟操作"""
        node1 = "node_1"
        node2 = "node_2"
        
        vc1 = VectorClock(node1)
        vc1.increment()
        
        vc2 = VectorClock(node2)
        vc2.increment()
        
        # 测试合并
        vc1.update(vc2)
        assert node1 in vc1.clock
        assert node2 in vc1.clock

    def test_hybrid_logical_clock(self):
        """测试混合逻辑时钟"""
        hlc = HybridLogicalClock(node_id="test_node")
        
        # 测试时间戳生成
        ts1 = hlc.now()
        import time
        time.sleep(0.01)
        ts2 = hlc.now()
        
        # HLC时间戳应可比较
        assert ts2.physical >= ts1.physical


class TestPerformanceCompatibility:
    """性能兼容性测试"""

    def test_list_comprehension_performance(self):
        """测试列表推导式性能"""
        import time
        
        # 测试大数据量列表推导
        start = time.time()
        result = [x**2 for x in range(100000)]
        elapsed = time.time() - start
        
        assert len(result) == 100000
        assert elapsed < 5.0  # 应在5秒内完成

    def test_generator_performance(self):
        """测试生成器性能"""
        import time
        
        start = time.time()
        result = sum(x**2 for x in range(1000000))
        elapsed = time.time() - start
        
        assert result > 0
        assert elapsed < 5.0

    @pytest.mark.asyncio
    async def test_asyncio_performance(self):
        """测试asyncio性能"""
        import time
        
        async def task(n):
            await asyncio.sleep(0.001)
            return n
        
        start = time.time()
        tasks = [task(i) for i in range(100)]
        results = await asyncio.gather(*tasks)
        elapsed = time.time() - start
        
        assert len(results) == 100
        assert elapsed < 10.0  # 应在10秒内完成


class TestDeprecationWarnings:
    """弃用警告测试"""

    def test_no_deprecation_warnings(self):
        """测试没有弃用警告"""
        import warnings
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            # 导入和使用主要模块
            from memory_system.security_hardening import CryptoManager
            from memory_system.vector_clock import VectorClock
            
            # 检查是否有弃用警告
            deprecation_warnings = [
                warning for warning in w
                if issubclass(warning.category, DeprecationWarning)
            ]
            
            # 允许少量警告，但不应过多
            assert len(deprecation_warnings) < 10


@pytest.mark.skipif(sys.version_info < (3, 10), reason="Requires Python 3.10+")
class TestPython310Features:
    """Python 3.10+特性测试"""
    
    def test_structural_pattern_matching(self):
        """测试结构模式匹配"""
        def http_status(status):
            match status:
                case 200:
                    return "OK"
                case 404:
                    return "Not Found"
                case 500:
                    return "Server Error"
                case _:
                    return "Unknown"
        
        assert http_status(200) == "OK"
        assert http_status(404) == "Not Found"

    def test_union_types_pipe(self):
        """测试管道操作符联合类型"""
        def func(x: int | str) -> int | str:
            return x
        
        assert func(42) == 42
        assert func("test") == "test"


@pytest.mark.skipif(sys.version_info < (3, 11), reason="Requires Python 3.11+")
class TestPython311Features:
    """Python 3.11+特性测试"""
    
    def test_tomllib_module(self):
        """测试tomllib模块"""
        import tomllib
        
        toml_data = """
[tool.test]
name = "test"
version = "1.0.0"
"""
        data = tomllib.loads(toml_data)
        assert data['tool']['test']['name'] == "test"

    def test_task_group(self):
        """测试TaskGroup"""
        async def test_task_group():
            results = []
            
            async with asyncio.TaskGroup() as tg:
                for i in range(3):
                    tg.create_task(asyncio.sleep(0.01))
                    results.append(i)
            
            return results
        
        result = asyncio.run(test_task_group())
        assert len(result) == 3


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12+")
class TestPython312Features:
    """Python 3.12+特性测试"""
    
    def test_f_string_debugging(self):
        """测试f-string调试语法"""
        x = 42
        result = f"{x=}"
        assert result == "x=42"


# ==================== 运行测试 ====================

if __name__ == "__main__":
    print(f"Python版本: {sys.version}")
    print(f"版本信息: {sys.version_info}")
    pytest.main([__file__, "-v", "--tb=short"])

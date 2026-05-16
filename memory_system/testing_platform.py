#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化测试平台 (Automated Testing Platform)

提供端到端测试、契约测试(Pact)、性能测试、模糊测试(Fuzzing)、
测试数据管理和CI/CD集成能力。

Author: Agent Memory System Team
Version: 3.0.0
"""

import asyncio
import hashlib
import inspect
import json
import logging
import random
import statistics
import string
import subprocess
import sys
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from contextlib import contextmanager
from functools import wraps

# 配置日志
logger = logging.getLogger(__name__)


class TestStatus(Enum):
    """测试状态"""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


class TestType(Enum):
    """测试类型"""
    UNIT = "unit"
    INTEGRATION = "integration"
    E2E = "e2e"
    CONTRACT = "contract"
    PERFORMANCE = "performance"
    FUZZ = "fuzz"
    SECURITY = "security"


@dataclass
class TestResult:
    """测试结果"""
    test_id: str
    name: str
    test_type: TestType
    status: TestStatus
    duration_ms: float
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    stack_trace: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_id": self.test_id,
            "name": self.name,
            "test_type": self.test_type.value,
            "status": self.status.value,
            "duration_ms": self.duration_ms,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "metadata": self.metadata,
        }


@dataclass
class TestSuite:
    """测试套件"""
    suite_id: str
    name: str
    description: str
    test_type: TestType
    tests: List["TestCase"] = field(default_factory=list)
    setup: Optional[Callable] = None
    teardown: Optional[Callable] = None
    parallel: bool = False
    max_workers: int = 4


@dataclass
class TestCase:
    """测试用例"""
    test_id: str
    name: str
    test_type: TestType
    func: Callable
    setup: Optional[Callable] = None
    teardown: Optional[Callable] = None
    tags: List[str] = field(default_factory=list)
    timeout_seconds: float = 60.0
    retries: int = 0


@dataclass
class PerformanceMetrics:
    """性能指标"""
    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    std_dev_ms: float
    throughput_rps: float
    error_rate: float
    total_requests: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "mean_ms": self.mean_ms,
            "median_ms": self.median_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "std_dev_ms": self.std_dev_ms,
            "throughput_rps": self.throughput_rps,
            "error_rate": self.error_rate,
            "total_requests": self.total_requests,
        }


@dataclass
class Contract:
    """契约定义"""
    consumer: str
    provider: str
    interaction: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "consumer": self.consumer,
            "provider": self.provider,
            "interaction": self.interaction,
        }


class TestDataManager:
    """测试数据管理器"""
    
    def __init__(self):
        self._data_pools: Dict[str, List[Any]] = {}
        self._factories: Dict[str, Callable] = {}
        self._lock = threading.Lock()
    
    def register_factory(self, name: str, factory: Callable) -> None:
        """注册数据工厂"""
        with self._lock:
            self._factories[name] = factory
    
    def generate(self, name: str, count: int = 1, **kwargs) -> List[Any]:
        """生成测试数据"""
        with self._lock:
            factory = self._factories.get(name)
            if not factory:
                raise ValueError(f"Unknown factory: {name}")
        
        return [factory(**kwargs) for _ in range(count)]
    
    def create_pool(self, name: str, data: List[Any]) -> None:
        """创建数据池"""
        with self._lock:
            self._data_pools[name] = data.copy()
    
    def get_from_pool(self, name: str, index: Optional[int] = None) -> Any:
        """从数据池获取数据"""
        with self._lock:
            pool = self._data_pools.get(name, [])
            if not pool:
                raise ValueError(f"Empty or unknown pool: {name}")
            
            if index is None:
                return random.choice(pool)
            return pool[index % len(pool)]
    
    def clear_pool(self, name: str) -> None:
        """清空数据池"""
        with self._lock:
            if name in self._data_pools:
                del self._data_pools[name]


class BaseTestRunner(ABC):
    """测试运行器基类"""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self._lock = threading.Lock()
    
    @abstractmethod
    def run(self, test_case: TestCase) -> TestResult:
        """运行单个测试"""
        pass
    
    def _create_result(
        self,
        test_case: TestCase,
        status: TestStatus,
        duration_ms: float,
        error: Optional[Exception] = None
    ) -> TestResult:
        """创建测试结果"""
        now = datetime.utcnow()
        
        result = TestResult(
            test_id=test_case.test_id,
            name=test_case.name,
            test_type=test_case.test_type,
            status=status,
            duration_ms=duration_ms,
            start_time=now - timedelta(milliseconds=duration_ms),
            end_time=now,
            error_message=str(error) if error else None,
            stack_trace=self._get_stack_trace(error) if error else None,
        )
        
        with self._lock:
            self.results.append(result)
        
        return result
    
    def _get_stack_trace(self, error: Exception) -> str:
        """获取堆栈跟踪"""
        import traceback
        return traceback.format_exc()


class UnitTestRunner(BaseTestRunner):
    """单元测试运行器"""
    
    def run(self, test_case: TestCase) -> TestResult:
        """运行单元测试"""
        start_time = time.time()
        
        try:
            # 执行setup
            if test_case.setup:
                test_case.setup()
            
            # 执行测试
            result = test_case.func()
            
            # 如果是协程，运行它
            if asyncio.iscoroutine(result):
                result = asyncio.run(result)
            
            # 执行teardown
            if test_case.teardown:
                test_case.teardown()
            
            duration_ms = (time.time() - start_time) * 1000
            return self._create_result(test_case, TestStatus.PASSED, duration_ms)
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            
            # 执行teardown (即使测试失败)
            if test_case.teardown:
                try:
                    test_case.teardown()
                except Exception as teardown_error:
                    logger.error(f"Teardown failed: {teardown_error}")
            
            return self._create_result(test_case, TestStatus.FAILED, duration_ms, e)


class E2ETestRunner(BaseTestRunner):
    """端到端测试运行器"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        super().__init__()
        self.base_url = base_url
        self._session = None
    
    def run(self, test_case: TestCase) -> TestResult:
        """运行E2E测试"""
        start_time = time.time()
        
        try:
            # 导入requests (延迟导入以避免依赖)
            try:
                import requests
            except ImportError:
                duration_ms = (time.time() - start_time) * 1000
                error = ImportError("requests library required for E2E tests")
                return self._create_result(test_case, TestStatus.ERROR, duration_ms, error)
            
            # 执行setup
            if test_case.setup:
                test_case.setup()
            
            # 执行测试
            test_case.func(base_url=self.base_url, requests=requests)
            
            # 执行teardown
            if test_case.teardown:
                test_case.teardown()
            
            duration_ms = (time.time() - start_time) * 1000
            return self._create_result(test_case, TestStatus.PASSED, duration_ms)
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            
            if test_case.teardown:
                try:
                    test_case.teardown()
                except Exception as teardown_error:
                    logger.error(f"Teardown failed: {teardown_error}")
            
            return self._create_result(test_case, TestStatus.FAILED, duration_ms, e)


class ContractTestRunner(BaseTestRunner):
    """契约测试运行器 (Pact风格)"""
    
    def __init__(self, pact_dir: str = "./pacts"):
        super().__init__()
        self.pact_dir = pact_dir
        self._contracts: List[Contract] = []
    
    def add_contract(self, contract: Contract) -> None:
        """添加契约"""
        self._contracts.append(contract)
    
    def run(self, test_case: TestCase) -> TestResult:
        """运行契约测试"""
        start_time = time.time()
        
        try:
            # 执行契约验证
            test_case.func(contracts=self._contracts)
            
            duration_ms = (time.time() - start_time) * 1000
            return self._create_result(test_case, TestStatus.PASSED, duration_ms)
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return self._create_result(test_case, TestStatus.FAILED, duration_ms, e)
    
    def verify_provider(self, provider_name: str, provider_url: str) -> List[TestResult]:
        """验证提供者契约"""
        results = []
        
        for contract in self._contracts:
            if contract.provider == provider_name:
                test_case = TestCase(
                    test_id=str(uuid.uuid4()),
                    name=f"verify_{contract.consumer}_{provider_name}",
                    test_type=TestType.CONTRACT,
                    func=lambda: self._verify_contract(contract, provider_url),
                )
                result = self.run(test_case)
                results.append(result)
        
        return results
    
    def _verify_contract(self, contract: Contract, provider_url: str) -> None:
        """验证单个契约"""
        import requests
        
        interaction = contract.interaction
        request_spec = interaction.get("request", {})
        expected_response = interaction.get("response", {})
        
        # 发送请求
        method = request_spec.get("method", "GET")
        path = request_spec.get("path", "/")
        headers = request_spec.get("headers", {})
        body = request_spec.get("body")
        
        url = f"{provider_url}{path}"
        response = requests.request(method, url, headers=headers, json=body)
        
        # 验证响应
        expected_status = expected_response.get("status", 200)
        if response.status_code != expected_status:
            raise AssertionError(
                f"Status mismatch: expected {expected_status}, got {response.status_code}"
            )
        
        expected_body = expected_response.get("body")
        if expected_body and response.json() != expected_body:
            raise AssertionError("Response body mismatch")


class PerformanceTestRunner(BaseTestRunner):
    """性能测试运行器"""
    
    def __init__(self):
        super().__init__()
        self.metrics: Optional[PerformanceMetrics] = None
    
    def run(
        self,
        test_case: TestCase,
        duration_seconds: float = 60.0,
        concurrent_users: int = 10,
        ramp_up_seconds: float = 10.0
    ) -> TestResult:
        """运行性能测试"""
        start_time = time.time()
        
        try:
            latencies: List[float] = []
            errors = 0
            total_requests = 0
            
            end_time = time.time() + duration_seconds
            ramp_end = time.time() + ramp_up_seconds
            
            def worker():
                nonlocal errors, total_requests
                while time.time() < end_time:
                    try:
                        req_start = time.time()
                        test_case.func()
                        latency = (time.time() - req_start) * 1000
                        latencies.append(latency)
                        total_requests += 1
                    except Exception:
                        errors += 1
                        total_requests += 1
            
            # 启动工作线程
            threads = []
            for i in range(concurrent_users):
                # 渐进式启动
                if time.time() < ramp_end:
                    time.sleep(ramp_up_seconds / concurrent_users)
                
                t = threading.Thread(target=worker)
                t.daemon = True
                t.start()
                threads.append(t)
            
            # 等待测试完成
            for t in threads:
                t.join(timeout=duration_seconds + 10)
            
            # 计算指标
            if latencies:
                latencies.sort()
                self.metrics = PerformanceMetrics(
                    min_ms=min(latencies),
                    max_ms=max(latencies),
                    mean_ms=statistics.mean(latencies),
                    median_ms=statistics.median(latencies),
                    p95_ms=latencies[int(len(latencies) * 0.95)],
                    p99_ms=latencies[int(len(latencies) * 0.99)],
                    std_dev_ms=statistics.stdev(latencies) if len(latencies) > 1 else 0,
                    throughput_rps=total_requests / duration_seconds,
                    error_rate=errors / total_requests if total_requests > 0 else 0,
                    total_requests=total_requests,
                )
            
            duration_ms = (time.time() - start_time) * 1000
            
            result = self._create_result(test_case, TestStatus.PASSED, duration_ms)
            result.metadata["performance"] = self.metrics.to_dict() if self.metrics else {}
            
            return result
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return self._create_result(test_case, TestStatus.ERROR, duration_ms, e)


class FuzzTestRunner(BaseTestRunner):
    """模糊测试运行器"""
    
    def __init__(self, seed: Optional[int] = None):
        super().__init__()
        self.seed = seed or int(time.time())
        self.random = random.Random(self.seed)
        self.corpus: List[Any] = []
        self.crashes: List[Dict[str, Any]] = []
    
    def run(
        self,
        test_case: TestCase,
        iterations: int = 1000,
        max_input_size: int = 1024
    ) -> TestResult:
        """运行模糊测试"""
        start_time = time.time()
        
        try:
            for i in range(iterations):
                # 生成随机输入
                input_data = self._generate_input(max_input_size)
                
                try:
                    test_case.func(input_data)
                except Exception as e:
                    # 记录崩溃
                    self.crashes.append({
                        "iteration": i,
                        "input": input_data,
                        "error": str(e),
                    })
            
            duration_ms = (time.time() - start_time) * 1000
            
            result = self._create_result(
                test_case,
                TestStatus.PASSED if not self.crashes else TestStatus.FAILED,
                duration_ms
            )
            result.metadata["iterations"] = iterations
            result.metadata["crashes_found"] = len(self.crashes)
            result.metadata["seed"] = self.seed
            
            return result
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return self._create_result(test_case, TestStatus.ERROR, duration_ms, e)
    
    def _generate_input(self, max_size: int) -> Union[str, bytes, Dict]:
        """生成随机输入"""
        input_type = self.random.choice(["string", "bytes", "json"])
        
        if input_type == "string":
            size = self.random.randint(0, max_size)
            chars = string.printable
            return ''.join(self.random.choice(chars) for _ in range(size))
        
        elif input_type == "bytes":
            size = self.random.randint(0, max_size)
            return bytes(self.random.randint(0, 255) for _ in range(size))
        
        else:  # json
            return self._generate_random_json()
    
    def _generate_random_json(self) -> Dict[str, Any]:
        """生成随机JSON对象"""
        def random_value(depth: int = 0) -> Any:
            if depth > 3:
                return self.random.choice([None, True, False, self.random.randint(-1000, 1000)])
            
            type_choice = self.random.choice(["null", "bool", "int", "float", "string", "list", "dict"])
            
            if type_choice == "null":
                return None
            elif type_choice == "bool":
                return self.random.choice([True, False])
            elif type_choice == "int":
                return self.random.randint(-1000000, 1000000)
            elif type_choice == "float":
                return self.random.uniform(-1000000.0, 1000000.0)
            elif type_choice == "string":
                return ''.join(self.random.choice(string.ascii_letters) for _ in range(self.random.randint(0, 100)))
            elif type_choice == "list":
                return [random_value(depth + 1) for _ in range(self.random.randint(0, 10))]
            else:  # dict
                return {
                    ''.join(self.random.choice(string.ascii_lowercase) for _ in range(self.random.randint(1, 20))): 
                    random_value(depth + 1)
                    for _ in range(self.random.randint(0, 10))
                }
        
        return random_value()


class TestingPlatform:
    """自动化测试平台主类"""
    
    def __init__(self):
        self._test_suites: Dict[str, TestSuite] = {}
        self._test_cases: Dict[str, TestCase] = {}
        self._runners: Dict[TestType, BaseTestRunner] = {}
        self._data_manager = TestDataManager()
        self._results: List[TestResult] = []
        self._lock = threading.Lock()
        
        # 初始化默认运行器
        self._init_default_runners()
    
    def _init_default_runners(self) -> None:
        """初始化默认测试运行器"""
        self._runners[TestType.UNIT] = UnitTestRunner()
        self._runners[TestType.E2E] = E2ETestRunner()
        self._runners[TestType.CONTRACT] = ContractTestRunner()
        self._runners[TestType.PERFORMANCE] = PerformanceTestRunner()
        self._runners[TestType.FUZZ] = FuzzTestRunner()
    
    def register_test_case(
        self,
        name: str,
        test_type: TestType,
        func: Callable,
        setup: Optional[Callable] = None,
        teardown: Optional[Callable] = None,
        tags: Optional[List[str]] = None,
        timeout_seconds: float = 60.0,
        retries: int = 0
    ) -> TestCase:
        """注册测试用例"""
        test_case = TestCase(
            test_id=str(uuid.uuid4()),
            name=name,
            test_type=test_type,
            func=func,
            setup=setup,
            teardown=teardown,
            tags=tags or [],
            timeout_seconds=timeout_seconds,
            retries=retries,
        )
        
        with self._lock:
            self._test_cases[test_case.test_id] = test_case
        
        return test_case
    
    def create_test_suite(
        self,
        name: str,
        description: str,
        test_type: TestType,
        tests: Optional[List[TestCase]] = None,
        parallel: bool = False,
        max_workers: int = 4
    ) -> TestSuite:
        """创建测试套件"""
        suite = TestSuite(
            suite_id=str(uuid.uuid4()),
            name=name,
            description=description,
            test_type=test_type,
            tests=tests or [],
            parallel=parallel,
            max_workers=max_workers,
        )
        
        with self._lock:
            self._test_suites[suite.suite_id] = suite
        
        return suite
    
    def run_test(self, test_id: str, **kwargs) -> Optional[TestResult]:
        """运行单个测试"""
        with self._lock:
            test_case = self._test_cases.get(test_id)
        
        if not test_case:
            logger.error(f"Test not found: {test_id}")
            return None
        
        runner = self._runners.get(test_case.test_type)
        if not runner:
            logger.error(f"No runner for test type: {test_case.test_type}")
            return None
        
        # 重试逻辑
        last_result = None
        for attempt in range(test_case.retries + 1):
            if test_case.test_type == TestType.PERFORMANCE:
                last_result = runner.run(test_case, **kwargs)
            elif test_case.test_type == TestType.FUZZ:
                last_result = runner.run(test_case, **kwargs)
            else:
                last_result = runner.run(test_case)
            
            if last_result.status == TestStatus.PASSED:
                break
            
            if attempt < test_case.retries:
                logger.info(f"Retrying test {test_case.name} (attempt {attempt + 2})")
                time.sleep(1)
        
        with self._lock:
            self._results.append(last_result)
        
        return last_result
    
    def run_suite(self, suite_id: str, **kwargs) -> List[TestResult]:
        """运行测试套件"""
        with self._lock:
            suite = self._test_suites.get(suite_id)
        
        if not suite:
            logger.error(f"Suite not found: {suite_id}")
            return []
        
        results = []
        
        # 执行setup
        if suite.setup:
            suite.setup()
        
        try:
            if suite.parallel and len(suite.tests) > 1:
                # 并行执行
                from concurrent.futures import ThreadPoolExecutor
                
                with ThreadPoolExecutor(max_workers=suite.max_workers) as executor:
                    futures = [
                        executor.submit(self.run_test, test.test_id, **kwargs)
                        for test in suite.tests
                    ]
                    results = [f.result() for f in futures if f.result()]
            else:
                # 串行执行
                for test in suite.tests:
                    result = self.run_test(test.test_id, **kwargs)
                    if result:
                        results.append(result)
        finally:
            # 执行teardown
            if suite.teardown:
                suite.teardown()
        
        return results
    
    def run_all(self, test_type: Optional[TestType] = None) -> List[TestResult]:
        """运行所有测试"""
        with self._lock:
            tests = list(self._test_cases.values())
        
        if test_type:
            tests = [t for t in tests if t.test_type == test_type]
        
        results = []
        for test in tests:
            result = self.run_test(test.test_id)
            if result:
                results.append(result)
        
        return results
    
    def get_test_report(self) -> Dict[str, Any]:
        """生成测试报告"""
        with self._lock:
            results = self._results.copy()
        
        if not results:
            return {"message": "No tests run yet"}
        
        total = len(results)
        passed = sum(1 for r in results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in results if r.status == TestStatus.FAILED)
        errors = sum(1 for r in results if r.status == TestStatus.ERROR)
        skipped = sum(1 for r in results if r.status == TestStatus.SKIPPED)
        
        by_type: Dict[str, Dict[str, int]] = {}
        for result in results:
            type_name = result.test_type.value
            if type_name not in by_type:
                by_type[type_name] = {"total": 0, "passed": 0, "failed": 0}
            by_type[type_name]["total"] += 1
            if result.status == TestStatus.PASSED:
                by_type[type_name]["passed"] += 1
            else:
                by_type[type_name]["failed"] += 1
        
        total_duration = sum(r.duration_ms for r in results)
        
        return {
            "summary": {
                "total": total,
                "passed": passed,
                "failed": failed,
                "errors": errors,
                "skipped": skipped,
                "pass_rate": passed / total if total > 0 else 0,
                "total_duration_ms": total_duration,
            },
            "by_type": by_type,
            "failed_tests": [
                {"name": r.name, "error": r.error_message}
                for r in results if r.status in [TestStatus.FAILED, TestStatus.ERROR]
            ],
        }
    
    def get_data_manager(self) -> TestDataManager:
        """获取测试数据管理器"""
        return self._data_manager


# 全局测试平台实例
_testing_platform: Optional[TestingPlatform] = None


def get_testing_platform() -> TestingPlatform:
    """获取全局测试平台实例"""
    global _testing_platform
    if _testing_platform is None:
        _testing_platform = TestingPlatform()
    return _testing_platform


# 装饰器
def test(
    name: Optional[str] = None,
    test_type: TestType = TestType.UNIT,
    tags: Optional[List[str]] = None,
    timeout: float = 60.0
):
    """测试装饰器"""
    def decorator(func: Callable) -> Callable:
        test_name = name or func.__name__
        
        platform = get_testing_platform()
        platform.register_test_case(
            name=test_name,
            test_type=test_type,
            func=func,
            tags=tags or [],
            timeout_seconds=timeout
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    print("=== Testing Platform Demo ===\n")
    
    # 创建测试平台
    platform = get_testing_platform()
    
    # 示例1: 单元测试
    print("1. Unit Tests:")
    
    @test(name="test_addition", test_type=TestType.UNIT, tags=["math"])
    def test_addition():
        assert 1 + 1 == 2
    
    @test(name="test_subtraction", test_type=TestType.UNIT, tags=["math"])
    def test_subtraction():
        assert 5 - 3 == 2
    
    unit_results = platform.run_all(test_type=TestType.UNIT)
    print(f"   Run {len(unit_results)} unit tests")
    for r in unit_results:
        print(f"   - {r.name}: {r.status.value}")
    
    # 示例2: 性能测试
    print("\n2. Performance Tests:")
    
    def benchmark_function():
        # 模拟一些工作
        result = sum(i ** 2 for i in range(1000))
        return result
    
    perf_test = platform.register_test_case(
        name="benchmark_sum",
        test_type=TestType.PERFORMANCE,
        func=benchmark_function
    )
    
    perf_result = platform.run_test(perf_test.test_id, duration_seconds=2.0, concurrent_users=4)
    if perf_result and perf_result.metadata.get("performance"):
        metrics = perf_result.metadata["performance"]
        print(f"   Mean latency: {metrics['mean_ms']:.2f}ms")
        print(f"   P99 latency: {metrics['p99_ms']:.2f}ms")
        print(f"   Throughput: {metrics['throughput_rps']:.2f} req/s")
    
    # 示例3: 模糊测试
    print("\n3. Fuzz Tests:")
    
    def fuzz_target(data):
        # 测试一个解析函数
        if isinstance(data, str) and len(data) > 100:
            raise ValueError("String too long")
        return True
    
    fuzz_test = platform.register_test_case(
        name="fuzz_parser",
        test_type=TestType.FUZZ,
        func=fuzz_target
    )
    
    fuzz_result = platform.run_test(fuzz_test.test_id, iterations=100)
    if fuzz_result:
        print(f"   Iterations: {fuzz_result.metadata.get('iterations')}")
        print(f"   Crashes found: {fuzz_result.metadata.get('crashes_found')}")
    
    # 示例4: 测试数据管理
    print("\n4. Test Data Management:")
    
    def user_factory(user_id: int = 1) -> Dict:
        return {
            "id": user_id,
            "name": f"User_{user_id}",
            "email": f"user{user_id}@example.com"
        }
    
    data_manager = platform.get_data_manager()
    data_manager.register_factory("user", user_factory)
    
    users = data_manager.generate("user", count=3, user_id=100)
    print(f"   Generated {len(users)} test users")
    for u in users:
        print(f"   - {u['name']} ({u['email']})")
    
    # 示例5: 测试报告
    print("\n5. Test Report:")
    
    report = platform.get_test_report()
    summary = report["summary"]
    print(f"   Total tests: {summary['total']}")
    print(f"   Passed: {summary['passed']}")
    print(f"   Failed: {summary['failed']}")
    print(f"   Pass rate: {summary['pass_rate']:.1%}")
    
    print("\n=== Testing Platform Demo Completed ===")

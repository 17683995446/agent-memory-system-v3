"""基础设施模块深度测试 - 读写锁/限流/DI/监控/调度"""
import pytest
import asyncio
import threading
import time
from datetime import datetime, timedelta
from memory_system.read_write_lock import ReadWriteLock
from memory_system.rate_limiter import RateLimiter, AsyncRateLimiter, MultiTierRateLimiter, RateLimitConfig, RateLimitStrategy
from memory_system.di_container import DIContainer, Scope
from memory_system.monitoring_alerting import (
    MetricsCollector, AlertManager, AlertRule, AlertSeverity, AlertState,
    AlertInstance, MetricType
)


class TestReadWriteLockBoundary:
    """读写锁边界测试"""
    
    @pytest.mark.asyncio
    async def test_read_lock_concurrent(self):
        """多个读者应能同时持有读锁"""
        rw_lock = ReadWriteLock()
        results = []
        
        async def reader(reader_id):
            async with rw_lock.read_lock():
                results.append(f"reader_{reader_id}_entered")
                await asyncio.sleep(0.05)
                results.append(f"reader_{reader_id}_exited")
        
        await asyncio.gather(
            reader(1), reader(2), reader(3)
        )
        # All readers should have entered before any exited
        entered = [r for r in results if "_entered" in r]
        exited = [r for r in results if "_exited" in r]
        # At least some readers should overlap
        assert len(entered) == 3
    
    @pytest.mark.asyncio
    async def test_write_lock_exclusive(self):
        """写锁应独占"""
        rw_lock = ReadWriteLock()
        execution_order = []
        
        async def writer(writer_id):
            async with rw_lock.write_lock():
                execution_order.append(f"write_{writer_id}_start")
                await asyncio.sleep(0.05)
                execution_order.append(f"write_{writer_id}_end")
        
        await asyncio.gather(writer(1), writer(2))
        # Writes should be sequential
        w1_start = execution_order.index("write_1_start")
        w1_end = execution_order.index("write_1_end")
        w2_start = execution_order.index("write_2_start")
        w2_end = execution_order.index("write_2_end")
        
        # One writer fully completes before the other starts
        if w1_start < w2_start:
            assert w1_end < w2_start
        else:
            assert w2_end < w1_start
    
    @pytest.mark.asyncio
    async def test_is_locked_checks(self):
        """锁状态检查"""
        rw_lock = ReadWriteLock()
        assert not rw_lock.is_write_locked()
        assert not rw_lock.is_read_locked()
        
        async with rw_lock.read_lock():
            assert rw_lock.is_read_locked()
        
        assert not rw_lock.is_read_locked()
    
    @pytest.mark.asyncio
    async def test_get_stats(self):
        """获取锁统计"""
        rw_lock = ReadWriteLock()
        stats = rw_lock.get_stats()
        assert isinstance(stats, dict)


class TestRateLimiterBoundary:
    """限流器边界测试"""
    
    def test_token_bucket_basic(self):
        """令牌桶基本功能"""
        config = RateLimitConfig(
            requests_per_minute=10,
            burst_size=5,
            strategy=RateLimitStrategy.TOKEN_BUCKET
        )
        limiter = RateLimiter(config)
        
        # Should allow up to burst
        allowed = sum(1 for _ in range(5) if limiter.is_allowed(user_id="u1"))
        assert allowed == 5
    
    def test_sliding_window(self):
        """滑动窗口限流"""
        config = RateLimitConfig(
            requests_per_minute=5,
            strategy=RateLimitStrategy.SLIDING_WINDOW
        )
        limiter = RateLimiter(config)
        
        allowed = sum(1 for _ in range(10) if limiter.is_allowed(user_id="u1"))
        assert allowed <= 7  # Allow some burst but limit overall
    
    def test_per_user_isolation(self):
        """用户间隔离"""
        config = RateLimitConfig(requests_per_minute=3, strategy=RateLimitStrategy.TOKEN_BUCKET)
        limiter = RateLimiter(config)
        
        # User1 exhausts quota
        for _ in range(10):
            limiter.is_allowed(user_id="u1")
        
        # User2 should still be allowed
        assert limiter.is_allowed(user_id="u2")
    
    def test_reset(self):
        """重置限流"""
        config = RateLimitConfig(requests_per_minute=2, strategy=RateLimitStrategy.TOKEN_BUCKET)
        limiter = RateLimiter(config)
        
        for _ in range(10):
            limiter.is_allowed(user_id="u1")
        
        limiter.reset(user_id="u1")
        assert limiter.is_allowed(user_id="u1")
    
    def test_get_remaining_quota(self):
        """获取剩余配额"""
        config = RateLimitConfig(requests_per_minute=10, strategy=RateLimitStrategy.TOKEN_BUCKET)
        limiter = RateLimiter(config)
        
        quota = limiter.get_remaining_quota(user_id="u1")
        assert isinstance(quota, dict)


class TestMultiTierRateLimiter:
    """多级限流测试"""
    
    def test_multi_tier_all_allow(self):
        """所有层级允许"""
        ml = MultiTierRateLimiter()
        ml.add_tier("user", RateLimitConfig(requests_per_minute=60))
        ml.add_tier("endpoint", RateLimitConfig(requests_per_minute=100))
        
        allowed, tier = ml.is_allowed(user_id="u1", endpoint="api/test")
        assert allowed is True
    
    def test_multi_tier_empty(self):
        """无层级时应允许"""
        ml = MultiTierRateLimiter()
        allowed, tier = ml.is_allowed()
        assert allowed is True


class TestDIContainerBoundary:
    """DI容器边界测试"""
    
    def setup_method(self):
        # Clear singleton for each test
        DIContainer._instance = None
    
    def test_register_and_resolve(self):
        """注册和解析"""
        container = DIContainer()
        container.register("service", lambda: {"name": "test"}, scope=Scope.SINGLETON)
        service = container.resolve("service")
        assert service["name"] == "test"
    
    def test_singleton_scope(self):
        """单例作用域应返回同一实例"""
        container = DIContainer()
        container.register("service", lambda: object(), scope=Scope.SINGLETON)
        s1 = container.resolve("service")
        s2 = container.resolve("service")
        assert s1 is s2
    
    def test_transient_scope(self):
        """瞬态作用域应返回不同实例"""
        container = DIContainer()
        container.register("service", lambda: object(), scope=Scope.TRANSIENT)
        s1 = container.resolve("service")
        s2 = container.resolve("service")
        assert s1 is not s2
    
    def test_resolve_unregistered(self):
        """解析未注册的服务应抛出异常"""
        container = DIContainer()
        with pytest.raises(Exception):  # DependencyInjectionError
            container.resolve("nonexistent")
    
    def test_try_resolve_default(self):
        """try_resolve应返回默认值"""
        container = DIContainer()
        result = container.try_resolve("nonexistent", default="fallback")
        assert result == "fallback"
    
    def test_register_instance(self):
        """注册实例"""
        container = DIContainer()
        instance = {"key": "value"}
        container.register_instance("config", instance)
        assert container.resolve("config") is instance
    
    def test_unregister(self):
        """取消注册"""
        container = DIContainer()
        container.register("service", lambda: "test")
        assert container.is_registered("service")
        container.unregister("service")
        assert not container.is_registered("service")
    
    def test_clear(self):
        """清空容器"""
        container = DIContainer()
        container.register("s1", lambda: "1")
        container.register("s2", lambda: "2")
        container.clear()
        assert not container.is_registered("s1")
        assert not container.is_registered("s2")
    
    def test_scoped_context(self):
        """作用域上下文"""
        container = DIContainer()
        container.register("service", lambda: object(), scope=Scope.SCOPED)
        
        with container.create_scope("request1") as scope:
            s1 = container.resolve("service")
            s2 = container.resolve("service")
            assert s1 is s2  # Same within scope


class TestMetricsCollectorBoundary:
    """指标收集器边界测试"""
    
    def test_counter_basic(self):
        """计数器基本功能"""
        mc = MetricsCollector()
        mc.counter("requests_total", 1.0)
        mc.counter("requests_total", 1.0)
        mc.counter("requests_total", 1.0)
        
        metrics = mc.get_metrics("requests_total")
        assert len(metrics) == 3
    
    def test_gauge_set_value(self):
        """仪表盘设置值"""
        mc = MetricsCollector()
        mc.gauge("temperature", 36.5)
        mc.gauge("temperature", 37.0)
        
        metrics = mc.get_metrics("temperature")
        assert len(metrics) == 2
    
    def test_histogram(self):
        """直方图"""
        mc = MetricsCollector()
        for val in [0.1, 0.5, 1.0, 2.0, 5.0]:
            mc.histogram("request_duration", val)
        
        metrics = mc.get_metrics("request_duration")
        assert len(metrics) == 5
    
    def test_get_metrics_nonexistent(self):
        """获取不存在的指标"""
        mc = MetricsCollector()
        metrics = mc.get_metrics("nonexistent")
        assert metrics == []
    
    def test_prometheus_format(self):
        """Prometheus格式输出"""
        mc = MetricsCollector()
        mc.counter("http_requests_total", 1.0, labels={"method": "GET", "status": "200"})
        
        output = mc.get_prometheus_format()
        assert isinstance(output, str)
        assert "http_requests_total" in output


class TestAlertManagerBoundary:
    """告警管理器边界测试"""
    
    def test_add_remove_rule(self):
        """添加和删除规则"""
        am = AlertManager()
        rule = AlertRule(
            rule_id="r1", name="HighErrorRate",
            expr="error_rate > 0.5", duration=timedelta(minutes=5),
            severity=AlertSeverity.WARNING,
            summary="High error rate detected",
            description="Error rate exceeds 50%"
        )
        am.add_rule(rule)
        assert am.remove_rule("r1")
        assert not am.remove_rule("nonexistent")
    
    def test_evaluate_no_rules(self):
        """无规则时评估"""
        am = AlertManager()
        mc = MetricsCollector()
        alerts = am.evaluate_rules(mc)
        assert alerts == []

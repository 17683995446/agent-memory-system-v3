"""
Monitoring & Alerting 深度测试 - 覆盖 monitoring_alerting.py 模块 (之前零覆盖)

测试范围:
- MetricType, AlertSeverity, AlertState 枚举
- Metric, LogEntry, AlertRule, AlertInstance 数据类
- MetricsCollector: counter, gauge, histogram, get_metrics, query, get_prometheus_format
- LogAggregator: log, debug, info, warning, error, query, aggregate
- AlertManager: add_rule, remove_rule, evaluate_rules, get_active_alerts, get_alert_stats
- MonitoringAlertingSystem: record_metric, log, add_alert_rule, evaluate_alerts, get_system_health
"""

import sys
sys.path.insert(0, '/workspace/agent_memory_system_v2')

import pytest
from datetime import datetime, timedelta

from memory_system.monitoring_alerting import (
    MetricType,
    AlertSeverity,
    AlertState,
    Metric,
    LogEntry,
    AlertRule,
    AlertInstance,
    MetricsCollector,
    LogAggregator,
    AlertManager,
    MonitoringAlertingSystem,
)


# ============================================================================
# 枚举测试
# ============================================================================

class TestEnums:
    """枚举类型测试"""

    def test_metric_type_values(self):
        """MetricType 枚举值完整性"""
        assert MetricType.COUNTER.value == "counter"
        assert MetricType.GAUGE.value == "gauge"
        assert MetricType.HISTOGRAM.value == "histogram"
        assert MetricType.SUMMARY.value == "summary"

    def test_alert_severity_values(self):
        """AlertSeverity 枚举值完整性"""
        assert AlertSeverity.CRITICAL.value == "critical"
        assert AlertSeverity.WARNING.value == "warning"
        assert AlertSeverity.INFO.value == "info"

    def test_alert_state_values(self):
        """AlertState 枚举值完整性"""
        assert AlertState.PENDING.value == "pending"
        assert AlertState.FIRING.value == "firing"
        assert AlertState.RESOLVED.value == "resolved"
        assert AlertState.SUPPRESSED.value == "suppressed"


# ============================================================================
# 数据类测试
# ============================================================================

class TestDataClasses:
    """数据类测试"""

    def test_metric_to_dict(self):
        """Metric 序列化"""
        m = Metric(
            name="cpu_usage",
            value=85.5,
            timestamp=datetime(2025, 1, 1, 12, 0, 0),
            labels={"host": "server1"},
            metric_type=MetricType.GAUGE,
        )
        d = m.to_dict()
        assert d["name"] == "cpu_usage"
        assert d["value"] == 85.5
        assert d["type"] == "gauge"
        assert d["labels"] == {"host": "server1"}

    def test_metric_fq_name(self):
        """Metric 完全限定名称"""
        m = Metric(
            name="http_requests",
            value=100,
            timestamp=datetime.utcnow(),
            labels={"method": "GET", "status": "200"},
        )
        fq = m.get_fq_name()
        assert 'http_requests' in fq
        assert 'method="GET"' in fq
        assert 'status="200"' in fq

    def test_metric_fq_name_no_labels(self):
        """Metric 无标签时完全限定名称"""
        m = Metric(name="uptime", value=3600, timestamp=datetime.utcnow())
        fq = m.get_fq_name()
        assert fq == "uptime{}"

    def test_log_entry_to_dict(self):
        """LogEntry 序列化"""
        entry = LogEntry(
            timestamp=datetime(2025, 1, 1, 12, 0, 0),
            level="ERROR",
            message="Connection failed",
            source="db",
            labels={"retry": "3"},
        )
        d = entry.to_dict()
        assert d["level"] == "ERROR"
        assert d["message"] == "Connection failed"
        assert d["source"] == "db"
        assert d["labels"] == {"retry": "3"}

    def test_alert_rule_to_dict(self):
        """AlertRule 序列化"""
        rule = AlertRule(
            rule_id="rule-1",
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(minutes=5),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="CPU usage exceeded 80%",
            labels={"team": "ops"},
        )
        d = rule.to_dict()
        assert d["rule_id"] == "rule-1"
        assert d["name"] == "HighCPU"
        assert d["duration_seconds"] == 300.0
        assert d["severity"] == "warning"

    def test_alert_instance_fingerprint(self):
        """AlertInstance 指纹生成"""
        alert1 = AlertInstance(
            alert_id="a1", rule_id="r1", name="HighCPU",
            severity=AlertSeverity.WARNING, state=AlertState.FIRING,
            value=85.0, labels={"host": "s1"},
            summary="CPU high", description="CPU > 80%",
            starts_at=datetime.utcnow(),
        )
        alert2 = AlertInstance(
            alert_id="a2", rule_id="r1", name="HighCPU",
            severity=AlertSeverity.WARNING, state=AlertState.FIRING,
            value=90.0, labels={"host": "s1"},
            summary="CPU high", description="CPU > 80%",
            starts_at=datetime.utcnow(),
        )
        # 相同 name + labels => 相同指纹
        assert alert1.fingerprint() == alert2.fingerprint()

    def test_alert_instance_different_fingerprint(self):
        """不同 labels 的 AlertInstance 指纹不同"""
        alert1 = AlertInstance(
            alert_id="a1", rule_id="r1", name="HighCPU",
            severity=AlertSeverity.WARNING, state=AlertState.FIRING,
            value=85.0, labels={"host": "s1"},
            summary="CPU high", description="desc",
            starts_at=datetime.utcnow(),
        )
        alert2 = AlertInstance(
            alert_id="a2", rule_id="r1", name="HighCPU",
            severity=AlertSeverity.WARNING, state=AlertState.FIRING,
            value=85.0, labels={"host": "s2"},
            summary="CPU high", description="desc",
            starts_at=datetime.utcnow(),
        )
        assert alert1.fingerprint() != alert2.fingerprint()

    def test_alert_instance_to_dict(self):
        """AlertInstance 序列化"""
        alert = AlertInstance(
            alert_id="a1", rule_id="r1", name="TestAlert",
            severity=AlertSeverity.CRITICAL, state=AlertState.FIRING,
            value=99.0, labels={"env": "prod"},
            summary="Critical alert", description="Something broke",
            starts_at=datetime(2025, 1, 1),
        )
        d = alert.to_dict()
        assert d["alert_id"] == "a1"
        assert d["severity"] == "critical"
        assert d["state"] == "firing"
        assert "fingerprint" in d


# ============================================================================
# MetricsCollector 指标收集器测试
# ============================================================================

class TestMetricsCollector:
    """MetricsCollector 测试"""

    def test_counter_increment(self):
        """Counter 递增"""
        collector = MetricsCollector()
        collector.counter("requests_total", value=1.0)
        collector.counter("requests_total", value=1.0)
        collector.counter("requests_total", value=1.0)

        metrics = collector.get_metrics("requests_total")
        assert len(metrics) == 3
        # 最后一个值应为累计值 3.0
        assert metrics[-1].value == 3.0

    def test_counter_with_labels(self):
        """Counter 带标签"""
        collector = MetricsCollector()
        collector.counter("http_requests", value=1.0, labels={"method": "GET"})
        collector.counter("http_requests", value=1.0, labels={"method": "POST"})

        get_metrics = collector.query("http_requests", labels={"method": "GET"})
        post_metrics = collector.query("http_requests", labels={"method": "POST"})
        assert len(get_metrics) == 1
        assert len(post_metrics) == 1

    def test_gauge_set(self):
        """Gauge 设置值"""
        collector = MetricsCollector()
        collector.gauge("temperature", 36.5)
        collector.gauge("temperature", 37.0)
        collector.gauge("temperature", 36.8)

        metrics = collector.get_metrics("temperature")
        assert len(metrics) == 3
        # Gauge 记录每次设置的值
        assert metrics[0].value == 36.5
        assert metrics[1].value == 37.0
        assert metrics[2].value == 36.8

    def test_gauge_with_labels(self):
        """Gauge 带标签"""
        collector = MetricsCollector()
        collector.gauge("cpu_usage", 80.0, labels={"host": "server1"})
        collector.gauge("cpu_usage", 60.0, labels={"host": "server2"})

        metrics = collector.query("cpu_usage", labels={"host": "server1"})
        assert len(metrics) == 1
        assert metrics[0].value == 80.0

    def test_histogram_record(self):
        """Histogram 记录值"""
        collector = MetricsCollector()
        collector.histogram("request_duration", 0.1)
        collector.histogram("request_duration", 0.5)
        collector.histogram("request_duration", 1.5)

        metrics = collector.get_metrics("request_duration")
        assert len(metrics) == 3
        assert all(m.metric_type == MetricType.HISTOGRAM for m in metrics)

    def test_histogram_custom_buckets(self):
        """Histogram 自定义桶"""
        collector = MetricsCollector()
        collector.histogram("latency", 0.01, buckets=[0.01, 0.05, 0.1])
        collector.histogram("latency", 0.03, buckets=[0.01, 0.05, 0.1])
        collector.histogram("latency", 0.08, buckets=[0.01, 0.05, 0.1])

        # Prometheus 格式应包含桶信息
        prom = collector.get_prometheus_format()
        assert "latency_bucket" in prom
        assert "latency_sum" in prom
        assert "latency_count" in prom

    def test_get_metrics_all(self):
        """获取所有指标"""
        collector = MetricsCollector()
        collector.counter("m1", 1.0)
        collector.gauge("m2", 42.0)
        collector.histogram("m3", 0.5)

        all_metrics = collector.get_metrics()
        assert len(all_metrics) >= 3

    def test_get_metrics_by_name(self):
        """按名称获取指标"""
        collector = MetricsCollector()
        collector.counter("target_metric", 1.0)
        collector.counter("other_metric", 1.0)

        metrics = collector.get_metrics("target_metric")
        assert len(metrics) == 1
        assert metrics[0].name == "target_metric"

    def test_get_metrics_nonexistent(self):
        """获取不存在的指标名称"""
        collector = MetricsCollector()
        metrics = collector.get_metrics("nonexistent")
        assert len(metrics) == 0

    def test_query_by_labels(self):
        """按标签查询指标"""
        collector = MetricsCollector()
        collector.gauge("cpu", 80.0, labels={"host": "s1", "dc": "us"})
        collector.gauge("cpu", 60.0, labels={"host": "s2", "dc": "us"})
        collector.gauge("cpu", 70.0, labels={"host": "s1", "dc": "eu"})

        results = collector.query("cpu", labels={"host": "s1"})
        assert len(results) == 2

        results = collector.query("cpu", labels={"dc": "eu"})
        assert len(results) == 1

    def test_query_by_time_range(self):
        """按时间范围查询指标"""
        collector = MetricsCollector()
        now = datetime.utcnow()
        collector.gauge("m1", 1.0)
        collector.gauge("m1", 2.0)

        # 查询从 now 开始的所有指标
        results = collector.query("m1", start=now)
        assert len(results) >= 1

    def test_prometheus_format_counter(self):
        """Prometheus 格式输出 - Counter"""
        collector = MetricsCollector()
        collector.counter("requests_total", 5.0, labels={"method": "GET"})

        output = collector.get_prometheus_format()
        assert "# TYPE requests_total counter" in output
        assert "requests_total" in output
        assert 'method="GET"' in output

    def test_prometheus_format_gauge(self):
        """Prometheus 格式输出 - Gauge"""
        collector = MetricsCollector()
        collector.gauge("temperature", 36.5)

        output = collector.get_prometheus_format()
        assert "# TYPE temperature gauge" in output
        assert "temperature" in output

    def test_prometheus_format_empty(self):
        """空收集器的 Prometheus 格式"""
        collector = MetricsCollector()
        output = collector.get_prometheus_format()
        assert output == ""


# ============================================================================
# LogAggregator 日志聚合器测试
# ============================================================================

class TestLogAggregator:
    """LogAggregator 测试"""

    def test_log_basic(self):
        """基本日志记录"""
        aggregator = LogAggregator()
        aggregator.log("INFO", "Test message", source="test")

        logs = aggregator.query()
        assert len(logs) == 1
        assert logs[0].level == "INFO"
        assert logs[0].message == "Test message"
        assert logs[0].source == "test"

    def test_log_debug(self):
        """DEBUG 级别日志"""
        aggregator = LogAggregator()
        aggregator.debug("Debug msg", source="app")
        logs = aggregator.query(level="DEBUG")
        assert len(logs) == 1

    def test_log_info(self):
        """INFO 级别日志"""
        aggregator = LogAggregator()
        aggregator.info("Info msg", source="app")
        logs = aggregator.query(level="INFO")
        assert len(logs) == 1

    def test_log_warning(self):
        """WARNING 级别日志"""
        aggregator = LogAggregator()
        aggregator.warning("Warning msg", source="monitor")
        logs = aggregator.query(level="WARNING")
        assert len(logs) == 1

    def test_log_error(self):
        """ERROR 级别日志"""
        aggregator = LogAggregator()
        aggregator.error("Error msg", source="db")
        logs = aggregator.query(level="ERROR")
        assert len(logs) == 1

    def test_log_level_case_insensitive(self):
        """日志级别大小写不敏感"""
        aggregator = LogAggregator()
        aggregator.log("warning", "msg", source="s")
        logs = aggregator.query(level="WARNING")
        assert len(logs) == 1

    def test_query_by_level(self):
        """按级别过滤日志"""
        aggregator = LogAggregator()
        aggregator.info("info1", source="s")
        aggregator.error("error1", source="s")
        aggregator.info("info2", source="s")
        aggregator.debug("debug1", source="s")

        error_logs = aggregator.query(level="ERROR")
        assert len(error_logs) == 1

        info_logs = aggregator.query(level="INFO")
        assert len(info_logs) == 2

    def test_query_by_source(self):
        """按来源过滤日志"""
        aggregator = LogAggregator()
        aggregator.info("msg1", source="app")
        aggregator.info("msg2", source="db")
        aggregator.info("msg3", source="app")

        app_logs = aggregator.query(source="app")
        assert len(app_logs) == 2

        db_logs = aggregator.query(source="db")
        assert len(db_logs) == 1

    def test_query_by_pattern(self):
        """按模式匹配日志"""
        aggregator = LogAggregator()
        aggregator.error("Database connection timeout", source="db")
        aggregator.error("Cache miss rate high", source="cache")
        aggregator.info("User logged in", source="auth")

        timeout_logs = aggregator.query(pattern="timeout")
        assert len(timeout_logs) == 1
        assert "timeout" in timeout_logs[0].message.lower()

        connection_logs = aggregator.query(pattern="connection")
        assert len(connection_logs) == 1

    def test_query_limit(self):
        """查询结果限制"""
        aggregator = LogAggregator()
        for i in range(20):
            aggregator.info(f"Message {i}", source="test")

        logs = aggregator.query(limit=5)
        assert len(logs) == 5

    def test_aggregate_stats(self):
        """日志聚合统计"""
        aggregator = LogAggregator()
        aggregator.info("msg1", source="app")
        aggregator.info("msg2", source="app")
        aggregator.error("err1", source="db")
        aggregator.warning("warn1", source="monitor")
        aggregator.debug("dbg1", source="app")

        stats = aggregator.aggregate()
        assert stats["total"] == 5
        assert stats["by_level"]["INFO"] == 2
        assert stats["by_level"]["ERROR"] == 1
        assert stats["by_level"]["WARNING"] == 1
        assert stats["by_level"]["DEBUG"] == 1
        assert stats["by_source"]["app"] == 3
        assert stats["by_source"]["db"] == 1

    def test_aggregate_time_window(self):
        """聚合统计 - 时间窗口"""
        aggregator = LogAggregator()
        aggregator.info("recent", source="s")

        # 窄时间窗口
        stats = aggregator.aggregate(time_window=timedelta(seconds=1))
        assert stats["total"] == 1

        # 宽时间窗口
        stats = aggregator.aggregate(time_window=timedelta(hours=1))
        assert stats["total"] == 1

    def test_log_with_labels(self):
        """带标签的日志"""
        aggregator = LogAggregator()
        aggregator.info("msg", source="app", labels={"version": "1.0", "env": "prod"})

        logs = aggregator.query()
        assert len(logs) == 1
        assert logs[0].labels == {"version": "1.0", "env": "prod"}


# ============================================================================
# AlertManager 告警管理器测试
# ============================================================================

class TestAlertManager:
    """AlertManager 测试"""

    def test_add_rule(self):
        """添加告警规则"""
        am = AlertManager()
        rule = AlertRule(
            rule_id="r1",
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(seconds=60),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="CPU exceeded 80%",
        )
        am.add_rule(rule)
        assert "r1" in am._rules

    def test_remove_rule(self):
        """删除告警规则"""
        am = AlertManager()
        rule = AlertRule(
            rule_id="r1",
            name="Test",
            expr="x > 1",
            duration=timedelta(seconds=60),
            severity=AlertSeverity.INFO,
            summary="test",
            description="test",
        )
        am.add_rule(rule)
        assert am.remove_rule("r1") is True
        assert am.remove_rule("r1") is False

    def test_remove_nonexistent_rule(self):
        """删除不存在的规则"""
        am = AlertManager()
        assert am.remove_rule("nonexistent") is False

    def test_evaluate_rules_no_trigger(self):
        """告警评估 - 未触发"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("cpu_usage", 50.0)

        rule = AlertRule(
            rule_id="r1",
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(seconds=60),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="CPU exceeded 80%",
        )
        am.add_rule(rule)

        new_alerts = am.evaluate_rules(collector)
        assert len(new_alerts) == 0
        assert len(am.get_active_alerts()) == 0

    def test_evaluate_rules_trigger(self):
        """告警评估 - 触发告警"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("cpu_usage", 95.0)

        rule = AlertRule(
            rule_id="r1",
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.CRITICAL,
            summary="CPU critical",
            description="CPU exceeded 80%",
        )
        am.add_rule(rule)

        new_alerts = am.evaluate_rules(collector)
        assert len(new_alerts) == 1
        assert new_alerts[0].name == "HighCPU"
        assert new_alerts[0].severity == AlertSeverity.CRITICAL
        assert new_alerts[0].state == AlertState.PENDING

    def test_evaluate_rules_disabled(self):
        """告警评估 - 禁用的规则不触发"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("cpu_usage", 99.0)

        rule = AlertRule(
            rule_id="r1",
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="desc",
            enabled=False,
        )
        am.add_rule(rule)

        new_alerts = am.evaluate_rules(collector)
        assert len(new_alerts) == 0

    def test_evaluate_rules_resolve(self):
        """告警评估 - 条件恢复后告警解决"""
        am = AlertManager()
        collector = MetricsCollector()

        rule = AlertRule(
            rule_id="r1",
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="desc",
        )
        am.add_rule(rule)

        # 触发
        collector.gauge("cpu_usage", 95.0)
        am.evaluate_rules(collector)
        assert len(am.get_active_alerts()) == 1

        # 恢复
        collector.gauge("cpu_usage", 50.0)
        am.evaluate_rules(collector)
        assert len(am.get_active_alerts()) == 0

    def test_evaluate_rules_greater_equal(self):
        """告警评估 - >= 操作符"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("disk_usage", 80.0)

        rule = AlertRule(
            rule_id="r1",
            name="DiskFull",
            expr="disk_usage >= 80",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.WARNING,
            summary="Disk full",
            description="desc",
        )
        am.add_rule(rule)

        new_alerts = am.evaluate_rules(collector)
        assert len(new_alerts) == 1

    def test_evaluate_rules_less_than(self):
        """告警评估 - < 操作符"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("free_memory", 10.0)

        rule = AlertRule(
            rule_id="r1",
            name="LowMemory",
            expr="free_memory < 100",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.CRITICAL,
            summary="Low memory",
            description="desc",
        )
        am.add_rule(rule)

        new_alerts = am.evaluate_rules(collector)
        assert len(new_alerts) == 1

    def test_evaluate_rules_invalid_expr(self):
        """告警评估 - 无效表达式不触发"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("cpu", 99.0)

        rule = AlertRule(
            rule_id="r1",
            name="BadExpr",
            expr="this is not a valid expression",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.WARNING,
            summary="bad",
            description="bad",
        )
        am.add_rule(rule)

        new_alerts = am.evaluate_rules(collector)
        assert len(new_alerts) == 0

    def test_get_active_alerts(self):
        """获取活动告警列表"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("cpu", 95.0)

        rule = AlertRule(
            rule_id="r1",
            name="HighCPU",
            expr="cpu > 80",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="desc",
        )
        am.add_rule(rule)
        am.evaluate_rules(collector)

        active = am.get_active_alerts()
        assert len(active) == 1
        assert active[0].name == "HighCPU"

    def test_get_alert_stats(self):
        """获取告警统计"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("cpu", 95.0)
        collector.gauge("memory", 95.0)

        rule1 = AlertRule(
            rule_id="r1", name="HighCPU", expr="cpu > 80",
            duration=timedelta(seconds=0), severity=AlertSeverity.CRITICAL,
            summary="CPU", description="desc",
        )
        rule2 = AlertRule(
            rule_id="r2", name="HighMemory", expr="memory > 80",
            duration=timedelta(seconds=0), severity=AlertSeverity.WARNING,
            summary="Memory", description="desc",
        )
        am.add_rule(rule1)
        am.add_rule(rule2)
        am.evaluate_rules(collector)

        stats = am.get_alert_stats()
        assert stats["active_count"] == 2
        assert stats["by_severity"]["critical"] == 1
        assert stats["by_severity"]["warning"] == 1

    def test_alert_state_transition_pending_to_firing(self):
        """告警状态转换 - PENDING -> FIRING"""
        am = AlertManager()
        collector = MetricsCollector()
        collector.gauge("cpu", 95.0)

        rule = AlertRule(
            rule_id="r1", name="HighCPU", expr="cpu > 80",
            duration=timedelta(seconds=0), severity=AlertSeverity.WARNING,
            summary="CPU", description="desc",
        )
        am.add_rule(rule)

        # 第一次评估 -> PENDING
        new_alerts = am.evaluate_rules(collector)
        assert new_alerts[0].state == AlertState.PENDING

        # process_notifications 应将 PENDING 转为 FIRING (duration=0)
        am.process_notifications()
        active = am.get_active_alerts()
        assert active[0].state == AlertState.FIRING


# ============================================================================
# MonitoringAlertingSystem 集成测试
# ============================================================================

class TestMonitoringAlertingSystem:
    """MonitoringAlertingSystem 集成测试"""

    def test_record_metric_counter(self):
        """记录 Counter 指标"""
        system = MonitoringAlertingSystem()
        system.record_metric("requests", 1.0, MetricType.COUNTER)
        system.record_metric("requests", 1.0, MetricType.COUNTER)

        metrics = system.metrics.get_metrics("requests")
        assert len(metrics) == 2

    def test_record_metric_gauge(self):
        """记录 Gauge 指标"""
        system = MonitoringAlertingSystem()
        system.record_metric("temperature", 36.5, MetricType.GAUGE)

        metrics = system.metrics.get_metrics("temperature")
        assert len(metrics) == 1
        assert metrics[0].value == 36.5

    def test_record_metric_histogram(self):
        """记录 Histogram 指标"""
        system = MonitoringAlertingSystem()
        system.record_metric("latency", 0.123, MetricType.HISTOGRAM)

        metrics = system.metrics.get_metrics("latency")
        assert len(metrics) == 1
        assert metrics[0].metric_type == MetricType.HISTOGRAM

    def test_log_message(self):
        """记录日志消息"""
        system = MonitoringAlertingSystem()
        system.log("ERROR", "Something failed", source="test")

        logs = system.logs.query(level="ERROR")
        assert len(logs) == 1
        assert logs[0].message == "Something failed"

    def test_add_alert_rule(self):
        """添加告警规则"""
        system = MonitoringAlertingSystem()
        rule = system.add_alert_rule(
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(minutes=5),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="CPU exceeded 80%",
        )
        assert rule.name == "HighCPU"
        assert rule.rule_id is not None

    def test_evaluate_alerts(self):
        """评估告警"""
        system = MonitoringAlertingSystem()
        system.record_metric("cpu_usage", 95.0, MetricType.GAUGE)

        system.add_alert_rule(
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.CRITICAL,
            summary="CPU critical",
            description="desc",
        )

        alerts = system.evaluate_alerts()
        assert len(alerts) == 1
        assert alerts[0].name == "HighCPU"

    def test_evaluate_alerts_no_trigger(self):
        """评估告警 - 未触发"""
        system = MonitoringAlertingSystem()
        system.record_metric("cpu_usage", 50.0, MetricType.GAUGE)

        system.add_alert_rule(
            name="HighCPU",
            expr="cpu_usage > 80",
            duration=timedelta(seconds=0),
            severity=AlertSeverity.WARNING,
            summary="CPU high",
            description="desc",
        )

        alerts = system.evaluate_alerts()
        assert len(alerts) == 0

    def test_get_system_health(self):
        """系统健康检查"""
        system = MonitoringAlertingSystem()
        system.record_metric("cpu", 80.0, MetricType.GAUGE)
        system.log("INFO", "System started", source="app")

        health = system.get_system_health()
        assert "timestamp" in health
        assert "metrics" in health
        assert "logs" in health
        assert "alerts" in health
        assert health["metrics"]["total_metrics"] >= 1
        assert health["logs"]["total"] >= 1

    def test_system_health_empty(self):
        """空系统的健康检查"""
        system = MonitoringAlertingSystem()
        health = system.get_system_health()
        assert health["metrics"]["total_metrics"] == 0
        assert health["logs"]["total"] == 0
        assert health["alerts"]["active_count"] == 0

    def test_dashboard_creation(self):
        """创建仪表盘"""
        system = MonitoringAlertingSystem()
        dashboard = system.create_dashboard(
            name="Overview",
            description="System overview"
        )
        assert dashboard.name == "Overview"
        assert system.get_dashboard("Overview") is not None

    def test_dashboard_not_found(self):
        """获取不存在的仪表盘"""
        system = MonitoringAlertingSystem()
        assert system.get_dashboard("nonexistent") is None

    def test_start_and_stop(self):
        """启动和停止系统"""
        system = MonitoringAlertingSystem()
        system.start()
        # 运行中无异常
        system.stop()

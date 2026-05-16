#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
监控告警系统 (Monitoring and Alerting System)

提供Prometheus风格的指标收集、日志聚合分析、多维度告警规则、
多渠道告警通知、告警抑制和升级、仪表盘可视化等功能。

Author: Agent Memory System Team
Version: 3.0.0
"""

import asyncio
import hashlib
import json
import logging
import re
import smtplib
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

# 配置日志
logger = logging.getLogger(__name__)


class MetricType(Enum):
    """指标类型"""
    COUNTER = "counter"      # 计数器(单调递增)
    GAUGE = "gauge"          # 仪表盘(可增可减)
    HISTOGRAM = "histogram"  # 直方图
    SUMMARY = "summary"      # 摘要


class AlertSeverity(Enum):
    """告警严重级别"""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertState(Enum):
    """告警状态"""
    PENDING = "pending"
    FIRING = "firing"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


@dataclass
class Metric:
    """指标数据点"""
    name: str
    value: float
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: MetricType = MetricType.GAUGE
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "labels": self.labels,
            "type": self.metric_type.value,
        }
    
    def get_fq_name(self) -> str:
        """获取完全限定名称(包含标签)"""
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(self.labels.items()))
        return f"{self.name}{{{label_str}}}"


@dataclass
class LogEntry:
    """日志条目"""
    timestamp: datetime
    level: str
    message: str
    source: str
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level,
            "message": self.message,
            "source": self.source,
            "labels": self.labels,
        }


@dataclass
class AlertRule:
    """告警规则"""
    rule_id: str
    name: str
    expr: str  # 表达式
    duration: timedelta
    severity: AlertSeverity
    summary: str
    description: str
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    enabled: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "expr": self.expr,
            "duration_seconds": self.duration.total_seconds(),
            "severity": self.severity.value,
            "summary": self.summary,
            "description": self.description,
            "labels": self.labels,
            "annotations": self.annotations,
            "enabled": self.enabled,
        }


@dataclass
class AlertInstance:
    """告警实例"""
    alert_id: str
    rule_id: str
    name: str
    severity: AlertSeverity
    state: AlertState
    value: float
    labels: Dict[str, str]
    summary: str
    description: str
    starts_at: datetime
    ends_at: Optional[datetime] = None
    last_evaluated: Optional[datetime] = None
    notification_count: int = 0
    
    def fingerprint(self) -> str:
        """生成告警指纹(用于去重)"""
        content = f"{self.name}:{sorted(self.labels.items())}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "rule_id": self.rule_id,
            "name": self.name,
            "severity": self.severity.value,
            "state": self.state.value,
            "value": self.value,
            "labels": self.labels,
            "summary": self.summary,
            "description": self.description,
            "starts_at": self.starts_at.isoformat(),
            "ends_at": self.ends_at.isoformat() if self.ends_at else None,
            "fingerprint": self.fingerprint(),
        }


class MetricsCollector:
    """
    指标收集器 (Prometheus风格)
    
    DEFECT-007 [P2] 新增使用示例:
    
    示例用法:
        >>> from monitoring_alerting import MetricsCollector
        >>> 
        >>> collector = MetricsCollector()
        >>> 
        >>> # 记录计数器
        >>> collector.counter("http_requests_total", labels={"method": "GET", "status": "200"})
        >>> 
        >>> # 记录仪表盘值
        >>> collector.gauge("active_connections", 42)
        >>> 
        >>> # 记录直方图
        >>> collector.histogram("request_duration_seconds", 0.123, labels={"endpoint": "/api"})
        >>> 
        >>> # 获取指标
        >>> metrics = collector.get_metrics()
    """
    
    def __init__(self, max_history: int = 10000):
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._histograms: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"buckets": defaultdict(int), "sum": 0.0, "count": 0}
        )
        self._lock = threading.RLock()
    
    def counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """增加计数器"""
        with self._lock:
            key = self._make_key(name, labels or {})
            self._counters[key] += value
            
            metric = Metric(
                name=name,
                value=self._counters[key],
                timestamp=datetime.utcnow(),
                labels=labels or {},
                metric_type=MetricType.COUNTER,
            )
            self._metrics[name].append(metric)
    
    def gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """设置仪表盘值"""
        with self._lock:
            key = self._make_key(name, labels or {})
            self._gauges[key] = value
            
            metric = Metric(
                name=name,
                value=value,
                timestamp=datetime.utcnow(),
                labels=labels or {},
                metric_type=MetricType.GAUGE,
            )
            self._metrics[name].append(metric)
    
    def histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None,
                  buckets: Optional[List[float]] = None) -> None:
        """记录直方图值"""
        with self._lock:
            key = self._make_key(name, labels or {})
            hist = self._histograms[key]
            
            # 默认桶边界
            if buckets is None:
                buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
            
            # 更新桶计数
            for bucket in buckets:
                if value <= bucket:
                    hist["buckets"][bucket] += 1
            hist["buckets"]["+Inf"] += 1
            
            hist["sum"] += value
            hist["count"] += 1
            
            metric = Metric(
                name=name,
                value=value,
                timestamp=datetime.utcnow(),
                labels=labels or {},
                metric_type=MetricType.HISTOGRAM,
            )
            self._metrics[name].append(metric)
    
    def _make_key(self, name: str, labels: Dict[str, str]) -> str:
        """生成唯一键"""
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}:{label_str}"
    
    def get_metrics(self, name: Optional[str] = None) -> List[Metric]:
        """获取指标"""
        with self._lock:
            if name:
                return list(self._metrics.get(name, []))
            
            all_metrics = []
            for metrics in self._metrics.values():
                all_metrics.extend(metrics)
            return all_metrics
    
    def query(self, name: str, labels: Optional[Dict[str, str]] = None,
              start: Optional[datetime] = None, end: Optional[datetime] = None) -> List[Metric]:
        """查询指标"""
        with self._lock:
            metrics = list(self._metrics.get(name, []))
        
        # 过滤
        if labels:
            metrics = [m for m in metrics if all(m.labels.get(k) == v for k, v in labels.items())]
        
        if start:
            metrics = [m for m in metrics if m.timestamp >= start]
        
        if end:
            metrics = [m for m in metrics if m.timestamp <= end]
        
        return metrics
    
    def get_prometheus_format(self) -> str:
        """导出Prometheus格式"""
        lines = []
        
        with self._lock:
            # Counters
            for key, value in self._counters.items():
                name, labels = self._parse_key(key)
                label_str = self._format_labels(labels)
                lines.append(f"# TYPE {name} counter")
                lines.append(f"{name}{label_str} {value}")
            
            # Gauges
            for key, value in self._gauges.items():
                name, labels = self._parse_key(key)
                label_str = self._format_labels(labels)
                lines.append(f"# TYPE {name} gauge")
                lines.append(f"{name}{label_str} {value}")
            
            # Histograms
            for key, hist in self._histograms.items():
                name, labels = self._parse_key(key)
                label_str = self._format_labels(labels)
                
                lines.append(f"# TYPE {name} histogram")
                
                for bucket, count in sorted(hist["buckets"].items()):
                    bucket_label = f'le="{bucket}"' if bucket != "+Inf" else 'le="+Inf"'
                    full_labels = label_str.rstrip("}") + f',{bucket_label}}}' if label_str != "{}" else f"{{{bucket_label}}}"
                    lines.append(f"{name}_bucket{full_labels} {count}")
                
                lines.append(f"{name}_sum{label_str} {hist['sum']}")
                lines.append(f"{name}_count{label_str} {hist['count']}")
        
        return "\n".join(lines)
    
    def _parse_key(self, key: str) -> Tuple[str, Dict[str, str]]:
        """解析键"""
        if ":" in key:
            name, label_str = key.split(":", 1)
            labels = {}
            for pair in label_str.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    labels[k] = v
            return name, labels
        return key, {}
    
    def _format_labels(self, labels: Dict[str, str]) -> str:
        """格式化标签"""
        if not labels:
            return "{}"
        return "{" + ",".join(f'{k}="{v}"' for k, v in sorted(labels.items())) + "}"


class LogAggregator:
    """日志聚合器"""
    
    def __init__(self, max_entries: int = 100000):
        self._logs: deque = deque(maxlen=max_entries)
        self._lock = threading.RLock()
        self._index: Dict[str, Set[int]] = defaultdict(set)  # 简单索引
    
    def log(self, level: str, message: str, source: str = "",
            labels: Optional[Dict[str, str]] = None) -> None:
        """记录日志"""
        entry = LogEntry(
            timestamp=datetime.utcnow(),
            level=level.upper(),
            message=message,
            source=source,
            labels=labels or {},
        )
        
        with self._lock:
            idx = len(self._logs)
            self._logs.append(entry)
            
            # 更新索引
            self._index[f"level:{entry.level}"].add(idx)
            self._index[f"source:{entry.source}"].add(idx)
            
            for key, value in entry.labels.items():
                self._index[f"{key}:{value}"].add(idx)
    
    def debug(self, message: str, source: str = "", labels: Optional[Dict[str, str]] = None) -> None:
        self.log("DEBUG", message, source, labels)
    
    def info(self, message: str, source: str = "", labels: Optional[Dict[str, str]] = None) -> None:
        self.log("INFO", message, source, labels)
    
    def warning(self, message: str, source: str = "", labels: Optional[Dict[str, str]] = None) -> None:
        self.log("WARNING", message, source, labels)
    
    def error(self, message: str, source: str = "", labels: Optional[Dict[str, str]] = None) -> None:
        self.log("ERROR", message, source, labels)
    
    def query(self, level: Optional[str] = None, source: Optional[str] = None,
              pattern: Optional[str] = None, start: Optional[datetime] = None,
              end: Optional[datetime] = None, limit: int = 100) -> List[LogEntry]:
        """查询日志"""
        with self._lock:
            logs = list(self._logs)
        
        results = logs
        
        # 按级别过滤
        if level:
            results = [e for e in results if e.level == level.upper()]
        
        # 按源过滤
        if source:
            results = [e for e in results if e.source == source]
        
        # 按模式过滤
        if pattern:
            regex = re.compile(pattern, re.IGNORECASE)
            results = [e for e in results if regex.search(e.message)]
        
        # 按时间过滤
        if start:
            results = [e for e in results if e.timestamp >= start]
        
        if end:
            results = [e for e in results if e.timestamp <= end]
        
        return results[-limit:]
    
    def aggregate(self, time_window: timedelta = timedelta(minutes=5)) -> Dict[str, Any]:
        """聚合统计"""
        with self._lock:
            logs = list(self._logs)
        
        cutoff = datetime.utcnow() - time_window
        recent_logs = [e for e in logs if e.timestamp >= cutoff]
        
        level_counts = defaultdict(int)
        source_counts = defaultdict(int)
        
        for entry in recent_logs:
            level_counts[entry.level] += 1
            source_counts[entry.source] += 1
        
        return {
            "total": len(recent_logs),
            "by_level": dict(level_counts),
            "by_source": dict(source_counts),
            "time_window_minutes": time_window.total_seconds() / 60,
        }


class AlertNotifier(ABC):
    """告警通知器抽象基类"""
    
    @abstractmethod
    def notify(self, alert: AlertInstance) -> bool:
        """发送通知"""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """获取通知器名称"""
        pass


class EmailNotifier(AlertNotifier):
    """邮件通知器"""
    
    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str,
        password: str,
        from_addr: str,
        to_addrs: List[str],
        use_tls: bool = True
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_addr = from_addr
        self.to_addrs = to_addrs
        self.use_tls = use_tls
    
    def get_name(self) -> str:
        return "email"
    
    def notify(self, alert: AlertInstance) -> bool:
        """发送邮件通知"""
        try:
            subject = f"[{alert.severity.value.upper()}] {alert.name}"
            
            body = f"""
Alert: {alert.name}
Severity: {alert.severity.value}
State: {alert.state.value}
Value: {alert.value}
Time: {alert.starts_at.isoformat()}

Summary: {alert.summary}
Description: {alert.description}

Labels:
{json.dumps(alert.labels, indent=2)}
"""
            
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = self.from_addr
            msg["To"] = ", ".join(self.to_addrs)
            
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            if self.use_tls:
                server.starttls()
            server.login(self.username, self.password)
            server.sendmail(self.from_addr, self.to_addrs, msg.as_string())
            server.quit()
            
            return True
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False


class WebhookNotifier(AlertNotifier):
    """Webhook通知器"""
    
    def __init__(self, webhook_url: str, headers: Optional[Dict[str, str]] = None):
        self.webhook_url = webhook_url
        self.headers = headers or {"Content-Type": "application/json"}
    
    def get_name(self) -> str:
        return "webhook"
    
    def notify(self, alert: AlertInstance) -> bool:
        """发送Webhook通知"""
        try:
            import requests
            
            payload = {
                "alert": alert.to_dict(),
                "timestamp": datetime.utcnow().isoformat(),
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
                timeout=30
            )
            
            return response.status_code < 400
        except Exception as e:
            logger.error(f"Failed to send webhook notification: {e}")
            return False


class SlackNotifier(AlertNotifier):
    """Slack通知器"""
    
    def __init__(self, webhook_url: str, channel: Optional[str] = None):
        self.webhook_url = webhook_url
        self.channel = channel
    
    def get_name(self) -> str:
        return "slack"
    
    def notify(self, alert: AlertInstance) -> bool:
        """发送Slack通知"""
        try:
            import requests
            
            color = {
                AlertSeverity.CRITICAL: "danger",
                AlertSeverity.WARNING: "warning",
                AlertSeverity.INFO: "good",
            }.get(alert.severity, "#808080")
            
            payload = {
                "attachments": [{
                    "color": color,
                    "title": f"[{alert.severity.value.upper()}] {alert.name}",
                    "text": alert.description,
                    "fields": [
                        {"title": "State", "value": alert.state.value, "short": True},
                        {"title": "Value", "value": str(alert.value), "short": True},
                        {"title": "Labels", "value": json.dumps(alert.labels), "short": False},
                    ],
                    "footer": "Agent Memory System",
                    "ts": int(time.time()),
                }]
            }
            
            if self.channel:
                payload["channel"] = self.channel
            
            response = requests.post(self.webhook_url, json=payload, timeout=30)
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False


class DingTalkNotifier(AlertNotifier):
    """钉钉通知器"""
    
    def __init__(self, webhook_url: str, secret: Optional[str] = None):
        self.webhook_url = webhook_url
        self.secret = secret
    
    def get_name(self) -> str:
        return "dingtalk"
    
    def notify(self, alert: AlertInstance) -> bool:
        """发送钉钉通知"""
        try:
            import requests
            import hmac
            import base64
            import urllib.parse
            
            timestamp = str(round(time.time() * 1000))
            
            # 签名
            if self.secret:
                string_to_sign = f"{timestamp}\n{self.secret}"
                hmac_code = hmac.new(
                    self.secret.encode(),
                    string_to_sign.encode(),
                    digestmod=hashlib.sha256
                ).digest()
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
                url = f"{self.webhook_url}&timestamp={timestamp}&sign={sign}"
            else:
                url = self.webhook_url
            
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"告警: {alert.name}",
                    "text": f"""### [{alert.severity.value.upper()}] {alert.name}

**状态**: {alert.state.value}

**值**: {alert.value}

**描述**: {alert.description}

**时间**: {alert.starts_at.isoformat()}

**标签**: {json.dumps(alert.labels, ensure_ascii=False)}
"""
                }
            }
            
            response = requests.post(url, json=payload, timeout=30)
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Failed to send DingTalk notification: {e}")
            return False


class AlertManager:
    """告警管理器"""
    
    def __init__(self, evaluation_interval: float = 30.0):
        self._rules: Dict[str, AlertRule] = {}
        self._active_alerts: Dict[str, AlertInstance] = {}
        self._alert_history: deque = deque(maxlen=10000)
        self._notifiers: List[AlertNotifier] = []
        self._suppression_rules: List[Dict[str, Any]] = []
        self._inhibition_rules: List[Dict[str, Any]] = []
        
        self.evaluation_interval = evaluation_interval
        self._lock = threading.RLock()
        self._running = False
        self._eval_thread: Optional[threading.Thread] = None
    
    def add_rule(self, rule: AlertRule) -> None:
        """添加告警规则"""
        with self._lock:
            self._rules[rule.rule_id] = rule
    
    def remove_rule(self, rule_id: str) -> bool:
        """移除告警规则"""
        with self._lock:
            if rule_id in self._rules:
                del self._rules[rule_id]
                return True
            return False
    
    def add_notifier(self, notifier: AlertNotifier) -> None:
        """添加通知器"""
        self._notifiers.append(notifier)
    
    def add_suppression_rule(self, matchers: Dict[str, str], duration: timedelta) -> None:
        """添加抑制规则"""
        self._suppression_rules.append({
            "matchers": matchers,
            "duration": duration,
            "created_at": datetime.utcnow(),
        })
    
    def evaluate_rules(self, metrics_collector: MetricsCollector) -> List[AlertInstance]:
        """评估告警规则"""
        new_alerts = []
        
        with self._lock:
            rules = list(self._rules.values())
        
        for rule in rules:
            if not rule.enabled:
                continue
            
            # 解析表达式 (简化实现)
            triggered, value, labels = self._evaluate_expr(rule.expr, metrics_collector)
            
            if triggered:
                alert_key = f"{rule.rule_id}:{sorted(labels.items())}"
                
                with self._lock:
                    if alert_key in self._active_alerts:
                        # 更新现有告警
                        alert = self._active_alerts[alert_key]
                        alert.value = value
                        alert.last_evaluated = datetime.utcnow()
                    else:
                        # 创建新告警
                        alert = AlertInstance(
                            alert_id=str(uuid.uuid4()),
                            rule_id=rule.rule_id,
                            name=rule.name,
                            severity=rule.severity,
                            state=AlertState.PENDING,
                            value=value,
                            labels={**rule.labels, **labels},
                            summary=rule.summary,
                            description=rule.description,
                            starts_at=datetime.utcnow(),
                        )
                        self._active_alerts[alert_key] = alert
                        new_alerts.append(alert)
            else:
                # 检查是否需要解决
                alert_key = f"{rule.rule_id}:{sorted(labels.items())}"
                with self._lock:
                    if alert_key in self._active_alerts:
                        alert = self._active_alerts[alert_key]
                        alert.state = AlertState.RESOLVED
                        alert.ends_at = datetime.utcnow()
                        self._alert_history.append(alert)
                        del self._active_alerts[alert_key]
        
        return new_alerts
    
    def _evaluate_expr(self, expr: str, metrics_collector: MetricsCollector) -> Tuple[bool, float, Dict[str, str]]:
        """评估表达式 (简化实现)"""
        # 支持简单的表达式: metric_name > threshold
        # 例如: cpu_usage > 80
        
        match = re.match(r"(\w+)\s*([><=!]+)\s*([\d.]+)", expr)
        if not match:
            return False, 0.0, {}
        
        metric_name, operator, threshold_str = match.groups()
        threshold = float(threshold_str)
        
        # 获取最新指标值
        metrics = metrics_collector.query(metric_name)
        if not metrics:
            return False, 0.0, {}
        
        latest = metrics[-1]
        value = latest.value
        
        # 评估条件
        triggered = False
        if operator == ">":
            triggered = value > threshold
        elif operator == ">=":
            triggered = value >= threshold
        elif operator == "<":
            triggered = value < threshold
        elif operator == "<=":
            triggered = value <= threshold
        elif operator == "==":
            triggered = value == threshold
        
        return triggered, value, latest.labels
    
    def process_notifications(self) -> None:
        """处理通知"""
        with self._lock:
            alerts = list(self._active_alerts.values())
        
        for alert in alerts:
            # 检查抑制
            if self._is_suppressed(alert):
                if alert.state != AlertState.SUPPRESSED:
                    alert.state = AlertState.SUPPRESSED
                continue
            
            # 检查是否需要发送通知
            if alert.state == AlertState.PENDING:
                # 检查持续时间
                if datetime.utcnow() - alert.starts_at >= self._rules[alert.rule_id].duration:
                    alert.state = AlertState.FIRING
                    self._send_notifications(alert)
            elif alert.state == AlertState.FIRING:
                # 重试通知 (每30分钟)
                if alert.notification_count == 0 or \
                   (datetime.utcnow() - alert.starts_at).total_seconds() / 1800 > alert.notification_count:
                    self._send_notifications(alert)
    
    def _is_suppressed(self, alert: AlertInstance) -> bool:
        """检查告警是否被抑制"""
        now = datetime.utcnow()
        
        for rule in self._suppression_rules:
            matchers = rule["matchers"]
            duration = rule["duration"]
            created_at = rule["created_at"]
            
            # 检查规则是否过期
            if now - created_at > duration:
                continue
            
            # 检查是否匹配
            if all(alert.labels.get(k) == v for k, v in matchers.items()):
                return True
        
        return False
    
    def _send_notifications(self, alert: AlertInstance) -> None:
        """发送通知"""
        for notifier in self._notifiers:
            try:
                if notifier.notify(alert):
                    alert.notification_count += 1
                    logger.info(f"Notification sent via {notifier.get_name()} for alert {alert.name}")
            except Exception as e:
                logger.error(f"Failed to send notification via {notifier.get_name()}: {e}")
    
    def start(self) -> None:
        """启动告警管理器"""
        self._running = True
        self._eval_thread = threading.Thread(target=self._evaluation_loop, daemon=True)
        self._eval_thread.start()
        logger.info("Alert manager started")
    
    def stop(self) -> None:
        """停止告警管理器"""
        self._running = False
        if self._eval_thread:
            self._eval_thread.join(timeout=5.0)
        logger.info("Alert manager stopped")
    
    def _evaluation_loop(self) -> None:
        """评估循环"""
        while self._running:
            time.sleep(self.evaluation_interval)
    
    def get_active_alerts(self) -> List[AlertInstance]:
        """获取活动告警"""
        with self._lock:
            return list(self._active_alerts.values())
    
    def get_alert_stats(self) -> Dict[str, Any]:
        """获取告警统计"""
        with self._lock:
            alerts = list(self._active_alerts.values())
        
        severity_counts = defaultdict(int)
        state_counts = defaultdict(int)
        
        for alert in alerts:
            severity_counts[alert.severity.value] += 1
            state_counts[alert.state.value] += 1
        
        return {
            "active_count": len(alerts),
            "by_severity": dict(severity_counts),
            "by_state": dict(state_counts),
        }


class Dashboard:
    """仪表盘"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.panels: List[Dict[str, Any]] = []
    
    def add_panel(
        self,
        title: str,
        panel_type: str,  # graph, singlestat, table, text
        metrics: List[str],
        query: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> None:
        """添加面板"""
        self.panels.append({
            "title": title,
            "type": panel_type,
            "metrics": metrics,
            "query": query,
            "options": options or {},
        })
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "panels": self.panels,
        }


class MonitoringAlertingSystem:
    """监控告警系统主类"""
    
    def __init__(self):
        self.metrics = MetricsCollector()
        self.logs = LogAggregator()
        self.alert_manager = AlertManager()
        self.dashboards: Dict[str, Dashboard] = {}
        self._lock = threading.RLock()
    
    def create_dashboard(self, name: str, description: str = "") -> Dashboard:
        """创建仪表盘"""
        dashboard = Dashboard(name, description)
        with self._lock:
            self.dashboards[name] = dashboard
        return dashboard
    
    def get_dashboard(self, name: str) -> Optional[Dashboard]:
        """获取仪表盘"""
        with self._lock:
            return self.dashboards.get(name)
    
    def record_metric(self, name: str, value: float, metric_type: MetricType = MetricType.GAUGE,
                      labels: Optional[Dict[str, str]] = None) -> None:
        """记录指标"""
        if metric_type == MetricType.COUNTER:
            self.metrics.counter(name, value, labels)
        elif metric_type == MetricType.GAUGE:
            self.metrics.gauge(name, value, labels)
        elif metric_type == MetricType.HISTOGRAM:
            self.metrics.histogram(name, value, labels)
    
    def log(self, level: str, message: str, source: str = "",
            labels: Optional[Dict[str, str]] = None) -> None:
        """记录日志"""
        self.logs.log(level, message, source, labels)
    
    def add_alert_rule(
        self,
        name: str,
        expr: str,
        duration: timedelta,
        severity: AlertSeverity,
        summary: str,
        description: str,
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None
    ) -> AlertRule:
        """添加告警规则"""
        rule = AlertRule(
            rule_id=str(uuid.uuid4()),
            name=name,
            expr=expr,
            duration=duration,
            severity=severity,
            summary=summary,
            description=description,
            labels=labels or {},
            annotations=annotations or {},
        )
        self.alert_manager.add_rule(rule)
        return rule
    
    def evaluate_alerts(self) -> List[AlertInstance]:
        """评估告警"""
        return self.alert_manager.evaluate_rules(self.metrics)
    
    def get_system_health(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {
                "total_metrics": len(self.metrics.get_metrics()),
            },
            "logs": self.logs.aggregate(),
            "alerts": self.alert_manager.get_alert_stats(),
        }
    
    def start(self) -> None:
        """启动系统"""
        self.alert_manager.start()
    
    def stop(self) -> None:
        """停止系统"""
        self.alert_manager.stop()


# 全局监控告警系统实例
_monitoring_system: Optional[MonitoringAlertingSystem] = None


def get_monitoring_system() -> MonitoringAlertingSystem:
    """获取全局监控告警系统实例"""
    global _monitoring_system
    if _monitoring_system is None:
        _monitoring_system = MonitoringAlertingSystem()
    return _monitoring_system


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    print("=== Monitoring and Alerting System Demo ===\n")
    
    # 创建监控告警系统
    system = get_monitoring_system()
    
    # 示例1: 指标收集
    print("1. Metrics Collection:")
    
    # 记录一些指标
    for i in range(10):
        system.record_metric("cpu_usage", 50 + i * 2, MetricType.GAUGE, {"host": "server-01"})
        system.record_metric("memory_usage", 60 + i, MetricType.GAUGE, {"host": "server-01"})
        system.record_metric("requests_total", 1, MetricType.COUNTER, {"endpoint": "/api/data"})
        system.metrics.histogram("request_duration_seconds", 0.05 + i * 0.01, {"endpoint": "/api/data"})
    
    print(f"   Recorded {len(system.metrics.get_metrics())} metrics")
    
    # 导出Prometheus格式
    prometheus_output = system.metrics.get_prometheus_format()
    print(f"   Prometheus format output length: {len(prometheus_output)} chars")
    
    # 示例2: 日志聚合
    print("\n2. Log Aggregation:")
    
    system.log("INFO", "Application started", source="app", labels={"version": "1.0.0"})
    system.log("WARNING", "High memory usage detected", source="monitor", labels={"host": "server-01"})
    system.log("ERROR", "Database connection failed", source="db", labels={"retry": "1"})
    system.log("INFO", "Request processed", source="api", labels={"endpoint": "/api/data"})
    
    # 查询日志
    error_logs = system.logs.query(level="ERROR")
    print(f"   Error logs: {len(error_logs)}")
    
    # 聚合统计
    stats = system.logs.aggregate()
    print(f"   Log stats: {stats}")
    
    # 示例3: 告警规则
    print("\n3. Alert Rules:")
    
    rule = system.add_alert_rule(
        name="HighCPUUsage",
        expr="cpu_usage > 60",
        duration=timedelta(seconds=5),
        severity=AlertSeverity.WARNING,
        summary="High CPU usage detected",
        description="CPU usage has exceeded 60% for more than 5 seconds",
        labels={"team": "ops"},
    )
    
    print(f"   Created rule: {rule.name}")
    
    # 评估告警
    alerts = system.evaluate_alerts()
    print(f"   Active alerts: {len(alerts)}")
    
    # 示例4: 仪表盘
    print("\n4. Dashboard:")
    
    dashboard = system.create_dashboard(
        name="System Overview",
        description="Overview of system metrics"
    )
    
    dashboard.add_panel(
        title="CPU Usage",
        panel_type="graph",
        metrics=["cpu_usage"],
        options={"y_axis": "percentage"}
    )
    
    dashboard.add_panel(
        title="Memory Usage",
        panel_type="graph",
        metrics=["memory_usage"],
        options={"y_axis": "percentage"}
    )
    
    print(f"   Created dashboard: {dashboard.name}")
    print(f"   Panels: {len(dashboard.panels)}")
    
    # 示例5: 系统健康
    print("\n5. System Health:")
    
    health = system.get_system_health()
    print(f"   Timestamp: {health['timestamp']}")
    print(f"   Total metrics: {health['metrics']['total_metrics']}")
    print(f"   Log entries: {health['logs']['total']}")
    
    print("\n=== Monitoring and Alerting System Demo Completed ===")

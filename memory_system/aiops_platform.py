#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AIOps智能运维平台 (AIOps Intelligent Operations Platform)

提供异常检测、时序数据预测、根因分析、智能告警降噪、容量规划等
AI驱动的运维能力。

Author: Agent Memory System Team
Version: 3.0.0
"""

import asyncio
import json
import logging
import math
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
import heapq

# 配置日志
logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    """异常类型"""
    SPIKE = "spike"              # 尖峰
    DIP = "dip"                  # 骤降
    TREND_CHANGE = "trend_change"  # 趋势变化
    SEASONALITY_BREAK = "seasonality_break"  # 季节性中断
    LEVEL_SHIFT = "level_shift"  # 水平漂移
    VOLATILITY_CHANGE = "volatility_change"  # 波动性变化


class DetectionMethod(Enum):
    """检测方法"""
    STATISTICAL = "statistical"  # 统计方法
    ML = "machine_learning"      # 机器学习方法
    DL = "deep_learning"         # 深度学习方法
    HYBRID = "hybrid"            # 混合方法


@dataclass
class MetricPoint:
    """指标数据点"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "labels": self.labels,
        }


@dataclass
class AnomalyResult:
    """异常检测结果"""
    timestamp: datetime
    metric_name: str
    anomaly_type: AnomalyType
    severity: float  # 0-1
    expected_value: float
    actual_value: float
    confidence: float
    method: DetectionMethod
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "metric_name": self.metric_name,
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity,
            "expected_value": self.expected_value,
            "actual_value": self.actual_value,
            "confidence": self.confidence,
            "method": self.method.value,
            "details": self.details,
        }


@dataclass
class PredictionResult:
    """预测结果"""
    metric_name: str
    timestamps: List[datetime]
    values: List[float]
    confidence_intervals: List[Tuple[float, float]]
    horizon: int
    model_name: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "timestamps": [t.isoformat() for t in self.timestamps],
            "values": self.values,
            "confidence_intervals": self.confidence_intervals,
            "horizon": self.horizon,
            "model_name": self.model_name,
        }


@dataclass
class RootCause:
    """根因分析结果"""
    component: str
    probability: float
    evidence: List[str]
    recommendations: List[str]
    related_metrics: List[str]


@dataclass
class Alert:
    """告警"""
    id: str
    title: str
    description: str
    severity: str  # critical, warning, info
    source: str
    timestamp: datetime
    labels: Dict[str, str]
    status: str = "firing"  # firing, resolved, suppressed
    root_cause: Optional[RootCause] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "severity": self.severity,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "labels": self.labels,
            "status": self.status,
            "root_cause": {
                "component": self.root_cause.component,
                "probability": self.root_cause.probability,
            } if self.root_cause else None,
        }


class AnomalyDetector(ABC):
    """异常检测器抽象基类"""
    
    @abstractmethod
    def detect(self, metric_name: str, data: List[MetricPoint]) -> List[AnomalyResult]:
        """检测异常"""
        pass
    
    @abstractmethod
    def train(self, data: List[MetricPoint]) -> None:
        """训练模型"""
        pass


class StatisticalDetector(AnomalyDetector):
    """统计异常检测器"""
    
    def __init__(
        self,
        window_size: int = 60,
        threshold_sigma: float = 3.0,
        min_points: int = 30
    ):
        self.window_size = window_size
        self.threshold_sigma = threshold_sigma
        self.min_points = min_points
        self._history: Dict[str, deque] = {}
        self._stats: Dict[str, Dict[str, float]] = {}
    
    def train(self, data: List[MetricPoint]) -> None:
        """训练：计算历史统计量"""
        if len(data) < self.min_points:
            return
        
        values = [p.value for p in data]
        metric_name = data[0].labels.get("__name__", "unknown")
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(variance)
        
        self._stats[metric_name] = {
            "mean": mean,
            "std": std,
            "min": min(values),
            "max": max(values),
        }
    
    def detect(self, metric_name: str, data: List[MetricPoint]) -> List[AnomalyResult]:
        """基于统计方法检测异常"""
        anomalies = []
        
        if metric_name not in self._stats:
            # 使用滑动窗口统计
            if len(data) < self.window_size:
                return anomalies
            
            for i in range(self.window_size, len(data)):
                window = data[i - self.window_size:i]
                current = data[i]
                
                values = [p.value for p in window]
                mean = sum(values) / len(values)
                std = math.sqrt(sum((x - mean) ** 2 for x in values) / len(values))
                
                if std == 0:
                    continue
                
                z_score = abs(current.value - mean) / std
                
                if z_score > self.threshold_sigma:
                    anomaly_type = AnomalyType.SPIKE if current.value > mean else AnomalyType.DIP
                    
                    anomalies.append(AnomalyResult(
                        timestamp=current.timestamp,
                        metric_name=metric_name,
                        anomaly_type=anomaly_type,
                        severity=min(1.0, z_score / (self.threshold_sigma * 2)),
                        expected_value=mean,
                        actual_value=current.value,
                        confidence=min(1.0, z_score / self.threshold_sigma),
                        method=DetectionMethod.STATISTICAL,
                        details={"z_score": z_score, "window_mean": mean, "window_std": std}
                    ))
        else:
            # 使用预计算的统计量
            stats = self._stats[metric_name]
            mean, std = stats["mean"], stats["std"]
            
            if std > 0:
                for point in data:
                    z_score = abs(point.value - mean) / std
                    
                    if z_score > self.threshold_sigma:
                        anomaly_type = AnomalyType.SPIKE if point.value > mean else AnomalyType.DIP
                        
                        anomalies.append(AnomalyResult(
                            timestamp=point.timestamp,
                            metric_name=metric_name,
                            anomaly_type=anomaly_type,
                            severity=min(1.0, z_score / (self.threshold_sigma * 2)),
                            expected_value=mean,
                            actual_value=point.value,
                            confidence=min(1.0, z_score / self.threshold_sigma),
                            method=DetectionMethod.STATISTICAL,
                            details={"z_score": z_score, "historical_mean": mean, "historical_std": std}
                        ))
        
        return anomalies


class IsolationForestDetector(AnomalyDetector):
    """隔离森林异常检测器 (ML方法)"""
    
    def __init__(
        self,
        n_estimators: int = 100,
        contamination: float = 0.1,
        window_size: int = 10
    ):
        self.n_estimators = n_estimators
        self.contamination = contamination
        self.window_size = window_size
        self._trees: List[Dict] = []
        self._trained = False
    
    def train(self, data: List[MetricPoint]) -> None:
        """训练隔离森林模型"""
        if len(data) < self.window_size * 2:
            return
        
        # 简化实现：构建随机分割树
        values = [p.value for p in data]
        self._trees = []
        
        for _ in range(self.n_estimators):
            tree = self._build_tree(values)
            self._trees.append(tree)
        
        self._trained = True
    
    def _build_tree(self, values: List[float]) -> Dict:
        """构建单棵随机树"""
        if len(values) <= 1:
            return {"leaf": True, "size": len(values)}
        
        # 随机选择分割点
        min_val, max_val = min(values), max(values)
        if min_val == max_val:
            return {"leaf": True, "size": len(values)}
        
        split = min_val + (max_val - min_val) * (0.2 + 0.6 * (hash(str(time.time())) % 1000) / 1000)
        
        left = [v for v in values if v < split]
        right = [v for v in values if v >= split]
        
        return {
            "leaf": False,
            "split": split,
            "left": self._build_tree(left),
            "right": self._build_tree(right),
        }
    
    def _path_length(self, value: float, tree: Dict, current_length: int = 0) -> int:
        """计算路径长度"""
        if tree.get("leaf", False):
            return current_length + self._c(tree.get("size", 0))
        
        if value < tree["split"]:
            return self._path_length(value, tree["left"], current_length + 1)
        else:
            return self._path_length(value, tree["right"], current_length + 1)
    
    def _c(self, n: int) -> float:
        """平均路径长度修正"""
        if n <= 1:
            return 0
        return 2 * (math.log(n - 1) + 0.5772156649) - 2 * (n - 1) / n
    
    def detect(self, metric_name: str, data: List[MetricPoint]) -> List[AnomalyResult]:
        """使用隔离森林检测异常"""
        anomalies = []
        
        if not self._trained or not self._trees:
            return anomalies
        
        for point in data:
            # 计算平均路径长度
            avg_path = sum(self._path_length(point.value, tree) for tree in self._trees) / len(self._trees)
            
            # 计算异常分数
            expected_path = self._c(len(data))
            if expected_path > 0:
                anomaly_score = 2 ** (-avg_path / expected_path)
            else:
                anomaly_score = 0
            
            if anomaly_score > 0.6:  # 异常阈值
                # 确定异常类型
                values = [p.value for p in data[-self.window_size:]] if len(data) >= self.window_size else [p.value for p in data]
                mean_val = sum(values) / len(values) if values else 0
                
                anomaly_type = AnomalyType.SPIKE if point.value > mean_val else AnomalyType.DIP
                
                anomalies.append(AnomalyResult(
                    timestamp=point.timestamp,
                    metric_name=metric_name,
                    anomaly_type=anomaly_type,
                    severity=anomaly_score,
                    expected_value=mean_val,
                    actual_value=point.value,
                    confidence=anomaly_score,
                    method=DetectionMethod.ML,
                    details={"anomaly_score": anomaly_score, "avg_path_length": avg_path}
                ))
        
        return anomalies


class TimeSeriesForecaster(ABC):
    """时序预测器抽象基类"""
    
    @abstractmethod
    def forecast(
        self,
        metric_name: str,
        data: List[MetricPoint],
        horizon: int
    ) -> PredictionResult:
        """预测未来值"""
        pass
    
    @abstractmethod
    def train(self, data: List[MetricPoint]) -> None:
        """训练模型"""
        pass


class ExponentialSmoothingForecaster(TimeSeriesForecaster):
    """指数平滑预测器"""
    
    def __init__(
        self,
        alpha: float = 0.3,  # 平滑系数
        beta: float = 0.1,   # 趋势系数
        gamma: float = 0.1,  # 季节性系数
        season_length: int = 24
    ):
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.season_length = season_length
        self._level: Optional[float] = None
        self._trend: Optional[float] = None
        self._seasonals: List[float] = []
    
    def train(self, data: List[MetricPoint]) -> None:
        """训练：初始化水平、趋势和季节性分量"""
        if len(data) < self.season_length * 2:
            return
        
        values = [p.value for p in data]
        
        # 初始化季节性分量
        season_averages = []
        n_seasons = len(values) // self.season_length
        for j in range(self.season_length):
            season_avg = sum(values[j + i * self.season_length] for i in range(n_seasons)) / n_seasons
            season_averages.append(season_avg)
        
        overall_avg = sum(season_averages) / len(season_averages)
        self._seasonals = [avg / overall_avg for avg in season_averages]
        
        # 初始化水平和趋势
        self._level = values[0]
        self._trend = (sum(values[self.season_length:2 * self.season_length]) - 
                       sum(values[:self.season_length])) / (self.season_length ** 2)
        
        # 训练
        for i, value in enumerate(values):
            season_idx = i % self.season_length
            
            # 更新水平
            last_level = self._level
            self._level = self.alpha * (value / self._seasonals[season_idx]) + (1 - self.alpha) * (last_level + self._trend)
            
            # 更新趋势
            self._trend = self.beta * (self._level - last_level) + (1 - self.beta) * self._trend
            
            # 更新季节性
            self._seasonals[season_idx] = self.gamma * (value / self._level) + (1 - self.gamma) * self._seasonals[season_idx]
    
    def forecast(
        self,
        metric_name: str,
        data: List[MetricPoint],
        horizon: int
    ) -> PredictionResult:
        """预测未来值"""
        if self._level is None:
            # 使用简单预测
            values = [p.value for p in data]
            avg = sum(values) / len(values) if values else 0
            
            last_time = data[-1].timestamp if data else datetime.utcnow()
            interval = timedelta(minutes=1)  # 假设1分钟间隔
            
            timestamps = [last_time + interval * (i + 1) for i in range(horizon)]
            forecast_values = [avg] * horizon
            confidence_intervals = [(avg * 0.9, avg * 1.1) for _ in range(horizon)]
        else:
            last_time = data[-1].timestamp if data else datetime.utcnow()
            interval = timedelta(minutes=1)
            
            timestamps = []
            forecast_values = []
            confidence_intervals = []
            
            for i in range(1, horizon + 1):
                season_idx = (len(data) + i - 1) % self.season_length
                forecast = (self._level + i * self._trend) * self._seasonals[season_idx]
                
                timestamps.append(last_time + interval * i)
                forecast_values.append(forecast)
                
                # 简单的置信区间
                margin = forecast * 0.1 * (1 + i * 0.05)  # 随时间增加不确定性
                confidence_intervals.append((forecast - margin, forecast + margin))
        
        return PredictionResult(
            metric_name=metric_name,
            timestamps=timestamps,
            values=forecast_values,
            confidence_intervals=confidence_intervals,
            horizon=horizon,
            model_name="ExponentialSmoothing"
        )


class RootCauseAnalyzer:
    """根因分析器"""
    
    def __init__(self):
        self._component_graph: Dict[str, List[str]] = {}
        self._metric_dependencies: Dict[str, List[str]] = {}
        self._historical_incidents: List[Dict[str, Any]] = []
    
    def register_component_graph(self, graph: Dict[str, List[str]]) -> None:
        """注册组件依赖图"""
        self._component_graph = graph
    
    def register_metric_dependencies(self, dependencies: Dict[str, List[str]]) -> None:
        """注册指标依赖关系"""
        self._metric_dependencies = dependencies
    
    def analyze(
        self,
        alert: Alert,
        metrics_data: Dict[str, List[MetricPoint]]
    ) -> Optional[RootCause]:
        """分析告警的根因"""
        # 基于依赖图分析
        source = alert.source
        
        # 查找上游组件
        upstream_components = self._find_upstream(source)
        
        # 分析相关指标
        related_metrics = []
        for metric_name, points in metrics_data.items():
            if self._is_related(metric_name, source):
                # 检查该指标是否有异常
                if self._has_anomaly(points):
                    related_metrics.append(metric_name)
        
        # 计算概率
        probability = self._calculate_probability(source, related_metrics)
        
        # 生成证据
        evidence = []
        for metric in related_metrics[:5]:
            evidence.append(f"Metric {metric} shows anomalous behavior")
        
        # 生成建议
        recommendations = self._generate_recommendations(source, related_metrics)
        
        return RootCause(
            component=source,
            probability=probability,
            evidence=evidence,
            recommendations=recommendations,
            related_metrics=related_metrics
        )
    
    def _find_upstream(self, component: str) -> List[str]:
        """查找上游组件"""
        upstream = []
        for comp, deps in self._component_graph.items():
            if component in deps:
                upstream.append(comp)
        return upstream
    
    def _is_related(self, metric_name: str, component: str) -> bool:
        """检查指标是否与组件相关"""
        deps = self._metric_dependencies.get(component, [])
        return metric_name in deps or any(metric_name.startswith(d) for d in deps)
    
    def _has_anomaly(self, points: List[MetricPoint]) -> bool:
        """检查指标是否有异常"""
        if len(points) < 2:
            return False
        
        # 简单的变化检测
        recent = [p.value for p in points[-5:]]
        if len(recent) >= 2:
            avg_recent = sum(recent) / len(recent)
            previous = [p.value for p in points[-10:-5]] if len(points) >= 10 else [points[0].value]
            avg_previous = sum(previous) / len(previous)
            
            if avg_previous > 0:
                change_ratio = abs(avg_recent - avg_previous) / avg_previous
                return change_ratio > 0.2  # 20%变化视为异常
        
        return False
    
    def _calculate_probability(self, source: str, related_metrics: List[str]) -> float:
        """计算根因概率"""
        base_prob = 0.5
        metric_boost = min(0.3, len(related_metrics) * 0.05)
        return min(1.0, base_prob + metric_boost)
    
    def _generate_recommendations(self, source: str, related_metrics: List[str]) -> List[str]:
        """生成修复建议"""
        recommendations = [
            f"Check the health status of {source}",
            "Review recent deployment or configuration changes",
        ]
        
        for metric in related_metrics[:3]:
            recommendations.append(f"Investigate metric: {metric}")
        
        return recommendations


class AlertManager:
    """告警管理器"""
    
    def __init__(
        self,
        dedup_window_seconds: float = 300.0,
        suppression_rules: Optional[List[Dict]] = None
    ):
        self.dedup_window = timedelta(seconds=dedup_window_seconds)
        self.suppression_rules = suppression_rules or []
        self._alerts: Dict[str, Alert] = {}
        self._alert_history: deque = deque(maxlen=10000)
        self._lock = threading.RLock()
        self._alert_handlers: List[Callable[[Alert], None]] = []
    
    def add_handler(self, handler: Callable[[Alert], None]) -> None:
        """添加告警处理器"""
        self._alert_handlers.append(handler)
    
    def create_alert(
        self,
        title: str,
        description: str,
        severity: str,
        source: str,
        labels: Dict[str, str],
        root_cause: Optional[RootCause] = None
    ) -> Alert:
        """创建告警"""
        alert_id = self._generate_alert_id(title, source, labels)
        
        with self._lock:
            # 检查是否重复
            if alert_id in self._alerts:
                existing = self._alerts[alert_id]
                if datetime.utcnow() - existing.timestamp < self.dedup_window:
                    logger.debug(f"Duplicate alert suppressed: {alert_id}")
                    return existing
            
            # 检查抑制规则
            if self._should_suppress(title, source, labels):
                logger.debug(f"Alert suppressed by rule: {title}")
                alert = Alert(
                    id=alert_id,
                    title=title,
                    description=description,
                    severity=severity,
                    source=source,
                    timestamp=datetime.utcnow(),
                    labels=labels,
                    status="suppressed",
                    root_cause=root_cause
                )
                return alert
            
            # 创建新告警
            alert = Alert(
                id=alert_id,
                title=title,
                description=description,
                severity=severity,
                source=source,
                timestamp=datetime.utcnow(),
                labels=labels,
                status="firing",
                root_cause=root_cause
            )
            
            self._alerts[alert_id] = alert
            self._alert_history.append(alert)
            
            # 通知处理器
            for handler in self._alert_handlers:
                try:
                    handler(alert)
                except Exception as e:
                    logger.error(f"Alert handler failed: {e}")
            
            return alert
    
    def resolve_alert(self, alert_id: str) -> bool:
        """解决告警"""
        with self._lock:
            if alert_id in self._alerts:
                self._alerts[alert_id].status = "resolved"
                return True
            return False
    
    def _generate_alert_id(self, title: str, source: str, labels: Dict[str, str]) -> str:
        """生成告警ID"""
        content = f"{title}:{source}:{sorted(labels.items())}"
        import hashlib
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def _should_suppress(self, title: str, source: str, labels: Dict[str, str]) -> bool:
        """检查是否应该抑制告警"""
        for rule in self.suppression_rules:
            if self._matches_rule(title, source, labels, rule):
                return True
        return False
    
    def _matches_rule(
        self,
        title: str,
        source: str,
        labels: Dict[str, str],
        rule: Dict[str, Any]
    ) -> bool:
        """检查是否匹配抑制规则"""
        matchers = rule.get("matchers", {})
        
        for key, pattern in matchers.items():
            if key == "title" and pattern not in title:
                return False
            elif key == "source" and pattern != source:
                return False
            elif key in labels and labels[key] != pattern:
                return False
        
        return True
    
    def get_active_alerts(self) -> List[Alert]:
        """获取活动告警"""
        with self._lock:
            return [a for a in self._alerts.values() if a.status == "firing"]
    
    def get_alert_stats(self) -> Dict[str, Any]:
        """获取告警统计"""
        with self._lock:
            active = self.get_active_alerts()
            
            severity_counts = {}
            for alert in active:
                severity_counts[alert.severity] = severity_counts.get(alert.severity, 0) + 1
            
            return {
                "active_count": len(active),
                "total_count": len(self._alert_history),
                "severity_distribution": severity_counts,
                "by_source": self._group_by_source(active),
            }
    
    def _group_by_source(self, alerts: List[Alert]) -> Dict[str, int]:
        """按源分组"""
        result = {}
        for alert in alerts:
            result[alert.source] = result.get(alert.source, 0) + 1
        return result


class CapacityPlanner:
    """容量规划器"""
    
    def __init__(self):
        self._resource_configs: Dict[str, Dict[str, Any]] = {}
        self._growth_models: Dict[str, Callable] = {}
    
    def register_resource(
        self,
        name: str,
        current_capacity: float,
        unit: str,
        thresholds: Dict[str, float]
    ) -> None:
        """注册资源"""
        self._resource_configs[name] = {
            "current_capacity": current_capacity,
            "unit": unit,
            "thresholds": thresholds,
        }
    
    def plan_capacity(
        self,
        resource_name: str,
        historical_usage: List[MetricPoint],
        forecast_horizon_days: int = 90
    ) -> Dict[str, Any]:
        """规划容量"""
        if resource_name not in self._resource_configs:
            return {"error": f"Resource {resource_name} not registered"}
        
        config = self._resource_configs[resource_name]
        current_capacity = config["current_capacity"]
        
        # 计算历史增长趋势
        if len(historical_usage) < 2:
            return {"error": "Insufficient historical data"}
        
        values = [p.value for p in historical_usage]
        
        # 计算平均增长率
        if len(values) >= 30:
            recent_avg = sum(values[-7:]) / 7
            older_avg = sum(values[-30:-23]) / 7 if len(values) >= 30 else values[0]
            
            if older_avg > 0:
                weekly_growth_rate = (recent_avg - older_avg) / older_avg
            else:
                weekly_growth_rate = 0
        else:
            weekly_growth_rate = 0
        
        # 预测未来使用率
        current_usage = values[-1]
        days_to_threshold = {}
        
        for threshold_name, threshold_value in config["thresholds"].items():
            if weekly_growth_rate > 0:
                # 计算达到阈值所需天数
                target_usage = current_capacity * threshold_value
                if current_usage < target_usage:
                    days = math.log(target_usage / current_usage) / math.log(1 + weekly_growth_rate) * 7
                    days_to_threshold[threshold_name] = max(0, int(days))
                else:
                    days_to_threshold[threshold_name] = 0
            else:
                days_to_threshold[threshold_name] = float('inf')
        
        # 生成建议
        recommendations = []
        
        if "critical" in days_to_threshold and days_to_threshold["critical"] < 30:
            recommendations.append(
                f"CRITICAL: Capacity will reach critical threshold in {days_to_threshold['critical']} days. "
                "Immediate action required."
            )
        elif "warning" in days_to_threshold and days_to_threshold["warning"] < 60:
            recommendations.append(
                f"WARNING: Capacity will reach warning threshold in {days_to_threshold['warning']} days. "
                "Plan for expansion."
            )
        
        # 计算建议容量
        projected_usage_90d = current_usage * ((1 + weekly_growth_rate) ** (90 / 7))
        recommended_capacity = projected_usage_90d * 1.3  # 30% buffer
        
        return {
            "resource_name": resource_name,
            "current_usage": current_usage,
            "current_capacity": current_capacity,
            "utilization_percent": (current_usage / current_capacity * 100) if current_capacity > 0 else 0,
            "weekly_growth_rate": weekly_growth_rate * 100,
            "days_to_threshold": days_to_threshold,
            "projected_usage_90d": projected_usage_90d,
            "recommended_capacity": recommended_capacity,
            "recommendations": recommendations,
        }


class AutoRemediationEngine:
    """自动修复引擎"""
    
    def __init__(self):
        self._remediation_actions: Dict[str, Callable] = {}
        self._action_history: deque = deque(maxlen=1000)
        self._enabled = True
        self._risk_threshold = 0.7
    
    def register_action(self, name: str, action: Callable, risk_level: float = 0.5) -> None:
        """注册修复动作"""
        self._remediation_actions[name] = {
            "action": action,
            "risk_level": risk_level,
        }
    
    def suggest_remediation(self, alert: Alert) -> List[Dict[str, Any]]:
        """建议修复方案"""
        suggestions = []
        
        # 基于告警类型匹配修复动作
        for action_name, config in self._remediation_actions.items():
            relevance = self._calculate_relevance(alert, action_name)
            
            if relevance > 0.5:  # 相关性阈值
                suggestions.append({
                    "action": action_name,
                    "relevance": relevance,
                    "risk_level": config["risk_level"],
                    "auto_executable": config["risk_level"] < self._risk_threshold and self._enabled,
                    "description": self._get_action_description(action_name),
                })
        
        # 按相关性排序
        suggestions.sort(key=lambda x: x["relevance"], reverse=True)
        return suggestions
    
    def execute_remediation(self, action_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行修复动作"""
        if action_name not in self._remediation_actions:
            return {"success": False, "error": f"Unknown action: {action_name}"}
        
        config = self._remediation_actions[action_name]
        
        if config["risk_level"] >= self._risk_threshold:
            return {
                "success": False,
                "error": "Action risk level too high for auto-execution",
                "requires_approval": True,
            }
        
        try:
            result = config["action"](context)
            
            self._action_history.append({
                "action": action_name,
                "timestamp": datetime.utcnow(),
                "context": context,
                "result": result,
            })
            
            return {"success": True, "result": result}
        except Exception as e:
            logger.exception(f"Remediation action failed: {action_name}")
            return {"success": False, "error": str(e)}
    
    def _calculate_relevance(self, alert: Alert, action_name: str) -> float:
        """计算修复动作与告警的相关性"""
        # 简化实现：基于关键词匹配
        relevance = 0.5
        
        if action_name.lower() in alert.title.lower():
            relevance += 0.3
        if action_name.lower() in alert.source.lower():
            relevance += 0.2
        
        return min(1.0, relevance)
    
    def _get_action_description(self, action_name: str) -> str:
        """获取动作描述"""
        descriptions = {
            "restart_service": "Restart the affected service",
            "scale_up": "Increase resource allocation",
            "clear_cache": "Clear system cache",
            "switch_backup": "Failover to backup system",
        }
        return descriptions.get(action_name, f"Execute {action_name}")


class AIOpsPlatform:
    """AIOps平台主类"""
    
    def __init__(self):
        self._detectors: Dict[DetectionMethod, AnomalyDetector] = {}
        self._forecasters: Dict[str, TimeSeriesForecaster] = {}
        self._root_cause_analyzer = RootCauseAnalyzer()
        self._alert_manager = AlertManager()
        self._capacity_planner = CapacityPlanner()
        self._remediation_engine = AutoRemediationEngine()
        
        self._metrics_buffer: Dict[str, deque] = {}
        self._buffer_size = 1000
        self._lock = threading.RLock()
        
        # 初始化默认检测器
        self._init_default_detectors()
    
    def _init_default_detectors(self) -> None:
        """初始化默认检测器"""
        self._detectors[DetectionMethod.STATISTICAL] = StatisticalDetector()
        self._detectors[DetectionMethod.ML] = IsolationForestDetector()
        self._forecasters["default"] = ExponentialSmoothingForecaster()
    
    def ingest_metric(self, point: MetricPoint) -> None:
        """摄入指标数据"""
        metric_name = point.labels.get("__name__", "unknown")
        
        with self._lock:
            if metric_name not in self._metrics_buffer:
                self._metrics_buffer[metric_name] = deque(maxlen=self._buffer_size)
            
            self._metrics_buffer[metric_name].append(point)
    
    def ingest_metrics_batch(self, points: List[MetricPoint]) -> None:
        """批量摄入指标数据"""
        for point in points:
            self.ingest_metric(point)
    
    def detect_anomalies(
        self,
        metric_name: str,
        method: DetectionMethod = DetectionMethod.STATISTICAL
    ) -> List[AnomalyResult]:
        """检测异常"""
        with self._lock:
            if metric_name not in self._metrics_buffer:
                return []
            
            data = list(self._metrics_buffer[metric_name])
        
        detector = self._detectors.get(method)
        if not detector:
            return []
        
        return detector.detect(metric_name, data)
    
    def forecast(
        self,
        metric_name: str,
        horizon: int = 24,
        model: str = "default"
    ) -> Optional[PredictionResult]:
        """预测时序数据"""
        with self._lock:
            if metric_name not in self._metrics_buffer:
                return None
            
            data = list(self._metrics_buffer[metric_name])
        
        forecaster = self._forecasters.get(model)
        if not forecaster:
            return None
        
        # 训练并预测
        forecaster.train(data)
        return forecaster.forecast(metric_name, data, horizon)
    
    def analyze_root_cause(self, alert: Alert) -> Optional[RootCause]:
        """分析根因"""
        with self._lock:
            metrics_data = {k: list(v) for k, v in self._metrics_buffer.items()}
        
        return self._root_cause_analyzer.analyze(alert, metrics_data)
    
    def create_alert(
        self,
        title: str,
        description: str,
        severity: str,
        source: str,
        labels: Dict[str, str]
    ) -> Alert:
        """创建告警"""
        # 尝试分析根因
        temp_alert = Alert(
            id="",
            title=title,
            description=description,
            severity=severity,
            source=source,
            timestamp=datetime.utcnow(),
            labels=labels,
        )
        
        root_cause = self.analyze_root_cause(temp_alert)
        
        return self._alert_manager.create_alert(
            title=title,
            description=description,
            severity=severity,
            source=source,
            labels=labels,
            root_cause=root_cause
        )
    
    def get_remediation_suggestions(self, alert_id: str) -> List[Dict[str, Any]]:
        """获取修复建议"""
        # 查找告警
        for alert in self._alert_manager.get_active_alerts():
            if alert.id == alert_id:
                return self._remediation_engine.suggest_remediation(alert)
        
        return []
    
    def plan_capacity(
        self,
        resource_name: str,
        forecast_horizon_days: int = 90
    ) -> Dict[str, Any]:
        """容量规划"""
        with self._lock:
            # 查找相关指标
            usage_metric = None
            for metric_name in self._metrics_buffer.keys():
                if resource_name in metric_name and "usage" in metric_name.lower():
                    usage_metric = metric_name
                    break
            
            if not usage_metric:
                return {"error": f"No usage metric found for resource: {resource_name}"}
            
            historical_usage = list(self._metrics_buffer[usage_metric])
        
        return self._capacity_planner.plan_capacity(
            resource_name,
            historical_usage,
            forecast_horizon_days
        )
    
    def get_platform_stats(self) -> Dict[str, Any]:
        """获取平台统计信息"""
        with self._lock:
            return {
                "metrics_count": len(self._metrics_buffer),
                "total_datapoints": sum(len(v) for v in self._metrics_buffer.values()),
                "alert_stats": self._alert_manager.get_alert_stats(),
                "detectors": list(self._detectors.keys()),
                "forecasters": list(self._forecasters.keys()),
            }


# 全局AIOps平台实例
_aiops_platform: Optional[AIOpsPlatform] = None


def get_aiops_platform() -> AIOpsPlatform:
    """获取全局AIOps平台实例"""
    global _aiops_platform
    if _aiops_platform is None:
        _aiops_platform = AIOpsPlatform()
    return _aiops_platform


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    print("=== AIOps Platform Demo ===\n")
    
    # 创建AIOps平台
    platform = get_aiops_platform()
    
    # 示例1: 摄入指标数据
    print("1. Ingesting Metrics:")
    
    import random
    base_time = datetime.utcnow() - timedelta(hours=1)
    
    for i in range(100):
        # 正常数据带一些随机波动
        value = 50 + random.gauss(0, 5)
        
        # 在特定点注入异常
        if 40 <= i <= 45:
            value += 30  # 尖峰
        
        point = MetricPoint(
            timestamp=base_time + timedelta(minutes=i),
            value=value,
            labels={"__name__": "cpu_usage", "host": "server-01"}
        )
        platform.ingest_metric(point)
    
    print(f"   Ingested 100 data points")
    
    # 示例2: 异常检测
    print("\n2. Anomaly Detection:")
    
    anomalies = platform.detect_anomalies("cpu_usage", DetectionMethod.STATISTICAL)
    print(f"   Found {len(anomalies)} anomalies")
    
    for anomaly in anomalies[:3]:
        print(f"   - {anomaly.anomaly_type.value}: severity={anomaly.severity:.2f}, "
              f"value={anomaly.actual_value:.2f}")
    
    # 示例3: 时序预测
    print("\n3. Time Series Forecasting:")
    
    prediction = platform.forecast("cpu_usage", horizon=12)
    if prediction:
        print(f"   Forecast for next {prediction.horizon} points")
        print(f"   Predicted values: {[f'{v:.2f}' for v in prediction.values[:5]]}...")
    
    # 示例4: 容量规划
    print("\n4. Capacity Planning:")
    
    # 注册资源
    platform._capacity_planner.register_resource(
        name="cpu",
        current_capacity=100.0,
        unit="percent",
        thresholds={"warning": 0.7, "critical": 0.9}
    )
    
    # 添加更多历史数据用于容量规划
    for i in range(200):
        value = 40 + i * 0.1 + random.gauss(0, 3)  # 上升趋势
        point = MetricPoint(
            timestamp=base_time + timedelta(minutes=i),
            value=value,
            labels={"__name__": "cpu_usage", "host": "server-01"}
        )
        platform.ingest_metric(point)
    
    capacity_plan = platform.plan_capacity("cpu", forecast_horizon_days=30)
    print(f"   Current utilization: {capacity_plan.get('utilization_percent', 0):.1f}%")
    print(f"   Weekly growth rate: {capacity_plan.get('weekly_growth_rate', 0):.2f}%")
    print(f"   Recommendations: {capacity_plan.get('recommendations', [])}")
    
    # 示例5: 创建告警
    print("\n5. Alert Management:")
    
    alert = platform.create_alert(
        title="High CPU Usage",
        description="CPU usage exceeded threshold",
        severity="warning",
        source="server-01",
        labels={"metric": "cpu_usage", "threshold": "70"}
    )
    
    print(f"   Created alert: {alert.id}")
    print(f"   Status: {alert.status}")
    
    if alert.root_cause:
        print(f"   Root cause probability: {alert.root_cause.probability:.2f}")
    
    # 示例6: 平台统计
    print("\n6. Platform Statistics:")
    
    stats = platform.get_platform_stats()
    print(f"   Metrics tracked: {stats['metrics_count']}")
    print(f"   Total data points: {stats['total_datapoints']}")
    print(f"   Active alerts: {stats['alert_stats']['active_count']}")
    
    print("\n=== AIOps Platform Demo Completed ===")

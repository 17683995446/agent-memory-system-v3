#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
混沌工程框架 (Chaos Engineering Framework)

提供系统韧性测试和故障注入能力，支持CPU、内存、网络、磁盘等多种故障类型，
与Kubernetes集成实现Pod级别故障注入，包含完整的实验编排和安全回滚机制。

Author: Agent Memory System Team
Version: 3.0.0
"""

import asyncio
import hashlib
import json
import logging
import os
import random
try:
    import resource
    HAS_RESOURCE = True
except ImportError:
    HAS_RESOURCE = False
import signal
import socket
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
    Generic,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
    cast,
)
from collections import defaultdict
import tempfile
import shutil

# 配置日志
logger = logging.getLogger(__name__)


class FaultType(Enum):
    """故障类型枚举"""
    CPU = auto()
    MEMORY = auto()
    NETWORK = auto()
    DISK = auto()
    POD = auto()
    PROCESS = auto()
    TIME = auto()
    DNS = auto()


class FaultSeverity(Enum):
    """故障严重程度"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ExperimentStatus(Enum):
    """实验状态"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class FaultConfig:
    """故障配置基类"""
    duration: float = 60.0  # 故障持续时间(秒)
    ramp_up: float = 5.0    # 故障渐增时间(秒)
    ramp_down: float = 5.0  # 故障渐减时间(秒)
    auto_rollback: bool = True  # 是否自动回滚
    rollback_timeout: float = 30.0  # 回滚超时时间(秒)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CPUFaultConfig(FaultConfig):
    """CPU故障配置"""
    cores: int = 1  # 影响的CPU核心数
    load_percent: float = 80.0  # CPU负载百分比
    target_processes: Optional[List[str]] = None  # 目标进程列表


@dataclass
class MemoryFaultConfig(FaultConfig):
    """内存故障配置"""
    memory_mb: int = 512  # 分配的内存大小(MB)
    leak_rate_mb_per_sec: float = 0.0  # 内存泄漏速率
    oom_trigger: bool = False  # 是否触发OOM


@dataclass
class NetworkFaultConfig(FaultConfig):
    """网络故障配置"""
    latency_ms: float = 100.0  # 延迟(毫秒)
    jitter_ms: float = 10.0    # 抖动(毫秒)
    packet_loss_percent: float = 0.0  # 丢包率(%)
    bandwidth_limit_mbps: Optional[float] = None  # 带宽限制
    target_hosts: Optional[List[str]] = None  # 目标主机
    target_ports: Optional[List[int]] = None  # 目标端口


@dataclass
class DiskFaultConfig(FaultConfig):
    """磁盘故障配置"""
    io_delay_ms: float = 50.0  # IO延迟(毫秒)
    fill_percent: Optional[float] = None  # 磁盘填充百分比
    read_error_rate: float = 0.0  # 读取错误率
    write_error_rate: float = 0.0  # 写入错误率
    target_paths: Optional[List[str]] = None  # 目标路径


@dataclass
class PodFaultConfig(FaultConfig):
    """Pod故障配置"""
    namespace: str = "default"
    label_selector: Optional[Dict[str, str]] = None
    pod_name: Optional[str] = None
    fault_action: str = "terminate"  # terminate, restart, network-partition
    affected_pods_percent: float = 50.0  # 影响Pod百分比


@dataclass
class ProcessFaultConfig(FaultConfig):
    """进程故障配置"""
    process_name: str = ""
    process_pattern: Optional[str] = None
    signal: int = signal.SIGTERM  # 发送的信号
    kill_probability: float = 1.0  # 杀死进程的概率


@dataclass
class ChaosExperiment:
    """混沌实验定义"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    fault_type: FaultType = FaultType.CPU
    fault_config: FaultConfig = field(default_factory=FaultConfig)
    target_scope: Dict[str, Any] = field(default_factory=dict)
    schedule: Optional[str] = None  # Cron表达式
    status: ExperimentStatus = ExperimentStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    rollback_actions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ResilienceScore:
    """韧性评分结果"""
    overall_score: float = 0.0  # 总体评分(0-100)
    availability_score: float = 0.0
    recoverability_score: float = 0.0
    latency_score: float = 0.0
    error_rate_score: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class FaultInjector(ABC):
    """故障注入器抽象基类"""

    def __init__(self, config: FaultConfig):
        self.config = config
        self._active = False
        self._start_time: Optional[float] = None
        self._lock = threading.RLock()
        self._rollback_state: Dict[str, Any] = {}

    @abstractmethod
    def inject(self) -> bool:
        """注入故障，返回是否成功"""
        pass

    @abstractmethod
    def rollback(self) -> bool:
        """回滚故障，返回是否成功"""
        pass

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """获取当前故障指标"""
        pass

    def is_active(self) -> bool:
        """检查故障是否处于活动状态"""
        with self._lock:
            return self._active

    def start(self) -> bool:
        """启动故障注入"""
        with self._lock:
            if self._active:
                logger.warning("Fault injection already active")
                return False
            
            try:
                # 保存回滚状态
                self._save_rollback_state()
                
                # 执行故障注入
                if self.inject():
                    self._active = True
                    self._start_time = time.time()
                    logger.info(f"Fault injection started: {self.__class__.__name__}")
                    return True
                else:
                    logger.error("Failed to inject fault")
                    return False
            except Exception as e:
                logger.exception(f"Error starting fault injection: {e}")
                return False

    def stop(self) -> bool:
        """停止故障注入"""
        with self._lock:
            if not self._active:
                return True
            
            try:
                if self.rollback():
                    self._active = False
                    duration = time.time() - (self._start_time or time.time())
                    logger.info(f"Fault injection stopped after {duration:.2f}s")
                    return True
                else:
                    logger.error("Failed to rollback fault")
                    return False
            except Exception as e:
                logger.exception(f"Error stopping fault injection: {e}")
                return False

    def _save_rollback_state(self) -> None:
        """保存回滚状态，子类应重写此方法"""
        pass

    def get_duration(self) -> float:
        """获取故障持续时间"""
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time


class CPUFaultInjector(FaultInjector):
    """CPU故障注入器"""

    def __init__(self, config: CPUFaultConfig):
        super().__init__(config)
        self.config = cast(CPUFaultConfig, config)
        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self._original_nice: Optional[int] = None

    def _save_rollback_state(self) -> None:
        """保存CPU相关的回滚状态"""
        try:
            self._rollback_state['process_priority'] = os.nice(0)
        except Exception:
            pass

    def inject(self) -> bool:
        """注入CPU负载"""
        try:
            config = self.config
            
            # 创建CPU负载线程
            for i in range(config.cores):
                thread = threading.Thread(
                    target=self._cpu_stress_thread,
                    args=(config.load_percent,),
                    name=f"cpu-stress-{i}"
                )
                thread.daemon = True
                thread.start()
                self._threads.append(thread)
            
            # 如果有目标进程，降低其优先级
            if config.target_processes:
                self._adjust_process_priority(config.target_processes, increase_load=True)
            
            return True
        except Exception as e:
            logger.exception(f"CPU fault injection failed: {e}")
            return False

    def _cpu_stress_thread(self, load_percent: float) -> None:
        """CPU压力线程"""
        # 边界校验：将 load_percent 限制在 [0, 100] 范围内
        load_percent = max(0.0, min(100.0, load_percent))

        while not self._stop_event.is_set():
            # 计算工作时间和休眠时间以达到目标负载
            cycle_duration = 0.1  # 100ms周期
            work_duration = cycle_duration * (load_percent / 100.0)
            sleep_duration = cycle_duration - work_duration
            
            # 工作阶段 - 使用更小的批次检查停止事件
            start = time.time()
            while time.time() - start < work_duration:
                if self._stop_event.is_set():
                    break
                # 执行计算密集型操作，但每批次后检查停止事件
                _ = sum(i * i for i in range(1000))
                # 每10ms检查一次停止事件
                if time.time() - start >= work_duration:
                    break
            
            # 休眠阶段 - 使用短睡眠循环以便快速响应停止
            if sleep_duration > 0 and not self._stop_event.is_set():
                sleep_start = time.time()
                while time.time() - sleep_start < sleep_duration:
                    if self._stop_event.is_set():
                        break
                    time.sleep(min(0.01, sleep_duration))

    def _adjust_process_priority(self, process_names: List[str], increase_load: bool) -> None:
        """调整进程优先级"""
        import psutil
        
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] in process_names:
                    if increase_load:
                        # 降低优先级(增加nice值)
                        psutil.Process(proc.info['pid']).nice(10)
                    else:
                        # 恢复优先级
                        psutil.Process(proc.info['pid']).nice(0)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def rollback(self) -> bool:
        """回滚CPU故障"""
        try:
            # 停止所有压力线程
            self._stop_event.set()
            
            # 等待所有线程结束，使用更长的超时
            remaining_threads = []
            for thread in self._threads:
                thread.join(timeout=10.0)
                if thread.is_alive():
                    remaining_threads.append(thread)
                    logger.warning(f"CPU stress thread {thread.name} did not terminate gracefully")
            
            # 清理线程列表
            self._threads.clear()
            
            # 重置停止事件（为下次使用做准备）
            self._stop_event.clear()
            
            # 恢复进程优先级
            config = self.config
            if config.target_processes:
                self._adjust_process_priority(config.target_processes, increase_load=False)
            
            # 如果有未终止的线程，记录警告但不返回失败
            if remaining_threads:
                logger.warning(f"{len(remaining_threads)} CPU stress threads could not be terminated")
            
            return True
        except Exception as e:
            logger.exception(f"CPU fault rollback failed: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """获取CPU指标"""
        import psutil
        
        cpu_percent = psutil.cpu_percent(interval=0.1)
        cpu_per_cpu = psutil.cpu_percent(interval=0.1, percpu=True)
        
        return {
            "cpu_percent": cpu_percent,
            "cpu_per_cpu": cpu_per_cpu,
            "active_cores": len(self._threads),
            "target_load": self.config.load_percent,
            "duration": self.get_duration(),
        }


class MemoryFaultInjector(FaultInjector):
    """内存故障注入器"""

    def __init__(self, config: MemoryFaultConfig):
        super().__init__(config)
        self.config = cast(MemoryFaultConfig, config)
        self._allocated_memory: List[bytearray] = []
        self._leak_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._current_allocation = 0

    def _save_rollback_state(self) -> None:
        """保存内存限制状态"""
        try:
            if HAS_RESOURCE:
                self._rollback_state['memory_limit'] = resource.getrlimit(resource.RLIMIT_AS)
        except Exception:
            pass

    def inject(self) -> bool:
        """注入内存故障"""
        try:
            config = self.config
            
            # 分配初始内存
            allocation_bytes = config.memory_mb * 1024 * 1024
            chunk = bytearray(allocation_bytes)
            self._allocated_memory.append(chunk)
            self._current_allocation += allocation_bytes
            
            # 如果需要内存泄漏，启动泄漏线程
            if config.leak_rate_mb_per_sec > 0:
                self._leak_thread = threading.Thread(
                    target=self._memory_leak_thread,
                    name="memory-leak"
                )
                self._leak_thread.daemon = True
                self._leak_thread.start()
            
            # 如果触发OOM，设置较低的内存限制
            if config.oom_trigger:
                self._set_memory_limit(self._current_allocation + 1024 * 1024)
            
            return True
        except Exception as e:
            logger.exception(f"Memory fault injection failed: {e}")
            return False

    def _memory_leak_thread(self) -> None:
        """内存泄漏线程"""
        config = self.config
        leak_bytes_per_sec = config.leak_rate_mb_per_sec * 1024 * 1024
        chunk_size = int(leak_bytes_per_sec)  # 每秒分配
        
        while not self._stop_event.is_set():
            try:
                chunk = bytearray(chunk_size)
                self._allocated_memory.append(chunk)
                self._current_allocation += chunk_size
                time.sleep(1.0)
            except MemoryError:
                logger.warning("Memory allocation failed - possible OOM")
                break

    def _set_memory_limit(self, limit_bytes: int) -> None:
        """设置内存限制"""
        try:
            if HAS_RESOURCE:
                resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
        except Exception as e:
            logger.warning(f"Failed to set memory limit: {e}")

    def rollback(self) -> bool:
        """回滚内存故障"""
        try:
            # 停止泄漏线程
            self._stop_event.set()
            if self._leak_thread:
                self._leak_thread.join(timeout=5.0)
            
            # 释放分配的内存
            self._allocated_memory.clear()
            self._current_allocation = 0
            
            # 恢复内存限制
            if 'memory_limit' in self._rollback_state:
                try:
                    if HAS_RESOURCE:
                        resource.setrlimit(resource.RLIMIT_AS, self._rollback_state['memory_limit'])
                except Exception:
                    pass
            
            # 强制垃圾回收
            import gc
            gc.collect()
            
            return True
        except Exception as e:
            logger.exception(f"Memory fault rollback failed: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """获取内存指标"""
        import psutil
        
        memory = psutil.virtual_memory()
        
        return {
            "total_mb": memory.total / (1024 * 1024),
            "available_mb": memory.available / (1024 * 1024),
            "percent": memory.percent,
            "allocated_mb": self._current_allocation / (1024 * 1024),
            "target_allocation_mb": self.config.memory_mb,
            "leak_rate_mb_per_sec": self.config.leak_rate_mb_per_sec,
        }


class NetworkFaultInjector(FaultInjector):
    """网络故障注入器"""

    def __init__(self, config: NetworkFaultConfig):
        super().__init__(config)
        self.config = cast(NetworkFaultConfig, config)
        self._tc_rules_applied = False
        self._original_tc_state: List[str] = []
        self._interface: str = "eth0"

    def _save_rollback_state(self) -> None:
        """保存网络配置状态"""
        try:
            # 保存当前的tc规则
            result = subprocess.run(
                ["tc", "qdisc", "show", "dev", self._interface],
                capture_output=True,
                text=True
            )
            self._original_tc_state = result.stdout.strip().split('\n') if result.returncode == 0 else []
        except Exception:
            pass

    def inject(self) -> bool:
        """注入网络故障"""
        try:
            config = self.config
            
            # 构建tc命令
            commands = []
            
            # 添加根qdisc
            commands.append(f"tc qdisc add dev {self._interface} root handle 1: htb default 12")
            
            # 添加延迟和丢包
            delay_cmd = f"tc qdisc add dev {self._interface} parent 1:1 handle 10: netem"
            
            if config.latency_ms > 0:
                delay_cmd += f" delay {config.latency_ms}ms"
                if config.jitter_ms > 0:
                    delay_cmd += f" {config.jitter_ms}ms"
            
            if config.packet_loss_percent > 0:
                delay_cmd += f" loss {config.packet_loss_percent}%"
            
            commands.append(delay_cmd)
            
            # 应用tc规则
            for cmd in commands:
                result = subprocess.run(cmd.split(), capture_output=True, text=True)
                if result.returncode != 0 and "File exists" not in result.stderr:
                    logger.error(f"tc command failed: {result.stderr}")
                    self.rollback()
                    return False
            
            self._tc_rules_applied = True
            return True
            
        except Exception as e:
            logger.exception(f"Network fault injection failed: {e}")
            return False

    def rollback(self) -> bool:
        """回滚网络故障"""
        try:
            if not self._tc_rules_applied:
                return True
            
            # 删除tc规则
            subprocess.run(
                ["tc", "qdisc", "del", "dev", self._interface, "root"],
                capture_output=True,
                text=True
            )
            
            self._tc_rules_applied = False
            return True
        except Exception as e:
            logger.exception(f"Network fault rollback failed: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """获取网络指标"""
        import psutil
        
        net_io = psutil.net_io_counters()
        
        return {
            "bytes_sent": net_io.bytes_sent,
            "bytes_recv": net_io.bytes_recv,
            "packets_sent": net_io.packets_sent,
            "packets_recv": net_io.packets_recv,
            "latency_ms": self.config.latency_ms,
            "packet_loss_percent": self.config.packet_loss_percent,
            "tc_rules_applied": self._tc_rules_applied,
        }


class DiskFaultInjector(FaultInjector):
    """磁盘故障注入器"""

    def __init__(self, config: DiskFaultConfig):
        super().__init__(config)
        self.config = cast(DiskFaultConfig, config)
        self._temp_files: List[str] = []
        self._io_delay_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def _save_rollback_state(self) -> None:
        """保存磁盘状态"""
        pass

    def inject(self) -> bool:
        """注入磁盘故障"""
        try:
            config = self.config
            
            # 如果需要填充磁盘
            if config.fill_percent is not None:
                self._fill_disk(config.fill_percent)
            
            # 如果需要IO延迟，使用device mapper或其他技术
            if config.io_delay_ms > 0:
                # 这里简化处理，实际可能需要dm-delay或其他内核技术
                logger.warning("IO delay injection requires kernel-level support")
            
            return True
        except Exception as e:
            logger.exception(f"Disk fault injection failed: {e}")
            return False

    def _fill_disk(self, fill_percent: float) -> None:
        """填充磁盘空间"""
        import psutil
        
        disk = psutil.disk_usage('/')
        target_usage = disk.total * (fill_percent / 100.0)
        to_fill = target_usage - disk.used
        
        if to_fill > 0:
            # 创建大文件填充磁盘
            chunk_size = 100 * 1024 * 1024  # 100MB chunks
            fd, path = tempfile.mkstemp(prefix="chaos_disk_fill_")
            
            try:
                with os.fdopen(fd, 'wb') as f:
                    while to_fill > 0 and not self._stop_event.is_set():
                        write_size = min(chunk_size, to_fill)
                        f.write(b'\x00' * write_size)
                        f.flush()
                        to_fill -= write_size
                
                self._temp_files.append(path)
            except Exception as e:
                logger.error(f"Failed to fill disk: {e}")
                try:
                    os.unlink(path)
                except Exception:
                    pass

    def rollback(self) -> bool:
        """回滚磁盘故障"""
        try:
            self._stop_event.set()
            
            # 删除临时文件
            for path in self._temp_files:
                try:
                    if os.path.exists(path):
                        os.unlink(path)
                except Exception as e:
                    logger.warning(f"Failed to remove temp file {path}: {e}")
            
            self._temp_files.clear()
            return True
        except Exception as e:
            logger.exception(f"Disk fault rollback failed: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """获取磁盘指标"""
        import psutil
        
        disk = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()
        
        return {
            "total_gb": disk.total / (1024 ** 3),
            "used_gb": disk.used / (1024 ** 3),
            "free_gb": disk.free / (1024 ** 3),
            "percent": disk.percent,
            "read_bytes": disk_io.read_bytes if disk_io else 0,
            "write_bytes": disk_io.write_bytes if disk_io else 0,
            "temp_files_count": len(self._temp_files),
        }


class KubernetesPodFaultInjector(FaultInjector):
    """Kubernetes Pod故障注入器"""

    def __init__(self, config: PodFaultConfig):
        super().__init__(config)
        self.config = cast(PodFaultConfig, config)
        self._affected_pods: List[str] = []
        self._k8s_available = self._check_k8s_availability()

    def _check_k8s_availability(self) -> bool:
        """检查Kubernetes是否可用"""
        try:
            result = subprocess.run(
                ["kubectl", "version"],
                capture_output=True,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False

    def _save_rollback_state(self) -> None:
        """保存Pod状态"""
        pass

    def inject(self) -> bool:
        """注入Pod故障"""
        if not self._k8s_available:
            logger.error("Kubernetes not available for pod fault injection")
            return False
        
        try:
            config = self.config
            
            # 获取目标Pod列表
            pods = self._get_target_pods()
            
            if not pods:
                logger.warning("No target pods found")
                return False
            
            # 计算要影响的Pod数量
            affected_count = max(1, int(len(pods) * config.affected_pods_percent / 100))
            selected_pods = random.sample(pods, min(affected_count, len(pods)))
            
            # 执行故障操作
            for pod in selected_pods:
                if config.fault_action == "terminate":
                    self._terminate_pod(pod)
                elif config.fault_action == "restart":
                    self._restart_pod(pod)
                elif config.fault_action == "network-partition":
                    self._network_partition_pod(pod)
                
                self._affected_pods.append(pod)
            
            return True
        except Exception as e:
            logger.exception(f"Pod fault injection failed: {e}")
            return False

    def _get_target_pods(self) -> List[str]:
        """获取目标Pod列表"""
        config = self.config
        
        cmd = ["kubectl", "get", "pods", "-n", config.namespace, "-o", "jsonpath={.items[*].metadata.name}"]
        
        if config.label_selector:
            selector = ",".join(f"{k}={v}" for k, v in config.label_selector.items())
            cmd.extend(["-l", selector])
        
        if config.pod_name:
            cmd.extend([config.pod_name])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            return result.stdout.strip().split()
        return []

    def _terminate_pod(self, pod_name: str) -> None:
        """终止Pod"""
        cmd = ["kubectl", "delete", "pod", pod_name, "-n", self.config.namespace, "--force"]
        subprocess.run(cmd, capture_output=True)
        logger.info(f"Terminated pod: {pod_name}")

    def _restart_pod(self, pod_name: str) -> None:
        """重启Pod (通过删除实现)"""
        self._terminate_pod(pod_name)

    def _network_partition_pod(self, pod_name: str) -> None:
        """网络隔离Pod"""
        # 使用kubectl exec在Pod内执行iptables命令
        cmd = [
            "kubectl", "exec", pod_name, "-n", self.config.namespace,
            "--", "iptables", "-A", "OUTPUT", "-j", "DROP"
        ]
        subprocess.run(cmd, capture_output=True)
        logger.info(f"Network partitioned pod: {pod_name}")

    def rollback(self) -> bool:
        """回滚Pod故障"""
        try:
            # 对于终止的Pod，Kubernetes会自动重新创建
            # 对于网络隔离，需要恢复网络
            config = self.config
            
            if config.fault_action == "network-partition":
                for pod in self._affected_pods:
                    cmd = [
                        "kubectl", "exec", pod, "-n", config.namespace,
                        "--", "iptables", "-F"
                    ]
                    subprocess.run(cmd, capture_output=True)
            
            self._affected_pods.clear()
            return True
        except Exception as e:
            logger.exception(f"Pod fault rollback failed: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """获取Pod指标"""
        metrics = {
            "k8s_available": self._k8s_available,
            "affected_pods": self._affected_pods,
            "namespace": self.config.namespace,
            "fault_action": self.config.fault_action,
        }
        
        if self._k8s_available:
            # 获取Pod状态
            cmd = ["kubectl", "get", "pods", "-n", self.config.namespace, "-o", "json"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    pod_statuses = {}
                    for item in data.get('items', []):
                        name = item['metadata']['name']
                        phase = item['status'].get('phase', 'Unknown')
                        pod_statuses[name] = phase
                    metrics['pod_statuses'] = pod_statuses
                except json.JSONDecodeError:
                    pass
        
        return metrics


class ChaosEngineeringFramework:
    """
    混沌工程框架主类
    
    DEFECT-007 [P2] 新增使用示例:
    
    示例用法:
        >>> from chaos_engineering import ChaosEngineeringFramework, FaultConfig, FaultType
        >>> 
        >>> framework = ChaosEngineeringFramework()
        >>> 
        >>> # 创建故障实验
        >>> config = FaultConfig(
        ...     fault_type=FaultType.CPU,
        ...     duration=30,
        ...     intensity=0.5
        ... )
        >>> experiment = framework.create_experiment(
        ...     name="cpu-stress-test",
        ...     description="Test service under CPU stress",
        ...     fault_config=config
        ... )
        >>> 
        >>> # 运行实验
        >>> result = await framework.run_experiment("cpu-stress-test")
        >>> print(result.resilience_score)
    """

    def __init__(self):
        self._experiments: Dict[str, ChaosExperiment] = {}
        self._active_injectors: Dict[str, FaultInjector] = {}
        self._experiment_history: List[Dict[str, Any]] = []
        self._lock = threading.RLock()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_scheduler = threading.Event()
        self._resilience_scores: List[ResilienceScore] = []
        
        # 注册故障注入器工厂
        self._injector_factories: Dict[FaultType, Callable[[FaultConfig], FaultInjector]] = {
            FaultType.CPU: lambda c: CPUFaultInjector(cast(CPUFaultConfig, c)),
            FaultType.MEMORY: lambda c: MemoryFaultInjector(cast(MemoryFaultConfig, c)),
            FaultType.NETWORK: lambda c: NetworkFaultInjector(cast(NetworkFaultConfig, c)),
            FaultType.DISK: lambda c: DiskFaultInjector(cast(DiskFaultConfig, c)),
            FaultType.POD: lambda c: KubernetesPodFaultInjector(cast(PodFaultConfig, c)),
        }

    def create_experiment(
        self,
        name: str,
        description: str,
        fault_type: FaultType,
        fault_config: FaultConfig,
        target_scope: Optional[Dict[str, Any]] = None,
        schedule: Optional[str] = None
    ) -> ChaosExperiment:
        """创建混沌实验"""
        experiment = ChaosExperiment(
            name=name,
            description=description,
            fault_type=fault_type,
            fault_config=fault_config,
            target_scope=target_scope or {},
            schedule=schedule
        )
        
        with self._lock:
            self._experiments[experiment.id] = experiment
        
        logger.info(f"Created chaos experiment: {name} (ID: {experiment.id})")
        return experiment

    def start_experiment(self, experiment_id: str) -> bool:
        """启动混沌实验"""
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if not experiment:
                logger.error(f"Experiment not found: {experiment_id}")
                return False
            
            if experiment.status == ExperimentStatus.RUNNING:
                logger.warning(f"Experiment already running: {experiment_id}")
                return False
            
            # 创建故障注入器
            factory = self._injector_factories.get(experiment.fault_type)
            if not factory:
                logger.error(f"No injector factory for fault type: {experiment.fault_type}")
                return False
            
            injector = factory(experiment.fault_config)
            
            # 启动故障注入
            if injector.start():
                experiment.status = ExperimentStatus.RUNNING
                experiment.started_at = datetime.utcnow()
                self._active_injectors[experiment_id] = injector
                
                # 启动监控线程
                threading.Thread(
                    target=self._monitor_experiment,
                    args=(experiment_id,),
                    name=f"monitor-{experiment_id[:8]}"
                ).start()
                
                logger.info(f"Started chaos experiment: {experiment.name}")
                return True
            else:
                experiment.status = ExperimentStatus.FAILED
                logger.error(f"Failed to start experiment: {experiment_id}")
                return False

    def stop_experiment(self, experiment_id: str) -> bool:
        """停止混沌实验"""
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if not experiment:
                return False
            
            injector = self._active_injectors.get(experiment_id)
            if injector:
                if injector.stop():
                    experiment.status = ExperimentStatus.COMPLETED
                    experiment.completed_at = datetime.utcnow()
                    del self._active_injectors[experiment_id]
                    
                    # 记录历史
                    self._experiment_history.append({
                        "experiment_id": experiment_id,
                        "name": experiment.name,
                        "status": experiment.status.value,
                        "started_at": experiment.started_at.isoformat() if experiment.started_at else None,
                        "completed_at": experiment.completed_at.isoformat() if experiment.completed_at else None,
                    })
                    
                    logger.info(f"Stopped chaos experiment: {experiment.name}")
                    return True
                else:
                    experiment.status = ExperimentStatus.FAILED
                    return False
            
            return True

    def _monitor_experiment(self, experiment_id: str) -> None:
        """监控实验执行"""
        max_iterations = 1000  # 防止无限循环
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            should_stop = False
            auto_rollback = False
            
            with self._lock:
                experiment = self._experiments.get(experiment_id)
                injector = self._active_injectors.get(experiment_id)
                
                if not experiment or not injector:
                    logger.debug(f"Monitor for {experiment_id[:8]}: experiment or injector not found, exiting")
                    break
                
                # 检查是否超过持续时间
                if experiment.started_at:
                    duration = (datetime.utcnow() - experiment.started_at).total_seconds()
                    if duration >= experiment.fault_config.duration:
                        # 标记需要停止，但在释放锁后执行
                        should_stop = True
                        auto_rollback = experiment.fault_config.auto_rollback
                        logger.debug(f"Monitor for {experiment_id[:8]}: duration {duration:.1f}s >= {experiment.fault_config.duration}s, should_stop={should_stop}, auto_rollback={auto_rollback}")
                
                # 收集指标
                metrics = injector.get_metrics()
                experiment.result = metrics
            
            # 在锁外调用stop_experiment，避免死锁
            if should_stop:
                if auto_rollback:
                    logger.debug(f"Monitor for {experiment_id[:8]}: calling stop_experiment")
                    self.stop_experiment(experiment_id)
                break
            
            time.sleep(1.0)  # 缩短检查间隔，更快响应
        
        logger.debug(f"Monitor for {experiment_id[:8]}: exited after {iteration} iterations")

    def get_experiment_status(self, experiment_id: str) -> Optional[Dict[str, Any]]:
        """获取实验状态"""
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if not experiment:
                return None
            
            injector = self._active_injectors.get(experiment_id)
            metrics = injector.get_metrics() if injector else {}
            
            return {
                "id": experiment.id,
                "name": experiment.name,
                "status": experiment.status.value,
                "fault_type": experiment.fault_type.name,
                "started_at": experiment.started_at.isoformat() if experiment.started_at else None,
                "completed_at": experiment.completed_at.isoformat() if experiment.completed_at else None,
                "duration": injector.get_duration() if injector else 0,
                "metrics": metrics,
            }

    def list_experiments(self) -> List[Dict[str, Any]]:
        """列出所有实验"""
        with self._lock:
            return [
                {
                    "id": exp.id,
                    "name": exp.name,
                    "status": exp.status.value,
                    "fault_type": exp.fault_type.name,
                    "created_at": exp.created_at.isoformat(),
                }
                for exp in self._experiments.values()
            ]

    def calculate_resilience_score(
        self,
        availability_data: Dict[str, Any],
        latency_data: Dict[str, Any],
        error_data: Dict[str, Any],
        recovery_data: Dict[str, Any]
    ) -> ResilienceScore:
        """计算系统韧性评分"""
        # 可用性评分 (0-25)
        uptime_percent = availability_data.get("uptime_percent", 100)
        availability_score = min(25, uptime_percent / 4)
        
        # 延迟评分 (0-25)
        p99_latency = latency_data.get("p99_ms", 0)
        target_latency = latency_data.get("target_ms", 100)
        if p99_latency <= target_latency:
            latency_score = 25
        else:
            latency_score = max(0, 25 * (1 - (p99_latency - target_latency) / target_latency))
        
        # 错误率评分 (0-25)
        error_rate = error_data.get("error_rate_percent", 0)
        error_score = max(0, 25 - error_rate)
        
        # 恢复能力评分 (0-25)
        mttr = recovery_data.get("mttr_seconds", 0)
        target_mttr = recovery_data.get("target_mttr_seconds", 60)
        if mttr <= target_mttr:
            recoverability_score = 25
        else:
            recoverability_score = max(0, 25 * (1 - (mttr - target_mttr) / target_mttr))
        
        overall_score = availability_score + latency_score + error_score + recoverability_score
        
        score = ResilienceScore(
            overall_score=overall_score,
            availability_score=availability_score,
            recoverability_score=recoverability_score,
            latency_score=latency_score,
            error_rate_score=error_score,
            details={
                "availability": availability_data,
                "latency": latency_data,
                "errors": error_data,
                "recovery": recovery_data,
            }
        )
        
        self._resilience_scores.append(score)
        return score

    def get_resilience_trend(self, hours: int = 24) -> List[Dict[str, Any]]:
        """获取韧性评分趋势"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        scores = [
            {
                "timestamp": score.timestamp.isoformat(),
                "overall_score": score.overall_score,
                "availability_score": score.availability_score,
                "recoverability_score": score.recoverability_score,
                "latency_score": score.latency_score,
                "error_rate_score": score.error_rate_score,
            }
            for score in self._resilience_scores
            if score.timestamp >= cutoff_time
        ]
        
        return scores

    def run_chaos_suite(
        self,
        experiments: List[ChaosExperiment],
        delay_between: float = 60.0,
        max_wait_per_experiment: float = 300.0  # 每个实验最大等待时间
    ) -> Dict[str, Any]:
        """运行混沌实验套件"""
        results = []
        
        for experiment in experiments:
            logger.info(f"Running experiment: {experiment.name}")
            start_time = time.time()
            
            # 启动实验
            if self.start_experiment(experiment.id):
                # 等待实验完成或达到持续时间
                while True:
                    status = self.get_experiment_status(experiment.id)
                    if status and status["status"] in ["completed", "failed", "rolled_back"]:
                        break
                    
                    # 检查是否超过最大等待时间
                    elapsed = time.time() - start_time
                    if elapsed >= max_wait_per_experiment:
                        logger.warning(f"Experiment {experiment.name} exceeded max wait time, forcing stop")
                        self.stop_experiment(experiment.id)
                        break
                    
                    time.sleep(1.0)
                
                # 重新获取最终状态
                final_status = self.get_experiment_status(experiment.id)
                results.append({
                    "experiment_id": experiment.id,
                    "name": experiment.name,
                    "status": final_status["status"] if final_status else "unknown",
                    "metrics": final_status.get("metrics", {}) if final_status else {},
                })
            else:
                results.append({
                    "experiment_id": experiment.id,
                    "name": experiment.name,
                    "status": "failed_to_start",
                    "metrics": {},
                })
            
            # 等待一段时间再运行下一个实验
            if delay_between > 0:
                time.sleep(delay_between)
        
        return {
            "total_experiments": len(experiments),
            "completed": sum(1 for r in results if r["status"] == "completed"),
            "failed": sum(1 for r in results if r["status"] in ["failed", "failed_to_start"]),
            "results": results,
        }

    def emergency_stop_all(self) -> bool:
        """紧急停止所有实验"""
        with self._lock:
            experiment_ids = list(self._active_injectors.keys())
        
        success = True
        for exp_id in experiment_ids:
            if not self.stop_experiment(exp_id):
                success = False
        
        logger.info(f"Emergency stop executed for {len(experiment_ids)} experiments")
        return success

    def shutdown(self) -> None:
        """关闭框架"""
        self._stop_scheduler.set()
        self.emergency_stop_all()
        logger.info("Chaos engineering framework shutdown")


# 全局框架实例
_chaos_framework: Optional[ChaosEngineeringFramework] = None


def get_chaos_framework() -> ChaosEngineeringFramework:
    """获取全局混沌工程框架实例"""
    global _chaos_framework
    if _chaos_framework is None:
        _chaos_framework = ChaosEngineeringFramework()
    return _chaos_framework


# 便捷函数
def inject_cpu_fault(
    cores: int = 1,
    load_percent: float = 80.0,
    duration: float = 60.0
) -> str:
    """便捷函数：注入CPU故障"""
    framework = get_chaos_framework()
    
    config = CPUFaultConfig(
        cores=cores,
        load_percent=load_percent,
        duration=duration
    )
    
    experiment = framework.create_experiment(
        name=f"cpu-fault-{int(time.time())}",
        description=f"CPU fault injection: {load_percent}% on {cores} cores",
        fault_type=FaultType.CPU,
        fault_config=config
    )
    
    framework.start_experiment(experiment.id)
    return experiment.id


def inject_memory_fault(
    memory_mb: int = 512,
    leak_rate_mb_per_sec: float = 0.0,
    duration: float = 60.0
) -> str:
    """便捷函数：注入内存故障"""
    framework = get_chaos_framework()
    
    config = MemoryFaultConfig(
        memory_mb=memory_mb,
        leak_rate_mb_per_sec=leak_rate_mb_per_sec,
        duration=duration
    )
    
    experiment = framework.create_experiment(
        name=f"memory-fault-{int(time.time())}",
        description=f"Memory fault injection: {memory_mb}MB",
        fault_type=FaultType.MEMORY,
        fault_config=config
    )
    
    framework.start_experiment(experiment.id)
    return experiment.id


def inject_network_fault(
    latency_ms: float = 100.0,
    packet_loss_percent: float = 0.0,
    duration: float = 60.0
) -> str:
    """便捷函数：注入网络故障"""
    framework = get_chaos_framework()
    
    config = NetworkFaultConfig(
        latency_ms=latency_ms,
        packet_loss_percent=packet_loss_percent,
        duration=duration
    )
    
    experiment = framework.create_experiment(
        name=f"network-fault-{int(time.time())}",
        description=f"Network fault: {latency_ms}ms latency, {packet_loss_percent}% loss",
        fault_type=FaultType.NETWORK,
        fault_config=config
    )
    
    framework.start_experiment(experiment.id)
    return experiment.id


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    # 创建框架实例
    framework = get_chaos_framework()
    
    # 示例1: CPU故障注入
    print("=== CPU Fault Injection Example ===")
    cpu_config = CPUFaultConfig(
        cores=2,
        load_percent=50.0,
        duration=10.0
    )
    
    cpu_experiment = framework.create_experiment(
        name="cpu-stress-test",
        description="Test system behavior under CPU stress",
        fault_type=FaultType.CPU,
        fault_config=cpu_config
    )
    
    print(f"Created experiment: {cpu_experiment.id}")
    
    # 示例2: 内存故障注入
    print("\n=== Memory Fault Injection Example ===")
    mem_config = MemoryFaultConfig(
        memory_mb=256,
        leak_rate_mb_per_sec=10.0,
        duration=30.0
    )
    
    mem_experiment = framework.create_experiment(
        name="memory-leak-test",
        description="Test system behavior under memory leak",
        fault_type=FaultType.MEMORY,
        fault_config=mem_config
    )
    
    print(f"Created experiment: {mem_experiment.id}")
    
    # 示例3: 计算韧性评分
    print("\n=== Resilience Score Calculation ===")
    score = framework.calculate_resilience_score(
        availability_data={"uptime_percent": 99.9},
        latency_data={"p99_ms": 50, "target_ms": 100},
        error_data={"error_rate_percent": 0.1},
        recovery_data={"mttr_seconds": 30, "target_mttr_seconds": 60}
    )
    
    print(f"Overall Resilience Score: {score.overall_score}/100")
    print(f"  - Availability: {score.availability_score}/25")
    print(f"  - Latency: {score.latency_score}/25")
    print(f"  - Error Rate: {score.error_rate_score}/25")
    print(f"  - Recoverability: {score.recoverability_score}/25")
    
    # 清理
    framework.shutdown()
    print("\nChaos engineering framework demo completed.")

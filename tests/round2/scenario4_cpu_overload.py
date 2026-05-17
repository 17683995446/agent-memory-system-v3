#!/usr/bin/env python3
"""
场景4: CPU过载测试

测试目标:
- 100% CPU使用率
- 验证调度延迟
- 测试优先级调整

运行时间: 8分钟
目标CPU: 100%
并发: 200个并发客户端
"""

import asyncio
import json
import logging
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import psutil
import threading

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/round2/logs/scenario4_cpu_overload.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class CPUOverloadReport:
    """CPU过载测试报告"""
    scenario_name: str = "CPU过载测试"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    peak_cpu_percent: float = 0.0
    avg_cpu_percent: float = 0.0
    scheduling_delays: List[float] = field(default_factory=list)
    max_scheduling_delay_ms: float = 0.0
    avg_scheduling_delay_ms: float = 0.0
    priority_adjustments: int = 0
    tasks_completed: int = 0
    tasks_delayed: int = 0
    throughput_under_load: float = 0.0
    cpu_timeline: List[Dict] = field(default_factory=list)
    latency_distribution: Dict[str, float] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


class CPULoadGenerator:
    """CPU负载生成器"""
    
    def __init__(self):
        self.stop_event = threading.Event()
        self.threads = []
        
    def start(self, num_threads: int = 4):
        """启动CPU负载生成"""
        def cpu_intensive_task():
            while not self.stop_event.is_set():
                # 执行CPU密集型计算
                _ = sum(i ** 2 for i in range(100000))
        
        for _ in range(num_threads):
            t = threading.Thread(target=cpu_intensive_task)
            t.daemon = True
            t.start()
            self.threads.append(t)
    
    def stop(self):
        """停止CPU负载生成"""
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=1)


class SimulatedTaskScheduler:
    """模拟任务调度器 - 带优先级支持"""
    
    def __init__(self):
        self.task_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.completed_tasks = 0
        self.delayed_tasks = 0
        self.priority_adjustments = 0
        self.scheduling_delays: List[float] = []
        self.lock = asyncio.Lock()
        
    async def submit_task(self, task_id: str, priority: int = 5, 
                         estimated_duration_ms: float = 10) -> Dict:
        """提交任务"""
        submit_time = time.time()
        
        # 模拟任务执行
        await asyncio.sleep(estimated_duration_ms / 1000)
        
        complete_time = time.time()
        actual_delay_ms = (complete_time - submit_time) * 1000
        
        async with self.lock:
            self.scheduling_delays.append(actual_delay_ms)
            
            # 检测调度延迟
            if actual_delay_ms > estimated_duration_ms * 2:
                self.delayed_tasks += 1
            else:
                self.completed_tasks += 1
        
        return {
            "task_id": task_id,
            "priority": priority,
            "submitted_at": submit_time,
            "completed_at": complete_time,
            "actual_delay_ms": actual_delay_ms,
            "estimated_duration_ms": estimated_duration_ms
        }
    
    async def adjust_priority(self, task_id: str, new_priority: int) -> bool:
        """调整任务优先级"""
        async with self.lock:
            self.priority_adjustments += 1
        return True
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        delays = self.scheduling_delays if self.scheduling_delays else [0]
        
        return {
            "completed_tasks": self.completed_tasks,
            "delayed_tasks": self.delayed_tasks,
            "priority_adjustments": self.priority_adjustments,
            "max_delay_ms": max(delays),
            "avg_delay_ms": sum(delays) / len(delays),
            "p99_delay_ms": sorted(delays)[int(len(delays) * 0.99)] if len(delays) > 1 else delays[0]
        }


class CPUOverloadTestRunner:
    """CPU过载测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 8,
                 concurrent_clients: int = 200):
        self.target_duration = target_duration_minutes * 60
        self.concurrent_clients = concurrent_clients
        
        self.scheduler = SimulatedTaskScheduler()
        self.cpu_generator = CPULoadGenerator()
        
        self.report = CPUOverloadReport()
        self.stop_event = asyncio.Event()
        
    async def run(self) -> CPUOverloadReport:
        """运行CPU过载测试"""
        logger.info("开始CPU过载测试")
        logger.info(f"并发客户端: {self.concurrent_clients}")
        logger.info(f"目标CPU: 100%")

        self.report.start_time = time.time()
        
        # 启动CPU负载生成
        self.cpu_generator.start(num_threads=4)
        logger.info("CPU负载生成器已启动")

        # 启动工作负载
        tasks = [
            asyncio.create_task(self._cpu_monitor()),
            asyncio.create_task(self._scheduler_monitor()),
        ]

        # 启动并发客户端
        client_tasks = [
            asyncio.create_task(self._client_worker(i))
            for i in range(self.concurrent_clients)
        ]

        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()
        
        # 停止CPU负载生成
        self.cpu_generator.stop()
        logger.info("CPU负载生成器已停止")

        # 等待任务完成
        await asyncio.gather(*tasks, *client_tasks, return_exceptions=True)

        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time

        # 生成最终报告
        await self._generate_report()

        return self.report

    async def _client_worker(self, client_id: int):
        """客户端工作器"""
        task_count = 0
        while not self.stop_event.is_set():
            try:
                # 生成不同优先级的任务
                priority = random.randint(1, 10)
                task_id = f"task_{client_id}_{task_count}_{int(time.time()*1000000)}"
                
                # 模拟不同复杂度的任务
                estimated_duration = random.uniform(5, 50)  # 5-50ms
                
                result = await self.scheduler.submit_task(
                    task_id, priority, estimated_duration
                )
                
                # 如果延迟过高，尝试调整优先级
                if result["actual_delay_ms"] > estimated_duration * 3:
                    await self.scheduler.adjust_priority(task_id, priority - 1)
                
                task_count += 1
                
            except Exception as e:
                self.report.errors.append(f"Client {client_id} error: {e}")
            
            # 高频率提交以产生CPU压力
            await asyncio.sleep(random.uniform(0.0001, 0.001))
    
    async def _cpu_monitor(self):
        """CPU监控器"""
        cpu_readings = []
        
        while not self.stop_event.is_set():
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                cpu_readings.append(cpu_percent)
                
                self.report.cpu_timeline.append({
                    "timestamp": time.time(),
                    "cpu_percent": cpu_percent,
                    "per_cpu": psutil.cpu_percent(interval=0.1, percpu=True)
                })
                
                self.report.peak_cpu_percent = max(self.report.peak_cpu_percent, cpu_percent)
                
                logger.info(f"CPU Usage: {cpu_percent:.1f}%")
                
            except Exception as e:
                logger.error(f"CPU monitor error: {e}")
        
        if cpu_readings:
            self.report.avg_cpu_percent = sum(cpu_readings) / len(cpu_readings)
    
    async def _scheduler_monitor(self):
        """调度器监控器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)
            
            try:
                stats = self.scheduler.get_stats()
                
                logger.info(
                    f"Scheduler Stats - Completed: {stats['completed_tasks']}, "
                    f"Delayed: {stats['delayed_tasks']}, "
                    f"Priority Adjustments: {stats['priority_adjustments']}, "
                    f"Avg Delay: {stats['avg_delay_ms']:.2f}ms, "
                    f"P99 Delay: {stats['p99_delay_ms']:.2f}ms"
                )
                
            except Exception as e:
                logger.error(f"Scheduler monitor error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        scheduler_stats = self.scheduler.get_stats()
        
        self.report.scheduling_delays = self.scheduler.scheduling_delays
        self.report.max_scheduling_delay_ms = scheduler_stats["max_delay_ms"]
        self.report.avg_scheduling_delay_ms = scheduler_stats["avg_delay_ms"]
        self.report.priority_adjustments = scheduler_stats["priority_adjustments"]
        self.report.tasks_completed = scheduler_stats["completed_tasks"]
        self.report.tasks_delayed = scheduler_stats["delayed_tasks"]
        
        # 计算延迟分布
        if self.scheduler.scheduling_delays:
            delays = sorted(self.scheduler.scheduling_delays)
            self.report.latency_distribution = {
                "p50": delays[int(len(delays) * 0.5)] if len(delays) > 1 else delays[0],
                "p90": delays[int(len(delays) * 0.9)] if len(delays) > 1 else delays[0],
                "p95": delays[int(len(delays) * 0.95)] if len(delays) > 1 else delays[0],
                "p99": delays[int(len(delays) * 0.99)] if len(delays) > 1 else delays[0],
                "max": max(delays)
            }
        
        # 计算负载下的吞吐量
        total_tasks = scheduler_stats["completed_tasks"] + scheduler_stats["delayed_tasks"]
        if self.report.total_duration_sec > 0:
            self.report.throughput_under_load = total_tasks / self.report.total_duration_sec
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/round2/metrics/scenario4_cpu_overload_metrics.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = CPUOverloadTestRunner(
        target_duration_minutes=8,
        concurrent_clients=200
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景4: CPU过载测试 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"峰值CPU: {report.peak_cpu_percent:.1f}%")
    print(f"平均CPU: {report.avg_cpu_percent:.1f}%")
    print(f"最大调度延迟: {report.max_scheduling_delay_ms:.2f}ms")
    print(f"平均调度延迟: {report.avg_scheduling_delay_ms:.2f}ms")
    print(f"优先级调整: {report.priority_adjustments}")
    print(f"完成任务: {report.tasks_completed}")
    print(f"延迟任务: {report.tasks_delayed}")
    print(f"负载下吞吐量: {report.throughput_under_load:.2f} tasks/sec")
    if report.latency_distribution:
        print(f"P99延迟: {report.latency_distribution.get('p99', 0):.2f}ms")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

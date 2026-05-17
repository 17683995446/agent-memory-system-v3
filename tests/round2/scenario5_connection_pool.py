#!/usr/bin/env python3
"""
场景5: 连接池耗尽测试

测试目标:
- 耗尽所有连接
- 验证优雅降级
- 测试连接回收

运行时间: 8分钟
连接池大小: 50
并发: 300个并发客户端
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
from typing import Any, Dict, List, Optional, Set, Tuple
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/round2/logs/scenario5_connection_pool.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ConnectionPoolReport:
    """连接池测试报告"""
    scenario_name: str = "连接池耗尽测试"
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration_sec: float = 0.0
    pool_size: int = 50
    pool_exhaustion_events: int = 0
    wait_queue_max_length: int = 0
    wait_queue_avg_wait_ms: float = 0.0
    graceful_degradations: int = 0
    connections_recycled: int = 0
    connection_timeouts: int = 0
    requests_served: int = 0
    requests_rejected: int = 0
    pool_utilization_timeline: List[Dict] = field(default_factory=list)
    wait_queue_timeline: List[Dict] = field(default_factory=list)
    degradation_events: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class SimulatedConnection:
    """模拟连接"""
    
    def __init__(self, connection_id: str):
        self.id = connection_id
        self.created_at = time.time()
        self.last_used_at = time.time()
        self.use_count = 0
        self.is_active = True
        self.is_broken = False
        
    async def execute(self, operation: str, data: Dict = None) -> Dict:
        """执行操作"""
        if self.is_broken:
            raise Exception("Connection is broken")
        
        self.last_used_at = time.time()
        self.use_count += 1
        
        # 模拟操作延迟
        await asyncio.sleep(random.uniform(0.001, 0.01))
        
        return {
            "success": True,
            "connection_id": self.id,
            "operation": operation,
            "result": data or {}
        }
    
    def mark_broken(self):
        """标记连接为损坏"""
        self.is_broken = True
        self.is_active = False
    
    def mark_idle(self):
        """标记连接为空闲"""
        self.is_active = False


class ConnectionPool:
    """模拟连接池"""
    
    def __init__(self, max_connections: int = 50, 
                 connection_timeout: float = 30.0,
                 max_wait_queue: int = 100):
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.max_wait_queue = max_wait_queue
        
        self.connections: Dict[str, SimulatedConnection] = {}
        self.available_connections: asyncio.Queue = asyncio.Queue()
        self.wait_queue: asyncio.Queue = asyncio.Queue()
        
        self.connection_counter = 0
        self.exhaustion_count = 0
        self.recycled_count = 0
        self.timeout_count = 0
        self.requests_served = 0
        self.requests_rejected = 0
        
        self.lock = asyncio.Lock()
        
    async def initialize(self):
        """初始化连接池"""
        for _ in range(self.max_connections):
            await self._create_connection()
    
    async def _create_connection(self) -> SimulatedConnection:
        """创建新连接"""
        self.connection_counter += 1
        conn_id = f"conn_{self.connection_counter}_{int(time.time()*1000000)}"
        conn = SimulatedConnection(conn_id)
        self.connections[conn_id] = conn
        await self.available_connections.put(conn)
        return conn
    
    async def acquire_connection(self, timeout: float = None) -> Optional[SimulatedConnection]:
        """获取连接"""
        timeout = timeout or self.connection_timeout
        
        try:
            # 尝试从可用连接池获取
            conn = await asyncio.wait_for(
                self.available_connections.get(),
                timeout=0.5
            )
            conn.is_active = True
            return conn
            
        except asyncio.TimeoutError:
            # 连接池耗尽，加入等待队列
            async with self.lock:
                if self.wait_queue.qsize() >= self.max_wait_queue:
                    self.requests_rejected += 1
                    return None
                
                self.exhaustion_count += 1
                
                # 尝试回收空闲连接
                recycled = await self._recycle_connections()
                if recycled > 0:
                    # 重试获取
                    try:
                        conn = await asyncio.wait_for(
                            self.available_connections.get(),
                            timeout=timeout
                        )
                        conn.is_active = True
                        return conn
                    except asyncio.TimeoutError:
                        self.timeout_count += 1
                        return None
                else:
                    # 优雅降级 - 等待更长时间
                    try:
                        conn = await asyncio.wait_for(
                            self.available_connections.get(),
                            timeout=timeout * 2
                        )
                        conn.is_active = True
                        return conn
                    except asyncio.TimeoutError:
                        self.timeout_count += 1
                        return None
    
    async def release_connection(self, conn: SimulatedConnection):
        """释放连接"""
        if conn.is_broken:
            # 移除损坏的连接并创建新连接
            del self.connections[conn.id]
            await self._create_connection()
        else:
            conn.is_active = False
            conn.mark_idle()
            await self.available_connections.put(conn)
    
    async def _recycle_connections(self) -> int:
        """回收空闲连接"""
        recycled = 0
        current_time = time.time()
        
        # 检查是否有长时间未使用的连接
        for conn in list(self.connections.values()):
            if not conn.is_active and (current_time - conn.last_used_at) > 60:
                # 重置连接
                conn.last_used_at = current_time
                conn.use_count = 0
                recycled += 1
                self.recycled_count += 1
        
        return recycled
    
    async def execute_with_connection(self, operation: str, data: Dict = None) -> Dict:
        """使用连接执行操作"""
        conn = await self.acquire_connection()
        
        if conn is None:
            return {
                "success": False,
                "error": "connection_pool_exhausted",
                "degraded": True
            }
        
        try:
            result = await conn.execute(operation, data)
            self.requests_served += 1
            return result
        except Exception as e:
            conn.mark_broken()
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            await self.release_connection(conn)
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        active_count = sum(1 for c in self.connections.values() if c.is_active)
        
        return {
            "total_connections": len(self.connections),
            "active_connections": active_count,
            "available_connections": self.available_connections.qsize(),
            "wait_queue_size": self.wait_queue.qsize(),
            "exhaustion_count": self.exhaustion_count,
            "recycled_count": self.recycled_count,
            "timeout_count": self.timeout_count,
            "requests_served": self.requests_served,
            "requests_rejected": self.requests_rejected,
            "utilization_percent": (active_count / self.max_connections) * 100 if self.max_connections > 0 else 0
        }


class ConnectionPoolFaultInjector:
    """连接池故障注入器"""
    
    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.fault_history: List[Dict] = []
    
    async def inject_connection_failure(self, probability: float = 0.01):
        """注入连接故障"""
        if random.random() < probability:
            # 随机标记一个连接为损坏
            for conn in self.pool.connections.values():
                if random.random() < 0.1:
                    conn.mark_broken()
                    self.fault_history.append({
                        "type": "connection_failure",
                        "connection_id": conn.id,
                        "timestamp": time.time()
                    })
                    return True
        return False
    
    async def inject_slow_connection(self, probability: float = 0.02):
        """注入慢连接"""
        if random.random() < probability:
            delay = random.uniform(0.1, 0.5)
            await asyncio.sleep(delay)
            self.fault_history.append({
                "type": "slow_connection",
                "delay_sec": delay,
                "timestamp": time.time()
            })
            return True
        return False


class ConnectionPoolTestRunner:
    """连接池测试运行器"""
    
    def __init__(self, 
                 target_duration_minutes: int = 8,
                 pool_size: int = 50,
                 concurrent_clients: int = 300):
        self.target_duration = target_duration_minutes * 60
        self.pool_size = pool_size
        self.concurrent_clients = concurrent_clients
        
        self.pool = ConnectionPool(pool_size)
        self.fault_injector = ConnectionPoolFaultInjector(self.pool)
        
        self.report = ConnectionPoolReport()
        self.stop_event = asyncio.Event()
        
    async def run(self) -> ConnectionPoolReport:
        """运行连接池测试"""
        logger.info("开始连接池耗尽测试")
        logger.info(f"连接池大小: {self.pool_size}")
        logger.info(f"并发客户端: {self.concurrent_clients}")

        self.report.start_time = time.time()
        self.report.pool_size = self.pool_size
        
        # 初始化连接池
        await self.pool.initialize()
        logger.info(f"连接池已初始化，创建 {self.pool_size} 个连接")

        # 启动工作负载
        tasks = [
            asyncio.create_task(self._pool_monitor()),
            asyncio.create_task(self._fault_injector_worker()),
            asyncio.create_task(self._recycle_worker()),
        ]

        # 启动并发客户端
        client_tasks = [
            asyncio.create_task(self._client_worker(i))
            for i in range(self.concurrent_clients)
        ]

        # 运行指定时间
        await asyncio.sleep(self.target_duration)
        self.stop_event.set()

        # 等待任务完成
        await asyncio.gather(*tasks, *client_tasks, return_exceptions=True)

        self.report.end_time = time.time()
        self.report.total_duration_sec = self.report.end_time - self.report.start_time

        # 生成最终报告
        await self._generate_report()

        return self.report

    async def _client_worker(self, client_id: int):
        """客户端工作器"""
        request_count = 0
        while not self.stop_event.is_set():
            try:
                operation = random.choice(["read", "write", "query", "update"])
                data = {
                    "client_id": client_id,
                    "request_id": request_count,
                    "payload": "x" * random.randint(100, 1000)
                }
                
                result = await self.pool.execute_with_connection(operation, data)
                
                if result.get("degraded"):
                    self.report.graceful_degradations += 1
                    self.report.degradation_events.append({
                        "timestamp": time.time(),
                        "client_id": client_id,
                        "type": "graceful_degradation"
                    })
                
                request_count += 1
                
            except Exception as e:
                self.report.errors.append(f"Client {client_id} error: {e}")
            
            # 高频率请求以耗尽连接池
            await asyncio.sleep(random.uniform(0.0001, 0.005))
    
    async def _pool_monitor(self):
        """连接池监控器"""
        max_wait_queue = 0
        wait_times = []
        
        while not self.stop_event.is_set():
            await asyncio.sleep(2)
            
            try:
                stats = self.pool.get_stats()
                
                self.report.pool_utilization_timeline.append({
                    "timestamp": time.time(),
                    "total_connections": stats["total_connections"],
                    "active_connections": stats["active_connections"],
                    "available_connections": stats["available_connections"],
                    "utilization_percent": stats["utilization_percent"]
                })
                
                self.report.wait_queue_timeline.append({
                    "timestamp": time.time(),
                    "wait_queue_size": stats["wait_queue_size"]
                })
                
                max_wait_queue = max(max_wait_queue, stats["wait_queue_size"])
                
                logger.info(
                    f"Pool Monitor - Total: {stats['total_connections']}, "
                    f"Active: {stats['active_connections']}, "
                    f"Available: {stats['available_connections']}, "
                    f"Utilization: {stats['utilization_percent']:.1f}%, "
                    f"Served: {stats['requests_served']}, "
                    f"Rejected: {stats['requests_rejected']}"
                )
                
            except Exception as e:
                logger.error(f"Pool monitor error: {e}")
        
        self.report.wait_queue_max_length = max_wait_queue
    
    async def _fault_injector_worker(self):
        """故障注入工作器"""
        while not self.stop_event.is_set():
            try:
                await self.fault_injector.inject_connection_failure()
                await self.fault_injector.inject_slow_connection()
                
            except Exception as e:
                logger.warning(f"Fault injector error: {e}")
            
            await asyncio.sleep(3)
    
    async def _recycle_worker(self):
        """连接回收工作器"""
        while not self.stop_event.is_set():
            await asyncio.sleep(10)
            
            try:
                recycled = await self.pool._recycle_connections()
                
                if recycled > 0:
                    self.report.connections_recycled += recycled
                    logger.info(f"Recycled {recycled} idle connections")
                    
            except Exception as e:
                logger.warning(f"Recycle worker error: {e}")
    
    async def _generate_report(self):
        """生成测试报告"""
        final_stats = self.pool.get_stats()
        
        self.report.pool_exhaustion_events = final_stats["exhaustion_count"]
        self.report.connections_recycled = final_stats["recycled_count"]
        self.report.connection_timeouts = final_stats["timeout_count"]
        self.report.requests_served = final_stats["requests_served"]
        self.report.requests_rejected = final_stats["requests_rejected"]
        
        # 计算平均等待时间
        if self.report.wait_queue_timeline:
            self.report.wait_queue_avg_wait_ms = sum(
                w["wait_queue_size"] for w in self.report.wait_queue_timeline
            ) / len(self.report.wait_queue_timeline) * 10  # 估算
        
        # 保存JSON报告
        report_path = Path("/workspace/agent_memory_system_v2/tests/round2/metrics/scenario5_connection_pool_metrics.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(self.report), f, indent=2, default=str)
        
        logger.info(f"测试报告已保存到: {report_path}")


async def main():
    """主函数"""
    runner = ConnectionPoolTestRunner(
        target_duration_minutes=8,
        pool_size=50,
        concurrent_clients=300
    )
    
    report = await runner.run()
    
    # 打印摘要
    print("\n" + "="*60)
    print("场景5: 连接池耗尽测试 - 完成")
    print("="*60)
    print(f"运行时间: {report.total_duration_sec/60:.2f} 分钟")
    print(f"连接池大小: {report.pool_size}")
    print(f"池耗尽事件: {report.pool_exhaustion_events}")
    print(f"等待队列最大长度: {report.wait_queue_max_length}")
    print(f"优雅降级次数: {report.graceful_degradations}")
    print(f"连接回收: {report.connections_recycled}")
    print(f"连接超时: {report.connection_timeouts}")
    print(f"请求服务: {report.requests_served:,}")
    print(f"请求拒绝: {report.requests_rejected}")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

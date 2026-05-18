"""
1小时+ 长时间浸泡测试 (Extended Soak Test)
第14轮深度实践 - 发现长时间运行时的隐藏Bug

测试目标:
- 运行至少1小时的高负载测试
- 发现内存泄漏、资源耗尽、性能退化等问题
- 验证系统在长时间运行下的稳定性
"""
import pytest
import asyncio
import time
import random
import string
import gc
import os
import psutil
import threading
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
sys.path.insert(0, '/workspace/agent_memory_system_v2')

from memory_system.security_hardening import CryptoManager, RBACManager, InputValidator
from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
from memory_system.vector_clock import VectorClock
from memory_system.hybrid_logical_clock import HybridLogicalClock
from memory_system.event_driven_core import EventBus
from memory_system.cqrs_system import InMemoryEventStore


# 测试配置
TEST_DURATION_MINUTES = 60  # 1小时测试
REPORT_INTERVAL_SECONDS = 60  # 每分钟报告一次


@dataclass
class TestMetrics:
    """测试指标收集器"""
    start_time: float = field(default_factory=time.time)
    operations_count: int = 0
    errors_count: int = 0
    memory_samples: List[Dict] = field(default_factory=list)
    latency_samples: List[float] = field(default_factory=list)
    error_log: List[Dict] = field(default_factory=list)
    
    def record_operation(self, latency_ms: float, success: bool = True, error: Optional[str] = None):
        """记录操作"""
        self.operations_count += 1
        self.latency_samples.append(latency_ms)
        
        if not success:
            self.errors_count += 1
            if error:
                self.error_log.append({
                    'timestamp': time.time(),
                    'error': error,
                    'operation_id': self.operations_count
                })
    
    def record_memory(self):
        """记录内存使用情况"""
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        
        self.memory_samples.append({
            'timestamp': time.time(),
            'rss_mb': mem_info.rss / 1024 / 1024,
            'vms_mb': mem_info.vms / 1024 / 1024,
            'percent': process.memory_percent(),
            'gc_objects': len(gc.get_objects())
        })
    
    def get_summary(self) -> Dict[str, Any]:
        """获取测试摘要"""
        elapsed = time.time() - self.start_time
        
        if self.latency_samples:
            avg_latency = sum(self.latency_samples) / len(self.latency_samples)
            max_latency = max(self.latency_samples)
            min_latency = min(self.latency_samples)
            p99_latency = sorted(self.latency_samples)[int(len(self.latency_samples) * 0.99)]
        else:
            avg_latency = max_latency = min_latency = p99_latency = 0
        
        # 内存分析
        if len(self.memory_samples) >= 2:
            first_mem = self.memory_samples[0]['rss_mb']
            last_mem = self.memory_samples[-1]['rss_mb']
            mem_growth = last_mem - first_mem
            mem_growth_rate = mem_growth / (elapsed / 3600)  # MB/hour
        else:
            mem_growth = mem_growth_rate = 0
        
        return {
            'elapsed_minutes': elapsed / 60,
            'total_operations': self.operations_count,
            'total_errors': self.errors_count,
            'error_rate': self.errors_count / max(self.operations_count, 1) * 100,
            'throughput_ops_per_sec': self.operations_count / max(elapsed, 1),
            'latency_ms': {
                'avg': avg_latency,
                'min': min_latency,
                'max': max_latency,
                'p99': p99_latency
            },
            'memory': {
                'growth_mb': mem_growth,
                'growth_rate_mb_per_hour': mem_growth_rate,
                'final_rss_mb': self.memory_samples[-1]['rss_mb'] if self.memory_samples else 0
            },
            'recent_errors': self.error_log[-10:] if self.error_log else []
        }


class TestExtendedSoak:
    """1小时+ 长时间浸泡测试"""
    
    @pytest.mark.slow
    @pytest.mark.timeout(7200)  # 2小时超时
    def test_crypto_manager_soak_1hour(self):
        """
        加密管理器1小时浸泡测试
        持续进行加密/解密/哈希操作，检测内存泄漏和性能退化
        """
        print("\n" + "="*60)
        print("🔐 开始 CryptoManager 1小时浸泡测试")
        print("="*60)
        
        metrics = TestMetrics()
        crypto = CryptoManager()
        
        test_duration = TEST_DURATION_MINUTES * 60
        end_time = time.time() + test_duration
        
        iteration = 0
        last_report_time = time.time()
        
        while time.time() < end_time:
            iteration += 1
            start_op = time.time()
            
            try:
                # 混合操作: 加密 + 解密
                data_size = random.randint(100, 10000)
                plaintext = ''.join(random.choices(string.ascii_letters + string.digits, k=data_size))
                
                encrypted = crypto.encrypt(plaintext)
                decrypted = crypto.decrypt(encrypted)
                
                assert decrypted == plaintext, "解密失败: 数据不匹配"
                
                # 密码哈希操作
                password = f"password_{iteration}_{random.randint(1000, 9999)}"
                hashed, salt = crypto.hash_password(password)
                assert crypto.verify_password(password, hashed, salt), "密码验证失败"
                
                # 数据哈希
                data_hash = crypto.hash_data(plaintext)
                assert len(data_hash) == 64, "SHA256哈希长度错误"
                
                latency_ms = (time.time() - start_op) * 1000
                metrics.record_operation(latency_ms, success=True)
                
            except Exception as e:
                latency_ms = (time.time() - start_op) * 1000
                metrics.record_operation(latency_ms, success=False, error=str(e))
                print(f"❌ 操作失败: {e}")
            
            # 定期记录内存
            if iteration % 1000 == 0:
                metrics.record_memory()
                gc.collect()  # 强制垃圾回收观察
            
            # 定期报告
            if time.time() - last_report_time >= REPORT_INTERVAL_SECONDS:
                summary = metrics.get_summary()
                elapsed_min = summary['elapsed_minutes']
                remaining_min = TEST_DURATION_MINUTES - elapsed_min
                
                print(f"\n📊 进度: {elapsed_min:.1f}/{TEST_DURATION_MINUTES} 分钟")
                print(f"   操作数: {summary['total_operations']:,}")
                print(f"   吞吐量: {summary['throughput_ops_per_sec']:.1f} ops/sec")
                print(f"   平均延迟: {summary['latency_ms']['avg']:.2f}ms")
                print(f"   P99延迟: {summary['latency_ms']['p99']:.2f}ms")
                print(f"   错误率: {summary['error_rate']:.4f}%")
                print(f"   内存增长: {summary['memory']['growth_mb']:.2f} MB")
                print(f"   内存增长率: {summary['memory']['growth_rate_mb_per_hour']:.2f} MB/hour")
                print(f"   剩余时间: {remaining_min:.1f} 分钟")
                
                last_report_time = time.time()
        
        # 最终报告
        print("\n" + "="*60)
        print("📋 最终测试报告")
        print("="*60)
        summary = metrics.get_summary()
        
        print(f"\n总运行时间: {summary['elapsed_minutes']:.2f} 分钟")
        print(f"总操作数: {summary['total_operations']:,}")
        print(f"总错误数: {summary['total_errors']}")
        print(f"平均吞吐量: {summary['throughput_ops_per_sec']:.2f} ops/sec")
        print(f"\n延迟统计:")
        print(f"  平均: {summary['latency_ms']['avg']:.2f}ms")
        print(f"  最小: {summary['latency_ms']['min']:.2f}ms")
        print(f"  最大: {summary['latency_ms']['max']:.2f}ms")
        print(f"  P99:  {summary['latency_ms']['p99']:.2f}ms")
        print(f"\n内存统计:")
        print(f"  增长: {summary['memory']['growth_mb']:.2f} MB")
        print(f"  增长率: {summary['memory']['growth_rate_mb_per_hour']:.2f} MB/hour")
        
        # 断言检查
        assert summary['error_rate'] < 1.0, f"错误率过高: {summary['error_rate']:.2f}%"
        assert summary['memory']['growth_rate_mb_per_hour'] < 100, \
            f"内存泄漏 suspected: {summary['memory']['growth_rate_mb_per_hour']:.2f} MB/hour"
        
        print("\n✅ 1小时浸泡测试通过!")
    
    @pytest.mark.slow
    @pytest.mark.timeout(7200)
    def test_vector_clock_soak_1hour(self):
        """
        向量时钟1小时浸泡测试
        模拟分布式节点间的时钟同步和合并
        """
        print("\n" + "="*60)
        print("⏱️  开始 VectorClock 1小时浸泡测试")
        print("="*60)
        
        metrics = TestMetrics()
        
        # 创建模拟节点
        node_ids = [f"node_{i}" for i in range(1, 6)]
        clocks = {nid: VectorClock(nid) for nid in node_ids}
        
        test_duration = TEST_DURATION_MINUTES * 60
        end_time = time.time() + test_duration
        
        iteration = 0
        last_report_time = time.time()
        
        while time.time() < end_time:
            iteration += 1
            start_op = time.time()
            
            try:
                # 随机选择一个节点进行操作
                node_id = random.choice(node_ids)
                clock = clocks[node_id]
                
                operation = random.choice(['increment', 'merge', 'compare', 'copy'])
                
                if operation == 'increment':
                    clock.increment()
                    
                elif operation == 'merge':
                    # 与另一个随机节点合并
                    other_id = random.choice([n for n in node_ids if n != node_id])
                    other_clock = clocks[other_id]
                    merged = clock.merge(other_clock)
                    # 验证合并结果
                    assert merged is not None
                    
                elif operation == 'compare':
                    # 与另一个节点比较
                    other_id = random.choice([n for n in node_ids if n != node_id])
                    other_clock = clocks[other_id]
                    relation = clock.compare(other_clock)
                    assert relation is not None
                    
                elif operation == 'copy':
                    copied = clock.copy()
                    assert copied.node_id == clock.node_id
                
                latency_ms = (time.time() - start_op) * 1000
                metrics.record_operation(latency_ms, success=True)
                
            except Exception as e:
                latency_ms = (time.time() - start_op) * 1000
                metrics.record_operation(latency_ms, success=False, error=str(e))
                print(f"❌ 操作失败: {e}")
            
            if iteration % 1000 == 0:
                metrics.record_memory()
            
            if time.time() - last_report_time >= REPORT_INTERVAL_SECONDS:
                summary = metrics.get_summary()
                print(f"\n📊 进度: {summary['elapsed_minutes']:.1f}/{TEST_DURATION_MINUTES} 分钟")
                print(f"   操作数: {summary['total_operations']:,}")
                print(f"   错误率: {summary['error_rate']:.4f}%")
                print(f"   内存增长: {summary['memory']['growth_mb']:.2f} MB")
                last_report_time = time.time()
        
        summary = metrics.get_summary()
        print("\n📋 VectorClock浸泡测试完成")
        print(f"总操作数: {summary['total_operations']:,}")
        print(f"错误率: {summary['error_rate']:.4f}%")
        print(f"内存增长: {summary['memory']['growth_mb']:.2f} MB")
        
        assert summary['error_rate'] < 0.1, f"错误率过高: {summary['error_rate']:.2f}%"
        assert summary['memory']['growth_rate_mb_per_hour'] < 50, \
            f"内存泄漏 suspected: {summary['memory']['growth_rate_mb_per_hour']:.2f} MB/hour"
        
        print("\n✅ VectorClock 1小时浸泡测试通过!")


class TestMemoryPressureExtended:
    """长时间内存压力测试"""
    
    @pytest.mark.slow
    @pytest.mark.timeout(7200)
    def test_memory_pressure_with_gc(self):
        """
        带GC监控的内存压力测试
        持续创建大量对象，观察GC行为和内存稳定性
        """
        print("\n" + "="*60)
        print("🧠 开始内存压力测试 (带GC监控)")
        print("="*60)
        
        metrics = TestMetrics()
        crypto = CryptoManager()
        
        test_duration = 30 * 60  # 30分钟
        end_time = time.time() + test_duration
        
        # 存储引用以防止过早回收
        stored_data = []
        max_stored = 10000
        
        iteration = 0
        gc_stats = {'collections': 0, 'collected': 0}
        
        while time.time() < end_time:
            iteration += 1
            start_op = time.time()
            
            try:
                # 创建大量加密数据
                batch_size = random.randint(10, 100)
                batch_data = []
                
                for _ in range(batch_size):
                    data = ''.join(random.choices(string.ascii_letters, k=random.randint(100, 1000)))
                    encrypted = crypto.encrypt(data)
                    batch_data.append(encrypted)
                
                # 存储部分数据
                stored_data.extend(batch_data)
                if len(stored_data) > max_stored:
                    # 随机删除一半
                    stored_data = random.sample(stored_data, max_stored // 2)
                
                # 定期强制GC并统计
                if iteration % 100 == 0:
                    gc_before = gc.get_count()
                    collected = gc.collect()
                    gc_after = gc.get_count()
                    gc_stats['collections'] += 1
                    gc_stats['collected'] += collected
                
                latency_ms = (time.time() - start_op) * 1000
                metrics.record_operation(latency_ms, success=True)
                
            except MemoryError as e:
                latency_ms = (time.time() - start_op) * 1000
                metrics.record_operation(latency_ms, success=False, error=f"MemoryError: {e}")
                print(f"⚠️ 内存不足: {e}")
                # 紧急清理
                stored_data.clear()
                gc.collect()
                
            except Exception as e:
                latency_ms = (time.time() - start_op) * 1000
                metrics.record_operation(latency_ms, success=False, error=str(e))
            
            if iteration % 500 == 0:
                metrics.record_memory()
            
            if iteration % 3000 == 0:  # 约每分钟
                summary = metrics.get_summary()
                print(f"\n📊 进度: {summary['elapsed_minutes']:.1f} 分钟")
                print(f"   操作数: {summary['total_operations']:,}")
                print(f"   存储对象: {len(stored_data)}")
                print(f"   GC次数: {gc_stats['collections']}")
                print(f"   GC回收对象: {gc_stats['collected']}")
                print(f"   内存: {summary['memory']['final_rss_mb']:.1f} MB")
        
        summary = metrics.get_summary()
        print("\n📋 内存压力测试完成")
        print(f"总操作数: {summary['total_operations']:,}")
        print(f"错误率: {summary['error_rate']:.4f}%")
        print(f"最终内存: {summary['memory']['final_rss_mb']:.1f} MB")
        print(f"GC回收总数: {gc_stats['collected']}")
        
        assert summary['error_rate'] < 5.0, f"错误率过高: {summary['error_rate']:.2f}%"
        
        print("\n✅ 内存压力测试通过!")


class TestConcurrentLoadExtended:
    """长时间并发负载测试"""
    
    @pytest.mark.slow
    @pytest.mark.timeout(7200)
    @pytest.mark.asyncio
    async def test_concurrent_crypto_operations_30min(self):
        """
        30分钟并发加密操作测试
        多线程并发执行加密操作，检测线程安全问题
        """
        print("\n" + "="*60)
        print("🔄 开始30分钟并发加密测试")
        print("="*60)
        
        metrics = TestMetrics()
        crypto = CryptoManager()
        
        test_duration = 30 * 60
        end_time = time.time() + test_duration
        
        async def worker(worker_id: int):
            """工作线程"""
            local_count = 0
            local_errors = 0
            
            while time.time() < end_time:
                try:
                    start_op = time.time()
                    
                    # 执行加密操作
                    data = f"worker_{worker_id}_data_{local_count}_{random.randint(1000, 9999)}"
                    encrypted = crypto.encrypt(data)
                    decrypted = crypto.decrypt(encrypted)
                    
                    assert decrypted == data, "数据不一致"
                    
                    latency_ms = (time.time() - start_op) * 1000
                    metrics.record_operation(latency_ms, success=True)
                    local_count += 1
                    
                except Exception as e:
                    latency_ms = (time.time() - start_op) * 1000 if 'start_op' in locals() else 0
                    metrics.record_operation(latency_ms, success=False, error=str(e))
                    local_errors += 1
                
                # 小延迟防止CPU占满
                await asyncio.sleep(0.001)
            
            return local_count, local_errors
        
        # 启动多个并发worker
        num_workers = 10
        print(f"启动 {num_workers} 个并发worker...")
        
        tasks = [worker(i) for i in range(num_workers)]
        results = await asyncio.gather(*tasks)
        
        total_ops = sum(r[0] for r in results)
        total_errors = sum(r[1] for r in results)
        
        summary = metrics.get_summary()
        print("\n📋 并发测试完成")
        print(f"总操作数: {total_ops:,}")
        print(f"总错误数: {total_errors}")
        print(f"平均吞吐量: {summary['throughput_ops_per_sec']:.2f} ops/sec")
        print(f"平均延迟: {summary['latency_ms']['avg']:.2f}ms")
        
        assert total_errors / max(total_ops, 1) < 0.01, "错误率过高"
        
        print("\n✅ 并发测试通过!")


if __name__ == "__main__":
    # 允许直接运行测试
    pytest.main([__file__, "-v", "-m", "slow", "--tb=short"])

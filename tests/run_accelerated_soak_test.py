#!/usr/bin/env python3
"""
加速版长时间浸泡测试 (5分钟模拟1小时效果)
第14轮深度实践 - 快速发现问题
"""
import sys
sys.path.insert(0, '/workspace/agent_memory_system_v2')

import time
import random
import string
import gc
import os
import json
from datetime import datetime
from memory_system.security_hardening import CryptoManager
from memory_system.vector_clock import VectorClock
from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
from memory_system.hybrid_logical_clock import HybridLogicalClock

# 测试配置 - 5分钟测试
TEST_DURATION_MINUTES = 5
REPORT_INTERVAL = 30  # 每30秒报告一次

class AcceleratedSoakTest:
    def __init__(self):
        self.start_time = time.time()
        self.results = {}
        
    def test_crypto_manager_soak(self):
        """CryptoManager 浸泡测试"""
        print("\n" + "="*70)
        print("🔐 CryptoManager 加速浸泡测试 (5分钟)")
        print("="*70)
        
        crypto = CryptoManager()
        iteration = 0
        errors = 0
        latencies = []
        memory_samples = []
        error_log = []
        
        test_duration = TEST_DURATION_MINUTES * 60
        end_time = time.time() + test_duration
        last_report = time.time()
        
        # 记录初始内存
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            memory_samples.append({'rss_mb': mem_info.rss / 1024 / 1024, 'timestamp': time.time()})
        except:
            pass
        
        while time.time() < end_time:
            iteration += 1
            start_op = time.time()
            
            try:
                # 高强度混合操作
                data_size = random.randint(50, 10000)
                plaintext = ''.join(random.choices(string.ascii_letters + string.digits, k=data_size))
                
                # 加密/解密
                encrypted = crypto.encrypt(plaintext)
                decrypted = crypto.decrypt(encrypted)
                assert decrypted == plaintext
                
                # 密码哈希 (PBKDF2 - CPU密集型)
                password = f"pwd_{iteration}_{random.randint(1000,9999)}"
                hashed, salt = crypto.hash_password(password)
                assert crypto.verify_password(password, hashed, salt)
                
                # 数据哈希
                crypto.hash_data(plaintext)
                
                latencies.append((time.time() - start_op) * 1000)
                
            except Exception as e:
                errors += 1
                if len(error_log) < 50:
                    error_log.append({'iteration': iteration, 'error': str(e)})
            
            # 定期记录内存
            if iteration % 3000 == 0:
                try:
                    import psutil
                    process = psutil.Process(os.getpid())
                    mem_info = process.memory_info()
                    memory_samples.append({'rss_mb': mem_info.rss / 1024 / 1024, 'timestamp': time.time()})
                    gc.collect()
                except:
                    pass
            
            # 定期报告
            if time.time() - last_report >= REPORT_INTERVAL:
                elapsed = time.time() - self.start_time
                remaining = test_duration - (time.time() - self.start_time)
                
                avg_lat = sum(latencies[-1000:]) / len(latencies[-1000:]) if latencies else 0
                current_mem = memory_samples[-1]['rss_mb'] if memory_samples else 0
                
                print(f"\n📊 [{elapsed/60:.1f}/{TEST_DURATION_MINUTES} min] 剩余: {remaining/60:.1f}min")
                print(f"   操作: {iteration:,} | 错误: {errors} | 延迟: {avg_lat:.2f}ms")
                print(f"   内存: {current_mem:.1f}MB")
                
                last_report = time.time()
        
        # 计算结果
        elapsed_total = time.time() - self.start_time
        
        if len(memory_samples) >= 2:
            mem_growth = memory_samples[-1]['rss_mb'] - memory_samples[0]['rss_mb']
            mem_rate = mem_growth / (elapsed_total / 3600)
        else:
            mem_growth = mem_rate = 0
        
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        max_latency = max(latencies) if latencies else 0
        
        result = {
            'test': 'CryptoManager Soak',
            'duration_minutes': elapsed_total / 60,
            'operations': iteration,
            'errors': errors,
            'error_rate': errors / max(iteration, 1) * 100,
            'throughput': iteration / elapsed_total,
            'latency_avg_ms': avg_latency,
            'latency_max_ms': max_latency,
            'memory_growth_mb': mem_growth,
            'memory_growth_rate_mb_per_hour': mem_rate,
            'error_log': error_log
        }
        
        print("\n📋 CryptoManager 测试结果:")
        print(f"   操作数: {result['operations']:,}")
        print(f"   错误率: {result['error_rate']:.4f}%")
        print(f"   平均延迟: {result['latency_avg_ms']:.2f}ms")
        print(f"   内存增长: {result['memory_growth_mb']:.2f}MB ({result['memory_growth_rate_mb_per_hour']:.1f}MB/h)")
        
        self.results['crypto'] = result
        return result['error_rate'] < 1.0 and result['memory_growth_rate_mb_per_hour'] < 100
    
    def test_vector_clock_soak(self):
        """VectorClock 浸泡测试"""
        print("\n" + "="*70)
        print("⏱️  VectorClock 加速浸泡测试 (5分钟)")
        print("="*70)
        
        node_ids = [f"node_{i}" for i in range(1, 11)]  # 10个节点
        clocks = {nid: VectorClock(nid) for nid in node_ids}
        
        iteration = 0
        errors = 0
        
        test_duration = TEST_DURATION_MINUTES * 60
        end_time = time.time() + test_duration
        last_report = time.time()
        
        while time.time() < end_time:
            iteration += 1
            
            try:
                node_id = random.choice(node_ids)
                clock = clocks[node_id]
                
                op = random.choice(['increment', 'merge', 'compare', 'copy', 'get_timestamp'])
                
                if op == 'increment':
                    clock.increment()
                elif op == 'merge':
                    other_id = random.choice([n for n in node_ids if n != node_id])
                    merged = clock.merge(clocks[other_id])
                    assert merged is not None
                elif op == 'compare':
                    other_id = random.choice([n for n in node_ids if n != node_id])
                    relation = clock.compare(clocks[other_id])
                    assert relation is not None
                elif op == 'copy':
                    copied = clock.copy()
                    assert copied.node_id == clock.node_id
                elif op == 'get_timestamp':
                    ts = clock.get_timestamp()
                    assert ts >= 0
                    
            except Exception as e:
                errors += 1
            
            if time.time() - last_report >= REPORT_INTERVAL:
                elapsed = time.time() - self.start_time
                remaining = test_duration - (time.time() - self.start_time)
                print(f"📊 [{elapsed/60:.1f}/{TEST_DURATION_MINUTES} min] 剩余: {remaining/60:.1f}min | 操作: {iteration:,} | 错误: {errors}")
                last_report = time.time()
        
        elapsed_total = time.time() - self.start_time
        
        result = {
            'test': 'VectorClock Soak',
            'duration_minutes': elapsed_total / 60,
            'operations': iteration,
            'errors': errors,
            'error_rate': errors / max(iteration, 1) * 100,
            'throughput': iteration / elapsed_total
        }
        
        print(f"\n📋 VectorClock 测试结果:")
        print(f"   操作数: {result['operations']:,}")
        print(f"   错误率: {result['error_rate']:.4f}%")
        
        self.results['vector_clock'] = result
        return result['error_rate'] < 0.1
    
    def test_rate_limiter_soak(self):
        """RateLimiter 浸泡测试"""
        print("\n" + "="*70)
        print("🚦 RateLimiter 加速浸泡测试 (5分钟)")
        print("="*70)
        
        config = RateLimitConfig(
            requests_per_minute=1000,
            burst_size=100,
            strategy=RateLimitStrategy.SLIDING_WINDOW
        )
        limiter = RateLimiter(config)
        
        iteration = 0
        errors = 0
        allowed_count = 0
        blocked_count = 0
        
        test_duration = TEST_DURATION_MINUTES * 60
        end_time = time.time() + test_duration
        last_report = time.time()
        
        while time.time() < end_time:
            iteration += 1
            
            try:
                user_id = f"user_{random.randint(1, 100)}"
                endpoint = f"api_{random.randint(1, 10)}"
                
                is_allowed = limiter.is_allowed(user_id, endpoint)
                if is_allowed:
                    allowed_count += 1
                else:
                    blocked_count += 1
                    
            except Exception as e:
                errors += 1
            
            if time.time() - last_report >= REPORT_INTERVAL:
                elapsed = time.time() - self.start_time
                remaining = test_duration - (time.time() - self.start_time)
                block_rate = blocked_count / max(allowed_count + blocked_count, 1) * 100
                print(f"📊 [{elapsed/60:.1f}/{TEST_DURATION_MINUTES} min] 剩余: {remaining/60:.1f}min | 操作: {iteration:,} | 限制率: {block_rate:.1f}%")
                last_report = time.time()
        
        elapsed_total = time.time() - self.start_time
        
        result = {
            'test': 'RateLimiter Soak',
            'duration_minutes': elapsed_total / 60,
            'operations': iteration,
            'errors': errors,
            'allowed': allowed_count,
            'blocked': blocked_count,
            'block_rate': blocked_count / max(allowed_count + blocked_count, 1) * 100
        }
        
        print(f"\n📋 RateLimiter 测试结果:")
        print(f"   操作数: {result['operations']:,}")
        print(f"   允许: {result['allowed']:,} | 限制: {result['blocked']:,}")
        print(f"   限制率: {result['block_rate']:.1f}%")
        
        self.results['rate_limiter'] = result
        return result['error_rate'] < 0.1 and 5 < result['block_rate'] < 50
    
    def run_all_tests(self):
        """运行所有测试"""
        print("\n" + "🚀"*35)
        print("  第14轮深度实践 - 加速版长时间浸泡测试")
        print("🚀"*35)
        print(f"\n开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"测试时长: {TEST_DURATION_MINUTES} 分钟 (模拟1小时压力)")
        
        all_passed = True
        
        # 测试1: CryptoManager
        if not self.test_crypto_manager_soak():
            all_passed = False
            print("❌ CryptoManager 测试失败")
        else:
            print("✅ CryptoManager 测试通过")
        
        # 测试2: VectorClock
        if not self.test_vector_clock_soak():
            all_passed = False
            print("❌ VectorClock 测试失败")
        else:
            print("✅ VectorClock 测试通过")
        
        # 测试3: RateLimiter
        if not self.test_rate_limiter_soak():
            all_passed = False
            print("❌ RateLimiter 测试失败")
        else:
            print("✅ RateLimiter 测试通过")
        
        # 保存结果
        result_file = f'/workspace/accelerated_soak_result_{int(time.time())}.json'
        with open(result_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'results': self.results
            }, f, indent=2)
        
        print("\n" + "="*70)
        print("📋 最终汇总报告")
        print("="*70)
        total_ops = sum(r.get('operations', 0) for r in self.results.values())
        total_errors = sum(r.get('errors', 0) for r in self.results.values())
        
        print(f"总操作数: {total_ops:,}")
        print(f"总错误数: {total_errors}")
        print(f"整体错误率: {total_errors/max(total_ops,1)*100:.4f}%")
        print(f"结果保存: {result_file}")
        
        if all_passed:
            print("\n✅ 所有测试通过!")
        else:
            print("\n❌ 部分测试失败!")
        
        print("="*70)
        return all_passed


if __name__ == "__main__":
    test = AcceleratedSoakTest()
    success = test.run_all_tests()
    sys.exit(0 if success else 1)

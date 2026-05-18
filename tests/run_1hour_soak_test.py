#!/usr/bin/env python3
"""
1小时长时间浸泡测试 - 独立运行脚本
第14轮深度实践

使用方法:
    python run_1hour_soak_test.py
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

# 测试配置
TEST_DURATION_MINUTES = 60
REPORT_INTERVAL = 60  # 每60秒报告一次

class SoakTestRunner:
    def __init__(self):
        self.start_time = time.time()
        self.iteration = 0
        self.errors = 0
        self.latencies = []
        self.memory_samples = []
        self.error_log = []
        
    def record_memory(self):
        """记录内存使用情况"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            self.memory_samples.append({
                'timestamp': time.time(),
                'rss_mb': mem_info.rss / 1024 / 1024,
                'vms_mb': mem_info.vms / 1024 / 1024
            })
        except:
            pass
    
    def get_summary(self):
        """获取测试摘要"""
        elapsed = time.time() - self.start_time
        
        if self.latencies:
            avg_latency = sum(self.latencies) / len(self.latencies)
            max_latency = max(self.latencies)
            p99 = sorted(self.latencies)[int(len(self.latencies) * 0.99)] if len(self.latencies) > 100 else max_latency
        else:
            avg_latency = max_latency = p99 = 0
        
        if len(self.memory_samples) >= 2:
            mem_growth = self.memory_samples[-1]['rss_mb'] - self.memory_samples[0]['rss_mb']
            mem_rate = mem_growth / (elapsed / 3600)
        else:
            mem_growth = mem_rate = 0
        
        return {
            'elapsed_minutes': elapsed / 60,
            'operations': self.iteration,
            'errors': self.errors,
            'error_rate': self.errors / max(self.iteration, 1) * 100,
            'throughput': self.iteration / max(elapsed, 1),
            'latency_avg_ms': avg_latency,
            'latency_max_ms': max_latency,
            'latency_p99_ms': p99,
            'memory_growth_mb': mem_growth,
            'memory_growth_rate_mb_per_hour': mem_rate
        }
    
    def run_crypto_soak(self):
        """运行CryptoManager浸泡测试"""
        print("\n" + "="*70)
        print("🔐 CryptoManager 1小时浸泡测试")
        print("="*70)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"测试时长: {TEST_DURATION_MINUTES} 分钟")
        print("="*70)
        
        crypto = CryptoManager()
        test_duration = TEST_DURATION_MINUTES * 60
        end_time = time.time() + test_duration
        last_report = time.time()
        
        # 初始内存记录
        self.record_memory()
        
        while time.time() < end_time:
            self.iteration += 1
            start_op = time.time()
            
            try:
                # 混合操作
                data_size = random.randint(50, 5000)
                plaintext = ''.join(random.choices(string.ascii_letters + string.digits, k=data_size))
                
                # 加密/解密
                encrypted = crypto.encrypt(plaintext)
                decrypted = crypto.decrypt(encrypted)
                assert decrypted == plaintext, "解密失败"
                
                # 密码哈希
                password = f"pwd_{self.iteration}_{random.randint(1000,9999)}"
                hashed, salt = crypto.hash_password(password)
                assert crypto.verify_password(password, hashed, salt), "密码验证失败"
                
                # 数据哈希
                crypto.hash_data(plaintext)
                
                latency = (time.time() - start_op) * 1000
                self.latencies.append(latency)
                
            except Exception as e:
                self.errors += 1
                if len(self.error_log) < 100:  # 只保留前100个错误
                    self.error_log.append({
                        'iteration': self.iteration,
                        'error': str(e),
                        'timestamp': time.time()
                    })
            
            # 定期记录内存
            if self.iteration % 2000 == 0:
                self.record_memory()
                gc.collect()
            
            # 定期报告
            if time.time() - last_report >= REPORT_INTERVAL:
                summary = self.get_summary()
                remaining = TEST_DURATION_MINUTES - summary['elapsed_minutes']
                
                print(f"\n📊 [{summary['elapsed_minutes']:.1f}/{TEST_DURATION_MINUTES} min] "
                      f"剩余: {remaining:.1f}min")
                print(f"   操作: {summary['operations']:,} | "
                      f"吞吐量: {summary['throughput']:.0f} ops/s | "
                      f"错误: {summary['errors']}")
                print(f"   延迟: avg={summary['latency_avg_ms']:.2f}ms "
                      f"p99={summary['latency_p99_ms']:.2f}ms "
                      f"max={summary['latency_max_ms']:.2f}ms")
                print(f"   内存: {summary['memory_growth_mb']:.1f}MB "
                      f"({summary['memory_growth_rate_mb_per_hour']:.1f}MB/h)")
                
                last_report = time.time()
        
        # 最终报告
        print("\n" + "="*70)
        print("📋 最终测试报告")
        print("="*70)
        summary = self.get_summary()
        
        print(f"\n总运行时间: {summary['elapsed_minutes']:.2f} 分钟")
        print(f"总操作数: {summary['operations']:,}")
        print(f"总错误数: {summary['errors']}")
        print(f"错误率: {summary['error_rate']:.4f}%")
        print(f"平均吞吐量: {summary['throughput']:.2f} ops/sec")
        print(f"\n延迟统计:")
        print(f"  平均: {summary['latency_avg_ms']:.2f}ms")
        print(f"  P99:  {summary['latency_p99_ms']:.2f}ms")
        print(f"  最大: {summary['latency_max_ms']:.2f}ms")
        print(f"\n内存统计:")
        print(f"  增长: {summary['memory_growth_mb']:.2f} MB")
        print(f"  增长率: {summary['memory_growth_rate_mb_per_hour']:.2f} MB/hour")
        
        # 保存详细结果
        result_file = f'/workspace/soak_test_result_{int(time.time())}.json'
        with open(result_file, 'w') as f:
            json.dump({
                'summary': summary,
                'error_log': self.error_log,
                'memory_samples': self.memory_samples
            }, f, indent=2)
        print(f"\n详细结果已保存: {result_file}")
        
        # 验证
        success = True
        if summary['error_rate'] > 1.0:
            print(f"\n❌ 错误率过高: {summary['error_rate']:.2f}%")
            success = False
        else:
            print(f"\n✅ 错误率正常: {summary['error_rate']:.4f}%")
        
        if summary['memory_growth_rate_mb_per_hour'] > 100:
            print(f"⚠️  内存泄漏 suspected: {summary['memory_growth_rate_mb_per_hour']:.2f} MB/hour")
        else:
            print(f"✅ 内存增长正常: {summary['memory_growth_rate_mb_per_hour']:.2f} MB/hour")
        
        print("="*70)
        return success


if __name__ == "__main__":
    print("\n" + "🚀"*35)
    print("  第14轮深度实践 - 1小时长时间浸泡测试")
    print("🚀"*35)
    
    runner = SoakTestRunner()
    success = runner.run_crypto_soak()
    
    if success:
        print("\n✅ 测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 测试失败!")
        sys.exit(1)

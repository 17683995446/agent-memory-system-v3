#!/usr/bin/env python3
"""
Agent Memory System V3.0 - 第2轮实践：长时间压力与资源耗尽测试
运行所有5个测试场景
"""

import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# 场景配置
SCENARIOS = [
    {
        "name": "场景1: 48小时Soak测试（压缩版）",
        "script": "scenario1_48h_soak_test.py",
        "description": "模拟48小时连续运行，监控内存泄漏、句柄泄漏、性能衰减",
        "duration_minutes": 10
    },
    {
        "name": "场景2: 内存压力测试",
        "script": "scenario2_memory_pressure.py",
        "description": "限制内存到256MB，验证OOM处理和恢复",
        "duration_minutes": 8
    },
    {
        "name": "场景3: 磁盘压力测试",
        "script": "scenario3_disk_pressure.py",
        "description": "模拟磁盘满（100%使用率），验证写入失败处理",
        "duration_minutes": 8
    },
    {
        "name": "场景4: CPU过载测试",
        "script": "scenario4_cpu_overload.py",
        "description": "100% CPU使用率，验证调度延迟和优先级调整",
        "duration_minutes": 8
    },
    {
        "name": "场景5: 连接池耗尽测试",
        "script": "scenario5_connection_pool.py",
        "description": "耗尽所有连接，验证优雅降级和连接回收",
        "duration_minutes": 8
    }
]


def run_scenario(scenario: Dict, scenario_dir: Path) -> Dict:
    """运行单个场景"""
    print(f"\n{'='*60}")
    print(f"开始: {scenario['name']}")
    print(f"描述: {scenario['description']}")
    print(f"预计耗时: {scenario['duration_minutes']} 分钟")
    print(f"{'='*60}\n")
    
    script_path = scenario_dir / scenario['script']
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(scenario_dir),
            capture_output=True,
            text=True,
            timeout=scenario['duration_minutes'] * 60 + 120  # 额外2分钟缓冲
        )
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        success = result.returncode == 0
        
        print(f"\n场景完成: {scenario['name']}")
        print(f"实际耗时: {actual_duration/60:.2f} 分钟")
        print(f"结果: {'成功' if success else '失败'}")
        
        if not success:
            print(f"错误输出: {result.stderr[:500]}")
        
        return {
            "scenario_name": scenario['name'],
            "script": scenario['script'],
            "success": success,
            "duration_sec": actual_duration,
            "return_code": result.returncode,
            "stdout": result.stdout[-2000:] if result.stdout else "",  # 最后2000字符
            "stderr": result.stderr[-1000:] if result.stderr else ""
        }
        
    except subprocess.TimeoutExpired:
        print(f"\n场景超时: {scenario['name']}")
        return {
            "scenario_name": scenario['name'],
            "script": scenario['script'],
            "success": False,
            "duration_sec": scenario['duration_minutes'] * 60 + 120,
            "error": "Timeout"
        }
    except Exception as e:
        print(f"\n场景异常: {scenario['name']} - {e}")
        return {
            "scenario_name": scenario['name'],
            "script": scenario['script'],
            "success": False,
            "error": str(e)
        }


def generate_defects_report(scenario_results: List[Dict], metrics_dir: Path) -> Dict:
    """生成缺陷报告"""
    defects = []
    
    # 读取各场景的指标文件
    metrics_files = {
        "scenario1_48h_soak_test.py": "scenario1_48h_soak_metrics.json",
        "scenario2_memory_pressure.py": "scenario2_memory_pressure_metrics.json",
        "scenario3_disk_pressure.py": "scenario3_disk_pressure_metrics.json",
        "scenario4_cpu_overload.py": "scenario4_cpu_overload_metrics.json",
        "scenario5_connection_pool.py": "scenario5_connection_pool_metrics.json"
    }
    
    for result in scenario_results:
        script_name = result['script']
        metrics_file = metrics_files.get(script_name)
        
        if metrics_file:
            metrics_path = metrics_dir / metrics_file
            if metrics_path.exists():
                try:
                    with open(metrics_path, 'r') as f:
                        metrics = json.load(f)
                    
                    # 分析各场景的缺陷
                    if script_name == "scenario1_48h_soak_test.py":
                        if metrics.get('memory_leak_detected'):
                            defects.append({
                                "id": f"DEFECT-R2-001",
                                "severity": "P1",
                                "category": "memory_leak",
                                "scenario": result['scenario_name'],
                                "description": "48小时Soak测试检测到内存泄漏",
                                "status": "detected"
                            })
                        if metrics.get('handle_leak_detected'):
                            defects.append({
                                "id": f"DEFECT-R2-002",
                                "severity": "P1",
                                "category": "handle_leak",
                                "scenario": result['scenario_name'],
                                "description": "48小时Soak测试检测到句柄泄漏",
                                "status": "detected"
                            })
                        if metrics.get('performance_degradation'):
                            defects.append({
                                "id": f"DEFECT-R2-003",
                                "severity": "P2",
                                "category": "performance",
                                "scenario": result['scenario_name'],
                                "description": "长时间运行后性能衰减超过30%",
                                "status": "detected"
                            })
                    
                    elif script_name == "scenario2_memory_pressure.py":
                        oom_events = metrics.get('oom_events', 0)
                        if oom_events > 50:
                            defects.append({
                                "id": f"DEFECT-R2-004",
                                "severity": "P1",
                                "category": "oom_handling",
                                "scenario": result['scenario_name'],
                                "description": f"OOM事件过多({oom_events}次)，内存管理需要优化",
                                "status": "detected"
                            })
                        recovery_count = metrics.get('recovery_success_count', 0)
                        if recovery_count < oom_events * 0.8:
                            defects.append({
                                "id": f"DEFECT-R2-005",
                                "severity": "P0",
                                "category": "oom_recovery",
                                "scenario": result['scenario_name'],
                                "description": f"OOM恢复成功率过低({recovery_count}/{oom_events})",
                                "status": "detected"
                            })
                    
                    elif script_name == "scenario3_disk_pressure.py":
                        write_failures = metrics.get('write_failures', 0)
                        if write_failures > 100:
                            defects.append({
                                "id": f"DEFECT-R2-006",
                                "severity": "P2",
                                "category": "disk_handling",
                                "scenario": result['scenario_name'],
                                "description": f"磁盘写入失败次数过多({write_failures})",
                                "status": "detected"
                            })
                    
                    elif script_name == "scenario4_cpu_overload.py":
                        max_delay = metrics.get('max_scheduling_delay_ms', 0)
                        if max_delay > 1000:
                            defects.append({
                                "id": f"DEFECT-R2-007",
                                "severity": "P2",
                                "category": "scheduling",
                                "scenario": result['scenario_name'],
                                "description": f"CPU过载时调度延迟过高({max_delay:.2f}ms)",
                                "status": "detected"
                            })
                    
                    elif script_name == "scenario5_connection_pool.py":
                        rejected = metrics.get('requests_rejected', 0)
                        served = metrics.get('requests_served', 0)
                        if rejected > served * 0.1:  # 拒绝率超过10%
                            defects.append({
                                "id": f"DEFECT-R2-008",
                                "severity": "P1",
                                "category": "connection_pool",
                                "scenario": result['scenario_name'],
                                "description": f"连接池耗尽时请求拒绝率过高({rejected}/{served+rejected})",
                                "status": "detected"
                            })
                
                except Exception as e:
                    print(f"读取指标文件失败 {metrics_path}: {e}")
    
    return {
        "test_round": "round2",
        "test_type": "长时间压力与资源耗尽测试",
        "execution_time": datetime.now().isoformat(),
        "total_defects": len(defects),
        "defects": defects,
        "summary": {
            "p0_count": sum(1 for d in defects if d['severity'] == 'P0'),
            "p1_count": sum(1 for d in defects if d['severity'] == 'P1'),
            "p2_count": sum(1 for d in defects if d['severity'] == 'P2'),
            "p3_count": sum(1 for d in defects if d['severity'] == 'P3')
        }
    }


def main():
    """主函数"""
    print("="*60)
    print("Agent Memory System V3.0")
    print("第2轮实践：长时间压力与资源耗尽测试")
    print("="*60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"预计总耗时: {sum(s['duration_minutes'] for s in SCENARIOS)} 分钟")
    print("="*60)
    
    scenario_dir = Path("/workspace/agent_memory_system_v2/tests/round2")
    metrics_dir = scenario_dir / "metrics"
    
    # 确保目录存在
    metrics_dir.mkdir(parents=True, exist_ok=True)
    (scenario_dir / "logs").mkdir(parents=True, exist_ok=True)
    
    # 运行所有场景
    results = []
    total_start = time.time()
    
    for scenario in SCENARIOS:
        result = run_scenario(scenario, scenario_dir)
        results.append(result)
    
    total_duration = time.time() - total_start
    
    # 生成缺陷报告
    defects_report = generate_defects_report(results, metrics_dir)
    
    # 保存缺陷报告
    defects_path = scenario_dir / "defects.json"
    with open(defects_path, 'w', encoding='utf-8') as f:
        json.dump(defects_report, f, indent=2, ensure_ascii=False)
    
    # 生成执行摘要
    summary = {
        "test_round": "round2",
        "test_type": "长时间压力与资源耗尽测试",
        "execution_time": datetime.now().isoformat(),
        "total_duration_minutes": total_duration / 60,
        "scenarios_executed": len(results),
        "scenarios_passed": sum(1 for r in results if r['success']),
        "scenarios_failed": sum(1 for r in results if not r['success']),
        "defects_found": defects_report['total_defects'],
        "scenario_results": [
            {
                "name": r['scenario_name'],
                "success": r['success'],
                "duration_minutes": r.get('duration_sec', 0) / 60
            }
            for r in results
        ]
    }
    
    summary_path = scenario_dir / "execution_summary.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    # 打印最终摘要
    print("\n" + "="*60)
    print("测试执行完成 - 最终摘要")
    print("="*60)
    print(f"总耗时: {total_duration/60:.2f} 分钟")
    print(f"场景执行: {summary['scenarios_executed']}")
    print(f"通过: {summary['scenarios_passed']}")
    print(f"失败: {summary['scenarios_failed']}")
    print(f"发现缺陷: {summary['defects_found']}")
    print(f"\n缺陷分布:")
    print(f"  P0: {defects_report['summary']['p0_count']}")
    print(f"  P1: {defects_report['summary']['p1_count']}")
    print(f"  P2: {defects_report['summary']['p2_count']}")
    print(f"  P3: {defects_report['summary']['p3_count']}")
    print("="*60)
    print(f"\n输出文件:")
    print(f"  缺陷报告: {defects_path}")
    print(f"  执行摘要: {summary_path}")
    print(f"  日志目录: {scenario_dir / 'logs'}")
    print(f"  指标目录: {metrics_dir}")
    print("="*60)


if __name__ == "__main__":
    main()

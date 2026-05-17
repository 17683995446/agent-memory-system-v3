#!/usr/bin/env python3
"""
Agent Memory System V3.0 - 第1轮实践测试运行器
执行所有极端并发与故障注入测试场景
"""

import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# 测试场景配置
SCENARIOS = [
    {
        "name": "场景1: 万级并发写入风暴",
        "script": "scenario1_write_storm_10k.py",
        "timeout": 600  # 10分钟
    },
    {
        "name": "场景2: 读写混合高并发",
        "script": "scenario2_read_write_mixed.py",
        "timeout": 600
    },
    {
        "name": "场景3: Raft集群脑裂恢复",
        "script": "scenario3_raft_split_recovery.py",
        "timeout": 300
    },
    {
        "name": "场景4: 级联故障注入",
        "script": "scenario4_cascading_failure.py",
        "timeout": 300
    },
    {
        "name": "场景5: 分布式事务压力",
        "script": "scenario5_distributed_transaction.py",
        "timeout": 300
    }
]


def run_scenario(scenario: Dict, round_dir: Path) -> Dict:
    """运行单个测试场景"""
    print(f"\n{'='*80}")
    print(f"开始执行: {scenario['name']}")
    print(f"{'='*80}")
    
    script_path = round_dir / scenario['script']
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(round_dir),
            capture_output=True,
            text=True,
            timeout=scenario['timeout']
        )
        
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✓ {scenario['name']} 完成 (耗时: {elapsed:.1f}秒)")
            return {
                "name": scenario['name'],
                "script": scenario['script'],
                "status": "success",
                "duration_sec": elapsed,
                "returncode": result.returncode
            }
        else:
            print(f"✗ {scenario['name']} 失败 (返回码: {result.returncode})")
            print(f"错误输出: {result.stderr[:500]}")
            return {
                "name": scenario['name'],
                "script": scenario['script'],
                "status": "failed",
                "duration_sec": elapsed,
                "returncode": result.returncode,
                "error": result.stderr[:1000]
            }
            
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start_time
        print(f"✗ {scenario['name']} 超时 (>{scenario['timeout']}秒)")
        return {
            "name": scenario['name'],
            "script": scenario['script'],
            "status": "timeout",
            "duration_sec": elapsed,
            "error": f"Timeout after {scenario['timeout']} seconds"
        }
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"✗ {scenario['name']} 异常: {e}")
        return {
            "name": scenario['name'],
            "script": scenario['script'],
            "status": "error",
            "duration_sec": elapsed,
            "error": str(e)
        }


def collect_defects(round_dir: Path) -> List[Dict]:
    """收集所有场景的缺陷"""
    all_defects = []
    metrics_dir = round_dir / "metrics"
    
    if not metrics_dir.exists():
        return all_defects
    
    for metrics_file in metrics_dir.glob("*_metrics.json"):
        try:
            with open(metrics_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            scenario_name = data.get('scenario_name', 'Unknown')
            defects = data.get('defects_found', [])
            
            for defect in defects:
                defect['scenario'] = scenario_name
                defect['source_file'] = metrics_file.name
                all_defects.append(defect)
                
        except Exception as e:
            print(f"警告: 无法解析 {metrics_file}: {e}")
    
    return all_defects


def generate_defects_report(round_dir: Path, defects: List[Dict]):
    """生成缺陷报告"""
    defects_path = round_dir / "defects.json"
    
    # 按严重级别分类
    defects_by_severity = {
        "P0": [],
        "P1": [],
        "P2": [],
        "P3": []
    }
    
    for defect in defects:
        severity = defect.get('severity', 'P3')
        if severity in defects_by_severity:
            defects_by_severity[severity].append(defect)
        else:
            defects_by_severity['P3'].append(defect)
    
    report = {
        "report_info": {
            "generated_at": datetime.now().isoformat(),
            "round": "round1",
            "total_defects": len(defects),
            "total_scenarios": len(SCENARIOS)
        },
        "summary": {
            "p0_count": len(defects_by_severity['P0']),
            "p1_count": len(defects_by_severity['P1']),
            "p2_count": len(defects_by_severity['P2']),
            "p3_count": len(defects_by_severity['P3'])
        },
        "defects_by_severity": defects_by_severity,
        "all_defects": defects
    }
    
    with open(defects_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    return report


def print_summary(results: List[Dict], defects_report: Dict):
    """打印测试摘要"""
    print("\n" + "="*80)
    print("Agent Memory System V3.0 - 第1轮实践测试完成")
    print("="*80)
    
    print("\n场景执行结果:")
    print("-"*80)
    for result in results:
        status_icon = "✓" if result['status'] == 'success' else "✗"
        print(f"{status_icon} {result['name']}: {result['status'].upper()} ({result['duration_sec']:.1f}s)")
    
    print("\n缺陷统计:")
    print("-"*80)
    summary = defects_report['summary']
    print(f"  P0 (严重): {summary['p0_count']} 个")
    print(f"  P1 (高):   {summary['p1_count']} 个")
    print(f"  P2 (中):   {summary['p2_count']} 个")
    print(f"  P3 (低):   {summary['p3_count']} 个")
    print(f"  总计:      {defects_report['report_info']['total_defects']} 个")
    
    print("\n详细缺陷列表:")
    print("-"*80)
    
    for severity in ['P0', 'P1', 'P2', 'P3']:
        defects = defects_report['defects_by_severity'][severity]
        if defects:
            print(f"\n[{severity}] 级别缺陷:")
            for defect in defects:
                print(f"  - {defect.get('id', 'N/A')}: {defect.get('description', 'No description')}")
                print(f"    场景: {defect.get('scenario', 'Unknown')}")
    
    print("\n" + "="*80)
    print(f"缺陷报告已保存到: {defects_report.get('report_path', 'N/A')}")
    print("="*80)


def main():
    """主函数"""
    round_dir = Path("/workspace/agent_memory_system_v2/tests/round1")
    
    print("="*80)
    print("Agent Memory System V3.0 - 第1轮实践")
    print("极端并发与故障注入测试")
    print("="*80)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试目录: {round_dir}")
    print("="*80)
    
    # 确保目录存在
    (round_dir / "logs").mkdir(parents=True, exist_ok=True)
    (round_dir / "metrics").mkdir(parents=True, exist_ok=True)
    
    # 运行所有场景
    results = []
    for scenario in SCENARIOS:
        result = run_scenario(scenario, round_dir)
        results.append(result)
    
    # 收集缺陷
    print("\n正在收集缺陷报告...")
    defects = collect_defects(round_dir)
    
    # 生成缺陷报告
    defects_report = generate_defects_report(round_dir, defects)
    defects_report['report_path'] = str(round_dir / "defects.json")
    
    # 打印摘要
    print_summary(results, defects_report)
    
    # 返回整体状态
    success_count = sum(1 for r in results if r['status'] == 'success')
    p0_count = defects_report['summary']['p0_count']
    
    print(f"\n总结: {success_count}/{len(SCENARIOS)} 个场景成功, {p0_count} 个P0缺陷")
    
    if p0_count > 0:
        print("警告: 发现P0级别缺陷, 建议立即修复!")
        return 1
    elif success_count < len(SCENARIOS):
        print("警告: 部分场景执行失败!")
        return 1
    else:
        print("所有场景执行成功!")
        return 0


if __name__ == "__main__":
    sys.exit(main())

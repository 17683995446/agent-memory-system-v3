#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 实践场景测试主运行器
Main Runner for All Practice Scenarios

执行所有5个高难度真实实践场景测试:
1. 分布式一致性压力测试
2. 高并发竞争条件测试
3. 故障注入与恢复测试
4. 内存和资源泄漏测试
5. 性能瓶颈和边界测试

每个场景都设计用于暴露系统的深层缺陷。
"""

import asyncio
import time
import json
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor
import subprocess

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ScenarioResult:
    """场景测试结果"""
    scenario_id: str
    scenario_name: str
    success: bool
    start_time: str
    end_time: str
    duration_seconds: float
    defects_found: int
    critical_defects: int
    high_defects: int
    medium_defects: int
    low_defects: int
    error_message: Optional[str] = None
    report_path: Optional[str] = None


class ScenarioRunner:
    """场景测试运行器"""
    
    def __init__(self):
        self.results: List[ScenarioResult] = []
        self.scenarios = [
            {
                'id': 'scenario1_distributed_consistency',
                'name': '分布式一致性压力测试',
                'module': 'scenario1_distributed_consistency',
                'description': '模拟5节点Raft集群，网络分区下的并发写入，验证Leader切换后数据一致性'
            },
            {
                'id': 'scenario2_concurrent_race',
                'name': '高并发竞争条件测试',
                'module': 'scenario2_concurrent_race',
                'description': '1000个并发客户端同时读写，测试分布式事务隔离性，验证锁正确性'
            },
            {
                'id': 'scenario3_fault_injection',
                'name': '故障注入与恢复测试',
                'module': 'scenario3_fault_injection',
                'description': '随机杀死节点，模拟磁盘损坏和网络延迟，验证故障转移和数据恢复'
            },
            {
                'id': 'scenario4_resource_leak',
                'name': '内存和资源泄漏测试',
                'module': 'scenario4_resource_leak',
                'description': '长时间运行模拟24小时，频繁创建/删除记忆，监控资源泄漏'
            },
            {
                'id': 'scenario5_performance_boundary',
                'name': '性能瓶颈和边界测试',
                'module': 'scenario5_performance_boundary',
                'description': '插入100万条数据，测试索引构建和查询性能，验证分片再平衡'
            }
        ]
        
        # 报告输出目录
        self.reports_dir = "/workspace/agent_memory_system_v2/tests/reports"
        os.makedirs(self.reports_dir, exist_ok=True)
    
    async def run_all_scenarios(self, parallel: bool = False) -> Dict[str, Any]:
        """
        运行所有场景测试
        
        Args:
            parallel: 是否并行执行（默认串行）
        """
        logger.info("=" * 80)
        logger.info("Agent Memory System V3.0 - 实践场景测试套件")
        logger.info("=" * 80)
        logger.info(f"共 {len(self.scenarios)} 个测试场景")
        logger.info(f"开始时间: {datetime.now().isoformat()}")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        if parallel:
            # 并行执行
            tasks = [self.run_scenario(s) for s in self.scenarios]
            self.results = await asyncio.gather(*tasks, return_exceptions=True)
            self.results = [
                r if isinstance(r, ScenarioResult) else self._create_error_result(s, str(r))
                for r, s in zip(self.results, self.scenarios)
            ]
        else:
            # 串行执行
            for scenario in self.scenarios:
                try:
                    result = await self.run_scenario(scenario)
                    self.results.append(result)
                except Exception as e:
                    logger.error(f"场景 {scenario['id']} 执行失败: {e}")
                    self.results.append(self._create_error_result(scenario, str(e)))
        
        end_time = time.time()
        
        # 生成汇总报告
        summary = self._generate_summary(start_time, end_time)
        
        return summary
    
    async def run_scenario(self, scenario: Dict[str, Any]) -> ScenarioResult:
        """运行单个场景"""
        scenario_id = scenario['id']
        scenario_name = scenario['name']
        
        logger.info("\n" + "=" * 80)
        logger.info(f"开始执行: {scenario_name}")
        logger.info(f"场景ID: {scenario_id}")
        logger.info(f"描述: {scenario['description']}")
        logger.info("=" * 80)
        
        start_time = time.time()
        start_datetime = datetime.now()
        
        try:
            # 动态导入并执行场景
            module_name = scenario['module']
            
            # 使用 subprocess 执行独立进程
            script_path = os.path.join(
                "/workspace/agent_memory_system_v2/tests/practice_scenarios",
                f"{module_name}.py"
            )
            
            if os.path.exists(script_path):
                # 执行脚本
                process = await asyncio.create_subprocess_exec(
                    sys.executable, script_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    logger.error(f"场景执行失败: {stderr.decode()}")
                    raise Exception(f"Process failed with code {process.returncode}: {stderr.decode()}")
                
                # 读取报告
                report_path = os.path.join(
                    self.reports_dir,
                    f"{scenario_id}_report.json"
                )
                
                if os.path.exists(report_path):
                    with open(report_path, 'r', encoding='utf-8') as f:
                        report = json.load(f)
                    
                    defects_found = report.get('summary', {}).get('defects_found', 0)
                    critical = report.get('summary', {}).get('critical_defects', 0)
                    high = report.get('summary', {}).get('high_defects', 0)
                    medium = report.get('summary', {}).get('medium_defects', 0)
                    low = report.get('summary', {}).get('low_defects', 0)
                else:
                    defects_found = 0
                    critical = high = medium = low = 0
                    report_path = None
                
                success = True
                error_message = None
                
            else:
                # 直接导入执行
                module = __import__(f"practice_scenarios.{module_name}", fromlist=['main'])
                report = await module.main()
                
                defects_found = report.get('summary', {}).get('defects_found', 0)
                critical = report.get('summary', {}).get('critical_defects', 0)
                high = report.get('summary', {}).get('high_defects', 0)
                medium = report.get('summary', {}).get('medium_defects', 0)
                low = report.get('summary', {}).get('low_defects', 0)
                
                success = True
                error_message = None
                report_path = os.path.join(
                    self.reports_dir,
                    f"{scenario_id}_report.json"
                )
            
        except Exception as e:
            logger.error(f"场景执行异常: {e}")
            success = False
            error_message = str(e)
            defects_found = 0
            critical = high = medium = low = 0
            report_path = None
        
        end_time = time.time()
        end_datetime = datetime.now()
        
        result = ScenarioResult(
            scenario_id=scenario_id,
            scenario_name=scenario_name,
            success=success,
            start_time=start_datetime.isoformat(),
            end_time=end_datetime.isoformat(),
            duration_seconds=end_time - start_time,
            defects_found=defects_found,
            critical_defects=critical,
            high_defects=high,
            medium_defects=medium,
            low_defects=low,
            error_message=error_message,
            report_path=report_path
        )
        
        # 打印结果摘要
        status = "成功" if success else "失败"
        logger.info("\n" + "-" * 80)
        logger.info(f"场景执行完成: {scenario_name}")
        logger.info(f"状态: {status}")
        logger.info(f"持续时间: {result.duration_seconds:.2f} 秒")
        logger.info(f"发现缺陷: {defects_found} 个")
        if defects_found > 0:
            logger.info(f"  - 严重: {critical}, 高危: {high}, 中危: {medium}, 低危: {low}")
        if report_path:
            logger.info(f"报告路径: {report_path}")
        logger.info("-" * 80)
        
        return result
    
    def _create_error_result(self, scenario: Dict[str, Any], 
                            error_message: str) -> ScenarioResult:
        """创建错误结果"""
        return ScenarioResult(
            scenario_id=scenario['id'],
            scenario_name=scenario['name'],
            success=False,
            start_time=datetime.now().isoformat(),
            end_time=datetime.now().isoformat(),
            duration_seconds=0,
            defects_found=0,
            critical_defects=0,
            high_defects=0,
            medium_defects=0,
            low_defects=0,
            error_message=error_message
        )
    
    def _generate_summary(self, start_time: float, end_time: float) -> Dict[str, Any]:
        """生成汇总报告"""
        total_defects = sum(r.defects_found for r in self.results)
        total_critical = sum(r.critical_defects for r in self.results)
        total_high = sum(r.high_defects for r in self.results)
        total_medium = sum(r.medium_defects for r in self.results)
        total_low = sum(r.low_defects for r in self.results)
        
        passed = sum(1 for r in self.results if r.success)
        failed = sum(1 for r in self.results if not r.success)
        
        summary = {
            'test_suite': 'Agent Memory System V3.0 实践场景测试',
            'version': '3.0',
            'generated_at': datetime.now().isoformat(),
            'total_duration_seconds': end_time - start_time,
            'scenarios': {
                'total': len(self.scenarios),
                'passed': passed,
                'failed': failed
            },
            'defects': {
                'total': total_defects,
                'critical': total_critical,
                'high': total_high,
                'medium': total_medium,
                'low': total_low
            },
            'results': [asdict(r) for r in self.results],
            'conclusions': self._generate_conclusions()
        }
        
        # 保存汇总报告
        summary_path = os.path.join(self.reports_dir, "all_scenarios_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n汇总报告已保存: {summary_path}")
        
        return summary
    
    def _generate_conclusions(self) -> List[str]:
        """生成结论"""
        conclusions = []
        
        total_defects = sum(r.defects_found for r in self.results)
        total_critical = sum(r.critical_defects for r in self.results)
        total_high = sum(r.high_defects for r in self.results)
        
        if total_critical > 0:
            conclusions.append(
                f"发现 {total_critical} 个严重缺陷，需要立即修复。"
                "这些缺陷可能导致数据丢失、系统崩溃或安全漏洞。"
            )
        
        if total_high > 0:
            conclusions.append(
                f"发现 {total_high} 个高危缺陷，建议优先处理。"
                "这些缺陷可能影响系统稳定性和数据一致性。"
            )
        
        # 分析各场景结果
        for result in self.results:
            if not result.success:
                conclusions.append(
                    f"场景 '{result.scenario_name}' 执行失败: {result.error_message}"
                )
            elif result.defects_found > 5:
                conclusions.append(
                    f"场景 '{result.scenario_name}' 发现较多缺陷 ({result.defects_found}个)，"
                    f"建议深入分析该场景相关模块。"
                )
        
        if total_defects == 0:
            conclusions.append(
                "所有场景测试通过，未发现明显缺陷。"
                "建议继续进行更深入的测试和代码审查。"
            )
        
        return conclusions
    
    def print_summary(self, summary: Dict[str, Any]):
        """打印汇总信息"""
        print("\n")
        print("=" * 80)
        print("Agent Memory System V3.0 - 实践场景测试汇总")
        print("=" * 80)
        print(f"执行时间: {summary['generated_at']}")
        print(f"总耗时: {summary['total_duration_seconds']:.2f} 秒")
        print()
        print("场景执行情况:")
        print(f"  总场景数: {summary['scenarios']['total']}")
        print(f"  通过: {summary['scenarios']['passed']}")
        print(f"  失败: {summary['scenarios']['failed']}")
        print()
        print("缺陷统计:")
        print(f"  总缺陷数: {summary['defects']['total']}")
        print(f"  严重: {summary['defects']['critical']}")
        print(f"  高危: {summary['defects']['high']}")
        print(f"  中危: {summary['defects']['medium']}")
        print(f"  低危: {summary['defects']['low']}")
        print()
        
        # 打印各场景详情
        print("-" * 80)
        print("各场景详情:")
        print("-" * 80)
        
        for result in summary['results']:
            status = "PASS" if result['success'] else "FAIL"
            print(f"\n[{status}] {result['scenario_name']}")
            print(f"  ID: {result['scenario_id']}")
            print(f"  持续时间: {result['duration_seconds']:.2f} 秒")
            print(f"  缺陷数: {result['defects_found']}")
            if result['defects_found'] > 0:
                print(f"    严重: {result['critical_defects']}, "
                      f"高危: {result['high_defects']}, "
                      f"中危: {result['medium_defects']}, "
                      f"低危: {result['low_defects']}")
            if result.get('error_message'):
                print(f"  错误: {result['error_message']}")
        
        # 打印结论
        print()
        print("-" * 80)
        print("结论:")
        print("-" * 80)
        for conclusion in summary['conclusions']:
            print(f"  - {conclusion}")
        
        print()
        print("=" * 80)


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Agent Memory System V3.0 实践场景测试')
    parser.add_argument('--parallel', action='store_true', 
                       help='并行执行所有场景（默认串行）')
    parser.add_argument('--scenario', type=str, 
                       help='只运行指定场景（场景ID）')
    
    args = parser.parse_args()
    
    runner = ScenarioRunner()
    
    if args.scenario:
        # 只运行指定场景
        scenario = next(
            (s for s in runner.scenarios if s['id'] == args.scenario),
            None
        )
        if scenario:
            result = await runner.run_scenario(scenario)
            runner.results = [result]
            summary = runner._generate_summary(time.time(), time.time())
        else:
            logger.error(f"未找到场景: {args.scenario}")
            return
    else:
        # 运行所有场景
        summary = await runner.run_all_scenarios(parallel=args.parallel)
    
    # 打印汇总
    runner.print_summary(summary)
    
    # 返回退出码
    if summary['scenarios']['failed'] > 0 or summary['defects']['critical'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())

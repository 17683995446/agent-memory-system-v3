#!/usr/bin/env python3
"""
Agent Memory System V3.0 - 长时间运行测试统一执行脚本

此脚本用于统一运行所有8个长时间、高复杂度测试场景:
1. 24小时高负载Soak测试
2. 极端并发写入风暴
3. 级联故障恢复测试
4. 内存压力与OOM测试
5. 磁盘故障与数据损坏
6. 时钟漂移与分布式一致性
7. 网络分区脑裂测试
8. 完整灾难恢复演练

使用方法:
    python run_long_running_tests.py [选项]

选项:
    --all              运行所有测试场景 (默认)
    --scenario N       只运行指定场景 (1-8)
    --parallel         并行运行所有场景
    --sequential       串行运行所有场景 (默认)
    --duration MIN     设置每个场景的运行时间(分钟), 默认10分钟
    --output DIR       指定输出目录
    --verbose          详细输出
    --help             显示帮助信息

示例:
    python run_long_running_tests.py --all
    python run_long_running_tests.py --scenario 1 --duration 15
    python run_long_running_tests.py --parallel --duration 5
"""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/long_running/test_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class TestScenario:
    """测试场景定义"""
    id: int
    name: str
    description: str
    script_path: str
    default_duration: int = 10  # 默认运行10分钟
    estimated_memory_mb: int = 512
    concurrent_clients: int = 100


@dataclass
class TestResult:
    """测试结果"""
    scenario_id: int
    scenario_name: str
    status: str  # "passed", "failed", "skipped"
    start_time: float = 0.0
    end_time: float = 0.0
    duration_sec: float = 0.0
    return_code: int = 0
    stdout: str = ""
    stderr: str = ""
    report_file: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)
    error_message: str = ""


@dataclass
class TestSuiteReport:
    """测试套件报告"""
    suite_name: str = "Agent Memory System V3.0 - 长时间运行测试套件"
    execution_start: float = 0.0
    execution_end: float = 0.0
    total_duration_sec: float = 0.0
    total_scenarios: int = 8
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    results: List[TestResult] = field(default_factory=list)
    system_info: Dict[str, Any] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)


# 定义所有测试场景
TEST_SCENARIOS = [
    TestScenario(
        id=1,
        name="24小时高负载Soak测试",
        description="持续写入/读取/查询操作, 模拟100万条记忆的完整生命周期, 监控内存泄漏和性能衰减",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario1_soak_test.py",
        default_duration=10,
        estimated_memory_mb=2048,
        concurrent_clients=100
    ),
    TestScenario(
        id=2,
        name="极端并发写入风暴",
        description="1000个并发客户端同时写入, 每秒10000次写入操作, 验证数据一致性和去重",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario2_write_storm.py",
        default_duration=10,
        estimated_memory_mb=1024,
        concurrent_clients=1000
    ),
    TestScenario(
        id=3,
        name="级联故障恢复测试",
        description="5节点Raft集群, 依次杀死Leader、Follower, 网络分区+节点重启, 验证自动恢复",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario3_cascading_failure.py",
        default_duration=10,
        estimated_memory_mb=512,
        concurrent_clients=100
    ),
    TestScenario(
        id=4,
        name="内存压力与OOM测试",
        description="限制容器内存为512MB, 持续加载大量数据, 触发GC压力, 验证优雅降级和恢复",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario4_memory_pressure.py",
        default_duration=10,
        estimated_memory_mb=512,
        concurrent_clients=100
    ),
    TestScenario(
        id=5,
        name="磁盘故障与数据损坏",
        description="模拟磁盘满、磁盘损坏, 随机数据损坏注入, 验证数据校验和恢复, 测试备份恢复流程",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario5_disk_failure.py",
        default_duration=10,
        estimated_memory_mb=512,
        concurrent_clients=100
    ),
    TestScenario(
        id=6,
        name="时钟漂移与分布式一致性",
        description="各节点时钟不同步(±5秒), 验证分布式事务一致性, 测试时间戳冲突解决, 验证因果一致性",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario6_clock_drift.py",
        default_duration=10,
        estimated_memory_mb=512,
        concurrent_clients=100
    ),
    TestScenario(
        id=7,
        name="网络分区脑裂测试",
        description="将集群分为两个分区, 各自选举Leader, 恢复分区后验证数据合并, 测试冲突解决策略",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario7_network_partition.py",
        default_duration=10,
        estimated_memory_mb=512,
        concurrent_clients=100
    ),
    TestScenario(
        id=8,
        name="完整灾难恢复演练",
        description="模拟数据中心完全故障, 从备份恢复, 验证RPO/RTO指标, 测试多区域故障转移",
        script_path="/workspace/agent_memory_system_v2/tests/long_running/scenario8_disaster_recovery.py",
        default_duration=10,
        estimated_memory_mb=1024,
        concurrent_clients=100
    ),
]


def get_system_info() -> Dict[str, Any]:
    """获取系统信息"""
    cpu_info = {
        "physical_cores": psutil.cpu_count(logical=False),
        "logical_cores": psutil.cpu_count(logical=True),
        "cpu_percent": psutil.cpu_percent(interval=1),
    }
    
    memory = psutil.virtual_memory()
    memory_info = {
        "total_gb": memory.total / (1024**3),
        "available_gb": memory.available / (1024**3),
        "percent_used": memory.percent,
    }
    
    disk = psutil.disk_usage('/')
    disk_info = {
        "total_gb": disk.total / (1024**3),
        "free_gb": disk.free / (1024**3),
        "percent_used": disk.percent,
    }
    
    return {
        "cpu": cpu_info,
        "memory": memory_info,
        "disk": disk_info,
        "platform": sys.platform,
        "python_version": sys.version,
        "timestamp": datetime.now().isoformat(),
    }


def run_scenario(scenario: TestScenario, duration_minutes: int) -> TestResult:
    """运行单个测试场景 - DEFECT-012 [P2] 修复: 添加全局超时处理"""
    logger.info(f"开始运行场景 {scenario.id}: {scenario.name}")
    logger.info(f"  描述: {scenario.description}")
    logger.info(f"  脚本: {scenario.script_path}")
    logger.info(f"  持续时间: {duration_minutes}分钟")

    result = TestResult(
        scenario_id=scenario.id,
        scenario_name=scenario.name,
        status="running"
    )

    result.start_time = time.time()

    try:
        # 检查脚本是否存在
        if not Path(scenario.script_path).exists():
            result.status = "failed"
            result.error_message = f"Script not found: {scenario.script_path}"
            result.end_time = time.time()
            result.duration_sec = result.end_time - result.start_time
            return result

        # 构建命令
        env = os.environ.copy()
        env["TEST_DURATION_MINUTES"] = str(duration_minutes)
        env["PYTHONPATH"] = "/workspace/agent_memory_system_v2"

        # DEFECT-012 [P2] 修复: 计算全局超时时间
        # 基础运行时间 + 启动缓冲(30秒) + 清理缓冲(60秒)
        global_timeout = (duration_minutes * 60) + 30 + 60
        logger.info(f"  全局超时: {global_timeout}秒")

        # 运行测试脚本
        process = subprocess.Popen(
            [sys.executable, scenario.script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            cwd="/workspace/agent_memory_system_v2/tests/long_running"
        )

        # DEFECT-012 [P2] 修复: 使用超时机制等待进程完成
        try:
            stdout, stderr = process.communicate(timeout=global_timeout)
        except subprocess.TimeoutExpired:
            # 超时处理
            logger.error(f"场景 {scenario.id} 执行超时 ({global_timeout}秒)，强制终止")
            process.kill()
            stdout, stderr = process.communicate()

            result.status = "failed"
            result.error_message = f"Test timeout after {global_timeout} seconds"
            result.return_code = -1
            result.stdout = stdout
            result.stderr = stderr + f"\n[TIMEOUT] Test exceeded {global_timeout} seconds"
            result.end_time = time.time()
            result.duration_sec = result.end_time - result.start_time
            return result

        result.end_time = time.time()
        result.duration_sec = result.end_time - result.start_time
        result.return_code = process.returncode
        result.stdout = stdout
        result.stderr = stderr

        # 确定状态
        if process.returncode == 0:
            result.status = "passed"
            logger.info(f"场景 {scenario.id} 通过 ({result.duration_sec:.2f}秒)")
        else:
            result.status = "failed"
            result.error_message = f"Process returned code {process.returncode}"
            logger.error(f"场景 {scenario.id} 失败: {result.error_message}")

        # 尝试读取报告文件
        report_file = scenario.script_path.replace(".py", "_report.json")
        if Path(report_file).exists():
            result.report_file = report_file
            try:
                with open(report_file, 'r') as f:
                    result.metrics = json.load(f)
            except Exception as e:
                logger.warning(f"无法读取报告文件 {report_file}: {e}")

    except Exception as e:
        result.status = "failed"
        result.error_message = str(e)
        result.end_time = time.time()
        result.duration_sec = result.end_time - result.start_time
        logger.error(f"场景 {scenario.id} 执行异常: {e}")

    return result


async def run_scenario_async(scenario: TestScenario, duration_minutes: int) -> TestResult:
    """异步运行单个测试场景"""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(
            pool, run_scenario, scenario, duration_minutes
        )
    return result


async def run_all_scenarios_parallel(scenarios: List[TestScenario], 
                                     duration_minutes: int) -> List[TestResult]:
    """并行运行所有场景"""
    logger.info(f"并行运行 {len(scenarios)} 个场景")
    
    tasks = [
        run_scenario_async(scenario, duration_minutes)
        for scenario in scenarios
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 处理异常结果
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            # 创建失败结果
            failed_result = TestResult(
                scenario_id=scenarios[i].id,
                scenario_name=scenarios[i].name,
                status="failed",
                error_message=str(result)
            )
            processed_results.append(failed_result)
        else:
            processed_results.append(result)
    
    return processed_results


async def run_all_scenarios_sequential(scenarios: List[TestScenario], 
                                       duration_minutes: int) -> List[TestResult]:
    """串行运行所有场景"""
    logger.info(f"串行运行 {len(scenarios)} 个场景")
    
    results = []
    for scenario in scenarios:
        result = await run_scenario_async(scenario, duration_minutes)
        results.append(result)
        
        # 场景之间短暂停顿
        await asyncio.sleep(2)
    
    return results


def generate_html_report(report: TestSuiteReport, output_path: str):
    """生成HTML报告"""
    html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{report.suite_name} - 测试报告</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .summary-card.passed {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .summary-card.failed {{
            background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .status-passed {{
            color: #4CAF50;
            font-weight: bold;
        }}
        .status-failed {{
            color: #f44336;
            font-weight: bold;
        }}
        .status-skipped {{
            color: #ff9800;
            font-weight: bold;
        }}
        .system-info {{
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .system-info pre {{
            margin: 0;
            overflow-x: auto;
        }}
        .timestamp {{
            color: #666;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{report.suite_name}</h1>
        <p class="timestamp">执行时间: {datetime.fromtimestamp(report.execution_start).strftime('%Y-%m-%d %H:%M:%S')} - 
           {datetime.fromtimestamp(report.execution_end).strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p class="timestamp">总耗时: {report.total_duration_sec/60:.2f} 分钟</p>
        
        <h2>执行摘要</h2>
        <div class="summary">
            <div class="summary-card">
                <h3>总场景数</h3>
                <div class="value">{report.total_scenarios}</div>
            </div>
            <div class="summary-card passed">
                <h3>通过</h3>
                <div class="value">{report.passed}</div>
            </div>
            <div class="summary-card failed">
                <h3>失败</h3>
                <div class="value">{report.failed}</div>
            </div>
            <div class="summary-card">
                <h3>跳过</h3>
                <div class="value">{report.skipped}</div>
            </div>
        </div>
        
        <h2>系统信息</h2>
        <div class="system-info">
            <pre>{json.dumps(report.system_info, indent=2, ensure_ascii=False)}</pre>
        </div>
        
        <h2>详细结果</h2>
        <table>
            <thead>
                <tr>
                    <th>场景ID</th>
                    <th>场景名称</th>
                    <th>状态</th>
                    <th>耗时(秒)</th>
                    <th>返回码</th>
                    <th>报告文件</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for result in report.results:
        status_class = f"status-{result.status}"
        html_content += f"""
                <tr>
                    <td>{result.scenario_id}</td>
                    <td>{result.scenario_name}</td>
                    <td class="{status_class}">{result.status.upper()}</td>
                    <td>{result.duration_sec:.2f}</td>
                    <td>{result.return_code}</td>
                    <td>{result.report_file or 'N/A'}</td>
                </tr>
"""
    
    html_content += """
            </tbody>
        </table>
        
        <h2>失败详情</h2>
"""
    
    failed_results = [r for r in report.results if r.status == "failed"]
    if failed_results:
        for result in failed_results:
            html_content += f"""
        <div style="background-color: #ffebee; padding: 15px; margin: 10px 0; border-radius: 5px;">
            <h4>场景 {result.scenario_id}: {result.scenario_name}</h4>
            <p><strong>错误:</strong> {result.error_message}</p>
            <pre style="background-color: #f5f5f5; padding: 10px; overflow-x: auto;">{result.stderr[:2000]}</pre>
        </div>
"""
    else:
        html_content += "<p>所有场景均通过, 无失败详情。</p>"
    
    html_content += """
    </div>
</body>
</html>
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"HTML报告已生成: {output_path}")


def print_summary(report: TestSuiteReport):
    """打印测试摘要"""
    print("\n" + "="*80)
    print(f"{report.suite_name}")
    print("="*80)
    print(f"\n执行时间: {datetime.fromtimestamp(report.execution_start).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {report.total_duration_sec/60:.2f} 分钟")
    print(f"\n结果统计:")
    print(f"  - 总场景数: {report.total_scenarios}")
    print(f"  - 通过: {report.passed} ({report.passed/report.total_scenarios*100:.1f}%)")
    print(f"  - 失败: {report.failed} ({report.failed/report.total_scenarios*100:.1f}%)")
    print(f"  - 跳过: {report.skipped}")
    
    print(f"\n详细结果:")
    print("-"*80)
    print(f"{'ID':<5} {'名称':<30} {'状态':<10} {'耗时(秒)':<12} {'报告文件'}")
    print("-"*80)
    
    for result in report.results:
        status_symbol = "✓" if result.status == "passed" else "✗" if result.status == "failed" else "○"
        report_file = Path(result.report_file).name if result.report_file else "N/A"
        print(f"{result.scenario_id:<5} {result.scenario_name[:28]:<30} {status_symbol} {result.status:<8} {result.duration_sec:<12.2f} {report_file}")
    
    print("="*80)
    
    if report.failed > 0:
        print(f"\n失败的场景:")
        for result in report.results:
            if result.status == "failed":
                print(f"  - 场景 {result.scenario_id}: {result.scenario_name}")
                print(f"    错误: {result.error_message}")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Agent Memory System V3.0 - 长时间运行测试套件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_long_running_tests.py --all
  python run_long_running_tests.py --scenario 1 --duration 15
  python run_long_running_tests.py --parallel --duration 5
        """
    )
    
    parser.add_argument(
        "--all", 
        action="store_true", 
        help="运行所有测试场景 (默认)"
    )
    parser.add_argument(
        "--scenario", 
        type=int, 
        choices=range(1, 9),
        metavar="N",
        help="只运行指定场景 (1-8)"
    )
    parser.add_argument(
        "--parallel", 
        action="store_true", 
        help="并行运行所有场景"
    )
    parser.add_argument(
        "--sequential", 
        action="store_true", 
        help="串行运行所有场景 (默认)"
    )
    parser.add_argument(
        "--duration", 
        type=int, 
        default=10,
        metavar="MIN",
        help="设置每个场景的运行时间(分钟), 默认10分钟"
    )
    parser.add_argument(
        "--output", 
        type=str,
        default="/workspace/agent_memory_system_v2/tests/long_running",
        metavar="DIR",
        help="指定输出目录"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="详细输出"
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 确定要运行的场景
    if args.scenario:
        scenarios = [s for s in TEST_SCENARIOS if s.id == args.scenario]
    else:
        scenarios = TEST_SCENARIOS
    
    logger.info("="*80)
    logger.info("Agent Memory System V3.0 - 长时间运行测试套件")
    logger.info("="*80)
    logger.info(f"运行模式: {'并行' if args.parallel else '串行'}")
    logger.info(f"场景数量: {len(scenarios)}")
    logger.info(f"每个场景持续时间: {args.duration}分钟")
    logger.info(f"预计总耗时: {len(scenarios) * args.duration if not args.parallel else args.duration}分钟")
    logger.info("="*80)
    
    # 创建测试套件报告
    report = TestSuiteReport()
    report.execution_start = time.time()
    report.system_info = get_system_info()
    report.total_scenarios = len(scenarios)
    
    # 运行测试
    if args.parallel:
        results = await run_all_scenarios_parallel(scenarios, args.duration)
    else:
        results = await run_all_scenarios_sequential(scenarios, args.duration)
    
    report.results = results
    report.execution_end = time.time()
    report.total_duration_sec = report.execution_end - report.execution_start
    
    # 统计结果
    report.passed = sum(1 for r in results if r.status == "passed")
    report.failed = sum(1 for r in results if r.status == "failed")
    report.skipped = sum(1 for r in results if r.status == "skipped")
    
    # 保存JSON报告
    report_path = Path(args.output) / "test_suite_report.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(asdict(report), f, indent=2, default=str)
    logger.info(f"JSON报告已保存: {report_path}")
    
    # 生成HTML报告
    html_path = Path(args.output) / "test_suite_report.html"
    generate_html_report(report, str(html_path))
    
    # 打印摘要
    print_summary(report)
    
    # 返回退出码
    return 0 if report.failed == 0 else 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("测试被用户中断")
        sys.exit(130)
    except Exception as e:
        logger.error(f"执行异常: {e}")
        sys.exit(1)

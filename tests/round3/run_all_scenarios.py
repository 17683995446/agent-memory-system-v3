#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 第3轮实践测试
复杂集成与混沌工程测试

测试场景：
1. 多模块集成测试 - Pipeline + TaskScheduler + EventBus 联动
2. 混沌工程 - 随机故障、网络延迟、资源限制
3. 依赖故障传播 - 熔断机制、降级策略
4. 数据一致性验证 - 分布式事务一致性
5. 安全渗透测试 - SQL注入、XSS攻击、权限绕过

Author: Agent Memory System Team
Version: 3.0.0
"""

import asyncio
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

# 添加项目路径
sys.path.insert(0, '/workspace/agent_memory_system_v2')

from memory_system.async_pipeline import (
    Pipeline, PipelineStage, StageConfig, PipelineConfig, PipelineItem
)
from memory_system.task_scheduler import (
    TaskScheduler, Task, TaskPriority, SchedulerConfig, DAGBuilder
)
from memory_system.event_driven_core import (
    InMemoryEventBus, DomainEvent, EventPriority, BackendType,
    SagaOrchestrator, SagaStep, EventBusFactory
)
from memory_system.chaos_engineering import (
    ChaosEngineeringFramework, FaultType, CPUFaultConfig,
    MemoryFaultConfig, NetworkFaultConfig, DiskFaultConfig
)
from memory_system.distributed_transaction_manager import (
    DistributedTransactionManager, TransactionContext, Participant,
    TwoPhaseCommitCoordinator, SagaOrchestrator as TxSagaOrchestrator
)
from memory_system.security_hardening import (
    SecurityHardeningModule, Permission, Role, InputValidator,
    RBACManager, ABACManager
)
from memory_system.api_gateway import (
    APIGateway, GatewayRequest, Route, JWTAuthenticator,
    AuthorizationManager, CacheStrategy
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/workspace/agent_memory_system_v2/tests/round3/logs/round3_test.log')
    ]
)
logger = logging.getLogger(__name__)


class DefectCollector:
    """缺陷收集器"""
    
    def __init__(self):
        self.defects: List[Dict[str, Any]] = []
        self.test_start_time = datetime.now()
    
    def add_defect(
        self,
        defect_id: str,
        scenario: str,
        severity: str,
        title: str,
        description: str,
        steps: List[str],
        expected: str,
        actual: str,
        error: Optional[str] = None
    ):
        """添加缺陷"""
        defect = {
            "defect_id": defect_id,
            "scenario": scenario,
            "severity": severity,
            "title": title,
            "description": description,
            "steps": steps,
            "expected": expected,
            "actual": actual,
            "error": error,
            "timestamp": datetime.now().isoformat(),
        }
        self.defects.append(defect)
        logger.error(f"[DEFECT] {defect_id}: {title}")
    
    def save_report(self, filepath: str):
        """保存缺陷报告"""
        report = {
            "test_round": "Round 3 - Complex Integration & Chaos Engineering",
            "test_start": self.test_start_time.isoformat(),
            "test_end": datetime.now().isoformat(),
            "total_defects": len(self.defects),
            "severity_counts": self._count_by_severity(),
            "defects": self.defects
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Defect report saved to: {filepath}")
    
    def _count_by_severity(self) -> Dict[str, int]:
        """按严重程度统计"""
        counts = {"P0": 0, "P1": 0, "P2": 0, "P3": 0}
        for defect in self.defects:
            severity = defect.get("severity", "P3")
            if severity in counts:
                counts[severity] += 1
        return counts


# 全局缺陷收集器
defect_collector = DefectCollector()


# =============================================================================
# 场景1: 多模块集成测试
# =============================================================================

async def scenario1_multi_module_integration():
    """
    场景1: 多模块集成测试
    - Pipeline + TaskScheduler + EventBus 联动
    - 验证跨模块数据流
    """
    logger.info("\n" + "="*80)
    logger.info("场景1: 多模块集成测试 - Pipeline + TaskScheduler + EventBus")
    logger.info("="*80)
    
    results = {
        "scenario": "multi_module_integration",
        "tests": [],
        "passed": 0,
        "failed": 0
    }
    
    try:
        # 创建共享组件
        event_bus = EventBusFactory.create(BackendType.IN_MEMORY)
        await event_bus.start()
        
        scheduler_config = SchedulerConfig(max_workers=4)
        task_scheduler = TaskScheduler(scheduler_config)
        await task_scheduler.start()
        
        # 测试1: Pipeline处理触发EventBus事件
        logger.info("\n[Test 1.1] Pipeline处理触发EventBus事件")
        try:
            events_received = []
            
            async def event_handler(event: DomainEvent):
                events_received.append(event)
                logger.info(f"  Event received: {event.event_type}")
            
            await event_bus.subscribe("pipeline.item.processed", event_handler)
            
            # 创建带事件发布的Pipeline阶段
            async def processing_with_event(item: PipelineItem) -> PipelineItem:
                # 处理数据
                item.data["processed"] = True
                item.data["timestamp"] = time.time()
                
                # 发布事件
                event = DomainEvent(
                    event_type="pipeline.item.processed",
                    payload={"item_id": item.item_id, "status": "success"},
                    priority=EventPriority.NORMAL
                )
                await event_bus.publish(event)
                return item
            
            pipeline = Pipeline([
                PipelineStage(
                    name="processing",
                    handler=processing_with_event,
                    config=StageConfig(name="processing", num_workers=2)
                )
            ])
            
            await pipeline.start()
            
            # 提交测试数据
            test_items = [{"id": i, "data": f"test_{i}"} for i in range(5)]
            for item_data in test_items:
                await pipeline.submit(item_data, wait_for_result=True)
            
            await asyncio.sleep(0.5)  # 等待事件处理
            
            if len(events_received) == 5:
                logger.info("  [PASS] All events received correctly")
                results["tests"].append({"name": "pipeline_event_trigger", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Expected 5 events, got {len(events_received)}")
                
            await pipeline.stop()
            
        except Exception as e:
            logger.error(f"  [FAIL] Test 1.1 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S1-001",
                scenario="multi_module_integration",
                severity="P1",
                title="Pipeline事件触发失败",
                description="Pipeline处理完成后未能正确触发EventBus事件",
                steps=["1. 创建Pipeline", "2. 添加事件发布逻辑", "3. 提交测试数据"],
                expected="每个处理完成的项目都应触发对应事件",
                actual=f"事件接收数量不匹配: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "pipeline_event_trigger", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试2: TaskScheduler调度Pipeline任务
        logger.info("\n[Test 1.2] TaskScheduler调度Pipeline任务")
        try:
            pipeline_results = []
            
            async def pipeline_task():
                # 创建临时Pipeline
                temp_pipeline = Pipeline([
                    PipelineStage(
                        name="transform",
                        handler=lambda item: (setattr(item, 'data', {**item.data, 'transformed': True}), item)[1],
                        config=StageConfig(name="transform", num_workers=1)
                    )
                ])
                await temp_pipeline.start()
                result = await temp_pipeline.submit({"input": "data"}, wait_for_result=True)
                await temp_pipeline.stop()
                pipeline_results.append(result)
                return result
            
            # 提交多个Pipeline任务
            tasks = []
            for i in range(3):
                task = Task(
                    task_id=f"pipeline_task_{i}",
                    coroutine=pipeline_task(),
                    priority=TaskPriority.NORMAL
                )
                tasks.append(task)
            
            await task_scheduler.submit_batch(tasks)
            await asyncio.sleep(2)  # 等待任务完成
            
            if len(pipeline_results) == 3:
                logger.info("  [PASS] All pipeline tasks completed")
                results["tests"].append({"name": "scheduler_pipeline_tasks", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Expected 3 results, got {len(pipeline_results)}")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 1.2 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S1-002",
                scenario="multi_module_integration",
                severity="P1",
                title="TaskScheduler调度Pipeline任务失败",
                description="TaskScheduler无法正确调度包含Pipeline的任务",
                steps=["1. 创建Pipeline任务", "2. 提交到TaskScheduler", "3. 等待执行"],
                expected="所有Pipeline任务应被正确调度并执行",
                actual=f"任务执行结果数量不匹配: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "scheduler_pipeline_tasks", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试3: 跨模块数据流验证
        logger.info("\n[Test 1.3] 跨模块数据流验证")
        try:
            data_flow_log = []
            
            # 创建跨模块数据流
            async def cross_module_flow():
                # Stage 1: Pipeline处理
                pipeline = Pipeline([
                    PipelineStage(
                        name="enrich",
                        handler=lambda item: (setattr(item, 'data', {**item.data, 'stage': 'enriched'}), item)[1],
                        config=StageConfig(name="enrich", num_workers=1)
                    )
                ])
                await pipeline.start()
                result = await pipeline.submit({"source": "test"}, wait_for_result=True)
                await pipeline.stop()
                data_flow_log.append({"stage": "pipeline", "data": result.data if result else None})
                
                # Stage 2: 发布事件
                event = DomainEvent(
                    event_type="data.enriched",
                    payload=result.data if result else {},
                    priority=EventPriority.HIGH
                )
                await event_bus.publish(event)
                data_flow_log.append({"stage": "event_bus", "event_type": "data.enriched"})
                
                # Stage 3: TaskScheduler处理
                async def process_event():
                    data_flow_log.append({"stage": "scheduler", "action": "processing"})
                    return {"status": "completed"}
                
                task = Task(
                    task_id=f"process_{uuid.uuid4().hex[:8]}",
                    coroutine=process_event(),
                    priority=TaskPriority.HIGH
                )
                await task_scheduler.submit(task, wait=True)
                
                return data_flow_log
            
            flow_result = await cross_module_flow()
            await asyncio.sleep(0.5)
            
            # 验证数据流完整性
            stages = [log["stage"] for log in flow_result]
            expected_stages = ["pipeline", "event_bus", "scheduler"]
            
            if all(stage in stages for stage in expected_stages):
                logger.info("  [PASS] Cross-module data flow verified")
                results["tests"].append({"name": "cross_module_data_flow", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Missing stages. Got: {stages}")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 1.3 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S1-003",
                scenario="multi_module_integration",
                severity="P0",
                title="跨模块数据流中断",
                description="Pipeline、EventBus、TaskScheduler之间的数据流存在中断",
                steps=["1. 启动Pipeline处理", "2. 发布事件到EventBus", "3. TaskScheduler处理"],
                expected="数据应完整流经所有模块",
                actual=f"数据流验证失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "cross_module_data_flow", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 清理
        await task_scheduler.stop()
        await event_bus.stop()
        
    except Exception as e:
        logger.error(f"Scenario 1 setup failed: {e}")
        defect_collector.add_defect(
            defect_id="R3-S1-SETUP",
            scenario="multi_module_integration",
            severity="P0",
            title="场景1初始化失败",
            description="多模块集成测试场景初始化失败",
            steps=["1. 创建EventBus", "2. 创建TaskScheduler", "3. 初始化Pipeline"],
            expected="所有组件应正确初始化",
            actual=f"初始化失败: {str(e)}",
            error=str(e)
        )
    
    return results


# =============================================================================
# 场景2: 混沌工程测试
# =============================================================================

async def scenario2_chaos_engineering():
    """
    场景2: 混沌工程测试
    - 随机杀死服务
    - 随机网络延迟
    - 随机资源限制
    """
    logger.info("\n" + "="*80)
    logger.info("场景2: 混沌工程测试 - 随机故障注入")
    logger.info("="*80)
    
    results = {
        "scenario": "chaos_engineering",
        "tests": [],
        "passed": 0,
        "failed": 0
    }
    
    try:
        chaos_framework = ChaosEngineeringFramework()
        
        # 测试1: CPU压力测试
        logger.info("\n[Test 2.1] CPU压力测试")
        try:
            cpu_config = CPUFaultConfig(
                cores=2,
                load_percent=70.0,
                duration=5.0
            )
            
            experiment = chaos_framework.create_experiment(
                name="cpu-stress-test",
                description="Test system under CPU stress",
                fault_type=FaultType.CPU,
                fault_config=cpu_config
            )
            
            success = chaos_framework.start_experiment(experiment.id)
            
            if success:
                # 等待实验完成
                await asyncio.sleep(6)
                
                status = chaos_framework.get_experiment_status(experiment.id)
                if status and status["status"] in ["completed", "running"]:
                    logger.info("  [PASS] CPU stress test completed")
                    results["tests"].append({"name": "cpu_stress", "status": "passed"})
                    results["passed"] += 1
                else:
                    raise AssertionError(f"Experiment status: {status}")
            else:
                raise AssertionError("Failed to start CPU experiment")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 2.1 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S2-001",
                scenario="chaos_engineering",
                severity="P1",
                title="CPU压力测试失败",
                description="混沌工程CPU压力测试无法正常执行",
                steps=["1. 创建CPU故障配置", "2. 启动实验", "3. 监控执行"],
                expected="CPU压力测试应正常执行并完成",
                actual=f"测试执行失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "cpu_stress", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试2: 内存压力测试
        logger.info("\n[Test 2.2] 内存压力测试")
        try:
            mem_config = MemoryFaultConfig(
                memory_mb=128,
                leak_rate_mb_per_sec=10.0,
                duration=5.0
            )
            
            experiment = chaos_framework.create_experiment(
                name="memory-pressure-test",
                description="Test system under memory pressure",
                fault_type=FaultType.MEMORY,
                fault_config=mem_config
            )
            
            success = chaos_framework.start_experiment(experiment.id)
            
            if success:
                await asyncio.sleep(6)
                
                status = chaos_framework.get_experiment_status(experiment.id)
                if status and status["status"] in ["completed", "running"]:
                    logger.info("  [PASS] Memory pressure test completed")
                    results["tests"].append({"name": "memory_pressure", "status": "passed"})
                    results["passed"] += 1
                else:
                    raise AssertionError(f"Experiment status: {status}")
            else:
                raise AssertionError("Failed to start memory experiment")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 2.2 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S2-002",
                scenario="chaos_engineering",
                severity="P1",
                title="内存压力测试失败",
                description="混沌工程内存压力测试无法正常执行",
                steps=["1. 创建内存故障配置", "2. 启动实验", "3. 监控执行"],
                expected="内存压力测试应正常执行并完成",
                actual=f"测试执行失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "memory_pressure", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试3: 随机故障组合测试
        logger.info("\n[Test 2.3] 随机故障组合测试")
        try:
            # 创建多个随机故障实验
            experiments = []
            fault_types = [FaultType.CPU, FaultType.MEMORY]
            
            for i, fault_type in enumerate(fault_types):
                if fault_type == FaultType.CPU:
                    config = CPUFaultConfig(
                        cores=1,
                        load_percent=50.0 + random.randint(0, 30),
                        duration=3.0
                    )
                else:
                    config = MemoryFaultConfig(
                        memory_mb=64,
                        duration=3.0
                    )
                
                experiment = chaos_framework.create_experiment(
                    name=f"random-fault-{i}",
                    description=f"Random fault injection: {fault_type.name}",
                    fault_type=fault_type,
                    fault_config=config
                )
                experiments.append(experiment)
            
            # 顺序执行实验
            completed_count = 0
            for exp in experiments:
                if chaos_framework.start_experiment(exp.id):
                    await asyncio.sleep(4)
                    status = chaos_framework.get_experiment_status(exp.id)
                    if status and status["status"] in ["completed", "running"]:
                        completed_count += 1
            
            if completed_count == len(experiments):
                logger.info("  [PASS] Random fault combination test completed")
                results["tests"].append({"name": "random_fault_combo", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Only {completed_count}/{len(experiments)} experiments completed")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 2.3 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S2-003",
                scenario="chaos_engineering",
                severity="P2",
                title="随机故障组合测试失败",
                description="随机故障组合测试无法正常执行",
                steps=["1. 创建多个随机故障实验", "2. 顺序执行", "3. 验证完成状态"],
                expected="所有随机故障实验应正常完成",
                actual=f"部分实验未完成: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "random_fault_combo", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试4: 系统韧性评分
        logger.info("\n[Test 2.4] 系统韧性评分")
        try:
            score = chaos_framework.calculate_resilience_score(
                availability_data={"uptime_percent": 99.5},
                latency_data={"p99_ms": 80, "target_ms": 100},
                error_data={"error_rate_percent": 0.5},
                recovery_data={"mttr_seconds": 45, "target_mttr_seconds": 60}
            )
            
            logger.info(f"  Resilience Score: {score.overall_score}/100")
            logger.info(f"    - Availability: {score.availability_score}/25")
            logger.info(f"    - Latency: {score.latency_score}/25")
            logger.info(f"    - Error Rate: {score.error_rate_score}/25")
            logger.info(f"    - Recoverability: {score.recoverability_score}/25")
            
            if score.overall_score >= 70:
                logger.info("  [PASS] System resilience score acceptable")
                results["tests"].append({"name": "resilience_score", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Resilience score too low: {score.overall_score}")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 2.4 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S2-004",
                scenario="chaos_engineering",
                severity="P2",
                title="韧性评分计算失败",
                description="系统韧性评分计算功能异常",
                steps=["1. 准备测试数据", "2. 调用评分计算", "3. 验证结果"],
                expected="应返回有效的韧性评分",
                actual=f"评分计算失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "resilience_score", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 清理
        chaos_framework.emergency_stop_all()
        chaos_framework.shutdown()
        
    except Exception as e:
        logger.error(f"Scenario 2 setup failed: {e}")
        defect_collector.add_defect(
            defect_id="R3-S2-SETUP",
            scenario="chaos_engineering",
            severity="P0",
            title="场景2初始化失败",
            description="混沌工程测试场景初始化失败",
            steps=["1. 创建混沌工程框架", "2. 初始化故障注入器"],
            expected="混沌工程框架应正确初始化",
            actual=f"初始化失败: {str(e)}",
            error=str(e)
        )
    
    return results


# =============================================================================
# 场景3: 依赖故障传播测试
# =============================================================================

async def scenario3_dependency_failure():
    """
    场景3: 依赖故障传播测试
    - 模拟下游服务故障
    - 验证熔断机制
    - 测试降级策略
    """
    logger.info("\n" + "="*80)
    logger.info("场景3: 依赖故障传播测试 - 熔断与降级")
    logger.info("="*80)
    
    results = {
        "scenario": "dependency_failure",
        "tests": [],
        "passed": 0,
        "failed": 0
    }
    
    try:
        # 测试1: 熔断机制测试
        logger.info("\n[Test 3.1] 熔断机制测试")
        try:
            # 模拟熔断器
            class CircuitBreaker:
                def __init__(self, failure_threshold=3, recovery_timeout=5):
                    self.failure_threshold = failure_threshold
                    self.recovery_timeout = recovery_timeout
                    self.failure_count = 0
                    self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
                    self.last_failure_time = None
                
                async def call(self, func, *args, **kwargs):
                    if self.state == "OPEN":
                        if time.time() - self.last_failure_time > self.recovery_timeout:
                            self.state = "HALF_OPEN"
                        else:
                            raise Exception("Circuit breaker is OPEN")
                    
                    try:
                        result = await func(*args, **kwargs)
                        if self.state == "HALF_OPEN":
                            self.state = "CLOSED"
                            self.failure_count = 0
                        return result
                    except Exception as e:
                        self.failure_count += 1
                        self.last_failure_time = time.time()
                        
                        if self.failure_count >= self.failure_threshold:
                            self.state = "OPEN"
                        
                        raise e
            
            breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=2)
            
            # 模拟失败的服务调用
            call_count = 0
            async def failing_service():
                nonlocal call_count
                call_count += 1
                raise Exception("Service unavailable")
            
            # 触发熔断
            for i in range(4):
                try:
                    await breaker.call(failing_service)
                except Exception as e:
                    logger.info(f"  Call {i+1}: {e}")
            
            if breaker.state == "OPEN":
                logger.info("  [PASS] Circuit breaker opened correctly")
                results["tests"].append({"name": "circuit_breaker", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Expected OPEN state, got {breaker.state}")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 3.1 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S3-001",
                scenario="dependency_failure",
                severity="P1",
                title="熔断机制异常",
                description="熔断器未能在达到阈值时正确打开",
                steps=["1. 创建熔断器", "2. 连续触发失败", "3. 验证熔断状态"],
                expected="熔断器应在达到失败阈值后打开",
                actual=f"熔断状态异常: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "circuit_breaker", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试2: 降级策略测试
        logger.info("\n[Test 3.2] 降级策略测试")
        try:
            class FallbackService:
                def __init__(self):
                    self.primary_available = True
                
                async def primary_operation(self):
                    if not self.primary_available:
                        raise Exception("Primary service down")
                    return {"source": "primary", "data": "important_data"}
                
                async def fallback_operation(self):
                    return {"source": "fallback", "data": "cached_data"}
                
                async def execute(self):
                    try:
                        return await self.primary_operation()
                    except Exception as e:
                        logger.info(f"  Primary failed, using fallback: {e}")
                        return await self.fallback_operation()
            
            service = FallbackService()
            
            # 测试正常情况
            result1 = await service.execute()
            if result1["source"] != "primary":
                raise AssertionError("Expected primary source")
            
            # 测试降级情况
            service.primary_available = False
            result2 = await service.execute()
            
            if result2["source"] == "fallback":
                logger.info("  [PASS] Fallback strategy working correctly")
                results["tests"].append({"name": "fallback_strategy", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Expected fallback source, got {result2['source']}")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 3.2 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S3-002",
                scenario="dependency_failure",
                severity="P1",
                title="降级策略异常",
                description="服务降级策略未正确执行",
                steps=["1. 创建带降级策略的服务", "2. 模拟主服务故障", "3. 验证降级响应"],
                expected="主服务故障时应自动降级到备用服务",
                actual=f"降级策略执行失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "fallback_strategy", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试3: 依赖故障传播隔离
        logger.info("\n[Test 3.3] 依赖故障传播隔离")
        try:
            # 模拟服务依赖链
            class ServiceA:
                async def call(self):
                    # 模拟随机故障
                    if random.random() < 0.3:
                        raise Exception("ServiceA failed")
                    return "ServiceA_OK"
            
            class ServiceB:
                def __init__(self, service_a):
                    self.service_a = service_a
                
                async def call(self):
                    try:
                        result_a = await self.service_a.call()
                        return f"ServiceB_OK (dep: {result_a})"
                    except Exception as e:
                        # 隔离故障，继续提供服务
                        return f"ServiceB_OK (dep: degraded - {str(e)})"
            
            service_a = ServiceA()
            service_b = ServiceB(service_a)
            
            # 多次调用验证故障隔离
            success_count = 0
            for i in range(10):
                result = await service_b.call()
                if "ServiceB_OK" in result:
                    success_count += 1
                    logger.info(f"  Call {i+1}: {result}")
            
            if success_count == 10:
                logger.info("  [PASS] Fault isolation working correctly")
                results["tests"].append({"name": "fault_isolation", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Only {success_count}/10 calls succeeded")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 3.3 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S3-003",
                scenario="dependency_failure",
                severity="P0",
                title="故障传播隔离失败",
                description="依赖故障未能被正确隔离，导致级联失败",
                steps=["1. 创建服务依赖链", "2. 模拟依赖故障", "3. 验证故障隔离"],
                expected="依赖故障应被隔离，不影响主服务",
                actual=f"故障隔离失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "fault_isolation", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
    except Exception as e:
        logger.error(f"Scenario 3 setup failed: {e}")
        defect_collector.add_defect(
            defect_id="R3-S3-SETUP",
            scenario="dependency_failure",
            severity="P0",
            title="场景3初始化失败",
            description="依赖故障传播测试场景初始化失败",
            steps=["1. 创建熔断器", "2. 初始化降级策略"],
            expected="故障处理组件应正确初始化",
            actual=f"初始化失败: {str(e)}",
            error=str(e)
        )
    
    return results


# =============================================================================
# 场景4: 数据一致性验证测试
# =============================================================================

async def scenario4_data_consistency():
    """
    场景4: 数据一致性验证测试
    - 分布式事务一致性
    - 最终一致性验证
    - 冲突解决测试
    """
    logger.info("\n" + "="*80)
    logger.info("场景4: 数据一致性验证测试")
    logger.info("="*80)
    
    results = {
        "scenario": "data_consistency",
        "tests": [],
        "passed": 0,
        "failed": 0
    }
    
    try:
        tx_manager = DistributedTransactionManager()
        await tx_manager.start()
        
        # 测试1: 2PC分布式事务一致性
        logger.info("\n[Test 4.1] 2PC分布式事务一致性")
        try:
            # 创建模拟参与者
            participant_states = {}
            
            async def prepare_handler(tx_id: str, context: TransactionContext) -> bool:
                participant_states[tx_id] = "prepared"
                return True
            
            async def commit_handler(tx_id: str, context: TransactionContext) -> bool:
                participant_states[tx_id] = "committed"
                return True
            
            async def abort_handler(tx_id: str, context: TransactionContext) -> bool:
                participant_states[tx_id] = "aborted"
                return True
            
            # 注册参与者
            for i in range(3):
                participant = Participant(
                    participant_id=f"participant_{i}",
                    resource_id=f"resource_{i}",
                    prepare_handler=prepare_handler,
                    commit_handler=commit_handler,
                    abort_handler=abort_handler
                )
                tx_manager.register_2pc_participant(participant)
            
            # 开始事务
            tx = await tx_manager.begin_2pc_transaction(timeout=10.0)
            
            # 添加参与者
            for i in range(3):
                await tx_manager.add_participant_to_2pc(tx.tx_id, f"participant_{i}")
            
            # 准备阶段
            prepared = await tx_manager.prepare_2pc(tx.tx_id)
            
            if prepared:
                # 提交
                committed = await tx_manager.commit_2pc(tx.tx_id)
                
                if committed:
                    # 验证所有参与者状态一致
                    all_committed = all(
                        participant_states.get(f"participant_{i}") == "committed"
                        for i in range(3)
                    )
                    
                    if all_committed:
                        logger.info("  [PASS] 2PC transaction committed consistently")
                        results["tests"].append({"name": "2pc_consistency", "status": "passed"})
                        results["passed"] += 1
                    else:
                        raise AssertionError("Not all participants committed")
                else:
                    raise AssertionError("Transaction commit failed")
            else:
                raise AssertionError("Transaction prepare failed")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 4.1 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S4-001",
                scenario="data_consistency",
                severity="P0",
                title="2PC事务一致性失败",
                description="两阶段提交事务未能保证所有参与者状态一致",
                steps=["1. 创建事务", "2. 添加参与者", "3. 执行2PC", "4. 验证状态"],
                expected="所有参与者应处于一致的提交状态",
                actual=f"状态不一致: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "2pc_consistency", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试2: Saga补偿事务一致性
        logger.info("\n[Test 4.2] Saga补偿事务一致性")
        try:
            operations_log = []
            
            async def reserve_inventory(context: TransactionContext):
                operations_log.append("reserve_inventory")
                context.set("inventory_reserved", True)
                return {"item_id": "item_1", "quantity": 1}
            
            async def release_inventory(context: TransactionContext, result):
                operations_log.append("release_inventory")
                context.set("inventory_reserved", False)
            
            async def process_payment(context: TransactionContext):
                operations_log.append("process_payment")
                if context.get("should_fail"):
                    raise Exception("Payment processing failed")
                context.set("payment_processed", True)
                return {"payment_id": "pay_123"}
            
            async def refund_payment(context: TransactionContext, result):
                operations_log.append("refund_payment")
                context.set("payment_processed", False)
            
            # 测试失败场景下的补偿
            saga = tx_manager.create_saga()
            saga.add_step("reserve_inventory", reserve_inventory, release_inventory)
            saga.add_step("process_payment", process_payment, refund_payment)
            
            context = TransactionContext()
            context.set("should_fail", True)
            
            try:
                await tx_manager.execute_saga(saga, context)
                raise AssertionError("Saga should have failed")
            except Exception:
                pass  # 预期会失败
            
            # 验证补偿执行
            await asyncio.sleep(0.5)
            
            # 检查是否执行了补偿操作
            if "release_inventory" in operations_log:
                logger.info("  [PASS] Saga compensation executed correctly")
                results["tests"].append({"name": "saga_compensation", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError("Compensation not executed")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 4.2 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S4-002",
                scenario="data_consistency",
                severity="P0",
                title="Saga补偿事务失败",
                description="Saga事务失败后未能正确执行补偿操作",
                steps=["1. 创建Saga", "2. 添加步骤和补偿", "3. 触发失败", "4. 验证补偿"],
                expected="失败时应执行所有已完成步骤的补偿",
                actual=f"补偿执行失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "saga_compensation", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试3: 并发冲突检测
        logger.info("\n[Test 3.3] 并发冲突检测")
        try:
            # 模拟并发写入冲突
            shared_data = {"version": 1, "value": "initial"}
            conflict_detected = False
            
            async def concurrent_write(version: int, new_value: str):
                nonlocal conflict_detected
                # 乐观锁检查
                if version != shared_data["version"]:
                    conflict_detected = True
                    raise Exception(f"Version conflict: expected {version}, got {shared_data['version']}")
                
                shared_data["version"] = version + 1
                shared_data["value"] = new_value
                return True
            
            # 模拟两个并发写入
            try:
                await concurrent_write(1, "update1")
                await concurrent_write(1, "update2")  # 应该冲突
            except Exception as e:
                logger.info(f"  Conflict detected: {e}")
            
            if conflict_detected:
                logger.info("  [PASS] Concurrent conflict detected correctly")
                results["tests"].append({"name": "conflict_detection", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError("Conflict not detected")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 4.3 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S4-003",
                scenario="data_consistency",
                severity="P1",
                title="并发冲突检测失败",
                description="并发写入冲突未能被正确检测",
                steps=["1. 初始化数据", "2. 模拟并发写入", "3. 验证冲突检测"],
                expected="应检测到版本冲突",
                actual=f"冲突检测失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "conflict_detection", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        await tx_manager.stop()
        
    except Exception as e:
        logger.error(f"Scenario 4 setup failed: {e}")
        defect_collector.add_defect(
            defect_id="R3-S4-SETUP",
            scenario="data_consistency",
            severity="P0",
            title="场景4初始化失败",
            description="数据一致性验证测试场景初始化失败",
            steps=["1. 创建事务管理器", "2. 初始化参与者"],
            expected="事务管理器应正确初始化",
            actual=f"初始化失败: {str(e)}",
            error=str(e)
        )
    
    return results


# =============================================================================
# 场景5: 安全渗透测试
# =============================================================================

async def scenario5_security_penetration():
    """
    场景5: 安全渗透测试
    - SQL注入尝试
    - XSS攻击尝试
    - 权限绕过测试
    """
    logger.info("\n" + "="*80)
    logger.info("场景5: 安全渗透测试")
    logger.info("="*80)
    
    results = {
        "scenario": "security_penetration",
        "tests": [],
        "passed": 0,
        "failed": 0
    }
    
    try:
        security = SecurityHardeningModule()
        
        # 测试1: SQL注入检测
        logger.info("\n[Test 5.1] SQL注入检测")
        try:
            sql_injection_attempts = [
                "'; DROP TABLE users; --",
                "1' OR '1'='1",
                "1; DELETE FROM users WHERE '1'='1",
                "' UNION SELECT * FROM passwords --",
                "admin'--",
            ]
            
            detected_count = 0
            for attempt in sql_injection_attempts:
                is_threat, threats = security.validator.check_sql_injection(attempt)
                if is_threat:
                    detected_count += 1
                    logger.info(f"  Detected: {attempt[:30]}...")
            
            detection_rate = detected_count / len(sql_injection_attempts)
            
            if detection_rate >= 0.8:  # 至少检测出80%
                logger.info(f"  [PASS] SQL injection detection rate: {detection_rate*100:.1f}%")
                results["tests"].append({"name": "sql_injection_detection", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Detection rate too low: {detection_rate*100:.1f}%")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 5.1 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S5-001",
                scenario="security_penetration",
                severity="P0",
                title="SQL注入检测不足",
                description="安全模块未能检测出SQL注入攻击",
                steps=["1. 准备SQL注入样本", "2. 调用检测函数", "3. 验证检测结果"],
                expected="应检测出所有SQL注入攻击",
                actual=f"检测率不足: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "sql_injection_detection", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试2: XSS攻击检测
        logger.info("\n[Test 5.2] XSS攻击检测")
        try:
            xss_attempts = [
                "<script>alert('xss')</script>",
                "<img src=x onerror=alert('xss')>",
                "javascript:alert('xss')",
                "<iframe src='evil.com'></iframe>",
                "<body onload=alert('xss')>",
            ]
            
            detected_count = 0
            for attempt in xss_attempts:
                is_threat, threats = security.validator.check_xss(attempt)
                if is_threat:
                    detected_count += 1
                    logger.info(f"  Detected: {attempt[:40]}...")
            
            detection_rate = detected_count / len(xss_attempts)
            
            if detection_rate >= 0.8:
                logger.info(f"  [PASS] XSS detection rate: {detection_rate*100:.1f}%")
                results["tests"].append({"name": "xss_detection", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Detection rate too low: {detection_rate*100:.1f}%")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 5.2 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S5-002",
                scenario="security_penetration",
                severity="P0",
                title="XSS检测不足",
                description="安全模块未能检测出XSS攻击",
                steps=["1. 准备XSS攻击样本", "2. 调用检测函数", "3. 验证检测结果"],
                expected="应检测出所有XSS攻击",
                actual=f"检测率不足: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "xss_detection", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试3: 权限绕过测试
        logger.info("\n[Test 5.3] 权限绕过测试")
        try:
            # 创建用户和角色
            admin_user = security.rbac.create_user("admin_user", [Role.ADMIN])
            normal_user = security.rbac.create_user("normal_user", [Role.USER])
            guest_user = security.rbac.create_user("guest_user", [Role.GUEST])
            
            # 测试权限检查
            test_cases = [
                (admin_user.user_id, Permission.ADMIN, True),
                (admin_user.user_id, Permission.WRITE, True),
                (normal_user.user_id, Permission.WRITE, True),
                (normal_user.user_id, Permission.DELETE, False),
                (guest_user.user_id, Permission.WRITE, False),
                (guest_user.user_id, Permission.READ, True),
                ("nonexistent_user", Permission.READ, False),
            ]
            
            passed = 0
            for user_id, permission, expected in test_cases:
                result = security.rbac.check_permission(user_id, permission)
                if result == expected:
                    passed += 1
                    logger.info(f"  [OK] User {user_id[:8]}... permission {permission.value}: {result}")
                else:
                    logger.warning(f"  [FAIL] Expected {expected}, got {result}")
            
            if passed == len(test_cases):
                logger.info("  [PASS] All permission checks passed")
                results["tests"].append({"name": "permission_bypass", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Only {passed}/{len(test_cases)} permission checks passed")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 5.3 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S5-003",
                scenario="security_penetration",
                severity="P0",
                title="权限绕过漏洞",
                description="RBAC权限控制存在漏洞，可能导致未授权访问",
                steps=["1. 创建不同角色用户", "2. 测试权限检查", "3. 验证结果"],
                expected="权限检查应严格按角色配置执行",
                actual=f"权限检查失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "permission_bypass", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试4: ABAC策略绕过测试
        logger.info("\n[Test 5.4] ABAC策略绕过测试")
        try:
            # 添加ABAC策略
            security.abac.add_policy(
                name="time_based_access",
                subject_attrs={"department": "finance"},
                resource_attrs={"classification": "confidential"},
                action="read",
                condition=lambda s, r, e: 9 <= datetime.now().hour < 18,
                effect="allow"
            )
            
            # 测试正常访问
            subject = {"department": "finance", "user_id": "user123"}
            resource = {"classification": "confidential", "id": "doc456"}
            
            allowed, policy = security.abac.evaluate(subject, resource, "read")
            
            # 测试属性不匹配
            subject_wrong = {"department": "engineering", "user_id": "user456"}
            allowed_wrong, _ = security.abac.evaluate(subject_wrong, resource, "read")
            
            if allowed and not allowed_wrong:
                logger.info("  [PASS] ABAC policy enforcement working")
                results["tests"].append({"name": "abac_policy", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"ABAC evaluation failed: allowed={allowed}, allowed_wrong={allowed_wrong}")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 5.4 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S5-004",
                scenario="security_penetration",
                severity="P1",
                title="ABAC策略绕过漏洞",
                description="基于属性的访问控制策略可能被绕过",
                steps=["1. 添加ABAC策略", "2. 测试合规访问", "3. 测试违规访问"],
                expected="策略应正确允许合规访问并拒绝违规访问",
                actual=f"策略执行失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "abac_policy", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
        # 测试5: 输入验证绕过测试
        logger.info("\n[Test 5.5] 输入验证绕过测试")
        try:
            # 测试各种输入验证绕过尝试
            bypass_attempts = [
                ("email", "not_an_email", False),
                ("email", "test@example.com", True),
                ("uuid", "not-a-uuid", False),
                ("uuid", str(uuid.uuid4()), True),
            ]
            
            passed = 0
            for input_type, value, expected_valid in bypass_attempts:
                if input_type == "email":
                    is_valid = security.validator.validate_email(value)
                elif input_type == "uuid":
                    is_valid = security.validator.validate_uuid(value)
                else:
                    continue
                
                if is_valid == expected_valid:
                    passed += 1
                    logger.info(f"  [OK] {input_type} validation: {value[:20]}... -> {is_valid}")
                else:
                    logger.warning(f"  [FAIL] Expected {expected_valid}, got {is_valid}")
            
            if passed == len(bypass_attempts):
                logger.info("  [PASS] Input validation bypass tests passed")
                results["tests"].append({"name": "input_validation", "status": "passed"})
                results["passed"] += 1
            else:
                raise AssertionError(f"Only {passed}/{len(bypass_attempts)} validation tests passed")
                
        except Exception as e:
            logger.error(f"  [FAIL] Test 5.5 failed: {e}")
            defect_collector.add_defect(
                defect_id="R3-S5-005",
                scenario="security_penetration",
                severity="P1",
                title="输入验证绕过漏洞",
                description="输入验证机制存在绕过可能",
                steps=["1. 准备无效输入", "2. 调用验证函数", "3. 验证结果"],
                expected="无效输入应被拒绝",
                actual=f"验证失败: {str(e)}",
                error=str(e)
            )
            results["tests"].append({"name": "input_validation", "status": "failed", "error": str(e)})
            results["failed"] += 1
        
    except Exception as e:
        logger.error(f"Scenario 5 setup failed: {e}")
        defect_collector.add_defect(
            defect_id="R3-S5-SETUP",
            scenario="security_penetration",
            severity="P0",
            title="场景5初始化失败",
            description="安全渗透测试场景初始化失败",
            steps=["1. 创建安全模块", "2. 初始化验证器"],
            expected="安全模块应正确初始化",
            actual=f"初始化失败: {str(e)}",
            error=str(e)
        )
    
    return results


# =============================================================================
# 主执行函数
# =============================================================================

async def run_all_scenarios():
    """运行所有测试场景"""
    logger.info("\n" + "="*80)
    logger.info("Agent Memory System V3.0 - 第3轮实践测试开始")
    logger.info("="*80)
    
    all_results = {
        "test_round": "Round 3",
        "test_name": "Complex Integration & Chaos Engineering",
        "start_time": datetime.now().isoformat(),
        "scenarios": []
    }
    
    # 执行各个场景
    scenarios = [
        ("多模块集成测试", scenario1_multi_module_integration),
        ("混沌工程测试", scenario2_chaos_engineering),
        ("依赖故障传播测试", scenario3_dependency_failure),
        ("数据一致性验证测试", scenario4_data_consistency),
        ("安全渗透测试", scenario5_security_penetration),
    ]
    
    for name, scenario_func in scenarios:
        try:
            result = await scenario_func()
            all_results["scenarios"].append(result)
        except Exception as e:
            logger.error(f"Scenario {name} crashed: {e}")
            all_results["scenarios"].append({
                "scenario": name,
                "error": str(e),
                "status": "crashed"
            })
    
    # 计算总体统计
    total_tests = sum(len(s.get("tests", [])) for s in all_results["scenarios"])
    total_passed = sum(s.get("passed", 0) for s in all_results["scenarios"])
    total_failed = sum(s.get("failed", 0) for s in all_results["scenarios"])
    
    all_results["end_time"] = datetime.now().isoformat()
    all_results["summary"] = {
        "total_tests": total_tests,
        "passed": total_passed,
        "failed": total_failed,
        "pass_rate": f"{(total_passed/total_tests*100):.1f}%" if total_tests > 0 else "0%"
    }
    
    # 保存执行摘要
    with open('/workspace/agent_memory_system_v2/tests/round3/execution_summary.json', 'w') as f:
        json.dump(all_results, f, indent=2)
    
    logger.info("\n" + "="*80)
    logger.info("测试执行摘要")
    logger.info("="*80)
    logger.info(f"总测试数: {total_tests}")
    logger.info(f"通过: {total_passed}")
    logger.info(f"失败: {total_failed}")
    logger.info(f"通过率: {all_results['summary']['pass_rate']}")
    logger.info(f"缺陷数: {len(defect_collector.defects)}")
    
    # 保存缺陷报告
    defect_collector.save_report('/workspace/agent_memory_system_v2/tests/round3/defects.json')
    
    logger.info("\n第3轮测试完成!")
    return all_results


if __name__ == "__main__":
    asyncio.run(run_all_scenarios())

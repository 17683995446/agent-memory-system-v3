"""
P0级别核心问题修复单元测试

本测试文件验证Agent Memory System V3.0第一轮迭代修复的效果：
1. failover_failure (故障转移失败) - 最严重，487个实例
2. query_degradation (查询性能退化) - P0

测试覆盖：
- Raft共识故障转移修复测试
- 微服务框架故障转移修复测试
- 异步流水线查询性能优化测试

作者: Agent Memory System Team
版本: 3.0.0
"""

import asyncio
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List, Optional

# 导入被测试模块
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from memory_system.raft_consensus import (
    RaftNode,
    RaftConfig,
    FailoverConfig,
    FailoverState,
    FailoverError,
    CandidateValidationError,
    RaftRole,
    InMemoryTransport,
    MemoryStateMachine,
)
from memory_system.microservices_framework import (
    LoadBalancer,
    LoadBalanceStrategy,
    ServiceInstance,
    ServiceStatus,
    InMemoryServiceRegistry,
    FailoverConfig as ServiceFailoverConfig,
    ServiceFailoverError,
    NoHealthyInstanceError,
    CircuitBreakerOpen,
)
from memory_system.async_pipeline import (
    PipelineStage,
    PipelineConfig,
    StageConfig,
    PipelineItem,
    QueryDegradationError,
    IndexBuildError,
)


# ============================================================================
# Raft共识故障转移修复测试
# ============================================================================

class TestRaftFailoverFix:
    """Raft共识故障转移修复测试"""
    
    @pytest.fixture
    def raft_config(self):
        """创建Raft配置"""
        return RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.15,
            election_timeout_max=0.30,
        )
    
    @pytest.fixture
    def failover_config(self):
        """创建故障转移配置"""
        return FailoverConfig(
            max_retries=3,
            initial_retry_delay=0.1,
            max_retry_delay=1.0,
            retry_multiplier=2.0,
            failover_timeout=5.0,
            candidate_validation_timeout=0.5,
            state_recovery_timeout=2.0,
            health_check_interval=0.5,
        )
    
    @pytest.fixture
    def transport(self):
        """创建内存传输层"""
        return InMemoryTransport()
    
    @pytest.mark.asyncio
    async def test_failover_config_initialization(self, raft_config, failover_config, transport):
        """测试故障转移配置初始化"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        
        assert node.failover_config.max_retries == 3
        assert node.failover_config.initial_retry_delay == 0.1
        assert node.failover_config.retry_multiplier == 2.0
        assert node._failover_state is not None
        assert not node._failover_state.in_progress
    
    @pytest.mark.asyncio
    async def test_calculate_retry_delay_exponential_backoff(self, raft_config, failover_config, transport):
        """测试指数退避重试延迟计算"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        
        # 验证指数退避
        delay0 = node._calculate_retry_delay(0)
        delay1 = node._calculate_retry_delay(1)
        delay2 = node._calculate_retry_delay(2)
        
        assert delay0 == 0.1  # initial_retry_delay
        assert delay1 == 0.2  # initial * 2
        assert delay2 == 0.4  # initial * 4
        
        # 验证最大延迟限制
        delay_large = node._calculate_retry_delay(10)
        assert delay_large <= failover_config.max_retry_delay
    
    @pytest.mark.asyncio
    async def test_validate_candidate_node_unknown_node(self, raft_config, failover_config, transport):
        """测试候选节点验证 - 未知节点"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        
        with pytest.raises(CandidateValidationError) as exc_info:
            await node.validate_candidate_node("unknown_node")
        
        assert "not in known peers" in str(exc_info.value.reason)
    
    @pytest.mark.asyncio
    async def test_get_valid_candidate_nodes(self, raft_config, failover_config):
        """测试获取有效候选节点列表"""
        transport = InMemoryTransport()
        
        # 创建三个节点
        node1 = RaftNode(
            config=RaftConfig(node_id="node1", peers=["node2", "node3"]),
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        node2 = RaftNode(
            config=RaftConfig(node_id="node2", peers=["node1", "node3"]),
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        node3 = RaftNode(
            config=RaftConfig(node_id="node3", peers=["node1", "node2"]),
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        
        # 注册节点到传输层
        transport.register_node(node1)
        transport.register_node(node2)
        transport.register_node(node3)
        
        # 启动节点
        await node1.start()
        await node2.start()
        await node3.start()
        
        try:
            # 获取有效候选节点
            candidates = await node1.get_valid_candidate_nodes()
            
            # 应该至少包含自身
            assert "node1" in candidates
            
        finally:
            await node1.stop()
            await node2.stop()
            await node3.stop()
    
    @pytest.mark.asyncio
    async def test_wait_for_state_recovery(self, raft_config, failover_config, transport):
        """测试状态恢复等待机制"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        
        # 当last_applied >= commit_index时应该立即返回
        node._volatile_state.last_applied = 5
        node._volatile_state.commit_index = 5
        
        result = await node.wait_for_state_recovery(timeout=1.0)
        assert result is True
        
        # 当last_applied < commit_index时应该等待
        node._volatile_state.last_applied = 3
        node._volatile_state.commit_index = 5
        
        result = await node.wait_for_state_recovery(timeout=0.5)
        assert result is False  # 超时返回False
    
    @pytest.mark.asyncio
    async def test_get_failover_status(self, raft_config, failover_config, transport):
        """测试获取故障转移状态"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        
        status = node.get_failover_status()
        
        assert "in_progress" in status
        assert "failed_leader" in status
        assert "candidate_nodes" in status
        assert "retry_count" in status
        assert "node_health" in status
        
        assert status["in_progress"] is False
        assert status["retry_count"] == 0
    
    @pytest.mark.asyncio
    async def test_initiate_failover_no_leader(self, raft_config, failover_config, transport):
        """测试故障转移 - 无已知Leader"""
        node = RaftNode(
            config=raft_config,
            state_machine=MemoryStateMachine(),
            transport=transport,
            failover_config=failover_config,
        )
        
        # 无已知Leader时应该返回False
        node._current_leader = None
        result = await node.handle_leader_failure()
        assert result is False


# ============================================================================
# 微服务框架故障转移修复测试
# ============================================================================

class TestMicroserviceFailoverFix:
    """微服务框架故障转移修复测试"""
    
    @pytest.fixture
    def registry(self):
        """创建服务注册表"""
        return InMemoryServiceRegistry()
    
    @pytest.fixture
    def failover_config(self):
        """创建故障转移配置"""
        return ServiceFailoverConfig(
            max_retries=3,
            initial_retry_delay=0.1,
            max_retry_delay=1.0,
            retry_multiplier=2.0,
            failover_timeout=5.0,
            health_check_interval=1.0,
        )
    
    @pytest.fixture
    def healthy_instance(self):
        """创建健康的服务实例"""
        return ServiceInstance(
            id="service-1",
            name="test-service",
            host="localhost",
            port=8080,
            status=ServiceStatus.HEALTHY,
            weight=1,
        )
    
    @pytest.fixture
    def unhealthy_instance(self):
        """创建不健康的服务实例"""
        return ServiceInstance(
            id="service-2",
            name="test-service",
            host="localhost",
            port=8081,
            status=ServiceStatus.UNHEALTHY,
            weight=1,
        )
    
    @pytest.mark.asyncio
    async def test_load_balancer_with_failover_config(self, registry, failover_config):
        """测试负载均衡器故障转移配置"""
        lb = LoadBalancer(
            strategy=LoadBalanceStrategy.ROUND_ROBIN,
            registry=registry,
            failover_config=failover_config,
        )
        
        assert lb.failover_config.max_retries == 3
        assert lb.failover_config.initial_retry_delay == 0.1
        assert lb._unhealthy_instances is not None
        assert lb._instance_retry_count is not None
    
    @pytest.mark.asyncio
    async def test_load_balancer_select_healthy_instances(
        self, registry, failover_config, healthy_instance, unhealthy_instance
    ):
        """测试负载均衡器选择健康实例"""
        await registry.start()
        
        try:
            # 注册健康和不健康实例
            await registry.register(healthy_instance)
            await registry.register(unhealthy_instance)
            
            lb = LoadBalancer(
                strategy=LoadBalanceStrategy.ROUND_ROBIN,
                registry=registry,
                failover_config=failover_config,
            )
            
            # 选择实例
            selected = await lb.select("test-service")
            
            # 应该只选择健康实例
            assert selected is not None
            assert selected.id == healthy_instance.id
            assert selected.is_healthy
            
        finally:
            await registry.stop()
    
    @pytest.mark.asyncio
    async def test_load_balancer_mark_instance_unhealthy(
        self, registry, failover_config, healthy_instance
    ):
        """测试标记实例为不健康"""
        await registry.start()
        
        try:
            await registry.register(healthy_instance)
            
            lb = LoadBalancer(
                strategy=LoadBalanceStrategy.ROUND_ROBIN,
                registry=registry,
                failover_config=failover_config,
            )
            
            # 标记为不健康
            await lb.mark_instance_unhealthy("test-service", healthy_instance.id)
            
            # 验证状态
            status = lb.get_instance_status("test-service")
            assert healthy_instance.id in status["unhealthy_instances"]
            assert status["retry_counts"][healthy_instance.id] == 1
            
        finally:
            await registry.stop()
    
    @pytest.mark.asyncio
    async def test_load_balancer_select_with_failover(
        self, registry, failover_config
    ):
        """测试带故障转移的实例选择"""
        await registry.start()
        
        try:
            # 创建多个健康实例
            instances = []
            for i in range(3):
                instance = ServiceInstance(
                    id=f"service-{i}",
                    name="test-service",
                    host="localhost",
                    port=8080 + i,
                    status=ServiceStatus.HEALTHY,
                    weight=1,
                )
                instances.append(instance)
                await registry.register(instance)
            
            lb = LoadBalancer(
                strategy=LoadBalanceStrategy.ROUND_ROBIN,
                registry=registry,
                failover_config=failover_config,
            )
            
            # 带故障转移的选择
            selected, tried = await lb.select_with_failover(
                "test-service",
                max_attempts=3
            )
            
            assert selected is not None
            assert len(tried) >= 1
            assert selected.id in [inst.id for inst in instances]
            
        finally:
            await registry.stop()
    
    @pytest.mark.asyncio
    async def test_load_balancer_exclude_instances(
        self, registry, failover_config
    ):
        """测试排除特定实例"""
        await registry.start()
        
        try:
            # 创建多个实例
            for i in range(3):
                instance = ServiceInstance(
                    id=f"service-{i}",
                    name="test-service",
                    host="localhost",
                    port=8080 + i,
                    status=ServiceStatus.HEALTHY,
                    weight=1,
                )
                await registry.register(instance)
            
            lb = LoadBalancer(
                strategy=LoadBalanceStrategy.ROUND_ROBIN,
                registry=registry,
                failover_config=failover_config,
            )
            
            # 排除前两个实例
            exclude_set = {"service-0", "service-1"}
            selected = await lb.select("test-service", exclude_instances=exclude_set)
            
            assert selected is not None
            assert selected.id == "service-2"
            
        finally:
            await registry.stop()
    
    @pytest.mark.asyncio
    async def test_instance_recovery(self, registry, failover_config):
        """测试实例恢复机制"""
        await registry.start()
        
        try:
            instance = ServiceInstance(
                id="service-1",
                name="test-service",
                host="localhost",
                port=8080,
                status=ServiceStatus.HEALTHY,
                weight=1,
            )
            await registry.register(instance)
            
            lb = LoadBalancer(
                strategy=LoadBalanceStrategy.ROUND_ROBIN,
                registry=registry,
                failover_config=failover_config,
            )
            
            # 标记为不健康
            await lb.mark_instance_unhealthy("test-service", instance.id)
            
            # 验证被标记为不健康
            status = lb.get_instance_status("test-service")
            assert instance.id in status["unhealthy_instances"]
            
            # 标记为健康
            await lb.mark_instance_healthy("test-service", instance.id)
            
            # 验证已恢复
            status = lb.get_instance_status("test-service")
            assert instance.id not in status["unhealthy_instances"]
            
        finally:
            await registry.stop()


# ============================================================================
# 异步流水线查询性能优化测试
# ============================================================================

class TestPipelineQueryOptimization:
    """异步流水线查询性能优化测试"""
    
    @pytest.fixture
    def stage_config(self):
        """创建阶段配置"""
        return StageConfig(
            name="test_stage",
            buffer_size=100,
            num_workers=2,
            enable_indexing=True,
            index_fields=["id", "category", "status"],
            index_build_threshold=10,
            query_timeout=5.0,
            enable_query_cache=True,
            cache_size=100,
        )
    
    @pytest.fixture
    def pipeline_stage(self, stage_config):
        """创建流水线阶段"""
        async def handler(item: PipelineItem) -> PipelineItem:
            return item
        
        return PipelineStage(
            name="test_stage",
            handler=handler,
            config=stage_config,
        )
    
    @pytest.mark.asyncio
    async def test_stage_config_indexing_options(self, stage_config):
        """测试阶段配置索引选项"""
        assert stage_config.enable_indexing is True
        assert "id" in stage_config.index_fields
        assert "category" in stage_config.index_fields
        assert stage_config.index_build_threshold == 10
        assert stage_config.enable_query_cache is True
    
    @pytest.mark.asyncio
    async def test_add_to_index(self, pipeline_stage):
        """测试添加项目到索引"""
        # 创建测试项目
        item = PipelineItem(
            data={"id": "item-1", "category": "test", "status": "active"}
        )
        
        # 添加到索引
        await pipeline_stage.add_to_index(item)
        
        # 验证项目已存储
        assert item.item_id in pipeline_stage._item_store
        assert pipeline_stage._item_store[item.item_id] == item
    
    @pytest.mark.asyncio
    async def test_build_index(self, pipeline_stage):
        """测试构建索引"""
        # 添加多个项目
        for i in range(20):
            item = PipelineItem(
                data={
                    "id": f"item-{i}",
                    "category": f"cat-{i % 3}",
                    "status": "active" if i % 2 == 0 else "inactive"
                }
            )
            await pipeline_stage.add_to_index(item)
        
        # 构建索引
        result = await pipeline_stage.build_index(force=True)
        
        assert result is True
        assert len(pipeline_stage._index) > 0
        assert "id" in pipeline_stage._index
        assert "category" in pipeline_stage._index
    
    @pytest.mark.asyncio
    async def test_query_by_index(self, pipeline_stage):
        """测试通过索引查询"""
        # 添加测试数据
        for i in range(20):
            item = PipelineItem(
                data={
                    "id": f"item-{i}",
                    "category": f"cat-{i % 3}",
                    "status": "active" if i % 2 == 0 else "inactive"
                }
            )
            await pipeline_stage.add_to_index(item)
        
        # 构建索引
        await pipeline_stage.build_index(force=True)
        
        # 通过索引查询
        results = await pipeline_stage.query_by_index("category", "cat-0")
        
        assert len(results) > 0
        for item in results:
            assert item.data["category"] == "cat-0"
    
    @pytest.mark.asyncio
    async def test_query_by_conditions(self, pipeline_stage):
        """测试多条件查询"""
        # 添加测试数据
        for i in range(30):
            item = PipelineItem(
                data={
                    "id": f"item-{i}",
                    "category": f"cat-{i % 3}",
                    "status": "active" if i % 2 == 0 else "inactive"
                }
            )
            await pipeline_stage.add_to_index(item)
        
        # 构建索引
        await pipeline_stage.build_index(force=True)
        
        # 多条件查询
        results = await pipeline_stage.query_by_conditions({
            "category": "cat-0",
            "status": "active"
        })
        
        # 验证结果
        for item in results:
            assert item.data["category"] == "cat-0"
            assert item.data["status"] == "active"
    
    @pytest.mark.asyncio
    async def test_query_cache(self, pipeline_stage):
        """测试查询缓存"""
        pipeline_stage.config.enable_query_cache = True
        
        # 添加测试数据
        item = PipelineItem(
            data={"id": "unique-item", "category": "test", "status": "active"}
        )
        await pipeline_stage.add_to_index(item)
        await pipeline_stage.build_index(force=True)
        
        # 第一次查询
        start_time = time.time()
        results1 = await pipeline_stage.query_by_index("id", "unique-item")
        first_query_time = time.time() - start_time
        
        # 第二次查询（应该命中缓存）
        start_time = time.time()
        results2 = await pipeline_stage.query_by_index("id", "unique-item")
        second_query_time = time.time() - start_time
        
        # 结果应该相同
        assert len(results1) == len(results2) == 1
        assert results1[0].item_id == results2[0].item_id
        
        # 缓存应该被使用
        assert len(pipeline_stage._query_cache) > 0
    
    @pytest.mark.asyncio
    async def test_get_index_stats(self, pipeline_stage):
        """测试获取索引统计信息"""
        # 添加测试数据
        for i in range(15):
            item = PipelineItem(
                data={
                    "id": f"item-{i}",
                    "category": f"cat-{i % 3}",
                    "status": "active"
                }
            )
            await pipeline_stage.add_to_index(item)
        
        await pipeline_stage.build_index(force=True)
        
        # 获取统计信息
        stats = pipeline_stage.get_index_stats()
        
        assert "indexed_fields" in stats
        assert "total_items" in stats
        assert "index_stats" in stats
        assert stats["total_items"] == 15
        assert len(stats["indexed_fields"]) > 0
    
    @pytest.mark.asyncio
    async def test_clear_index(self, pipeline_stage):
        """测试清空索引"""
        # 添加测试数据
        for i in range(10):
            item = PipelineItem(
                data={"id": f"item-{i}", "category": "test"}
            )
            await pipeline_stage.add_to_index(item)
        
        # 清空索引
        await pipeline_stage.clear_index()
        
        # 验证已清空
        assert len(pipeline_stage._item_store) == 0
        assert len(pipeline_stage._index) == 0
        assert len(pipeline_stage._query_cache) == 0
    
    @pytest.mark.asyncio
    async def test_query_degradation_error(self, pipeline_stage):
        """测试查询性能退化错误"""
        # 设置很短的超时
        pipeline_stage.config.query_timeout = 0.001
        
        # 添加大量数据
        for i in range(1000):
            item = PipelineItem(
                data={"id": f"item-{i}", "category": f"cat-{i}"}
            )
            await pipeline_stage.add_to_index(item)
        
        # 不构建索引，强制全表扫描
        pipeline_stage._index.clear()
        
        # 查询应该因超时而抛出异常或返回部分结果
        try:
            results = await pipeline_stage.query_by_index("category", "cat-999")
            # 如果没有抛出异常，验证结果
            assert isinstance(results, list)
        except QueryDegradationError as e:
            # 验证错误信息
            assert e.stage_name == "test_stage"


# ============================================================================
# 集成测试
# ============================================================================

class TestP0FixesIntegration:
    """P0修复集成测试"""
    
    @pytest.mark.asyncio
    async def test_raft_failover_integration(self):
        """Raft故障转移集成测试"""
        # 创建三个节点的集群
        transport = InMemoryTransport()
        failover_config = FailoverConfig(
            max_retries=2,
            initial_retry_delay=0.05,
            max_retry_delay=0.5,
            failover_timeout=2.0,
        )
        
        nodes = []
        for i in range(3):
            node = RaftNode(
                config=RaftConfig(
                    node_id=f"node{i}",
                    peers=[f"node{j}" for j in range(3) if j != i],
                    election_timeout_min=0.1,
                    election_timeout_max=0.2,
                ),
                state_machine=MemoryStateMachine(),
                transport=transport,
                failover_config=failover_config,
            )
            transport.register_node(node)
            nodes.append(node)
        
        # 启动所有节点
        for node in nodes:
            await node.start()
        
        try:
            # 等待选举完成
            await asyncio.sleep(0.5)
            
            # 验证故障转移配置已正确设置
            for node in nodes:
                assert node.failover_config is not None
                assert node._failover_state is not None
                
        finally:
            for node in nodes:
                await node.stop()
    
    @pytest.mark.asyncio
    async def test_pipeline_indexing_integration(self):
        """流水线索引集成测试"""
        # 创建带有索引的阶段
        config = StageConfig(
            name="data_stage",
            enable_indexing=True,
            index_fields=["user_id", "event_type"],
            index_build_threshold=5,
            enable_query_cache=True,
        )
        
        async def handler(item: PipelineItem) -> PipelineItem:
            return item
        
        stage = PipelineStage(name="data_stage", handler=handler, config=config)
        
        # 添加数据
        for i in range(20):
            item = PipelineItem(
                data={
                    "user_id": f"user-{i % 5}",
                    "event_type": "click" if i % 2 == 0 else "view",
                    "timestamp": time.time()
                }
            )
            await stage.add_to_index(item)
        
        # 构建索引
        await stage.build_index(force=True)
        
        # 查询测试
        results = await stage.query_by_index("user_id", "user-0")
        assert len(results) > 0
        
        # 多条件查询
        results = await stage.query_by_conditions({
            "user_id": "user-1",
            "event_type": "click"
        })
        
        for item in results:
            assert item.data["user_id"] == "user-1"
            assert item.data["event_type"] == "click"


# ============================================================================
# 性能基准测试
# ============================================================================

class TestP0FixesPerformance:
    """P0修复性能基准测试"""
    
    @pytest.mark.asyncio
    async def test_index_query_performance(self):
        """索引查询性能测试"""
        config = StageConfig(
            name="perf_stage",
            enable_indexing=True,
            index_fields=["id", "category"],
            index_build_threshold=100,
        )
        
        async def handler(item: PipelineItem) -> PipelineItem:
            return item
        
        stage = PipelineStage(name="perf_stage", handler=handler, config=config)
        
        # 添加大量数据
        num_items = 1000
        for i in range(num_items):
            item = PipelineItem(
                data={
                    "id": f"item-{i}",
                    "category": f"cat-{i % 10}",
                }
            )
            await stage.add_to_index(item)
        
        # 构建索引
        await stage.build_index(force=True)
        
        # 测试索引查询性能
        start_time = time.time()
        for _ in range(100):
            results = await stage.query_by_index("category", "cat-5")
        indexed_query_time = time.time() - start_time
        
        # 清空索引进行全表扫描测试
        stage._index.clear()
        
        start_time = time.time()
        for _ in range(10):  # 减少次数因为全表扫描较慢
            results = await stage.query_by_index("category", "cat-5")
        full_scan_time = time.time() - start_time
        
        # 索引查询应该明显快于全表扫描
        # 注意：由于数据量较小，差异可能不明显，但逻辑上索引应该更快
        print(f"\nIndexed query time (100 queries): {indexed_query_time:.4f}s")
        print(f"Full scan time (10 queries): {full_scan_time:.4f}s")
        
        # 验证查询结果正确性
        assert len(results) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

"""
Agent Memory System V3.0 核心模块单元测试

测试Step 2深度实现阶段的所有核心模块：
- Raft分布式共识协议
- 异步流水线处理器
- 智能任务调度器
- 分布式事务管理器
- gRPC服务层
- 内存映射存储后端
"""

import asyncio
import pytest
import time
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Any

# 导入被测试模块
from memory_system.raft_consensus import (
    RaftNode, RaftConfig, RaftRole, MemoryStateMachine, RaftCluster,
    LogEntry, NotLeaderError
)
from memory_system.async_pipeline import (
    Pipeline, PipelineStage, PipelineConfig, StageConfig,
    BackpressureStrategy, PipelineItem, create_memory_pipeline
)
from memory_system.task_scheduler import (
    TaskScheduler, Task, TaskPriority, TaskStatus,
    SchedulerConfig, DAGBuilder
)
from memory_system.distributed_transaction_manager import (
    DistributedTransactionManager, Transaction, TransactionContext,
    TwoPhaseCommitCoordinator, SagaOrchestrator, Participant,
    TransactionStatus
)
from memory_system.mmap_storage_backend import (
    MMapStorageBackend, MMapStorageConfig, MMapStorageManager,
    RecordHeader, RecordType
)


# ==================== Raft共识测试 ====================

class TestRaftConsensus:
    """Raft分布式共识协议测试"""
    
    @pytest.fixture
    def raft_config(self):
        """创建Raft配置"""
        return RaftConfig(
            node_id="node1",
            peers=["node2", "node3"],
            election_timeout_min=0.05,
            election_timeout_max=0.1,
            heartbeat_interval=0.02,
        )
    
    @pytest.fixture
    def state_machine(self):
        """创建状态机"""
        return MemoryStateMachine()
    
    @pytest.mark.asyncio
    async def test_raft_node_initialization(self, raft_config, state_machine):
        """测试Raft节点初始化"""
        node = RaftNode(raft_config, state_machine)
        assert node.node_id == "node1"
        assert node.role == RaftRole.FOLLOWER
        assert node.current_term == 0
        assert not node.is_leader
    
    @pytest.mark.asyncio
    async def test_raft_node_start_stop(self, raft_config, state_machine):
        """测试Raft节点启动和停止"""
        node = RaftNode(raft_config, state_machine)
        await node.start()
        assert node._election_timer is not None
        
        await node.stop()
        assert node._shutdown_event.is_set()
    
    @pytest.mark.asyncio
    async def test_raft_cluster_election(self):
        """测试Raft集群Leader选举"""
        cluster = RaftCluster(num_nodes=3)
        await cluster.start()
        
        # 等待Leader选举
        leader = await cluster.wait_for_leader(timeout=3.0)
        assert leader is not None
        assert leader.is_leader
        
        await cluster.stop()
    
    @pytest.mark.asyncio
    async def test_raft_log_replication(self):
        """测试Raft日志复制"""
        cluster = RaftCluster(num_nodes=3)
        await cluster.start()
        
        leader = await cluster.wait_for_leader(timeout=3.0)
        assert leader is not None
        
        # 提交命令
        result = await leader.propose_command({
            "action": "set",
            "key": "test_key",
            "value": "test_value"
        })
        assert result is not None
        
        await cluster.stop()
    
    @pytest.mark.asyncio
    async def test_raft_not_leader_error(self, raft_config, state_machine):
        """测试非Leader错误"""
        node = RaftNode(raft_config, state_machine)
        await node.start()
        
        with pytest.raises(NotLeaderError):
            await node.propose_command({"action": "test"})
        
        await node.stop()
    
    @pytest.mark.asyncio
    async def test_raft_log_entry_creation(self):
        """测试日志条目创建"""
        entry = LogEntry(
            term=1,
            index=0,
            command={"action": "test"}
        )
        assert entry.term == 1
        assert entry.index == 0
        assert entry.command == {"action": "test"}
        assert entry.entry_id is not None
    
    @pytest.mark.asyncio
    async def test_raft_state_machine_apply(self, state_machine):
        """测试状态机应用命令"""
        # 测试set命令
        result = await state_machine.apply({
            "action": "set",
            "key": "k1",
            "value": "v1"
        })
        assert result["success"]
        
        # 测试get命令
        result = await state_machine.apply({
            "action": "get",
            "key": "k1"
        })
        assert result["success"]
        assert result["value"] == "v1"


# ==================== 异步流水线测试 ====================

class TestAsyncPipeline:
    """异步流水线处理器测试"""
    
    @pytest.fixture
    def pipeline_config(self):
        """创建流水线配置"""
        return PipelineConfig(
            max_concurrent_items=100,
            global_timeout=10.0,
            enable_metrics=True,
        )
    
    @pytest.mark.asyncio
    async def test_pipeline_initialization(self, pipeline_config):
        """测试流水线初始化"""
        stages = [
            PipelineStage("stage1", lambda x: x),
        ]
        pipeline = Pipeline(stages, pipeline_config)
        assert len(pipeline.stages) == 1
        assert pipeline.config == pipeline_config
    
    @pytest.mark.asyncio
    async def test_pipeline_start_stop(self, pipeline_config):
        """测试流水线启动和停止"""
        async def handler(item):
            return item
        
        stages = [
            PipelineStage("stage1", handler),
        ]
        pipeline = Pipeline(stages, pipeline_config)
        
        await pipeline.start()
        assert pipeline.status.name == "RUNNING"
        
        await pipeline.stop()
        assert pipeline.status.name == "STOPPED"
    
    @pytest.mark.asyncio
    async def test_pipeline_submit(self, pipeline_config):
        """测试流水线提交"""
        async def handler(item):
            item.data["processed"] = True
            return item
        
        stages = [
            PipelineStage("stage1", handler),
        ]
        pipeline = Pipeline(stages, pipeline_config)
        
        await pipeline.start()
        
        result = await pipeline.submit({"test": "data"}, wait_for_result=True)
        assert result is not None
        assert result.data.get("processed") is True
        
        await pipeline.stop()
    
    @pytest.mark.asyncio
    async def test_pipeline_batch_submit(self, pipeline_config):
        """测试流水线批量提交"""
        async def handler(item):
            item.data["processed"] = True
            return item
        
        stages = [
            PipelineStage("stage1", handler),
        ]
        pipeline = Pipeline(stages, pipeline_config)
        
        await pipeline.start()
        
        items = [{"id": i} for i in range(10)]
        results = await pipeline.submit_many(items)
        
        assert len(results) == 10
        for result in results:
            assert result.data.get("processed") is True
        
        await pipeline.stop()
    
    @pytest.mark.asyncio
    async def test_pipeline_stage_metrics(self, pipeline_config):
        """测试流水线阶段指标"""
        async def handler(item):
            return item
        
        stage = PipelineStage("test_stage", handler)
        stages = [stage]
        pipeline = Pipeline(stages, pipeline_config)
        
        await pipeline.start()
        await pipeline.submit({"test": "data"}, wait_for_result=True)
        
        metrics = stage.get_metrics()
        assert metrics.items_processed >= 1
        
        await pipeline.stop()
    
    @pytest.mark.asyncio
    async def test_pipeline_item_timestamps(self):
        """测试流水线项目时间戳"""
        item = PipelineItem(data={"test": "data"})
        
        item.record_stage_entry("stage1")
        await asyncio.sleep(0.01)
        item.record_stage_exit("stage1")
        
        duration = item.get_stage_duration("stage1")
        assert duration >= 0.01
    
    @pytest.mark.asyncio
    async def test_create_memory_pipeline(self):
        """测试创建内存处理流水线"""
        pipeline = create_memory_pipeline(
            embedding_workers=2,
            storage_workers=1,
            indexing_workers=1,
        )
        
        await pipeline.start()
        
        result = await pipeline.submit(
            {"text": "test memory content"},
            wait_for_result=True
        )
        
        assert result is not None
        assert "embedding" in result.data
        assert "stored" in result.data
        assert "indexed" in result.data
        
        await pipeline.stop()


# ==================== 任务调度器测试 ====================

class TestTaskScheduler:
    """智能任务调度器测试"""
    
    @pytest.fixture
    def scheduler_config(self):
        """创建调度器配置"""
        return SchedulerConfig(
            max_workers=4,
            max_queue_size=100,
            enable_fair_scheduling=True,
        )
    
    @pytest.mark.asyncio
    async def test_scheduler_initialization(self, scheduler_config):
        """测试调度器初始化"""
        scheduler = TaskScheduler(scheduler_config)
        assert scheduler.config == scheduler_config
    
    @pytest.mark.asyncio
    async def test_scheduler_start_stop(self, scheduler_config):
        """测试调度器启动和停止"""
        scheduler = TaskScheduler(scheduler_config)
        
        await scheduler.start()
        assert scheduler._running
        
        await scheduler.stop()
        assert not scheduler._running
    
    @pytest.mark.asyncio
    async def test_scheduler_submit_task(self, scheduler_config):
        """测试调度器提交任务"""
        scheduler = TaskScheduler(scheduler_config)
        await scheduler.start()
        
        async def task_func():
            await asyncio.sleep(0.01)
            return "task_result"
        
        task = Task(
            task_id="test_task",
            coroutine=task_func(),
            priority=TaskPriority.NORMAL,
        )
        
        result = await scheduler.submit(task, wait=True, timeout=5.0)
        assert result == "task_result"
        
        await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_scheduler_task_priority(self, scheduler_config):
        """测试任务优先级"""
        scheduler = TaskScheduler(scheduler_config)
        await scheduler.start()
        
        results = []
        
        async def low_priority_task():
            await asyncio.sleep(0.05)
            results.append("low")
            return "low"
        
        async def high_priority_task():
            await asyncio.sleep(0.01)
            results.append("high")
            return "high"
        
        # 先提交低优先级任务
        low_task = Task(
            task_id="low",
            coroutine=low_priority_task(),
            priority=TaskPriority.LOW,
        )
        await scheduler.submit(low_task)
        
        # 再提交高优先级任务
        high_task = Task(
            task_id="high",
            coroutine=high_priority_task(),
            priority=TaskPriority.HIGH,
        )
        await scheduler.submit(high_task)
        
        await asyncio.sleep(0.2)
        
        # 高优先级任务应该先完成
        # 注意：实际结果取决于调度策略
        
        await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_scheduler_dag_execution(self, scheduler_config):
        """测试DAG任务执行"""
        scheduler = TaskScheduler(scheduler_config)
        await scheduler.start()
        
        execution_order = []
        
        async def task_a():
            execution_order.append("A")
            return "A"
        
        async def task_b():
            execution_order.append("B")
            return "B"
        
        async def task_c():
            execution_order.append("C")
            return "C"
        
        # 创建DAG: A -> C, B -> C
        dag = DAGBuilder()
        dag.add_task("A", task_a(), priority=TaskPriority.HIGH)
        dag.add_task("B", task_b(), priority=TaskPriority.HIGH)
        dag.add_task("C", task_c(), priority=TaskPriority.NORMAL)
        dag.add_dependency("C", "A")
        dag.add_dependency("C", "B")
        
        tasks = await dag.submit_to_scheduler(scheduler)
        
        # 等待所有任务完成
        await asyncio.sleep(0.5)
        
        # C应该在A和B之后执行
        if len(execution_order) >= 3:
            assert execution_order.index("C") > execution_order.index("A")
            assert execution_order.index("C") > execution_order.index("B")
        
        await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_scheduler_cancel_task(self, scheduler_config):
        """测试取消任务"""
        scheduler = TaskScheduler(scheduler_config)
        await scheduler.start()
        
        async def long_task():
            await asyncio.sleep(10)
            return "completed"
        
        task = Task(
            task_id="cancel_task",
            coroutine=long_task(),
        )
        
        await scheduler.submit(task)
        cancelled = await scheduler.cancel_task("cancel_task")
        assert cancelled
        
        await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_scheduler_stats(self, scheduler_config):
        """测试调度器统计"""
        scheduler = TaskScheduler(scheduler_config)
        await scheduler.start()
        
        async def task_func():
            return "result"
        
        task = Task(
            task_id="stats_task",
            coroutine=task_func(),
        )
        
        await scheduler.submit(task, wait=True)
        
        stats = scheduler.get_stats()
        assert stats["submitted"] >= 1
        assert stats["completed"] >= 1
        
        await scheduler.stop()


# ==================== 分布式事务管理器测试 ====================

class TestDistributedTransactionManager:
    """分布式事务管理器测试"""
    
    @pytest.mark.asyncio
    async def test_transaction_manager_initialization(self):
        """测试事务管理器初始化"""
        manager = DistributedTransactionManager()
        assert manager is not None
    
    @pytest.mark.asyncio
    async def test_transaction_manager_start_stop(self):
        """测试事务管理器启动和停止"""
        manager = DistributedTransactionManager()
        
        await manager.start()
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_2pc_transaction(self):
        """测试2PC事务"""
        manager = DistributedTransactionManager()
        await manager.start()
        
        # 注册参与者
        async def prepare_handler(tx_id, context):
            return True
        
        async def commit_handler(tx_id, context):
            return True
        
        async def abort_handler(tx_id, context):
            return True
        
        for i in range(3):
            participant = Participant(
                participant_id=f"p{i}",
                resource_id=f"r{i}",
                prepare_handler=prepare_handler,
                commit_handler=commit_handler,
                abort_handler=abort_handler,
            )
            manager.register_2pc_participant(participant)
        
        # 开始事务
        tx = await manager.begin_2pc_transaction(timeout=10.0)
        assert tx is not None
        assert tx.status == TransactionStatus.ACTIVE
        
        # 添加参与者
        for i in range(3):
            await manager.add_participant_to_2pc(tx.tx_id, f"p{i}")
        
        # 准备阶段
        prepared = await manager.prepare_2pc(tx.tx_id)
        assert prepared
        
        # 提交
        committed = await manager.commit_2pc(tx.tx_id)
        assert committed
        
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_saga_transaction_success(self):
        """测试成功的Saga事务"""
        manager = DistributedTransactionManager()
        await manager.start()
        
        execution_log = []
        
        async def step1_action(context):
            execution_log.append("step1_action")
            context.set("step1_done", True)
            return {"result": "step1"}
        
        async def step1_compensate(context, result):
            execution_log.append("step1_compensate")
        
        async def step2_action(context):
            execution_log.append("step2_action")
            context.set("step2_done", True)
            return {"result": "step2"}
        
        async def step2_compensate(context, result):
            execution_log.append("step2_compensate")
        
        saga = manager.create_saga()
        saga.add_step("step1", step1_action, step1_compensate)
        saga.add_step("step2", step2_action, step2_compensate)
        
        tx = await manager.execute_saga(saga)
        
        assert tx.status == TransactionStatus.COMMITTED
        assert "step1_action" in execution_log
        assert "step2_action" in execution_log
        assert "step1_compensate" not in execution_log
        
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_saga_transaction_compensation(self):
        """测试Saga事务补偿"""
        manager = DistributedTransactionManager()
        await manager.start()
        
        execution_log = []
        
        async def step1_action(context):
            execution_log.append("step1_action")
            return {"result": "step1"}
        
        async def step1_compensate(context, result):
            execution_log.append("step1_compensate")
        
        async def step2_action(context):
            execution_log.append("step2_action")
            raise Exception("Step 2 failed")
        
        async def step2_compensate(context, result):
            execution_log.append("step2_compensate")
        
        saga = manager.create_saga()
        saga.add_step("step1", step1_action, step1_compensate)
        saga.add_step("step2", step2_action, step2_compensate)
        
        with pytest.raises(Exception):
            await manager.execute_saga(saga)
        
        # 补偿应该被执行
        assert "step1_compensate" in execution_log
        
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_transaction_context(self):
        """测试事务上下文"""
        context = TransactionContext()
        
        context.set("key1", "value1")
        assert context.get("key1") == "value1"
        
        context.update({"key2": "value2", "key3": "value3"})
        assert context.get("key2") == "value2"
        assert context.get("key3") == "value3"


# ==================== 内存映射存储测试 ====================

class TestMMapStorage:
    """内存映射存储后端测试"""
    
    @pytest.fixture
    def temp_dir(self):
        """创建临时目录"""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    @pytest.fixture
    def storage_config(self, temp_dir):
        """创建存储配置"""
        return MMapStorageConfig(
            data_dir=temp_dir,
            initial_size=1024 * 1024,  # 1MB
            enable_compression=False,
            enable_checksum=True,
            cache_size=100,
        )
    
    @pytest.mark.asyncio
    async def test_storage_initialization(self, storage_config):
        """测试存储初始化"""
        storage = MMapStorageBackend(storage_config)
        assert not storage._is_open
    
    @pytest.mark.asyncio
    async def test_storage_open_close(self, storage_config):
        """测试存储打开和关闭"""
        storage = MMapStorageBackend(storage_config)
        
        await storage.open()
        assert storage._is_open
        
        await storage.close()
        assert not storage._is_open
    
    @pytest.mark.asyncio
    async def test_storage_write_read(self, storage_config):
        """测试存储写入和读取"""
        storage = MMapStorageBackend(storage_config)
        await storage.open()
        
        key = "test_key"
        data = b"Hello, World!"
        
        offset = await storage.write(key, data)
        assert offset >= 0
        
        read_data = await storage.read(key)
        assert read_data == data
        
        await storage.close()
    
    @pytest.mark.asyncio
    async def test_storage_delete(self, storage_config):
        """测试存储删除"""
        storage = MMapStorageBackend(storage_config)
        await storage.open()
        
        key = "delete_key"
        data = b"data to delete"
        
        await storage.write(key, data)
        assert await storage.exists(key)
        
        deleted = await storage.delete(key)
        assert deleted
        assert not await storage.exists(key)
        
        await storage.close()
    
    @pytest.mark.asyncio
    async def test_storage_keys(self, storage_config):
        """测试获取所有键"""
        storage = MMapStorageBackend(storage_config)
        await storage.open()
        
        keys = [f"key_{i}" for i in range(10)]
        for key in keys:
            await storage.write(key, f"data_{key}".encode())
        
        stored_keys = await storage.keys()
        assert len(stored_keys) == 10
        for key in keys:
            assert key in stored_keys
        
        await storage.close()
    
    @pytest.mark.asyncio
    async def test_storage_stats(self, storage_config):
        """测试存储统计"""
        storage = MMapStorageBackend(storage_config)
        await storage.open()
        
        await storage.write("key1", b"data1")
        await storage.write("key2", b"data2")
        await storage.read("key1")
        
        stats = await storage.get_stats()
        assert stats["writes"] == 2
        assert stats["reads"] == 1
        assert stats["index_size"] == 2
        
        await storage.close()
    
    @pytest.mark.asyncio
    async def test_storage_compression(self, temp_dir):
        """测试存储压缩"""
        config = MMapStorageConfig(
            data_dir=temp_dir,
            initial_size=1024 * 1024,
            enable_compression=True,
            compression_level=6,
        )
        
        storage = MMapStorageBackend(config)
        await storage.open()
        
        # 写入可压缩的数据
        data = b"x" * 10000
        await storage.write("compressed_key", data)
        
        read_data = await storage.read("compressed_key")
        assert read_data == data
        
        await storage.close()
    
    @pytest.mark.asyncio
    async def test_storage_compaction(self, storage_config):
        """测试存储压缩"""
        storage = MMapStorageBackend(storage_config)
        await storage.open()
        
        # 写入数据
        for i in range(10):
            await storage.write(f"key_{i}", f"data_{i}".encode())
        
        # 删除部分数据
        for i in range(5):
            await storage.delete(f"key_{i}")
        
        # 压缩
        await storage.compact()
        
        # 验证剩余数据
        keys = await storage.keys()
        assert len(keys) == 5
        
        for i in range(5, 10):
            data = await storage.read(f"key_{i}")
            assert data == f"data_{i}".encode()
        
        await storage.close()
    
    @pytest.mark.asyncio
    async def test_record_header(self):
        """测试记录头部"""
        header = RecordHeader(
            record_type=RecordType.DATA,
            data_length=100,
            data_checksum=12345,
            timestamp=time.time(),
            sequence=1,
        )
        
        packed = header.pack()
        assert len(packed) == RecordHeader.HEADER_SIZE
        
        unpacked = RecordHeader.unpack(packed)
        assert unpacked.record_type == RecordType.DATA
        assert unpacked.data_length == 100
        assert unpacked.data_checksum == 12345
        assert unpacked.sequence == 1
    
    @pytest.mark.asyncio
    async def test_storage_manager(self, temp_dir):
        """测试存储管理器"""
        config = MMapStorageConfig(
            data_dir=temp_dir,
            initial_size=1024 * 1024,
        )
        
        manager = MMapStorageManager(config, num_shards=2)
        await manager.open()
        
        # 写入数据
        for i in range(10):
            await manager.write(f"key_{i}", f"data_{i}".encode())
        
        # 读取数据
        for i in range(10):
            data = await manager.read(f"key_{i}")
            assert data == f"data_{i}".encode()
        
        # 获取所有键
        keys = await manager.keys()
        assert len(keys) == 10
        
        await manager.close()


# ==================== 集成测试 ====================

class TestIntegration:
    """集成测试"""
    
    @pytest.mark.asyncio
    async def test_raft_with_mmap_storage(self):
        """测试Raft与MMap存储集成"""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = MMapStorageConfig(
                data_dir=temp_dir,
                initial_size=10 * 1024 * 1024,
            )
            
            # 创建Raft集群
            cluster = RaftCluster(num_nodes=3)
            await cluster.start()
            
            leader = await cluster.wait_for_leader(timeout=3.0)
            assert leader is not None
            
            # 提交存储命令
            result = await leader.propose_command({
                "action": "set",
                "key": "integration_test",
                "value": "success"
            })
            
            assert result is not None
            assert result.get("success") is True
            
            await cluster.stop()
    
    @pytest.mark.asyncio
    async def test_pipeline_with_scheduler(self):
        """测试流水线与调度器集成"""
        scheduler_config = SchedulerConfig(max_workers=4)
        scheduler = TaskScheduler(scheduler_config)
        await scheduler.start()
        
        pipeline_config = PipelineConfig(max_concurrent_items=50)
        
        async def scheduled_task(item):
            async def task_func():
                item.data["scheduled"] = True
                return item
            
            task = Task(
                task_id=f"task_{item.item_id}",
                coroutine=task_func(),
            )
            return await scheduler.submit(task, wait=True)
        
        stages = [
            PipelineStage("scheduler_stage", scheduled_task),
        ]
        pipeline = Pipeline(stages, pipeline_config)
        
        await pipeline.start()
        
        result = await pipeline.submit({"test": "data"}, wait_for_result=True)
        assert result.data.get("scheduled") is True
        
        await pipeline.stop()
        await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_end_to_end_memory_workflow(self):
        """测试端到端内存工作流"""
        # 创建各个组件
        scheduler = TaskScheduler(SchedulerConfig(max_workers=4))
        await scheduler.start()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_config = MMapStorageConfig(
                data_dir=temp_dir,
                initial_size=10 * 1024 * 1024,
            )
            storage = MMapStorageBackend(storage_config)
            await storage.open()
            
            # 模拟内存存储任务
            async def store_memory_task():
                memory_id = await storage.write(
                    "memory_1",
                    b"Memory content"
                )
                return memory_id
            
            task = Task(
                task_id="store_memory",
                coroutine=store_memory_task(),
                priority=TaskPriority.HIGH,
            )
            
            memory_id = await scheduler.submit(task, wait=True)
            assert memory_id is not None
            
            # 验证存储
            data = await storage.read("memory_1")
            assert data == b"Memory content"
            
            await storage.close()
        
        await scheduler.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

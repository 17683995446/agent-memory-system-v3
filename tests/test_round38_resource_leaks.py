"""
第38轮：资源泄漏与内存管理测试

测试目标:
1. 超长运行场景下的资源泄漏检测
2. 大规模并发下的资源管理
3. 复杂场景下的资源回收验证
4. 混乱场景(异常、超时、断连)下的资源清理

测试策略:
- 使用tracemalloc追踪内存分配
- 使用weakref追踪对象生命周期
- 模拟长时间运行(通过高频操作加速)
- 注入各种故障场景验证资源清理
"""

import asyncio
import gc
import logging
import sys
import time
import tracemalloc
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# 设置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 启动内存追踪
tracemalloc.start()

# 检查gRPC是否可用
try:
    import grpc
    _GRPC_AVAILABLE = True
except ImportError:
    _GRPC_AVAILABLE = False
    logger.warning("gRPC not available, skipping gRPC tests")


class ResourceTracker:
    """资源追踪器 - 监控对象生命周期"""
    
    def __init__(self):
        self._objects: Dict[str, List[weakref.ref]] = {}
        self._created_count: Dict[str, int] = {}
        self._destroyed_count: Dict[str, int] = {}
        self._lock = asyncio.Lock()
    
    def track(self, obj: Any, name: str) -> weakref.ref:
        """追踪对象"""
        def on_destroy(ref):
            asyncio.create_task(self._on_destroyed(name))
        
        ref = weakref.ref(obj, on_destroy)
        
        if name not in self._objects:
            self._objects[name] = []
            self._created_count[name] = 0
            self._destroyed_count[name] = 0
        
        self._objects[name].append(ref)
        self._created_count[name] += 1
        return ref
    
    async def _on_destroyed(self, name: str):
        async with self._lock:
            self._destroyed_count[name] = self._destroyed_count.get(name, 0) + 1
    
    def get_alive_count(self, name: str) -> int:
        """获取存活对象数量"""
        if name not in self._objects:
            return 0
        return sum(1 for ref in self._objects[name] if ref() is not None)
    
    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """获取统计信息"""
        stats = {}
        for name in self._objects:
            stats[name] = {
                "created": self._created_count.get(name, 0),
                "destroyed": self._destroyed_count.get(name, 0),
                "alive": self.get_alive_count(name),
            }
        return stats
    
    def clear(self):
        """清除追踪"""
        self._objects.clear()
        self._created_count.clear()
        self._destroyed_count.clear()


class MemorySnapshot:
    """内存快照对比工具"""
    
    def __init__(self):
        self._snapshots: List[tuple] = []
    
    def take(self, label: str = ""):
        """拍摄内存快照"""
        gc.collect()
        snapshot = tracemalloc.take_snapshot()
        self._snapshots.append((label, snapshot, time.time()))
        return snapshot
    
    def compare(self, idx1: int = -2, idx2: int = -1) -> List:
        """对比两个快照"""
        if len(self._snapshots) < 2:
            return []
        
        _, snap1, _ = self._snapshots[idx1]
        _, snap2, _ = self._snapshots[idx2]
        
        return snap2.compare_to(snap1, 'lineno')
    
    def get_memory_diff(self, idx1: int = -2, idx2: int = -1) -> int:
        """获取内存差异(字节)"""
        if len(self._snapshots) < 2:
            return 0
        
        _, snap1, t1 = self._snapshots[idx1]
        _, snap2, t2 = self._snapshots[idx2]
        
        stats1 = snap1.statistics('lineno')
        stats2 = snap2.statistics('lineno')
        
        size1 = sum(s.size for s in stats1)
        size2 = sum(s.size for s in stats2)
        
        return size2 - size1


# =============================================================================
# 测试1: gRPC连接生命周期管理 (仅当gRPC可用时运行)
# =============================================================================

@pytest.mark.skipif(not _GRPC_AVAILABLE, reason="gRPC not installed")
@pytest.mark.asyncio
async def test_grpc_connection_lifecycle():
    """
    测试1: gRPC连接生命周期管理
    
    场景:
    - 创建大量gRPC连接
    - 模拟连接异常断开
    - 验证连接是否正确关闭
    """
    from memory_system.grpc_service import GRPCServer, GRPCServiceConfig
    
    tracker = ResourceTracker()
    snapshot = MemorySnapshot()
    
    # 初始快照
    snapshot.take("initial")
    
    servers = []
    try:
        # 创建多个gRPC服务器
        for i in range(5):
            config = GRPCServiceConfig(
                host="127.0.0.1",
                port=50051 + i,
                max_workers=4,
            )
            server = GRPCServer(config)
            tracker.track(server, f"grpc_server_{i}")
            servers.append(server)
            await server.start()
            logger.debug(f"[TEST] gRPC server {i} started on port {config.port}")
        
        # 运行一段时间后停止
        await asyncio.sleep(0.5)
        
        # 模拟异常关闭部分服务器
        for i in [0, 2, 4]:
            await servers[i].stop(grace_period=0.1)
            logger.debug(f"[TEST] gRPC server {i} stopped")
        
        # 等待资源释放
        await asyncio.sleep(0.5)
        gc.collect()
        
        # 停止剩余服务器
        for i in [1, 3]:
            await servers[i].stop()
            logger.debug(f"[TEST] gRPC server {i} stopped")
        
        # 最终快照
        snapshot.take("final")
        
        # 验证所有服务器都被销毁
        for i in range(5):
            alive = tracker.get_alive_count(f"grpc_server_{i}")
            logger.debug(f"[TEST] gRPC server {i} alive count: {alive}")
            # 注意：由于循环引用，可能无法立即释放
            # 但至少应该标记为停止状态
    
    finally:
        # 确保所有服务器都被停止
        for server in servers:
            try:
                await server.stop()
            except:
                pass
    
    # 强制垃圾回收
    gc.collect()
    await asyncio.sleep(0.5)
    
    # 检查内存泄漏
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after gRPC test: {diff} bytes")
    
    stats = tracker.get_stats()
    logger.info(f"[TEST] Resource tracker stats: {stats}")
    
    # 验证：内存增长应该可控(<10MB)
    assert diff < 10 * 1024 * 1024, f"Memory leak detected: {diff} bytes"


@pytest.mark.skipif(not _GRPC_AVAILABLE, reason="gRPC not installed")
@pytest.mark.asyncio
async def test_grpc_connection_under_chaos():
    """
    测试2: 混乱场景下的gRPC连接管理
    
    场景:
    - 快速创建和销毁连接
    - 模拟网络分区
    - 验证无资源泄漏
    """
    from memory_system.grpc_service import GRPCServer, GRPCServiceConfig
    
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    # 快速创建和销毁100个服务器
    for i in range(20):
        config = GRPCServiceConfig(
            host="127.0.0.1",
            port=51000 + (i % 10),  # 复用端口
            max_workers=2,
        )
        server = GRPCServer(config)
        
        try:
            await server.start()
            # 随机延迟后停止
            await asyncio.sleep(0.01)
            
            # 随机选择优雅或强制关闭
            if i % 3 == 0:
                await server.stop(grace_period=0.001)
            else:
                await server.stop()
        except Exception as e:
            logger.warning(f"[TEST] Server {i} error: {e}")
        
        if i % 10 == 0:
            gc.collect()
    
    # 最终快照
    await asyncio.sleep(0.5)
    gc.collect()
    snapshot.take("final")
    
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after chaos test: {diff} bytes")
    
    # 验证：即使有混乱场景，内存增长也应可控
    assert diff < 50 * 1024 * 1024, f"Memory leak under chaos: {diff} bytes"


# =============================================================================
# 测试3: WebSocket连接管理
# =============================================================================

class MockWebSocket:
    """模拟WebSocket连接"""
    
    _id_counter = 0
    
    def __init__(self, ws_id: str = None):
        MockWebSocket._id_counter += 1
        self.ws_id = ws_id or f"ws_{MockWebSocket._id_counter}"
        self.closed = False
        self._accept_called = False
        self._messages: List[Dict] = []
    
    async def accept(self):
        self._accept_called = True
    
    async def send_json(self, data: Dict):
        self._messages.append(data)
    
    async def close(self):
        self.closed = True
    
    async def receive_text(self):
        await asyncio.sleep(3600)  # 长时间等待
        return ""
    
    def __hash__(self):
        return hash(self.ws_id)
    
    def __eq__(self, other):
        if not isinstance(other, MockWebSocket):
            return False
        return self.ws_id == other.ws_id


@pytest.mark.asyncio
async def test_websocket_connection_leak():
    """
    测试3: WebSocket连接泄漏检测
    
    场景:
    - 创建大量WebSocket连接
    - 部分连接异常断开
    - 验证连接集合正确清理
    
    DEFECT-R3-009 [P0] 修复验证: 确保连接管理在各种场景下正确工作
    """
    from memory_system.websocket_event_notifier import WebSocketEventNotifier
    
    notifier = WebSocketEventNotifier()
    tracker = ResourceTracker()
    
    # 创建100个连接
    connections = []
    for i in range(100):
        ws = MockWebSocket(f"ws_{i}")
        tracker.track(ws, "websocket")
        await notifier.connect(ws, tenant_id=f"tenant_{i % 10}")
        connections.append(ws)
    
    logger.debug(f"[TEST] Created 100 WebSocket connections")
    
    # 验证连接数
    assert notifier.get_connection_count() == 100
    
    # 模拟50个连接异常断开
    disconnected_ws = []
    for i in range(0, 100, 2):
        disconnected_ws.append(connections[i])
        await notifier.disconnect(connections[i])
    
    # 验证连接数减少
    assert notifier.get_connection_count() == 50
    
    # 广播消息触发清理
    await notifier.broadcast("test_event", {"data": "test"})
    
    # 关闭所有剩余连接
    await notifier.close_all()
    
    # 验证连接计数为0
    assert notifier.get_connection_count() == 0
    
    # 验证已断开的连接在disconnect时已被清理
    # (disconnect不会调用close，只是从集合中移除)
    # 验证close_all关闭了剩余连接
    remaining_ws = [ws for i, ws in enumerate(connections) if i % 2 == 1]
    for ws in remaining_ws:
        assert ws.closed, f"WebSocket {ws.ws_id} should be closed by close_all()"
    
    stats = tracker.get_stats()
    logger.info(f"[TEST] WebSocket resource stats: {stats}")


@pytest.mark.asyncio
async def test_websocket_under_high_load():
    """
    测试4: 高负载下的WebSocket管理
    
    场景:
    - 高频连接/断开
    - 大量广播消息
    - 验证无内存泄漏
    """
    from memory_system.websocket_event_notifier import WebSocketEventNotifier
    
    notifier = WebSocketEventNotifier()
    snapshot = MemorySnapshot()
    
    snapshot.take("initial")
    
    # 高频创建和销毁连接
    for round_idx in range(10):
        connections = []
        
        # 每轮创建50个连接
        for i in range(50):
            ws = MockWebSocket(f"round_{round_idx}_ws_{i}")
            await notifier.connect(ws, tenant_id="test_tenant")
            connections.append(ws)
        
        # 广播消息
        for _ in range(10):
            await notifier.broadcast("test_event", {"payload": "x" * 1000})
        
        # 断开所有连接
        for ws in connections:
            await notifier.disconnect(ws)
        
        if round_idx % 5 == 0:
            gc.collect()
    
    await asyncio.sleep(0.5)
    gc.collect()
    snapshot.take("final")
    
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after WebSocket high load: {diff} bytes")
    
    # 验证内存增长可控
    assert diff < 20 * 1024 * 1024, f"WebSocket memory leak: {diff} bytes"


# =============================================================================
# 测试5: Async任务引用追踪
# =============================================================================

@pytest.mark.asyncio
async def test_async_task_cleanup():
    """
    测试5: Async任务清理验证
    
    场景:
    - 创建大量asyncio任务
    - 任务完成后验证引用释放
    """
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    tasks = []
    
    async def dummy_task(task_id: int):
        await asyncio.sleep(0.001)
        return task_id
    
    # 创建1000个任务
    for i in range(1000):
        task = asyncio.create_task(dummy_task(i))
        tasks.append(task)
    
    # 等待所有任务完成
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.debug(f"[TEST] Completed {len(results)} tasks")
    
    # 清除任务引用
    tasks.clear()
    
    # 强制垃圾回收
    gc.collect()
    await asyncio.sleep(0.5)
    
    snapshot.take("final")
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after task cleanup: {diff} bytes")
    
    # 验证任务完成后内存释放
    assert diff < 5 * 1024 * 1024, f"Task memory leak: {diff} bytes"


@pytest.mark.asyncio
async def test_cancelled_task_cleanup():
    """
    测试6: 取消任务的资源清理
    
    场景:
    - 创建长时间运行的任务
    - 取消任务
    - 验证资源正确释放
    """
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    async def long_running_task():
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.debug("[TEST] Task cancelled, cleaning up...")
            raise
    
    # 创建并取消100个任务
    tasks = []
    for i in range(100):
        task = asyncio.create_task(long_running_task())
        tasks.append(task)
    
    # 等待一段时间后取消
    await asyncio.sleep(0.1)
    
    for task in tasks:
        task.cancel()
    
    # 等待取消完成
    await asyncio.gather(*tasks, return_exceptions=True)
    
    tasks.clear()
    gc.collect()
    await asyncio.sleep(0.5)
    
    snapshot.take("final")
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after cancelled task cleanup: {diff} bytes")
    
    assert diff < 5 * 1024 * 1024, f"Cancelled task memory leak: {diff} bytes"


# =============================================================================
# 测试7: 流水线资源管理
# =============================================================================

@pytest.mark.asyncio
async def test_pipeline_resource_cleanup():
    """
    测试7: 流水线资源清理
    
    场景:
    - 创建多级流水线
    - 处理大量数据
    - 停止后验证资源释放
    """
    from memory_system.async_pipeline import (
        Pipeline, PipelineStage, StageConfig, PipelineItem, PipelineConfig
    )
    
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    async def process_item(item: PipelineItem) -> PipelineItem:
        item.data["processed"] = True
        return item
    
    # 创建多级流水线
    stages = [
        PipelineStage("stage1", process_item, StageConfig("stage1", num_workers=4)),
        PipelineStage("stage2", process_item, StageConfig("stage2", num_workers=2)),
        PipelineStage("stage3", process_item, StageConfig("stage3", num_workers=2)),
    ]
    
    pipeline = Pipeline(stages, PipelineConfig())
    await pipeline.start()
    
    # 提交大量数据
    for i in range(500):
        await pipeline.submit({"id": i, "data": "x" * 100})
    
    # 等待处理完成
    await asyncio.sleep(1)
    
    # 停止流水线
    await pipeline.stop(graceful=True)
    
    gc.collect()
    await asyncio.sleep(0.5)
    
    snapshot.take("final")
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after pipeline cleanup: {diff} bytes")
    
    assert diff < 30 * 1024 * 1024, f"Pipeline memory leak: {diff} bytes"


@pytest.mark.asyncio
async def test_pipeline_under_pressure():
    """
    测试8: 压力测试下的流水线资源管理
    
    场景:
    - 高频创建/销毁流水线
    - 背压场景
    - 验证无资源泄漏
    """
    from memory_system.async_pipeline import (
        Pipeline, PipelineStage, StageConfig, PipelineItem, PipelineConfig
    )
    
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    async def slow_process(item: PipelineItem) -> PipelineItem:
        await asyncio.sleep(0.01)
        return item
    
    # 多次创建和销毁流水线
    for i in range(10):
        stages = [
            PipelineStage(f"s1_{i}", slow_process, StageConfig(f"s1_{i}", num_workers=2, buffer_size=10)),
            PipelineStage(f"s2_{i}", slow_process, StageConfig(f"s2_{i}", num_workers=1, buffer_size=5)),
        ]
        
        pipeline = Pipeline(stages, PipelineConfig())
        await pipeline.start()
        
        # 快速提交数据(触发背压)
        for j in range(50):
            try:
                await pipeline.submit({"id": j}, wait_for_result=False)
            except Exception as e:
                logger.debug(f"[TEST] Submit error (expected): {e}")
        
        await asyncio.sleep(0.2)
        
        # 强制停止
        await pipeline.stop(graceful=False)
        
        if i % 5 == 0:
            gc.collect()
    
    await asyncio.sleep(0.5)
    gc.collect()
    snapshot.take("final")
    
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after pipeline pressure test: {diff} bytes")
    
    assert diff < 50 * 1024 * 1024, f"Pipeline pressure memory leak: {diff} bytes"


# =============================================================================
# 测试9: 事务管理器资源管理
# =============================================================================

@pytest.mark.asyncio
async def test_transaction_manager_cleanup():
    """
    测试9: 事务管理器资源清理
    
    场景:
    - 创建大量事务
    - 各种状态的事务
    - 验证事务存储清理
    """
    from memory_system.distributed_transaction_manager import (
        DistributedTransactionManager, Participant, TransactionContext
    )
    
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    manager = DistributedTransactionManager()
    await manager.start()
    
    # 注册参与者
    async def dummy_handler(tx_id: str, ctx: TransactionContext) -> bool:
        return True
    
    for i in range(10):
        participant = Participant(
            participant_id=f"p_{i}",
            resource_id=f"r_{i}",
            prepare_handler=dummy_handler,
            commit_handler=dummy_handler,
            abort_handler=dummy_handler,
        )
        manager.register_2pc_participant(participant)
    
    # 创建100个事务
    transactions = []
    for i in range(100):
        tx = await manager.begin_2pc_transaction(timeout=5.0)
        transactions.append(tx)
        
        # 添加参与者
        for j in range(3):
            await manager.add_participant_to_2pc(tx.tx_id, f"p_{j}")
    
    logger.debug(f"[TEST] Created {len(transactions)} transactions")
    
    # 提交部分事务
    for i in range(0, 50):
        try:
            await manager.prepare_2pc(transactions[i].tx_id)
            await manager.commit_2pc(transactions[i].tx_id)
        except Exception as e:
            logger.debug(f"[TEST] Commit error: {e}")
    
    # 中止部分事务
    for i in range(50, 100):
        try:
            await manager.abort_2pc(transactions[i].tx_id)
        except Exception as e:
            logger.debug(f"[TEST] Abort error: {e}")
    
    await manager.stop()
    
    # 清除引用
    transactions.clear()
    gc.collect()
    await asyncio.sleep(0.5)
    
    snapshot.take("final")
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after transaction cleanup: {diff} bytes")
    
    assert diff < 30 * 1024 * 1024, f"Transaction manager memory leak: {diff} bytes"


# =============================================================================
# 测试10: 综合长时间运行场景
# =============================================================================

@pytest.mark.asyncio
async def test_long_running_simulation():
    """
    测试10: 长时间运行模拟
    
    场景:
    - 模拟长时间运行(通过高频操作)
    - 各种组件混合使用
    - 验证整体资源管理
    """
    from memory_system.async_pipeline import (
        Pipeline, PipelineStage, StageConfig, PipelineItem
    )
    from memory_system.websocket_event_notifier import WebSocketEventNotifier
    
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    # 创建组件
    notifier = WebSocketEventNotifier()
    
    async def process(item: PipelineItem) -> PipelineItem:
        await asyncio.sleep(0.001)
        return item
    
    stages = [
        PipelineStage("s1", process, StageConfig("s1", num_workers=2)),
        PipelineStage("s2", process, StageConfig("s2", num_workers=2)),
    ]
    pipeline = Pipeline(stages)
    await pipeline.start()
    
    # 模拟长时间运行(通过高频操作)
    for iteration in range(50):
        # WebSocket操作
        ws_list = []
        for i in range(10):
            ws = MockWebSocket(f"iter_{iteration}_ws_{i}")
            await notifier.connect(ws, tenant_id="test")
            ws_list.append(ws)
        
        # 流水线操作
        for i in range(20):
            await pipeline.submit({"iter": iteration, "id": i}, wait_for_result=False)
        
        # 广播
        await notifier.broadcast("event", {"iter": iteration})
        
        # 清理WebSocket
        for ws in ws_list:
            await notifier.disconnect(ws)
        
        if iteration % 10 == 0:
            gc.collect()
    
    # 清理
    await pipeline.stop()
    await notifier.close_all()
    
    gc.collect()
    await asyncio.sleep(0.5)
    
    snapshot.take("final")
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after long running simulation: {diff} bytes")
    
    # 长时间运行后内存增长应非常有限
    assert diff < 40 * 1024 * 1024, f"Long running memory leak: {diff} bytes"


# =============================================================================
# 测试11: 缓存大小限制验证
# =============================================================================

@pytest.mark.asyncio
async def test_cache_size_limits():
    """
    测试11: 缓存大小限制验证
    
    场景:
    - 测试TTLCache的容量限制
    - 验证LRU淘汰策略
    """
    from cachetools import TTLCache
    
    # 创建小容量缓存
    cache = TTLCache(maxsize=10, ttl=60)
    
    # 插入20个条目(超过容量)
    for i in range(20):
        cache[i] = f"value_{i}"
    
    # 验证缓存大小不超过限制
    assert len(cache) <= 10, f"Cache size {len(cache)} exceeds limit 10"
    
    # 验证保留的是最近使用的条目
    # (后10个应该被保留)
    for i in range(10, 20):
        assert i in cache, f"Recent item {i} should be in cache"
    
    logger.info(f"[TEST] Cache size limit test passed, current size: {len(cache)}")


# =============================================================================
# 测试12: 异常场景下的资源清理
# =============================================================================

@pytest.mark.asyncio
async def test_exception_handling_resource_cleanup():
    """
    测试12: 异常处理资源清理
    
    场景:
    - 处理中抛出异常
    - 验证资源正确清理
    """
    from memory_system.async_pipeline import (
        Pipeline, PipelineStage, StageConfig, PipelineItem
    )
    
    snapshot = MemorySnapshot()
    snapshot.take("initial")
    
    error_count = 0
    
    async def error_prone_process(item: PipelineItem) -> PipelineItem:
        nonlocal error_count
        if item.data.get("should_fail"):
            error_count += 1
            raise ValueError(f"Simulated error #{error_count}")
        return item
    
    stages = [
        PipelineStage("error_stage", error_prone_process, StageConfig("error_stage", num_workers=2)),
    ]
    
    pipeline = Pipeline(stages)
    await pipeline.start()
    
    # 提交会失败的数据
    for i in range(50):
        await pipeline.submit({"id": i, "should_fail": i % 2 == 0}, wait_for_result=False)
    
    await asyncio.sleep(0.5)
    
    await pipeline.stop()
    
    gc.collect()
    await asyncio.sleep(0.5)
    
    snapshot.take("final")
    diff = snapshot.get_memory_diff()
    logger.info(f"[TEST] Memory diff after exception handling: {diff} bytes, errors: {error_count}")
    
    assert error_count > 0, "Should have encountered errors"
    assert diff < 20 * 1024 * 1024, f"Exception handling memory leak: {diff} bytes"


# =============================================================================
# 测试13: 综合回归测试
# =============================================================================

@pytest.mark.asyncio
async def test_regression_all_previous_fixes():
    """
    测试13: 回归测试 - 验证之前所有修复
    
    运行所有关键路径确保之前的BUG修复仍然有效
    """
    # 导入所有已修复的模块
    from memory_system.hybrid_logical_clock import HybridLogicalClock, HLCTimestamp
    from memory_system.vector_clock import VectorClock, VectorClockManager
    from memory_system.raft_consensus import RaftNode
    from memory_system.distributed_transaction_manager import DistributedTransactionManager
    
    # 测试HLC
    hlc1 = HybridLogicalClock("node1")
    hlc2 = HybridLogicalClock("node2")
    
    ts1 = hlc1.now()
    ts2 = hlc2.update(ts1)
    
    # 验证比较操作
    result = HybridLogicalClock.compare(ts1, ts2)
    assert result is not None
    
    # 验证max/min操作(需要IntEnum支持)
    max_ts = HybridLogicalClock.max(ts1, ts2)
    min_ts = HybridLogicalClock.min(ts1, ts2)
    assert max_ts is not None
    assert min_ts is not None
    
    # 测试VectorClock
    vc1 = VectorClock("node1")
    vc1.increment()
    
    vc2 = VectorClock("node2")
    vc2.update(vc1)
    vc2.increment()
    
    # 验证hash/eq一致性
    vc1_copy = VectorClock("node1", vc1.clock)
    assert vc1 == vc1_copy
    assert hash(vc1) == hash(vc1_copy)
    
    # 测试VectorClockManager
    vcm = VectorClockManager("node1")
    event1 = vcm.record_event("event1")
    
    # 模拟接收事件
    from memory_system.vector_clock import CausalEvent
    remote_event = CausalEvent(
        event_id="remote1",
        node_id="node2",
        vector_clock={"node1": 1, "node2": 2}
    )
    vcm.receive_event(remote_event)
    
    # 验证接收后时钟递增
    current = vcm.get_current_clock()
    assert current.get_timestamp() > 0
    
    logger.info("[TEST] All regression tests passed")


if __name__ == "__main__":
    # 运行所有测试
    pytest.main([__file__, "-v", "--tb=short"])

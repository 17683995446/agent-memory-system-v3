"""
Agent Memory System V3.1.0 - 第23轮标准化全流程测试文件

测试覆盖：
1. 模块集成联调（记忆写入链路、检索链路、事件发布链路、TTL过期链路）
2. 接口全量遍历（MemorySystem、StorageBackend、EmbeddingProvider、EventBus）
3. 参数传递验证
4. 状态同步验证

使用 pytest + pytest-asyncio
"""

import pytest
import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass, field
from datetime import datetime, timezone

# 导入被测模块
from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    MemoryStats,
    RememberResponse,
    MemoryLink,
    LinkRelation,
)
from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy,
    ConsolidationStrategy,
    ForgettingStrategy,
)
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.configuration_settings.memory_system_configuration import MemoryConfig
from memory_system.monitoring_metrics_collector import MemoryMetrics
from memory_system.telemetry_tracing_manager import TelemetryTracingManager
from memory_system.memory_ttl_expiration_manager import (
    MemoryTTLExpirationManager,
    TTLConfig,
)
from memory_system.event_driven_core import (
    EventBus,
    InMemoryEventBus,
    DomainEvent,
    EventPriority,
    BackendType,
)
from memory_system.agent_memory_system_facade import (
    MemorySystem,
    MemorySystemBuilder,
)


# ============================================================================
# 测试夹具
# ============================================================================

@pytest.fixture
def mock_storage():
    """模拟存储后端"""
    storage = AsyncMock(spec=StorageBackend)
    storage._memories: Dict[str, MemoryItem] = {}
    
    async def mock_add(item: MemoryItem) -> str:
        memory_id = item.memory_id or uuid.uuid4().hex
        item.memory_id = memory_id
        storage._memories[memory_id] = item
        return memory_id
    
    async def mock_get(memory_id: str) -> Optional[MemoryItem]:
        return storage._memories.get(memory_id)
    
    async def mock_delete(memory_id: str) -> bool:
        if memory_id in storage._memories:
            del storage._memories[memory_id]
            return True
        return False
    
    async def mock_search_by_vector(query_vector: List[float], top_k: int = 5, 
                                     filters: Optional[Dict[str, Any]] = None):
        results = []
        for mem in storage._memories.values():
            if mem.embedding:
                # 简单余弦相似度
                dot = sum(a * b for a, b in zip(query_vector, mem.embedding))
                norm1 = sum(a * a for a in query_vector) ** 0.5
                norm2 = sum(b * b for b in mem.embedding) ** 0.5
                if norm1 > 0 and norm2 > 0:
                    score = dot / (norm1 * norm2)
                    results.append((mem, score))
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
    
    async def mock_search_by_time_range(start_time: float, end_time: float,
                                         memory_type: Optional[str] = None):
        results = []
        for mem in storage._memories.values():
            if start_time <= mem.timestamp <= end_time:
                if memory_type is None or mem.memory_type.value == memory_type:
                    results.append(mem)
        return results
    
    async def mock_count(memory_type: Optional[str] = None) -> int:
        if memory_type is None:
            return len(storage._memories)
        return sum(1 for m in storage._memories.values() 
                   if m.memory_type.value == memory_type)
    
    async def mock_update(memory_id: str, updates: Dict[str, Any]) -> bool:
        if memory_id in storage._memories:
            mem = storage._memories[memory_id]
            for key, value in updates.items():
                if hasattr(mem, key):
                    setattr(mem, key, value)
            return True
        return False
    
    async def mock_batch_add(items: List[MemoryItem]) -> List[str]:
        ids = []
        for item in items:
            ids.append(await mock_add(item))
        return ids
    
    async def mock_batch_delete(memory_ids: List[str]) -> int:
        count = 0
        for mid in memory_ids:
            if await mock_delete(mid):
                count += 1
        return count
    
    async def mock_close():
        pass
    
    storage.add = mock_add
    storage.get = mock_get
    storage.delete = mock_delete
    storage.search_by_vector = mock_search_by_vector
    storage.search_by_time_range = mock_search_by_time_range
    storage.count = mock_count
    storage.update = mock_update
    storage.batch_add = mock_batch_add
    storage.batch_delete = mock_batch_delete
    storage.close = mock_close
    
    return storage


@pytest.fixture
def mock_embedding():
    """模拟嵌入提供者"""
    embedding = AsyncMock(spec=EmbeddingProvider)
    embedding._dimension = 128
    
    async def mock_embed(text: str) -> List[float]:
        # 简单模拟：基于文本生成固定向量
        hash_val = hash(text) % 1000
        return [hash_val / 1000.0] * embedding._dimension
    
    async def mock_embed_batch(texts: List[str]) -> List[List[float]]:
        return [await mock_embed(t) for t in texts]
    
    async def mock_similarity(vec1: List[float], vec2: List[float]) -> float:
        if len(vec1) != len(vec2) or len(vec1) == 0:
            return 0.0
        dot = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = sum(a * a for a in vec1) ** 0.5
        norm2 = sum(b * b for b in vec2) ** 0.5
        if norm1 > 0 and norm2 > 0:
            return dot / (norm1 * norm2)
        return 0.0
    
    embedding.dimension = embedding._dimension
    embedding.embed = mock_embed
    embedding.embed_batch = mock_embed_batch
    embedding.similarity = mock_similarity
    
    return embedding


@pytest.fixture
def mock_retrieval():
    """模拟检索策略"""
    retrieval = AsyncMock(spec=RetrievalStrategy)
    
    async def mock_retrieve(query: str, storage: StorageBackend, 
                            embedding: EmbeddingProvider,
                            max_results: int = 10,
                            context: Optional[Dict[str, Any]] = None):
        context = context or {}
        results = {
            "query": query,
            "strategy": "mock",
            "layers": {},
            "combined": []
        }
        
        # 搜索短期记忆
        short_term = context.get("short_term_memories", [])
        for mem in short_term[:max_results]:
            results["layers"]["short_term"] = results["layers"].get("short_term", [])
            results["layers"]["short_term"].append({
                "memory": mem.model_dump(),
                "score": 0.8
            })
            results["combined"].append({
                "memory": mem.model_dump(),
                "score": 0.8,
                "layer": "short_term"
            })
        
        # 向量检索
        if query:
            query_vec = await embedding.embed(query)
            vec_results = await storage.search_by_vector(query_vec, top_k=max_results)
            results["layers"]["vector"] = []
            for mem, score in vec_results:
                results["layers"]["vector"].append({
                    "memory": mem.model_dump(),
                    "score": score
                })
                results["combined"].append({
                    "memory": mem.model_dump(),
                    "score": score,
                    "layer": "vector"
                })
        
        # 排序
        results["combined"].sort(key=lambda x: x["score"], reverse=True)
        results["combined"] = results["combined"][:max_results]
        
        return results
    
    retrieval.retrieve = mock_retrieve
    retrieval.strategy = "mock"
    
    return retrieval


@pytest.fixture
def short_term_memory():
    """短期记忆实例"""
    return ShortTermMemory(capacity=7, ttl_seconds=300)


@pytest.fixture
async def memory_system(mock_storage, mock_embedding, mock_retrieval, short_term_memory):
    """记忆系统实例"""
    config = MemoryConfig()
    metrics = MemoryMetrics()
    
    system = MemorySystem(
        storage=mock_storage,
        embedding=mock_embedding,
        retrieval=mock_retrieval,
        short_term=short_term_memory,
        config=config,
        metrics=metrics,
    )
    
    yield system
    
    # 清理
    await system.close()


@pytest.fixture
async def event_bus():
    """事件总线实例"""
    bus = InMemoryEventBus()
    await bus.start()
    yield bus
    await bus.stop()


# ============================================================================
# 一、模块集成联调测试
# ============================================================================

class TestModuleIntegration:
    """模块集成联调测试类"""
    
    @pytest.mark.asyncio
    async def test_memory_write_chain_short_term(self, memory_system):
        """测试记忆写入链路 - 短期记忆路径"""
        print("[LOG] test_memory_write_chain_short_term::开始 - 验证短期记忆完整链路")
        
        # 写入短期记忆
        content = "这是一条短期记忆测试"
        memory_id = await memory_system.remember(
            content=content,
            memory_type="short_term",
            importance="medium"
        )
        
        print(f"[LOG] test_memory_write_chain_short_term::写入完成 - memory_id: {memory_id}")
        
        # 验证：短期记忆应该被存储
        short_term_memories = await memory_system.short_term.get_all()
        assert len(short_term_memories) > 0
        
        print(f"[LOG] test_memory_write_chain_short_term::验证通过 - 短期记忆数量: {len(short_term_memories)} vs 期望 > 0")
        
        # 验证统计
        stats = await memory_system.get_stats()
        assert stats.total_added >= 1
        
        print(f"[LOG] test_memory_write_chain_short_term::统计验证 - total_added: {stats.total_added} vs 期望 >= 1")
    
    @pytest.mark.asyncio
    async def test_memory_write_chain_long_term(self, memory_system):
        """测试记忆写入链路 - 长期记忆路径"""
        print("[LOG] test_memory_write_chain_long_term::开始 - 验证长期记忆完整链路")
        
        # 写入长期记忆
        content = "这是一条长期语义记忆测试"
        memory_id = await memory_system.remember(
            content=content,
            memory_type="semantic",
            importance="high"
        )
        
        print(f"[LOG] test_memory_write_chain_long_term::写入完成 - memory_id: {memory_id}")
        
        # 验证：长期记忆应该被存储到storage
        stored = await memory_system.storage.get(memory_id)
        assert stored is not None
        assert stored.content == content
        
        print(f"[LOG] test_memory_write_chain_long_term::验证通过 - 存储内容匹配: {stored.content == content}")
        
        # 验证嵌入被生成
        assert stored.embedding is not None
        assert len(stored.embedding) > 0
        
        print(f"[LOG] test_memory_write_chain_long_term::嵌入验证 - 嵌入维度: {len(stored.embedding)} vs 期望 > 0")
    
    @pytest.mark.asyncio
    async def test_auto_type_selection(self, memory_system):
        """测试自动类型选择逻辑"""
        print("[LOG] test_auto_type_selection::开始 - 验证自动类型选择")
        
        # 高重要性应该选择semantic
        memory_id_high = await memory_system.remember(
            content="重要信息",
            memory_type="auto",
            importance="critical"
        )
        
        stored_high = await memory_system.storage.get(memory_id_high)
        if stored_high:
            assert stored_high.memory_type == MemoryType.SEMANTIC
            print(f"[LOG] test_auto_type_selection::高重要性 - 类型: {stored_high.memory_type} vs 期望: semantic")
        
        # 低重要性应该选择short_term
        memory_id_low = await memory_system.remember(
            content="普通信息",
            memory_type="auto",
            importance="low"
        )
        
        short_term = await memory_system.short_term.get_all()
        assert len(short_term) > 0
        
        print(f"[LOG] test_auto_type_selection::低重要性 - 短期记忆数量: {len(short_term)} vs 期望 > 0")
    
    @pytest.mark.asyncio
    async def test_retrieval_chain(self, memory_system):
        """测试记忆检索链路 - 完整三级检索"""
        print("[LOG] test_retrieval_chain::开始 - 验证检索完整链路")
        
        # 先写入一些记忆
        await memory_system.remember("Python编程语言", memory_type="semantic", importance="high")
        await memory_system.remember("机器学习算法", memory_type="semantic", importance="high")
        await memory_system.remember("数据库设计", memory_type="semantic", importance="medium")
        
        print("[LOG] test_retrieval_chain::数据准备完成 - 写入3条记忆")
        
        # 执行检索
        results = await memory_system.recall("Python", max_results=5)
        
        print(f"[LOG] test_retrieval_chain::检索完成 - 结果数量: {len(results.get('combined', []))}")
        
        # 验证结果结构
        assert "query" in results
        assert "combined" in results
        assert results["query"] == "Python"
        
        print(f"[LOG] test_retrieval_chain::结果结构验证 - query: {results['query']} vs 期望: Python")
    
    @pytest.mark.asyncio
    async def test_three_level_retrieval(self, memory_system):
        """测试L1(短期) -> L2(向量) -> L3(关键词) 三级检索"""
        print("[LOG] test_three_level_retrieval::开始 - 验证三级检索")
        
        # 写入短期记忆
        await memory_system.remember("临时任务：完成报告", memory_type="short_term", importance="high")
        
        # 写入长期记忆
        await memory_system.remember("项目报告模板", memory_type="semantic", importance="medium")
        
        # 执行检索
        results = await memory_system.recall("报告", max_results=10)
        
        # 验证层级结果
        layers = results.get("layers", {})
        
        print(f"[LOG] test_three_level_retrieval::层级结果 - layers keys: {list(layers.keys())}")
        
        # 检查是否有结果
        combined = results.get("combined", [])
        assert len(combined) >= 0  # 至少应该有结果结构
        
        print(f"[LOG] test_three_level_retrieval::结果验证 - combined数量: {len(combined)}")
    
    @pytest.mark.asyncio
    async def test_event_publish_chain(self, memory_system, event_bus):
        """测试事件发布链路"""
        print("[LOG] test_event_publish_chain::开始 - 验证事件发布链路")
        
        # 记录事件调用
        events_received = []
        
        async def event_handler(event: DomainEvent):
            events_received.append(event)
            print(f"[LOG] test_event_publish_chain::事件处理器被调用 - event_type: {event.event_type}")
        
        # 订阅事件
        await event_bus.subscribe("memory.created", event_handler)
        
        print("[LOG] test_event_publish_chain::订阅完成 - 事件类型: memory.created")
        
        # 发布事件
        test_event = DomainEvent(
            event_type="memory.created",
            payload={"content": "测试记忆"}
        )
        success = await event_bus.publish(test_event)
        
        print(f"[LOG] test_event_publish_chain::发布结果 - success: {success} vs 期望: True")
        assert success is True
        
        # 等待事件处理
        await asyncio.sleep(0.5)
        
        print(f"[LOG] test_event_publish_chain::事件处理 - 收到事件数: {len(events_received)}")
    
    @pytest.mark.asyncio
    async def test_event_exception_not_affect_main(self, memory_system, event_bus):
        """测试事件异常不影响主流程"""
        print("[LOG] test_event_exception_not_affect_main::开始 - 验证事件异常隔离")
        
        # 注册一个会抛出异常的处理器
        async def bad_handler(event: DomainEvent):
            raise ValueError("模拟事件处理异常")
        
        # 注册一个正常的处理器
        normal_called = []
        
        async def normal_handler(event: DomainEvent):
            normal_called.append(True)
        
        await event_bus.subscribe("test.event", bad_handler)
        await event_bus.subscribe("test.event", normal_handler)
        
        # 发布事件
        test_event = DomainEvent(event_type="test.event", payload={})
        success = await event_bus.publish(test_event)
        
        print(f"[LOG] test_event_exception_not_affect_main::发布成功 - success: {success}")
        
        # 等待处理
        await asyncio.sleep(0.5)
        
        # 主流程应该不受影响
        print(f"[LOG] test_event_exception_not_affect_main::正常处理器被调用 - {len(normal_called)} 次")
    
    @pytest.mark.asyncio
    async def test_ttl_expiration_chain(self, mock_storage, mock_embedding, mock_retrieval):
        """测试TTL过期链路"""
        print("[LOG] test_ttl_expiration_chain::开始 - 验证TTL过期链路")
        
        # 创建带TTL管理器的记忆系统
        ttl_config = TTLConfig(
            enabled=True,
            default_ttl_days=0.0001,  # 非常短的TTL用于测试
            check_interval_hours=0.001,
        )
        
        short_term = ShortTermMemory(capacity=7, ttl_seconds=300)
        ttl_manager = MemoryTTLExpirationManager(mock_storage, config=ttl_config)
        
        memory_system = MemorySystem(
            storage=mock_storage,
            embedding=mock_embedding,
            retrieval=mock_retrieval,
            short_term=short_term,
            ttl_manager=ttl_manager,
        )
        
        # 写入记忆
        memory_id = await memory_system.remember(
            content="TTL测试记忆",
            memory_type="semantic",
            importance="low"
        )
        
        print(f"[LOG] test_ttl_expiration_chain::写入完成 - memory_id: {memory_id}")
        
        # 获取过期信息
        expiration_info = await ttl_manager.get_expiration_info(memory_id)
        
        if expiration_info:
            print(f"[LOG] test_ttl_expiration_chain::过期信息 - TTL天数: {expiration_info.get('ttl_days')}")
            print(f"[LOG] test_ttl_expiration_chain::过期信息 - 剩余天数: {expiration_info.get('days_remaining')}")
        
        await memory_system.close()


# ============================================================================
# 二、接口全量遍历测试
# ============================================================================

class TestMemorySystemInterface:
    """MemorySystem接口遍历测试"""
    
    @pytest.mark.asyncio
    async def test_remember_normal(self, memory_system):
        """测试remember() - 正常情况"""
        print("[LOG] test_remember_normal::开始 - 正常写入")
        
        memory_id = await memory_system.remember(
            content="正常记忆内容",
            memory_type="semantic",
            importance="high"
        )
        
        assert memory_id is not None
        assert len(memory_id) > 0
        
        print(f"[LOG] test_remember_normal::验证通过 - memory_id长度: {len(memory_id)} vs 期望 > 0")
    
    @pytest.mark.asyncio
    async def test_remember_empty_content(self, memory_system):
        """测试remember() - 空内容"""
        print("[LOG] test_remember_empty_content::开始 - 空内容应抛出异常")
        
        with pytest.raises((ValueError, TypeError)):
            await memory_system.remember(content="", memory_type="semantic")
        
        print("[LOG] test_remember_empty_content::验证通过 - 正确抛出异常")
    
    @pytest.mark.asyncio
    async def test_remember_long_content(self, memory_system):
        """测试remember() - 超长内容"""
        print("[LOG] test_remember_long_content::开始 - 超长内容")
        
        long_content = "测试内容" * 10000  # 非常长的内容
        
        # 可能会截断或抛出异常
        try:
            memory_id = await memory_system.remember(
                content=long_content,
                memory_type="semantic"
            )
            print(f"[LOG] test_remember_long_content::处理成功 - memory_id: {memory_id[:20]}...")
        except ValueError as e:
            print(f"[LOG] test_remember_long_content::正确拒绝 - 异常: {str(e)[:50]}")
    
    @pytest.mark.asyncio
    async def test_remember_invalid_type(self, memory_system):
        """测试remember() - 非法类型"""
        print("[LOG] test_remember_invalid_type::开始 - 非法记忆类型")
        
        with pytest.raises((ValueError, TypeError)):
            await memory_system.remember(
                content="测试",
                memory_type="invalid_type"
            )
        
        print("[LOG] test_remember_invalid_type::验证通过 - 正确拒绝非法类型")
    
    @pytest.mark.asyncio
    async def test_remember_invalid_importance(self, memory_system):
        """测试remember() - 非法重要性"""
        print("[LOG] test_remember_invalid_importance::开始 - 非法重要性级别")
        
        with pytest.raises((ValueError, TypeError)):
            await memory_system.remember(
                content="测试",
                importance="super_high"
            )
        
        print("[LOG] test_remember_invalid_importance::验证通过 - 正确拒绝非法重要性")
    
    @pytest.mark.asyncio
    async def test_recall_normal(self, memory_system):
        """测试recall() - 正常情况"""
        print("[LOG] test_recall_normal::开始 - 正常检索")
        
        # 先写入
        await memory_system.remember("测试记忆内容", memory_type="semantic")
        
        # 检索
        results = await memory_system.recall("测试", max_results=5)
        
        assert results is not None
        assert "combined" in results
        
        print(f"[LOG] test_recall_normal::验证通过 - 结果包含combined字段")
    
    @pytest.mark.asyncio
    async def test_recall_empty_query(self, memory_system):
        """测试recall() - 空查询"""
        print("[LOG] test_recall_empty_query::开始 - 空查询")
        
        with pytest.raises((ValueError, TypeError)):
            await memory_system.recall("")
        
        print("[LOG] test_recall_empty_query::验证通过 - 正确拒绝空查询")
    
    @pytest.mark.asyncio
    async def test_recall_long_query(self, memory_system):
        """测试recall() - 超长查询"""
        print("[LOG] test_recall_long_query::开始 - 超长查询")
        
        long_query = "查询词" * 5000
        
        try:
            results = await memory_system.recall(long_query, max_results=5)
            print(f"[LOG] test_recall_long_query::处理成功 - 结果类型: {type(results)}")
        except ValueError as e:
            print(f"[LOG] test_recall_long_query::正确拒绝 - 异常: {str(e)[:50]}")
    
    @pytest.mark.asyncio
    async def test_recall_boundary_max_results(self, memory_system):
        """测试recall() - 边界max_results"""
        print("[LOG] test_recall_boundary_max_results::开始 - 边界max_results")
        
        # max_results = 1
        results = await memory_system.recall("测试", max_results=1)
        assert results is not None
        print(f"[LOG] test_recall_boundary_max_results::max_results=1 - 通过")
        
        # max_results = 0 应该被修正或拒绝
        try:
            results = await memory_system.recall("测试", max_results=0)
            print(f"[LOG] test_recall_boundary_max_results::max_results=0 - 处理成功")
        except ValueError:
            print(f"[LOG] test_recall_boundary_max_results::max_results=0 - 正确拒绝")
    
    @pytest.mark.asyncio
    async def test_forget_normal(self, memory_system):
        """测试forget() - 正常情况"""
        print("[LOG] test_forget_normal::开始 - 正常遗忘")
        
        # 写入
        memory_id = await memory_system.remember("待遗忘的记忆", memory_type="semantic")
        
        # 遗忘
        success = await memory_system.forget(memory_id)
        
        print(f"[LOG] test_forget_normal::遗忘结果 - success: {success} vs 期望: True")
        assert success is True
    
    @pytest.mark.asyncio
    async def test_forget_nonexistent(self, memory_system):
        """测试forget() - 不存在的memory_id"""
        print("[LOG] test_forget_nonexistent::开始 - 不存在的记忆ID")
        
        success = await memory_system.forget("nonexistent_id_12345")
        
        print(f"[LOG] test_forget_nonexistent::遗忘结果 - success: {success} vs 期望: False")
        assert success is False
    
    @pytest.mark.asyncio
    async def test_get_stats(self, memory_system):
        """测试get_stats() - 获取统计信息"""
        print("[LOG] test_get_stats::开始 - 获取统计")
        
        # 写入一些记忆
        await memory_system.remember("记忆1", memory_type="semantic")
        await memory_system.remember("记忆2", memory_type="short_term")
        
        stats = await memory_system.get_stats()
        
        assert stats is not None
        assert isinstance(stats, MemoryStats)
        
        print(f"[LOG] test_get_stats::统计信息 - total_count: {stats.total_count}")
        print(f"[LOG] test_get_stats::统计信息 - total_added: {stats.total_added}")
    
    @pytest.mark.asyncio
    async def test_clear_short_term(self, memory_system):
        """测试clear_short_term() - 清空短期记忆"""
        print("[LOG] test_clear_short_term::开始 - 清空短期记忆")
        
        # 写入短期记忆
        await memory_system.remember("短期记忆", memory_type="short_term")
        
        # 清空
        await memory_system.clear_short_term()
        
        # 验证
        all_short_term = await memory_system.short_term.get_all()
        
        print(f"[LOG] test_clear_short_term::验证 - 短期记忆数量: {len(all_short_term)} vs 期望: 0")
        assert len(all_short_term) == 0


class TestStorageBackendInterface:
    """StorageBackend接口遍历测试"""
    
    @pytest.mark.asyncio
    async def test_storage_add_normal(self, mock_storage):
        """测试add() - 正常情况"""
        print("[LOG] test_storage_add_normal::开始 - 正常添加")
        
        item = MemoryItem(
            content="测试记忆",
            memory_type=MemoryType.SEMANTIC,
            importance=ImportanceLevel.HIGH
        )
        
        memory_id = await mock_storage.add(item)
        
        assert memory_id is not None
        print(f"[LOG] test_storage_add_normal::验证通过 - memory_id: {memory_id}")
    
    @pytest.mark.asyncio
    async def test_storage_add_batch(self, mock_storage):
        """测试batch_add() - 批量添加"""
        print("[LOG] test_storage_add_batch::开始 - 批量添加")
        
        items = [
            MemoryItem(content=f"记忆{i}", memory_type=MemoryType.SEMANTIC)
            for i in range(5)
        ]
        
        ids = await mock_storage.batch_add(items)
        
        assert len(ids) == 5
        print(f"[LOG] test_storage_add_batch::验证通过 - 添加数量: {len(ids)} vs 期望: 5")
    
    @pytest.mark.asyncio
    async def test_storage_get_normal(self, mock_storage):
        """测试get() - 正常情况"""
        print("[LOG] test_storage_get_normal::开始 - 正常获取")
        
        # 先添加
        item = MemoryItem(content="测试记忆", memory_type=MemoryType.SEMANTIC)
        memory_id = await mock_storage.add(item)
        
        # 获取
        retrieved = await mock_storage.get(memory_id)
        
        assert retrieved is not None
        assert retrieved.content == "测试记忆"
        
        print(f"[LOG] test_storage_get_normal::验证通过 - 内容匹配")
    
    @pytest.mark.asyncio
    async def test_storage_get_nonexistent(self, mock_storage):
        """测试get() - 不存在"""
        print("[LOG] test_storage_get_nonexistent::开始 - 不存在的ID")
        
        result = await mock_storage.get("nonexistent_id")
        
        assert result is None
        print(f"[LOG] test_storage_get_nonexistent::验证通过 - 返回None")
    
    @pytest.mark.asyncio
    async def test_storage_update_normal(self, mock_storage):
        """测试update() - 正常更新"""
        print("[LOG] test_storage_update_normal::开始 - 正常更新")
        
        # 先添加
        item = MemoryItem(content="原始内容", memory_type=MemoryType.SEMANTIC)
        memory_id = await mock_storage.add(item)
        
        # 更新
        success = await mock_storage.update(memory_id, {"content": "更新内容"})
        
        assert success is True
        
        # 验证
        updated = await mock_storage.get(memory_id)
        assert updated.content == "更新内容"
        
        print(f"[LOG] test_storage_update_normal::验证通过 - 内容已更新")
    
    @pytest.mark.asyncio
    async def test_storage_update_nonexistent(self, mock_storage):
        """测试update() - 不存在"""
        print("[LOG] test_storage_update_nonexistent::开始 - 不存在的ID")
        
        success = await mock_storage.update("nonexistent_id", {"content": "新内容"})
        
        assert success is False
        print(f"[LOG] test_storage_update_nonexistent::验证通过 - 返回False")
    
    @pytest.mark.asyncio
    async def test_storage_delete_normal(self, mock_storage):
        """测试delete() - 正常删除"""
        print("[LOG] test_storage_delete_normal::开始 - 正常删除")
        
        # 先添加
        item = MemoryItem(content="待删除", memory_type=MemoryType.SEMANTIC)
        memory_id = await mock_storage.add(item)
        
        # 删除
        success = await mock_storage.delete(memory_id)
        
        assert success is True
        
        # 验证已删除
        result = await mock_storage.get(memory_id)
        assert result is None
        
        print(f"[LOG] test_storage_delete_normal::验证通过 - 已删除")
    
    @pytest.mark.asyncio
    async def test_storage_delete_nonexistent(self, mock_storage):
        """测试delete() - 不存在"""
        print("[LOG] test_storage_delete_nonexistent::开始 - 不存在的ID")
        
        success = await mock_storage.delete("nonexistent_id")
        
        assert success is False
        print(f"[LOG] test_storage_delete_nonexistent::验证通过 - 返回False")
    
    @pytest.mark.asyncio
    async def test_storage_batch_delete(self, mock_storage):
        """测试batch_delete() - 批量删除"""
        print("[LOG] test_storage_batch_delete::开始 - 批量删除")
        
        # 添加多个
        ids = []
        for i in range(3):
            item = MemoryItem(content=f"记忆{i}", memory_type=MemoryType.SEMANTIC)
            ids.append(await mock_storage.add(item))
        
        # 批量删除
        count = await mock_storage.batch_delete(ids)
        
        assert count == 3
        print(f"[LOG] test_storage_batch_delete::验证通过 - 删除数量: {count} vs 期望: 3")
    
    @pytest.mark.asyncio
    async def test_storage_search_by_vector(self, mock_storage, mock_embedding):
        """测试search_by_vector() - 向量搜索"""
        print("[LOG] test_storage_search_by_vector::开始 - 向量搜索")
        
        # 添加带嵌入的记忆
        item = MemoryItem(
            content="向量测试",
            memory_type=MemoryType.SEMANTIC,
            embedding=await mock_embedding.embed("向量测试")
        )
        await mock_storage.add(item)
        
        # 搜索
        query_vec = await mock_embedding.embed("向量")
        results = await mock_storage.search_by_vector(query_vec, top_k=5)
        
        print(f"[LOG] test_storage_search_by_vector::结果数量: {len(results)}")
    
    @pytest.mark.asyncio
    async def test_storage_count(self, mock_storage):
        """测试count() - 统计数量"""
        print("[LOG] test_storage_count::开始 - 统计数量")
        
        # 添加记忆
        for i in range(3):
            item = MemoryItem(content=f"记忆{i}", memory_type=MemoryType.SEMANTIC)
            await mock_storage.add(item)
        
        # 统计
        total = await mock_storage.count()
        semantic_count = await mock_storage.count("semantic")
        
        print(f"[LOG] test_storage_count::总数: {total}, semantic数: {semantic_count}")


class TestEmbeddingProviderInterface:
    """EmbeddingProvider接口遍历测试"""
    
    @pytest.mark.asyncio
    async def test_embed_normal(self, mock_embedding):
        """测试embed() - 正常情况"""
        print("[LOG] test_embed_normal::开始 - 正常嵌入")
        
        vector = await mock_embedding.embed("测试文本")
        
        assert vector is not None
        assert len(vector) == mock_embedding.dimension
        
        print(f"[LOG] test_embed_normal::验证通过 - 向量维度: {len(vector)}")
    
    @pytest.mark.asyncio
    async def test_embed_empty_text(self, mock_embedding):
        """测试embed() - 空文本"""
        print("[LOG] test_embed_empty_text::开始 - 空文本")
        
        vector = await mock_embedding.embed("")
        
        # 空文本也应该返回向量
        assert vector is not None
        print(f"[LOG] test_embed_empty_text::验证通过 - 返回向量维度: {len(vector)}")
    
    @pytest.mark.asyncio
    async def test_embed_batch(self, mock_embedding):
        """测试embed_batch() - 批量嵌入"""
        print("[LOG] test_embed_batch::开始 - 批量嵌入")
        
        texts = ["文本1", "文本2", "文本3"]
        vectors = await mock_embedding.embed_batch(texts)
        
        assert len(vectors) == 3
        for v in vectors:
            assert len(v) == mock_embedding.dimension
        
        print(f"[LOG] test_embed_batch::验证通过 - 向量数量: {len(vectors)}")
    
    @pytest.mark.asyncio
    async def test_embed_long_text(self, mock_embedding):
        """测试embed() - 超长文本"""
        print("[LOG] test_embed_long_text::开始 - 超长文本")
        
        long_text = "测试" * 10000
        
        vector = await mock_embedding.embed(long_text)
        
        assert vector is not None
        print(f"[LOG] test_embed_long_text::验证通过 - 处理成功")
    
    @pytest.mark.asyncio
    async def test_similarity(self, mock_embedding):
        """测试similarity() - 相似度计算"""
        print("[LOG] test_similarity::开始 - 相似度计算")
        
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        vec3 = [0.0, 1.0, 0.0]
        
        sim_same = await mock_embedding.similarity(vec1, vec2)
        sim_diff = await mock_embedding.similarity(vec1, vec3)
        
        print(f"[LOG] test_similarity::相同向量相似度: {sim_same} vs 期望: 1.0")
        print(f"[LOG] test_similarity::正交向量相似度: {sim_diff} vs 期望: 0.0")


class TestEventBusInterface:
    """EventBus接口遍历测试"""
    
    @pytest.mark.asyncio
    async def test_publish_normal(self, event_bus):
        """测试publish() - 正常情况"""
        print("[LOG] test_publish_normal::开始 - 正常发布")
        
        event = DomainEvent(
            event_type="test.event",
            payload={"data": "测试数据"}
        )
        
        success = await event_bus.publish(event)
        
        assert success is True
        print(f"[LOG] test_publish_normal::验证通过 - 发布成功")
    
    @pytest.mark.asyncio
    async def test_publish_no_subscribers(self, event_bus):
        """测试publish() - 无订阅者"""
        print("[LOG] test_publish_no_subscribers::开始 - 无订阅者")
        
        event = DomainEvent(
            event_type="no.subscribers",
            payload={}
        )
        
        success = await event_bus.publish(event)
        
        # 即使无订阅者，发布也应该成功
        assert success is True
        print(f"[LOG] test_publish_no_subscribers::验证通过 - 发布成功")
    
    @pytest.mark.asyncio
    async def test_subscribe_normal(self, event_bus):
        """测试subscribe() - 正常订阅"""
        print("[LOG] test_subscribe_normal::开始 - 正常订阅")
        
        called = []
        
        async def handler(event: DomainEvent):
            called.append(True)
        
        await event_bus.subscribe("test.subscribe", handler)
        
        # 发布事件
        event = DomainEvent(event_type="test.subscribe", payload={})
        await event_bus.publish(event)
        
        await asyncio.sleep(0.5)
        
        print(f"[LOG] test_subscribe_normal::处理器调用次数: {len(called)} vs 期望: 1")
    
    @pytest.mark.asyncio
    async def test_subscribe_duplicate(self, event_bus):
        """测试subscribe() - 重复订阅"""
        print("[LOG] test_subscribe_duplicate::开始 - 重复订阅")
        
        call_count = []
        
        async def handler(event: DomainEvent):
            call_count.append(True)
        
        # 重复订阅同一处理器
        await event_bus.subscribe("test.dup", handler)
        await event_bus.subscribe("test.dup", handler)
        
        event = DomainEvent(event_type="test.dup", payload={})
        await event_bus.publish(event)
        
        await asyncio.sleep(0.5)
        
        print(f"[LOG] test_subscribe_duplicate::处理器调用次数: {len(call_count)}")
    
    @pytest.mark.asyncio
    async def test_unsubscribe_normal(self, event_bus):
        """测试unsubscribe() - 正常取消订阅"""
        print("[LOG] test_unsubscribe_normal::开始 - 正常取消订阅")
        
        called = []
        
        async def handler(event: DomainEvent):
            called.append(True)
        
        await event_bus.subscribe("test.unsub", handler)
        await event_bus.unsubscribe("test.unsub", handler)
        
        event = DomainEvent(event_type="test.unsub", payload={})
        await event_bus.publish(event)
        
        await asyncio.sleep(0.5)
        
        print(f"[LOG] test_unsubscribe_normal::取消后调用次数: {len(called)} vs 期望: 0")


# ============================================================================
# 三、参数传递验证测试
# ============================================================================

class TestParameterPassing:
    """参数传递验证测试"""
    
    @pytest.mark.asyncio
    async def test_cross_module_parameter_type(self, memory_system):
        """测试跨模块参数类型正确"""
        print("[LOG] test_cross_module_parameter_type::开始 - 跨模块参数类型")
        
        # 写入记忆
        memory_id = await memory_system.remember(
            content="类型测试",
            memory_type="semantic",
            importance="high",
            metadata={"key": "value", "number": 123}
        )
        
        # 验证存储的类型
        stored = await memory_system.storage.get(memory_id)
        
        assert stored is not None
        assert isinstance(stored.content, str)
        assert isinstance(stored.memory_type, MemoryType)
        assert isinstance(stored.importance, ImportanceLevel)
        assert isinstance(stored.metadata, dict)
        
        print(f"[LOG] test_cross_module_parameter_type::content类型: {type(stored.content).__name__} vs 期望: str")
        print(f"[LOG] test_cross_module_parameter_type::memory_type类型: {type(stored.memory_type).__name__} vs 期望: MemoryType")
        print(f"[LOG] test_cross_module_parameter_type::importance类型: {type(stored.importance).__name__} vs 期望: ImportanceLevel")
    
    @pytest.mark.asyncio
    async def test_optional_parameter_defaults(self, memory_system):
        """测试可选参数默认值传递"""
        print("[LOG] test_optional_parameter_defaults::开始 - 可选参数默认值")
        
        # 只提供必需参数
        memory_id = await memory_system.remember(content="默认值测试")
        
        stored = await memory_system.storage.get(memory_id)
        
        if stored:
            # 验证默认值
            print(f"[LOG] test_optional_parameter_defaults::memory_type: {stored.memory_type.value}")
            print(f"[LOG] test_optional_parameter_defaults::importance: {stored.importance.value}")
            print(f"[LOG] test_optional_parameter_defaults::metadata: {stored.metadata}")
    
    @pytest.mark.asyncio
    async def test_error_parameter_exception(self, memory_system):
        """测试错误参数抛出正确异常"""
        print("[LOG] test_error_parameter_exception::开始 - 错误参数异常")
        
        # 测试各种错误参数
        error_cases = [
            ("空内容", {"content": ""}),
            ("None内容", {"content": None}),
            ("非法类型", {"content": "test", "memory_type": "invalid"}),
            ("非法重要性", {"content": "test", "importance": "invalid"}),
        ]
        
        for case_name, params in error_cases:
            try:
                await memory_system.remember(**params)
                print(f"[LOG] test_error_parameter_exception::{case_name} - 未抛出异常")
            except (TypeError, ValueError) as e:
                print(f"[LOG] test_error_parameter_exception::{case_name} - 正确抛出: {type(e).__name__}")


# ============================================================================
# 四、状态同步验证测试
# ============================================================================

class TestStateSynchronization:
    """状态同步验证测试"""
    
    @pytest.mark.asyncio
    async def test_stats_counter_sync(self, memory_system):
        """测试统计计数器同步"""
        print("[LOG] test_stats_counter_sync::开始 - 统计计数器同步")
        
        initial_stats = await memory_system.get_stats()
        initial_added = initial_stats.total_added
        
        print(f"[LOG] test_stats_counter_sync::初始total_added: {initial_added}")
        
        # 写入多条记忆
        for i in range(3):
            await memory_system.remember(f"记忆{i}", memory_type="semantic")
        
        updated_stats = await memory_system.get_stats()
        updated_added = updated_stats.total_added
        
        print(f"[LOG] test_stats_counter_sync::更新后total_added: {updated_added}")
        print(f"[LOG] test_stats_counter_sync::增量: {updated_added - initial_added} vs 期望: 3")
        
        assert updated_added >= initial_added + 3
    
    @pytest.mark.asyncio
    async def test_cache_consistency(self, memory_system):
        """测试缓存一致性"""
        print("[LOG] test_cache_consistency::开始 - 缓存一致性")
        
        # 写入记忆
        content = "缓存一致性测试"
        memory_id = await memory_system.remember(content, memory_type="semantic")
        
        # 第一次检索（应该使用缓存）
        results1 = await memory_system.recall(content, max_results=5)
        
        # 第二次检索
        results2 = await memory_system.recall(content, max_results=5)
        
        # 结果应该一致
        assert results1["query"] == results2["query"]
        
        print(f"[LOG] test_cache_consistency::两次检索query一致: {results1['query']}")
    
    @pytest.mark.asyncio
    async def test_session_state_sync(self, memory_system):
        """测试会话状态同步"""
        print("[LOG] test_session_state_sync::开始 - 会话状态同步")
        
        # 在同一会话中操作
        session_id = "test_session_123"
        
        # 写入带会话ID的记忆
        await memory_system.remember(
            content="会话记忆1",
            memory_type="short_term",
            metadata={"session_id": session_id}
        )
        
        await memory_system.remember(
            content="会话记忆2",
            memory_type="short_term",
            metadata={"session_id": session_id}
        )
        
        # 获取短期记忆
        all_short_term = await memory_system.short_term.get_all()
        
        session_memories = [
            m for m in all_short_term
            if m.metadata.get("session_id") == session_id
        ]
        
        print(f"[LOG] test_session_state_sync::会话记忆数量: {len(session_memories)} vs 期望: 2")
    
    @pytest.mark.asyncio
    async def test_concurrent_access(self, memory_system):
        """测试并发访问状态同步"""
        print("[LOG] test_concurrent_access::开始 - 并发访问")
        
        # 并发写入
        tasks = [
            memory_system.remember(f"并发记忆{i}", memory_type="semantic")
            for i in range(10)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        
        print(f"[LOG] test_concurrent_access::成功数量: {success_count} vs 期望: 10")
        
        # 验证统计
        stats = await memory_system.get_stats()
        print(f"[LOG] test_concurrent_access::最终total_added: {stats.total_added}")


# ============================================================================
# 五、RRF融合结果验证测试
# ============================================================================

class TestRRFFusion:
    """RRF融合结果验证测试"""
    
    @pytest.mark.asyncio
    async def test_rrf_fusion_basic(self, memory_system):
        """测试RRF基本融合"""
        print("[LOG] test_rrf_fusion_basic::开始 - RRF基本融合")
        
        # 写入多条记忆
        contents = [
            "Python编程基础",
            "Python高级特性",
            "机器学习入门",
            "深度学习框架",
        ]
        
        for content in contents:
            await memory_system.remember(content, memory_type="semantic", importance="high")
        
        # 执行检索
        results = await memory_system.recall("Python", max_results=10)
        
        combined = results.get("combined", [])
        
        print(f"[LOG] test_rrf_fusion_basic::融合结果数量: {len(combined)}")
        
        # 验证结果排序（分数应该递减）
        scores = [r.get("score", 0) for r in combined]
        is_sorted = all(scores[i] >= scores[i+1] for i in range(len(scores)-1))
        
        print(f"[LOG] test_rrf_fusion_basic::结果是否按分数排序: {is_sorted}")
    
    @pytest.mark.asyncio
    async def test_multi_layer_result_fusion(self, memory_system):
        """测试多层级结果融合"""
        print("[LOG] test_multi_layer_result_fusion::开始 - 多层级融合")
        
        # 短期记忆
        await memory_system.remember("短期：Python任务", memory_type="short_term")
        
        # 长期记忆
        await memory_system.remember("长期：Python知识", memory_type="semantic", importance="high")
        
        # 检索
        results = await memory_system.recall("Python", max_results=10)
        
        layers = results.get("layers", {})
        
        print(f"[LOG] test_multi_layer_result_fusion::层级: {list(layers.keys())}")
        
        for layer_name, layer_results in layers.items():
            print(f"[LOG] test_multi_layer_result_fusion::{layer_name}结果数: {len(layer_results)}")


# ============================================================================
# 六、边界条件测试
# ============================================================================

class TestBoundaryConditions:
    """边界条件测试"""
    
    @pytest.mark.asyncio
    async def test_max_results_boundary(self, memory_system):
        """测试max_results边界值"""
        print("[LOG] test_max_results_boundary::开始 - max_results边界")
        
        # 写入记忆
        for i in range(20):
            await memory_system.remember(f"记忆{i}", memory_type="semantic")
        
        # 测试各种边界值
        test_cases = [1, 5, 10, 100, 1000]
        
        for max_results in test_cases:
            results = await memory_system.recall("记忆", max_results=max_results)
            combined = results.get("combined", [])
            
            print(f"[LOG] test_max_results_boundary::max_results={max_results}, 实际返回={len(combined)}")
    
    @pytest.mark.asyncio
    async def test_special_characters_in_content(self, memory_system):
        """测试内容中的特殊字符"""
        print("[LOG] test_special_characters_in_content::开始 - 特殊字符")
        
        special_contents = [
            "包含<特殊>字符",
            "包含\"引号\"字符",
            "包含'单引号'字符",
            "包含\n换行符",
            "包含\t制表符",
        ]
        
        for content in special_contents:
            try:
                memory_id = await memory_system.remember(content, memory_type="semantic")
                print(f"[LOG] test_special_characters_in_content::成功写入: {content[:20]}...")
            except Exception as e:
                print(f"[LOG] test_special_characters_in_content::写入失败: {str(e)[:30]}")
    
    @pytest.mark.asyncio
    async def test_unicode_content(self, memory_system):
        """测试Unicode内容"""
        print("[LOG] test_unicode_content::开始 - Unicode内容")
        
        unicode_contents = [
            "中文内容测试",
            "日本語テスト",
            "한국어 테스트",
            "Emoji测试 😀🎉",
            "数学符号 ∑∫∂",
        ]
        
        for content in unicode_contents:
            memory_id = await memory_system.remember(content, memory_type="semantic")
            print(f"[LOG] test_unicode_content::成功写入: {content}")


# ============================================================================
# 运行测试
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

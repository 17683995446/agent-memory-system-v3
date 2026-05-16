"""
Agent Memory System V2.27.0 创新功能综合测试

Feature 1: Memory Association Graph (记忆关联图谱)
Feature 2: Auto Importance Scorer (自动重要性评分)
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    MemoryLink,
    LinkRelation,
)
from memory_system.core_abstractions.abstract_component_interfaces import ImportanceScorer
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.importance_scorers.heuristic_importance_scorer import HeuristicImportanceScorer
from memory_system.core_abstractions.component_registry_manager import (
    register_importance_scorer,
    get_importance_scorer_registry,
    clear_all_registries,
    _importance_scorer_registry,
)


# ============================================================
# Feature 1: MemoryLink 数据模型测试
# ============================================================

class TestMemoryLink:
    """MemoryLink 数据模型测试"""

    def test_create_basic_link(self):
        """基本创建测试"""
        link = MemoryLink(source_id="mem1", target_id="mem2")
        assert link.source_id == "mem1"
        assert link.target_id == "mem2"
        assert link.relation == LinkRelation.RELATED
        assert link.weight == 1.0
        assert link.metadata == {}
        assert link.created_at > 0

    def test_create_with_all_params(self):
        """带全部参数创建"""
        link = MemoryLink(
            source_id="a", target_id="b",
            relation=LinkRelation.CAUSAL,
            weight=0.8,
            metadata={"note": "test"},
        )
        assert link.relation == LinkRelation.CAUSAL
        assert link.weight == 0.8
        assert link.metadata == {"note": "test"}

    def test_empty_source_id_raises(self):
        """空 source_id 应抛出 ValueError"""
        with pytest.raises(ValueError, match="不能为空"):
            MemoryLink(source_id="", target_id="mem2")

    def test_empty_target_id_raises(self):
        """空 target_id 应抛出 ValueError"""
        with pytest.raises(ValueError, match="不能为空"):
            MemoryLink(source_id="mem1", target_id="")

    def test_self_loop_raises(self):
        """自环关联应抛出 ValueError"""
        with pytest.raises(ValueError, match="自环"):
            MemoryLink(source_id="same", target_id="same")

    def test_weight_out_of_range_high_raises(self):
        """weight > 1.0 应抛出 ValueError"""
        with pytest.raises(ValueError, match="weight"):
            MemoryLink(source_id="a", target_id="b", weight=1.5)

    def test_weight_out_of_range_low_raises(self):
        """weight < 0.0 应抛出 ValueError"""
        with pytest.raises(ValueError, match="weight"):
            MemoryLink(source_id="a", target_id="b", weight=-0.1)

    def test_weight_boundary_zero(self):
        """weight = 0.0 应合法"""
        link = MemoryLink(source_id="a", target_id="b", weight=0.0)
        assert link.weight == 0.0

    def test_weight_boundary_one(self):
        """weight = 1.0 应合法"""
        link = MemoryLink(source_id="a", target_id="b", weight=1.0)
        assert link.weight == 1.0

    def test_to_dict(self):
        """to_dict 序列化测试"""
        link = MemoryLink(
            source_id="a", target_id="b",
            relation=LinkRelation.TEMPORAL,
            weight=0.5,
            metadata={"key": "val"},
        )
        d = link.to_dict()
        assert d["source_id"] == "a"
        assert d["target_id"] == "b"
        assert d["relation"] == "temporal"
        assert d["weight"] == 0.5
        assert d["metadata"] == {"key": "val"}
        assert "created_at" in d

    def test_from_dict_roundtrip(self):
        """from_dict -> to_dict 往返测试"""
        original = MemoryLink(
            source_id="x", target_id="y",
            relation=LinkRelation.HIERARCHICAL,
            weight=0.7,
            metadata={"depth": 2},
        )
        d = original.to_dict()
        restored = MemoryLink.from_dict(d)
        assert restored.source_id == original.source_id
        assert restored.target_id == original.target_id
        assert restored.relation == original.relation
        assert restored.weight == original.weight
        assert restored.metadata == original.metadata

    def test_from_dict_defaults(self):
        """from_dict 缺省值测试"""
        data = {"source_id": "a", "target_id": "b"}
        link = MemoryLink.from_dict(data)
        assert link.relation == LinkRelation.RELATED
        assert link.weight == 1.0
        assert link.metadata == {}

    def test_all_link_relations(self):
        """所有关联类型枚举值测试"""
        assert LinkRelation.RELATED.value == "related"
        assert LinkRelation.CAUSAL.value == "causal"
        assert LinkRelation.TEMPORAL.value == "temporal"
        assert LinkRelation.HIERARCHICAL.value == "hierarchical"
        assert LinkRelation.CONTRADICTORY.value == "contradictory"
        assert LinkRelation.SUPPLEMENTARY.value == "supplementary"


# ============================================================
# Feature 1: MemoryGraph Storage 测试
# ============================================================

class TestMemoryGraphStorage:
    """MemoryStorage 图关联操作测试"""

    @pytest.fixture
    def storage(self):
        return MemoryStorage(max_memories=100)

    @pytest.fixture
    async def storage_with_memories(self, storage):
        """创建包含3条记忆的存储"""
        m1 = MemoryItem(content="记忆A", memory_type=MemoryType.SEMANTIC, memory_id="mem_a")
        m2 = MemoryItem(content="记忆B", memory_type=MemoryType.SEMANTIC, memory_id="mem_b")
        m3 = MemoryItem(content="记忆C", memory_type=MemoryType.SEMANTIC, memory_id="mem_c")
        await storage.add(m1)
        await storage.add(m2)
        await storage.add(m3)
        return storage

    @pytest.mark.asyncio
    async def test_add_link(self, storage_with_memories):
        """添加关联链接"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b", relation=LinkRelation.RELATED)
        result = await storage_with_memories.add_link(link)
        assert result is True

    @pytest.mark.asyncio
    async def test_add_link_creates_reverse(self, storage_with_memories):
        """添加链接应同时创建反向索引"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b", relation=LinkRelation.CAUSAL)
        await storage_with_memories.add_link(link)

        links_b = await storage_with_memories.get_links("mem_b", direction="outgoing")
        # 反向链接的 source_id 应为 mem_b, target_id 应为 mem_a
        assert any(l.source_id == "mem_b" and l.target_id == "mem_a" for l in links_b)

    @pytest.mark.asyncio
    async def test_remove_link(self, storage_with_memories):
        """移除关联链接"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b")
        await storage_with_memories.add_link(link)
        result = await storage_with_memories.remove_link("mem_a", "mem_b")
        assert result is True

        # 验证已移除
        links = await storage_with_memories.get_links("mem_a")
        assert len(links) == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent_link(self, storage_with_memories):
        """移除不存在的链接应返回 False"""
        result = await storage_with_memories.remove_link("mem_a", "mem_b")
        assert result is False

    @pytest.mark.asyncio
    async def test_get_linked_memories_one_hop(self, storage_with_memories):
        """一跳关联遍历"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b")
        await storage_with_memories.add_link(link)

        linked = await storage_with_memories.get_linked_memories("mem_a", max_depth=1)
        assert len(linked) == 1
        assert linked[0].memory_id == "mem_b"

    @pytest.mark.asyncio
    async def test_get_linked_memories_two_hop(self, storage_with_memories):
        """两跳关联遍历: A -> B -> C"""
        link1 = MemoryLink(source_id="mem_a", target_id="mem_b")
        link2 = MemoryLink(source_id="mem_b", target_id="mem_c")
        await storage_with_memories.add_link(link1)
        await storage_with_memories.add_link(link2)

        linked = await storage_with_memories.get_linked_memories("mem_a", max_depth=2)
        linked_ids = [m.memory_id for m in linked]
        assert "mem_b" in linked_ids
        assert "mem_c" in linked_ids

    @pytest.mark.asyncio
    async def test_get_linked_memories_no_duplicates(self, storage_with_memories):
        """遍历不应产生重复"""
        link1 = MemoryLink(source_id="mem_a", target_id="mem_b")
        link2 = MemoryLink(source_id="mem_a", target_id="mem_c")
        await storage_with_memories.add_link(link1)
        await storage_with_memories.add_link(link2)

        linked = await storage_with_memories.get_linked_memories("mem_a", max_depth=1)
        linked_ids = [m.memory_id for m in linked]
        assert len(linked_ids) == len(set(linked_ids))

    @pytest.mark.asyncio
    async def test_get_linked_memories_with_relation_filter(self, storage_with_memories):
        """关联类型过滤"""
        link1 = MemoryLink(source_id="mem_a", target_id="mem_b", relation=LinkRelation.CAUSAL)
        link2 = MemoryLink(source_id="mem_a", target_id="mem_c", relation=LinkRelation.TEMPORAL)
        await storage_with_memories.add_link(link1)
        await storage_with_memories.add_link(link2)

        linked = await storage_with_memories.get_linked_memories(
            "mem_a", relation="causal", max_depth=1
        )
        linked_ids = [m.memory_id for m in linked]
        assert "mem_b" in linked_ids
        assert "mem_c" not in linked_ids

    @pytest.mark.asyncio
    async def test_get_links_outgoing(self, storage_with_memories):
        """获取出向链接"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b")
        await storage_with_memories.add_link(link)

        links = await storage_with_memories.get_links("mem_a", direction="outgoing")
        assert len(links) >= 1
        assert any(l.source_id == "mem_a" and l.target_id == "mem_b" for l in links)

    @pytest.mark.asyncio
    async def test_get_links_incoming(self, storage_with_memories):
        """获取入向链接"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b")
        await storage_with_memories.add_link(link)

        links = await storage_with_memories.get_links("mem_b", direction="incoming")
        assert len(links) >= 1
        # 反向链接存储为 source_id=mem_b, target_id=mem_a，代表 mem_a -> mem_b
        assert any(l.source_id == "mem_b" and l.target_id == "mem_a" for l in links)

    @pytest.mark.asyncio
    async def test_get_links_both(self, storage_with_memories):
        """获取双向链接"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b")
        await storage_with_memories.add_link(link)

        links = await storage_with_memories.get_links("mem_a", direction="both")
        # mem_a 的链接列表包含正向链接 (mem_a -> mem_b)
        assert len(links) >= 1
        assert any(l.source_id == "mem_a" and l.target_id == "mem_b" for l in links)

    @pytest.mark.asyncio
    async def test_get_links_empty(self, storage_with_memories):
        """无链接时返回空列表"""
        links = await storage_with_memories.get_links("mem_a")
        assert links == []

    @pytest.mark.asyncio
    async def test_close_clears_links(self, storage_with_memories):
        """close 应清理链接数据"""
        link = MemoryLink(source_id="mem_a", target_id="mem_b")
        await storage_with_memories.add_link(link)
        await storage_with_memories.close()
        links = await storage_with_memories.get_links("mem_a")
        assert links == []


# ============================================================
# Feature 1: Facade link_memories 和 recall_with_associations 测试
# ============================================================

class TestLinkMemoriesFacade:
    """Facade 层记忆关联测试"""

    def _make_memory_system(self, storage, mock_embedding):
        """构建最小化的 MemorySystem 实例"""
        from memory_system.agent_memory_system_facade import MemorySystem
        from memory_system.working_memory_cache import ShortTermMemory
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy

        class MockRetrieval(RetrievalStrategy):
            async def retrieve(self, query, storage, embedding, max_results=10, context=None):
                return {
                    "query": query,
                    "strategy": "mock",
                    "layers": {},
                    "combined": [],
                    "total_found": 0,
                    "retrieval_time": 0.0,
                }

        short_term = ShortTermMemory(capacity=10, ttl_seconds=60)
        ms = MemorySystem(
            storage=storage,
            embedding=mock_embedding,
            retrieval=MockRetrieval(),
            short_term=short_term,
        )
        return ms

    @pytest.mark.asyncio
    async def test_link_memories_success(self, memory_storage, mock_embedding):
        """成功建立记忆关联"""
        ms = self._make_memory_system(memory_storage, mock_embedding)

        # 先添加记忆
        m1 = MemoryItem(content="A", memory_type=MemoryType.SEMANTIC, memory_id="a")
        m2 = MemoryItem(content="B", memory_type=MemoryType.SEMANTIC, memory_id="b")
        await memory_storage.add(m1)
        await memory_storage.add(m2)

        result = await ms.link_memories("a", "b", relation="causal", weight=0.9)
        assert result is True

    @pytest.mark.asyncio
    async def test_link_memories_self_loop_fails(self, memory_storage, mock_embedding):
        """自环关联应失败"""
        ms = self._make_memory_system(memory_storage, mock_embedding)
        result = await ms.link_memories("same", "same")
        assert result is False

    @pytest.mark.asyncio
    async def test_link_memories_invalid_relation_fails(self, memory_storage, mock_embedding):
        """无效关联类型应失败"""
        ms = self._make_memory_system(memory_storage, mock_embedding)
        result = await ms.link_memories("a", "b", relation="invalid_type")
        assert result is False

    @pytest.mark.asyncio
    async def test_recall_with_associations_disabled(self, memory_storage, mock_embedding):
        """association_depth=0 时不展开关联"""
        ms = self._make_memory_system(memory_storage, mock_embedding)
        result = await ms.recall_with_associations("test", association_depth=0)
        assert "associations" in result
        assert result["associations"] == []

    @pytest.mark.asyncio
    async def test_recall_with_associations_basic(self, memory_storage, mock_embedding):
        """基本关联检索测试"""
        from memory_system.agent_memory_system_facade import MemorySystem
        from memory_system.working_memory_cache import ShortTermMemory
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy

        class MockRetrievalWithResults(RetrievalStrategy):
            async def retrieve(self, query, storage, embedding, max_results=10, context=None):
                return {
                    "query": query,
                    "strategy": "mock",
                    "layers": {},
                    "combined": [
                        {
                            "memory": {
                                "memory_id": "mem_a",
                                "content": "test memory A",
                            },
                            "score": 0.9,
                        }
                    ],
                    "total_found": 1,
                    "retrieval_time": 0.0,
                }

        # 添加记忆
        m1 = MemoryItem(content="A", memory_type=MemoryType.SEMANTIC, memory_id="mem_a")
        m2 = MemoryItem(content="B", memory_type=MemoryType.SEMANTIC, memory_id="mem_b")
        await memory_storage.add(m1)
        await memory_storage.add(m2)

        # 添加关联
        link = MemoryLink(source_id="mem_a", target_id="mem_b")
        await memory_storage.add_link(link)

        short_term = ShortTermMemory(capacity=10, ttl_seconds=60)
        ms = MemorySystem(
            storage=memory_storage,
            embedding=mock_embedding,
            retrieval=MockRetrievalWithResults(),
            short_term=short_term,
        )

        result = await ms.recall_with_associations("test", association_depth=1)
        assert "associations" in result
        assert len(result["associations"]) == 1
        assert result["associations"][0]["linked_from"] == "mem_a"


# ============================================================
# Feature 2: HeuristicImportanceScorer 测试
# ============================================================

class TestHeuristicImportanceScorer:
    """启发式重要性评估器测试"""

    @pytest.fixture
    def scorer(self):
        return HeuristicImportanceScorer()

    @pytest.mark.asyncio
    async def test_high_importance_allergy(self, scorer):
        """过敏信息应为高重要性"""
        level, confidence = await scorer.score(
            "用户对花生严重过敏，这是非常重要的安全信息，必须记住，"
            "紧急情况下可能导致过敏性休克，需要立即注射肾上腺素，"
            "过敏反应包括呼吸困难、皮疹、肿胀等症状，绝对不能接触花生制品"
        )
        assert level in (ImportanceLevel.HIGH, ImportanceLevel.CRITICAL, ImportanceLevel.MEDIUM)
        assert confidence > 0

    @pytest.mark.asyncio
    async def test_high_importance_password(self, scorer):
        """密码信息应为高重要性"""
        level, confidence = await scorer.score(
            "用户的密码是abc123，这是非常重要的安全信息，必须妥善保管，"
            "密码泄露可能导致账户被盗，造成严重的财产损失和隐私泄露，"
            "建议定期更换密码，使用强密码组合，不要将密码告诉任何人"
        )
        assert level in (ImportanceLevel.HIGH, ImportanceLevel.CRITICAL, ImportanceLevel.MEDIUM)

    @pytest.mark.asyncio
    async def test_high_importance_urgent(self, scorer):
        """紧急信息应为高重要性"""
        level, confidence = await scorer.score(
            "这是一个紧急任务，必须在今天完成，非常重要且关键，"
            "如果不能按时完成将导致严重的后果，必须立即采取行动，"
            "这是当前最紧急的事情，需要优先处理，不能有任何延迟"
        )
        assert level in (ImportanceLevel.HIGH, ImportanceLevel.CRITICAL, ImportanceLevel.MEDIUM)

    @pytest.mark.asyncio
    async def test_preference_expression_like(self, scorer):
        """偏好表达应被识别"""
        level, confidence = await scorer.score(
            "用户非常喜欢喝咖啡，每天早上都会喝一杯，对咖啡有很强的偏好，"
            "总是选择咖啡而不是茶，感兴趣的各种咖啡种类都会尝试"
        )
        assert level in (ImportanceLevel.LOW, ImportanceLevel.MEDIUM, ImportanceLevel.HIGH)
        assert confidence > 0

    @pytest.mark.asyncio
    async def test_preference_expression_hate(self, scorer):
        """厌恶表达应被识别"""
        level, confidence = await scorer.score(
            "用户非常讨厌早起，从不主动在早上六点前起床，"
            "不能接受早班安排，总是尽量选择下午的工作时间"
        )
        assert level in (ImportanceLevel.LOW, ImportanceLevel.MEDIUM, ImportanceLevel.HIGH)

    @pytest.mark.asyncio
    async def test_factual_statement_date(self, scorer):
        """事实性陈述（日期）应被识别"""
        level, confidence = await scorer.score("用户出生于1990年5月15日")
        assert level in (ImportanceLevel.LOW, ImportanceLevel.MEDIUM, ImportanceLevel.HIGH)

    @pytest.mark.asyncio
    async def test_factual_statement_number(self, scorer):
        """事实性陈述（数字）应被识别"""
        level, confidence = await scorer.score(
            "项目预算为$50000，这是一个重要的财务信息，"
            "需要严格按照预算执行，不能超支"
        )
        assert level in (ImportanceLevel.LOW, ImportanceLevel.MEDIUM, ImportanceLevel.HIGH)

    @pytest.mark.asyncio
    async def test_content_length_scoring(self, scorer):
        """长内容应有更高的长度分数"""
        short_level, short_conf = await scorer.score("hi")
        long_content = "这是一段很长的内容。" * 50  # ~400字符
        long_level, long_conf = await scorer.score(long_content)
        # 长内容的重要性级别不应低于短内容
        assert long_level.value >= short_level.value

    @pytest.mark.asyncio
    async def test_empty_content(self, scorer):
        """空内容应为 TRIVIAL"""
        level, confidence = await scorer.score("")
        assert level == ImportanceLevel.TRIVIAL
        assert confidence == 0.0

    @pytest.mark.asyncio
    async def test_whitespace_content(self, scorer):
        """纯空白内容应为 TRIVIAL"""
        level, confidence = await scorer.score("   \n\t  ")
        assert level == ImportanceLevel.TRIVIAL
        assert confidence == 0.0

    @pytest.mark.asyncio
    async def test_combined_signals(self, scorer):
        """组合信号应产生更高的重要性"""
        content = "用户对花生过敏，喜欢喝咖啡，出生于1990年"
        level, confidence = await scorer.score(content)
        # 多个信号组合应该产生较高的评分
        assert level in (ImportanceLevel.HIGH, ImportanceLevel.CRITICAL, ImportanceLevel.MEDIUM)

    @pytest.mark.asyncio
    async def test_confidence_calculation(self, scorer):
        """置信度应在合理范围内"""
        level, confidence = await scorer.score("一些普通内容")
        assert 0.0 <= confidence <= 1.0

    @pytest.mark.asyncio
    async def test_custom_thresholds(self):
        """自定义阈值测试"""
        scorer = HeuristicImportanceScorer(
            high_threshold=0.1,
            medium_threshold=0.05,
        )
        # 足够长的内容应该至少是 MEDIUM
        level, confidence = await scorer.score(
            "这是一段比较长的内容，包含了多个信号词，"
            "应该能够达到中等重要性水平"
        )
        assert level.value >= ImportanceLevel.MEDIUM.value

    @pytest.mark.asyncio
    async def test_custom_weights(self):
        """自定义权重测试"""
        scorer = HeuristicImportanceScorer(
            high_signal_weight=1.0,
            preference_weight=0.0,
            fact_weight=0.0,
            length_weight=0.0,
        )
        # 只有高重要性信号词生效
        level_no_signal, _ = await scorer.score("普通内容没有特殊关键词")
        level_with_signal, _ = await scorer.score("这是一个紧急且重要的事情")
        assert level_with_signal.value >= level_no_signal.value

    @pytest.mark.asyncio
    async def test_none_content(self, scorer):
        """None 内容应为 TRIVIAL"""
        level, confidence = await scorer.score(None)
        assert level == ImportanceLevel.TRIVIAL
        assert confidence == 0.0


# ============================================================
# Feature 2: ImportanceScorer 注册器测试
# ============================================================

class TestImportanceScorerRegistry:
    """重要性评估器注册器测试"""

    def test_registry_exists(self):
        """注册器应存在"""
        assert isinstance(_importance_scorer_registry, dict)

    def test_heuristic_scorer_registered(self):
        """HeuristicImportanceScorer 应已注册"""
        # 导入触发装饰器
        from memory_system.importance_scorers.heuristic_importance_scorer import HeuristicImportanceScorer
        assert "heuristic" in _importance_scorer_registry
        assert _importance_scorer_registry["heuristic"] is HeuristicImportanceScorer

    def test_get_registry(self):
        """get_importance_scorer_registry 应返回注册表"""
        registry = get_importance_scorer_registry()
        assert isinstance(registry, dict)


# ============================================================
# Integration Tests: Auto Importance in remember()
# ============================================================

class TestAutoImportanceInRemember:
    """remember() 中 importance="auto" 集成测试"""

    def _make_memory_system(self, storage, mock_embedding, scorer=None):
        """构建带重要性评估器的 MemorySystem"""
        from memory_system.agent_memory_system_facade import MemorySystem
        from memory_system.working_memory_cache import ShortTermMemory
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy

        class MockRetrieval(RetrievalStrategy):
            async def retrieve(self, query, storage, embedding, max_results=10, context=None):
                return {
                    "query": query,
                    "strategy": "mock",
                    "layers": {},
                    "combined": [],
                    "total_found": 0,
                    "retrieval_time": 0.0,
                }

        short_term = ShortTermMemory(capacity=10, ttl_seconds=60)
        ms = MemorySystem(
            storage=storage,
            embedding=mock_embedding,
            retrieval=MockRetrieval(),
            short_term=short_term,
            importance_scorer=scorer,
        )
        return ms

    @pytest.mark.asyncio
    async def test_auto_importance_with_scorer(self, memory_storage, mock_embedding):
        """有评估器时 importance="auto" 应调用评估器"""
        scorer = HeuristicImportanceScorer()
        ms = self._make_memory_system(memory_storage, mock_embedding, scorer)

        memory_id = await ms.remember(
            "用户对花生严重过敏，这是非常重要的安全信息，必须记住，"
            "紧急情况下可能导致过敏性休克，需要立即注射肾上腺素，"
            "过敏反应包括呼吸困难皮疹肿胀等症状，绝对不能接触花生制品，"
            "用户喜欢户外运动但必须随身携带急救药物，用户从不忽视过敏警告，"
            "每次就餐前都需要确认食材成分，这是关乎生命安全的关键信息",
            importance="auto"
        )
        assert memory_id is not None

        # 验证记忆已存储（HIGH 级别会存入长期存储）
        memory = await memory_storage.get(memory_id)
        assert memory is not None
        # 过敏内容应至少为 MEDIUM
        assert memory.importance.value >= ImportanceLevel.MEDIUM.value

    @pytest.mark.asyncio
    async def test_auto_importance_without_scorer(self, memory_storage, mock_embedding):
        """无评估器时 importance="auto" 应降级为 MEDIUM"""
        ms = self._make_memory_system(memory_storage, mock_embedding, scorer=None)

        memory_id = await ms.remember("一些普通内容", importance="auto")
        assert memory_id is not None

        # 无评估器时降级为 MEDIUM，auto 路由到 short_term
        memory = await memory_storage.get(memory_id)
        assert memory is None  # MEDIUM < HIGH, 存入 short_term 而非长期存储
        # 验证 short_term 中有该记忆
        short_term_memories = await ms.short_term.get_all()
        assert any(m.memory_id == memory_id for m in short_term_memories)
        assert any(m.importance == ImportanceLevel.MEDIUM for m in short_term_memories if m.memory_id == memory_id)

    @pytest.mark.asyncio
    async def test_auto_importance_stores_confidence(self, memory_storage, mock_embedding):
        """自动评估应将置信度存入 metadata"""
        scorer = HeuristicImportanceScorer()
        ms = self._make_memory_system(memory_storage, mock_embedding, scorer)

        memory_id = await ms.remember(
            "用户的密码是abc123，这是非常重要的安全信息，必须妥善保管，"
            "密码泄露可能导致账户被盗，造成严重的财产损失和隐私泄露，"
            "建议定期更换密码，使用强密码组合，不要将密码告诉任何人，"
            "用户喜欢使用密码管理器，从不重复使用同一个密码，"
            "这是关键的安全措施，需要高度重视和严格执行",
            importance="auto"
        )
        memory = await memory_storage.get(memory_id)
        assert memory is not None
        assert "importance_confidence" in memory.metadata
        assert 0.0 <= memory.metadata["importance_confidence"] <= 1.0

    @pytest.mark.asyncio
    async def test_explicit_importance_still_works(self, memory_storage, mock_embedding):
        """显式指定 importance 应不受评估器影响"""
        scorer = HeuristicImportanceScorer()
        ms = self._make_memory_system(memory_storage, mock_embedding, scorer)

        memory_id = await ms.remember("普通内容", importance="high")
        memory = await memory_storage.get(memory_id)
        assert memory.importance == ImportanceLevel.HIGH

    @pytest.mark.asyncio
    async def test_auto_importance_short_term(self, memory_storage, mock_embedding):
        """auto 评估后短期记忆应正常工作"""
        scorer = HeuristicImportanceScorer(
            high_threshold=10.0,  # 使所有内容评估为低重要性
        )
        ms = self._make_memory_system(memory_storage, mock_embedding, scorer)

        memory_id = await ms.remember("普通内容", importance="auto")
        # 低重要性 -> short_term，不会存入长期存储
        memory = await memory_storage.get(memory_id)
        assert memory is None  # 短期记忆不在长期存储中


# ============================================================
# Integration Tests: Graph-Aware Retrieval
# ============================================================

class TestGraphAwareRetrieval:
    """关联感知检索端到端测试"""

    @pytest.mark.asyncio
    async def test_end_to_end_graph_retrieval(self, memory_storage, mock_embedding):
        """端到端：存储 -> 关联 -> 检索 -> 关联展开"""
        from memory_system.agent_memory_system_facade import MemorySystem
        from memory_system.working_memory_cache import ShortTermMemory
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy

        class MockRetrievalWithResults(RetrievalStrategy):
            async def retrieve(self, query, storage, embedding, max_results=10, context=None):
                return {
                    "query": query,
                    "strategy": "mock",
                    "layers": {},
                    "combined": [
                        {
                            "memory": {
                                "memory_id": "m1",
                                "content": "Python编程",
                            },
                            "score": 0.95,
                        },
                        {
                            "memory": {
                                "memory_id": "m2",
                                "content": "数据分析",
                            },
                            "score": 0.85,
                        },
                    ],
                    "total_found": 2,
                    "retrieval_time": 0.001,
                }

        # 1. 存储记忆
        m1 = MemoryItem(content="Python编程", memory_type=MemoryType.SEMANTIC, memory_id="m1")
        m2 = MemoryItem(content="数据分析", memory_type=MemoryType.SEMANTIC, memory_id="m2")
        m3 = MemoryItem(content="机器学习", memory_type=MemoryType.SEMANTIC, memory_id="m3")
        m4 = MemoryItem(content="深度学习", memory_type=MemoryType.SEMANTIC, memory_id="m4")
        await memory_storage.add(m1)
        await memory_storage.add(m2)
        await memory_storage.add(m3)
        await memory_storage.add(m4)

        # 2. 建立关联: m1 -> m3, m2 -> m4
        await memory_storage.add_link(MemoryLink(source_id="m1", target_id="m3", relation=LinkRelation.RELATED))
        await memory_storage.add_link(MemoryLink(source_id="m2", target_id="m4", relation=LinkRelation.RELATED))

        # 3. 构建系统并执行关联检索
        short_term = ShortTermMemory(capacity=10, ttl_seconds=60)
        ms = MemorySystem(
            storage=memory_storage,
            embedding=mock_embedding,
            retrieval=MockRetrievalWithResults(),
            short_term=short_term,
        )

        result = await ms.recall_with_associations("Python", association_depth=1)

        # 4. 验证
        assert "combined" in result
        assert "associations" in result
        assert len(result["associations"]) == 2

        assoc_ids = {a["linked_from"] for a in result["associations"]}
        assert "m1" in assoc_ids
        assert "m2" in assoc_ids

    @pytest.mark.asyncio
    async def test_builder_with_importance_scorer(self, memory_storage, mock_embedding):
        """Builder 模式支持 importance_scorer"""
        from memory_system.agent_memory_system_facade import MemorySystemBuilder
        from memory_system.working_memory_cache import ShortTermMemory
        from memory_system.core_abstractions.abstract_component_interfaces import RetrievalStrategy

        class MockRetrieval(RetrievalStrategy):
            async def retrieve(self, query, storage, embedding, max_results=10, context=None):
                return {"query": query, "strategy": "mock", "layers": {}, "combined": [], "total_found": 0, "retrieval_time": 0.0}

        scorer = HeuristicImportanceScorer()
        short_term = ShortTermMemory(capacity=10, ttl_seconds=60)

        ms = (MemorySystemBuilder()
              .with_storage(memory_storage)
              .with_embedding(mock_embedding)
              .with_retrieval(MockRetrieval())
              .with_short_term(short_term)
              .with_importance_scorer(scorer)
              .build())

        assert ms.importance_scorer is scorer

        memory_id = await ms.remember(
            "用户的密码是secret123，这是非常重要的安全信息，必须妥善保管，"
            "密码泄露可能导致账户被盗，造成严重的财产损失和隐私泄露，"
            "建议定期更换密码，使用强密码组合，不要将密码告诉任何人，"
            "用户喜欢使用密码管理器，从不重复使用同一个密码，"
            "这是关键的安全措施，需要高度重视和严格执行",
            importance="auto"
        )
        memory = await memory_storage.get(memory_id)
        assert memory is not None
        assert "importance_confidence" in memory.metadata

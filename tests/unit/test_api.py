"""
Agent Memory System API 单元测试
使用 FastAPI TestClient + MemoryStorage + MockEmbeddingProvider
"""

import sys
import os

import pytest
from starlette.testclient import TestClient

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.conftest import MockEmbeddingProvider, MemoryStorage, ShortTermMemory
from memory_system.agent_memory_system_facade import MemorySystem
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from memory_system.restful_api_server import app, reset_memory_system, disable_rate_limit, enable_rate_limit
from memory_system.core_abstractions.authentication_middleware import reset_auth_manager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_singleton():
    """每个测试前后重置全局单例，避免状态泄漏。"""
    reset_memory_system(None)
    reset_auth_manager()
    disable_rate_limit()
    yield
    reset_memory_system(None)
    reset_auth_manager()
    enable_rate_limit()


@pytest.fixture()
def ms_instance(mock_embedding, memory_storage, short_term_memory):
    """
    构建一个使用 MemoryStorage + MockEmbeddingProvider 的 MemorySystem 实例，
    并注入到 API 单例中。
    """
    retrieval = HierarchicalRetrieval(strategy="adaptive")
    system = MemorySystem(
        storage=memory_storage,
        embedding=mock_embedding,
        retrieval=retrieval,
        short_term=short_term_memory,
        consolidation=None,
        forgetting=None,
    )
    reset_memory_system(system)
    return system


@pytest.fixture()
def client(ms_instance):
    """返回绑定了已注入 MemorySystem 的 FastAPI TestClient。"""
    return TestClient(app)


@pytest.fixture()
def _seeded_client(client):
    """预填充一些记忆数据的 client。"""
    # 存入短期记忆
    client.post(
        "/api/v1/memories",
        json={"content": "用户正在调试内存泄漏", "memory_type": "short_term"},
    )
    # 存入长期记忆（semantic）
    client.post(
        "/api/v1/memories",
        json={
            "content": "Python 是一种通用编程语言",
            "memory_type": "semantic",
            "importance": "high",
        },
    )
    return client


# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """GET /api/v1/health"""

    def test_health_returns_healthy(self, client):
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "version" in data

    def test_health_version_format(self, client):
        resp = client.get("/api/v1/health")
        data = resp.json()
        # version 应为 semver 格式
        parts = data["version"].split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------


class TestRegistries:
    """GET /api/v1/registries"""

    def test_registries_returns_all_categories(self, client):
        resp = client.get("/api/v1/registries")
        assert resp.status_code == 200
        data = resp.json()
        for key in ("storages", "embeddings", "retrievals", "consolidations", "forgettings"):
            assert key in data
            assert isinstance(data[key], list)

    def test_registries_includes_memory_storage(self, client):
        resp = client.get("/api/v1/registries")
        data = resp.json()
        assert "memory" in data["storages"]


# ---------------------------------------------------------------------------
# 存储记忆
# ---------------------------------------------------------------------------


class TestStoreMemory:
    """POST /api/v1/memories"""

    def test_store_short_term_memory(self, client):
        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试短期记忆", "memory_type": "short_term"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "stored"
        assert "memory_id" in data
        assert len(data["memory_id"]) > 0

    def test_store_semantic_memory(self, client):
        resp = client.post(
            "/api/v1/memories",
            json={"content": "语义记忆内容", "memory_type": "semantic", "importance": "high"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "stored"
        assert "memory_id" in data

    def test_store_episodic_memory(self, client):
        resp = client.post(
            "/api/v1/memories",
            json={"content": "情景记忆内容", "memory_type": "episodic"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "stored"

    def test_store_auto_type(self, client):
        """auto 类型：低重要性 → short_term, 高重要性 → semantic"""
        resp_low = client.post(
            "/api/v1/memories",
            json={"content": "低重要性", "memory_type": "auto", "importance": "low"},
        )
        assert resp_low.status_code == 201

        resp_high = client.post(
            "/api/v1/memories",
            json={"content": "高重要性", "memory_type": "auto", "importance": "high"},
        )
        assert resp_high.status_code == 201

    def test_store_with_metadata(self, client):
        resp = client.post(
            "/api/v1/memories",
            json={
                "content": "带元数据的记忆",
                "metadata": {"source": "test", "tag": "demo"},
            },
        )
        assert resp.status_code == 201

    def test_store_empty_content_422(self, client):
        resp = client.post(
            "/api/v1/memories",
            json={"content": ""},
        )
        # Pydantic validation rejects empty string via min_length=1
        assert resp.status_code == 422

    def test_store_invalid_type_422(self, client):
        resp = client.post(
            "/api/v1/memories",
            json={"content": "内容", "memory_type": "invalid_type"},
        )
        assert resp.status_code == 422

    def test_store_invalid_importance_422(self, client):
        resp = client.post(
            "/api/v1/memories",
            json={"content": "内容", "importance": "super_critical"},
        )
        assert resp.status_code == 422

    def test_store_missing_content_422(self, client):
        resp = client.post("/api/v1/memories", json={})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 批量存储
# ---------------------------------------------------------------------------


class TestBatchStore:
    """POST /api/v1/memories/batch"""

    def test_batch_store(self, client):
        resp = client.post(
            "/api/v1/memories/batch",
            json={
                "contents": ["记忆一", "记忆二", "记忆三"],
                "memory_type": "short_term",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["count"] == 3
        assert len(data["memory_ids"]) == 3

    def test_batch_store_empty_list_422(self, client):
        resp = client.post(
            "/api/v1/memories/batch",
            json={"contents": []},
        )
        assert resp.status_code == 422

    def test_batch_store_with_importance(self, client):
        resp = client.post(
            "/api/v1/memories/batch",
            json={
                "contents": ["重要记忆A", "重要记忆B"],
                "importance": "high",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["count"] == 2


# ---------------------------------------------------------------------------
# 检索记忆
# ---------------------------------------------------------------------------


class TestRecall:
    """GET /api/v1/memories/recall"""

    def test_recall_returns_query(self, _seeded_client):
        resp = _seeded_client.get(
            "/api/v1/memories/recall",
            params={"query": "Python 编程"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["query"] == "Python 编程"
        assert "results" in data

    def test_recall_max_results(self, _seeded_client):
        resp = _seeded_client.get(
            "/api/v1/memories/recall",
            params={"query": "test", "max_results": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) <= 1

    def test_recall_empty_query_422(self, client):
        resp = client.get(
            "/api/v1/memories/recall",
            params={"query": ""},
        )
        assert resp.status_code == 422

    def test_recall_no_results(self, client):
        resp = client.get(
            "/api/v1/memories/recall",
            params={"query": "完全不相关的内容xyz"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["results"], list)


# ---------------------------------------------------------------------------
# 获取单条记忆
# ---------------------------------------------------------------------------


class TestGetMemory:
    """GET /api/v1/memories/{memory_id}"""

    def test_get_existing_memory(self, _seeded_client):
        # 先存入一条 semantic 记忆
        store_resp = _seeded_client.post(
            "/api/v1/memories",
            json={"content": "可被获取的记忆", "memory_type": "semantic"},
        )
        memory_id = store_resp.json()["memory_id"]

        resp = _seeded_client.get(f"/api/v1/memories/{memory_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["memory_id"] == memory_id
        assert data["content"] == "可被获取的记忆"
        assert "memory_type" in data
        assert "importance" in data
        assert "timestamp" in data

    def test_get_nonexistent_memory_404(self, client):
        resp = client.get("/api/v1/memories/nonexistent_id_12345")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 删除记忆
# ---------------------------------------------------------------------------


class TestDeleteMemory:
    """DELETE /api/v1/memories/{memory_id}"""

    def test_delete_existing_memory(self, _seeded_client):
        store_resp = _seeded_client.post(
            "/api/v1/memories",
            json={"content": "待删除记忆", "memory_type": "semantic"},
        )
        memory_id = store_resp.json()["memory_id"]

        del_resp = _seeded_client.delete(f"/api/v1/memories/{memory_id}")
        assert del_resp.status_code == 200
        assert del_resp.json()["status"] == "deleted"

        # 确认已删除
        get_resp = _seeded_client.get(f"/api/v1/memories/{memory_id}")
        assert get_resp.status_code == 404

    def test_delete_nonexistent_memory_404(self, client):
        resp = client.delete("/api/v1/memories/nonexistent_id_99999")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 短期记忆
# ---------------------------------------------------------------------------


class TestShortTerm:
    """GET /api/v1/short-term/recent & DELETE /api/v1/short-term"""

    def test_get_recent_short_term(self, client):
        # 存入几条短期记忆
        for i in range(3):
            client.post(
                "/api/v1/memories",
                json={"content": f"短期记忆 {i}", "memory_type": "short_term"},
            )

        resp = client.get("/api/v1/short-term/recent", params={"n": 5})
        assert resp.status_code == 200
        data = resp.json()
        assert "memories" in data
        assert len(data["memories"]) <= 5

    def test_get_recent_short_term_limits_n(self, client):
        for i in range(5):
            client.post(
                "/api/v1/memories",
                json={"content": f"记忆 {i}", "memory_type": "short_term"},
            )

        resp = client.get("/api/v1/short-term/recent", params={"n": 2})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["memories"]) <= 2

    def test_clear_short_term(self, client):
        client.post(
            "/api/v1/memories",
            json={"content": "临时记忆", "memory_type": "short_term"},
        )

        resp = client.delete("/api/v1/short-term")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cleared"

        # 确认清空
        recent_resp = client.get("/api/v1/short-term/recent")
        assert recent_resp.status_code == 200
        assert len(recent_resp.json()["memories"]) == 0


# ---------------------------------------------------------------------------
# 统计
# ---------------------------------------------------------------------------


class TestStats:
    """GET /api/v1/stats"""

    def test_stats_returns_all_fields(self, _seeded_client):
        resp = _seeded_client.get("/api/v1/stats")
        assert resp.status_code == 200
        data = resp.json()
        for field in (
            "short_term_count",
            "episodic_count",
            "semantic_count",
            "total_count",
            "total_added",
            "total_retrieved",
            "uptime_hours",
        ):
            assert field in data

    def test_stats_counts_after_store(self, client):
        # 初始统计
        resp0 = client.get("/api/v1/stats")
        initial_total = resp0.json()["total_added"]

        # 存入记忆
        client.post(
            "/api/v1/memories",
            json={"content": "统计测试", "memory_type": "semantic"},
        )

        resp1 = client.get("/api/v1/stats")
        assert resp1.json()["total_added"] >= initial_total


# ---------------------------------------------------------------------------
# 整合
# ---------------------------------------------------------------------------


class TestConsolidate:
    """POST /api/v1/consolidate"""

    def test_consolidate_without_strategy(self, client):
        """没有 consolidation 策略时仍返回 ok。"""
        resp = client.post("/api/v1/consolidate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"


# ---------------------------------------------------------------------------
# 端到端流程
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """完整的存储 → 检索 → 获取 → 删除流程"""

    def test_full_lifecycle(self, client):
        # 1. 存储
        store_resp = client.post(
            "/api/v1/memories",
            json={
                "content": "端到端测试记忆",
                "memory_type": "semantic",
                "importance": "high",
                "metadata": {"test": "e2e"},
            },
        )
        assert store_resp.status_code == 201
        memory_id = store_resp.json()["memory_id"]

        # 2. 获取
        get_resp = client.get(f"/api/v1/memories/{memory_id}")
        assert get_resp.status_code == 200
        assert get_resp.json()["content"] == "端到端测试记忆"

        # 3. 检索
        recall_resp = client.get(
            "/api/v1/memories/recall",
            params={"query": "端到端"},
        )
        assert recall_resp.status_code == 200

        # 4. 删除
        del_resp = client.delete(f"/api/v1/memories/{memory_id}")
        assert del_resp.status_code == 200

        # 5. 确认删除
        get_resp2 = client.get(f"/api/v1/memories/{memory_id}")
        assert get_resp2.status_code == 404

    def test_batch_store_then_recall(self, client):
        contents = ["机器学习基础", "深度学习入门", "自然语言处理"]
        batch_resp = client.post(
            "/api/v1/memories/batch",
            json={"contents": contents, "memory_type": "short_term"},
        )
        assert batch_resp.status_code == 201
        ids = batch_resp.json()["memory_ids"]
        assert len(ids) == 3

        # 检索
        recall_resp = client.get(
            "/api/v1/memories/recall",
            params={"query": "学习"},
        )
        assert recall_resp.status_code == 200

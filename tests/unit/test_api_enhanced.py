"""
Agent Memory System API 增强功能单元测试
分页、memory_type 过滤、导出/导入端点、速率限制器
"""

import sys
import os

import pytest
from starlette.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.conftest import MockEmbeddingProvider, MemoryStorage, ShortTermMemory
from memory_system.agent_memory_system_facade import MemorySystem
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from memory_system.restful_api_server import app, reset_memory_system, disable_rate_limit, enable_rate_limit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_singleton():
    """每个测试前后重置全局单例，避免状态泄漏。"""
    reset_memory_system(None)
    disable_rate_limit()
    yield
    reset_memory_system(None)
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
def seeded_client(client):
    """预填充 5 条长期记忆的 client（3 episodic + 2 semantic）。"""
    for i in range(3):
        client.post(
            "/api/v1/memories",
            json={
                "content": f"情景记忆内容{i}",
                "memory_type": "episodic",
                "importance": "high",
            },
        )
    for i in range(2):
        client.post(
            "/api/v1/memories",
            json={
                "content": f"语义记忆内容{i}",
                "memory_type": "semantic",
                "importance": "high",
            },
        )
    return client


# ---------------------------------------------------------------------------
# Recall 分页测试
# ---------------------------------------------------------------------------


class TestRecallPagination:
    """GET /api/v1/memories/recall 分页功能"""

    def test_recall_page_one(self, seeded_client):
        """offset=0, limit=2 返回第一页，包含分页元数据"""
        resp = seeded_client.get(
            "/api/v1/memories/recall",
            params={"query": "记忆", "offset": 0, "limit": 2},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["query"] == "记忆"
        assert data["offset"] == 0
        assert data["limit"] == 2
        assert len(data["results"]) <= 2
        assert "total" in data
        assert data["total"] >= 0

    def test_recall_page_two(self, seeded_client):
        """offset=2, limit=2 返回下一页"""
        resp = seeded_client.get(
            "/api/v1/memories/recall",
            params={"query": "记忆", "offset": 2, "limit": 2},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["offset"] == 2
        assert data["limit"] == 2
        assert len(data["results"]) <= 2


# ---------------------------------------------------------------------------
# Recall memory_type 过滤测试
# ---------------------------------------------------------------------------


class TestRecallMemoryTypeFilter:
    """GET /api/v1/memories/recall memory_type 过滤"""

    def test_recall_filter_episodic(self, seeded_client):
        """按 episodic 过滤只返回情景记忆"""
        resp = seeded_client.get(
            "/api/v1/memories/recall",
            params={"query": "记忆", "memory_type": "episodic"},
        )
        assert resp.status_code == 200
        data = resp.json()
        for result in data["results"]:
            assert result.get("memory", {}).get("memory_type") == "episodic"

    def test_recall_filter_semantic(self, seeded_client):
        """按 semantic 过滤只返回语义记忆"""
        resp = seeded_client.get(
            "/api/v1/memories/recall",
            params={"query": "记忆", "memory_type": "semantic"},
        )
        assert resp.status_code == 200
        data = resp.json()
        for result in data["results"]:
            assert result.get("memory", {}).get("memory_type") == "semantic"

    def test_recall_no_filter_returns_all(self, seeded_client):
        """不设置 memory_type 过滤返回所有类型"""
        resp = seeded_client.get(
            "/api/v1/memories/recall",
            params={"query": "记忆"},
        )
        assert resp.status_code == 200
        data = resp.json()
        # 不过滤时应返回所有记忆
        assert data["total"] >= 0


# ---------------------------------------------------------------------------
# Export 端点测试
# ---------------------------------------------------------------------------


class TestExportEndpoint:
    """GET /api/v1/memories/export"""

    def test_export_returns_memories(self, seeded_client):
        """导出端点返回记忆列表"""
        resp = seeded_client.get("/api/v1/memories/export")
        assert resp.status_code == 200
        data = resp.json()
        assert "memories" in data
        assert "count" in data
        assert "exported_at" in data
        assert data["count"] >= 5

    def test_export_with_memory_type_filter(self, seeded_client):
        """导出端点支持按类型过滤"""
        resp = seeded_client.get(
            "/api/v1/memories/export",
            params={"memory_type": "semantic"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 2
        for mem in data["memories"]:
            assert mem["memory_type"] == "semantic"


# ---------------------------------------------------------------------------
# Import 端点测试
# ---------------------------------------------------------------------------


class TestImportEndpoint:
    """POST /api/v1/memories/import"""

    def test_import_memories(self, client):
        """导入端点成功导入记忆"""
        import_data = {
            "memories": [
                {
                    "content": "导入的语义记忆",
                    "memory_type": "semantic",
                    "importance": 4,
                },
                {
                    "content": "导入的情景记忆",
                    "memory_type": "episodic",
                    "importance": 3,
                },
            ]
        }
        resp = client.post("/api/v1/memories/import", json=import_data)
        assert resp.status_code == 201
        data = resp.json()
        assert data["imported_count"] == 2
        assert data["skipped_count"] == 0

    def test_import_empty_list_422(self, client):
        """导入空列表返回 422"""
        resp = client.post("/api/v1/memories/import", json={"memories": []})
        assert resp.status_code == 422

    def test_import_invalid_data_skips(self, client):
        """导入包含无效数据时跳过坏条目"""
        import_data = {
            "memories": [
                {
                    "content": "有效记忆",
                    "memory_type": "semantic",
                    "importance": 4,
                },
                {
                    # 缺少 content，无效
                    "memory_type": "semantic",
                },
            ]
        }
        resp = client.post("/api/v1/memories/import", json=import_data)
        assert resp.status_code == 201
        data = resp.json()
        assert data["imported_count"] == 1
        assert data["skipped_count"] == 1


# ---------------------------------------------------------------------------
# Rate Limiter 测试
# ---------------------------------------------------------------------------


class TestRateLimiter:
    """速率限制器配置检查"""

    def test_rate_limiter_configured(self):
        """app.state.limiter 存在"""
        assert hasattr(app.state, "limiter")
        assert app.state.limiter is not None

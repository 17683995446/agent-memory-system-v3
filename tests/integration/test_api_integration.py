"""
集成测试: FastAPI REST API 接口
测试完整的 API 生命周期: health -> remember -> recall -> stats -> delete

使用 pytest.importorskip 在 api 模块或其依赖不存在时优雅跳过
注入 MockEmbeddingProvider 以避免依赖 sentence-transformers 等重型库
"""

import sys
import os

import pytest

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# 如果 fastapi 或 starlette 未安装，跳过整个测试文件
fastapi = pytest.importorskip("fastapi", reason="fastapi 未安装")
pytest.importorskip("starlette", reason="starlette 未安装")

# 尝试导入 api 模块；如果不存在则跳过
api_module = pytest.importorskip("memory_system.restful_api_server", reason="memory_system.api 模块不存在")

from starlette.testclient import TestClient

from tests.conftest import MockEmbeddingProvider
from memory_system.storage_backends.in_memory_storage_backend import MemoryStorage
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.agent_memory_system_facade import MemorySystem
from memory_system.configuration_settings.memory_system_configuration import MemoryConfig

app = api_module.app

# API 前缀
API = "/api/v1"


def _create_test_system():
    """创建用于测试的 MemorySystem 实例（使用 MockEmbeddingProvider）"""
    return MemorySystem(
        storage=MemoryStorage(max_memories=100),
        embedding=MockEmbeddingProvider(dimension=64),
        retrieval=HierarchicalRetrieval(strategy="adaptive"),
        short_term=ShortTermMemory(capacity=10, ttl_seconds=300),
        config=MemoryConfig(),
    )


@pytest.fixture(autouse=True)
def _inject_mock_system():
    """
    自动 fixture: 在每个测试前注入使用 MockEmbeddingProvider 的 MemorySystem，
    测试后恢复为 None，避免跨测试状态污染。
    同时调用 _ensure_registrations() 确保所有注册表被填充。
    """
    MemorySystem._ensure_registrations()
    api_module.reset_memory_system(_create_test_system())
    api_module.disable_rate_limit()
    yield
    api_module.reset_memory_system(None)
    api_module.enable_rate_limit()


def _client():
    return TestClient(app)


class TestHealthEndpoint:
    """test_health_endpoint: 健康检查端点"""

    def test_health_returns_ok(self):
        """GET /api/v1/health 返回 200 和正确状态"""
        c = _client()
        response = c.get(f"{API}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_health_response_contains_version(self):
        """健康检查响应包含版本号"""
        c = _client()
        response = c.get(f"{API}/health")
        data = response.json()
        assert "version" in data
        assert data["version"] != ""


class TestRememberEndpoint:
    """test_remember_endpoint: 记忆存储端点"""

    def test_remember_single_memory(self):
        """POST /api/v1/memories 存储单条记忆"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={
                "content": "Python是一种通用编程语言",
                "memory_type": "semantic",
                "importance": "high",
            },
        )
        assert response.status_code == 201
        data = response.json()
        assert "memory_id" in data
        assert data["status"] == "stored"

    def test_remember_with_default_params(self):
        """使用默认参数存储记忆（仅提供 content）"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={"content": "测试默认参数记忆"},
        )
        assert response.status_code == 201
        data = response.json()
        assert "memory_id" in data

    def test_remember_with_metadata(self):
        """存储带 metadata 的记忆"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={
                "content": "带元数据的记忆",
                "memory_type": "episodic",
                "importance": "medium",
                "metadata": {"source": "test", "tag": "integration"},
            },
        )
        assert response.status_code == 201

    def test_remember_empty_content_returns_error(self):
        """空内容应返回 422 错误"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={"content": "", "memory_type": "episodic"},
        )
        assert response.status_code == 422

    def test_remember_whitespace_content_returns_error(self):
        """纯空白内容应返回 422 错误"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={"content": "   ", "memory_type": "episodic"},
        )
        # FastAPI Pydantic min_length=1 会拒绝空白字符串
        assert response.status_code == 422

    def test_remember_invalid_memory_type_returns_error(self):
        """无效的 memory_type 应返回 422 错误"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={"content": "测试内容", "memory_type": "invalid_type"},
        )
        assert response.status_code == 422

    def test_remember_invalid_importance_returns_error(self):
        """无效的 importance 应返回 422 错误"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={"content": "测试内容", "importance": "super_important"},
        )
        assert response.status_code == 422


class TestRecallEndpoint:
    """test_recall_endpoint: 记忆检索端点"""

    def test_recall_returns_results(self):
        """GET /api/v1/memories/recall 返回检索结果"""
        c = _client()
        # 先存储一条记忆
        c.post(
            f"{API}/memories",
            json={"content": "用于检索测试的Python编程知识", "memory_type": "semantic", "importance": "high"},
        )

        # 检索
        response = c.get(f"{API}/memories/recall", params={"query": "Python编程", "max_results": 5})
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert "query" in data

    def test_recall_empty_query_returns_error(self):
        """空查询应返回 422 错误"""
        c = _client()
        response = c.get(f"{API}/memories/recall", params={"query": ""})
        assert response.status_code == 422

    def test_recall_with_max_results(self):
        """指定 max_results 参数"""
        c = _client()
        response = c.get(f"{API}/memories/recall", params={"query": "测试", "max_results": 3})
        assert response.status_code == 200

    def test_recall_without_query_returns_error(self):
        """不提供 query 参数应返回 422"""
        c = _client()
        response = c.get(f"{API}/memories/recall")
        assert response.status_code == 422


class TestStatsEndpoint:
    """test_stats_endpoint: 统计信息端点"""

    def test_stats_returns_valid_response(self):
        """GET /api/v1/stats 返回统计信息"""
        c = _client()
        response = c.get(f"{API}/stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_count" in data
        assert "short_term_count" in data
        assert "episodic_count" in data
        assert "semantic_count" in data


class TestDeleteEndpoint:
    """test_delete_endpoint: 记忆删除端点"""

    def test_delete_memory(self):
        """DELETE /api/v1/memories/{memory_id} 删除指定记忆"""
        c = _client()
        # 先存储
        resp = c.post(
            f"{API}/memories",
            json={"content": "待删除的记忆", "memory_type": "episodic"},
        )
        assert resp.status_code == 201
        memory_id = resp.json()["memory_id"]

        # 删除
        del_resp = c.delete(f"{API}/memories/{memory_id}")
        assert del_resp.status_code == 200
        assert del_resp.json()["status"] == "deleted"

    def test_delete_nonexistent_memory(self):
        """删除不存在的记忆应返回 404"""
        c = _client()
        response = c.delete(f"{API}/memories/nonexistent_id_12345")
        assert response.status_code == 404


class TestBatchRememberEndpoint:
    """test_batch_remember_endpoint: 批量记忆存储端点"""

    def test_batch_remember(self):
        """POST /api/v1/memories/batch 批量存储记忆"""
        c = _client()
        response = c.post(
            f"{API}/memories/batch",
            json={
                "contents": [
                    "批量记忆第一条",
                    "批量记忆第二条",
                    "批量记忆第三条",
                ],
                "memory_type": "semantic",
                "importance": "medium",
            },
        )
        assert response.status_code == 201
        data = response.json()
        assert "memory_ids" in data
        assert "count" in data
        assert data["count"] == 3
        assert len(data["memory_ids"]) == 3

    def test_batch_remember_empty_list_returns_error(self):
        """空列表应返回 422 验证错误"""
        c = _client()
        response = c.post(
            f"{API}/memories/batch",
            json={"contents": []},
        )
        assert response.status_code == 422

    def test_batch_remember_with_invalid_type(self):
        """批量存储时无效类型应返回 422 错误"""
        c = _client()
        response = c.post(
            f"{API}/memories/batch",
            json={
                "contents": ["测试"],
                "memory_type": "invalid_type",
            },
        )
        assert response.status_code == 422

    def test_batch_remember_with_invalid_importance(self):
        """批量存储时无效重要性应返回 422 错误"""
        c = _client()
        response = c.post(
            f"{API}/memories/batch",
            json={
                "contents": ["测试"],
                "importance": "super_high",
            },
        )
        assert response.status_code == 422


class TestShortTermMemoryEndpoints:
    """test_short_term_endpoints: 短期记忆相关端点"""

    def test_add_short_term_memory(self):
        """存储短期记忆"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={"content": "短期记忆测试", "memory_type": "short_term"},
        )
        assert response.status_code == 201

    def test_get_recent_short_term(self):
        """GET /api/v1/short-term/recent 获取最近短期记忆"""
        c = _client()
        # 先存几条短期记忆
        c.post(f"{API}/memories", json={"content": "最近上下文1", "memory_type": "short_term"})
        c.post(f"{API}/memories", json={"content": "最近上下文2", "memory_type": "short_term"})

        response = c.get(f"{API}/short-term/recent", params={"n": 5})
        assert response.status_code == 200
        data = response.json()
        assert "memories" in data
        assert isinstance(data["memories"], list)

    def test_clear_short_term(self):
        """DELETE /api/v1/short-term 清空短期记忆"""
        c = _client()
        response = c.delete(f"{API}/short-term")
        assert response.status_code == 200
        assert response.json()["status"] == "cleared"


class TestFullLifecycle:
    """test_full_lifecycle: 完整生命周期 - health -> remember -> recall -> stats -> delete"""

    def test_complete_api_lifecycle(self):
        """端到端 API 生命周期测试"""
        c = _client()

        # 1. 健康检查
        health_resp = c.get(f"{API}/health")
        assert health_resp.status_code == 200

        # 2. 存储记忆
        remember_resp = c.post(
            f"{API}/memories",
            json={
                "content": "生命周期测试记忆: FastAPI集成",
                "memory_type": "semantic",
                "importance": "high",
            },
        )
        assert remember_resp.status_code == 201
        remember_data = remember_resp.json()
        memory_id = remember_data["memory_id"]
        assert memory_id is not None

        # 3. 检索记忆
        recall_resp = c.get(
            f"{API}/memories/recall",
            params={"query": "FastAPI集成", "max_results": 5},
        )
        assert recall_resp.status_code == 200

        # 4. 获取统计
        stats_resp = c.get(f"{API}/stats")
        assert stats_resp.status_code == 200
        stats_data = stats_resp.json()
        assert stats_data["total_count"] >= 1 or stats_data["total_added"] >= 1

        # 5. 删除记忆
        delete_resp = c.delete(f"{API}/memories/{memory_id}")
        assert delete_resp.status_code == 200

    def test_lifecycle_with_batch_operations(self):
        """包含批量操作的完整生命周期"""
        c = _client()

        # 批量存储
        batch_resp = c.post(
            f"{API}/memories/batch",
            json={
                "contents": ["批量1", "批量2", "批量3"],
                "memory_type": "episodic",
                "importance": "medium",
            },
        )
        assert batch_resp.status_code == 201

        # 检索
        recall_resp = c.get(
            f"{API}/memories/recall",
            params={"query": "批量"},
        )
        assert recall_resp.status_code == 200

        # 统计
        stats_resp = c.get(f"{API}/stats")
        assert stats_resp.status_code == 200


class TestErrorHandling:
    """test_error_handling: API 错误处理和边界情况"""

    def test_post_memories_without_body(self):
        """不带请求体调用存储应返回 422"""
        c = _client()
        response = c.post(f"{API}/memories")
        assert response.status_code == 422

    def test_get_nonexistent_endpoint(self):
        """访问不存在的端点应返回 404"""
        c = _client()
        response = c.get("/nonexistent")
        assert response.status_code == 404

    def test_remember_with_auto_type(self):
        """auto 类型应自动选择记忆类型"""
        c = _client()
        response = c.post(
            f"{API}/memories",
            json={"content": "自动类型选择测试", "memory_type": "auto", "importance": "high"},
        )
        assert response.status_code == 201

    def test_recall_with_invalid_max_results(self):
        """max_results 超出范围应返回 422"""
        c = _client()
        response = c.get(
            f"{API}/memories/recall",
            params={"query": "测试", "max_results": 0},
        )
        assert response.status_code == 422

    def test_recall_with_excessive_max_results(self):
        """max_results 超过上限应返回 422"""
        c = _client()
        response = c.get(
            f"{API}/memories/recall",
            params={"query": "测试", "max_results": 999},
        )
        assert response.status_code == 422


class TestRegistriesEndpoint:
    """test_registries_endpoint: 注册表查询端点"""

    def test_list_registries(self):
        """GET /api/v1/registries 返回所有已注册组件"""
        c = _client()
        response = c.get(f"{API}/registries")
        assert response.status_code == 200
        data = response.json()
        assert "storages" in data
        assert "embeddings" in data
        assert "retrievals" in data
        assert "consolidations" in data
        assert "forgettings" in data

    def test_registries_contain_expected_components(self):
        """注册表包含预期的内置组件"""
        c = _client()
        response = c.get(f"{API}/registries")
        data = response.json()
        assert "memory" in data["storages"]
        assert "hierarchical" in data["retrievals"]
        assert "vector_only" in data["retrievals"]
        assert "time_based" in data["consolidations"]
        assert "score_based" in data["forgettings"]

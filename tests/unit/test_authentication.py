"""
API认证机制单元测试
测试API Key和OAuth2两种认证方式
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
from memory_system.restful_api_server import (
    app, reset_memory_system, disable_rate_limit, enable_rate_limit, init_authentication
)
from memory_system.core_abstractions.authentication_middleware import (
    reset_auth_manager,
    AuthenticationManager,
    set_auth_manager,
    hash_password,
)
from memory_system.configuration_settings.memory_system_configuration import MemoryConfig


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


# ---------------------------------------------------------------------------
# 无认证模式测试
# ---------------------------------------------------------------------------


class TestNoAuthentication:
    """未启用认证时的测试"""

    def test_health_check_no_auth(self, client):
        """健康检查端点无需认证"""
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_store_memory_no_auth_when_disabled(self, client):
        """认证禁用时可以直接访问"""
        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
        )
        assert resp.status_code == 201


# ---------------------------------------------------------------------------
# API Key认证测试
# ---------------------------------------------------------------------------


class TestAPIKeyAuthentication:
    """API Key认证测试"""

    def test_store_memory_with_valid_api_key(self, client):
        """使用有效的API Key可以访问"""
        # 设置API Key认证
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "test-secret-key-12345"
        init_authentication(config)

        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
            headers={"X-API-Key": "test-secret-key-12345"},
        )
        assert resp.status_code == 201

    def test_store_memory_with_invalid_api_key(self, client):
        """使用无效的API Key被拒绝"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "test-secret-key-12345"
        init_authentication(config)

        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
            headers={"X-API-Key": "wrong-key"},
        )
        assert resp.status_code == 401

    def test_store_memory_without_api_key(self, client):
        """未提供API Key被拒绝"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "test-secret-key-12345"
        init_authentication(config)

        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
        )
        assert resp.status_code == 401

    def test_recall_with_valid_api_key(self, client):
        """使用API Key可以检索记忆"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "test-secret-key-12345"
        init_authentication(config)

        resp = client.get(
            "/api/v1/memories/recall",
            params={"query": "测试"},
            headers={"X-API-Key": "test-secret-key-12345"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# OAuth2认证测试
# ---------------------------------------------------------------------------


class TestOAuth2Authentication:
    """OAuth2密码模式认证测试"""

    def test_login_with_valid_credentials(self, client):
        """使用有效的用户名密码可以获取token"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.oauth2_enabled = True
        config.authentication.oauth2_users = {
            "admin": hash_password("admin123"),
            "user": hash_password("user456"),
        }
        init_authentication(config)

        resp = client.post(
            "/api/v1/auth/token",
            data={"username": "admin", "password": "admin123"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert data["access_token"] == "admin"

    def test_login_with_invalid_credentials(self, client):
        """使用无效的用户名密码被拒绝"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.oauth2_enabled = True
        config.authentication.oauth2_users = {
            "admin": hash_password("admin123"),
        }
        init_authentication(config)

        resp = client.post(
            "/api/v1/auth/token",
            data={"username": "admin", "password": "wrongpassword"},
        )
        assert resp.status_code == 401

    def test_login_with_nonexistent_user(self, client):
        """使用不存在的用户被拒绝"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.oauth2_enabled = True
        config.authentication.oauth2_users = {
            "admin": hash_password("admin123"),
        }
        init_authentication(config)

        resp = client.post(
            "/api/v1/auth/token",
            data={"username": "nonexistent", "password": "password"},
        )
        assert resp.status_code == 401

    def test_store_memory_with_oauth2_token(self, client):
        """使用OAuth2 Token可以访问受保护端点"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.oauth2_enabled = True
        config.authentication.oauth2_users = {
            "admin": hash_password("admin123"),
        }
        init_authentication(config)

        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
            headers={"Authorization": "Bearer admin"},
        )
        assert resp.status_code == 201

    def test_store_memory_with_invalid_token(self, client):
        """使用无效的OAuth2 Token被拒绝"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.oauth2_enabled = True
        config.authentication.oauth2_users = {
            "admin": hash_password("admin123"),
        }
        init_authentication(config)

        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
            headers={"Authorization": "Bearer invalid_token"},
        )
        assert resp.status_code == 401

    def test_oauth2_disabled_returns_501(self, client):
        """OAuth2未启用时返回501"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.oauth2_enabled = False
        init_authentication(config)

        resp = client.post(
            "/api/v1/auth/token",
            data={"username": "admin", "password": "admin123"},
        )
        assert resp.status_code == 501


# ---------------------------------------------------------------------------
# 混合认证测试
# ---------------------------------------------------------------------------


class TestMixedAuthentication:
    """API Key和OAuth2混合认证测试"""

    def test_api_key_takes_precedence(self, client):
        """API Key优先于OAuth2"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "api-key-123"
        config.authentication.oauth2_enabled = True
        config.authentication.oauth2_users = {
            "admin": hash_password("admin123"),
        }
        init_authentication(config)

        # 使用API Key应该成功
        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
            headers={"X-API-Key": "api-key-123"},
        )
        assert resp.status_code == 201

    def test_oauth2_fallback_when_no_api_key(self, client):
        """没有API Key时回退到OAuth2"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "api-key-123"
        config.authentication.oauth2_enabled = True
        config.authentication.oauth2_users = {
            "admin": hash_password("admin123"),
        }
        init_authentication(config)

        # 使用OAuth2 Token应该成功
        resp = client.post(
            "/api/v1/memories",
            json={"content": "测试内容", "memory_type": "short_term"},
            headers={"Authorization": "Bearer admin"},
        )
        assert resp.status_code == 201


# ---------------------------------------------------------------------------
# 受保护端点测试
# ---------------------------------------------------------------------------


class TestProtectedEndpoints:
    """测试各端点的认证保护"""

    @pytest.fixture(autouse=True)
    def setup_auth(self):
        """设置认证"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "test-key"
        init_authentication(config)

    def test_batch_store_protected(self, client):
        """批量存储需要认证"""
        resp = client.post(
            "/api/v1/memories/batch",
            json={"contents": ["记忆1", "记忆2"]},
        )
        assert resp.status_code == 401

        resp = client.post(
            "/api/v1/memories/batch",
            json={"contents": ["记忆1", "记忆2"]},
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code == 201

    def test_delete_memory_protected(self, client):
        """删除记忆需要认证"""
        resp = client.delete("/api/v1/memories/some-id")
        assert resp.status_code == 401

        resp = client.delete(
            "/api/v1/memories/some-id",
            headers={"X-API-Key": "test-key"},
        )
        # 404是因为记忆不存在，不是401
        assert resp.status_code == 404

    def test_export_memories_protected(self, client):
        """导出记忆需要认证"""
        resp = client.get("/api/v1/memories/export")
        assert resp.status_code == 401

        resp = client.get(
            "/api/v1/memories/export",
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code == 200

    def test_import_memories_protected(self, client):
        """导入记忆需要认证"""
        resp = client.post(
            "/api/v1/memories/import",
            json={"memories": []},
        )
        assert resp.status_code == 401

    def test_consolidate_protected(self, client):
        """记忆整合需要认证"""
        resp = client.post("/api/v1/consolidate")
        assert resp.status_code == 401

        resp = client.post(
            "/api/v1/consolidate",
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code == 200

    def test_clear_short_term_protected(self, client):
        """清空短期记忆需要认证"""
        resp = client.delete("/api/v1/short-term")
        assert resp.status_code == 401

        resp = client.delete(
            "/api/v1/short-term",
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# 健康检查和注册表端点（无需认证）
# ---------------------------------------------------------------------------


class TestPublicEndpoints:
    """公开端点测试（无需认证）"""

    @pytest.fixture(autouse=True)
    def setup_auth(self):
        """设置认证"""
        config = MemoryConfig()
        config.authentication.enabled = True
        config.authentication.api_key = "test-key"
        init_authentication(config)

    def test_health_check_no_auth_required(self, client):
        """健康检查无需认证"""
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_registries_no_auth_required(self, client):
        """注册表端点无需认证"""
        resp = client.get("/api/v1/registries")
        assert resp.status_code == 200

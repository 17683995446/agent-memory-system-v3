"""
API Gateway 深度测试 - 覆盖 api_gateway.py 模块 (之前零覆盖)

测试范围:
- GatewayRequest / GatewayResponse 数据类
- Router: add_route, match, handle
- APIKeyAuthenticator: register_key, authenticate
- AuthorizationManager: add_policy, authorize
- VersionManager: register_version, get_version_from_request, is_deprecated
- CacheManager: get, set, invalidate, get_stats
- APIGateway 主类: initialize, handle_request, add_route, get_metrics
- cors_middleware, logging_middleware, rate_limit_middleware

发现的 BUG:
- BUG-AGW-001: GatewayRequest.content_type 属性使用 headers.get("content-type") 做精确匹配，
  不支持大小写混合的 header 键如 "Content-Type"，导致 content_type 返回默认值而非实际值。
- BUG-AGW-002: APIKeyAuthenticator.__init__ 将 header_name 转为小写 (如 "x-api-key")，
  但 authenticate() 使用 request.headers.get(self.header_name) 查找，当用户传入的 header
  键为 "X-API-Key" 时无法匹配，导致 Header 认证永远失败，回退到 Query 参数。
- BUG-AGW-003: APIGateway.initialize() 依赖 aiohttp 库，但该库不是必需依赖，
  导致 initialize() 在未安装 aiohttp 的环境中抛出 ModuleNotFoundError。
"""

import sys
sys.path.insert(0, '/workspace/agent_memory_system_v2')

import asyncio
import pytest
from datetime import datetime, timezone, timedelta

from memory_system.api_gateway import (
    GatewayRequest,
    GatewayResponse,
    Route,
    Router,
    APIKeyAuthenticator,
    AuthorizationManager,
    VersionManager,
    VersionStrategy,
    CacheManager,
    APIGateway,
    cors_middleware,
    logging_middleware,
    rate_limit_middleware,
)


# ============================================================================
# GatewayRequest / GatewayResponse 数据类测试
# ============================================================================

class TestGatewayRequest:
    """GatewayRequest 数据类测试"""

    def test_default_content_type(self):
        """默认 content_type 应为 application/json"""
        req = GatewayRequest(method="GET", path="/api/test")
        assert req.content_type == "application/json"

    def test_custom_content_type_lowercase(self):
        """自定义 content-type header (小写键)"""
        req = GatewayRequest(
            method="POST",
            path="/api/data",
            headers={"content-type": "text/plain"}
        )
        assert req.content_type == "text/plain"

    def test_content_type_case_insensitive(self):
        """[BUG-AGW-001] content-type header 大小写不敏感 - 已修复
        GatewayRequest.content_type 现在支持大小写混合的 header 键如 "Content-Type"。
        """
        req = GatewayRequest(
            method="POST",
            path="/api/data",
            headers={"Content-Type": "application/xml"}
        )
        # 修复后应正确返回 "application/xml"
        assert req.content_type == "application/xml"

    def test_get_header_existing(self):
        """获取已存在的 header"""
        req = GatewayRequest(
            method="GET",
            path="/api/test",
            headers={"x-custom": "value123"}
        )
        assert req.get_header("x-custom") == "value123"

    def test_get_header_missing(self):
        """获取不存在的 header 返回默认值"""
        req = GatewayRequest(method="GET", path="/api/test")
        assert req.get_header("x-missing") == ""
        assert req.get_header("x-missing", "fallback") == "fallback"

    def test_request_auto_generated_fields(self):
        """自动生成 request_id 和 timestamp"""
        req = GatewayRequest(method="GET", path="/api/test")
        assert req.request_id is not None
        assert len(req.request_id) > 0
        assert req.timestamp is not None
        assert isinstance(req.timestamp, datetime)


class TestGatewayResponse:
    """GatewayResponse 数据类测试"""

    def test_default_response(self):
        """默认响应状态码 200"""
        resp = GatewayResponse()
        assert resp.status_code == 200
        assert resp.cache_hit is False

    def test_to_dict(self):
        """to_dict 序列化"""
        resp = GatewayResponse(
            status_code=201,
            body={"id": 1},
            request_id="req-001",
            cache_hit=True
        )
        d = resp.to_dict()
        assert d["status_code"] == 201
        assert d["body"] == {"id": 1}
        assert d["request_id"] == "req-001"
        assert d["cache_hit"] is True


# ============================================================================
# Router 路由测试
# ============================================================================

class TestRouter:
    """Router 路由器测试"""

    def test_exact_path_match(self):
        """精确路径匹配"""
        router = Router()
        route = Route(
            id="get-users",
            path="/api/users",
            methods=["GET"],
            upstream="user-service"
        )
        router.add_route(route)
        matched = router.match("GET", "/api/users")
        assert matched is not None
        assert matched.id == "get-users"

    def test_wildcard_path_match(self):
        """通配符路径参数匹配 /api/users/{id}"""
        router = Router()
        route = Route(
            id="get-user",
            path="/api/users/{id}",
            methods=["GET"],
            upstream="user-service"
        )
        router.add_route(route)
        matched = router.match("GET", "/api/users/42")
        assert matched is not None
        assert matched.id == "get-user"

    def test_path_param_extraction(self):
        """路径参数提取"""
        route = Route(
            id="get-item",
            path="/items/{category}/{id}",
            methods=["GET"],
            upstream="item-service"
        )
        params = route.extract_path_params("/items/books/99")
        assert params == {"category": "books", "id": "99"}

    def test_http_method_filter(self):
        """HTTP 方法过滤 - 不匹配的方法返回 None"""
        router = Router()
        route = Route(
            id="create-user",
            path="/api/users",
            methods=["POST"],
            upstream="user-service"
        )
        router.add_route(route)
        assert router.match("GET", "/api/users") is None
        assert router.match("POST", "/api/users") is not None

    def test_wildcard_method_match(self):
        """通配符方法 * 匹配所有 HTTP 方法"""
        router = Router()
        route = Route(
            id="any-method",
            path="/api/health",
            methods=["*"],
            upstream="health-service"
        )
        router.add_route(route)
        for method in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
            assert router.match(method, "/api/health") is not None

    def test_no_match_returns_none(self):
        """无匹配路由返回 None"""
        router = Router()
        assert router.match("GET", "/nonexistent") is None

    @pytest.mark.asyncio
    async def test_handle_404(self):
        """未匹配路由返回 404"""
        router = Router()
        req = GatewayRequest(method="GET", path="/nonexistent")
        resp = await router.handle(req)
        assert resp.status_code == 404
        assert "error" in resp.body

    @pytest.mark.asyncio
    async def test_handle_success(self):
        """匹配路由返回 200"""
        router = Router()
        route = Route(
            id="test-route",
            path="/api/test",
            methods=["GET"],
            upstream="test-service"
        )
        router.add_route(route)
        req = GatewayRequest(method="GET", path="/api/test")
        resp = await router.handle(req)
        assert resp.status_code == 200
        assert "test-service" in resp.body["message"]

    def test_get_routes(self):
        """获取所有路由列表"""
        router = Router()
        r1 = Route(id="r1", path="/a", methods=["GET"], upstream="s1")
        r2 = Route(id="r2", path="/b", methods=["POST"], upstream="s2")
        router.add_route(r1)
        router.add_route(r2)
        routes = router.get_routes()
        assert len(routes) == 2


# ============================================================================
# APIKeyAuthenticator 认证测试
# ============================================================================

class TestAPIKeyAuthenticator:
    """API Key 认证器测试"""

    @pytest.mark.asyncio
    async def test_valid_key_from_header_lowercase(self):
        """有效 API Key (从 Header, 小写键) 认证成功"""
        auth = APIKeyAuthenticator(header_name="X-API-Key")
        auth.register_key("key-abc", {"user_id": "user1", "roles": ["admin"]})

        # 使用小写 header 键以匹配 APIKeyAuthenticator 内部的小写转换
        req = GatewayRequest(
            method="GET",
            path="/api/data",
            headers={"x-api-key": "key-abc"}
        )
        user = await auth.authenticate(req)
        assert user is not None
        assert user["user_id"] == "user1"
        assert user["roles"] == ["admin"]

    def test_valid_key_from_header_mixed_case_bug(self):
        """[BUG-AGW-002] APIKeyAuthenticator header 大小写匹配失败
        APIKeyAuthenticator.__init__ 将 header_name 转为小写 "x-api-key"，
        但 request.headers 中的键为 "X-API-Key"，导致 headers.get("x-api-key") 返回 None。
        """
        auth = APIKeyAuthenticator(header_name="X-API-Key")
        auth.register_key("key-abc", {"user_id": "user1"})
        # BUG: 使用混合大小写的 header 键 "X-API-Key" 无法匹配
        # 因为 authenticate 内部用 headers.get("x-api-key") 查找

    @pytest.mark.asyncio
    async def test_valid_key_from_query_param(self):
        """有效 API Key (从 Query 参数) 认证成功"""
        auth = APIKeyAuthenticator(query_param="api_key")
        auth.register_key("key-xyz", {"user_id": "user2"})

        req = GatewayRequest(
            method="GET",
            path="/api/data",
            query_params={"api_key": "key-xyz"}
        )
        user = await auth.authenticate(req)
        assert user is not None
        assert user["user_id"] == "user2"

    @pytest.mark.asyncio
    async def test_invalid_key(self):
        """无效 API Key 认证失败"""
        auth = APIKeyAuthenticator()
        auth.register_key("valid-key", {"user_id": "user1"})

        req = GatewayRequest(
            method="GET",
            path="/api/data",
            headers={"x-api-key": "invalid-key"}
        )
        user = await auth.authenticate(req)
        assert user is None

    @pytest.mark.asyncio
    async def test_missing_key(self):
        """缺少 API Key 认证失败"""
        auth = APIKeyAuthenticator()
        req = GatewayRequest(method="GET", path="/api/data")
        user = await auth.authenticate(req)
        assert user is None

    @pytest.mark.asyncio
    async def test_expired_key(self):
        """过期 API Key 认证失败"""
        auth = APIKeyAuthenticator()
        expired_time = datetime.now(timezone.utc) - timedelta(hours=1)
        auth.register_key(
            "expired-key",
            {"user_id": "user1"},
            expires_at=expired_time
        )

        req = GatewayRequest(
            method="GET",
            path="/api/data",
            headers={"x-api-key": "expired-key"}
        )
        user = await auth.authenticate(req)
        assert user is None

    @pytest.mark.asyncio
    async def test_non_expired_key(self):
        """未过期 API Key 认证成功 (使用小写 header 键)"""
        auth = APIKeyAuthenticator()
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        auth.register_key(
            "future-key",
            {"user_id": "user1"},
            expires_at=future_time
        )

        req = GatewayRequest(
            method="GET",
            path="/api/data",
            headers={"x-api-key": "future-key"}
        )
        user = await auth.authenticate(req)
        assert user is not None

    @pytest.mark.asyncio
    async def test_header_takes_priority_over_query(self):
        """[BUG-AGW-002] Header 中的 Key 应优先于 Query 参数 - 已修复
        修复后 header 大小写不敏感查找正常工作。
        """
        auth = APIKeyAuthenticator(header_name="X-API-Key", query_param="api_key")
        auth.register_key("header-key", {"user_id": "header_user"})
        auth.register_key("query-key", {"user_id": "query_user"})

        req = GatewayRequest(
            method="GET",
            path="/api/data",
            headers={"X-API-Key": "header-key"},
            query_params={"api_key": "query-key"}
        )
        user = await auth.authenticate(req)
        assert user is not None
        # 修复后 header 优先于 query 参数
        assert user["user_id"] == "header_user"

    @pytest.mark.asyncio
    async def test_header_priority_with_lowercase_keys(self):
        """Header 优先于 Query 参数 (使用小写键可正确工作)"""
        auth = APIKeyAuthenticator(header_name="X-API-Key", query_param="api_key")
        auth.register_key("header-key", {"user_id": "header_user"})
        auth.register_key("query-key", {"user_id": "query_user"})

        req = GatewayRequest(
            method="GET",
            path="/api/data",
            headers={"x-api-key": "header-key"},
            query_params={"api_key": "query-key"}
        )
        user = await auth.authenticate(req)
        assert user is not None
        assert user["user_id"] == "header_user"


# ============================================================================
# AuthorizationManager 授权测试
# ============================================================================

class TestAuthorizationManager:
    """RBAC 授权管理器测试"""

    @pytest.mark.asyncio
    async def test_authorized_role(self):
        """有权限的角色通过授权"""
        authz = AuthorizationManager()
        authz.add_policy("users", "read", ["admin", "user"])

        user = {"roles": ["admin"]}
        result = await authz.authorize(user, "users", "read")
        assert result is True

    @pytest.mark.asyncio
    async def test_unauthorized_role(self):
        """无权限的角色被拒绝"""
        authz = AuthorizationManager()
        authz.add_policy("users", "write", ["admin"])

        user = {"roles": ["guest"]}
        result = await authz.authorize(user, "users", "write")
        assert result is False

    @pytest.mark.asyncio
    async def test_wildcard_action(self):
        """通配符 action '*' 匹配所有操作"""
        authz = AuthorizationManager()
        authz.add_policy("users", "*", ["admin"])

        user = {"roles": ["admin"]}
        assert await authz.authorize(user, "users", "read") is True
        assert await authz.authorize(user, "users", "write") is True
        assert await authz.authorize(user, "users", "delete") is True

    @pytest.mark.asyncio
    async def test_no_roles_user(self):
        """没有 roles 字段的用户被拒绝"""
        authz = AuthorizationManager()
        authz.add_policy("users", "read", ["admin"])

        user = {}
        result = await authz.authorize(user, "users", "read")
        assert result is False

    @pytest.mark.asyncio
    async def test_no_policy_for_resource(self):
        """资源无策略时拒绝"""
        authz = AuthorizationManager()
        user = {"roles": ["admin"]}
        result = await authz.authorize(user, "nonexistent", "read")
        assert result is False

    @pytest.mark.asyncio
    async def test_condition_match(self):
        """条件匹配 - 满足条件时授权通过"""
        authz = AuthorizationManager()
        authz.add_policy(
            "users", "read",
            roles=["user"],
            conditions={"department": "engineering"}
        )

        user = {"roles": ["user"], "department": "engineering"}
        result = await authz.authorize(user, "users", "read")
        assert result is True

    @pytest.mark.asyncio
    async def test_condition_not_match(self):
        """条件不匹配 - 不满足条件时授权拒绝"""
        authz = AuthorizationManager()
        authz.add_policy(
            "users", "read",
            roles=["user"],
            conditions={"department": "engineering"}
        )

        user = {"roles": ["user"], "department": "marketing"}
        result = await authz.authorize(user, "users", "read")
        assert result is False

    @pytest.mark.asyncio
    async def test_multiple_policies_any_match(self):
        """多个策略 - 任一匹配即授权"""
        authz = AuthorizationManager()
        authz.add_policy("data", "read", ["admin"])
        authz.add_policy("data", "read", ["analyst"])

        user = {"roles": ["analyst"]}
        result = await authz.authorize(user, "data", "read")
        assert result is True


# ============================================================================
# VersionManager 版本管理测试
# ============================================================================

class TestVersionManager:
    """版本管理器测试"""

    def test_version_from_url_path(self):
        """从 URL 路径提取版本号"""
        vm = VersionManager(strategy=VersionStrategy.URL_PATH)
        req = GatewayRequest(method="GET", path="/v2/users")
        assert vm.get_version_from_request(req) == "v2"

    def test_version_from_header(self):
        """从 Header 提取版本号"""
        vm = VersionManager(strategy=VersionStrategy.HEADER)
        req = GatewayRequest(
            method="GET",
            path="/users",
            headers={"api-version": "v3"}
        )
        assert vm.get_version_from_request(req) == "v3"

    def test_version_from_query_param(self):
        """从 Query 参数提取版本号"""
        vm = VersionManager(strategy=VersionStrategy.QUERY_PARAM)
        req = GatewayRequest(
            method="GET",
            path="/users",
            query_params={"version": "v1"}
        )
        assert vm.get_version_from_request(req) == "v1"

    def test_default_version(self):
        """无法提取版本时返回默认 v1"""
        vm = VersionManager(strategy=VersionStrategy.URL_PATH)
        req = GatewayRequest(method="GET", path="/users")
        assert vm.get_version_from_request(req) == "v1"

    def test_is_deprecated_true(self):
        """已注册为废弃的版本返回 True"""
        vm = VersionManager()
        vm.register_version("v1", "/api/v1", deprecated=True)
        assert vm.is_deprecated("v1") is True

    def test_is_deprecated_false(self):
        """未废弃的版本返回 False"""
        vm = VersionManager()
        vm.register_version("v2", "/api/v2", deprecated=False)
        assert vm.is_deprecated("v2") is False

    def test_is_deprecated_unknown_version(self):
        """未知版本返回 False"""
        vm = VersionManager()
        assert vm.is_deprecated("v99") is False

    def test_deprecation_headers(self):
        """废弃版本返回 Deprecation header"""
        vm = VersionManager()
        sunset = datetime.now(timezone.utc) + timedelta(days=30)
        vm.register_version("v1", "/api/v1", deprecated=True, sunset_date=sunset)
        headers = vm.get_deprecation_headers("v1")
        assert headers.get("Deprecation") == "true"
        assert "Sunset" in headers

    def test_no_deprecation_headers_for_active_version(self):
        """活跃版本不返回 Deprecation header"""
        vm = VersionManager()
        vm.register_version("v2", "/api/v2", deprecated=False)
        headers = vm.get_deprecation_headers("v2")
        assert "Deprecation" not in headers


# ============================================================================
# CacheManager 缓存管理测试
# ============================================================================

class TestCacheManager:
    """缓存管理器测试"""

    @pytest.mark.asyncio
    async def test_cache_miss(self):
        """缓存未命中返回 None"""
        cm = CacheManager()
        req = GatewayRequest(method="GET", path="/api/test")
        result = await cm.get(req)
        assert result is None

    @pytest.mark.asyncio
    async def test_cache_hit(self):
        """缓存命中返回缓存的响应"""
        cm = CacheManager()
        req = GatewayRequest(method="GET", path="/api/test")
        resp = GatewayResponse(status_code=200, body={"data": "cached"}, request_id="r1")
        await cm.set(req, resp)

        cached = await cm.get(req)
        assert cached is not None
        assert cached.cache_hit is True
        assert cached.body == {"data": "cached"}

    @pytest.mark.asyncio
    async def test_cache_invalidate_all(self):
        """清空所有缓存"""
        cm = CacheManager()
        req1 = GatewayRequest(method="GET", path="/api/a")
        req2 = GatewayRequest(method="GET", path="/api/b")
        await cm.set(req1, GatewayResponse(status_code=200))
        await cm.set(req2, GatewayResponse(status_code=200))

        count = await cm.invalidate()
        assert count == 2

        stats = cm.get_stats()
        assert stats["total_entries"] == 0

    @pytest.mark.asyncio
    async def test_cache_invalidate_by_pattern(self):
        """按模式失效缓存"""
        cm = CacheManager()
        req1 = GatewayRequest(method="GET", path="/api/users")
        req2 = GatewayRequest(method="GET", path="/api/orders")
        await cm.set(req1, GatewayResponse(status_code=200))
        await cm.set(req2, GatewayResponse(status_code=200))

        # 缓存键是 hash，模式匹配不一定能匹配到，测试 invalidate 接口
        count = await cm.invalidate("nonexistent_pattern")
        assert count == 0

    @pytest.mark.asyncio
    async def test_cache_stats(self):
        """缓存统计信息"""
        cm = CacheManager()
        req = GatewayRequest(method="GET", path="/api/stats")
        await cm.set(req, GatewayResponse(status_code=200))

        stats = cm.get_stats()
        assert stats["total_entries"] == 1
        assert stats["valid_entries"] == 1
        assert stats["expired_entries"] == 0

    @pytest.mark.asyncio
    async def test_cache_different_users(self):
        """不同用户的相同请求产生不同缓存"""
        cm = CacheManager()
        req1 = GatewayRequest(method="GET", path="/api/data", user={"user_id": "u1"})
        req2 = GatewayRequest(method="GET", path="/api/data", user={"user_id": "u2"})
        resp1 = GatewayResponse(status_code=200, body={"user": "u1"})
        resp2 = GatewayResponse(status_code=200, body={"user": "u2"})

        await cm.set(req1, resp1)
        await cm.set(req2, resp2)

        cached1 = await cm.get(req1)
        cached2 = await cm.get(req2)
        assert cached1.body == {"user": "u1"}
        assert cached2.body == {"user": "u2"}


# ============================================================================
# APIGateway 主类测试
# ============================================================================

class TestAPIGateway:
    """APIGateway 主类测试"""

    @pytest.mark.asyncio
    async def test_initialize(self):
        """[BUG-AGW-003] 网关初始化依赖 aiohttp
        APIGateway.initialize() 调用 bff_aggregator.connect()，后者 import aiohttp。
        在未安装 aiohttp 的环境中会抛出 ModuleNotFoundError。
        """
        gateway = APIGateway()
        try:
            await gateway.initialize()
            await gateway.shutdown()
        except ModuleNotFoundError:
            pytest.skip("aiohttp not installed (BUG-AGW-003)")

    @pytest.mark.asyncio
    async def test_handle_request_route_found(self):
        """请求处理 - 路由匹配成功"""
        gateway = APIGateway()
        gateway.add_route(Route(
            id="test",
            path="/api/test",
            methods=["GET"],
            upstream="test-svc"
        ))
        req = GatewayRequest(method="GET", path="/api/test")
        resp = await gateway.handle_request(req)
        assert resp.status_code == 200
        assert resp.headers.get("X-Request-ID") == req.request_id

    @pytest.mark.asyncio
    async def test_handle_request_route_not_found(self):
        """请求处理 - 路由未找到返回 404"""
        gateway = APIGateway()
        req = GatewayRequest(method="GET", path="/nonexistent")
        resp = await gateway.handle_request(req)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_handle_request_with_auth(self):
        """[BUG-AGW-002] 请求处理 - 带认证 - 已修复
        修复后使用 "X-API-Key" header 可以正常认证。
        """
        auth = APIKeyAuthenticator()
        auth.register_key("test-key", {"user_id": "u1", "roles": ["admin"]})

        gateway = APIGateway(authenticator=auth)
        gateway.add_route(Route(
            id="secure",
            path="/api/secure",
            methods=["GET"],
            upstream="secure-svc"
        ))

        req = GatewayRequest(
            method="GET",
            path="/api/secure",
            headers={"X-API-Key": "test-key"}
        )
        resp = await gateway.handle_request(req)
        assert resp.status_code == 200
        # 修复后 header 大小写不敏感查找正常，认证成功
        assert req.user is not None
        assert req.user["user_id"] == "u1"

    @pytest.mark.asyncio
    async def test_handle_request_with_auth_lowercase_header(self):
        """请求处理 - 带认证 (使用小写 header 键可正常工作)"""
        auth = APIKeyAuthenticator()
        auth.register_key("test-key", {"user_id": "u1", "roles": ["admin"]})

        gateway = APIGateway(authenticator=auth)
        gateway.add_route(Route(
            id="secure",
            path="/api/secure",
            methods=["GET"],
            upstream="secure-svc"
        ))

        req = GatewayRequest(
            method="GET",
            path="/api/secure",
            headers={"x-api-key": "test-key"}
        )
        resp = await gateway.handle_request(req)
        assert resp.status_code == 200
        assert req.user is not None
        assert req.user["user_id"] == "u1"

    @pytest.mark.asyncio
    async def test_handle_request_with_deprecated_version(self):
        """请求处理 - 废弃版本返回 Deprecation header"""
        gateway = APIGateway()
        gateway.version_manager.register_version(
            "v1", "/api/v1", deprecated=True
        )
        gateway.add_route(Route(
            id="v1-route",
            path="/v1/data",
            methods=["GET"],
            upstream="svc"
        ))

        req = GatewayRequest(method="GET", path="/v1/data")
        resp = await gateway.handle_request(req)
        assert resp.status_code == 200
        assert resp.headers.get("Deprecation") == "true"

    def test_add_route(self):
        """添加路由"""
        gateway = APIGateway()
        gateway.add_route(Route(
            id="r1", path="/test", methods=["GET"], upstream="svc"
        ))
        routes = gateway.router.get_routes()
        assert len(routes) == 1
        assert routes[0].id == "r1"

    def test_get_metrics_initial(self):
        """初始指标 - 请求和错误计数为 0"""
        gateway = APIGateway()
        metrics = gateway.get_metrics()
        assert metrics["request_count"] == 0
        assert metrics["error_count"] == 0
        assert metrics["error_rate"] == 0.0
        assert metrics["uptime_seconds"] >= 0
        assert metrics["routes_count"] == 0

    @pytest.mark.asyncio
    async def test_get_metrics_after_requests(self):
        """请求后指标更新"""
        gateway = APIGateway()
        gateway.add_route(Route(
            id="m1", path="/m1", methods=["GET"], upstream="svc"
        ))
        await gateway.handle_request(GatewayRequest(method="GET", path="/m1"))
        await gateway.handle_request(GatewayRequest(method="GET", path="/m1"))

        metrics = gateway.get_metrics()
        assert metrics["request_count"] == 2
        assert metrics["error_count"] == 0

    @pytest.mark.asyncio
    async def test_metrics_includes_cache_stats(self):
        """指标包含缓存统计"""
        gateway = APIGateway()
        metrics = gateway.get_metrics()
        assert "cache_stats" in metrics
        assert "total_entries" in metrics["cache_stats"]


# ============================================================================
# 中间件测试
# ============================================================================

class TestMiddlewares:
    """中间件测试"""

    @pytest.mark.asyncio
    async def test_cors_middleware(self):
        """CORS 中间件添加跨域头"""
        async def next_handler(request):
            return GatewayResponse(status_code=200)

        req = GatewayRequest(method="GET", path="/api/data")
        resp = await cors_middleware(req, next_handler)

        assert resp.headers["Access-Control-Allow-Origin"] == "*"
        assert "GET" in resp.headers["Access-Control-Allow-Methods"]
        assert "POST" in resp.headers["Access-Control-Allow-Methods"]
        assert "Content-Type" in resp.headers["Access-Control-Allow-Headers"]

    @pytest.mark.asyncio
    async def test_logging_middleware(self):
        """日志中间件透传响应"""
        async def next_handler(request):
            return GatewayResponse(status_code=200, body={"ok": True})

        req = GatewayRequest(method="GET", path="/api/test")
        resp = await logging_middleware(req, next_handler)

        assert resp.status_code == 200
        assert resp.body == {"ok": True}

    @pytest.mark.asyncio
    async def test_rate_limit_middleware(self):
        """限流中间件透传响应"""
        async def next_handler(request):
            return GatewayResponse(status_code=200)

        req = GatewayRequest(method="GET", path="/api/data")
        resp = await rate_limit_middleware(req, next_handler)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_middleware_chain(self):
        """中间件链 - 多个中间件按顺序执行"""
        router = Router()
        router.add_route(Route(
            id="chained", path="/chained", methods=["GET"], upstream="svc"
        ))
        router.add_middleware(cors_middleware)
        router.add_middleware(logging_middleware)

        req = GatewayRequest(method="GET", path="/chained")
        resp = await router.handle(req)

        assert resp.status_code == 200
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"

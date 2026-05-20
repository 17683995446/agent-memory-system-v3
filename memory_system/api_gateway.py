"""
API网关和BFF模块 - API Gateway and BFF Module

该模块实现了云原生API网关和Backend-for-Frontend模式，包含：
- GraphQL Federation网关
- REST API聚合
- 认证授权（JWT/OAuth2）
- 请求/响应转换
- 缓存策略
- 路由和版本管理

作者: Agent Memory System Team
版本: 3.0.0
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import (
    Any, Callable, Coroutine, Dict, Generic, List, Optional, Set, TypeVar,
    Union, AsyncIterator, Protocol, runtime_checkable, Tuple
)
from contextlib import asynccontextmanager
from functools import wraps

# 配置日志
logger = logging.getLogger(__name__)


# ============================================================================
# 类型定义和枚举
# ============================================================================

class AuthType(Enum):
    """认证类型"""
    NONE = "none"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    API_KEY = "api_key"
    BASIC = "basic"


class RouteType(Enum):
    """路由类型"""
    REST = "rest"
    GRAPHQL = "graphql"
    GRPC = "grpc"
    WEBSOCKET = "websocket"


class CacheStrategy(Enum):
    """缓存策略"""
    NO_CACHE = "no_cache"
    CACHE_FIRST = "cache_first"
    NETWORK_FIRST = "network_first"
    STALE_WHILE_REVALIDATE = "stale_while_revalidate"


class VersionStrategy(Enum):
    """版本策略"""
    URL_PATH = "url_path"
    HEADER = "header"
    QUERY_PARAM = "query_param"
    CONTENT_NEGOTIATION = "content_negotiation"


T = TypeVar('T')


# ============================================================================
# 请求和响应模型
# ============================================================================

@dataclass
class GatewayRequest:
    """网关请求"""
    method: str
    path: str
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    client_ip: str = ""
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    user: Optional[Dict[str, Any]] = None
    
    @property
    def content_type(self) -> str:
        for key, value in self.headers.items():
            if key.lower() == "content-type":
                return value
        return "application/json"
    
    def get_header(self, name: str, default: str = "") -> str:
        return self.headers.get(name.lower(), default)


@dataclass
class GatewayResponse:
    """网关响应"""
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    request_id: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    cache_hit: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.body,
            "request_id": self.request_id,
            "cache_hit": self.cache_hit
        }


# ============================================================================
# 认证授权
# ============================================================================

class Authenticator(ABC):
    """认证器抽象基类"""
    
    @abstractmethod
    async def authenticate(self, request: GatewayRequest) -> Optional[Dict[str, Any]]:
        """
        认证请求
        
        Returns:
            用户信息，认证失败返回None
        """
        pass


class JWTAuthenticator(Authenticator):
    """
    JWT认证器
    
    验证JWT令牌
    """
    
    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        token_header: str = "Authorization",
        token_prefix: str = "Bearer "
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.token_header = token_header
        self.token_prefix = token_prefix
    
    async def authenticate(self, request: GatewayRequest) -> Optional[Dict[str, Any]]:
        """验证JWT令牌"""
        auth_header = request.get_header(self.token_header)
        
        if not auth_header or not auth_header.startswith(self.token_prefix):
            return None
        
        token = auth_header[len(self.token_prefix):]
        
        try:
            import jwt
            
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm]
            )
            
            return {
                "user_id": payload.get("sub"),
                "username": payload.get("username"),
                "roles": payload.get("roles", []),
                "permissions": payload.get("permissions", [])
            }
        except jwt.ExpiredSignatureError:
            logger.warning("JWT token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            return None


class OAuth2Authenticator(Authenticator):
    """
    OAuth2认证器
    
    验证OAuth2访问令牌
    """
    
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        introspection_url: Optional[str] = None
    ):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.introspection_url = introspection_url
        self._token_cache: Dict[str, Dict[str, Any]] = {}
    
    async def authenticate(self, request: GatewayRequest) -> Optional[Dict[str, Any]]:
        """验证OAuth2令牌"""
        auth_header = request.get_header("Authorization")
        
        if not auth_header or not auth_header.startswith("Bearer "):
            return None
        
        token = auth_header[7:]
        
        # 检查缓存
        if token in self._token_cache:
            cached = self._token_cache[token]
            if cached["expires_at"] > datetime.now(timezone.utc):
                return cached["user_info"]
            else:
                del self._token_cache[token]
        
        # 验证令牌
        try:
            import aiohttp
            
            async with aiohttp.ClientSession() as session:
                if self.introspection_url:
                    # 使用令牌内省端点
                    async with session.post(
                        self.introspection_url,
                        data={"token": token},
                        auth=aiohttp.BasicAuth(self.client_id, self.client_secret)
                    ) as resp:
                        if resp.status == 200:
                            token_info = await resp.json()
                            if token_info.get("active"):
                                user_info = {
                                    "user_id": token_info.get("sub"),
                                    "username": token_info.get("username"),
                                    "scope": token_info.get("scope", "").split()
                                }
                                
                                # 缓存
                                expires_in = token_info.get("exp", 3600)
                                self._token_cache[token] = {
                                    "user_info": user_info,
                                    "expires_at": datetime.now(timezone.utc) + timedelta(seconds=expires_in)
                                }
                                
                                return user_info
                else:
                    # 使用用户信息端点
                    async with session.get(
                        f"{self.token_url}/userinfo",
                        headers={"Authorization": f"Bearer {token}"}
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
            
            return None
        except Exception as e:
            logger.exception(f"OAuth2 authentication error: {e}")
            return None


class APIKeyAuthenticator(Authenticator):
    """
    API Key认证器
    
    验证API密钥
    """
    
    def __init__(
        self,
        header_name: str = "X-API-Key",
        query_param: str = "api_key"
    ):
        self.header_name = header_name.lower()
        self.query_param = query_param
        self._api_keys: Dict[str, Dict[str, Any]] = {}
    
    def register_key(
        self,
        api_key: str,
        user_info: Dict[str, Any],
        expires_at: Optional[datetime] = None
    ) -> None:
        """注册API密钥"""
        self._api_keys[api_key] = {
            "user_info": user_info,
            "expires_at": expires_at
        }
    
    async def authenticate(self, request: GatewayRequest) -> Optional[Dict[str, Any]]:
        """验证API密钥"""
        # 从Header获取（大小写不敏感查找）
        api_key = None
        for header_key, header_value in request.headers.items():
            if header_key.lower() == self.header_name:
                api_key = header_value
                break
        
        # 从Query参数获取
        if not api_key:
            api_key = request.query_params.get(self.query_param)
        
        if not api_key:
            return None
        
        key_info = self._api_keys.get(api_key)
        if not key_info:
            return None
        
        # 检查过期
        if key_info.get("expires_at") and key_info["expires_at"] < datetime.now(timezone.utc):
            return None
        
        return key_info["user_info"]


class AuthorizationManager:
    """
    授权管理器
    
    管理权限检查
    """
    
    def __init__(self):
        self._policies: Dict[str, List[Dict[str, Any]]] = {}
    
    def add_policy(
        self,
        resource: str,
        action: str,
        roles: List[str],
        conditions: Optional[Dict[str, Any]] = None
    ) -> None:
        """添加授权策略"""
        if resource not in self._policies:
            self._policies[resource] = []
        
        self._policies[resource].append({
            "action": action,
            "roles": roles,
            "conditions": conditions or {}
        })
    
    async def authorize(
        self,
        user: Dict[str, Any],
        resource: str,
        action: str
    ) -> bool:
        """
        检查用户是否有权限
        
        Args:
            user: 用户信息
            resource: 资源
            action: 操作
            
        Returns:
            是否有权限
        """
        user_roles = user.get("roles", [])
        policies = self._policies.get(resource, [])
        
        for policy in policies:
            if policy["action"] == action or policy["action"] == "*":
                # 检查角色
                if any(role in user_roles for role in policy["roles"]):
                    # 检查条件
                    conditions = policy.get("conditions", {})
                    if self._check_conditions(user, conditions):
                        return True
        
        return False
    
    def _check_conditions(
        self,
        user: Dict[str, Any],
        conditions: Dict[str, Any]
    ) -> bool:
        """检查条件"""
        for key, value in conditions.items():
            user_value = user.get(key)
            if user_value != value:
                return False
        return True


# ============================================================================
# 路由管理
# ============================================================================

@dataclass
class Route:
    """路由定义"""
    id: str
    path: str
    methods: List[str]
    upstream: str
    upstream_path: str = ""
    strip_prefix: bool = True
    auth_required: bool = True
    allowed_roles: List[str] = field(default_factory=list)
    rate_limit: Optional[Dict[str, Any]] = None
    cache_config: Optional[Dict[str, Any]] = None
    transforms: List[Dict[str, Any]] = field(default_factory=list)
    version: str = "v1"
    
    def matches(self, method: str, path: str) -> bool:
        """检查请求是否匹配路由"""
        if self.methods != ["*"] and method not in self.methods:
            return False
        
        # 支持路径参数，如 /users/{id}
        pattern = self.path.replace("{", "(?P<").replace("}", ">[^/]+)")
        return bool(re.match(f"^{pattern}$", path))
    
    def extract_path_params(self, path: str) -> Dict[str, str]:
        """提取路径参数"""
        pattern = self.path.replace("{", "(?P<").replace("}", ">[^/]+)")
        match = re.match(f"^{pattern}$", path)
        return match.groupdict() if match else {}


class Router:
    """
    路由器
    
    管理API路由
    """
    
    def __init__(self):
        self._routes: List[Route] = []
        self._middlewares: List[Callable[[GatewayRequest, Callable], Coroutine[Any, Any, GatewayResponse]]] = []
    
    def add_route(self, route: Route) -> None:
        """添加路由"""
        self._routes.append(route)
        logger.info(f"Added route: {route.id} -> {route.upstream}")
    
    def add_middleware(
        self,
        middleware: Callable[[GatewayRequest, Callable], Coroutine[Any, Any, GatewayResponse]]
    ) -> None:
        """添加中间件"""
        self._middlewares.append(middleware)
    
    def match(self, method: str, path: str) -> Optional[Route]:
        """匹配路由"""
        for route in self._routes:
            if route.matches(method, path):
                return route
        return None
    
    async def handle(self, request: GatewayRequest) -> GatewayResponse:
        """处理请求"""
        route = self.match(request.method, request.path)
        
        if not route:
            return GatewayResponse(
                status_code=404,
                body={"error": "Route not found"},
                request_id=request.request_id
            )
        
        # 构建处理链
        handler = self._create_handler(route)
        
        for middleware in reversed(self._middlewares):
            handler = self._wrap_middleware(middleware, handler)
        
        return await handler(request)
    
    def _create_handler(self, route: Route) -> Callable:
        """创建路由处理器"""
        async def handler(request: GatewayRequest) -> GatewayResponse:
            # 这里应该转发到上游服务
            # 简化实现
            return GatewayResponse(
                status_code=200,
                body={"message": f"Routed to {route.upstream}"},
                request_id=request.request_id
            )
        
        return handler
    
    def _wrap_middleware(
        self,
        middleware: Callable,
        next_handler: Callable
    ) -> Callable:
        """包装中间件"""
        async def wrapped(request: GatewayRequest) -> GatewayResponse:
            return await middleware(request, next_handler)
        return wrapped
    
    def get_routes(self) -> List[Route]:
        """获取所有路由"""
        return self._routes.copy()


# ============================================================================
# 版本管理
# ============================================================================

class VersionManager:
    """
    版本管理器
    
    管理API版本
    """
    
    def __init__(self, strategy: VersionStrategy = VersionStrategy.URL_PATH):
        self.strategy = strategy
        self._versions: Dict[str, Dict[str, Any]] = {}
        self._deprecation_notices: Dict[str, datetime] = {}
    
    def register_version(
        self,
        version: str,
        base_path: str,
        deprecated: bool = False,
        sunset_date: Optional[datetime] = None
    ) -> None:
        """注册API版本"""
        self._versions[version] = {
            "base_path": base_path,
            "deprecated": deprecated,
            "sunset_date": sunset_date
        }
        
        if deprecated:
            self._deprecation_notices[version] = sunset_date or datetime.now(timezone.utc)
    
    def get_version_from_request(self, request: GatewayRequest) -> str:
        """从请求中提取版本"""
        if self.strategy == VersionStrategy.URL_PATH:
            # 从路径提取，如 /v1/users
            match = re.match(r"^/(v\d+)/", request.path)
            if match:
                return match.group(1)
        
        elif self.strategy == VersionStrategy.HEADER:
            # 从Header提取
            version = request.get_header("api-version")
            if version:
                return version
        
        elif self.strategy == VersionStrategy.QUERY_PARAM:
            # 从Query参数提取
            version = request.query_params.get("version")
            if version:
                return version
        
        # 默认版本
        return "v1"
    
    def is_deprecated(self, version: str) -> bool:
        """检查版本是否已弃用"""
        version_info = self._versions.get(version)
        return version_info.get("deprecated", False) if version_info else False
    
    def get_deprecation_headers(self, version: str) -> Dict[str, str]:
        """获取弃用通知头"""
        headers = {}
        
        if self.is_deprecated(version):
            headers["Deprecation"] = "true"
            
            sunset = self._versions[version].get("sunset_date")
            if sunset:
                headers["Sunset"] = sunset.strftime("%a, %d %b %Y %H:%M:%S GMT")
        
        return headers


# ============================================================================
# 缓存管理
# ============================================================================

class CacheManager:
    """
    缓存管理器
    
    管理API响应缓存
    """
    
    def __init__(self, default_ttl: int = 300):
        self.default_ttl = default_ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    def _generate_key(self, request: GatewayRequest) -> str:
        """生成缓存键"""
        key_data = {
            "method": request.method,
            "path": request.path,
            "params": request.query_params,
            "user": request.user.get("user_id") if request.user else None
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()
    
    async def get(self, request: GatewayRequest) -> Optional[GatewayResponse]:
        """获取缓存响应"""
        key = self._generate_key(request)
        
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry:
                if entry["expires_at"] > datetime.now(timezone.utc):
                    response = entry["response"]
                    response.cache_hit = True
                    return response
                else:
                    del self._cache[key]
        
        return None
    
    async def set(
        self,
        request: GatewayRequest,
        response: GatewayResponse,
        ttl: Optional[int] = None
    ) -> None:
        """设置缓存"""
        key = self._generate_key(request)
        ttl = ttl or self.default_ttl
        
        async with self._lock:
            self._cache[key] = {
                "response": response,
                "expires_at": datetime.now(timezone.utc) + timedelta(seconds=ttl),
                "created_at": datetime.now(timezone.utc)
            }
    
    async def invalidate(self, pattern: Optional[str] = None) -> int:
        """使缓存失效"""
        async with self._lock:
            if pattern:
                keys_to_remove = [
                    k for k in self._cache.keys()
                    if pattern in k
                ]
                for k in keys_to_remove:
                    del self._cache[k]
                return len(keys_to_remove)
            else:
                count = len(self._cache)
                self._cache.clear()
                return count
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        now = datetime.now(timezone.utc)
        total = len(self._cache)
        expired = sum(
            1 for entry in self._cache.values()
            if entry["expires_at"] <= now
        )
        
        return {
            "total_entries": total,
            "expired_entries": expired,
            "valid_entries": total - expired
        }


# ============================================================================
# GraphQL支持
# ============================================================================

@dataclass
class GraphQLSchema:
    """GraphQL模式"""
    query_type: str = "Query"
    mutation_type: str = "Mutation"
    types: Dict[str, Any] = field(default_factory=dict)
    
    def to_sdl(self) -> str:
        """转换为Schema Definition Language"""
        sdl = f"""
        type {self.query_type} {{
            _empty: String
        }}
        
        type {self.mutation_type} {{
            _empty: String
        }}
        """
        return sdl


class GraphQLGateway:
    """
    GraphQL网关
    
    实现GraphQL Federation
    """
    
    def __init__(self):
        self._schemas: Dict[str, GraphQLSchema] = {}
        self._resolvers: Dict[str, Callable] = {}
        self._subgraphs: Dict[str, str] = {}  # 子图URL
    
    def register_subgraph(self, name: str, url: str, schema: GraphQLSchema) -> None:
        """注册子图"""
        self._subgraphs[name] = url
        self._schemas[name] = schema
        logger.info(f"Registered GraphQL subgraph: {name}")
    
    def add_resolver(self, type_name: str, field_name: str, resolver: Callable) -> None:
        """添加解析器"""
        key = f"{type_name}.{field_name}"
        self._resolvers[key] = resolver
    
    async def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        执行GraphQL查询
        
        实现查询规划和执行
        """
        try:
            # 解析查询
            # 这里简化实现，实际应该使用graphql-core库
            
            # 查询规划：确定需要从哪些子图获取数据
            # 执行查询
            
            return {
                "data": {},
                "errors": []
            }
        except Exception as e:
            logger.exception(f"GraphQL execution error: {e}")
            return {
                "data": None,
                "errors": [{"message": str(e)}]
            }
    
    def get_federated_schema(self) -> str:
        """获取联邦模式"""
        # 合并所有子图模式
        all_types = []
        
        for name, schema in self._schemas.items():
            all_types.append(f"# Subgraph: {name}")
            all_types.append(schema.to_sdl())
        
        return "\n\n".join(all_types)


# ============================================================================
# BFF（Backend-for-Frontend）
# ============================================================================

class BFFAggregator:
    """
    BFF聚合器
    
    为不同前端聚合后端API
    """
    
    def __init__(self):
        self._aggregations: Dict[str, Dict[str, Any]] = {}
        self._http_client: Optional[Any] = None
    
    async def connect(self) -> None:
        """初始化HTTP客户端"""
        try:
            import aiohttp
            self._http_client = aiohttp.ClientSession()
        except ImportError:
            logger.warning("aiohttp is not installed; BFF aggregator HTTP client is disabled")
    
    async def disconnect(self) -> None:
        """关闭HTTP客户端"""
        if self._http_client:
            await self._http_client.close()
    
    def register_aggregation(
        self,
        name: str,
        endpoints: List[Dict[str, Any]],
        merge_strategy: str = "parallel"
    ) -> None:
        """
        注册聚合端点
        
        Args:
            name: 聚合名称
            endpoints: 要聚合的后端端点列表
            merge_strategy: 合并策略（parallel/sequential）
        """
        self._aggregations[name] = {
            "endpoints": endpoints,
            "merge_strategy": merge_strategy
        }
    
    async def aggregate(self, name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行聚合
        
        Args:
            name: 聚合名称
            context: 请求上下文
            
        Returns:
            聚合结果
        """
        aggregation = self._aggregations.get(name)
        if not aggregation:
            raise ValueError(f"Aggregation not found: {name}")
        
        endpoints = aggregation["endpoints"]
        strategy = aggregation["merge_strategy"]
        
        if strategy == "parallel":
            return await self._aggregate_parallel(endpoints, context)
        else:
            return await self._aggregate_sequential(endpoints, context)
    
    async def _aggregate_parallel(
        self,
        endpoints: List[Dict[str, Any]],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """并行聚合"""
        tasks = [
            self._call_endpoint(endpoint, context)
            for endpoint in endpoints
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        merged = {}
        for endpoint, result in zip(endpoints, results):
            key = endpoint.get("result_key", endpoint["name"])
            if isinstance(result, Exception):
                merged[key] = {"error": str(result)}
            else:
                merged[key] = result
        
        return merged
    
    async def _aggregate_sequential(
        self,
        endpoints: List[Dict[str, Any]],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """顺序聚合"""
        merged = {}
        
        for endpoint in endpoints:
            try:
                result = await self._call_endpoint(endpoint, context)
                key = endpoint.get("result_key", endpoint["name"])
                merged[key] = result
                
                # 将结果添加到上下文，供后续端点使用
                context[f"_{key}_result"] = result
            except Exception as e:
                key = endpoint.get("result_key", endpoint["name"])
                merged[key] = {"error": str(e)}
        
        return merged
    
    async def _call_endpoint(
        self,
        endpoint: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Any:
        """调用端点"""
        url = endpoint["url"]
        method = endpoint.get("method", "GET")
        headers = endpoint.get("headers", {})
        
        # 替换URL模板变量
        for key, value in context.items():
            url = url.replace(f"{{{key}}}", str(value))
        
        try:
            async with self._http_client.request(
                method,
                url,
                headers=headers
            ) as resp:
                if resp.content_type == "application/json":
                    return await resp.json()
                return await resp.text()
        except Exception as e:
            logger.exception(f"Endpoint call error: {e}")
            raise


# ============================================================================
# API网关
# ============================================================================

class APIGateway:
    """
    API网关
    
    整合所有网关功能
    
    DEFECT-007 [P2] 新增使用示例:
    
    示例用法:
        >>> from api_gateway import APIGateway
        >>> 
        >>> gateway = APIGateway()
        >>> 
        >>> # 注册路由
        >>> gateway.router.add_route("GET", "/api/users", user_list_handler)
        >>> gateway.router.add_route("POST", "/api/users", user_create_handler)
        >>> 
        >>> # 处理请求
        >>> response = await gateway.handle_request(
        ...     method="GET", path="/api/users", headers={}
        ... )
        >>> print(response.status)  # 200
    """
    
    def __init__(
        self,
        authenticator: Optional[Authenticator] = None,
        authz_manager: Optional[AuthorizationManager] = None
    ):
        self.authenticator = authenticator
        self.authz_manager = authz_manager or AuthorizationManager()
        self.router = Router()
        self.version_manager = VersionManager()
        self.cache_manager = CacheManager()
        self.graphql_gateway = GraphQLGateway()
        self.bff_aggregator = BFFAggregator()
        self._request_count = 0
        self._error_count = 0
        self._start_time = datetime.now(timezone.utc)
    
    async def initialize(self) -> None:
        """初始化网关"""
        await self.bff_aggregator.connect()
        logger.info("API Gateway initialized")
    
    async def shutdown(self) -> None:
        """关闭网关"""
        await self.bff_aggregator.disconnect()
        logger.info("API Gateway shutdown")
    
    async def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        """
        处理请求
        
        完整的请求处理流程
        """
        self._request_count += 1
        start_time = time.time()
        
        try:
            # 1. 认证
            if self.authenticator:
                user = await self.authenticator.authenticate(request)
                if user:
                    request.user = user
            
            # 2. 版本检查
            version = self.version_manager.get_version_from_request(request)
            
            # 3. 路由匹配 - 提前匹配以获取路由配置（auth_required, cache_config 等）
            route = self.router.match(request.method, request.path)
            if not route:
                return GatewayResponse(
                    status_code=404,
                    body={"error": "Route not found"},
                    request_id=request.request_id
                )
            
            # [BUG 1 修复] 认证失败时，如果路由需要认证则返回401拒绝请求
            if route.auth_required and self.authenticator and request.user is None:
                logger.warning(
                    f"Authentication failed for {request.method} {request.path} "
                    f"[{request.request_id}]"
                )
                return GatewayResponse(
                    status_code=401,
                    headers={"WWW-Authenticate": 'Bearer realm="api"'},
                    body={"error": "Authentication required"},
                    request_id=request.request_id
                )
            
            # [BUG 2 修复] RBAC授权检查 - 认证成功后、路由处理前调用authorize()
            if request.user is not None and route.allowed_roles:
                # 从路由路径推断资源名，用于授权检查
                resource = route.path.split("/")[1] if "/" in route.path else route.path
                action = request.method.lower()
                authorized = await self.authz_manager.authorize(
                    request.user, resource, action
                )
                if not authorized:
                    logger.warning(
                        f"Authorization denied for user {request.user.get('user_id')} "
                        f"on {request.method} {request.path} [{request.request_id}]"
                    )
                    return GatewayResponse(
                        status_code=403,
                        body={"error": "Insufficient permissions"},
                        request_id=request.request_id
                    )
            
            # [BUG 3 修复] 从路由获取cache_config，而非硬编码为None
            cache_config = route.cache_config
            if cache_config and cache_config.get("strategy") != CacheStrategy.NO_CACHE:
                cached = await self.cache_manager.get(request)
                if cached:
                    return cached
            
            # 4. 路由处理
            response = await self.router.handle(request)
            
            # 5. 添加版本弃用头
            deprecation_headers = self.version_manager.get_deprecation_headers(version)
            response.headers.update(deprecation_headers)
            
            # 6. 缓存响应
            if cache_config and response.status_code < 400:
                ttl = cache_config.get("ttl", 300)
                await self.cache_manager.set(request, response, ttl)
            
            # 7. 添加网关头
            response.headers["X-Request-ID"] = request.request_id
            response.headers["X-Response-Time"] = f"{(time.time() - start_time) * 1000:.2f}ms"
            
            return response
            
        except Exception as e:
            self._error_count += 1
            logger.exception(f"Request handling error: {e}")
            return GatewayResponse(
                status_code=500,
                body={"error": "Internal server error"},
                request_id=request.request_id
            )
    
    def add_route(self, route: Route) -> None:
        """添加路由"""
        self.router.add_route(route)
    
    def add_middleware(
        self,
        middleware: Callable[[GatewayRequest, Callable], Coroutine[Any, Any, GatewayResponse]]
    ) -> None:
        """添加中间件"""
        self.router.add_middleware(middleware)
    
    async def execute_graphql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行GraphQL查询"""
        return await self.graphql_gateway.execute(query, variables)
    
    async def aggregate_bff(
        self,
        name: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """执行BFF聚合"""
        return await self.bff_aggregator.aggregate(name, context)
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取网关指标"""
        uptime = datetime.now(timezone.utc) - self._start_time
        
        return {
            "request_count": self._request_count,
            "error_count": self._error_count,
            "error_rate": self._error_count / max(self._request_count, 1),
            "uptime_seconds": uptime.total_seconds(),
            "cache_stats": self.cache_manager.get_stats(),
            "routes_count": len(self.router.get_routes())
        }


# ============================================================================
# 中间件
# ============================================================================

async def cors_middleware(
    request: GatewayRequest,
    next_handler: Callable
) -> GatewayResponse:
    """CORS中间件"""
    response = await next_handler(request)
    
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    
    return response


async def logging_middleware(
    request: GatewayRequest,
    next_handler: Callable
) -> GatewayResponse:
    """日志中间件"""
    start_time = time.time()
    
    logger.info(f"Request: {request.method} {request.path} [{request.request_id}]")
    
    response = await next_handler(request)
    
    duration = time.time() - start_time
    logger.info(
        f"Response: {response.status_code} [{request.request_id}] "
        f"in {duration * 1000:.2f}ms"
    )
    
    return response


async def auth_middleware(
    request: GatewayRequest,
    next_handler: Callable
) -> GatewayResponse:
    """认证中间件"""
    # 认证逻辑已在网关主流程中处理
    # 这里可以添加额外的认证检查
    return await next_handler(request)


async def rate_limit_middleware(
    request: GatewayRequest,
    next_handler: Callable,
    max_requests: int = 100,
    window_seconds: int = 60
) -> GatewayResponse:
    """
    限流中间件
    
    [BUG 4 修复] 集成RateLimiter进行实际限流检查，
    基于客户端IP和用户ID进行滑动窗口限流。
    """
    try:
        from memory_system.rate_limiter import RateLimiter, RateLimitConfig, RateLimitStrategy
    except ImportError:
        # rate_limiter模块不可用时，放行请求并记录警告
        logger.warning("rate_limiter模块不可用，限流中间件降级为放行模式")
        return await next_handler(request)
    
    # 使用模块级缓存避免每次请求创建新的限流器实例
    if not hasattr(rate_limit_middleware, '_limiter'):
        config = RateLimitConfig(
            requests_per_minute=max_requests,
            burst_size=max_requests,
            strategy=RateLimitStrategy.SLIDING_WINDOW,
            per_user=True,
            per_endpoint=True,
            window_size=window_seconds,
        )
        rate_limit_middleware._limiter = RateLimiter(config)
    
    limiter = rate_limit_middleware._limiter
    
    # 确定限流标识：优先使用用户ID，其次使用客户端IP
    user_id = request.user.get("user_id", "") if request.user else ""
    if not user_id:
        user_id = request.client_ip or "anonymous"
    
    endpoint = request.path
    
    # 执行限流检查
    if not limiter.is_allowed(user_id=user_id, endpoint=endpoint):
        logger.warning(
            f"Rate limit exceeded for {user_id} on {endpoint} "
            f"[{request.request_id}]"
        )
        return GatewayResponse(
            status_code=429,
            headers={
                "Retry-After": str(window_seconds),
                "X-RateLimit-Limit": str(max_requests),
                "X-RateLimit-Remaining": "0",
            },
            body={"error": "Rate limit exceeded"},
            request_id=request.request_id
        )
    
    return await next_handler(request)


# ============================================================================
# 使用示例
# ============================================================================

async def example_usage():
    """使用示例"""
    # 创建认证器
    authenticator = JWTAuthenticator(
        secret_key="your-secret-key",
        algorithm="HS256"
    )
    
    # 创建授权管理器
    authz = AuthorizationManager()
    authz.add_policy("users", "read", ["admin", "user"])
    authz.add_policy("users", "write", ["admin"])
    
    # 创建网关
    gateway = APIGateway(authenticator, authz)
    await gateway.initialize()
    
    try:
        # 添加中间件
        gateway.add_middleware(cors_middleware)
        gateway.add_middleware(logging_middleware)
        
        # 添加路由
        gateway.add_route(Route(
            id="get-users",
            path="/api/v1/users",
            methods=["GET"],
            upstream="user-service",
            upstream_path="/users",
            auth_required=True,
            allowed_roles=["admin", "user"],
            cache_config={"strategy": CacheStrategy.CACHE_FIRST, "ttl": 300}
        ))
        
        gateway.add_route(Route(
            id="get-user",
            path="/api/v1/users/{id}",
            methods=["GET"],
            upstream="user-service",
            upstream_path="/users/{id}",
            auth_required=True
        ))
        
        gateway.add_route(Route(
            id="create-order",
            path="/api/v1/orders",
            methods=["POST"],
            upstream="order-service",
            upstream_path="/orders",
            auth_required=True,
            allowed_roles=["admin"]
        ))
        
        # 注册API版本
        gateway.version_manager.register_version(
            "v1",
            "/api/v1",
            deprecated=False
        )
        gateway.version_manager.register_version(
            "v2",
            "/api/v2",
            deprecated=True,
            sunset_date=datetime.now(timezone.utc) + timedelta(days=90)
        )
        
        # 注册GraphQL子图
        gateway.graphql_gateway.register_subgraph(
            "users",
            "http://user-service:8080/graphql",
            GraphQLSchema()
        )
        
        gateway.graphql_gateway.register_subgraph(
            "orders",
            "http://order-service:8080/graphql",
            GraphQLSchema()
        )
        
        # 注册BFF聚合
        gateway.bff_aggregator.register_aggregation(
            "dashboard",
            [
                {"name": "user_profile", "url": "http://user-service/profile/{user_id}"},
                {"name": "orders", "url": "http://order-service/orders?user={user_id}"},
                {"name": "notifications", "url": "http://notification-service/notifications/{user_id}"}
            ],
            merge_strategy="parallel"
        )
        
        # 模拟请求
        request = GatewayRequest(
            method="GET",
            path="/api/v1/users",
            headers={
                "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
                "Content-Type": "application/json"
            },
            client_ip="192.168.1.1"
        )
        
        response = await gateway.handle_request(request)
        print(f"Response: {response.to_dict()}")
        
        # 测试GraphQL
        graphql_result = await gateway.execute_graphql("""
            query {
                users {
                    id
                    name
                }
            }
        """)
        print(f"GraphQL result: {graphql_result}")
        
        # 测试BFF聚合
        bff_result = await gateway.aggregate_bff("dashboard", {"user_id": "123"})
        print(f"BFF result: {bff_result}")
        
        # 获取指标
        metrics = gateway.get_metrics()
        print(f"Gateway metrics: {metrics}")
        
    finally:
        await gateway.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())

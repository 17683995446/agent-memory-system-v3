"""
Agent Memory System REST API
基于 FastAPI 的生产级 RESTful 接口

启动方式:
    uvicorn memory_system.api:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from memory_system.configuration_settings.memory_system_configuration import MemoryConfig
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryStats
from memory_system.core_abstractions.component_registry_manager import (
    _consolidation_registry,
    _embedding_registry,
    _forgetting_registry,
    _retrieval_registry,
    _storage_registry,
)
from memory_system.core_abstractions.authentication_middleware import (
    AuthenticationManager,
    set_auth_manager,
    reset_auth_manager,
    verify_authentication,
    hash_password,
)
from memory_system.structured_logging_config import get_logger
from memory_system.agent_memory_system_facade import MemorySystem
from memory_system.websocket_event_notifier import WebSocketEventNotifier, EventType

# 全局 WebSocket 事件通知器
notifier = WebSocketEventNotifier()

logger = get_logger(__name__)

__version__ = "2.1.0"

# ---------------------------------------------------------------------------
# Pydantic v2 请求 / 响应模型（API 专用，不复用内部 MemoryItem）
# ---------------------------------------------------------------------------


class StoreMemoryRequest(BaseModel):
    """存储单条记忆的请求体"""

    content: str = Field(..., min_length=1, description="记忆内容")
    memory_type: Optional[str] = Field(
        default="auto",
        description="记忆类型: short_term / episodic / semantic / auto",
    )
    importance: Optional[str] = Field(
        default="medium",
        description="重要性: critical / high / medium / low / trivial",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="附加元数据"
    )


class StoreMemoryResponse(BaseModel):
    """存储记忆的响应"""

    memory_id: str = Field(..., description="记忆唯一标识")
    status: str = Field(..., description="操作状态")


class BatchStoreRequest(BaseModel):
    """批量存储记忆的请求体"""

    contents: List[str] = Field(
        ..., min_length=1, description="记忆内容列表"
    )
    memory_type: Optional[str] = Field(default="auto", description="记忆类型")
    importance: Optional[str] = Field(default="medium", description="重要性")


class BatchStoreResponse(BaseModel):
    """批量存储记忆的响应"""

    memory_ids: List[str] = Field(..., description="记忆ID列表")
    count: int = Field(..., description="成功存储数量")


class RecallResponse(BaseModel):
    """记忆检索的响应"""

    query: str = Field(..., description="查询内容")
    results: List[Dict[str, Any]] = Field(
        default_factory=list, description="检索结果"
    )
    total: int = Field(default=0, description="总匹配数")
    offset: int = Field(default=0, description="分页偏移")
    limit: int = Field(default=10, description="每页数量")


class MemoryDetailResponse(BaseModel):
    """单条记忆详情的响应"""

    memory_id: str
    content: str
    memory_type: str
    importance: int
    timestamp: float
    access_count: int
    last_access: float
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DeleteMemoryResponse(BaseModel):
    """删除记忆的响应"""

    status: str = Field(..., description="操作状态")


class StatsResponse(BaseModel):
    """系统统计信息响应"""

    short_term_count: int = 0
    episodic_count: int = 0
    semantic_count: int = 0
    total_count: int = 0
    total_added: int = 0
    total_retrieved: int = 0
    uptime_hours: float = 0.0


class HealthResponse(BaseModel):
    """健康检查响应"""

    status: str = "healthy"
    version: str = __version__


class RecentShortTermResponse(BaseModel):
    """近期短期记忆响应"""

    memories: List[Dict[str, Any]] = Field(
        default_factory=list, description="近期短期记忆列表"
    )


class ConsolidateResponse(BaseModel):
    """记忆整合响应"""

    status: str = Field(..., description="操作状态")
    message: str = Field(..., description="操作信息")


class RegistriesResponse(BaseModel):
    """已注册组件列表响应"""

    storages: List[str] = Field(default_factory=list)
    embeddings: List[str] = Field(default_factory=list)
    retrievals: List[str] = Field(default_factory=list)
    consolidations: List[str] = Field(default_factory=list)
    forgettings: List[str] = Field(default_factory=list)


class ExportResponse(BaseModel):
    """导出记忆响应"""

    memories: List[Dict[str, Any]] = Field(..., description="导出的记忆列表")
    count: int = Field(..., description="导出数量")
    exported_at: float = Field(..., description="导出时间戳")


class ImportRequest(BaseModel):
    """导入记忆请求体"""

    memories: List[Dict[str, Any]] = Field(..., min_length=1, description="待导入的记忆列表")


class ImportResponse(BaseModel):
    """导入记忆响应"""

    imported_count: int = Field(..., description="成功导入数量")
    skipped_count: int = Field(..., description="跳过数量")


class TokenResponse(BaseModel):
    """OAuth2 Token响应"""

    access_token: str = Field(..., description="访问令牌")
    token_type: str = Field(default="bearer", description="令牌类型")


class StreamResultChunk(BaseModel):
    """SSE 流式结果块"""

    type: str = Field(..., description="块类型: start/total/result/complete/error")
    index: Optional[int] = None
    memory: Optional[Dict[str, Any]] = None
    score: Optional[float] = None
    layer: Optional[str] = None
    count: Optional[int] = None
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# FastAPI 应用实例 & 中间件
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Agent Memory System API",
    description="Agent 记忆系统 RESTful 接口 -- 提供记忆存储、检索、管理与统计能力",
    version=__version__,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "memories", "description": "记忆的增删查操作"},
        {"name": "short-term", "description": "短期记忆管理"},
        {"name": "system", "description": "系统级操作（统计、健康检查、整合、注册表）"},
        {"name": "authentication", "description": "认证相关操作"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"], enabled=True)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


def disable_rate_limit() -> None:
    """禁用速率限制（用于测试环境）"""
    limiter.enabled = False


def enable_rate_limit() -> None:
    """启用速率限制"""
    limiter.enabled = True

# ---------------------------------------------------------------------------
# 依赖注入：懒初始化 MemorySystem 单例
# ---------------------------------------------------------------------------

_memory_system_instance: Optional[MemorySystem] = None


def get_memory_system() -> MemorySystem:
    """
    FastAPI 依赖：获取 MemorySystem 实例（懒初始化单例）。

    首次调用时从默认配置创建实例，后续调用复用同一实例。
    初始化失败时抛出 HTTPException(503) 而非让异常传播为 500。
    """
    global _memory_system_instance
    if _memory_system_instance is None:
        logger.info("正在初始化 MemorySystem ...")
        try:
            config = MemoryConfig()
            _memory_system_instance = MemorySystem.from_config(config)
            logger.info("MemorySystem 初始化完成")
        except ImportError as e:
            logger.error("MemorySystem 初始化失败: 缺少依赖 - %s", e)
            raise HTTPException(
                status_code=503,
                detail=f"服务不可用: 缺少必要依赖 ({e}). "
                       f"请安装对应依赖或修改配置使用可用的后端"
            )
        except ValueError as e:
            logger.error("MemorySystem 初始化失败: 配置错误 - %s", e)
            raise HTTPException(
                status_code=503,
                detail=f"服务不可用: 配置错误 - {e}"
            )
        except Exception as e:
            logger.error("MemorySystem 初始化失败: %s", e)
            raise HTTPException(
                status_code=503,
                detail=f"服务不可用: {e}"
            )
    return _memory_system_instance


def reset_memory_system(system: Optional[MemorySystem] = None) -> None:
    """
    重置全局单例。主要用于测试时注入自定义实例。

    Args:
        system: 新的 MemorySystem 实例，None 表示清除。
    """
    global _memory_system_instance
    _memory_system_instance = system


def init_authentication(config: MemoryConfig) -> None:
    """
    初始化认证管理器

    Args:
        config: 内存系统配置
    """
    auth_config = config.authentication
    manager = AuthenticationManager(
        enabled=auth_config.enabled,
        api_key=auth_config.api_key,
        oauth2_enabled=auth_config.oauth2_enabled,
        oauth2_users=auth_config.oauth2_users,
    )
    set_auth_manager(manager)
    if auth_config.enabled:
        logger.info(
            "认证已启用",
            api_key_enabled=bool(auth_config.api_key),
            oauth2_enabled=auth_config.oauth2_enabled,
        )
    else:
        logger.info("认证已禁用")


# ---------------------------------------------------------------------------
# 路由：认证
# ---------------------------------------------------------------------------


@app.post(
    "/api/v1/auth/token",
    response_model=TokenResponse,
    tags=["authentication"],
    summary="获取访问令牌",
)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> TokenResponse:
    """OAuth2密码模式登录
    
    使用用户名和密码获取访问令牌。
    令牌在后续请求中通过 Authorization: Bearer <token> 头部发送。
    """
    from memory_system.core_abstractions.authentication_middleware import get_auth_manager
    
    manager = get_auth_manager()
    if manager is None or not manager.oauth2_enabled:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="OAuth2 authentication not enabled",
        )
    
    username = manager.authenticate_user(form_data.username, form_data.password)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 简化实现：token即username
    # 实际生产环境应使用JWT
    return TokenResponse(access_token=username, token_type="bearer")


# ---------------------------------------------------------------------------
# 路由：记忆 CRUD
# ---------------------------------------------------------------------------


@app.post(
    "/api/v1/memories",
    response_model=StoreMemoryResponse,
    tags=["memories"],
    summary="存储一条记忆",
    status_code=201,
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("30/minute")
async def store_memory(
    request: Request,
    body: StoreMemoryRequest,
    ms: MemorySystem = Depends(get_memory_system),
) -> StoreMemoryResponse:
    """将一条新记忆存入系统。"""
    try:
        memory_id = await ms.remember(
            content=body.content,
            memory_type=body.memory_type or "auto",
            importance=body.importance or "medium",
            metadata=body.metadata,
        )
        return StoreMemoryResponse(memory_id=memory_id, status="stored")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.error("存储记忆失败", error=str(exc))
        raise HTTPException(status_code=500, detail="存储记忆时发生内部错误")


@app.post(
    "/api/v1/memories/batch",
    response_model=BatchStoreResponse,
    tags=["memories"],
    summary="批量存储记忆",
    status_code=201,
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("30/minute")
async def batch_store_memories(
    request: Request,
    body: BatchStoreRequest,
    ms: MemorySystem = Depends(get_memory_system),
) -> BatchStoreResponse:
    """批量将多条记忆存入系统。"""
    try:
        memory_ids = await ms.remember_batch(
            contents=body.contents,
            memory_type=body.memory_type or "auto",
            importance=body.importance or "medium",
        )
        return BatchStoreResponse(memory_ids=memory_ids, count=len(memory_ids))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.error("批量存储记忆失败", error=str(exc))
        raise HTTPException(status_code=500, detail="批量存储记忆时发生内部错误")


@app.get(
    "/api/v1/memories/recall",
    response_model=RecallResponse,
    tags=["memories"],
    summary="检索记忆",
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("60/minute")
async def recall_memories(
    request: Request,
    query: str = Query(..., min_length=1, description="查询内容"),
    max_results: int = Query(
        default=100, ge=1, le=500, description="最大检索结果数"
    ),
    offset: int = Query(default=0, ge=0, description="分页偏移"),
    limit: int = Query(default=10, ge=1, le=100, description="每页数量"),
    memory_type: Optional[str] = Query(
        default=None, description="过滤记忆类型: episodic/semantic"
    ),
    ms: MemorySystem = Depends(get_memory_system),
) -> RecallResponse:
    """根据查询内容检索相关记忆，支持分页和类型过滤。"""
    try:
        result = await ms.recall(query=query, max_results=max_results)
        combined = result.get("combined", [])

        # 按记忆类型过滤
        if memory_type is not None:
            combined = [
                m for m in combined
                if m.get("memory", {}).get("memory_type") == memory_type
            ]

        total = len(combined)
        page = combined[offset : offset + limit]

        return RecallResponse(
            query=query,
            results=page,
            total=total,
            offset=offset,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.error("检索记忆失败", error=str(exc))
        raise HTTPException(status_code=500, detail="检索记忆时发生内部错误")


@app.get(
    "/api/v1/memories/recall/stream",
    tags=["memories"],
    summary="流式检索记忆",
    response_class=StreamingResponse,
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("60/minute")
async def recall_memories_stream(
    request: Request,
    query: str = Query(..., min_length=1, description="查询内容"),
    max_results: int = Query(default=100, ge=1, le=500, description="最大结果数"),
    memory_type: Optional[str] = Query(default=None, description="过滤记忆类型"),
    ms: MemorySystem = Depends(get_memory_system),
) -> StreamingResponse:
    """
    流式检索记忆，使用 Server-Sent Events 格式返回结果

    每找到一个结果就立即返回，适合大结果集场景
    """

    async def generate_results() -> AsyncGenerator[str, None]:
        try:
            # 发送开始标记
            yield f"data: {json.dumps({'type': 'start', 'query': query})}\n\n"

            # 执行检索
            result = await ms.recall(query=query, max_results=max_results)
            combined = result.get("combined", [])

            # 按类型过滤
            if memory_type:
                combined = [
                    r
                    for r in combined
                    if r.get("memory", {}).get("memory_type") == memory_type
                ]

            total = len(combined)
            yield f"data: {json.dumps({'type': 'total', 'count': total})}\n\n"

            # 流式返回每个结果
            for i, item in enumerate(combined):
                chunk = {
                    "type": "result",
                    "index": i,
                    "memory": item.get("memory"),
                    "score": item.get("score"),
                    "layer": item.get("layer"),
                }
                yield f"data: {json.dumps(chunk)}\n\n"
                await asyncio.sleep(0.01)  # 小延迟，避免阻塞

            # 发送完成标记
            yield f"data: {json.dumps({'type': 'complete', 'total': total})}\n\n"

        except Exception as exc:
            logger.error("流式检索失败", error=str(exc))
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

    return StreamingResponse(
        generate_results(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@app.post(
    "/api/v1/memories/batch/stream",
    tags=["memories"],
    summary="批量流式存储记忆",
    response_class=StreamingResponse,
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("30/minute")
async def batch_store_memories_stream(
    request: Request,
    body: BatchStoreRequest,
    ms: MemorySystem = Depends(get_memory_system),
) -> StreamingResponse:
    """
    批量流式存储记忆，每存储一条就返回进度
    """

    async def generate_progress() -> AsyncGenerator[str, None]:
        yield f"data: {json.dumps({'type': 'start', 'total': len(body.contents)})}\n\n"

        for i, content in enumerate(body.contents):
            try:
                memory_id = await ms.remember(
                    content=content,
                    memory_type=body.memory_type or "auto",
                    importance=body.importance or "medium",
                )
                yield f"data: {json.dumps({'type': 'progress', 'index': i, 'memory_id': memory_id})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'index': i, 'message': str(e)})}\n\n"

            await asyncio.sleep(0.01)

        yield f"data: {json.dumps({'type': 'complete'})}\n\n"

    return StreamingResponse(
        generate_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


# ---------------------------------------------------------------------------
# 路由：导出 / 导入（必须在 /{memory_id} 之前定义，避免路径参数冲突）
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/memories/export",
    response_model=ExportResponse,
    tags=["memories"],
    summary="导出所有记忆",
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("5/minute")
async def export_memories(
    request: Request,
    memory_type: Optional[str] = Query(default=None, description="按类型过滤"),
    ms: MemorySystem = Depends(get_memory_system),
) -> ExportResponse:
    """导出系统中的所有记忆，可选按类型过滤。"""
    try:
        all_memories = await ms.storage.search_by_time_range(
            0, time.time(), memory_type
        )
        memories_data = [m.to_storage_dict() for m in all_memories]
        return ExportResponse(
            memories=memories_data,
            count=len(memories_data),
            exported_at=time.time(),
        )
    except Exception as exc:
        logger.error("导出记忆失败", error=str(exc))
        raise HTTPException(status_code=500, detail="导出记忆时发生内部错误")


@app.post(
    "/api/v1/memories/import",
    response_model=ImportResponse,
    tags=["memories"],
    summary="导入记忆",
    status_code=201,
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("5/minute")
async def import_memories(
    request: Request,
    body: ImportRequest,
    ms: MemorySystem = Depends(get_memory_system),
) -> ImportResponse:
    """批量导入记忆到系统中。"""
    imported_count = 0
    skipped_count = 0
    for mem_dict in body.memories:
        try:
            item = MemoryItem.from_storage_dict(mem_dict)
            await ms.storage.add(item)
            imported_count += 1
        except Exception as e:
            logger.warning("导入记忆失败，跳过: %s", e)
            skipped_count += 1
    return ImportResponse(
        imported_count=imported_count,
        skipped_count=skipped_count,
    )


@app.get(
    "/api/v1/memories/{memory_id}",
    response_model=MemoryDetailResponse,
    tags=["memories"],
    summary="获取指定记忆",
    dependencies=[Depends(verify_authentication)],
)
async def get_memory(
    memory_id: str,
    ms: MemorySystem = Depends(get_memory_system),
) -> MemoryDetailResponse:
    """根据 ID 获取单条记忆的详细信息。"""
    memory = await ms.storage.get(memory_id)
    if memory is None:
        raise HTTPException(status_code=404, detail=f"记忆不存在: {memory_id}")
    return MemoryDetailResponse(
        memory_id=memory.memory_id,
        content=memory.content,
        memory_type=memory.memory_type.value,
        importance=memory.importance.value,
        timestamp=memory.timestamp,
        access_count=memory.access_count,
        last_access=memory.last_access,
        metadata=memory.metadata,
    )


@app.delete(
    "/api/v1/memories/{memory_id}",
    response_model=DeleteMemoryResponse,
    tags=["memories"],
    summary="删除指定记忆",
    dependencies=[Depends(verify_authentication)],
)
async def delete_memory(
    memory_id: str,
    ms: MemorySystem = Depends(get_memory_system),
) -> DeleteMemoryResponse:
    """根据 ID 删除一条记忆。"""
    deleted = await ms.storage.delete(memory_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"记忆不存在: {memory_id}")
    return DeleteMemoryResponse(status="deleted")


# ---------------------------------------------------------------------------
# 路由：短期记忆
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/short-term/recent",
    response_model=RecentShortTermResponse,
    tags=["short-term"],
    summary="获取近期短期记忆",
    dependencies=[Depends(verify_authentication)],
)
async def get_recent_short_term(
    n: int = Query(default=5, ge=1, le=50, description="返回条数"),
    ms: MemorySystem = Depends(get_memory_system),
) -> RecentShortTermResponse:
    """获取最近 N 条短期记忆。"""
    recent = await ms.short_term.get_recent(n)
    return RecentShortTermResponse(
        memories=[m.model_dump() for m in recent]
    )


@app.delete(
    "/api/v1/short-term",
    response_model=DeleteMemoryResponse,
    tags=["short-term"],
    summary="清空短期记忆",
    dependencies=[Depends(verify_authentication)],
)
async def clear_short_term(
    ms: MemorySystem = Depends(get_memory_system),
) -> DeleteMemoryResponse:
    """清空所有短期记忆。"""
    await ms.clear_short_term()
    return DeleteMemoryResponse(status="cleared")


# ---------------------------------------------------------------------------
# 路由：系统管理
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/stats",
    response_model=StatsResponse,
    tags=["system"],
    summary="获取系统统计信息",
    dependencies=[Depends(verify_authentication)],
)
async def get_stats(
    ms: MemorySystem = Depends(get_memory_system),
) -> StatsResponse:
    """返回记忆系统的统计信息。"""
    stats: MemoryStats = await ms.get_stats()
    return StatsResponse(
        short_term_count=stats.short_term_count,
        episodic_count=stats.episodic_count,
        semantic_count=stats.semantic_count,
        total_count=stats.total_count,
        total_added=stats.total_added,
        total_retrieved=stats.total_retrieved,
        uptime_hours=stats.uptime_hours,
    )


@app.get(
    "/api/v1/health",
    response_model=HealthResponse,
    tags=["system"],
    summary="健康检查",
)
async def health_check() -> HealthResponse:
    """返回服务健康状态和版本信息。检查 MemorySystem 是否可用。"""
    status = "healthy"
    if _memory_system_instance is None:
        try:
            get_memory_system()
        except Exception as e:
            logger.warning("健康检查: MemorySystem初始化失败: %s", e)
            status = "degraded"
    return HealthResponse(status=status, version=__version__)


@app.post(
    "/api/v1/consolidate",
    response_model=ConsolidateResponse,
    tags=["system"],
    summary="手动触发记忆整合",
    dependencies=[Depends(verify_authentication)],
)
@limiter.limit("10/minute")
async def trigger_consolidation(
    request: Request,
    ms: MemorySystem = Depends(get_memory_system),
) -> ConsolidateResponse:
    """手动触发一次记忆整合流程。"""
    try:
        await ms.consolidate()
        return ConsolidateResponse(
            status="ok", message="记忆整合已完成"
        )
    except Exception as exc:
        logger.error("记忆整合失败", error=str(exc))
        raise HTTPException(
            status_code=500, detail="记忆整合时发生内部错误"
        )


@app.get(
    "/api/v1/registries",
    response_model=RegistriesResponse,
    tags=["system"],
    summary="列出所有已注册组件",
)
async def list_registries() -> RegistriesResponse:
    """返回所有已注册的存储后端、嵌入提供者、检索策略等组件名称。"""
    return RegistriesResponse(
        storages=list(_storage_registry.keys()),
        embeddings=list(_embedding_registry.keys()),
        retrievals=list(_retrieval_registry.keys()),
        consolidations=list(_consolidation_registry.keys()),
        forgettings=list(_forgetting_registry.keys()),
    )


# ---------------------------------------------------------------------------
# 路由: WebSocket 事件订阅
# ---------------------------------------------------------------------------


@app.websocket("/api/v1/ws/events")
async def websocket_events(websocket: WebSocket):
    """WebSocket 事件订阅
    
    实时接收记忆系统的事件通知，包括：
    - memory_added: 新记忆添加
    - memory_recalled: 记忆被检索
    - memory_deleted: 记忆被删除
    - memory_consolidated: 记忆整合完成
    - short_term_cleared: 短期记忆清空
    """
    await notifier.connect(websocket)
    try:
        while True:
            # 接收客户端消息（可用于心跳或订阅变更）
            data = await websocket.receive_text()
            # 可解析 data 更新订阅
    except WebSocketDisconnect:
        await notifier.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

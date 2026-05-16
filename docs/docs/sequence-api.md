# API 生命周期时序图

本文档展示 REST API 请求的完整生命周期。

## POST /api/v1/memories (存储记忆)

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant FastAPI as FastAPI
    participant CORS as CORS中间件
    participant RateLimiter as 速率限制
    participant Auth as 认证中间件
    participant Dep as get_memory_system
    participant MS as MemorySystem
    participant Storage as StorageBackend
    participant Metrics as Prometheus

    Client->>FastAPI: POST /api/v1/memories
    FastAPI->>CORS: 检查 Origin
    CORS->>RateLimiter: 检查速率限制

    alt 速率超限
        RateLimiter-->>Client: 429 Too Many Requests
    else 速率正常
        RateLimiter->>Auth: 验证认证

        alt 认证失败
            Auth-->>Client: 401 Unauthorized
        else 认证成功
            Auth->>Dep: 依赖注入

            alt 首次调用
                Dep->>Dep: MemoryConfig()
                Dep->>MS: from_config(config)
                MS-->>Dep: MemorySystem 实例
            end

            Dep->>MS: remember(content, type, importance)
            MS->>MS: 参数验证
            MS->>MS: 创建 OTel Span

            alt 短期记忆
                MS->>Storage: short_term.add()
                Storage-->>MS: memory_id
            else 长期记忆
                MS->>MS: embedding.embed(content)
                MS->>MS: 创建 MemoryItem
                MS->>Storage: storage.add(item)
                Storage-->>MS: memory_id
            end

            MS->>Metrics: record_remember()
            MS-->>Dep: memory_id
            Dep-->>FastAPI: memory_id
            FastAPI-->>Client: 201 Created {memory_id}
        end
    end
```

---

## GET /api/v1/memories/recall (检索记忆)

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant FastAPI as FastAPI
    participant Auth as 认证中间件
    participant MS as MemorySystem
    participant Retrieval as HierarchicalRetrieval
    participant Storage as StorageBackend
    participant Embed as EmbeddingProvider

    Client->>FastAPI: GET /api/v1/memories/recall?query=...
    FastAPI->>Auth: 验证认证
    Auth->>MS: recall(query, max_results)

    MS->>MS: 参数验证
    MS->>MS: 创建 OTel Span
    MS->>Storage: short_term.get_all()
    Storage-->>MS: 短期记忆列表

    MS->>Retrieval: retrieve(query, storage, embedding, context)

    Note over Retrieval: L1: 短期记忆检索
    Retrieval->>Retrieval: _search_short_term()

    Note over Retrieval: L2: 时间范围检索
    Retrieval->>Storage: search_by_time_range(0, now)
    Storage-->>Retrieval: 时间匹配记忆

    Note over Retrieval: L3: 向量检索
    Retrieval->>Embed: embed(query)
    Embed-->>Retrieval: query_vector
    Retrieval->>Storage: search_by_vector(query_vector)
    Storage-->>Retrieval: 相似记忆

    Retrieval->>Retrieval: _merge_and_rank()
    Retrieval-->>MS: 检索结果

    MS->>MS: 记录指标
    MS-->>FastAPI: RetrievalResponse
    FastAPI-->>Client: 200 OK {results, total, query_time_ms}
```

---

## 错误处理时序

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant FastAPI as FastAPI
    participant Handler as 端点处理
    participant Logger as 日志

    Client->>FastAPI: 请求

    alt 参数验证失败
        FastAPI-->>Client: 422 Unprocessable Entity
    else 认证失败
        FastAPI-->>Client: 401 Unauthorized
    else 速率超限
        FastAPI-->>Client: 429 Too Many Requests
    else 服务不可用 (依赖缺失)
        FastAPI->>Logger: 记录错误
        FastAPI-->>Client: 503 Service Unavailable
    else 内部错误
        Handler->>Logger: 记录异常堆栈
        FastAPI-->>Client: 500 Internal Server Error
    end
```

---

## WebSocket 事件推送

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant WS as WebSocket
    participant Notifier as EventNotifier
    participant MS as MemorySystem

    Client->>WS: CONNECT /api/v1/ws/events
    WS->>Notifier: 注册连接

    loop 事件循环
        MS->>MS: 记忆操作 (add/delete/etc)
        MS->>Notifier: broadcast(event)

        alt 有活跃连接
            Notifier->>WS: 推送事件
            WS-->>Client: {type: "memory_added", data: {...}}
        else 无连接
            Notifier->>Notifier: 跳过推送
        end
    end

    Client->>WS: DISCONNECT
    WS->>Notifier: 移除连接
```

### 事件类型

| 事件类型 | 触发时机 | 数据 |
|----------|----------|------|
| `memory_added` | 记忆存储成功 | `{memory_id, memory_type}` |
| `memory_recalled` | 记忆检索成功 | `{query, result_count}` |
| `memory_deleted` | 记忆删除成功 | `{memory_id}` |
| `memory_consolidated` | 整合完成 | `{new_id, original_ids}` |
| `memory_expired` | TTL 过期 | `{memory_id}` |

---

## SSE 流式响应

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant FastAPI as FastAPI
    participant MS as MemorySystem

    Client->>FastAPI: GET /api/v1/memories/recall/stream?query=...
    FastAPI-->>Client: 200 OK (text/event-stream)

    FastAPI->>MS: recall(query)

    loop 每条结果
        MS-->>FastAPI: MemoryItem
        FastAPI-->>Client: data: {"memory": {...}, "score": 0.85}
    end

    FastAPI-->>Client: data: [DONE]
```

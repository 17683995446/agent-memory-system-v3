# API 参考

本文档列出所有 REST API 端点及其参数。

## 认证端点

### POST /api/v1/auth/token

获取 OAuth2 访问令牌。

**请求体**:
```json
{
  "username": "string",
  "password": "string"
}
```

**响应**:
```json
{
  "access_token": "string",
  "token_type": "bearer"
}
```

---

## 记忆管理端点

### POST /api/v1/memories

存储单条记忆。

**请求头**:
- `X-API-Key`: API 密钥 (可选，如果启用)
- `Authorization`: Bearer token (可选，如果启用 OAuth2)

**请求体**:
```json
{
  "content": "string (必填)",
  "memory_type": "auto | short_term | episodic | semantic (默认: auto)",
  "importance": "critical | high | medium | low | trivial (默认: medium)",
  "metadata": {}
}
```

**响应** `201 Created`:
```json
{
  "memory_id": "string",
  "status": "stored"
}
```

---

### POST /api/v1/memories/batch

批量存储记忆。

**请求体**:
```json
{
  "contents": ["string", "string", ...],
  "memory_type": "semantic",
  "importance": "medium"
}
```

**响应** `201 Created`:
```json
{
  "memory_ids": ["id1", "id2", ...],
  "count": 2
}
```

---

### GET /api/v1/memories/recall

检索记忆。

**查询参数**:
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `query` | string | 必填 | 检索查询 |
| `max_results` | int | 10 | 最大返回数量 |
| `offset` | int | 0 | 分页偏移 |
| `limit` | int | 10 | 分页限制 |
| `memory_type` | string | - | 过滤记忆类型 |

**响应** `200 OK`:
```json
{
  "results": [
    {
      "memory": { "memory_id": "...", "content": "...", ... },
      "score": 0.85,
      "layer": "vector"
    }
  ],
  "total": 5,
  "query_time_ms": 1.23
}
```

---

### GET /api/v1/memories/recall/stream

流式检索记忆 (SSE)。

**响应** `text/event-stream`:
```
data: {"memory": {...}, "score": 0.85}
data: {"memory": {...}, "score": 0.72}
data: [DONE]
```

---

### GET /api/v1/memories/{memory_id}

获取单条记忆。

**响应** `200 OK`:
```json
{
  "memory_id": "string",
  "content": "string",
  "memory_type": "semantic",
  "importance": "high",
  "timestamp": 1234567890.0,
  "access_count": 5,
  "metadata": {}
}
```

---

### DELETE /api/v1/memories/{memory_id}

删除单条记忆。

**响应** `204 No Content`

---

## 导出导入端点

### GET /api/v1/memories/export

导出所有记忆。

**响应** `200 OK`:
```json
{
  "memories": [
    { "memory_id": "...", "content": "...", ... }
  ],
  "exported_at": "2024-01-15T10:00:00Z",
  "count": 100
}
```

---

### POST /api/v1/memories/import

导入记忆。

**请求体**:
```json
{
  "memories": [
    { "memory_id": "...", "content": "...", ... }
  ]
}
```

**响应** `200 OK`:
```json
{
  "imported_count": 95,
  "skipped_count": 5
}
```

---

## 短期记忆端点

### GET /api/v1/short-term/recent

获取近期短期记忆。

**查询参数**:
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `n` | int | 5 | 返回数量 |

---

### DELETE /api/v1/short-term

清空短期记忆。

**响应** `204 No Content`

---

## 系统端点

### GET /api/v1/stats

获取系统统计。

**响应** `200 OK`:
```json
{
  "total_count": 1000,
  "episodic_count": 300,
  "semantic_count": 700,
  "short_term_count": 5
}
```

---

### GET /api/v1/health

健康检查。

**响应** `200 OK`:
```json
{
  "status": "healthy",
  "version": "2.24.0"
}
```

---

### POST /api/v1/consolidate

手动触发整合。

**响应** `200 OK`:
```json
{
  "status": "completed",
  "consolidated_count": 3
}
```

---

### GET /api/v1/registries

列出已注册组件。

**响应** `200 OK`:
```json
{
  "storage": ["memory", "chroma", "qdrant", "faiss"],
  "embedding": ["sentence_transformers", "openai", "bge"],
  "retrieval": ["hierarchical", "vector_only"],
  "consolidation": ["time_based", "similarity_based"],
  "forgetting": ["score_based"]
}
```

---

## WebSocket 端点

### WS /api/v1/ws/events

订阅实时事件。

**消息格式**:
```json
{
  "type": "memory_added",
  "data": {
    "memory_id": "...",
    "memory_type": "semantic"
  },
  "timestamp": "2024-01-15T10:00:00Z"
}
```

---

## 错误响应

所有错误响应格式：

```json
{
  "detail": "错误描述信息"
}
```

| 状态码 | 说明 |
|--------|------|
| 400 | 请求格式错误 |
| 401 | 认证失败 |
| 403 | 权限不足 |
| 404 | 资源不存在 |
| 422 | 参数验证失败 |
| 429 | 速率限制 |
| 500 | 内部错误 |
| 503 | 服务不可用 (依赖缺失) |

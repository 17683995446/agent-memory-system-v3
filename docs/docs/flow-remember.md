# 记忆存储流程

本文档详细描述 `remember()` 方法的完整执行流程。

## 流程概览

```mermaid
flowchart TD
    START([remember 调用]) --> VALIDATE{参数验证}
    VALIDATE -->|失败| ERR1[抛出 ValueError]
    VALIDATE -->|通过| AUTO{memory_type == auto?}

    AUTO -->|是| AUTO_CHECK{importance >= HIGH?}
    AUTO_CHECK -->|是| SET_SEMANTIC[memory_type = semantic]
    AUTO_CHECK -->|否| SET_SHORT[memory_type = short_term]

    AUTO -->|否| TYPE_CHECK{memory_type?}

    SET_SEMANTIC --> TYPE_CHECK
    SET_SHORT --> TYPE_CHECK

    TYPE_CHECK -->|short_term| STM_PATH[短期记忆路径]
    TYPE_CHECK -->|episodic/semantic| LTM_PATH[长期记忆路径]

    subgraph STM_PATH["短期记忆路径"]
        STM_PATH_START[ ] --> STM_ADD[short_term.add]
        STM_ADD --> STM_RETURN[返回 memory_id]
    end

    subgraph LTM_PATH["长期记忆路径"]
        LTM_START[ ] --> SPAN[创建 OTel Span]
        SPAN --> CACHE_CHECK{嵌入缓存命中?}
        CACHE_CHECK -->|命中| CACHE_HIT[使用缓存向量]
        CACHE_CHECK -->|未命中| EMBED[embedding.embed]
        EMBED --> CACHE_SET[写入缓存]
        CACHE_SET --> CREATE_ITEM[创建 MemoryItem]
        CACHE_HIT --> CREATE_ITEM
        CREATE_ITEM --> STORAGE_ADD[storage.add]
        STORAGE_ADD --> METRICS[record metrics]
        METRICS --> CHECK_CONSOLIDATE{_check_consolidation}
        CHECK_CONSOLIDATE -->|触发| CONSOLIDATE[执行整合]
        CHECK_CONSOLIDATE -->|跳过| CHECK_FORGET
        CONSOLIDATE --> CHECK_FORGET{_check_forgetting}
        CHECK_FORGET -->|触发| FORGET[执行遗忘]
        CHECK_FORGET -->|跳过| RETURN
        FORGET --> RETURN[返回 memory_id]
    end

    STM_RETURN --> END([结束])
    RETURN --> END
```

---

## 参数验证

```python
# 必填参数校验
if not content or not content.strip():
    raise ValueError("记忆内容不能为空")

# 枚举值校验
if memory_type not in ["auto", "short_term", "episodic", "semantic"]:
    raise ValueError(f"无效的 memory_type: {memory_type}")

if importance not in ["critical", "high", "medium", "low", "trivial"]:
    raise ValueError(f"无效的 importance: {importance}")
```

---

## 自动类型选择

```mermaid
flowchart LR
    A[memory_type = auto] --> B{importance >= HIGH?}
    B -->|是| C[semantic<br/>长期语义记忆]
    B -->|否| D[short_term<br/>短期工作记忆]

    style C fill:#e8f5e9
    style D fill:#fff3e0
```

| importance | 自动选择的类型 | 说明 |
|-----------|---------------|------|
| CRITICAL, HIGH | `semantic` | 重要信息存入长期记忆 |
| MEDIUM, LOW, TRIVIAL | `short_term` | 临时信息存入短期记忆 |

---

## 嵌入缓存机制

```mermaid
sequenceDiagram
    participant R as remember()
    participant Cache as TTLCache
    participant Emb as EmbeddingProvider

    R->>Cache: 检查 content hash
    alt 缓存命中
        Cache-->>R: 返回缓存向量
    else 缓存未命中
        R->>Emb: embed(content)
        Emb-->>R: 返回向量
        R->>Cache: 写入缓存 (TTL=1h)
    end
    R->>R: 继续存储流程
```

### 缓存配置

| 参数 | 值 | 说明 |
|------|-----|------|
| `maxsize` | 512 | 最多缓存 512 条嵌入 |
| `ttl` | 3600s (1小时) | 缓存有效期 |

---

## 后置检查

### 整合检查

```mermaid
flowchart TD
    A[_check_consolidation] --> B[获取记忆总数]
    B --> C{总数 >= threshold?}
    C -->|是| D[执行整合策略]
    C -->|否| E[跳过]
    D --> F[生成语义记忆摘要]
    F --> G[删除原始情景记忆]
```

### 遗忘检查

```mermaid
flowchart TD
    A[_check_forgetting] --> B[获取记忆总数]
    B --> C{总数 >= max_memories?}
    C -->|是| D[执行遗忘策略]
    C -->|否| E[跳过]
    D --> F[计算保留分数]
    F --> G[删除低分记忆]
```

---

## 批量存储优化

`remember_batch()` 针对批量场景优化：

```mermaid
flowchart TD
    A[remember_batch contents] --> B[分离短期/长期内容]
    B --> C[批量存储短期记忆]
    B --> D[批量嵌入长期内容]
    D --> E[embed_batch 一次调用]
    E --> F[批量创建 MemoryItem]
    F --> G[批量写入 storage]
```

| 优化点 | 说明 |
|--------|------|
| 批量嵌入 | 一次 `embed_batch()` 调用替代多次 `embed()` |
| 减少网络往返 | OpenAI 等远程 API 批量请求更高效 |
| 嵌入缓存复用 | 已缓存内容跳过嵌入计算 |

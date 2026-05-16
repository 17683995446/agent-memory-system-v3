# 系统架构

本文档详细描述 Agent Memory System 的整体架构设计。

## 架构总览

```mermaid
graph TB
    subgraph Client["客户端"]
        WEB[Web App]
        SDK[SDK/CLI]
        AGENT[AI Agent]
    end

    subgraph API["API 层"]
        direction TB
        FASTAPI[FastAPI REST<br/>17 endpoints]
        GRPC[gRPC Service<br/>Protobuf]
        WS[WebSocket<br/>实时事件]
    end

    subgraph Core["核心层"]
        FACADE[MemorySystem<br/>Facade Pattern]
        BUILDER[MemorySystemBuilder<br/>Builder Pattern]
    end

    subgraph Strategy["策略层"]
        RETRIEVAL[RetrievalStrategy<br/>检索策略]
        CONSOLIDATE[ConsolidationStrategy<br/>整合策略]
        FORGET[ForgettingStrategy<br/>遗忘策略]
    end

    subgraph Storage["存储层"]
        STM[ShortTermMemory<br/>L1 缓存]
        STORAGE[StorageBackend<br/>长期存储]
    end

    subgraph Embedding["嵌入层"]
        EMB[EmbeddingProvider<br/>向量嵌入]
        CACHE[Embedding Cache<br/>TTLCache 512/1h]
    end

    subgraph Infra["基础设施层"]
        AUTH[Authentication<br/>API Key + OAuth2]
        TENANT[Tenant Isolation<br/>命名空间隔离]
        TTL[TTL Manager<br/>分级过期]
        OBS[Observability<br/>Prometheus + OTel]
    end

    Client --> API
    API --> Core
    Core --> Strategy
    Core --> Storage
    Core --> Embedding
    Core --> Infra
    Strategy --> Storage
    Embedding --> CACHE
```

---

## 核心抽象层

### 接口定义

```mermaid
classDiagram
    class StorageBackend {
        <<abstract>>
        +add(item: MemoryItem) str
        +get(memory_id: str) MemoryItem
        +search_by_vector(vector, top_k) List
        +search_by_time_range(start, end) List
        +delete(memory_id) bool
        +count() int
        +close()
    }

    class EmbeddingProvider {
        <<abstract>>
        +dimension: int
        +embed(text: str) List~float~
        +embed_batch(texts) List~List~
        +similarity(v1, v2) float
    }

    class RetrievalStrategy {
        <<abstract>>
        +retrieve(query, storage, embedding, max_results, context) Dict
    }

    class ConsolidationStrategy {
        <<abstract>>
        +should_consolidate(memories) bool
        +consolidate(storage, embedding) List
    }

    class ForgettingStrategy {
        <<abstract>>
        +select_for_forgetting(storage, max_keep) List
    }

    class ShortTermMemoryBackend {
        <<abstract>>
        +add(content, importance, metadata) MemoryItem
        +get_recent(n) List
        +get_all() List
        +clear()
        +is_full() bool
    }
```

### 数据模型

```mermaid
classDiagram
    class MemoryItem {
        +memory_id: str
        +content: str
        +memory_type: MemoryType
        +importance: ImportanceLevel
        +embedding: List~float~
        +timestamp: float
        +access_count: int
        +last_access: float
        +metadata: Dict
        +calculate_relevance_score(current_time) float
        +to_storage_dict() Dict
        +from_storage_dict(data) MemoryItem
    }

    class MemoryType {
        <<enumeration>>
        EPISODIC
        SEMANTIC
    }

    class ImportanceLevel {
        <<enumeration>>
        CRITICAL: 5
        HIGH: 4
        MEDIUM: 3
        LOW: 2
        TRIVIAL: 1
    }

    class MemoryStats {
        +total_count: int
        +episodic_count: int
        +semantic_count: int
        +short_term_count: int
    }

    MemoryItem --> MemoryType
    MemoryItem --> ImportanceLevel
```

---

## 注册表模式

系统使用**装饰器 + 自动发现**实现组件注册，遵循开闭原则：

```mermaid
sequenceDiagram
    participant Main as 主程序
    participant Registry as 注册表
    participant Module as 子模块
    participant Decorator as @register_xxx

    Main->>Main: MemorySystem.from_config()
    Main->>Registry: _ensure_registrations()
    Registry->>Module: pkgutil.iter_modules()
    loop 每个子模块
        Registry->>Module: importlib.import_module()
        Module->>Decorator: 执行装饰器
        Decorator->>Registry: _registry[name] = cls
    end
    Registry-->>Main: 注册完成
    Main->>Registry: _registry.get(name)
    Registry-->>Main: 返回类对象
```

### 注册器列表

| 装饰器 | 注册表 | 用途 |
|--------|--------|------|
| `@register_storage("name")` | `_storage_registry` | 存储后端 |
| `@register_embedding("name")` | `_embedding_registry` | 嵌入模型 |
| `@register_retrieval("name")` | `_retrieval_registry` | 检索策略 |
| `@register_consolidation("name")` | `_consolidation_registry` | 整合策略 |
| `@register_forgetting("name")` | `_forgetting_registry` | 遗忘策略 |

---

## Builder 模式

解决多参数构造问题，支持链式调用：

```mermaid
graph LR
    A[MemorySystemBuilder] --> B[with_storage]
    B --> C[with_embedding]
    C --> D[with_retrieval]
    D --> E[with_short_term]
    E --> F[with_consolidation]
    F --> G[with_forgetting]
    G --> H[with_telemetry]
    H --> I[build]
    I --> J[MemorySystem]
```

```python
# 使用示例
system = (MemorySystemBuilder()
    .with_storage(MemoryStorage(max_memories=10000))
    .with_embedding(SentenceTransformerEmbedding(model="BAAI/bge-small-zh-v1.5"))
    .with_retrieval(HierarchicalRetrieval(strategy="adaptive"))
    .with_short_term(WorkingMemoryCache(capacity=7))
    .build())
```

---

## 条件注册机制

当依赖不可用时，组件不会被注册，避免运行时崩溃：

```mermaid
flowchart TD
    A[模块导入] --> B{依赖可用?}
    B -->|是| C[执行 @register 装饰器]
    B -->|否| D[跳过注册]
    C --> E[类加入注册表]
    D --> F[注册表中无该 key]

    G[from_config] --> H{_registry.get name}
    H -->|找到| I[创建实例]
    H -->|未找到| J[抛出 ValueError<br/>提示可用选项]
```

---

## 分层检索架构

```mermaid
graph TB
    subgraph L1["L1: 短期记忆 O(1)"]
        ST[WorkingMemoryCache<br/>TTLCache capacity=7]
    end

    subgraph L2["L2: 时间范围 O(log n)"]
        TIME[search_by_time_range<br/>时间索引]
    end

    subgraph L3["L3: 向量检索 O(log n)"]
        VEC[search_by_vector<br/>ANN 索引]
    end

    subgraph L4["L4: 重要性 O(n log n)"]
        IMP[全量排序<br/>importance 降序]
    end

    Q[Query] --> ST
    ST --> TIME
    TIME --> VEC
    VEC --> IMP
    IMP --> MERGE[_merge_and_rank]
    MERGE --> R[Top-K Results]

    style L1 fill:#e1f5fe
    style L2 fill:#fff3e0
    style L3 fill:#e8f5e9
    style L4 fill:#fce4ec
```

### 检索策略对比

| 策略 | 层级 | 场景 | 复杂度 |
|------|------|------|--------|
| `fast` | L1 | 实时响应，仅短期记忆 | O(1) |
| `adaptive` | L1 + L2 + L3 | 默认策略，平衡速度与召回 | O(log n) |
| `thorough` | L1 + L2 + L3 + L4 | 高召回，包含重要性排序 | O(n log n) |

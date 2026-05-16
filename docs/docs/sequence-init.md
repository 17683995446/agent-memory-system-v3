# 系统初始化时序图

本文档展示系统从配置加载到完全就绪的完整流程。

## MemorySystem.from_config() 初始化

```mermaid
sequenceDiagram
    participant User as 用户代码
    participant Facade as MemorySystem
    participant Builder as MemorySystemBuilder
    participant Registry as 注册表
    participant PkgUtil as pkgutil
    participant Config as MemoryConfig

    User->>Facade: MemorySystem.from_config(config)
    Facade->>Facade: _ensure_registrations()

    alt 首次调用
        Facade->>PkgUtil: iter_modules(storage_backends)
        loop 每个模块
            PkgUtil->>Registry: import_module()
            Note over Registry: @register_storage 装饰器执行
        end

        Facade->>PkgUtil: iter_modules(embedding_providers)
        loop 每个模块
            PkgUtil->>Registry: import_module()
            Note over Registry: @register_embedding 装饰器执行
        end

        Facade->>PkgUtil: iter_modules(retrieval_strategies)
        Facade->>PkgUtil: iter_modules(consolidation_strategies)
        Facade->>PkgUtil: iter_modules(forgetting_strategies)
    end

    Facade->>Builder: from_config(config)
    Builder->>Config: config.embedding
    Builder->>Registry: _embedding_registry.get(provider)

    alt 未找到
        Registry-->>Builder: None
        Builder-->>User: ValueError("未知的嵌入提供者")
    else 找到
        Registry-->>Builder: EmbeddingProvider 类
        Builder->>Builder: cls.from_config(config.embedding)
        Builder->>Builder: _embedding = 实例
    end

    Builder->>Registry: _storage_registry.get(backend)
    Registry-->>Builder: StorageBackend 类
    Builder->>Builder: cls.from_config(config.storage)
    Builder->>Builder: _storage = 实例

    Builder->>Registry: _retrieval_registry.get(strategy)
    Registry-->>Builder: RetrievalStrategy 类
    Builder->>Builder: _retrieval = 实例

    Builder->>Builder: WorkingMemoryCache(capacity, ttl)
    Builder->>Builder: _short_term = 实例

    opt consolidation.enabled
        Builder->>Registry: _consolidation_registry.get(...)
        Builder->>Builder: _consolidation = 实例
    end

    opt forgetting.enabled
        Builder->>Registry: _forgetting_registry.get(...)
        Builder->>Builder: _forgetting = 实例
    end

    opt telemetry.enabled
        Builder->>Builder: TelemetryTracingManager()
        Builder->>Builder: _telemetry.setup()
    end

    opt ttl.enabled
        Builder->>Builder: TTLExpirationManager()
    end

    Builder->>Builder: build()
    Builder->>Builder: 验证必需组件
    Builder->>Facade: 创建 MemorySystem 实例
    Facade-->>User: MemorySystem 实例
```

---

## 组件依赖关系

```mermaid
graph TB
    subgraph 必需组件
        STORAGE[StorageBackend]
        EMBED[EmbeddingProvider]
        RETRIEVE[RetrievalStrategy]
        SHORT[ShortTermMemory]
    end

    subgraph 可选组件
        CONS[ConsolidationStrategy]
        FORGET[ForgettingStrategy]
        TELEMETRY[TelemetryManager]
        TTL[TTLExpirationManager]
        TENANT[TenantIsolationManager]
    end

    MS[MemorySystem] --> STORAGE
    MS --> EMBED
    MS --> RETRIEVE
    MS --> SHORT
    MS -.-> CONS
    MS -.-> FORGET
    MS -.-> TELEMETRY
    MS -.-> TTL
    MS -.-> TENANT

    RETRIEVE --> STORAGE
    RETRIEVE --> EMBED
    CONS --> STORAGE
    CONS --> EMBED
    FORGET --> STORAGE
    TTL --> STORAGE

    style STORAGE fill:#ffeb3b
    style EMBED fill:#ffeb3b
    style RETRIEVE fill:#ffeb3b
    style SHORT fill:#ffeb3b
```

---

## 注册表自动发现

```mermaid
flowchart TD
    A[_ensure_registrations] --> B{已加载?}
    B -->|是| Z[返回]
    B -->|否| C[设置标志位]

    C --> D[扫描 storage_backends]
    D --> E[扫描 embedding_providers]
    E --> F[扫描 retrieval_strategies]
    F --> G[扫描 consolidation_strategies]
    G --> H[扫描 forgetting_strategies]

    subgraph SCAN["每个子包扫描"]
        I[pkgutil.iter_modules] --> J[遍历 .py 文件]
        J --> K[importlib.import_module]
        K --> L{导入成功?}
        L -->|是| M[装饰器执行<br/>类注册到字典]
        L -->|否| N[记录 warning]
    end

    D --> SCAN
    E --> SCAN
    F --> SCAN
    G --> SCAN
    H --> SCAN

    M --> O[继续下一个模块]
    N --> O
    O --> Z
```

---

## 条件注册流程

```mermaid
flowchart TD
    A[模块导入] --> B{依赖可用?}

    alt 依赖可用
        B -->|是| C[定义类]
        C --> D[执行 @register_xxx 装饰器]
        D --> E[类加入注册表]
        E --> F[可正常使用]
    else 依赖不可用
        B -->|否| G[跳过类定义]
        G --> H[注册表中无该 key]
        H --> I[from_config 时报错]
    end
```

### 示例：ChromaDB 条件注册

```python
# 条件注册：ChromaDB 不可用时不注册
_CHROMADB_AVAILABLE = False
try:
    import chromadb
    _CHROMADB_AVAILABLE = True
except ImportError:
    pass

if _CHROMADB_AVAILABLE:
    @register_storage("chroma")
    class ChromaStorage(StorageBackend):
        ...
```

---

## TTL 管理器启动

```mermaid
sequenceDiagram
    participant Builder as MemorySystemBuilder
    participant TTL as TTLExpirationManager
    participant Loop as asyncio.Task

    Builder->>TTL: TTLExpirationManager(storage, config)
    Note over TTL: 不立即启动

    alt 旧版 (已移除)
        Builder->>Loop: asyncio.create_task(ttl.start())
        Note over Loop: 在同步方法中启动<br/>可能导致无事件循环错误
    end

    Note over Builder: TTL 管理器存储在实例上<br/>可通过 start() 方法延迟启动
```

---

## 配置加载流程

```mermaid
flowchart TD
    A[MemoryConfig] --> B{加载方式?}

    B -->|默认| C[MemoryConfig]
    B -->|YAML| D[from_yaml path]
    B -->|JSON| E[from_json path]
    B -->|环境变量| F[from_env]

    C --> G[使用默认值]
    D --> H[解析 YAML 文件]
    E --> I[解析 JSON 文件]

    F --> J{pydantic-settings 可用?}
    J -->|是| K[MemoryEnvConfig]
    K --> L[自动读取 MEMORY_* 环境变量]
    L --> M[转换为 MemoryConfig]
    J -->|否| N[手动解析 os.environ]
    N --> O[回退到默认值]

    G --> P[返回 MemoryConfig]
    H --> P
    I --> P
    M --> P
    O --> P
```

### 环境变量映射

| 环境变量 | 配置字段 | 说明 |
|----------|----------|------|
| `MEMORY_STORAGE_BACKEND` | `storage.backend` | 存储后端类型 |
| `MEMORY_EMBEDDING_PROVIDER` | `embedding.provider` | 嵌入提供者 |
| `MEMORY_SHORT_TERM__CAPACITY` | `short_term.capacity` | 短期记忆容量 |
| `MEMORY_TTL__ENABLED` | `ttl.enabled` | TTL 过期开关 |

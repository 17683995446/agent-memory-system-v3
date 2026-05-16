# Agent Memory System

<div class="center" markdown>
  **A hierarchical long-short term memory system for AI agents with fast retrieval**
</div>

---

## ✨ 核心特性

| 特性 | 描述 |
|------|------|
| 🧠 **分层检索** | L1 短期记忆 → L2 时间范围 → L3 向量相似度 → L4 重要性 |
| ⚡ **高性能** | 嵌入缓存、FAISS ANN 索引、异步 I/O |
| 🔌 **可插拔** | 注册表模式 + 插件系统，支持多种存储后端 |
| 🏗️ **Builder 模式** | 链式调用构建，解决多参数构造问题 |
| 🔐 **多租户** | 命名空间隔离 + 配额管理 |
| 📊 **可观测性** | Prometheus 指标 + OpenTelemetry 追踪 + 结构化日志 |

---

## 📊 系统概览

```mermaid
graph TB
    subgraph "接入层"
        REST[REST API<br/>FastAPI]
        GRPC[gRPC Service]
        WS[WebSocket]
        CLI[CLI Interface]
    end

    subgraph "门面层"
        MS[MemorySystem<br/>remember/recall/consolidate]
    end

    subgraph "策略层"
        L1[L1 短期记忆<br/>TTLCache]
        L2[L2 时间检索<br/>O(log n)]
        L3[L3 向量检索<br/>ANN O(log n)]
        L4[L4 重要性<br/>排序]
    end

    subgraph "存储层"
        MEM[MemoryStorage]
        FAISS[FAISS ANN]
        CHROMA[ChromaDB]
        QDRANT[Qdrant]
    end

    subgraph "基础设施"
        AUTH[认证中间件<br/>API Key + OAuth2]
        TTL[TTL 过期管理]
        TENANT[多租户隔离]
        METRICS[Prometheus 指标]
        TRACE[OpenTelemetry]
    end

    REST --> MS
    GRPC --> MS
    WS --> MS
    CLI --> MS

    MS --> L1
    MS --> L2
    MS --> L3
    MS --> L4

    L2 --> MEM
    L3 --> FAISS
    L3 --> CHROMA
    L3 --> QDRANT

    MS --> AUTH
    MS --> TTL
    MS --> TENANT
    MS --> METRICS
    MS --> TRACE
```

---

## 🚀 快速开始

### 安装

```bash
# 基础安装
pip install agent-memory-system

# 完整安装（包含所有可选依赖）
pip install agent-memory-system[full]
```

### 基本使用

```python
from memory_system import MemorySystem, MemoryConfig

# 使用默认配置
config = MemoryConfig()
system = MemorySystem.from_config(config)

# 存储记忆
memory_id = await system.remember(
    content="用户偏好：喜欢深色主题",
    memory_type="semantic",
    importance="high"
)

# 检索记忆
results = await system.recall(
    query="用户界面偏好设置",
    max_results=5
)

# 获取统计
stats = await system.get_stats()
print(f"总记忆数: {stats.total_count}")
```

---

## 📁 项目结构

```
agent_memory_system_v2/
├── memory_system/
│   ├── core_abstractions/        # 核心抽象层
│   │   ├── memory_data_models.py # Pydantic 数据模型
│   │   ├── abstract_component_interfaces.py # ABC 接口
│   │   ├── component_registry_manager.py   # 注册表 + 装饰器
│   │   └── system_constants_definitions.py # 系统常量
│   │
│   ├── storage_backends/         # 存储后端
│   │   ├── in_memory_storage_backend.py
│   │   ├── faiss_ann_storage_backend.py
│   │   ├── chroma_vector_storage_backend.py
│   │   └── qdrant_vector_storage_backend.py
│   │
│   ├── embedding_providers/      # 嵌入提供者
│   │   ├── sentence_transformer_embedding_provider.py
│   │   ├── openai_embedding_provider.py
│   │   └── bge_embedding_provider.py
│   │
│   ├── retrieval_strategies/     # 检索策略
│   │   └── hierarchical_retrieval_strategy.py
│   │
│   ├── consolidation_strategies/ # 整合策略
│   ├── forgetting_strategies/    # 遗忘策略
│   ├── working_memory_cache.py   # L1 短期记忆
│   │
│   ├── agent_memory_system_facade.py  # 主入口
│   ├── restful_api_server.py          # REST API
│   └── grpc_service_server.py         # gRPC 服务
│
├── tests/                        # 435+ 测试用例
├── docs/                         # 文档源码
├── pyproject.toml                # 现代打包配置
└── README.md
```

---

## 📜 许可证

MIT License - 详见 [LICENSE](https://github.com/agent-memory-system/agent-memory-system/blob/main/LICENSE)

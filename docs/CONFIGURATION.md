# Agent Memory System 配置指南

## 快速开始

```python
from memory_system import MemoryConfig, MemorySystem

config = MemoryConfig.from_yaml("config.yaml")
memory = MemorySystem.from_config(config)
```

## 完整配置参考

### 存储配置 (StorageConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| backend | str | "memory" | 存储后端类型: memory/chroma/qdrant/faiss |
| max_memories | int | 10000 | 最大记忆数量 |
| embedding_dim | int | 768 | 嵌入向量维度 |
| persist_path | str | None | 持久化路径 |
| compression_enabled | bool | False | 是否启用压缩 |

### 嵌入配置 (EmbeddingConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| provider | str | "mock" | 嵌入提供者: mock/openai/bge/sentence_transformers |
| model_name | str | None | 模型名称 |
| api_key | str | None | API密钥 |
| api_base | str | None | API基础URL |
| dimension | int | 768 | 嵌入维度 |
| batch_size | int | 32 | 批处理大小 |
| cache_enabled | bool | True | 是否启用缓存 |

### 检索配置 (RetrievalConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| strategy | str | "hierarchical" | 检索策略: hierarchical/hybrid/vector_only |
| max_results | int | 10 | 最大返回结果数 |
| min_similarity | float | 0.0 | 最小相似度阈值 |
| association_depth | int | 0 | 关联展开深度 |
| reranking_enabled | bool | False | 是否启用重排序 |

### 短期记忆配置 (ShortTermConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| backend | str | "memory" | 后端类型: memory/redis/sqlite |
| capacity | int | 100 | 最大容量 |
| ttl_seconds | int | 300 | 默认TTL(秒) |
| eviction_policy | str | "lru" | 淘汰策略: lru/lfu/fifo |

### 租户配置 (TenantConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| enabled | bool | False | 是否启用多租户 |
| tenant_id | str | None | 租户标识 |
| quota | int | None | 记忆数量配额 |
| rate_limit | int | 60 | 每分钟请求限制 |
| isolation_level | str | "strict" | 隔离级别: strict/logical |

### 限流配置 (RateLimitConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| requests_per_minute | int | 60 | 每分钟请求数限制 |
| burst_size | int | 10 | 突发请求数 |
| strategy | str | "sliding_window" | 限流策略 |
| per_user | bool | True | 是否按用户限流 |
| per_endpoint | bool | True | 是否按端点限流 |
| block_duration | int | 0 | 封禁时长(秒) |

### 日志配置 (LoggingConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| level | str | "INFO" | 日志级别 |
| format | str | "json" | 日志格式: json/text |
| output | str | "stdout" | 输出目标: stdout/file |
| file_path | str | None | 日志文件路径 |
| rotation | str | "1 day" | 日志轮转策略 |

### 监控配置 (MonitoringConfig)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| enabled | bool | False | 是否启用监控 |
| metrics_endpoint | str | "/metrics" | 指标端点 |
| prometheus_enabled | bool | False | 是否启用Prometheus |
| tracing_enabled | bool | False | 是否启用链路追踪 |

## 配置示例

### 高性能配置
```yaml
storage:
  backend: faiss
  max_memories: 100000
  embedding_dim: 768

embedding:
  provider: bge
  model_name: BAAI/bge-large-zh
  dimension: 1024
  batch_size: 64

retrieval:
  strategy: hybrid
  max_results: 20
  reranking_enabled: true

short_term:
  backend: redis
  capacity: 1000
  ttl_seconds: 600
```

### 多租户SaaS配置
```yaml
tenant:
  enabled: true
  default_quota: 1000
  rate_limit: 100
  isolation_level: strict

rate_limit:
  requests_per_minute: 60
  burst_size: 10
  strategy: token_bucket
  per_user: true
  per_endpoint: true

storage:
  backend: qdrant
  max_memories: 1000000
```

### 开发环境配置
```yaml
storage:
  backend: memory
  max_memories: 1000

embedding:
  provider: mock
  dimension: 64

logging:
  level: DEBUG
  format: text
  output: stdout

monitoring:
  enabled: false
```

### 生产环境配置
```yaml
storage:
  backend: chroma
  max_memories: 50000
  persist_path: "/data/memories"
  compression_enabled: true

embedding:
  provider: openai
  model_name: "text-embedding-3-small"
  api_key: "${OPENAI_API_KEY}"
  cache_enabled: true

retrieval:
  strategy: hierarchical
  max_results: 10
  min_similarity: 0.7

logging:
  level: WARN
  format: json
  output: file
  file_path: "/var/log/memory_system/app.log"
  rotation: "1 day"

monitoring:
  enabled: true
  prometheus_enabled: true
  tracing_enabled: true
```

## 环境变量

配置支持从环境变量读取，使用 `${VAR_NAME}` 语法：

```yaml
embedding:
  api_key: "${OPENAI_API_KEY}"
  api_base: "${OPENAI_API_BASE:https://api.openai.com}"
```

支持的环境变量：

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| MEMORY_STORAGE_BACKEND | 存储后端 | memory |
| MEMORY_EMBEDDING_PROVIDER | 嵌入提供者 | mock |
| MEMORY_EMBEDDING_API_KEY | 嵌入API密钥 | None |
| MEMORY_LOG_LEVEL | 日志级别 | INFO |
| MEMORY_TENANT_ENABLED | 启用多租户 | false |
| MEMORY_RATE_LIMIT_RPM | 每分钟请求限制 | 60 |

## 配置验证

```python
from memory_system.configuration_settings import MemorySystemConfiguration

# 验证配置
config = MemorySystemConfiguration.from_yaml("config.yaml")
config.validate()  # 抛出异常如果配置无效

# 获取配置摘要
print(config.summary())
```

## 动态配置更新

```python
# 部分配置支持运行时更新
memory.update_config({
    "retrieval": {
        "max_results": 20
    }
})
```

## 配置热重载

```python
from memory_system.configuration_settings import ConfigWatcher

watcher = ConfigWatcher("config.yaml")
watcher.on_change(lambda new_config: memory.reload_config(new_config))
watcher.start()
```

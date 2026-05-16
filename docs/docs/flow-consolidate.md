# 记忆整合流程

本文档详细描述 `consolidate()` 方法和整合策略的执行流程。

## 流程概览

```mermaid
flowchart TD
    START([consolidate 触发]) --> CHECK{consolidation 策略?}
    CHECK -->|None| SKIP[跳过]
    CHECK -->|存在| SHOULD[should_consolidate]

    SHOULD --> NEED{需要整合?}
    NEED -->|否| SKIP
    NEED -->|是| GET_ALL[获取全部记忆]

    GET_ALL --> FILTER[筛选候选记忆]
    FILTER --> CLUSTER[时间窗口聚类]
    CLUSTER --> GEN[生成摘要]
    GEN --> STORE[存储新记忆]
    STORE --> DELETE[删除原始记忆]
    DELETE --> END([结束])
```

---

## 触发条件

### 自动触发

```mermaid
flowchart TD
    A[remember 完成] --> B[_check_consolidation]
    B --> C[获取 episodic + semantic 数量]
    C --> D{总数 >= threshold?}
    D -->|是| E[触发 consolidate]
    D -->|否| F[跳过]
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `consolidation.enabled` | `False` | 是否启用整合 |
| `consolidation.threshold` | `100` | 触发阈值 |
| `consolidation.min_cluster_size` | `3` | 最小聚类大小 |
| `consolidation.time_window_seconds` | `3600` | 时间窗口（1小时） |

### 手动触发

```python
# 通过 API
POST /api/v1/consolidate

# 通过代码
await system.consolidate()
```

---

## TimeBasedConsolidation 策略

```mermaid
sequenceDiagram
    participant S as Strategy
    participant Storage as Storage
    participant Emb as Embedding

    S->>Storage: search_by_time_range(0, now)
    Storage-->>S: 全部记忆

    S->>S: should_consolidate(memories)
    Note over S: 检查 episodic 数量 >= min_cluster_size * 2

    S->>S: 筛选候选记忆
    Note over S: 条件: EPISODIC + importance>=MEDIUM + 创建>1小时前

    S->>S: 时间窗口聚类
    Note over S: 时间差<=1小时的归为一组

    loop 每个聚类 (>=3条)
        S->>S: 拼接内容生成摘要
        S->>Emb: embed(summary)
        Emb-->>S: 向量
        S->>S: 创建 SEMANTIC MemoryItem
        S->>Storage: add(new_item)
    end

    loop 每条原始记忆
        S->>Storage: delete(original_id)
    end
```

### 候选筛选条件

```python
def _filter_candidates(self, memories: List[MemoryItem]) -> List[MemoryItem]:
    now = time.time()
    candidates = []

    for m in memories:
        # 必须是情景记忆
        if m.memory_type != MemoryType.EPISODIC:
            continue

        # 重要性至少为 MEDIUM
        if m.importance.value < ImportanceLevel.MEDIUM.value:
            continue

        # 创建时间超过 time_window_seconds
        age = now - m.timestamp
        if age < self.time_window_seconds:
            continue

        candidates.append(m)

    return candidates
```

### 时间窗口聚类

```mermaid
flowchart TD
    A[按时间戳排序] --> B[初始化聚类列表]
    B --> C[遍历记忆]

    subgraph LOOP["对每条记忆"]
        D{当前聚类为空 或 时间差<=窗口?}
        D -->|是| E[加入当前聚类]
        D -->|否| F[创建新聚类]
    end

    C --> LOOP
    LOOP --> G[过滤小聚类]
    G --> H[返回聚类列表]
```

```python
def _cluster_by_time(self, memories: List[MemoryItem]) -> List[List[MemoryItem]]:
    if not memories:
        return []

    # 按时间排序
    sorted_memories = sorted(memories, key=lambda m: m.timestamp)

    clusters = []
    current_cluster = [sorted_memories[0]]

    for i in range(1, len(sorted_memories)):
        time_diff = sorted_memories[i].timestamp - sorted_memories[i-1].timestamp

        if time_diff <= self.time_window_seconds:
            current_cluster.append(sorted_memories[i])
        else:
            if len(current_cluster) >= self.min_cluster_size:
                clusters.append(current_cluster)
            current_cluster = [sorted_memories[i]]

    # 处理最后一个聚类
    if len(current_cluster) >= self.min_cluster_size:
        clusters.append(current_cluster)

    return clusters
```

---

## SimilarityBasedConsolidation 策略

基于语义相似度聚类，而非时间窗口：

```mermaid
flowchart TD
    A[获取全部记忆] --> B[计算嵌入向量]
    B --> C[构建相似度矩阵]
    C --> D[聚类算法]
    D --> E[生成摘要]
    E --> F[存储 + 删除]
```

---

## 整合结果示例

```json
{
  "memory_id": "consolidated_abc123",
  "content": "用户在2024年1月进行了多次API调用，主要涉及用户认证和数据处理...",
  "memory_type": "semantic",
  "importance": "high",
  "metadata": {
    "consolidated_from": [
      "episodic_001",
      "episodic_002",
      "episodic_003"
    ],
    "cluster_time_range": ["2024-01-15T10:00:00", "2024-01-15T11:30:00"],
    "original_count": 3
  }
}
```

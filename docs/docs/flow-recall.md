# 记忆检索流程

本文档详细描述 `recall()` 方法和 L1-L4 分层检索的完整流程。

## 流程概览

```mermaid
flowchart TD
    START([recall 调用]) --> VALIDATE{参数验证}
    VALIDATE -->|失败| ERR[抛出 ValueError]
    VALIDATE -->|通过| SPAN[创建 OTel Span]

    SPAN --> GET_STM[获取短期记忆列表]
    GET_STM --> BUILD_CTX[构建检索上下文]

    BUILD_CTX --> RETRIEVE[retrieval.retrieve]

    subgraph HIERARCHICAL["HierarchicalRetrieval"]
        INIT[初始化 seen_ids] --> SAFE_MAX[safe_max = max max_results, 1]

        SAFE_MAX --> L1_CHECK{context.short_term_memories?}
        L1_CHECK -->|有| L1[_search_short_term]
        L1_CHECK -->|无| L2_CHECK
        L1 --> FAST_CHECK{fast模式 且 结果足够?}
        FAST_CHECK -->|是| MERGE
        FAST_CHECK -->|否| L2_CHECK

        L2_CHECK{adaptive/thorough?} -->|是| L2[_search_by_time]
        L2_CHECK -->|否| L3_CHECK
        L2 --> L3_CHECK

        L3_CHECK{adaptive/thorough?} -->|是| L3[_search_by_vector]
        L3_CHECK -->|否| L4_CHECK
        L3 --> L4_CHECK

        L4_CHECK{thorough?} -->|是| L4[_search_by_importance]
        L4_CHECK -->|否| MERGE
        L4 --> MERGE

        MERGE[_merge_and_rank] --> DEDUP[去重 + 排序]
        DEDUP --> TOP_K[取 top max_results]
    end

    TOP_K --> METRICS[记录指标]
    METRICS --> RETURN[返回结果]
    RETURN --> END([结束])
```

---

## L1: 短期记忆检索

**复杂度**: O(1) — 直接访问 TTLCache

```mermaid
flowchart TD
    A[_search_short_term] --> B[query 分词]
    B --> C[遍历短期记忆]

    subgraph LOOP["对每条短期记忆"]
        D[关键词交集] --> E[keyword_score = 交集大小 / query词数]
        E --> F[relevance = calculate_relevance_score]
        F --> G[total = keyword * 0.5 + relevance * 0.5]
    end

    C --> LOOP
    LOOP --> H[按分数降序排序]
    H --> I[取 top max_results]
```

### 相关性评分计算

```python
def calculate_relevance_score(self, current_time: float) -> float:
    # 重要性贡献 (30%)
    importance_score = self.importance.value / 5.0

    # 时效性衰减 (20%) - 30天半衰期
    age_days = (current_time - self.timestamp) / 86400
    recency_score = math.exp(-age_days / 30)

    # 访问频率贡献 (30%)
    access_score = min(1.0, self.access_count / 5.0)

    # 新鲜度衰减 (20%) - 7天半衰期
    last_access_days = (current_time - self.last_access) / 86400
    freshness_score = math.exp(-last_access_days / 7)

    return (importance_score * 0.3 + recency_score * 0.2 +
            access_score * 0.3 + freshness_score * 0.2)
```

---

## L2: 时间范围检索

**复杂度**: O(log n) — 时间索引

```mermaid
sequenceDiagram
    participant R as Retrieval
    participant S as Storage
    participant I as 时间索引

    R->>R: time_range = context.get or (0, now)
    R->>S: search_by_time_range(start, end)
    S->>I: 查询时间索引
    I-->>S: 返回匹配记忆
    S-->>R: List[MemoryItem]
    R->>R: 计算时间相关性分数
```

### 自动时间范围填充

```python
# 如果未提供 time_range，自动填充为全部时间
time_range = context.get("time_range")
if time_range is None:
    time_range = (0.0, time.time())  # 从 epoch 到现在
```

---

## L3: 向量相似度检索

**复杂度**: O(log n) — ANN 索引 (FAISS/HNSW)

```mermaid
sequenceDiagram
    participant R as Retrieval
    participant E as Embedding
    participant S as Storage
    participant ANN as ANN索引

    R->>E: embed(query)
    E-->>R: query_vector
    R->>S: search_by_vector(query_vector, top_k)
    S->>ANN: ANN 查询
    ANN-->>S: 返回 (id, distance) 对
    S->>S: 转换 distance → similarity
    S-->>R: List[(MemoryItem, similarity)]
```

### 向量检索实现

```python
async def search_by_vector(self, query_vector, top_k=5, filters=None):
    results = []
    async with self._lock:
        for memory in self._memories.values():
            if memory.embedding is None:
                continue  # 跳过无嵌入的记忆

            similarity = cosine_similarity(query_vector, memory.embedding)

            # 应用过滤器
            if filters and not matches_filters(memory, filters):
                continue

            results.append((memory, similarity))

    # 按相似度降序排序
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:top_k]
```

---

## L4: 重要性检索

**复杂度**: O(n log n) — 全量排序

```mermaid
flowchart TD
    A[_search_by_importance] --> B[storage.search_by_time_range 0, now]
    B --> C[获取全部记忆]
    C --> D[按 importance.value 降序排序]
    D --> E[取 top max_results]
```

!!! warning "性能警告"
    L4 层会加载**全部记忆**到内存并排序。当记忆数量大时（默认上限 10000），可能影响性能。建议仅在 `thorough` 模式下使用。

---

## 结果合并与去重

```mermaid
flowchart TD
    A[all_results] --> B[按 memory_id 去重]
    B --> C[保留最高分数]
    C --> D[按分数降序排序]
    D --> E[取 top max_results]
```

```python
def _merge_and_rank(self, results: List, max_results: int) -> List:
    # 去重：同一记忆只保留最高分
    best_scores: Dict[str, Tuple] = {}
    for memory, score, layer in results:
        mid = memory.memory_id
        if mid not in best_scores or score > best_scores[mid][1]:
            best_scores[mid] = (memory, score, layer)

    # 排序并取 top-K
    merged = sorted(best_scores.values(), key=lambda x: x[1], reverse=True)
    return merged[:max_results]
```

---

## 检索策略对比

| 策略 | 层级 | 典型延迟 | 召回率 | 使用场景 |
|------|------|---------|--------|----------|
| `fast` | L1 | < 0.1ms | 低 | 实时响应、仅短期记忆 |
| `adaptive` | L1+L2+L3 | < 1ms | 中 | **默认**，平衡速度与召回 |
| `thorough` | L1+L2+L3+L4 | < 10ms | 高 | 高召回、重要性敏感场景 |

### 比例分配

```python
# 各层结果数量比例
short_term_ratio = 0.33   # L1 取 33%
time_ratio = 0.33         # L2 取 33%
vector_ratio = 0.50       # L3 取 50%
importance_ratio = 0.25   # L4 取 25%
```

# Agent Memory System V3.0 - 最佳实践文档

## 文档概述

**文档日期**: 2026-05-16  
**系统版本**: V3.0.2  
**适用范围**: 分布式系统设计、Raft共识实现、资源管理、性能优化

---

## 一、分布式系统设计最佳实践

### 1.1 故障转移设计

#### 最佳实践

1. **必须实现重试机制**
   ```python
   # 推荐: 使用指数退避重试
   @dataclass
   class FailoverConfig:
       max_retries: int = 5
       initial_retry_delay: float = 0.1
       max_retry_delay: float = 5.0
       retry_multiplier: float = 2.0
   
   def _calculate_retry_delay(self, retry_count: int) -> float:
       delay = self.initial_retry_delay * (self.retry_multiplier ** retry_count)
       return min(delay, self.max_retry_delay)
   ```

2. **候选节点验证必须完整**
   ```python
   async def validate_candidate_node(self, candidate_id: str) -> bool:
       # 1. 检查节点是否在已知节点列表中
       if candidate_id not in self.known_peers:
           raise CandidateValidationError(candidate_id, "not in known peers")
       
       # 2. 检查节点健康状态
       if not await self.check_health(candidate_id):
           raise CandidateValidationError(candidate_id, "unhealthy")
       
       # 3. 检查节点的日志是否足够新
       if await self.get_log_length(candidate_id) < self.min_log_length:
           raise CandidateValidationError(candidate_id, "log too old")
       
       return True
   ```

3. **故障检测超时要合理配置**
   ```python
   # 推荐: 心跳超时 = 心跳间隔 * 5
   heartbeat_interval = 0.1  # 100ms
   heartbeat_timeout = heartbeat_interval * 5  # 500ms
   
   # 考虑网络延迟
   fault_detection_timeout = 1.0  # 1秒
   ```

#### 反模式

```python
# 错误: 单次失败就放弃
def failover(self):
    candidates = self.get_candidates()
    if not candidates:
        return False  # 直接放弃，没有重试
    ...

# 错误: 候选节点验证不完整
def failover(self):
    candidates = [n for n in self.nodes if n.is_alive]  # 只检查is_alive
    ...
```

### 1.2 心跳检测设计

#### 最佳实践

1. **心跳时间必须正确初始化**
   ```python
   # 推荐: 初始化为当前时间
   def __init__(self):
       self._last_heartbeat_sent = time.time()
       self._leader_heartbeat_received = time.time()
       for peer in self.peers:
           self._last_heartbeat_received[peer] = time.time()
           self._heartbeat_missed[peer] = 0
   ```

2. **心跳丢失要跟踪计数**
   ```python
   def check_heartbeat(self, peer: str):
       current_time = time.time()
       if current_time - self._last_heartbeat_received[peer] > self.heartbeat_timeout:
           self._heartbeat_missed[peer] += 1
           if self._heartbeat_missed[peer] >= self.max_missed:
               self.mark_peer_unhealthy(peer)
   ```

#### 反模式

```python
# 错误: 初始化为0
def __init__(self):
    self._last_heartbeat = 0  # 首次检测必然超时
```

### 1.3 Leader选举设计

#### 最佳实践

1. **多数票计算要正确**
   ```python
   # 推荐: 多数 = 半数 + 1
   total_nodes = len(self.peers) + 1
   majority = total_nodes // 2 + 1
   
   # 示例:
   # 3节点 -> majority = 2
   # 5节点 -> majority = 3
   # 7节点 -> majority = 4
   ```

2. **选举超时要随机化**
   ```python
   import random
   
   def get_election_timeout(self) -> float:
       return random.uniform(
           self.election_timeout_min,
           self.election_timeout_max
       )
   ```

3. **Leader切换要有冷却期**
   ```python
   def select_new_leader(self, candidates: List[Node]) -> Node:
       # 排除刚刚是Leader的节点
       candidates = [n for n in candidates if n.node_id != self.last_leader_id]
       if not candidates:
           candidates = self.all_alive_nodes  # 没有其他候选则只能选原来的
       return max(candidates, key=lambda n: n.priority)
   ```

#### 反模式

```python
# 错误: 多数票计算错误
majority = (total_nodes + 1) // 2  # 这是半数，不是多数

# 错误: 选举超时固定
election_timeout = 0.2  # 所有节点同时发起选举
```

---

## 二、Raft共识实现最佳实践

### 2.1 日志复制设计

#### 最佳实践

1. **必须验证PrevLogIndex和PrevLogTerm**
   ```python
   async def handle_append_entries(self, request: AppendEntriesRequest) -> Response:
       # 1. 检查prev_log_index是否存在
       if request.prev_log_index >= len(self.log):
           return Response(
               success=False,
               conflict_index=len(self.log)  # 告知Leader我们的日志长度
           )
       
       # 2. 检查prev_log_term是否匹配
       if self.log[request.prev_log_index].term != request.prev_log_term:
           # 找到冲突任期的第一个索引
           conflict_term = self.log[request.prev_log_index].term
           conflict_index = request.prev_log_index
           for i in range(request.prev_log_index, -1, -1):
               if self.log[i].term != conflict_term:
                   conflict_index = i + 1
                   break
           return Response(
               success=False,
               conflict_index=conflict_index
           )
       
       # 3. 追加新条目
       ...
   ```

2. **日志冲突要正确解决**
   ```python
   def resolve_conflict(self, conflict_index: int, new_entries: List[Entry]):
       # 截断冲突的日志
       self.log = self.log[:conflict_index]
       # 追加新条目
       self.log.extend(new_entries)
   ```

3. **日志复制要有重试机制**
   ```python
   async def replicate_log(self, peer: str, max_retries: int = 5):
       for attempt in range(max_retries):
           try:
               response = await self.send_append_entries(peer)
               if response.success:
                   return True
               # 根据conflict_index调整next_index
               self.next_index[peer] = response.conflict_index
           except Exception as e:
               await asyncio.sleep(self._calculate_retry_delay(attempt))
       return False
   ```

#### 反模式

```python
# 错误: 不验证PrevLogIndex/PrevLogTerm
def handle_append_entries(self, request):
    for entry in request.entries:
        self.log.append(entry)  # 直接追加，可能导致不一致
```

### 2.2 提交索引设计

#### 最佳实践

1. **提交索引更新要检查多数派**
   ```python
   async def advance_commit_index(self):
       if self.role != Role.LEADER:
           return
       
       last_log_index = len(self.log) - 1
       majority = (len(self.peers) + 1) // 2 + 1
       
       # 从高到低检查可以提交的索引
       for index in range(last_log_index, self.commit_index, -1):
           entry = self.log[index]
           
           # Leader只能提交当前任期的条目
           if entry.term != self.current_term:
               continue
           
           # 统计匹配此索引的节点数
           match_count = 1 + sum(
               1 for peer in self.peers 
               if self.match_index.get(peer, -1) >= index
           )
           
           if match_count >= majority:
               self.commit_index = index
               await self.apply_committed_entries()
               break
   ```

2. **提交索引要及时传播**
   ```python
   async def send_heartbeat(self, peer: str):
       request = AppendEntriesRequest(
           term=self.current_term,
           leader_id=self.node_id,
           prev_log_index=self.next_index[peer] - 1,
           prev_log_term=self.get_prev_log_term(peer),
           entries=[],
           leader_commit=self.commit_index,  # 携带最新的commit_index
       )
       await self.transport.send(peer, request)
   ```

---

## 三、资源管理最佳实践

### 3.1 定时器管理

#### 最佳实践

1. **使用上下文管理器**
   ```python
   from contextlib import contextmanager
   
   @contextmanager
   def managed_timer(self, delay: float, callback: Callable):
       timer = threading.Timer(delay, callback)
       timer.daemon = True
       timer.start()
       try:
           yield timer
       finally:
           timer.cancel()
   ```

2. **维护定时器映射**
   ```python
   class ResourceManager:
       def __init__(self):
           self._timers: Dict[str, List[threading.Timer]] = {}
           self._lock = threading.Lock()
       
       def create_timer(self, key: str, delay: float, callback: Callable):
           with self._lock:
               timer = threading.Timer(delay, callback)
               timer.daemon = True
               timer.start()
               if key not in self._timers:
                   self._timers[key] = []
               self._timers[key].append(timer)
       
       def cancel_timers(self, key: str):
           with self._lock:
               if key in self._timers:
                   for timer in self._timers[key]:
                       timer.cancel()
                   del self._timers[key]
       
       def cleanup_all(self):
           with self._lock:
               for timers in self._timers.values():
                   for timer in timers:
                       timer.cancel()
               self._timers.clear()
   ```

#### 反模式

```python
# 错误: 创建定时器但不管理
def create_timer(self):
    timer = threading.Timer(3600, callback)
    timer.start()
    # 没有保存timer引用，无法取消
```

### 3.2 连接管理

#### 最佳实践

1. **使用连接池**
   ```python
   class ConnectionPool:
       def __init__(self, max_connections: int = 100):
           self._pool: Dict[str, Queue] = {}
           self._max_connections = max_connections
       
       async def acquire(self, key: str) -> Connection:
           if key not in self._pool:
               self._pool[key] = Queue(maxsize=self._max_connections)
           
           pool = self._pool[key]
           if pool.empty():
               return await self._create_connection(key)
           return await pool.get()
       
       async def release(self, key: str, conn: Connection):
           if key in self._pool and not self._pool[key].full():
               await self._pool[key].put(conn)
           else:
               await conn.close()
       
       async def cleanup(self, key: str):
           if key in self._pool:
               while not self._pool[key].empty():
                   conn = await self._pool[key].get()
                   await conn.close()
               del self._pool[key]
   ```

### 3.3 内存管理

#### 最佳实践

1. **使用弱引用缓存**
   ```python
   import weakref
   
   class WeakCache:
       def __init__(self):
           self._cache: Dict[str, weakref.ref] = {}
       
       def get(self, key: str) -> Optional[Any]:
           if key in self._cache:
               ref = self._cache[key]()
               if ref is not None:
                   return ref
               del self._cache[key]
           return None
       
       def set(self, key: str, value: Any):
           self._cache[key] = weakref.ref(value)
   ```

2. **定期清理和GC**
   ```python
   import gc
   
   class ManagedCache:
       def __init__(self, cleanup_interval: int = 100):
           self._cache: Dict[str, Any] = {}
           self._access_count = 0
           self._cleanup_interval = cleanup_interval
       
       def get(self, key: str) -> Optional[Any]:
           self._access_count += 1
           if self._access_count % self._cleanup_interval == 0:
               self._cleanup()
           return self._cache.get(key)
       
       def _cleanup(self):
           # 清理过期条目
           expired = [k for k, v in self._cache.items() if self._is_expired(v)]
           for k in expired:
               del self._cache[k]
           # 触发垃圾回收
           gc.collect()
   ```

---

## 四、性能优化最佳实践

### 4.1 索引设计

#### 最佳实践

1. **自动维护索引**
   ```python
   class IndexedStorage:
       def __init__(self, index_fields: List[str]):
           self._data: Dict[str, Any] = {}
           self._indexes: Dict[str, Dict[Any, Set[str]]] = {
               field: {} for field in index_fields
           }
           self._index_threshold = 100
       
       async def insert(self, key: str, value: Dict[str, Any]):
           self._data[key] = value
           # 自动更新索引
           for field in self._indexes:
               if field in value:
                   self._add_to_index(field, value[field], key)
       
       def _add_to_index(self, field: str, value: Any, key: str):
           if value not in self._indexes[field]:
               self._indexes[field][value] = set()
           self._indexes[field][value].add(key)
       
       async def query(self, field: str, value: Any) -> List[Any]:
           if field in self._indexes and value in self._indexes[field]:
               keys = self._indexes[field][value]
               return [self._data[k] for k in keys]
           return []
   ```

2. **多条件查询优化**
   ```python
   async def query_by_conditions(self, conditions: Dict[str, Any]) -> List[Any]:
       # 获取每个条件的候选集
       candidate_sets = []
       for field, value in conditions.items():
           if field in self._indexes and value in self._indexes[field]:
               candidate_sets.append(self._indexes[field][value])
       
       if not candidate_sets:
           return []
       
       # 取交集
       result_keys = set.intersection(*candidate_sets)
       return [self._data[k] for k in result_keys]
   ```

### 4.2 缓存设计

#### 最佳实践

1. **LRU缓存**
   ```python
   from functools import lru_cache
   from collections import OrderedDict
   
   class LRUCache:
       def __init__(self, max_size: int = 1000):
           self._cache: OrderedDict = OrderedDict()
           self._max_size = max_size
       
       def get(self, key: str) -> Optional[Any]:
           if key in self._cache:
               # 移到末尾表示最近使用
               self._cache.move_to_end(key)
               return self._cache[key]
           return None
       
       def set(self, key: str, value: Any):
           if key in self._cache:
               self._cache.move_to_end(key)
           else:
               if len(self._cache) >= self._max_size:
                   # 删除最久未使用的
                   self._cache.popitem(last=False)
           self._cache[key] = value
   ```

2. **查询缓存装饰器**
   ```python
   def cached_query(ttl: float = 60.0):
       cache: Dict[str, Tuple[Any, float]] = {}
       
       def decorator(func):
           @wraps(func)
           async def wrapper(*args, **kwargs):
               cache_key = f"{func.__name__}:{args}:{kwargs}"
               if cache_key in cache:
                   value, timestamp = cache[cache_key]
                   if time.time() - timestamp < ttl:
                       return value
               result = await func(*args, **kwargs)
               cache[cache_key] = (result, time.time())
               return result
           return wrapper
       return decorator
   ```

### 4.3 热点分散

#### 最佳实践

```python
class HotspotDetector:
    def __init__(self, threshold: int = 1000, window: float = 60.0):
        self._access_count: Dict[str, int] = {}
        self._threshold = threshold
        self._window = window
        self._hotspots: Set[str] = set()
    
    def record_access(self, key: str):
        self._access_count[key] = self._access_count.get(key, 0) + 1
        if self._access_count[key] > self._threshold:
            self._hotspots.add(key)
    
    def is_hotspot(self, key: str) -> bool:
        return key in self._hotspots
    
    def get_replica_key(self, key: str) -> str:
        if self.is_hotspot(key):
            # 返回一个副本key用于分散负载
            replica_idx = hash(key + str(time.time() // self._window)) % 3
            return f"{key}:replica-{replica_idx}"
        return key
```

---

## 五、测试最佳实践

### 5.1 单元测试

#### 最佳实践

1. **每个修复都要有测试**
   ```python
   class TestFailoverFix:
       @pytest.mark.asyncio
       async def test_failover_with_retry(self):
           """测试故障转移重试机制"""
           cluster = await self.create_cluster()
           
           # 模拟Leader故障
           await cluster.kill_leader()
           
           # 验证故障转移成功
           assert await cluster.wait_for_failover(timeout=5.0)
           assert cluster.has_leader()
       
       @pytest.mark.asyncio
       async def test_failover_exponential_backoff(self):
           """测试指数退避重试"""
           node = self.create_node()
           
           delays = [node._calculate_retry_delay(i) for i in range(5)]
           
           # 验证指数增长
           for i in range(1, len(delays)):
               assert delays[i] >= delays[i-1]
           # 验证最大延迟限制
           assert all(d <= node.failover_config.max_retry_delay for d in delays)
   ```

2. **测试边界条件**
   ```python
   @pytest.mark.asyncio
   async def test_failover_no_candidates(self):
       """测试没有候选节点的情况"""
       cluster = await self.create_cluster(num_nodes=1)
       await cluster.kill_leader()
       
       # 单节点集群无法故障转移
       assert not await cluster.wait_for_failover(timeout=1.0)
   ```

### 5.2 集成测试

#### 最佳实践

```python
class TestRaftCluster:
   @pytest.mark.asyncio
   async def test_log_replication_consistency(self):
       """测试日志复制一致性"""
       cluster = await self.create_cluster(num_nodes=5)
       
       # 写入数据
       for i in range(100):
           await cluster.write(f"key{i}", f"value{i}")
       
       # 等待复制完成
       await asyncio.sleep(1.0)
       
       # 验证所有节点日志一致
       log_lengths = [len(node.log) for node in cluster.nodes]
       assert len(set(log_lengths)) == 1  # 所有节点日志长度相同
```

---

## 六、配置最佳实践

### 6.1 配置设计

#### 最佳实践

1. **所有配置都要有默认值**
   ```python
   @dataclass
   class RaftConfig:
       election_timeout_min: float = 0.15
       election_timeout_max: float = 0.30
       heartbeat_interval: float = 0.05
       heartbeat_timeout: float = 0.25
       # 新增配置也要有默认值
       fault_detection_timeout: float = 1.0
       log_replication_timeout: float = 2.0
   ```

2. **配置验证**
   ```python
   def __post_init__(self):
       if self.election_timeout_min >= self.election_timeout_max:
           raise ValueError("election_timeout_min must be less than election_timeout_max")
       if self.heartbeat_timeout <= self.heartbeat_interval:
           raise ValueError("heartbeat_timeout must be greater than heartbeat_interval")
   ```

---

## 七、监控最佳实践

### 7.1 关键指标

```python
class Metrics:
   # 故障转移指标
   failover_total: Counter
   failover_success: Counter
   failover_latency: Histogram
   
   # 日志复制指标
   log_replication_total: Counter
   log_replication_success: Counter
   log_replication_latency: Histogram
   
   # 选举指标
   election_total: Counter
   election_success: Counter
   
   # 资源指标
   active_connections: Gauge
   active_timers: Gauge
   memory_usage: Gauge
```

### 7.2 告警规则

```yaml
alerts:
  - name: HighFailoverFailureRate
    expr: rate(failover_total[5m]) - rate(failover_success[5m]) > 0.1
    severity: critical
    
  - name: HighLogReplicationLatency
    expr: histogram_quantile(0.99, log_replication_latency) > 1.0
    severity: warning
    
  - name: MemoryLeak
    expr: derivative(memory_usage[1h]) > 0
    severity: warning
```

---

## 八、总结

### 8.1 核心原则

1. **可靠性优先**: 故障转移必须有重试机制
2. **一致性保证**: Raft协议要完整实现
3. **资源安全**: 所有资源必须正确清理
4. **性能优化**: 索引和缓存要合理使用
5. **向后兼容**: 新增配置要有默认值

### 8.2 检查清单

- [ ] 故障转移有重试机制
- [ ] 心跳时间正确初始化
- [ ] 多数票计算正确
- [ ] PrevLogIndex/PrevLogTerm验证完整
- [ ] 定时器正确取消
- [ ] 连接正确关闭
- [ ] 缓存正确清理
- [ ] 索引自动维护
- [ ] 配置有默认值
- [ ] 测试覆盖完整

---

*文档生成时间: 2026-05-16*  
*系统版本: V3.0.2*  
*文档作者: Agent Memory System Team*

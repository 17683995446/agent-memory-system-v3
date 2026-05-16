# Changelog

All notable changes to the Agent Memory System will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.29.0] - 2026-05-16

### Added (实现 - 11项)
- **智能缓存预热 (CacheWarmer)**: 跟踪访问模式，启动时预计算热门记忆嵌入
- **异步批量写入队列 (AsyncBatchWriter)**: 异步批量写入，支持合并相似请求，定时刷新
- **增强多租户资源隔离 (EnhancedTenantIsolationManager)**: 细粒度资源跟踪，多维度配额管理
- **自适应检索策略选择 (AdaptiveStrategySelector)**: 根据查询特征自动选择最优检索策略
- **情境感知检索 (Context-Aware Retrieval)**: 基于时间、会话主题、位置计算情境相关性分数
- **多模态记忆支持框架 (Multi-Modal Memory)**: 支持文本/图像/音频/视频多模态内容
- **基于LLM的记忆摘要生成 (LLMSummarizer)**: 抽取式/生成式摘要，关键词标签提取
- **W-TinyLFU缓存策略 (WTinyLFUCache)**: 窗口+主缓存结构，Count-Min Sketch频率统计
- **时间索引TTL清理 (TimeIndexedTTLManager)**: 按过期时间桶组织，O(1)过期检测
- **HNSW近似最近邻索引 (HNSWIndex)**: 分层可导航小世界图，O(log n)检索复杂度
- **读写锁优化短期记忆并发 (ReadWriteLock)**: 读-读并发，读-写互斥

### Refactored (重构 - 11项)
- **统一存储接口异常处理**: 创建完整的异常层次结构 (StorageError/RetrievalError等)
- **解耦检索策略与存储后端**: RetrievalEngine类提供统一检索接口
- **重构统计信息收集**: MetricsCollector单例类支持计数器/仪表盘/直方图
- **标准化配置验证逻辑**: Pydantic v2兼容的验证器
- **重构嵌入提供者工厂模式**: EmbeddingFactory简化提供者注册和创建
- **提取公共检索结果处理逻辑**: RetrievalResultProcessor支持去重/排序/多样性重排
- **重构TTL管理器的存储依赖**: TTLStorageInterface抽象接口
- **统一日志格式和级别**: 结构化JSON日志，支持上下文信息
- **重构快照管理器的序列化逻辑**: 支持JSON/MsgPack/Pickle序列化
- **引入依赖注入容器**: DIContainer支持单例/瞬态/作用域生命周期
- **重构批处理抽象层**: BatchProcessor支持异步批处理/超时控制/背压机制

### Polished (完善 - 10项)
- **API文档生成器**: 自动生成API文档和示例代码，支持OpenAPI规范
- **全面单元测试覆盖**: 36个边界条件测试，覆盖安全测试和性能边界
- **类型注解检查工具**: 自动检查类型注解完整性，生成覆盖率报告
- **性能基准测试**: BenchmarkResult数据类，并发/批量/可扩展性测试
- **国际化支持 (i18n)**: 支持7种语言，30+错误消息翻译
- **精细化API限流器**: 4种限流策略，支持多层限流
- **配置文档**: 完整的配置参数参考表，4种环境配置示例
- **代码覆盖率配置**: 80%覆盖率目标，项目/补丁/变更检查
- **安全审计日志**: 17种审计事件类型，5种严重级别
- **性能回归测试**: 基线管理和性能退化检测，20%退化阈值

### 测试
- **新增测试**: 147个（创新32 + 重构47 + 完善36 + 边界34）
- **总测试数**: 734个通过
- **端到端集成测试**: 29个

## [2.28.0] - 2026-05-16

### Added (创新特性)
- **记忆关联图谱 (Memory Graph)**: `MemoryLink` 数据模型 + `LinkRelation` 枚举（6种关联类型），支持双向链接、BFS多跳遍历
- **关联感知检索**: `recall_with_associations()` 方法，检索后自动展开关联记忆（1-2跳）
- **混合检索策略 (Hybrid Retrieval)**: 稠密向量 + BM25稀疏检索 + RRF融合算法，显著提升检索准确率
- **倒排索引**: `InMemoryStorageBackend` 内置倒排索引，支持高效关键词搜索
- **自动重要性评分**: `HeuristicImportanceScorer` 多维度评估器（信号词/偏好/事实/长度），`importance="auto"` 智能路由
- **RememberResponse**: `remember_detailed()` 返回详细信息（置信度、相关记忆推荐）
- **遗忘操作增强**: `forget()` / `forget_batch()` / `forget_by_query()` 三种遗忘方式
- **元数据搜索**: `search_by_metadata()` 按键值对搜索记忆
- **批量删除**: `batch_delete()` 优化批量删除性能
- **BM25评分 + RRF融合**: 数学工具函数，支持标准BM25和Reciprocal Rank Fusion

### Changed (重构)
- `_search_by_importance()` 使用 `list_memories(order_by="importance")` 替代全量加载排序
- `StorageBackend` 新增 5 个接口方法（图操作4个 + search_by_keyword + search_by_metadata + batch_delete）
- `MemorySystemBuilder` 支持 `with_importance_scorer()` 构建器方法

### Fixed
- `get_links()` 方向过滤逻辑错误（incoming方向返回空结果）

### 测试
- 新增 56 个创新特性单元测试
- 新增 32 个混合检索单元测试
- 新增 30 个重构功能单元测试
- 新增 29 个端到端集成测试
- **总计新增 147 个测试用例**

## [2.27.0] - 2026-05-16

### Fixed
- **P0**: 21 处 `except Exception:` 块无日志记录，异常被静默吞没影响问题排查
- **P1**: `_counter` 自增无锁保护，高并发下可能溢出或重复
- **P1**: `_stats` 字典更新无锁，多协程并发写入可能丢失更新
- **P1**: `check_quota()` 存在 TOCTOU 竞态条件，检查与增加非原子操作
- **P2**: `memory_snapshot_manager.py` 构造函数缺少类型注解
- **P2**: `TenantAwareStorageWrapper` 方法缺少类型注解和文档字符串
- **P2**: `FAISSStorage.save_index()`/`load_index()` 缺少文档字符串
- **P2**: 快照 ID 验证允许过宽字符集，存在路径遍历风险
- **P2**: 租户 ID 验证不检查特殊字符（如 `/`）

### Added
- **P0**: 所有静默 except 块添加 `logger.warning()` 日志记录
- **P1**: `working_memory_cache.py` 添加 `asyncio.Lock` 保护计数器
- **P1**: `agent_memory_system_facade.py` 添加 `_stats_lock` 保护统计更新
- **P1**: `tenant_isolation_manager.py` 添加 `check_and_increment_quota()` 原子操作
- **P2**: 嵌入缓存配置常量: `EMBEDDING_CACHE_MAX_SIZE`, `EMBEDDING_CACHE_TTL_SECONDS`
- **P2**: 检索默认比例常量: `DEFAULT_SHORT_TERM_RATIO`, `DEFAULT_TIME_RATIO`, `DEFAULT_VECTOR_RATIO`, `DEFAULT_IMPORTANCE_RATIO`
- **P2**: 完整的类型注解覆盖 `memory_snapshot_manager.py`, `tenant_isolation_manager.py`, `memory_ttl_expiration_manager.py`
- **P2**: 详细的文档字符串覆盖 `FAISSStorage`, `TenantAwareStorageWrapper`
- **测试**: 27 个新边缘情况测试 (快照损坏/TTL并发/租户配额/遥测降级/WebSocket断开)

### Changed
- 所有硬编码值移至 `system_constants_definitions.py`
- 快照 ID 验证加强: 长度限制、前缀检查、路径遍历防护
- 租户 ID 验证加强: 特殊字符过滤

## [2.26.0] - 2026-05-16

### Fixed
- **P0**: `recall()` 未使用嵌入缓存 (`TTLCache`)，每次检索都重新计算嵌入向量，严重影响性能
- **P0**: `recall()` 的 `strategy` 参数被静默忽略，传入 `fast`/`adaptive`/`thorough` 无任何效果
- **P1**: `code_memory_support/__init__.py` 违反 Python 惯例，552 行业务逻辑代码全部堆积在 `__init__.py` 中
- **P1**: 8 处空 `except: pass` 块缺少日志记录，异常被静默吞没
- **P1**: `_check_forgetting()` 和 `consolidate()` 缺少操作日志，无法追踪遗忘/整合过程

### Added
- **P0**: `CachingEmbeddingProxy` 缓存代理类，透明包装 `EmbeddingProvider`，支持 `embed()`/`embed_batch()`/`similarity()` 缓存穿透优化
- **P0**: `HierarchicalRetrieval.retrieve()` 支持运行时策略覆盖 (`strategy_override`)，通过 `context` 字典传递
- **P1**: `code_memory_support` 模块拆分为 5 个独立子模块: `language_detector.py`, `ast_parser.py`, `code_analyzer.py`, `code_memory.py`, `utils.py`
- **P2**: `StorageBackend.list_memories()` 分页查询接口，支持类型过滤、排序、分页
- **P2**: `MemoryStorage` 优化实现 `list_memories()`，直接操作内存索引，避免全量扫描
- **P2**: `RetrievalResponse` 数据模型增强，新增 `total_found`/`retrieval_time` 字段和 `to_dict()`/`from_dict()` 序列化方法
- **测试**: 69 个新单元测试覆盖所有改进 (P0: 5, P1: 34, P2: 30)

### Changed
- `recall()` 使用 `CachingEmbeddingProxy` 包装嵌入提供者，查询嵌入自动缓存
- `recall()` 通过 `context["strategy_override"]` 传递策略覆盖给检索策略
- `_check_forgetting()` 添加详细日志: 触发原因、删除数量、剩余数量、耗时
- `consolidate()` 添加详细日志: 加载数量、生成数量、删除数量、耗时
- `memory_snapshot_manager.py` 8 处空 except 块添加 `logger.debug()` 日志

## [2.25.0] - 2026-05-15

### Fixed
- **P0**: 统一三处版本号不一致问题 (`__init__.py`, `system_constants_definitions.py`, `pyproject.toml`)，建立单一事实来源
- **P0**: `remember_batch()` 缺失整合检查 (`_check_consolidation`)，可能导致存储无限增长
- **P0**: `remember_batch()` 缺失遗忘检查 (`_check_forgetting`)，批量写入后不触发遗忘
- **P0**: `remember_batch()` 缺失指标记录 (`metrics.record_remember`) 和统计更新
- **P0**: `remember_batch()` 缺失分布式追踪 span
- **P1**: `ScoreBasedForgetting.forget_ratio` 参数为死代码，从未被使用
- **P1**: `ScoreBasedForgetting` 权重无验证，可传入负权重/缺失维度/零总和
- **P1**: `MemoryItem.calculate_relevance_score()` 与 `ScoreBasedForgetting._calculate_retention_score()` 评分逻辑不一致
- **P1**: `__init__.py` 的 `__all__` 导出可能为 `None` 的可选模块
- **P1**: `VERSION`、`MemorySystemBuilder`、`API_AVAILABLE` 未在 `__all__` 中导出
- **P2**: `remember()`/`recall()`/`remember_batch()` 无输入长度限制，存在安全风险
- **P2**: `pyproject.toml` 的 `build-backend` 配置错误导致 `pip install -e .` 失败

### Added
- **P1**: `MemorySystem` 支持异步上下文管理器 (`async with MemorySystem(...) as ms:`)
- **P2**: 输入安全限制常量: `MAX_CONTENT_LENGTH`, `MAX_QUERY_LENGTH`, `MAX_BATCH_SIZE`
- **P2**: `_validate_weights()` 权重验证与自动归一化函数
- **P2**: `ScoreBasedForgetting` 可配置半衰期参数 (`recency_half_life_days`, `freshness_half_life_days`)
- **P2**: 遗忘策略日志记录 (DEBUG 级别逐条记录, INFO 级别汇总)
- **P2**: 26 个新单元测试覆盖 V2.25 所有改进
- **P2**: `filter_utility_functions` 单元测试 (之前零覆盖)
- **P2**: `pytest-cov` 覆盖率工具配置
- **P3**: `CHANGELOG.md` 变更日志
- **P3**: `LICENSE` MIT 许可证文件
- **P3**: `.gitignore` 文件

### Changed
- `ScoreBasedForgetting` 默认权重改用系统常量 (`DEFAULT_IMPORTANCE_WEIGHT` 等)
- `ScoreBasedForgetting` 权重自动归一化，无需手动保证总和为 1
- `forget_ratio` 在 `max_keep <= 0` 时自动计算保留数量

## [2.24.0] - 2026-05-14

### Added
- 嵌入缓存 (TTLCache, 512 entries, 1hr TTL)
- `pydantic-settings` 环境变量配置支持
- `pyproject.toml` 现代打包配置
- 异步 `list_snapshots()` 和 `compare_snapshots()`

### Fixed
- `is_full()` 未调用 `expire()` 导致缓存永不过期
- `compare_snapshots` 对 list 格式记忆的 AttributeError
- `list_snapshots` 返回结果无序

## [2.23.0] - 2026-05-13

### Added
- FastAPI REST API (17 endpoints)
- gRPC 服务
- WebSocket 实时事件通知
- 多租户隔离
- TTL 过期管理
- Prometheus 监控指标
- OpenTelemetry 分布式追踪
- CLI 命令行工具
- 记忆快照管理
- 记忆导入/导出
- MkDocs 文档网站

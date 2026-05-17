"""
Agent Memory System 主类
整合所有模块，提供统一接口
"""

from typing import Any, Dict, List, Optional, Union, Callable
from contextlib import nullcontext as _nullcontext
import asyncio
import time
import importlib
import pkgutil
import weakref
import re
import gc
from cachetools import TTLCache
from contextlib import asynccontextmanager

# DEFECT-R10-002 [P0] 修复: 输入验证工具函数
def _validate_string_input(
    value: Any,
    field_name: str,
    max_length: int = 10000,
    allow_empty: bool = False,
    allow_none: bool = False
) -> str:
    """
    验证字符串输入

    DEFECT-R10-002 [P0] 修复:
        - 类型检查
        - 长度验证
        - 危险字符过滤
        - 空值检查

    Args:
        value: 输入值
        field_name: 字段名称（用于错误信息）
        max_length: 最大长度
        allow_empty: 是否允许空字符串
        allow_none: 是否允许None

    Returns:
        验证后的字符串

    Raises:
        TypeError: 类型错误
        ValueError: 值错误
    """
    if value is None:
        if allow_none:
            return ""
        raise TypeError(f"{field_name} cannot be None")

    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string, got {type(value).__name__}")

    if not allow_empty and not value.strip():
        raise ValueError(f"{field_name} cannot be empty or whitespace only")

    if len(value) > max_length:
        raise ValueError(f"{field_name} exceeds maximum length of {max_length} characters")

    # DEFECT-R10-002 [P0] 修复: 危险字符过滤（防止注入攻击）
    # 过滤控制字符（保留换行和制表符）
    dangerous_chars = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')
    cleaned = dangerous_chars.sub('', value)

    if cleaned != value:
        logger.warning(f"{field_name} contained dangerous characters that were removed")

    return cleaned


def _validate_integer_input(
    value: Any,
    field_name: str,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    allow_none: bool = False
) -> int:
    """
    验证整数输入

    DEFECT-R10-003 [P0] 修复:
        - 类型检查
        - 边界验证
        - 早期验证

    Args:
        value: 输入值
        field_name: 字段名称
        min_value: 最小值
        max_value: 最大值
        allow_none: 是否允许None

    Returns:
        验证后的整数

    Raises:
        TypeError: 类型错误
        ValueError: 值错误
    """
    if value is None:
        if allow_none:
            return 0
        raise TypeError(f"{field_name} cannot be None")

    # DEFECT-R10-003 [P0] 修复: 类型检查
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer, got boolean")

    if not isinstance(value, int):
        try:
            value = int(value)
        except (TypeError, ValueError):
            raise TypeError(f"{field_name} must be an integer, got {type(value).__name__}")

    # DEFECT-R10-003 [P0] 修复: 边界验证
    if min_value is not None and value < min_value:
        raise ValueError(f"{field_name} must be >= {min_value}, got {value}")

    if max_value is not None and value > max_value:
        raise ValueError(f"{field_name} must be <= {max_value}, got {value}")

    return value


def _validate_metadata(metadata: Any, field_name: str = "metadata") -> Dict[str, Any]:
    """
    验证元数据字典

    DEFECT-R10-002 [P0] 修复: 元数据验证

    Args:
        metadata: 元数据值
        field_name: 字段名称

    Returns:
        验证后的元数据字典
    """
    if metadata is None:
        return {}

    if not isinstance(metadata, dict):
        raise TypeError(f"{field_name} must be a dictionary, got {type(metadata).__name__}")

    # 验证元数据键值对
    validated = {}
    for key, value in metadata.items():
        # 验证键
        if not isinstance(key, str):
            raise TypeError(f"{field_name} keys must be strings, got {type(key).__name__}")

        # 过滤危险字符
        safe_key = re.sub(r'[\x00-\x1F\x7F]', '', key)

        # 验证值类型（允许基本类型）
        if value is not None and not isinstance(value, (str, int, float, bool, list, dict)):
            raise TypeError(f"{field_name}['{key}'] has unsupported type: {type(value).__name__}")

        validated[safe_key] = value

    return validated


def _validate_memory_type(memory_type: str) -> str:
    """
    验证记忆类型

    DEFECT-R10-002 [P0] 修复: 记忆类型验证

    Args:
        memory_type: 记忆类型字符串

    Returns:
        验证后的记忆类型
    """
    valid_types = {"short_term", "episodic", "semantic", "auto"}

    if not isinstance(memory_type, str):
        raise TypeError(f"memory_type must be a string, got {type(memory_type).__name__}")

    memory_type = memory_type.lower().strip()

    if memory_type not in valid_types:
        raise ValueError(f"Invalid memory_type: '{memory_type}'. Valid types: {valid_types}")

    return memory_type


def _validate_importance(importance: str) -> str:
    """
    验证重要性级别

    DEFECT-R10-002 [P0] 修复: 重要性级别验证

    Args:
        importance: 重要性级别字符串

    Returns:
        验证后的重要性级别
    """
    valid_levels = {"critical", "high", "medium", "low", "trivial", "auto"}

    if not isinstance(importance, str):
        raise TypeError(f"importance must be a string, got {type(importance).__name__}")

    importance = importance.lower().strip()

    if importance not in valid_levels:
        raise ValueError(f"Invalid importance: '{importance}'. Valid levels: {valid_levels}")

    return importance

from memory_system.core_abstractions.memory_data_models import (
    MemoryItem,
    MemoryType,
    ImportanceLevel,
    MemoryStats,
    RememberResponse
)
from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy,
    ConsolidationStrategy,
    ForgettingStrategy
)
from memory_system.configuration_settings.memory_system_configuration import MemoryConfig
from memory_system.working_memory_cache import ShortTermMemory
from memory_system.core_abstractions.component_registry_manager import (
    _storage_registry,
    _embedding_registry,
    _retrieval_registry,
    _consolidation_registry,
    _forgetting_registry,
)
from memory_system.monitoring_metrics_collector import MemoryMetrics
from memory_system.structured_logging_config import get_logger
from memory_system.telemetry_tracing_manager import TelemetryTracingManager
from memory_system.memory_ttl_expiration_manager import MemoryTTLExpirationManager, TTLConfig
from memory_system.core_abstractions.system_constants_definitions import (
    DEFAULT_EMBEDDING_DIMENSION,
    DEFAULT_CONSOLIDATION_THRESHOLD,
    DEFAULT_MAX_MEMORIES,
    VERSION,
    MAX_CONTENT_LENGTH,
    MAX_QUERY_LENGTH,
    MAX_BATCH_SIZE,
    EMBEDDING_CACHE_MAX_SIZE,
    EMBEDDING_CACHE_TTL_SECONDS,
)

logger = get_logger(__name__)

# DEFECT-R2-001 [P1] 修复: 全局记忆对象弱引用缓存，用于跟踪和清理记忆对象
_memory_object_cache: weakref.WeakSet = weakref.WeakSet()

# DEFECT-R2-001 [P1] 修复: 事件监听器注册表，用于自动清理
_event_listener_registry: Dict[str, List[callable]] = {}

# DEFECT-R2-001 [P1] 修复: 资源跟踪注册表
_resource_registry: Dict[int, Dict[str, Any]] = {}

# DEFECT-R2-001 [P1] 修复: 自动释放配置
AUTO_RELEASE_CONFIG = {
    "enabled": True,
    "check_interval_seconds": 60,
    "idle_timeout_seconds": 300,  # 5分钟无活动自动释放
    "memory_threshold_percent": 80,
}


class CachingEmbeddingProxy:
    """嵌入提供者缓存代理
    
    包装真实嵌入提供者，添加 TTL 缓存层。
    用于 recall() 路径中的嵌入缓存，避免重复计算。
    """
    
    def __init__(self, embedding: EmbeddingProvider, cache: TTLCache):
        self._embedding = embedding
        self._cache = cache
    
    @property
    def dimension(self) -> int:
        return self._embedding.dimension
    
    async def embed(self, text: str) -> List[float]:
        if text in self._cache:
            return self._cache[text]
        result = await self._embedding.embed(text)
        self._cache[text] = result
        return result
    
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        results = []
        cache_misses = []
        for text in texts:
            if text in self._cache:
                results.append(self._cache[text])
            else:
                cache_misses.append(text)
        if cache_misses:
            batch_results = await self._embedding.embed_batch(cache_misses)
            for text, vec in zip(cache_misses, batch_results):
                self._cache[text] = vec
                results.append(vec)
        return results
    
    async def similarity(self, vec1: List[float], vec2: List[float]) -> float:
        return await self._embedding.similarity(vec1, vec2)


class MemorySystem:
    """
    Agent 长短期记忆系统

    整合存储层、嵌入层、检索层、整合层
    提供简洁的统一接口

    使用示例:
        >>> config = MemoryConfig()
        >>> memory = MemorySystem.from_config(config)
        >>> memory.remember("用户喜欢Python", importance="high")
        >>> results = memory.recall("用户偏好")
    """

    _registrations_loaded = False

    def __init__(
        self,
        storage: StorageBackend,
        embedding: EmbeddingProvider,
        retrieval: RetrievalStrategy,
        short_term: ShortTermMemory,
        consolidation: Optional[ConsolidationStrategy] = None,
        forgetting: Optional[ForgettingStrategy] = None,
        config: Optional[MemoryConfig] = None,
        metrics: Optional[MemoryMetrics] = None,
        tracing: Optional[TelemetryTracingManager] = None,
        ttl_manager: Optional[MemoryTTLExpirationManager] = None,
        importance_scorer: Optional['ImportanceScorer'] = None,
    ):
        """
        初始化记忆系统

        Args:
            storage: 存储后端
            embedding: 嵌入提供者
            retrieval: 检索策略
            short_term: 短期记忆
            consolidation: 整合策略
            forgetting: 遗忘策略
            config: 配置
            metrics: 监控指标
            tracing: 分布式追踪管理器
        """
        self.storage = storage
        self.embedding = embedding
        self.retrieval = retrieval
        self.short_term = short_term
        self.consolidation = consolidation
        self.forgetting = forgetting
        self.config = config or MemoryConfig()
        self.metrics = metrics or MemoryMetrics()
        self.tracing = tracing
        self.ttl_manager = ttl_manager
        self.importance_scorer = importance_scorer

        # 嵌入缓存
        self._embedding_cache: TTLCache = TTLCache(
            maxsize=EMBEDDING_CACHE_MAX_SIZE, 
            ttl=EMBEDDING_CACHE_TTL_SECONDS
        )

        # 统计信息
        self._stats = {
            "total_added": 0,
            "total_retrieved": 0,
            "start_time": time.time()
        }
        self._stats_lock = asyncio.Lock()  # 线程安全锁

        # DEFECT-R2-001 [P1] 修复: 注册到弱引用缓存，允许垃圾回收
        _memory_object_cache.add(self)

        # DEFECT-R2-001 [P1] 修复: 初始化事件监听器列表
        self._event_listeners: List[callable] = []

        # DEFECT-R2-001 [P1] 修复: 启动定期清理任务
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_interval = 300  # 每5分钟清理一次
        self._start_cleanup_task()

        # DEFECT-R2-001 [P1] 修复: 资源自动释放机制
        self._last_activity_time = time.time()
        self._auto_release_task: Optional[asyncio.Task] = None
        self._resource_lock = asyncio.Lock()
        self._is_released = False
        self._active_operations = 0
        self._start_auto_release_task()

        # DEFECT-R2-001 [P1] 修复: 注册到资源注册表
        self._resource_id = id(self)
        _resource_registry[self._resource_id] = {
            "instance": weakref.ref(self),
            "created_at": time.time(),
            "last_activity": time.time(),
            "operation_count": 0,
        }

        # 设置系统信息
        self.metrics.set_system_info(
            version=VERSION,
            backend=getattr(config.storage, 'backend', 'unknown') if config else 'unknown',
            embedding=getattr(config.embedding, 'provider', 'unknown') if config else 'unknown'
        )

    @classmethod
    def from_config(cls, config: MemoryConfig) -> "MemorySystem":
        """
        从配置创建记忆系统

        Args:
            config: 配置对象

        Returns:
            MemorySystem实例
        """
        return MemorySystemBuilder.from_config(config).build()

    @staticmethod
    def _ensure_registrations():
        """确保所有组件已注册（通过自动发现子包中的模块并导入触发装饰器）"""
        if MemorySystem._registrations_loaded:
            return

        # 需要扫描的子包列表
        subpackages = [
            "memory_system.storage_backends",
            "memory_system.embedding_providers",
            "memory_system.retrieval_strategies",
            "memory_system.consolidation_strategies",
            "memory_system.forgetting_strategies",
            "memory_system.importance_scorers",
        ]

        for package_name in subpackages:
            try:
                package = importlib.import_module(package_name)
                package_path = getattr(package, "__path__", None)
                if package_path is None:
                    logger.warning(f"跳过非包模块: {package_name}")
                    continue

                for importer, module_name, is_pkg in pkgutil.iter_modules(package_path):
                    if not is_pkg:
                        full_module_name = f"{package_name}.{module_name}"
                        try:
                            importlib.import_module(full_module_name)
                        except ImportError as e:
                            logger.warning(f"导入模块失败: {full_module_name}，错误: {e}")

            except ImportError as e:
                logger.warning(f"导入子包失败: {package_name}，错误: {e}")

        MemorySystem._registrations_loaded = True

    async def remember(
        self,
        content: str,
        memory_type: str = "auto",
        importance: str = "medium",
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        存储记忆

        Args:
            content: 记忆内容
            memory_type: 记忆类型 (short_term/episodic/semantic/auto)
            importance: 重要性 (critical/high/medium/low/trivial)
            metadata: 附加元数据

        Returns:
            记忆ID

        Raises:
            TypeError: 输入类型错误
            ValueError: 输入值无效
        """
        # DEFECT-R10-002 [P0] 修复: 加强输入验证
        # 验证内容
        content = _validate_string_input(
            content,
            field_name="content",
            max_length=MAX_CONTENT_LENGTH,
            allow_empty=False,
            allow_none=False
        )

        # DEFECT-R10-002 [P0] 修复: 验证记忆类型
        memory_type = _validate_memory_type(memory_type)

        # DEFECT-R10-002 [P0] 修复: 验证重要性级别
        importance = _validate_importance(importance)

        # DEFECT-R10-002 [P0] 修复: 验证元数据
        metadata = _validate_metadata(metadata)

        if importance.upper() == "AUTO":
            if self.importance_scorer:
                importance_level, confidence = await self.importance_scorer.score(content)
                logger.info(f"自动评估重要性: {importance_level.name} (置信度: {confidence:.3f})")
                if metadata is None:
                    metadata = {}
                metadata["importance_confidence"] = confidence
            else:
                # 无评估器时的降级策略
                importance_level = ImportanceLevel.MEDIUM
                logger.debug("未配置重要性评估器，使用默认 MEDIUM 级别")
        else:
            try:
                importance_level = ImportanceLevel[importance.upper()]
            except KeyError:
                raise ValueError(f"无效的重要性级别: {importance}，可选值: {[e.name for e in ImportanceLevel]}")

        # 自动选择记忆类型
        if memory_type == "auto":
            if importance_level.value >= ImportanceLevel.HIGH.value:
                memory_type = "semantic"
            else:
                memory_type = "short_term"

        with self.tracing.start_as_current_span(
            "memory_system.remember",
            attributes={"memory.type": memory_type, "memory.importance": importance}
        ) if self.tracing else _nullcontext():
            # 存储到短期记忆
            if memory_type == "short_term":
                item = await self.short_term.add(
                    content=content,
                    importance=importance_level,
                    metadata=metadata
                )
                async with self._stats_lock:
                    self._stats["total_added"] += 1
                return item.memory_id

            # 生成嵌入
            cache_key = content
            if cache_key in self._embedding_cache:
                embedding_vector = self._embedding_cache[cache_key]
            else:
                embedding_vector = await self.embedding.embed(content)
                self._embedding_cache[cache_key] = embedding_vector

            # 创建记忆项
            try:
                item = MemoryItem(
                    content=content,
                    memory_type=MemoryType(memory_type),
                    importance=importance_level,
                    embedding=embedding_vector,
                    metadata=metadata or {}
                )
            except ValueError as e:
                raise ValueError(f"无效的记忆类型: {memory_type}") from e

            # 存储到长期存储
            start_time = time.time()
            memory_id = await self.storage.add(item)
            duration = time.time() - start_time

            # 更新统计
            async with self._stats_lock:
                self._stats["total_added"] += 1

            # 记录指标
            self.metrics.record_remember(memory_type, importance, duration)

            # 检查是否需要整合
            await self._check_consolidation()

            # 检查是否需要遗忘
            await self._check_forgetting()

            return memory_id

    async def remember_detailed(
        self,
        content: str,
        memory_type: str = "auto",
        importance: str = "medium",
        metadata: Optional[Dict[str, Any]] = None
    ) -> RememberResponse:
        """存储记忆并返回详细信息

        Args:
            content: 记忆内容
            memory_type: 记忆类型 (short_term/episodic/semantic/auto)
            importance: 重要性 (critical/high/medium/low/trivial/auto)
            metadata: 附加元数据

        Returns:
            RememberResponse 包含记忆ID、类型、重要性、置信度等信息

        Raises:
            TypeError: 输入类型错误
            ValueError: 输入值无效
        """
        # DEFECT-R10-002 [P0] 修复: 加强输入验证
        # 验证内容
        content = _validate_string_input(
            content,
            field_name="content",
            max_length=MAX_CONTENT_LENGTH,
            allow_empty=False,
            allow_none=False
        )

        # DEFECT-R10-002 [P0] 修复: 验证记忆类型
        memory_type = _validate_memory_type(memory_type)

        # DEFECT-R10-002 [P0] 修复: 验证重要性级别
        importance = _validate_importance(importance)

        # DEFECT-R10-002 [P0] 修复: 验证元数据
        metadata = _validate_metadata(metadata)

        confidence = None
        if importance.upper() == "AUTO":
            if self.importance_scorer:
                importance_level, confidence = await self.importance_scorer.score(content)
                logger.info(f"自动评估重要性: {importance_level.name} (置信度: {confidence:.3f})")
                if metadata is None:
                    metadata = {}
                metadata["importance_confidence"] = confidence
            else:
                importance_level = ImportanceLevel.MEDIUM
                logger.debug("未配置重要性评估器，使用默认 MEDIUM 级别")
        else:
            try:
                importance_level = ImportanceLevel[importance.upper()]
            except KeyError:
                raise ValueError(f"无效的重要性级别: {importance}，可选值: {[e.name for e in ImportanceLevel]}")

        # 自动选择记忆类型
        actual_memory_type = memory_type
        if memory_type == "auto":
            if importance_level.value >= ImportanceLevel.HIGH.value:
                actual_memory_type = "semantic"
            else:
                actual_memory_type = "short_term"

        # 查找相关记忆推荐
        related_memories = None
        if self.importance_scorer is not None:
            try:
                recall_results = await self.recall(content, max_results=3)
                related = recall_results.get("combined", [])
                if related:
                    related_memories = [
                        {
                            "memory_id": r.get("memory", {}).get("memory_id"),
                            "content": r.get("memory", {}).get("content", "")[:100],
                            "score": r.get("score", 0),
                        }
                        for r in related
                    ]
            except Exception:
                related_memories = None

        # 执行存储
        memory_id = await self.remember(
            content=content,
            memory_type=memory_type,
            importance=importance if importance.upper() != "AUTO" else importance_level.name.lower(),
            metadata=metadata
        )

        return RememberResponse(
            memory_id=memory_id,
            memory_type=actual_memory_type,
            importance=importance_level.name.lower(),
            importance_confidence=confidence,
            is_new=True,
            related_memories=related_memories,
        )

    async def remember_batch(
        self,
        contents: List[str],
        memory_type: str = "auto",
        importance: str = "medium",
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        批量存储记忆（使用批量嵌入优化）

        Args:
            contents: 记忆内容列表
            memory_type: 记忆类型
            importance: 重要性
            metadata: 附加元数据（应用于所有记忆）

        Returns:
            记忆ID列表

        Raises:
            TypeError: 输入类型错误
            ValueError: 输入值无效
        """
        # DEFECT-R10-002 [P0] 修复: 验证 contents 列表
        if contents is None:
            raise TypeError("contents cannot be None")

        if not isinstance(contents, list):
            raise TypeError(f"contents must be a list, got {type(contents).__name__}")

        if len(contents) == 0:
            return []

        # DEFECT-R10-003 [P0] 修复: 边界条件验证
        if len(contents) > MAX_BATCH_SIZE:
            raise ValueError(f"Batch size exceeds maximum of {MAX_BATCH_SIZE}, got {len(contents)}")

        # DEFECT-R10-002 [P0] 修复: 验证记忆类型
        memory_type = _validate_memory_type(memory_type)

        # DEFECT-R10-002 [P0] 修复: 验证重要性级别
        importance = _validate_importance(importance)

        # DEFECT-R10-002 [P0] 修复: 验证元数据
        metadata = _validate_metadata(metadata)

        # DEFECT-R10-002 [P0] 修复: 验证列表中的每个内容
        validated_contents = []
        for i, content in enumerate(contents):
            try:
                validated = _validate_string_input(
                    content,
                    field_name=f"contents[{i}]",
                    max_length=MAX_CONTENT_LENGTH,
                    allow_empty=False,
                    allow_none=False
                )
                validated_contents.append(validated)
            except (TypeError, ValueError) as e:
                raise ValueError(f"Invalid content at index {i}: {e}") from e

        contents = validated_contents

        try:
            importance_level = ImportanceLevel[importance.upper()]
        except KeyError:
            raise ValueError(f"Invalid importance level: {importance}")

        with self.tracing.start_as_current_span(
            "memory_system.remember_batch",
            attributes={"batch.size": len(contents), "memory.importance": importance}
        ) if self.tracing else _nullcontext():
            # 分离短期记忆和长期记忆
            short_term_contents = []
            long_term_contents = []

            for content in contents:
                if content is None:
                    raise TypeError("remember_batch() 不接受 None 值，所有内容必须为字符串")
                if not isinstance(content, str):
                    raise TypeError(f"remember_batch() 内容必须是字符串，当前类型: {type(content).__name__}")
                if not content.strip():
                    continue

                actual_type = memory_type
                if memory_type == "auto":
                    if importance_level.value >= ImportanceLevel.HIGH.value:
                        actual_type = "semantic"
                    else:
                        actual_type = "short_term"

                if actual_type == "short_term":
                    short_term_contents.append(content)
                else:
                    long_term_contents.append(content)

            memory_ids = []
            start_time = time.time()

            # 批量处理短期记忆
            for content in short_term_contents:
                item = await self.short_term.add(content=content, importance=importance_level, metadata=metadata)
                memory_ids.append(item.memory_id)
                async with self._stats_lock:
                    self._stats["total_added"] += 1

            # 批量处理长期记忆（使用批量嵌入）
            if long_term_contents:
                # 批量生成嵌入（带缓存）
                embeddings = []
                contents_to_embed = []
                for content in long_term_contents:
                    if content in self._embedding_cache:
                        embeddings.append(self._embedding_cache[content])
                    else:
                        contents_to_embed.append(content)

                if contents_to_embed:
                    new_embeddings = await self.embedding.embed_batch(contents_to_embed)
                    for content, embedding in zip(contents_to_embed, new_embeddings):
                        self._embedding_cache[content] = embedding
                    embeddings.extend(new_embeddings)

                for content, embedding_vector in zip(long_term_contents, embeddings):
                    item = MemoryItem(
                        content=content,
                        memory_type=MemoryType.SEMANTIC if memory_type in ["semantic", "auto"] else MemoryType.EPISODIC,
                        importance=importance_level,
                        embedding=embedding_vector,
                        metadata=metadata or {}
                    )

                    memory_id = await self.storage.add(item)
                    memory_ids.append(memory_id)
                    async with self._stats_lock:
                        self._stats["total_added"] += 1

            duration = time.time() - start_time

            # 记录指标
            self.metrics.record_remember(memory_type, importance, duration)

            # 检查是否需要整合（批量后也需检查）
            await self._check_consolidation()

            # 检查是否需要遗忘（批量后也需检查）
            await self._check_forgetting()

            return memory_ids

    async def recall(
        self,
        query: str,
        max_results: int = 10,
        strategy: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        检索记忆

        Args:
            query: 查询内容
            max_results: 最大结果数
            strategy: 检索策略 (fast/adaptive/thorough)
            context: 上下文信息
                - time_range: (start, end) 时间范围

        Returns:
            检索结果

        Raises:
            TypeError: 输入类型错误
            ValueError: 输入值无效
        """
        # DEFECT-R10-002 [P0] 修复: 加强输入验证
        # 验证查询内容
        query = _validate_string_input(
            query,
            field_name="query",
            max_length=MAX_QUERY_LENGTH,
            allow_empty=False,
            allow_none=False
        )

        # DEFECT-R10-003 [P0] 修复: 边界条件验证
        max_results = _validate_integer_input(
            max_results,
            field_name="max_results",
            min_value=1,
            max_value=1000,  # 设置合理的上限
            allow_none=False
        )

        # DEFECT-R10-002 [P0] 修复: 验证策略
        if strategy is not None:
            valid_strategies = {"fast", "adaptive", "thorough"}
            strategy = _validate_string_input(
                strategy,
                field_name="strategy",
                max_length=50,
                allow_empty=False
            )
            if strategy not in valid_strategies:
                raise ValueError(f"Invalid strategy: '{strategy}'. Valid strategies: {valid_strategies}")

        # DEFECT-R10-002 [P0] 修复: 验证上下文
        if context is not None:
            context = _validate_metadata(context, field_name="context")

        with self.tracing.start_as_current_span(
            "memory_system.recall",
            attributes={"recall.max_results": max_results, "recall.query_length": len(query)}
        ) if self.tracing else _nullcontext():
            # 构建上下文
            retrieval_context = context or {}
            retrieval_context["short_term_memories"] = await self.short_term.get_all()
            
            # 策略覆盖：通过 context 传递给检索策略
            if strategy and strategy != getattr(self.retrieval, 'strategy', None):
                retrieval_context["strategy_override"] = strategy

            # 执行检索（使用缓存代理）
            start_time = time.time()
            caching_proxy = CachingEmbeddingProxy(self.embedding, self._embedding_cache)
            results = await self.retrieval.retrieve(
                query=query,
                storage=self.storage,
                embedding=caching_proxy,
                max_results=max_results,
                context=retrieval_context
            )
            duration = time.time() - start_time

            # 更新统计
            async with self._stats_lock:
                self._stats["total_retrieved"] += 1

            # 记录指标
            self.metrics.record_recall(duration)

            return results

    async def get_recent_context(self, n: int = 5) -> List[str]:
        """
        获取最近的对话上下文

        Args:
            n: 数量

        Returns:
            内容列表
        """
        memories = await self.short_term.get_recent(n)
        return [m.content for m in memories]

    async def get_stats(self) -> MemoryStats:
        """
        获取系统统计信息

        Returns:
            统计信息
        """
        episodic_count = await self.storage.count("episodic")
        semantic_count = await self.storage.count("semantic")
        short_term_count = len(await self.short_term.get_all())

        # 更新监控指标
        self.metrics.update_memory_count("episodic", episodic_count)
        self.metrics.update_memory_count("semantic", semantic_count)
        self.metrics.update_short_term_size(short_term_count)

        return MemoryStats(
            short_term_count=short_term_count,
            episodic_count=episodic_count,
            semantic_count=semantic_count,
            total_count=episodic_count + semantic_count + short_term_count,
            total_added=self._stats["total_added"],
            total_retrieved=self._stats["total_retrieved"],
            uptime_hours=(time.time() - self._stats["start_time"]) / 3600
        )

    async def consolidate(self):
        """手动触发记忆整合"""
        if not self.consolidation:
            logger.debug("未配置整合策略，跳过整合")
            return

        with self.tracing.start_as_current_span(
            "memory_system.consolidate"
        ) if self.tracing else _nullcontext():
            start_time = time.time()
            
            # 获取所有情景记忆
            all_memories = await self.storage.search_by_time_range(0, time.time())
            logger.debug(f"整合: 加载了 {len(all_memories)} 条记忆")

            # 检查是否需要整合
            should = await self.consolidation.should_consolidate(all_memories, {})
            if not should:
                logger.debug("记忆数量不足，跳过整合")
                return

            # 执行整合
            new_memories = await self.consolidation.consolidate(all_memories, self.embedding)

            # 存储新的语义记忆
            for new_memory in new_memories:
                await self.storage.add(new_memory)
                logger.info(f"整合生成新语义记忆: {new_memory.content[:50]}...")

            # 删除被整合的原始情景记忆
            deleted_count = 0
            for new_memory in new_memories:
                if "consolidated_from" in new_memory.metadata:
                    for old_id in new_memory.metadata["consolidated_from"]:
                        await self.storage.delete(old_id)
                        deleted_count += 1

            duration = time.time() - start_time
            logger.info(
                f"记忆整合完成: 生成 {len(new_memories)} 条语义记忆，"
                f"删除 {deleted_count} 条原始记忆，耗时 {duration:.3f}s"
            )

    async def clear_short_term(self):
        """清空短期记忆"""
        await self.short_term.clear()

    async def forget(self, memory_id: str) -> bool:
        """遗忘指定记忆"""
        success = await self.storage.delete(memory_id)
        if success:
            self._stats["total_added"] = max(0, self._stats["total_added"] - 1)
            logger.info(f"手动遗忘记忆: {memory_id}")
        return success
    
    async def forget_batch(self, memory_ids: List[str]) -> int:
        """批量遗忘记忆"""
        count = await self.storage.batch_delete(memory_ids)
        self._stats["total_added"] = max(0, self._stats["total_added"] - count)
        logger.info(f"批量遗忘记忆: 删除 {count} 条")
        return count
    
    async def forget_by_query(
        self,
        query: str,
        max_results: int = 10,
        min_similarity: float = 0.8
    ) -> int:
        """基于查询遗忘相似记忆"""
        results = await self.recall(query, max_results=max_results)
        to_delete = []
        for item in results.get("combined", []):
            if item.get("score", 0) >= min_similarity:
                mem_id = item.get("memory", {}).get("memory_id")
                if mem_id:
                    to_delete.append(mem_id)
        if to_delete:
            return await self.forget_batch(to_delete)
        return 0

    async def link_memories(
        self,
        source_id: str,
        target_id: str,
        relation: str = "related",
        weight: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        建立记忆间关联
        
        Args:
            source_id: 源记忆ID
            target_id: 目标记忆ID
            relation: 关联类型 (related/causal/temporal/hierarchical/contradictory/supplementary)
            weight: 关联强度 (0.0-1.0)
            metadata: 附加元数据
            
        Returns:
            是否成功
        """
        from memory_system.core_abstractions.memory_data_models import MemoryLink, LinkRelation
        try:
            link = MemoryLink(
                source_id=source_id,
                target_id=target_id,
                relation=LinkRelation(relation),
                weight=weight,
                metadata=metadata or {}
            )
            return await self.storage.add_link(link)
        except (ValueError, KeyError) as e:
            logger.warning(f"创建记忆关联失败: {e}")
            return False

    async def recall_with_associations(
        self,
        query: str,
        max_results: int = 10,
        association_depth: int = 1,
        strategy: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        关联感知检索：先检索，再展开关联记忆
        
        Args:
            query: 查询内容
            max_results: 最大结果数
            association_depth: 关联展开深度 (0=不展开, 1=一跳, 2=两跳)
            strategy: 检索策略
            context: 上下文
            
        Returns:
            包含直接结果和关联结果的检索响应
        """
        # 先执行常规检索
        direct_results = await self.recall(query, max_results, strategy, context)
        
        if association_depth <= 0:
            return {**direct_results, "associations": []}
        
        # 收集命中记忆的关联
        seen_ids = set()
        associations = []
        
        combined = direct_results.get("combined", [])
        for item in combined:
            memory_data = item.get("memory", {})
            memory_id = memory_data.get("memory_id")
            if not memory_id or memory_id in seen_ids:
                continue
            seen_ids.add(memory_id)
            
            try:
                linked = await self.storage.get_linked_memories(
                    memory_id, max_depth=association_depth
                )
                for linked_memory in linked:
                    if linked_memory.memory_id not in seen_ids:
                        seen_ids.add(linked_memory.memory_id)
                        associations.append({
                            "memory": linked_memory.model_dump(),
                            "association_depth": 1,
                            "linked_from": memory_id,
                        })
            except NotImplementedError:
                logger.debug("当前存储后端不支持图关联操作")
                break
        
        return {**direct_results, "associations": associations}

    def _start_cleanup_task(self):
        """DEFECT-R2-001 [P1] 修复: 启动定期清理任务"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        except RuntimeError:
            # 没有事件循环，稍后启动
            pass

    def _start_auto_release_task(self):
        """DEFECT-R2-001 [P1] 修复: 启动自动释放任务"""
        if not AUTO_RELEASE_CONFIG["enabled"]:
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                self._auto_release_task = asyncio.create_task(self._auto_release_loop())
        except RuntimeError:
            pass

    async def _auto_release_loop(self):
        """DEFECT-R2-001 [P1] 修复: 自动释放循环 - 检测空闲并释放资源"""
        while True:
            try:
                await asyncio.sleep(AUTO_RELEASE_CONFIG["check_interval_seconds"])

                async with self._resource_lock:
                    if self._is_released:
                        break

                    idle_time = time.time() - self._last_activity_time
                    has_active_ops = self._active_operations > 0

                    # 检查是否需要自动释放
                    should_release = (
                        idle_time > AUTO_RELEASE_CONFIG["idle_timeout_seconds"]
                        and not has_active_ops
                    )

                    if should_release:
                        logger.info(f"Auto-releasing idle memory system (idle for {idle_time:.0f}s)")
                        await self._release_idle_resources()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Auto-release task error: {e}")

    async def _release_idle_resources(self):
        """DEFECT-R2-001 [P1] 修复: 释放空闲资源"""
        try:
            # 清理嵌入缓存
            if hasattr(self._embedding_cache, 'clear'):
                self._embedding_cache.clear()

            # 清理短期记忆
            if self.short_term and hasattr(self.short_term, 'clear'):
                await self.short_term.clear()

            # 强制垃圾回收
            gc.collect()

            logger.debug("Idle resources released")
        except Exception as e:
            logger.warning(f"Failed to release idle resources: {e}")

    async def _cleanup_loop(self):
        """DEFECT-R2-001 [P1] 修复: 定期清理循环"""
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)
                await self._perform_cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Cleanup task error: {e}")

    async def _perform_cleanup(self):
        """DEFECT-R2-001 [P1] 修复: 执行清理操作"""
        # 清理过期的嵌入缓存
        if hasattr(self._embedding_cache, 'expired'):
            self._embedding_cache.expired()

        # 清理短期记忆中的过期项
        if self.short_term and hasattr(self.short_term, '_cleanup_expired'):
            await self.short_term._cleanup_expired()

        # DEFECT-R2-001: 清理资源注册表中的死引用
        self._cleanup_resource_registry()

        logger.debug("Memory system cleanup completed")

    def _cleanup_resource_registry(self):
        """DEFECT-R2-001 [P1] 修复: 清理资源注册表中的死引用"""
        global _resource_registry
        dead_ids = []
        for rid, info in _resource_registry.items():
            ref = info.get("instance")
            if ref is None or ref() is None:
                dead_ids.append(rid)
        for rid in dead_ids:
            del _resource_registry[rid]

    def _update_activity(self):
        """DEFECT-R2-001 [P1] 修复: 更新活动时间戳"""
        self._last_activity_time = time.time()
        if self._resource_id in _resource_registry:
            _resource_registry[self._resource_id]["last_activity"] = time.time()
            _resource_registry[self._resource_id]["operation_count"] += 1

    @asynccontextmanager
    async def _operation_context(self):
        """DEFECT-R2-001 [P1] 修复: 操作上下文管理器 - 跟踪活动操作"""
        async with self._resource_lock:
            self._active_operations += 1
            self._update_activity()
        try:
            yield
        finally:
            async with self._resource_lock:
                self._active_operations -= 1
                self._update_activity()

    def _register_event_listener(self, listener: callable):
        """DEFECT-R2-001 [P1] 修复: 注册事件监听器"""
        self._event_listeners.append(listener)
        # 同时注册到全局注册表
        listener_id = id(listener)
        if listener_id not in _event_listener_registry:
            _event_listener_registry[listener_id] = []
        _event_listener_registry[listener_id].append(listener)

    def _unregister_event_listener(self, listener: callable):
        """DEFECT-R2-001 [P1] 修复: 注销事件监听器"""
        if listener in self._event_listeners:
            self._event_listeners.remove(listener)
        # 从全局注册表移除
        listener_id = id(listener)
        if listener_id in _event_listener_registry:
            if listener in _event_listener_registry[listener_id]:
                _event_listener_registry[listener_id].remove(listener)

    def _clear_all_event_listeners(self):
        """DEFECT-R2-001 [P1] 修复: 清除所有事件监听器"""
        for listener in self._event_listeners[:]:
            self._unregister_event_listener(listener)
        self._event_listeners.clear()

    async def close(self):
        """关闭系统 - DEFECT-R2-001 [P1] 修复: 确保资源正确释放"""
        async with self._resource_lock:
            if self._is_released:
                return
            self._is_released = True

        # DEFECT-R2-001 [P1] 修复: 停止自动释放任务
        if self._auto_release_task:
            self._auto_release_task.cancel()
            try:
                await self._auto_release_task
            except asyncio.CancelledError:
                pass
            self._auto_release_task = None

        # DEFECT-R2-001 [P1] 修复: 停止清理任务
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

        # DEFECT-R2-001 [P1] 修复: 清除所有事件监听器
        self._clear_all_event_listeners()

        # DEFECT-R2-001 [P1] 修复: 从弱引用缓存中移除
        if self in _memory_object_cache:
            _memory_object_cache.discard(self)

        # DEFECT-R2-001 [P1] 修复: 从资源注册表移除
        if self._resource_id in _resource_registry:
            del _resource_registry[self._resource_id]

        # DEFECT-R2-001 [P1] 修复: 清理嵌入缓存
        self._embedding_cache.clear()

        # DEFECT-R2-001 [P1] 修复: 强制垃圾回收
        gc.collect()

        if self.ttl_manager:
            await self.ttl_manager.stop()
        await self.storage.close()
        if self.tracing:
            self.tracing.shutdown()

    async def __aenter__(self):
        """异步上下文管理器入口"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()
        return False

    async def _check_consolidation(self):
        """检查是否需要整合"""
        if not self.consolidation:
            return

        total_count = await self.storage.count("episodic") + await self.storage.count("semantic")
        threshold = getattr(self.config.consolidation, 'consolidation_threshold', DEFAULT_CONSOLIDATION_THRESHOLD)
        if total_count >= threshold:
            logger.debug("记忆数量达到整合阈值，触发整合")
            await self.consolidate()

    async def _check_forgetting(self):
        """检查是否需要遗忘"""
        if not self.forgetting:
            return

        max_memories = getattr(self.config.storage, 'max_memories', DEFAULT_MAX_MEMORIES)
        total_count = await self.storage.count("episodic") + await self.storage.count("semantic")
        if total_count < max_memories:
            return

        logger.info(f"记忆数量 ({total_count}) 达到上限 ({max_memories})，触发遗忘")
        start_time = time.time()
        
        all_memories = await self.storage.search_by_time_range(0, time.time())
        if all_memories:
            to_forget = await self.forgetting.select_for_forgetting(
                all_memories, max_memories
            )
            for memory_id in to_forget:
                await self.storage.delete(memory_id)
            duration = time.time() - start_time
            logger.info(
                f"遗忘完成: 删除 {len(to_forget)} 条记忆，"
                f"剩余 {total_count - len(to_forget)} 条，耗时 {duration:.3f}s"
            )


class MemorySystemBuilder:
    """
    MemorySystem 构建器

    使用 Builder 模式解决构造函数参数过多的问题

    示例:
        >>> memory = MemorySystemBuilder()
        ...     .with_storage(storage)
        ...     .with_embedding(embedding)
        ...     .build()

    或者使用配置构建:
        >>> memory = MemorySystemBuilder.from_config(config).build()
    """

    def __init__(self):
        """初始化构建器，所有组件默认为 None"""
        self._storage: Optional[StorageBackend] = None
        self._embedding: Optional[EmbeddingProvider] = None
        self._retrieval: Optional[RetrievalStrategy] = None
        self._short_term: Optional[ShortTermMemory] = None
        self._consolidation: Optional[ConsolidationStrategy] = None
        self._forgetting: Optional[ForgettingStrategy] = None
        self._config: Optional[MemoryConfig] = None
        self._metrics: Optional[MemoryMetrics] = None
        self._tracing: Optional[TelemetryTracingManager] = None
        self._ttl_manager: Optional[MemoryTTLExpirationManager] = None
        self._importance_scorer: Optional['ImportanceScorer'] = None

    def with_storage(self, storage: StorageBackend) -> "MemorySystemBuilder":
        """
        设置存储后端

        Args:
            storage: 存储后端实例

        Returns:
            self，支持链式调用
        """
        self._storage = storage
        return self

    def with_embedding(self, embedding: EmbeddingProvider) -> "MemorySystemBuilder":
        """
        设置嵌入提供者

        Args:
            embedding: 嵌入提供者实例

        Returns:
            self，支持链式调用
        """
        self._embedding = embedding
        return self

    def with_retrieval(self, retrieval: RetrievalStrategy) -> "MemorySystemBuilder":
        """
        设置检索策略

        Args:
            retrieval: 检索策略实例

        Returns:
            self，支持链式调用
        """
        self._retrieval = retrieval
        return self

    def with_short_term(self, short_term: ShortTermMemory) -> "MemorySystemBuilder":
        """
        设置短期记忆

        Args:
            short_term: 短期记忆实例

        Returns:
            self，支持链式调用
        """
        self._short_term = short_term
        return self

    def with_consolidation(self, consolidation: ConsolidationStrategy) -> "MemorySystemBuilder":
        """
        设置整合策略

        Args:
            consolidation: 整合策略实例

        Returns:
            self，支持链式调用
        """
        self._consolidation = consolidation
        return self

    def with_forgetting(self, forgetting: ForgettingStrategy) -> "MemorySystemBuilder":
        """
        设置遗忘策略

        Args:
            forgetting: 遗忘策略实例

        Returns:
            self，支持链式调用
        """
        self._forgetting = forgetting
        return self

    def with_config(self, config: MemoryConfig) -> "MemorySystemBuilder":
        """
        设置配置

        Args:
            config: 配置实例

        Returns:
            self，支持链式调用
        """
        self._config = config
        return self

    def with_metrics(self, metrics: MemoryMetrics) -> "MemorySystemBuilder":
        """
        设置监控指标

        Args:
            metrics: 监控指标实例

        Returns:
            self，支持链式调用
        """
        self._metrics = metrics
        return self

    def with_tracing(self, tracing: TelemetryTracingManager) -> "MemorySystemBuilder":
        """
        设置分布式追踪

        Args:
            tracing: 分布式追踪管理器实例

        Returns:
            self，支持链式调用
        """
        self._tracing = tracing
        return self

    def with_ttl_manager(self, ttl_manager: MemoryTTLExpirationManager) -> "MemorySystemBuilder":
        """
        设置 TTL 过期管理器

        Args:
            ttl_manager: TTL 过期管理器实例

        Returns:
            self，支持链式调用
        """
        self._ttl_manager = ttl_manager
        return self

    def with_importance_scorer(self, importance_scorer: 'ImportanceScorer') -> "MemorySystemBuilder":
        """
        设置重要性评估器

        Args:
            importance_scorer: 重要性评估器实例

        Returns:
            self，支持链式调用
        """
        self._importance_scorer = importance_scorer
        return self

    def build(self) -> MemorySystem:
        """
        构建 MemorySystem 实例

        Returns:
            MemorySystem 实例

        Raises:
            ValueError: 如果必需组件未设置
        """
        # 验证必需组件
        if self._storage is None:
            raise ValueError("StorageBackend is required")
        if self._embedding is None:
            raise ValueError("EmbeddingProvider is required")
        if self._retrieval is None:
            raise ValueError("RetrievalStrategy is required")
        if self._short_term is None:
            raise ValueError("ShortTermMemory is required")

        instance = MemorySystem(
            storage=self._storage,
            embedding=self._embedding,
            retrieval=self._retrieval,
            short_term=self._short_term,
            consolidation=self._consolidation,
            forgetting=self._forgetting,
            config=self._config,
            metrics=self._metrics,
            tracing=self._tracing,
            ttl_manager=self._ttl_manager,
            importance_scorer=self._importance_scorer
        )

        # 启动 TTL 清理任务
        # TTL manager will be started lazily on first use or via start() call
        # to avoid requiring an event loop at construction time
        if self._ttl_manager:
            pass  # TTL manager stored on instance; start it via start() or first use

        return instance

    @classmethod
    def from_config(cls, config: MemoryConfig) -> "MemorySystemBuilder":
        """
        从配置创建构建器

        此方法会创建所有必要的组件并设置到构建器中，
        但不会调用 build()，允许用户在构建前进一步自定义。

        Args:
            config: 配置对象

        Returns:
            配置好的 MemorySystemBuilder 实例
        """
        # 确保各组件已注册（通过导入触发装饰器）
        if not MemorySystem._registrations_loaded:
            MemorySystem._ensure_registrations()

        builder = cls()
        builder._config = config

        # 创建嵌入提供者
        embedding_cls = _embedding_registry.get(config.embedding.provider)
        if embedding_cls is None:
            raise ValueError(f"未知的嵌入提供者: {config.embedding.provider}，已注册: {list(_embedding_registry.keys())}")
        builder._embedding = embedding_cls.from_config(config.embedding)

        # 获取实际嵌入维度
        embedding_dim = getattr(builder._embedding, 'dimension', None) or DEFAULT_EMBEDDING_DIMENSION

        # 创建存储后端
        storage_cls = _storage_registry.get(config.storage.backend)
        if storage_cls is None:
            raise ValueError(f"未知的存储后端: {config.storage.backend}，已注册: {list(_storage_registry.keys())}")
        builder._storage = storage_cls.from_config(config.storage, embedding_dim=embedding_dim)

        # 创建检索策略
        retrieval_cls = _retrieval_registry.get(config.retrieval.strategy)
        if retrieval_cls is None:
            raise ValueError(f"未知的检索策略: {config.retrieval.strategy}，已注册: {list(_retrieval_registry.keys())}")
        builder._retrieval = retrieval_cls()

        # 创建短期记忆
        builder._short_term = ShortTermMemory(
            capacity=config.short_term.capacity,
            ttl_seconds=config.short_term.ttl_seconds
        )

        # 创建整合策略
        if config.consolidation.enabled:
            consolidation_cls = _consolidation_registry.get(config.consolidation.strategy)
            if consolidation_cls:
                builder._consolidation = consolidation_cls()

        # 创建遗忘策略
        if config.forgetting.enabled:
            forgetting_cls = _forgetting_registry.get(config.forgetting.strategy)
            if forgetting_cls:
                builder._forgetting = forgetting_cls(forget_ratio=config.forgetting.forget_ratio)

        # 创建分布式追踪
        if config.telemetry.enabled:
            tracing = TelemetryTracingManager(
                service_name=config.telemetry.service_name,
                enabled=True,
                exporter_type=config.telemetry.exporter_type,
                otlp_endpoint=config.telemetry.otlp_endpoint,
            )
            tracing.setup()
            builder._tracing = tracing

        # 创建 TTL 管理器
        if hasattr(config, 'ttl') and config.ttl.enabled:
            ttl_config = TTLConfig(
                enabled=config.ttl.enabled,
                default_ttl_days=config.ttl.default_ttl_days,
                check_interval_hours=config.ttl.check_interval_hours,
                max_delete_per_run=config.ttl.max_delete_per_run,
                ttl_by_importance=config.ttl.ttl_by_importance
            )
            builder._ttl_manager = MemoryTTLExpirationManager(builder._storage, config=ttl_config)

        return builder

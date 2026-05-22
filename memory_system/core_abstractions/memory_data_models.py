"""
核心数据模型
使用Pydantic进行数据验证和序列化
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import time
import uuid
import threading
import logging

logger = logging.getLogger(__name__)

# D05 [P1 Integration] 修复: pydantic 作为可选依赖
try:
    from pydantic import BaseModel, Field, ConfigDict
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = None
    Field = None
    ConfigDict = None

    # D05 [P1 Integration] 修复: 提供后备实现，使模块在无 pydantic 时仍可导入
    class _FieldDescriptor:
        """Field 后备描述符 - 记录字段元数据"""
        def __init__(self, default=..., default_factory=None, description=None):
            self.default = default
            self.default_factory = default_factory
            self.description = description

        def __set_name__(self, owner, name):
            self.name = name

        def __get__(self, obj, objtype=None):
            if obj is None:
                return self
            return obj.__dict__.get(self.name, self._get_default())

        def __set__(self, obj, value):
            obj.__dict__[self.name] = value

        def _get_default(self):
            if self.default is not ...:
                return self.default
            if self.default_factory is not None:
                return self.default_factory()
            return None

    def Field(default=..., default_factory=None, description=None, **kwargs):
        """Field 后备实现"""
        return _FieldDescriptor(default=default, default_factory=default_factory, description=description)

    def ConfigDict(**kwargs):
        """ConfigDict 后备实现 - 返回一个空 dict，作为类属性存储"""
        return {}

    class _FallbackBaseModel:
        """BaseModel 后备实现 - 提供基本的 model_dump 支持"""
        model_config = {}

        def model_dump(self):
            result = {}
            for k, v in self.__dict__.items():
                if not k.startswith('_'):
                    val = v
                    # 处理枚举值
                    if hasattr(val, 'value'):
                        val = val.value
                    result[k] = val
            return result

        def __init__(self, **kwargs):
            # 收集所有 _FieldDescriptor 字段
            for cls in type(self).__mro__:
                for name, value in vars(cls).items():
                    if isinstance(value, _FieldDescriptor) and name not in kwargs:
                        kwargs[name] = value._get_default()
            self.__dict__.update(kwargs)

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)

    BaseModel = _FallbackBaseModel

from memory_system.core_abstractions.mathematical_utility_functions import exp


# DEFECT-006 [P1] 修复: 逻辑时钟实现，避免物理时钟漂移问题
class LogicalClock:
    """
    逻辑时钟实现 (Lamport Timestamp)
    用于替代物理时间戳，避免分布式系统中的时钟漂移问题
    """
    def __init__(self):
        self._counter: int = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """递增本地逻辑时钟"""
        with self._lock:
            self._counter += 1
            return self._counter

    def update(self, received_time: int) -> int:
        """根据接收到的逻辑时钟更新本地时钟"""
        with self._lock:
            self._counter = max(self._counter, received_time) + 1
            return self._counter

    def get_time(self) -> int:
        """获取当前逻辑时钟值"""
        with self._lock:
            return self._counter


# DEFECT-006 [P1] 修复: 混合时钟 - 结合物理时间和逻辑时钟
class HybridClock:
    """
    混合时钟实现
    结合物理时间(毫秒)和逻辑计数器，提供全序关系
    格式: (physical_time_ms, logical_counter)
    """
    def __init__(self):
        self._physical_time: int = 0
        self._logical_counter: int = 0
        self._lock = threading.Lock()

    def now(self) -> tuple:
        """获取当前混合时钟时间"""
        with self._lock:
            current_physical = int(time.time() * 1000)
            if current_physical > self._physical_time:
                self._physical_time = current_physical
                self._logical_counter = 0
            else:
                self._logical_counter += 1
            return (self._physical_time, self._logical_counter)

    def update(self, received_time: tuple) -> tuple:
        """根据接收到的混合时钟更新本地时钟"""
        with self._lock:
            current_physical = int(time.time() * 1000)
            recv_physical, recv_logical = received_time

            if recv_physical > self._physical_time:
                self._physical_time = recv_physical
                self._logical_counter = recv_logical + 1
            elif recv_physical == self._physical_time:
                self._logical_counter = max(self._logical_counter, recv_logical) + 1
            else:
                if current_physical > self._physical_time:
                    self._physical_time = current_physical
                    self._logical_counter = 0
                else:
                    self._logical_counter += 1

            return (self._physical_time, self._logical_counter)

    @staticmethod
    def compare(t1: tuple, t2: tuple) -> int:
        """比较两个混合时钟时间，返回 -1, 0, 1"""
        if t1[0] < t2[0]:
            return -1
        elif t1[0] > t2[0]:
            return 1
        else:
            if t1[1] < t2[1]:
                return -1
            elif t1[1] > t2[1]:
                return 1
            else:
                return 0


# 全局混合时钟实例
_global_hybrid_clock = HybridClock()


def get_hybrid_timestamp() -> tuple:
    """DEFECT-006 [P1] 修复: 获取混合时间戳"""
    return _global_hybrid_clock.now()


def compare_hybrid_timestamps(t1: tuple, t2: tuple) -> int:
    """DEFECT-006 [P1] 修复: 比较两个混合时间戳"""
    return HybridClock.compare(t1, t2)


class MemoryType(str, Enum):
    """记忆类型"""
    SHORT_TERM = "short_term"
    EPISODIC = "episodic"
    SEMANTIC = "semantic"


class ImportanceLevel(int, Enum):
    """重要性等级 1-5"""
    TRIVIAL = 1
    LOW = 2
    MEDIUM = 3
    HIGH = 4
    CRITICAL = 5


class MemoryItem(BaseModel):
    """
    记忆项数据模型 - DEFECT-006 [P1] 修复: 支持混合时间戳

    Attributes:
        content: 记忆内容
        memory_type: 记忆类型
        importance: 重要性等级
        timestamp: 创建时间戳 (物理时间，向后兼容)
        hybrid_timestamp: 混合时间戳 (逻辑时钟，避免时钟漂移)
        access_count: 访问次数
        last_access: 最后访问时间
        embedding: 向量嵌入 (可选)
        metadata: 附加元数据
        memory_id: 唯一标识符
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    content: str = Field(..., description="记忆内容")
    memory_type: MemoryType = Field(default=MemoryType.SHORT_TERM)
    importance: ImportanceLevel = Field(default=ImportanceLevel.MEDIUM)
    timestamp: float = Field(default_factory=time.time)
    # DEFECT-006 [P1] 修复: 添加混合时间戳字段
    hybrid_timestamp: tuple = Field(default_factory=get_hybrid_timestamp)
    access_count: int = Field(default=0)
    last_access: float = Field(default_factory=time.time)
    # DEFECT-006 [P1] 修复: 添加混合时间戳用于最后访问
    last_access_hybrid: tuple = Field(default_factory=get_hybrid_timestamp)
    embedding: Optional[List[float]] = Field(default=None)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    memory_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    
    def calculate_relevance_score(
        self,
        current_time: Optional[float] = None,
        short_term_half_life: float = 0.5,
        long_term_half_life: float = 168.0,
        importance_weight: float = 0.4,
        recency_weight: float = 0.4,
        access_weight: float = 0.2
    ) -> float:
        """
        计算记忆的相关性分数
        
        基于：重要性 + 时效性 + 访问频率，权重可通过参数配置
        
        Args:
            current_time: 当前时间戳
            short_term_half_life: 短期记忆半衰期（小时）
            long_term_half_life: 长期记忆半衰期（小时）
            importance_weight: 重要性权重
            recency_weight: 时效性权重
            access_weight: 访问频率权重
        """
        if current_time is None:
            current_time = time.time()
        
        # 重要性权重 (0.2 - 1.0)
        importance_score = self.importance.value / 5.0
        
        # 时效性衰减 (指数衰减)
        age_hours = (current_time - self.timestamp) / 3600
        if self.memory_type == MemoryType.SHORT_TERM:
            recency_score = exp(-age_hours / short_term_half_life)
        else:
            recency_score = exp(-age_hours / long_term_half_life)
        
        # 访问频率奖励
        access_score = min(1.0, self.access_count / 10.0)
        
        # 综合分数
        score = importance_score * importance_weight + recency_score * recency_weight + access_score * access_weight
        return float(score)
    
    def touch(self):
        """更新访问统计 - DEFECT-006 [P1] 修复: 同时更新混合时间戳"""
        self.access_count += 1
        self.last_access = time.time()
        # DEFECT-006 [P1] 修复: 更新混合时间戳
        self.last_access_hybrid = get_hybrid_timestamp()
    
    def to_storage_dict(self) -> Dict[str, Any]:
        """转换为存储格式"""
        return self.model_dump()
    
    @classmethod
    def from_storage_dict(cls, data: Dict[str, Any]) -> "MemoryItem":
        """从存储格式创建"""
        # 处理枚举类型转换
        data = data.copy()  # 避免修改原始数据
        if "memory_type" in data and isinstance(data["memory_type"], str):
            data["memory_type"] = MemoryType(data["memory_type"])
        if "importance" in data and isinstance(data["importance"], str):
            data["importance"] = ImportanceLevel[data["importance"].upper()]
        if "importance" in data and isinstance(data["importance"], int):
            try:
                data["importance"] = ImportanceLevel(data["importance"])
            except ValueError:
                # BUG FIX: 对超出范围的importance值提供默认值，而不是崩溃
                logger.warning(f"Invalid importance value: {data['importance']}, defaulting to MEDIUM")
                data["importance"] = ImportanceLevel.MEDIUM
        return cls(**data)


class RetrievalResult(BaseModel):
    """检索结果"""
    memory: MemoryItem
    score: float
    layer: str = Field(description="检索层级")
    
    model_config = ConfigDict(arbitrary_types_allowed=True)


class MemoryStats(BaseModel):
    """记忆统计信息"""
    short_term_count: int = 0
    episodic_count: int = 0
    semantic_count: int = 0
    total_count: int = 0
    total_added: int = 0
    total_retrieved: int = 0
    uptime_hours: float = 0.0


class RetrievalResponse(BaseModel):
    """检索响应数据模型
    
    替代原来 retrieve() 返回的 Dict[str, Any]，
    提供类型安全和 IDE 自动补全支持。
    
    Attributes:
        query: 原始查询内容
        strategy: 使用的检索策略
        layers: 各层级检索结果
        combined: 合并排序后的最终结果
        total_found: 找到的总结果数
        retrieval_time: 检索耗时（秒）
    """
    query: str = Field(description="查询内容")
    strategy: str = Field(description="检索策略")
    layers: Dict[str, Any] = Field(default_factory=dict, description="各层级检索结果")
    combined: List[Any] = Field(default_factory=list, description="合并排序后的结果")
    total_found: int = 0
    retrieval_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（向后兼容）"""
        return {
            "query": self.query,
            "strategy": self.strategy,
            "layers": self.layers,
            "combined": self.combined,
            "total_found": self.total_found,
            "retrieval_time": self.retrieval_time,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrievalResponse":
        """从字典创建（向后兼容）"""
        return cls(
            query=data.get("query", ""),
            strategy=data.get("strategy", ""),
            layers=data.get("layers", {}),
            combined=data.get("combined", []),
            total_found=data.get("total_found", len(data.get("combined", []))),
            retrieval_time=data.get("retrieval_time", 0.0),
        )


class LinkRelation(str, Enum):
    """记忆关联类型"""
    RELATED = "related"           # 语义相关
    CAUSAL = "causal"             # 因果关系
    TEMPORAL = "temporal"         # 时序关系
    HIERARCHICAL = "hierarchical" # 层级关系
    CONTRADICTORY = "contradictory" # 矛盾关系
    SUPPLEMENTARY = "supplementary" # 补充关系


class ModalityType(str, Enum):
    """多模态记忆类型"""
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    MIXED = "mixed"


@dataclass
class MultiModalContent:
    """
    多模态内容

    支持文本、图像、音频、视频等多种模态的记忆内容

    Attributes:
        modality: 模态类型
        text: 文本内容
        image_url: 图像URL或base64
        audio_url: 音频URL或base64
        video_url: 视频URL或base64
        metadata: 附加元数据
    """
    modality: ModalityType = ModalityType.TEXT
    text: Optional[str] = None
    image_url: Optional[str] = None
    audio_url: Optional[str] = None
    video_url: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "modality": self.modality.value,
            "text": self.text,
            "image_url": self.image_url,
            "audio_url": self.audio_url,
            "video_url": self.video_url,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MultiModalContent":
        return cls(
            modality=ModalityType(data.get("modality", "text")),
            text=data.get("text"),
            image_url=data.get("image_url"),
            audio_url=data.get("audio_url"),
            video_url=data.get("video_url"),
            metadata=data.get("metadata", {}),
        )

    def get_primary_content(self) -> str:
        """获取主要文本内容用于嵌入"""
        if self.text:
            return self.text
        # 对于非文本模态，返回描述性文本
        if self.modality == ModalityType.IMAGE and self.image_url:
            return f"[Image: {self.image_url}]"
        if self.modality == ModalityType.AUDIO and self.audio_url:
            return f"[Audio: {self.audio_url}]"
        if self.modality == ModalityType.VIDEO and self.video_url:
            return f"[Video: {self.video_url}]"
        return ""


@dataclass
class ContextInfo:
    """
    情境信息

    用于情境感知检索的上下文信息

    Attributes:
        session_id: 会话ID
        current_time: 当前时间戳
        location: 位置信息
        device_type: 设备类型
        user_intent: 用户意图
        session_topics: 会话主题列表
        previous_queries: 历史查询列表
    """
    session_id: Optional[str] = None
    current_time: Optional[float] = None
    location: Optional[str] = None
    device_type: Optional[str] = None
    user_intent: Optional[str] = None
    session_topics: List[str] = field(default_factory=list)
    previous_queries: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "current_time": self.current_time,
            "location": self.location,
            "device_type": self.device_type,
            "user_intent": self.user_intent,
            "session_topics": self.session_topics,
            "previous_queries": self.previous_queries,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContextInfo":
        return cls(
            session_id=data.get("session_id"),
            current_time=data.get("current_time"),
            location=data.get("location"),
            device_type=data.get("device_type"),
            user_intent=data.get("user_intent"),
            session_topics=data.get("session_topics", []),
            previous_queries=data.get("previous_queries", []),
        )


@dataclass
class RememberResponse:
    """记忆存储响应
    
    Attributes:
        memory_id: 记忆ID
        memory_type: 记忆类型
        importance: 重要性级别
        importance_confidence: 重要性置信度（自动评估时）
        is_new: 是否为新记忆（vs 更新已有记忆）
        related_memories: 相关记忆推荐列表
    """
    memory_id: str
    memory_type: str
    importance: str
    importance_confidence: Optional[float] = None
    is_new: bool = True
    related_memories: Optional[List[Dict[str, Any]]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "memory_id": self.memory_id,
            "memory_type": self.memory_type,
            "importance": self.importance,
            "importance_confidence": self.importance_confidence,
            "is_new": self.is_new,
            "related_memories": self.related_memories,
        }


@dataclass
class MemoryLink:
    """记忆关联链接
    
    表示两条记忆之间的关系，支持图遍历检索。
    
    Attributes:
        source_id: 源记忆ID
        target_id: 目标记忆ID
        relation: 关联类型
        weight: 关联强度 (0.0-1.0)
        metadata: 附加元数据
        created_at: 创建时间戳
    """
    source_id: str
    target_id: str
    relation: LinkRelation = LinkRelation.RELATED
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    
    def __post_init__(self):
        if not self.source_id or not self.target_id:
            raise ValueError("source_id 和 target_id 不能为空")
        if self.source_id == self.target_id:
            raise ValueError("不能创建自环关联")
        if not 0.0 <= self.weight <= 1.0:
            raise ValueError(f"weight 必须在 [0.0, 1.0] 范围内，当前值: {self.weight}")
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relation": self.relation.value,
            "weight": self.weight,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MemoryLink":
        return cls(
            source_id=data["source_id"],
            target_id=data["target_id"],
            relation=LinkRelation(data.get("relation", "related")),
            weight=data.get("weight", 1.0),
            metadata=data.get("metadata", {}),
            created_at=data.get("created_at", time.time()),
        )

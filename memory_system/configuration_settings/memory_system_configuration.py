"""
配置管理
支持YAML/JSON/环境变量
"""

from typing import Any, Dict, Optional
try:
    from pydantic import BaseModel, Field, field_validator
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = object
    Field = lambda **kwargs: kwargs
    def field_validator(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
import yaml
import json
import os

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
    _HAS_PYDANTIC_SETTINGS = True
except ImportError:
    _HAS_PYDANTIC_SETTINGS = False


class ShortTermBackendConfig(BaseModel):
    """短期记忆后端配置"""
    backend: str = Field(default="memory", description="后端类型: memory/redis/sqlite")
    # Redis配置
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    # SQLite配置
    sqlite_path: str = "./short_term_memory.db"


class AuthenticationConfig(BaseModel):
    """认证配置"""
    enabled: bool = Field(default=False, description="是否启用认证")
    api_key: Optional[str] = Field(default=None, description="API Key")
    oauth2_enabled: bool = Field(default=False, description="是否启用OAuth2")
    oauth2_users: Dict[str, str] = Field(default_factory=dict, description="用户名->密码哈希")


class StorageConfig(BaseModel):
    """存储层配置"""
    backend: str = Field(default="chroma", description="存储后端: chroma/qdrant/memory")
    
    # Chroma配置
    chroma_path: str = Field(default="./chroma_db", description="Chroma持久化路径")
    chroma_collection: str = Field(default="agent_memory", description="Chroma集合名")
    
    # Qdrant配置
    qdrant_host: str = Field(default="localhost", description="Qdrant主机")
    qdrant_port: int = Field(default=6333, description="Qdrant端口")
    qdrant_collection: str = Field(default="agent_memory", description="Qdrant集合名")
    qdrant_api_key: Optional[str] = Field(default=None, description="Qdrant API密钥")
    
    # 通用配置
    max_memories: int = Field(default=10000, description="最大记忆数量")
    
    @field_validator("max_memories")
    @classmethod
    def validate_max_memories(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("max_memories 必须大于 0")
        return v


class EmbeddingConfig(BaseModel):
    """嵌入层配置"""
    provider: str = Field(default="sentence_transformers", description="嵌入提供者")
    
    # 模型配置
    model_name: str = Field(default="BAAI/bge-small-zh-v1.5", description="模型名称")
    device: str = Field(default="cpu", description="运行设备: cpu/cuda")
    normalize_embeddings: bool = Field(default=True, description="是否归一化")
    
    # OpenAI配置 (可选)
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API密钥")
    openai_model: str = Field(default="text-embedding-3-small", description="OpenAI模型")


class ShortTermConfig(BaseModel):
    """短期记忆配置"""
    capacity: int = Field(default=7, description="容量限制(Miller定律)")
    ttl_seconds: float = Field(default=300, description="TTL过期时间")
    backend: ShortTermBackendConfig = Field(default_factory=ShortTermBackendConfig)

    @field_validator("capacity")
    @classmethod
    def validate_capacity(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("capacity 必须大于 0")
        return v


class RetrievalConfig(BaseModel):
    """检索层配置"""
    strategy: str = Field(default="hierarchical", description="检索策略")
    default_max_results: int = Field(default=10, description="默认返回结果数")
    enable_short_term: bool = Field(default=True, description="启用短期记忆检索")
    enable_vector_search: bool = Field(default=True, description="启用向量检索")
    enable_time_filter: bool = Field(default=True, description="启用时间过滤")


class ConsolidationConfig(BaseModel):
    """整合层配置"""
    enabled: bool = Field(default=True, description="启用记忆整合")
    strategy: str = Field(default="time_based", description="整合策略")
    threshold: int = Field(default=100, description="触发整合的阈值")
    interval_hours: float = Field(default=1.0, description="整合间隔")


class ForgettingConfig(BaseModel):
    """遗忘配置"""
    enabled: bool = Field(default=True, description="启用遗忘机制")
    strategy: str = Field(default="score_based", description="遗忘策略")
    max_long_term_memories: int = Field(default=10000, description="长期记忆上限")
    forget_ratio: float = Field(default=0.1, description="每次遗忘比例")
    
    @field_validator("forget_ratio")
    @classmethod
    def validate_forget_ratio(cls, v: float) -> float:
        if not 0 < v < 1:
            raise ValueError("forget_ratio 必须在 0 到 1 之间")
        return v


class TelemetryConfig(BaseModel):
    """分布式追踪配置"""
    enabled: bool = Field(default=False, description="启用 OpenTelemetry 追踪")
    service_name: str = Field(default="agent-memory-system", description="服务名称")
    exporter_type: str = Field(default="console", description="导出器类型: console/otlp/none")
    otlp_endpoint: Optional[str] = Field(default=None, description="OTLP 端点地址")


class TenantConfig(BaseModel):
    """租户配置"""
    enabled: bool = Field(default=False, description="是否启用多租户")
    default_tenant: str = Field(default="default", description="默认租户ID")
    max_memories_per_tenant: int = Field(default=10000, description="每租户最大记忆数")
    max_short_term_per_tenant: int = Field(default=100, description="每租户最大短期记忆数")


class TTLExpirationConfig(BaseModel):
    """TTL过期配置"""
    enabled: bool = Field(default=False, description="是否启用TTL过期清理")
    default_ttl_days: float = Field(default=365.0, description="默认TTL天数")
    check_interval_hours: float = Field(default=24.0, description="检查间隔小时")
    max_delete_per_run: int = Field(default=1000, description="每次最大删除数")
    ttl_by_importance: Dict[int, float] = Field(
        default_factory=lambda: {
            5: 730.0, 4: 365.0, 3: 180.0, 2: 30.0, 1: 7.0
        }
    )


class MemoryConfig(BaseModel):
    """主配置类"""
    storage: StorageConfig = Field(default_factory=StorageConfig)
    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    short_term: ShortTermConfig = Field(default_factory=ShortTermConfig)
    retrieval: RetrievalConfig = Field(default_factory=RetrievalConfig)
    consolidation: ConsolidationConfig = Field(default_factory=ConsolidationConfig)
    forgetting: ForgettingConfig = Field(default_factory=ForgettingConfig)
    authentication: AuthenticationConfig = Field(default_factory=AuthenticationConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)
    tenant: TenantConfig = Field(default_factory=TenantConfig)
    ttl: TTLExpirationConfig = Field(default_factory=TTLExpirationConfig)
    
    @classmethod
    def from_yaml(cls, filepath: str) -> "MemoryConfig":
        """从YAML文件加载配置"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件不存在: {filepath}")
        if data is None:
            raise ValueError(f"YAML文件为空: {filepath}")
        return cls(**data)

    @classmethod
    def from_json(cls, filepath: str) -> "MemoryConfig":
        """从JSON文件加载配置"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件不存在: {filepath}")
        if data is None:
            raise ValueError(f"JSON文件为空: {filepath}")
        return cls(**data)
    
    @classmethod
    def from_env(cls) -> "MemoryConfig":
        """从环境变量加载配置"""
        if _HAS_PYDANTIC_SETTINGS:
            return MemoryEnvConfig().to_memory_config()
        # Fallback to manual parsing
        config = cls()
        
        # 存储配置
        if os.getenv("MEMORY_STORAGE_BACKEND"):
            config.storage.backend = os.getenv("MEMORY_STORAGE_BACKEND")
        if os.getenv("MEMORY_CHROMA_PATH"):
            config.storage.chroma_path = os.getenv("MEMORY_CHROMA_PATH")
        if os.getenv("MEMORY_QDRANT_HOST"):
            config.storage.qdrant_host = os.getenv("MEMORY_QDRANT_HOST")
        if os.getenv("MEMORY_QDRANT_PORT"):
            config.storage.qdrant_port = int(os.getenv("MEMORY_QDRANT_PORT"))
        
        # 嵌入配置
        if os.getenv("MEMORY_EMBEDDING_PROVIDER"):
            config.embedding.provider = os.getenv("MEMORY_EMBEDDING_PROVIDER")
        if os.getenv("MEMORY_EMBEDDING_MODEL"):
            config.embedding.model_name = os.getenv("MEMORY_EMBEDDING_MODEL")
        if os.getenv("MEMORY_OPENAI_API_KEY"):
            config.embedding.openai_api_key = os.getenv("MEMORY_OPENAI_API_KEY")
        
        return config
    
    def to_yaml(self, filepath: str):
        """保存为YAML文件"""
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(self.model_dump(), f, allow_unicode=True, default_flow_style=False)
    
    def to_json(self, filepath: str):
        """保存为JSON文件"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.model_dump(), f, ensure_ascii=False, indent=2)


if _HAS_PYDANTIC_SETTINGS:
    class MemoryEnvConfig(BaseSettings):
        """Environment-based configuration using pydantic-settings.
        Automatically maps MEMORY_* environment variables to config fields.
        Usage: config = MemoryEnvConfig()  # reads from env automatically
        """
        model_config = SettingsConfigDict(env_prefix="MEMORY_", env_nested_delimiter="__")
        
        # Storage
        storage_backend: str = "memory"
        chroma_collection: str = "agent_memory"
        chroma_path: Optional[str] = None
        qdrant_host: str = "localhost"
        qdrant_port: int = 6333
        faiss_index_path: Optional[str] = None
        max_memories: int = 10000
        
        # Embedding
        embedding_provider: str = "sentence_transformers"
        embedding_model: str = "BAAI/bge-small-zh-v1.5"
        embedding_dimension: int = 64
        openai_api_key: Optional[str] = None
        
        # Short-term
        short_term_capacity: int = 7
        short_term_ttl: int = 300
        short_term_backend: str = "memory"
        
        # Retrieval
        retrieval_strategy: str = "hierarchical"
        
        # Consolidation
        consolidation_enabled: bool = False
        consolidation_threshold: int = 100
        
        # Forgetting
        forgetting_enabled: bool = True
        max_memories_limit: int = 10000
        
        def to_memory_config(self) -> "MemoryConfig":
            """Convert to MemoryConfig instance"""
            return MemoryConfig(
                storage=StorageConfig(
                    backend=self.storage_backend,
                    chroma_collection=self.chroma_collection,
                    chroma_path=self.chroma_path,
                    qdrant_host=self.qdrant_host,
                    qdrant_port=self.qdrant_port,
                    faiss_index_path=self.faiss_index_path,
                    max_memories=self.max_memories,
                ),
                embedding=EmbeddingConfig(
                    provider=self.embedding_provider,
                    model_name=self.embedding_model,
                    dimension=self.embedding_dimension,
                    openai_api_key=self.openai_api_key,
                ),
                short_term=ShortTermConfig(
                    capacity=self.short_term_capacity,
                    ttl_seconds=self.short_term_ttl,
                    backend=ShortTermBackendConfig(type=self.short_term_backend) if self.short_term_backend else None,
                ),
                retrieval=RetrievalConfig(
                    strategy=self.retrieval_strategy,
                ),
                consolidation=ConsolidationConfig(
                    enabled=self.consolidation_enabled,
                    threshold=self.consolidation_threshold,
                ),
                forgetting=ForgettingConfig(
                    enabled=self.forgetting_enabled,
                    max_memories=self.max_memories_limit,
                ),
            )


# 默认配置示例
DEFAULT_CONFIG_YAML = """
# Agent Memory System 配置

storage:
  backend: chroma  # chroma/qdrant/memory
  chroma_path: ./chroma_db
  chroma_collection: agent_memory
  max_memories: 10000

embedding:
  provider: sentence_transformers  # sentence_transformers/openai
  model_name: BAAI/bge-small-zh-v1.5
  device: cpu
  normalize_embeddings: true

short_term:
  capacity: 7
  ttl_seconds: 300

retrieval:
  strategy: hierarchical
  default_max_results: 10
  enable_short_term: true
  enable_vector_search: true

consolidation:
  enabled: true
  strategy: time_based
  threshold: 100

forgetting:
  enabled: true
  strategy: score_based
  max_long_term_memories: 10000
"""

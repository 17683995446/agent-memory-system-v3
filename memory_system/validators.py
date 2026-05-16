"""统一Pydantic验证器

Agent Memory System V2.29.0 重构项目 - R-04
标准化配置验证逻辑
"""

from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum
import re

from memory_system.core_abstractions.memory_data_models import MemoryType, ImportanceLevel


class StorageBackendType(str, Enum):
    """存储后端类型"""
    CHROMA = "chroma"
    QDRANT = "qdrant"
    FAISS = "faiss"
    IN_MEMORY = "in_memory"
    REDIS = "redis"
    SQLITE = "sqlite"


class EmbeddingProviderType(str, Enum):
    """嵌入提供者类型"""
    OPENAI = "openai"
    BGE = "bge"
    SENTENCE_TRANSFORMER = "sentence_transformer"
    MOCK = "mock"


class MemoryConfigValidator(BaseModel):
    """记忆系统配置验证器
    
    验证记忆系统的核心配置参数
    """
    # 内容长度限制
    max_content_length: int = Field(default=100000, ge=100, le=1000000)
    max_query_length: int = Field(default=10000, ge=10, le=100000)
    max_batch_size: int = Field(default=1000, ge=1, le=10000)
    
    # 存储配置
    storage_backend: StorageBackendType = Field(default=StorageBackendType.IN_MEMORY)
    storage_path: Optional[str] = None
    
    # 嵌入配置
    embedding_provider: EmbeddingProviderType = Field(default=EmbeddingProviderType.MOCK)
    embedding_dimension: int = Field(default=384, ge=64, le=4096)
    
    # 检索配置
    default_max_results: int = Field(default=10, ge=1, le=100)
    retrieval_timeout: float = Field(default=30.0, ge=1.0, le=300.0)
    
    # TTL配置
    default_ttl_seconds: Optional[int] = Field(default=None, ge=60, le=31536000)  # 1年
    enable_ttl_cleanup: bool = True
    
    # 性能配置
    enable_caching: bool = True
    cache_size: int = Field(default=1000, ge=100, le=100000)
    
    @field_validator('storage_path')
    @classmethod
    def validate_storage_path(cls, v, info):
        """验证存储路径"""
        data = info.data
        if v and data.get('storage_backend') in [StorageBackendType.CHROMA, StorageBackendType.SQLITE]:
            if not re.match(r'^[\w\-\./]+$', v):
                raise ValueError('storage_path contains invalid characters')
        return v
    
    @model_validator(mode='after')
    def validate_embedding_compatibility(self):
        """验证嵌入配置兼容性"""
        provider = self.embedding_provider
        dimension = self.embedding_dimension
        
        # 不同提供者有推荐的维度
        recommended_dims = {
            EmbeddingProviderType.OPENAI: 1536,
            EmbeddingProviderType.BGE: 768,
            EmbeddingProviderType.SENTENCE_TRANSFORMER: 384,
            EmbeddingProviderType.MOCK: 384,
        }
        
        if provider in recommended_dims:
            recommended = recommended_dims[provider]
            if dimension != recommended:
                # 记录警告但不阻止（某些模型支持多种维度）
                pass  # 实际使用时可以通过日志记录
        
        return self


class TenantConfigValidator(BaseModel):
    """租户配置验证器
    
    验证多租户隔离配置
    """
    tenant_id: str = Field(..., min_length=3, max_length=64)
    tenant_name: Optional[str] = Field(default=None, max_length=128)
    quota: Optional[int] = Field(default=None, ge=0)
    max_memories: Optional[int] = Field(default=None, ge=0)
    priority: int = Field(default=5, ge=1, le=10)
    enabled: bool = True
    
    @field_validator('tenant_id')
    @classmethod
    def validate_tenant_id(cls, v):
        """验证租户ID格式"""
        if not v:
            raise ValueError('tenant_id cannot be empty')
        if len(v) < 3:
            raise ValueError('tenant_id must be at least 3 characters')
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_-]*$', v):
            raise ValueError('tenant_id must start with alphanumeric and contain only alphanumeric, underscore, or hyphen')
        return v
    
    @field_validator('tenant_name')
    @classmethod
    def validate_tenant_name(cls, v):
        """验证租户名称"""
        if v and len(v.strip()) == 0:
            return None
        return v


class RetrievalConfigValidator(BaseModel):
    """检索配置验证器"""
    
    strategy: str = Field(default="hybrid", pattern=r'^(hybrid|vector_only|keyword|hierarchical)$')
    max_results: int = Field(default=10, ge=1, le=100)
    min_score_threshold: float = Field(default=0.0, ge=0.0, le=1.0)
    
    # 混合检索参数
    vector_weight: float = Field(default=0.6, ge=0.0, le=1.0)
    keyword_weight: float = Field(default=0.4, ge=0.0, le=1.0)
    rrf_k: int = Field(default=60, ge=1, le=200)
    
    # 多样性参数
    enable_diversity: bool = True
    diversity_factor: float = Field(default=0.5, ge=0.0, le=1.0)
    
    @model_validator(mode='after')
    def validate_weights(self):
        """验证权重总和"""
        vector_w = self.vector_weight
        keyword_w = self.keyword_weight
        if abs(vector_w + keyword_w - 1.0) > 0.01:
            raise ValueError('vector_weight + keyword_weight must equal 1.0')
        return self


class TTLConfigValidator(BaseModel):
    """TTL配置验证器"""
    
    enabled: bool = True
    check_interval_seconds: int = Field(default=300, ge=60, le=3600)
    batch_delete_size: int = Field(default=100, ge=10, le=1000)
    
    # 默认TTL值（按记忆类型）
    short_term_ttl: int = Field(default=3600, ge=60, le=86400)  # 1小时
    episodic_ttl: int = Field(default=604800, ge=3600, le=2592000)  # 7天
    semantic_ttl: Optional[int] = Field(default=None, ge=3600)  # 无限制


class SnapshotConfigValidator(BaseModel):
    """快照配置验证器"""
    
    enabled: bool = True
    auto_snapshot_interval: Optional[int] = Field(default=3600, ge=300)
    max_snapshots: int = Field(default=10, ge=1, le=100)
    compression_enabled: bool = True
    serializer: str = Field(default="json", pattern=r'^(json|msgpack|pickle)$')
    storage_path: Optional[str] = None


class SecurityConfigValidator(BaseModel):
    """安全配置验证器"""
    
    enable_encryption: bool = False
    encryption_key_path: Optional[str] = None
    enable_audit_log: bool = True
    max_failed_auth_attempts: int = Field(default=5, ge=1, le=20)
    auth_lockout_duration: int = Field(default=300, ge=60, le=3600)
    
    @field_validator('encryption_key_path')
    @classmethod
    def validate_key_path(cls, v, info):
        """验证加密密钥路径"""
        if info.data.get('enable_encryption') and not v:
            raise ValueError('encryption_key_path is required when encryption is enabled')
        return v


class APIConfigValidator(BaseModel):
    """API配置验证器"""
    
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000, ge=1, le=65535)
    workers: int = Field(default=1, ge=1, le=16)
    
    # CORS配置
    cors_origins: List[str] = Field(default=["*"])
    cors_allow_credentials: bool = False
    cors_allow_methods: List[str] = Field(default=["GET", "POST", "PUT", "DELETE"])
    cors_allow_headers: List[str] = Field(default=["*"])
    
    # 限流配置
    rate_limit_enabled: bool = True
    rate_limit_requests: int = Field(default=100, ge=10, le=10000)
    rate_limit_window: int = Field(default=60, ge=10, le=3600)
    
    @field_validator('host')
    @classmethod
    def validate_host(cls, v):
        """验证主机地址"""
        valid_hosts = ["0.0.0.0", "127.0.0.1", "localhost"]
        if v not in valid_hosts and not re.match(r'^(\d{1,3}\.){3}\d{1,3}$', v):
            raise ValueError(f'host must be a valid IP address or one of {valid_hosts}')
        return v


class MemoryItemValidator(BaseModel):
    """记忆项验证器
    
    用于验证记忆项数据的完整性
    """
    content: str = Field(..., min_length=1, max_length=100000)
    memory_type: MemoryType = Field(default=MemoryType.SHORT_TERM)
    importance: ImportanceLevel = Field(default=ImportanceLevel.MEDIUM)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator('content')
    @classmethod
    def validate_content(cls, v):
        """验证内容"""
        if not v or not v.strip():
            raise ValueError('content cannot be empty or whitespace only')
        return v.strip()
    
    @field_validator('metadata')
    @classmethod
    def validate_metadata(cls, v):
        """验证元数据大小"""
        import json
        try:
            metadata_size = len(json.dumps(v))
            if metadata_size > 10000:  # 10KB limit
                raise ValueError(f'metadata size exceeds 10KB: {metadata_size} bytes')
        except (TypeError, ValueError) as e:
            raise ValueError(f'metadata must be JSON serializable: {e}')
        return v


class BatchOperationValidator(BaseModel):
    """批量操作验证器"""
    
    items: List[Dict[str, Any]] = Field(..., min_items=1, max_items=1000)
    operation: str = Field(..., pattern=r'^(add|update|delete)$')
    batch_size: int = Field(default=100, ge=1, le=1000)
    continue_on_error: bool = True
    
    @field_validator('items')
    @classmethod
    def validate_items(cls, v, info):
        """验证批量操作项目"""
        operation = info.data.get('operation')
        
        for i, item in enumerate(v):
            if operation == 'add':
                if 'content' not in item:
                    raise ValueError(f'Item {i}: content is required for add operation')
            elif operation == 'delete':
                if 'memory_id' not in item:
                    raise ValueError(f'Item {i}: memory_id is required for delete operation')
            elif operation == 'update':
                if 'memory_id' not in item:
                    raise ValueError(f'Item {i}: memory_id is required for update operation')
                if 'updates' not in item:
                    raise ValueError(f'Item {i}: updates is required for update operation')
        
        return v


class QueryValidator(BaseModel):
    """查询验证器"""
    
    query: str = Field(..., min_length=1, max_length=10000)
    max_results: int = Field(default=10, ge=1, le=100)
    filters: Optional[Dict[str, Any]] = None
    
    @field_validator('query')
    @classmethod
    def validate_query(cls, v):
        """验证查询内容"""
        if not v or not v.strip():
            raise ValueError('query cannot be empty')
        # 检查是否包含危险字符（简单的SQL注入防护）
        dangerous_patterns = [r'--', r'/\*', r'\*/', r';\s*drop\s+', r';\s*delete\s+']
        for pattern in dangerous_patterns:
            if re.search(pattern, v, re.IGNORECASE):
                raise ValueError('query contains potentially dangerous characters')
        return v.strip()


class SystemConfigValidator(BaseModel):
    """系统整体配置验证器
    
    组合所有子配置进行统一验证
    """
    memory: MemoryConfigValidator = Field(default_factory=MemoryConfigValidator)
    retrieval: RetrievalConfigValidator = Field(default_factory=RetrievalConfigValidator)
    ttl: TTLConfigValidator = Field(default_factory=TTLConfigValidator)
    snapshot: SnapshotConfigValidator = Field(default_factory=SnapshotConfigValidator)
    security: SecurityConfigValidator = Field(default_factory=SecurityConfigValidator)
    api: APIConfigValidator = Field(default_factory=APIConfigValidator)
    
    # 日志配置
    log_level: str = Field(default="INFO", pattern=r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$')
    log_format: str = Field(default="structured", pattern=r'^(structured|text)$')
    
    # 调试配置
    debug_mode: bool = False
    enable_profiling: bool = False
    
    @model_validator(mode='after')
    def validate_consistency(self):
        """验证配置一致性"""
        memory = self.memory
        retrieval = self.retrieval
        
        # 确保检索的最大结果数不超过批处理大小
        if memory and retrieval:
            if retrieval.max_results > memory.max_batch_size:
                raise ValueError('retrieval.max_results cannot exceed memory.max_batch_size')
        
        return self


def validate_config(config_dict: Dict[str, Any]) -> SystemConfigValidator:
    """验证配置字典
    
    Args:
        config_dict: 配置字典
        
    Returns:
        验证后的配置对象
        
    Raises:
        ValidationError: 验证失败时抛出
    """
    return SystemConfigValidator(**config_dict)


def validate_memory_item(data: Dict[str, Any]) -> MemoryItemValidator:
    """验证记忆项数据
    
    Args:
        data: 记忆项数据字典
        
    Returns:
        验证后的记忆项对象
    """
    return MemoryItemValidator(**data)


def validate_tenant_config(data: Dict[str, Any]) -> TenantConfigValidator:
    """验证租户配置
    
    Args:
        data: 租户配置字典
        
    Returns:
        验证后的租户配置对象
    """
    return TenantConfigValidator(**data)


def validate_query(data: Dict[str, Any]) -> QueryValidator:
    """验证查询参数
    
    Args:
        data: 查询参数字典
        
    Returns:
        验证后的查询对象
    """
    return QueryValidator(**data)

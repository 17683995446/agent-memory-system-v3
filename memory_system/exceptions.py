"""统一异常层次结构

Agent Memory System V2.29.0 重构项目 - R-01
统一存储接口异常处理
"""


class MemorySystemError(Exception):
    """基础异常类 - 所有记忆系统异常的基类"""
    
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "MEMORY_SYSTEM_ERROR"
        self.details = details or {}
    
    def to_dict(self) -> dict:
        """将异常转换为字典格式，便于序列化"""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "exception_type": self.__class__.__name__
        }


class StorageError(MemorySystemError):
    """存储层异常 - 所有存储相关异常的基类"""
    
    def __init__(self, message: str, backend: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="STORAGE_ERROR",
            details=details
        )
        self.backend = backend


class StorageNotFoundError(StorageError):
    """记忆不存在异常 - 当尝试访问不存在的记忆时抛出"""
    
    def __init__(self, memory_id: str, backend: str = None, details: dict = None):
        super().__init__(
            message=f"Memory not found: {memory_id}",
            backend=backend,
            details=details
        )
        self.error_code = "STORAGE_NOT_FOUND"
        self.memory_id = memory_id


class StorageConnectionError(StorageError):
    """存储连接失败异常 - 当无法连接到存储后端时抛出"""
    
    def __init__(self, backend: str, connection_string: str = None, details: dict = None):
        message = f"Failed to connect to storage backend: {backend}"
        if connection_string:
            # 隐藏敏感信息
            safe_conn = connection_string.split("@")[-1] if "@" in connection_string else "***"
            message += f" (connection: {safe_conn})"
        super().__init__(
            message=message,
            backend=backend,
            details=details
        )
        self.error_code = "STORAGE_CONNECTION_ERROR"


class StorageQuotaExceededError(StorageError):
    """配额超限异常 - 当存储配额超出限制时抛出"""
    
    def __init__(self, backend: str, current_usage: int, quota_limit: int, details: dict = None):
        super().__init__(
            message=f"Storage quota exceeded: {current_usage}/{quota_limit} bytes",
            backend=backend,
            details=details
        )
        self.error_code = "STORAGE_QUOTA_EXCEEDED"
        self.current_usage = current_usage
        self.quota_limit = quota_limit


class StorageWriteError(StorageError):
    """存储写入异常 - 当写入操作失败时抛出"""
    
    def __init__(self, message: str, backend: str = None, operation: str = None, details: dict = None):
        super().__init__(
            message=message,
            backend=backend,
            details=details
        )
        self.error_code = "STORAGE_WRITE_ERROR"
        self.operation = operation


class StorageReadError(StorageError):
    """存储读取异常 - 当读取操作失败时抛出"""
    
    def __init__(self, message: str, backend: str = None, operation: str = None, details: dict = None):
        super().__init__(
            message=message,
            backend=backend,
            details=details
        )
        self.error_code = "STORAGE_READ_ERROR"
        self.operation = operation


class RetrievalError(MemorySystemError):
    """检索异常 - 所有检索相关异常的基类"""
    
    def __init__(self, message: str, strategy: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="RETRIEVAL_ERROR",
            details=details
        )
        self.strategy = strategy


class RetrievalTimeoutError(RetrievalError):
    """检索超时异常 - 当检索操作超时时抛出"""
    
    def __init__(self, timeout: float, strategy: str = None, details: dict = None):
        super().__init__(
            message=f"Retrieval operation timed out after {timeout}s",
            strategy=strategy,
            details=details
        )
        self.error_code = "RETRIEVAL_TIMEOUT"
        self.timeout = timeout


class RetrievalStrategyError(RetrievalError):
    """检索策略异常 - 当检索策略执行失败时抛出"""
    
    def __init__(self, message: str, strategy: str, details: dict = None):
        super().__init__(
            message=message,
            strategy=strategy,
            details=details
        )
        self.error_code = "RETRIEVAL_STRATEGY_ERROR"


class ValidationError(MemorySystemError):
    """验证异常 - 当数据验证失败时抛出"""
    
    def __init__(self, message: str, field: str = None, value=None, details: dict = None):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            details=details
        )
        self.field = field
        self.value = value


class ConfigurationError(MemorySystemError):
    """配置异常 - 当配置无效时抛出"""
    
    def __init__(self, message: str, config_key: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="CONFIGURATION_ERROR",
            details=details
        )
        self.config_key = config_key


class EmbeddingError(MemorySystemError):
    """嵌入异常 - 当嵌入生成失败时抛出"""
    
    def __init__(self, message: str, provider: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="EMBEDDING_ERROR",
            details=details
        )
        self.provider = provider


class EmbeddingProviderNotFoundError(EmbeddingError):
    """嵌入提供者未找到异常"""
    
    def __init__(self, provider_name: str, available_providers: list = None):
        message = f"Embedding provider not found: {provider_name}"
        if available_providers:
            message += f". Available providers: {', '.join(available_providers)}"
        super().__init__(
            message=message,
            provider=provider_name
        )
        self.error_code = "EMBEDDING_PROVIDER_NOT_FOUND"
        self.available_providers = available_providers or []


class TTLError(MemorySystemError):
    """TTL管理异常"""
    
    def __init__(self, message: str, memory_id: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="TTL_ERROR",
            details=details
        )
        self.memory_id = memory_id


class SnapshotError(MemorySystemError):
    """快照管理异常"""
    
    def __init__(self, message: str, snapshot_id: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="SNAPSHOT_ERROR",
            details=details
        )
        self.snapshot_id = snapshot_id


class SerializationError(SnapshotError):
    """序列化异常"""
    
    def __init__(self, message: str, serializer: str = None, details: dict = None):
        super().__init__(
            message=message,
            details=details
        )
        self.error_code = "SERIALIZATION_ERROR"
        self.serializer = serializer


class BatchProcessingError(MemorySystemError):
    """批处理异常"""
    
    def __init__(self, message: str, batch_size: int = None, failed_items: list = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="BATCH_PROCESSING_ERROR",
            details=details
        )
        self.batch_size = batch_size
        self.failed_items = failed_items or []


class TenantIsolationError(MemorySystemError):
    """租户隔离异常"""
    
    def __init__(self, message: str, tenant_id: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="TENANT_ISOLATION_ERROR",
            details=details
        )
        self.tenant_id = tenant_id


class DependencyInjectionError(MemorySystemError):
    """依赖注入异常"""
    
    def __init__(self, message: str, dependency_name: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="DI_ERROR",
            details=details
        )
        self.dependency_name = dependency_name


class MetricsError(MemorySystemError):
    """指标收集异常"""
    
    def __init__(self, message: str, metric_name: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="METRICS_ERROR",
            details=details
        )
        self.metric_name = metric_name

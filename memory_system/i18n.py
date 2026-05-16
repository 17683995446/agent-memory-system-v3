"""国际化支持 - 错误消息本地化"""

from typing import Dict, Optional, Any
from enum import Enum
from dataclasses import dataclass


class Locale(Enum):
    """支持的语言"""
    EN = "en"
    ZH = "zh"
    JA = "ja"
    KO = "ko"
    DE = "de"
    FR = "fr"
    ES = "es"


# 错误消息翻译表
MESSAGES: Dict[str, Dict[str, str]] = {
    "en": {
        # 通用错误
        "memory_not_found": "Memory not found: {memory_id}",
        "invalid_importance": "Invalid importance level: {level}",
        "storage_full": "Storage quota exceeded",
        "invalid_content": "Invalid content: {reason}",
        "query_too_long": "Query too long: {length} > {max}",
        "invalid_memory_type": "Invalid memory type: {type}",
        "operation_failed": "Operation failed: {reason}",
        "unauthorized": "Unauthorized access",
        "forbidden": "Access forbidden",
        "not_implemented": "Feature not implemented: {feature}",
        "internal_error": "Internal error occurred",
        "service_unavailable": "Service temporarily unavailable",
        "timeout": "Operation timed out",
        "rate_limit_exceeded": "Rate limit exceeded. Try again later",
        "invalid_parameter": "Invalid parameter: {param}",
        "missing_parameter": "Missing required parameter: {param}",
        "invalid_format": "Invalid format: {field}",
        "validation_error": "Validation error: {field} - {reason}",
        # 存储相关
        "storage_connection_error": "Storage connection error: {backend}",
        "storage_write_error": "Failed to write to storage",
        "storage_read_error": "Failed to read from storage",
        "storage_quota_exceeded": "Storage quota exceeded: {used}/{limit}",
        "memory_too_large": "Memory content too large: {size} bytes",
        # 嵌入相关
        "embedding_error": "Embedding generation failed: {reason}",
        "embedding_dimension_mismatch": "Embedding dimension mismatch: expected {expected}, got {actual}",
        "embedding_provider_error": "Embedding provider error: {provider}",
        # 检索相关
        "retrieval_error": "Retrieval operation failed: {reason}",
        "invalid_similarity_threshold": "Invalid similarity threshold: {value}",
        "invalid_max_results": "Invalid max results: {value}",
        # 租户相关
        "tenant_not_found": "Tenant not found: {tenant_id}",
        "tenant_quota_exceeded": "Tenant quota exceeded",
        "tenant_disabled": "Tenant is disabled: {tenant_id}",
        # 认证相关
        "auth_required": "Authentication required",
        "invalid_token": "Invalid or expired token",
        "insufficient_permissions": "Insufficient permissions: {permission}",
    },
    "zh": {
        # 通用错误
        "memory_not_found": "记忆不存在: {memory_id}",
        "invalid_importance": "无效的重要性级别: {level}",
        "storage_full": "存储配额已超出",
        "invalid_content": "无效的内容: {reason}",
        "query_too_long": "查询过长: {length} > {max}",
        "invalid_memory_type": "无效的记忆类型: {type}",
        "operation_failed": "操作失败: {reason}",
        "unauthorized": "未授权访问",
        "forbidden": "访问被禁止",
        "not_implemented": "功能未实现: {feature}",
        "internal_error": "发生内部错误",
        "service_unavailable": "服务暂时不可用",
        "timeout": "操作超时",
        "rate_limit_exceeded": "请求频率超限，请稍后再试",
        "invalid_parameter": "无效参数: {param}",
        "missing_parameter": "缺少必需参数: {param}",
        "invalid_format": "格式无效: {field}",
        "validation_error": "验证错误: {field} - {reason}",
        # 存储相关
        "storage_connection_error": "存储连接错误: {backend}",
        "storage_write_error": "写入存储失败",
        "storage_read_error": "从存储读取失败",
        "storage_quota_exceeded": "存储配额已超出: {used}/{limit}",
        "memory_too_large": "记忆内容过大: {size} 字节",
        # 嵌入相关
        "embedding_error": "嵌入生成失败: {reason}",
        "embedding_dimension_mismatch": "嵌入维度不匹配: 期望 {expected}, 实际 {actual}",
        "embedding_provider_error": "嵌入提供者错误: {provider}",
        # 检索相关
        "retrieval_error": "检索操作失败: {reason}",
        "invalid_similarity_threshold": "无效的相似度阈值: {value}",
        "invalid_max_results": "无效的最大结果数: {value}",
        # 租户相关
        "tenant_not_found": "租户不存在: {tenant_id}",
        "tenant_quota_exceeded": "租户配额已超出",
        "tenant_disabled": "租户已禁用: {tenant_id}",
        # 认证相关
        "auth_required": "需要认证",
        "invalid_token": "无效或已过期的令牌",
        "insufficient_permissions": "权限不足: {permission}",
    },
    "ja": {
        "memory_not_found": "メモリが見つかりません: {memory_id}",
        "invalid_importance": "無効な重要度レベル: {level}",
        "storage_full": "ストレージクォータを超過しました",
        "invalid_content": "無効なコンテンツ: {reason}",
        "query_too_long": "クエリが長すぎます: {length} > {max}",
        "operation_failed": "操作に失敗しました: {reason}",
        "unauthorized": "未承認のアクセス",
        "forbidden": "アクセスが禁止されています",
        "internal_error": "内部エラーが発生しました",
        "timeout": "操作がタイムアウトしました",
        "rate_limit_exceeded": "レート制限を超えました。後でもう一度お試しください",
    },
    "ko": {
        "memory_not_found": "메모리를 찾을 수 없음: {memory_id}",
        "invalid_importance": "잘못된 중요도 수준: {level}",
        "storage_full": "저소량 할당량 초과",
        "invalid_content": "잘못된 콘텐츠: {reason}",
        "query_too_long": "쿼리가 너무 김: {length} > {max}",
        "operation_failed": "작업 실패: {reason}",
        "unauthorized": "인증되지 않은 접근",
        "forbidden": "접근 금지",
        "internal_error": "낶部 오류 발생",
        "timeout": "작업 시간 초과",
        "rate_limit_exceeded": "요청 속도 제한 초과. 나중에 다시 시도하세요",
    },
    "de": {
        "memory_not_found": "Speicher nicht gefunden: {memory_id}",
        "invalid_importance": "Ungültige Wichtigkeitsstufe: {level}",
        "storage_full": "Speicherkontingent überschritten",
        "invalid_content": "Ungültiger Inhalt: {reason}",
        "query_too_long": "Abfrage zu lang: {length} > {max}",
        "operation_failed": "Vorgang fehlgeschlagen: {reason}",
        "unauthorized": "Unautorisierter Zugriff",
        "forbidden": "Zugriff verboten",
        "internal_error": "Interner Fehler aufgetreten",
        "timeout": "Zeitüberschreitung des Vorgangs",
        "rate_limit_exceeded": "Ratenlimit überschritten. Bitte später erneut versuchen",
    },
    "fr": {
        "memory_not_found": "Mémoire non trouvée: {memory_id}",
        "invalid_importance": "Niveau d'importance invalide: {level}",
        "storage_full": "Quota de stockage dépassé",
        "invalid_content": "Contenu invalide: {reason}",
        "query_too_long": "Requête trop longue: {length} > {max}",
        "operation_failed": "Opération échouée: {reason}",
        "unauthorized": "Accès non autorisé",
        "forbidden": "Accès interdit",
        "internal_error": "Erreur interne survenue",
        "timeout": "Délai d'opération dépassé",
        "rate_limit_exceeded": "Limite de débit dépassée. Réessayez plus tard",
    },
    "es": {
        "memory_not_found": "Memoria no encontrada: {memory_id}",
        "invalid_importance": "Nivel de importancia inválido: {level}",
        "storage_full": "Cuota de almacenamiento excedida",
        "invalid_content": "Contenido inválido: {reason}",
        "query_too_long": "Consulta demasiado larga: {length} > {max}",
        "operation_failed": "Operación fallida: {reason}",
        "unauthorized": "Acceso no autorizado",
        "forbidden": "Acceso prohibido",
        "internal_error": "Error interno ocurrido",
        "timeout": "Tiempo de operación agotado",
        "rate_limit_exceeded": "Límite de tasa excedido. Inténtelo de nuevo más tarde",
    },
}

# 当前语言设置
_current_locale: str = "zh"


def set_locale(locale: str) -> None:
    """设置当前语言

    Args:
        locale: 语言代码 (en, zh, ja, ko, de, fr, es)
    """
    global _current_locale
    if locale in [l.value for l in Locale]:
        _current_locale = locale
    else:
        raise ValueError(f"Unsupported locale: {locale}. Supported: {[l.value for l in Locale]}")


def get_locale() -> str:
    """获取当前语言设置"""
    return _current_locale


def get_message(key: str, locale: Optional[str] = None, **kwargs) -> str:
    """获取本地化错误消息

    Args:
        key: 消息键
        locale: 指定语言，默认使用当前设置
        **kwargs: 消息格式化参数

    Returns:
        str: 本地化后的消息
    """
    target_locale = locale or _current_locale
    messages = MESSAGES.get(target_locale, MESSAGES["en"])
    template = messages.get(key, MESSAGES["en"].get(key, key))
    try:
        return template.format(**kwargs)
    except KeyError as e:
        # 如果格式化失败，返回原始模板
        return template


def get_all_messages(key: str, **kwargs) -> Dict[str, str]:
    """获取所有语言的翻译

    Args:
        key: 消息键
        **kwargs: 消息格式化参数

    Returns:
        Dict[str, str]: 各语言的翻译
    """
    return {
        locale: get_message(key, locale=locale, **kwargs)
        for locale in MESSAGES.keys()
    }


# 便捷函数 - 常用错误消息
class ErrorMessages:
    """错误消息便捷访问类"""

    @staticmethod
    def memory_not_found(memory_id: str, **kwargs) -> str:
        return get_message("memory_not_found", memory_id=memory_id, **kwargs)

    @staticmethod
    def invalid_importance(level: str, **kwargs) -> str:
        return get_message("invalid_importance", level=level, **kwargs)

    @staticmethod
    def storage_full(**kwargs) -> str:
        return get_message("storage_full", **kwargs)

    @staticmethod
    def invalid_content(reason: str, **kwargs) -> str:
        return get_message("invalid_content", reason=reason, **kwargs)

    @staticmethod
    def query_too_long(length: int, max_length: int, **kwargs) -> str:
        return get_message("query_too_long", length=length, max=max_length, **kwargs)

    @staticmethod
    def operation_failed(reason: str, **kwargs) -> str:
        return get_message("operation_failed", reason=reason, **kwargs)

    @staticmethod
    def unauthorized(**kwargs) -> str:
        return get_message("unauthorized", **kwargs)

    @staticmethod
    def forbidden(**kwargs) -> str:
        return get_message("forbidden", **kwargs)

    @staticmethod
    def not_implemented(feature: str, **kwargs) -> str:
        return get_message("not_implemented", feature=feature, **kwargs)

    @staticmethod
    def internal_error(**kwargs) -> str:
        return get_message("internal_error", **kwargs)

    @staticmethod
    def service_unavailable(**kwargs) -> str:
        return get_message("service_unavailable", **kwargs)

    @staticmethod
    def timeout(**kwargs) -> str:
        return get_message("timeout", **kwargs)

    @staticmethod
    def rate_limit_exceeded(**kwargs) -> str:
        return get_message("rate_limit_exceeded", **kwargs)

    @staticmethod
    def invalid_parameter(param: str, **kwargs) -> str:
        return get_message("invalid_parameter", param=param, **kwargs)

    @staticmethod
    def missing_parameter(param: str, **kwargs) -> str:
        return get_message("missing_parameter", param=param, **kwargs)

    @staticmethod
    def validation_error(field: str, reason: str, **kwargs) -> str:
        return get_message("validation_error", field=field, reason=reason, **kwargs)

    @staticmethod
    def storage_connection_error(backend: str, **kwargs) -> str:
        return get_message("storage_connection_error", backend=backend, **kwargs)

    @staticmethod
    def embedding_error(reason: str, **kwargs) -> str:
        return get_message("embedding_error", reason=reason, **kwargs)

    @staticmethod
    def tenant_not_found(tenant_id: str, **kwargs) -> str:
        return get_message("tenant_not_found", tenant_id=tenant_id, **kwargs)

    @staticmethod
    def auth_required(**kwargs) -> str:
        return get_message("auth_required", **kwargs)

    @staticmethod
    def invalid_token(**kwargs) -> str:
        return get_message("invalid_token", **kwargs)

    @staticmethod
    def insufficient_permissions(permission: str, **kwargs) -> str:
        return get_message("insufficient_permissions", permission=permission, **kwargs)


# 向后兼容的便捷函数
memory_not_found = ErrorMessages.memory_not_found
invalid_importance = ErrorMessages.invalid_importance
storage_full = ErrorMessages.storage_full
invalid_content = ErrorMessages.invalid_content
query_too_long = ErrorMessages.query_too_long
operation_failed = ErrorMessages.operation_failed
unauthorized = ErrorMessages.unauthorized
forbidden = ErrorMessages.forbidden
not_implemented = ErrorMessages.not_implemented
internal_error = ErrorMessages.internal_error
service_unavailable = ErrorMessages.service_unavailable
timeout = ErrorMessages.timeout
rate_limit_exceeded = ErrorMessages.rate_limit_exceeded
invalid_parameter = ErrorMessages.invalid_parameter
missing_parameter = ErrorMessages.missing_parameter
validation_error = ErrorMessages.validation_error
storage_connection_error = ErrorMessages.storage_connection_error
embedding_error = ErrorMessages.embedding_error
tenant_not_found = ErrorMessages.tenant_not_found
auth_required = ErrorMessages.auth_required
invalid_token = ErrorMessages.invalid_token
insufficient_permissions = ErrorMessages.insufficient_permissions


class I18nMiddleware:
    """国际化中间件 - 用于Web框架集成"""

    def __init__(self, default_locale: str = "en"):
        self.default_locale = default_locale

    def get_locale_from_request(self, request_headers: Dict[str, str]) -> str:
        """从请求头中获取语言设置

        Args:
            request_headers: HTTP请求头

        Returns:
            str: 语言代码
        """
        # 从Accept-Language头解析
        accept_lang = request_headers.get("Accept-Language", "")
        if accept_lang:
            # 简单解析，取第一个语言
            lang = accept_lang.split(",")[0].split(";")[0].strip().lower()
            # 映射到支持的语言
            lang_map = {
                "zh-cn": "zh", "zh-tw": "zh", "zh-hk": "zh",
                "en-us": "en", "en-gb": "en",
                "ja-jp": "ja", "ko-kr": "ko",
                "de-de": "de", "fr-fr": "fr", "es-es": "es",
            }
            return lang_map.get(lang, lang) if lang in [l.value for l in Locale] else self.default_locale

        return self.default_locale

    def set_locale_for_request(self, request_headers: Dict[str, str]) -> str:
        """为当前请求设置语言

        Args:
            request_headers: HTTP请求头

        Returns:
            str: 设置的语言代码
        """
        locale = self.get_locale_from_request(request_headers)
        set_locale(locale)
        return locale


@dataclass
class LocalizedException(Exception):
    """本地化异常基类"""
    message_key: str
    locale: Optional[str] = None
    kwargs: Dict[str, Any] = None

    def __post_init__(self):
        self.kwargs = self.kwargs or {}
        self.message = get_message(self.message_key, self.locale, **self.kwargs)
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class MemoryNotFoundError(LocalizedException):
    """记忆不存在异常"""

    def __init__(self, memory_id: str, locale: Optional[str] = None):
        super().__init__("memory_not_found", locale, {"memory_id": memory_id})


class InvalidImportanceError(LocalizedException):
    """无效重要性级别异常"""

    def __init__(self, level: str, locale: Optional[str] = None):
        super().__init__("invalid_importance", locale, {"level": level})


class StorageFullError(LocalizedException):
    """存储已满异常"""

    def __init__(self, locale: Optional[str] = None):
        super().__init__("storage_full", locale)


class RateLimitExceededError(LocalizedException):
    """请求频率超限异常"""

    def __init__(self, locale: Optional[str] = None):
        super().__init__("rate_limit_exceeded", locale)


class UnauthorizedError(LocalizedException):
    """未授权异常"""

    def __init__(self, locale: Optional[str] = None):
        super().__init__("unauthorized", locale)


class ValidationError(LocalizedException):
    """验证错误异常"""

    def __init__(self, field: str, reason: str, locale: Optional[str] = None):
        super().__init__("validation_error", locale, {"field": field, "reason": reason})

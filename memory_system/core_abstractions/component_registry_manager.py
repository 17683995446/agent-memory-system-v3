"""
组件注册器 - 开闭原则，新增组件无需修改 from_config
"""

from typing import Dict, Type


_storage_registry: Dict[str, type] = {}
_embedding_registry: Dict[str, type] = {}
_retrieval_registry: Dict[str, type] = {}
_consolidation_registry: Dict[str, type] = {}
_forgetting_registry: Dict[str, type] = {}
_short_term_backend_registry: Dict[str, type] = {}
_importance_scorer_registry: Dict[str, type] = {}


def register_storage(name: str):
    """存储后端注册器装饰器"""
    def decorator(cls):
        _storage_registry[name] = cls
        return cls
    return decorator


def register_embedding(name: str):
    """嵌入提供者注册器装饰器"""
    def decorator(cls):
        _embedding_registry[name] = cls
        return cls
    return decorator


def register_retrieval(name: str):
    """检索策略注册器装饰器"""
    def decorator(cls):
        _retrieval_registry[name] = cls
        return cls
    return decorator


def register_consolidation(name: str):
    """整合策略注册器装饰器"""
    def decorator(cls):
        _consolidation_registry[name] = cls
        return cls
    return decorator


def register_forgetting(name: str):
    """遗忘策略注册器装饰器"""
    def decorator(cls):
        _forgetting_registry[name] = cls
        return cls
    return decorator


def register_short_term_backend(name: str):
    """短期记忆后端注册器装饰器"""
    def decorator(cls):
        _short_term_backend_registry[name] = cls
        return cls
    return decorator


def register_importance_scorer(name: str):
    """注册记忆重要性评估器"""
    def decorator(cls):
        _importance_scorer_registry[name] = cls
        return cls
    return decorator


def get_storage_registry() -> Dict[str, type]:
    """获取存储后端注册器"""
    return _storage_registry


def get_embedding_registry() -> Dict[str, type]:
    """获取嵌入提供者注册器"""
    return _embedding_registry


def get_retrieval_registry() -> Dict[str, type]:
    """获取检索策略注册器"""
    return _retrieval_registry


def get_consolidation_registry() -> Dict[str, type]:
    """获取整合策略注册器"""
    return _consolidation_registry


def get_forgetting_registry() -> Dict[str, type]:
    """获取遗忘策略注册器"""
    return _forgetting_registry


def get_short_term_backend_registry() -> Dict[str, type]:
    """获取短期记忆后端注册器"""
    return _short_term_backend_registry


def get_importance_scorer_registry() -> Dict[str, type]:
    """获取重要性评估器注册器"""
    return _importance_scorer_registry


def clear_all_registries() -> None:
    """清理所有注册表（用于测试环境）"""
    _storage_registry.clear()
    _embedding_registry.clear()
    _retrieval_registry.clear()
    _consolidation_registry.clear()
    _forgetting_registry.clear()
    _short_term_backend_registry.clear()
    _importance_scorer_registry.clear()

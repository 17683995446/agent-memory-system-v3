"""
过滤工具函数
统一处理记忆过滤和元数据分离逻辑，消除存储层重复代码
"""

from typing import Any, Dict, Set, Tuple

from memory_system.core_abstractions.memory_data_models import MemoryItem


def matches_filters(memory: MemoryItem, filters: Dict[str, Any]) -> bool:
    """
    检查记忆是否匹配过滤条件
    
    Args:
        memory: 记忆项
        filters: 过滤条件字典
            - memory_type: 记忆类型字符串
            - importance: 重要性整数值
            - metadata.xxx: 元数据字段
    
    Returns:
        是否匹配所有条件
    """
    for key, value in filters.items():
        if key == "memory_type" and memory.memory_type.value != value:
            return False
        if key == "importance" and memory.importance.value != value:
            return False
        if key.startswith("metadata."):
            meta_key = key[9:]
            if memory.metadata.get(meta_key) != value:
                return False
    return True


def separate_metadata(
    data: Dict[str, Any],
    system_keys: Set[str]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    将数据分离为系统字段和元数据字段
    
    Args:
        data: 原始数据字典
        system_keys: 系统保留键集合
    
    Returns:
        (系统字段字典, 元数据字段字典)
    """
    system_data = {}
    metadata = {}
    
    for key, value in data.items():
        if key in system_keys:
            system_data[key] = value
        else:
            metadata[key] = value
    
    return system_data, metadata


# 存储后端通用的系统键集合
STORAGE_SYSTEM_KEYS: Set[str] = {
    "memory_id", "content", "memory_type", "importance",
    "timestamp", "access_count", "last_access", "embedding"
}

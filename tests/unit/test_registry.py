"""
测试 memory_system/core/registry.py 中的注册器机制
"""

import pytest

# 导入 registry 模块本身
from memory_system.core_abstractions.component_registry_manager import (
    register_storage,
    register_embedding,
    get_storage_registry,
    get_embedding_registry,
)

# 导入 memory_storage 模块以触发 @register_storage("memory") 装饰器
import memory_system.storage_backends.in_memory_storage_backend


class TestRegistry:
    """注册器机制单元测试"""

    def test_register_and_get_storage(self):
        """注册一个自定义存储后端，验证能通过注册器获取"""

        @register_storage("custom_test_storage")
        class CustomStorage:
            pass

        registry = get_storage_registry()
        assert "custom_test_storage" in registry
        assert registry["custom_test_storage"] is CustomStorage

    def test_register_and_get_embedding(self):
        """注册一个自定义嵌入提供者"""

        @register_embedding("custom_test_embedding")
        class CustomEmbedding:
            pass

        registry = get_embedding_registry()
        assert "custom_test_embedding" in registry
        assert registry["custom_test_embedding"] is CustomEmbedding

    def test_register_duplicate_overwrites(self):
        """重复注册同名组件覆盖旧注册"""

        class FirstClass:
            pass

        class SecondClass:
            pass

        register_storage("dup_test")(FirstClass)
        register_storage("dup_test")(SecondClass)

        registry = get_storage_registry()
        assert registry["dup_test"] is SecondClass

    def test_get_unregistered_returns_none(self):
        """获取未注册的组件返回 None"""
        registry = get_storage_registry()
        # 确保一个不存在的名字不在注册器中
        assert registry.get("nonexistent_storage_xyz") is None

    def test_register_storage_decorator(self):
        """验证装饰器不改变原始类"""

        class OriginalClass:
            value = 42

        decorated = register_storage("decorator_test")(OriginalClass)
        assert decorated is OriginalClass
        assert decorated.value == 42

    def test_list_registered(self):
        """验证能列出所有已注册的组件名"""
        registry = get_storage_registry()
        names = list(registry.keys())
        assert isinstance(names, list)
        assert len(names) >= 1
        # memory_storage 模块导入了，至少有 "memory"
        assert "memory" in names

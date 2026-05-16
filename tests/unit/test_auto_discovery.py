"""
单元测试: _ensure_registrations() 自动发现机制
验证 MemorySystem 通过显式导入触发装饰器注册所有内置组件

注意: 由于 Python 模块缓存机制，一旦模块被导入，装饰器只会执行一次。
因此测试不应清除注册表后再期望重新导入能触发装饰器。
测试策略: 确保 _ensure_registrations() 被调用后，所有预期组件均已注册。
"""

import sys
import os
import inspect

import pytest

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from memory_system.core_abstractions.component_registry_manager import (
    _storage_registry,
    _embedding_registry,
    _retrieval_registry,
    _consolidation_registry,
    _forgetting_registry,
    get_storage_registry,
    get_embedding_registry,
    get_retrieval_registry,
    get_consolidation_registry,
    get_forgetting_registry,
)


def _snapshot_registries():
    """获取当前所有注册表的快照（用于幂等性比较）"""
    return {
        "storage": dict(_storage_registry),
        "embedding": dict(_embedding_registry),
        "retrieval": dict(_retrieval_registry),
        "consolidation": dict(_consolidation_registry),
        "forgetting": dict(_forgetting_registry),
    }


class TestEnsureRegistrationsLoadsAllComponents:
    """test_ensure_registrations_loads_all: 调用 _ensure_registrations() 后所有内置组件均已注册"""

    def test_all_storage_backends_registered(self):
        """验证 storage 注册表包含 memory, chroma(可选), qdrant(可选)"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        storage = get_storage_registry()
        assert "memory" in storage, "storage 注册表应包含 'memory'"
        # chroma 为可选依赖，仅在 chromadb 包已安装时注册
        if "chroma" not in storage:
            import importlib
            try:
                importlib.import_module("chromadb")
                assert False, "chromadb 已安装但未注册"
            except ImportError:
                pass  # chromadb 未安装，跳过断言
        else:
            assert "chroma" in storage
        # qdrant 为可选依赖，仅在 qdrant-client 包已安装时注册
        if "qdrant" not in storage:
            import importlib
            try:
                importlib.import_module("qdrant_client")
                assert False, "qdrant-client 已安装但未注册"
            except ImportError:
                pass  # qdrant-client 未安装，跳过断言
        else:
            assert "qdrant" in storage

    def test_all_embedding_providers_registered(self):
        """验证 embedding 注册表包含 sentence_transformers, bge; openai 可选"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        embedding = get_embedding_registry()
        assert "sentence_transformers" in embedding, "embedding 注册表应包含 'sentence_transformers'"
        assert "bge" in embedding, "embedding 注册表应包含 'bge'"
        # openai 为可选依赖，仅在 openai 包已安装时注册
        if "openai" not in embedding:
            import importlib
            try:
                importlib.import_module("openai")
            except ImportError:
                pass  # openai 未安装，跳过断言
            else:
                assert False, "openai 已安装但未注册到 embedding 注册表"

    def test_all_retrieval_strategies_registered(self):
        """验证 retrieval 注册表包含 hierarchical, vector_only"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        retrieval = get_retrieval_registry()
        assert "hierarchical" in retrieval, "retrieval 注册表应包含 'hierarchical'"
        assert "vector_only" in retrieval, "retrieval 注册表应包含 'vector_only'"

    def test_consolidation_strategy_registered(self):
        """验证 consolidation 注册表包含 time_based"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        consolidation = get_consolidation_registry()
        assert "time_based" in consolidation, "consolidation 注册表应包含 'time_based'"

    def test_forgetting_strategy_registered(self):
        """验证 forgetting 注册表包含 score_based"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        forgetting = get_forgetting_registry()
        assert "score_based" in forgetting, "forgetting 注册表应包含 'score_based'"

    def test_all_registries_non_empty(self):
        """所有五个注册表在 _ensure_registrations() 后均非空"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        assert len(_storage_registry) > 0, "storage 注册表不应为空"
        assert len(_embedding_registry) > 0, "embedding 注册表不应为空"
        assert len(_retrieval_registry) > 0, "retrieval 注册表不应为空"
        assert len(_consolidation_registry) > 0, "consolidation 注册表不应为空"
        assert len(_forgetting_registry) > 0, "forgetting 注册表不应为空"


class TestEnsureRegistrationsIdempotent:
    """test_ensure_registrations_idempotent: 多次调用 _ensure_registrations() 不会报错，注册表内容不变"""

    def test_call_twice_no_error(self):
        """连续调用两次 _ensure_registrations() 不抛出异常"""
        from memory_system.agent_memory_system_facade import MemorySystem

        # 第一次调用
        MemorySystem._ensure_registrations()
        # 第二次调用不应抛出任何异常
        MemorySystem._ensure_registrations()

    def test_call_twice_registry_unchanged(self):
        """连续调用两次后注册表内容与一次调用完全一致"""
        from memory_system.agent_memory_system_facade import MemorySystem

        # 确保已加载
        MemorySystem._ensure_registrations()
        snapshot_after_first = _snapshot_registries()

        # 再调用一次
        MemorySystem._ensure_registrations()
        snapshot_after_second = _snapshot_registries()

        # 比较每个注册表
        for category in snapshot_after_first:
            assert (
                snapshot_after_first[category] == snapshot_after_second[category]
            ), f"{category} 注册表在第二次调用后发生了变化"

    def test_call_three_times_stable(self):
        """连续调用三次后注册表仍然稳定"""
        from memory_system.agent_memory_system_facade import MemorySystem

        MemorySystem._ensure_registrations()
        MemorySystem._ensure_registrations()
        MemorySystem._ensure_registrations()

        # 验证关键条目仍然存在
        assert "memory" in _storage_registry
        assert "hierarchical" in _retrieval_registry
        assert "vector_only" in _retrieval_registry
        assert "time_based" in _consolidation_registry
        assert "score_based" in _forgetting_registry

    def test_repeated_calls_do_not_duplicate_entries(self):
        """多次调用不会导致注册表条目重复"""
        from memory_system.agent_memory_system_facade import MemorySystem

        MemorySystem._ensure_registrations()
        MemorySystem._ensure_registrations()
        MemorySystem._ensure_registrations()

        # 检查没有重复 key（字典本身不允许重复 key，但验证值未被覆盖）
        storage = get_storage_registry()
        # 每个注册名应该映射到一个唯一的类
        values = list(storage.values())
        assert len(values) == len(set(id(v) for v in values)), "storage 注册表不应有重复的类引用"


class TestRegistryMinimumContents:
    """test_registry_minimum_contents: 验证注册表中至少包含指定的最低组件集合"""

    def test_storage_has_memory(self):
        """storage 注册表至少包含 'memory'"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()
        assert "memory" in _storage_registry

    def test_retrieval_has_hierarchical(self):
        """retrieval 注册表至少包含 'hierarchical'"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()
        assert "hierarchical" in _retrieval_registry

    def test_retrieval_has_vector(self):
        """retrieval 注册表至少包含 'vector_only'"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()
        assert "vector_only" in _retrieval_registry

    def test_consolidation_has_time_based(self):
        """consolidation 注册表至少包含 'time_based'"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()
        assert "time_based" in _consolidation_registry

    def test_forgetting_has_score_based(self):
        """forgetting 注册表至少包含 'score_based'"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()
        assert "score_based" in _forgetting_registry


class TestEnsureRegistrationsIsStaticMethod:
    """test_ensure_registrations_is_static: 验证 _ensure_registrations 是静态方法，不依赖实例状态"""

    def test_callable_without_instance(self):
        """无需创建 MemorySystem 实例即可调用"""
        from memory_system.agent_memory_system_facade import MemorySystem

        # 直接通过类调用，不创建实例
        MemorySystem._ensure_registrations()

        # 验证注册表已被填充
        assert len(_storage_registry) > 0
        assert len(_retrieval_registry) > 0

    def test_no_class_level_flag_side_effects(self):
        """调用 _ensure_registrations 不会在类上留下意外状态"""
        from memory_system.agent_memory_system_facade import MemorySystem

        MemorySystem._ensure_registrations()

        # 静态方法不应修改类级别状态
        # 验证方法仍然是静态方法
        assert isinstance(
            inspect.getattr_static(MemorySystem, "_ensure_registrations"), staticmethod
        ), "_ensure_registrations 应该是静态方法"

    def test_static_method_does_not_require_self_or_cls(self):
        """静态方法签名不包含 self 或 cls 参数"""
        from memory_system.agent_memory_system_facade import MemorySystem
        import inspect

        # 通过 __dict__ 获取原始静态方法包装器，再取 __func__
        static_method = MemorySystem.__dict__["_ensure_registrations"]
        func = static_method.__func__
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        assert params == [], f"静态方法不应有参数，实际参数: {params}"


class TestRegistryValuesAreClasses:
    """test_registry_values_are_classes: 验证注册表中的值都是可调用的类"""

    def test_storage_values_are_classes(self):
        """storage 注册表中的值都是类"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        for name, cls in _storage_registry.items():
            assert isinstance(cls, type), f"storage['{name}'] 应该是类，实际是 {type(cls)}"

    def test_retrieval_values_are_classes(self):
        """retrieval 注册表中的值都是类"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        for name, cls in _retrieval_registry.items():
            assert isinstance(cls, type), f"retrieval['{name}'] 应该是类，实际是 {type(cls)}"

    def test_consolidation_values_are_classes(self):
        """consolidation 注册表中的值都是类"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        for name, cls in _consolidation_registry.items():
            assert isinstance(cls, type), f"consolidation['{name}'] 应该是类，实际是 {type(cls)}"

    def test_forgetting_values_are_classes(self):
        """forgetting 注册表中的值都是类"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        for name, cls in _forgetting_registry.items():
            assert isinstance(cls, type), f"forgetting['{name}'] 应该是类，实际是 {type(cls)}"

    def test_embedding_values_are_classes(self):
        """embedding 注册表中的值都是类"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        for name, cls in _embedding_registry.items():
            assert isinstance(cls, type), f"embedding['{name}'] 应该是类，实际是 {type(cls)}"


class TestEnsureRegistrationsImportsAllModules:
    """test_ensure_registrations_imports: 验证 _ensure_registrations() 导入了所有预期的模块"""

    def test_storage_modules_imported(self):
        """调用 _ensure_registrations() 后，所有存储模块已在 sys.modules 中"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        expected_modules = [
            "memory_system.storage_backends.in_memory_storage_backend",
            "memory_system.storage_backends.chroma_vector_storage_backend",
            "memory_system.storage_backends.qdrant_vector_storage_backend",
        ]
        for mod in expected_modules:
            assert mod in sys.modules, f"模块 {mod} 应在 sys.modules 中"

    def test_retrieval_modules_imported(self):
        """调用 _ensure_registrations() 后，所有检索模块已在 sys.modules 中"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        expected_modules = [
            "memory_system.retrieval_strategies.hierarchical_retrieval_strategy",
            "memory_system.retrieval_strategies.vector_only_retrieval_strategy",
        ]
        for mod in expected_modules:
            assert mod in sys.modules, f"模块 {mod} 应在 sys.modules 中"

    def test_consolidation_modules_imported(self):
        """调用 _ensure_registrations() 后，整合模块已在 sys.modules 中"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        assert "memory_system.consolidation_strategies.time_based_consolidation_strategy" in sys.modules

    def test_forgetting_modules_imported(self):
        """调用 _ensure_registrations() 后，遗忘模块已在 sys.modules 中"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        assert "memory_system.forgetting_strategies.score_based_forgetting_strategy" in sys.modules

    def test_embedding_modules_imported(self):
        """调用 _ensure_registrations() 后，嵌入模块已在 sys.modules 中（openai 可选）"""
        from memory_system.agent_memory_system_facade import MemorySystem
        MemorySystem._ensure_registrations()

        expected_modules = [
            "memory_system.embedding_providers.sentence_transformer_embedding_provider",
            "memory_system.embedding_providers.bge_embedding_provider",
        ]
        # openai 为可选依赖
        try:
            import importlib
            importlib.import_module("openai")
            expected_modules.append("memory_system.embedding_providers.openai_embedding_provider")
        except ImportError:
            pass

        for mod in expected_modules:
            assert mod in sys.modules, f"模块 {mod} 应在 sys.modules 中"

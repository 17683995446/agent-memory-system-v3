"""
插件系统
支持动态加载第三方扩展
"""

import importlib
import importlib.util
import inspect
import pkgutil
from pathlib import Path
from typing import Dict, List, Type, Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import (
        StorageBackend,
        EmbeddingProvider,
        RetrievalStrategy,
        ConsolidationStrategy,
        ForgettingStrategy
    )

from memory_system.core_abstractions.abstract_component_interfaces import (
    StorageBackend,
    EmbeddingProvider,
    RetrievalStrategy,
    ConsolidationStrategy,
    ForgettingStrategy
)


class PluginManager:
    """插件管理器"""

    def __init__(self):
        self._storages: Dict[str, Type[StorageBackend]] = {}
        self._embeddings: Dict[str, Type[EmbeddingProvider]] = {}
        self._retrievals: Dict[str, Type[RetrievalStrategy]] = {}
        self._consolidations: Dict[str, Type[ConsolidationStrategy]] = {}
        self._forgettings: Dict[str, Type[ForgettingStrategy]] = {}

    def discover_plugins(self, package_prefix: str = "memory_system_") -> List[str]:
        """
        自动发现插件包

        查找所有以 package_prefix 开头的已安装包
        """
        plugins = []
        for importer, modname, ispkg in pkgutil.iter_modules():
            if modname.startswith(package_prefix):
                plugins.append(modname)
        return plugins

    def load_plugin_module(self, module_name: str) -> Any:
        """加载插件模块"""
        try:
            return importlib.import_module(module_name)
        except ImportError as e:
            raise ImportError(f"无法加载插件 {module_name}: {e}")

    def load_plugin_from_path(self, path: str, module_name: str = None) -> Any:
        """从文件路径加载插件"""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"插件文件不存在: {path}")

        module_name = module_name or path.stem
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def register_from_module(self, module: Any):
        """从模块中自动注册组件"""
        for name, obj in inspect.getmembers(module):
            if not inspect.isclass(obj):
                continue

            # 检查是否有注册器装饰器标记
            if hasattr(obj, "_registered_name") and hasattr(obj, "_registered_type"):
                reg_type = obj._registered_type
                reg_name = obj._registered_name

                if reg_type == "storage":
                    self._storages[reg_name] = obj
                elif reg_type == "embedding":
                    self._embeddings[reg_name] = obj
                elif reg_type == "retrieval":
                    self._retrievals[reg_name] = obj
                elif reg_type == "consolidation":
                    self._consolidations[reg_name] = obj
                elif reg_type == "forgetting":
                    self._forgettings[reg_name] = obj

    def get_storage(self, name: str) -> Optional[Type["StorageBackend"]]:
        return self._storages.get(name)

    def get_embedding(self, name: str) -> Optional[Type["EmbeddingProvider"]]:
        return self._embeddings.get(name)

    def get_retrieval(self, name: str) -> Optional[Type["RetrievalStrategy"]]:
        return self._retrievals.get(name)

    def list_plugins(self) -> Dict[str, List[str]]:
        """列出所有已加载的插件"""
        return {
            "storages": list(self._storages.keys()),
            "embeddings": list(self._embeddings.keys()),
            "retrievals": list(self._retrievals.keys()),
            "consolidations": list(self._consolidations.keys()),
            "forgettings": list(self._forgettings.keys()),
        }


_VALID_PLUGIN_TYPES = {"storage", "embedding", "retrieval", "consolidation", "forgetting"}


def plugin(name: str, plugin_type: str):
    """
    插件装饰器

    用于标记第三方插件中的组件

    示例:
        @plugin("redis", "storage")
        class RedisStorage(StorageBackend):
            ...
    """
    if plugin_type not in _VALID_PLUGIN_TYPES:
        import warnings
        warnings.warn(f"未知的 plugin_type: {plugin_type}，有效值: {_VALID_PLUGIN_TYPES}")

    def decorator(cls):
        cls._registered_name = name
        cls._registered_type = plugin_type
        return cls
    return decorator


# 全局插件管理器实例
plugin_manager = PluginManager()

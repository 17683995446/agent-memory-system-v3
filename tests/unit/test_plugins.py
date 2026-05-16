"""
插件系统测试
"""

import pytest
import tempfile
import os

from memory_system.plugin_extension_manager import PluginManager, plugin


class TestPluginManager:
    """插件管理器测试"""

    def test_create_manager(self):
        """创建插件管理器"""
        manager = PluginManager()
        assert manager is not None

    def test_discover_plugins(self):
        """发现插件"""
        manager = PluginManager()
        plugins = manager.discover_plugins("memory_system_")
        # 返回列表（可能为空）
        assert isinstance(plugins, list)

    def test_list_plugins(self):
        """列出已加载插件"""
        manager = PluginManager()
        plugins = manager.list_plugins()
        assert isinstance(plugins, dict)
        assert "storages" in plugins
        assert "embeddings" in plugins
        assert "retrievals" in plugins

    def test_get_unregistered_storage(self):
        """获取未注册的存储"""
        manager = PluginManager()
        result = manager.get_storage("nonexistent")
        assert result is None

    def test_get_unregistered_embedding(self):
        """获取未注册的嵌入"""
        manager = PluginManager()
        result = manager.get_embedding("nonexistent")
        assert result is None

    def test_load_nonexistent_module(self):
        """加载不存在的模块"""
        manager = PluginManager()
        with pytest.raises(ImportError):
            manager.load_plugin_module("nonexistent_module_xyz")

    def test_load_plugin_from_nonexistent_path(self):
        """从不存在的路径加载"""
        manager = PluginManager()
        with pytest.raises(FileNotFoundError):
            manager.load_plugin_from_path("/nonexistent/path/plugin.py")

    def test_plugin_decorator(self):
        """测试插件装饰器"""
        @plugin("test_storage", "storage")
        class TestStorage:
            pass
        
        assert hasattr(TestStorage, "_registered_name")
        assert hasattr(TestStorage, "_registered_type")
        assert TestStorage._registered_name == "test_storage"
        assert TestStorage._registered_type == "storage"

    def test_register_from_module(self):
        """从模块注册组件"""
        manager = PluginManager()
        
        # 创建一个模拟模块
        class MockModule:
            @plugin("mock_storage", "storage")
            class MockStorage:
                pass
        
        manager.register_from_module(MockModule)
        
        # 验证注册成功
        storage = manager.get_storage("mock_storage")
        assert storage is not None
        assert storage.__name__ == "MockStorage"

    def test_load_plugin_from_path(self):
        """从文件路径加载插件"""
        manager = PluginManager()
        
        # 创建临时插件文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('''
from memory_system.plugin_extension_manager import plugin

@plugin("temp_storage", "storage")
class TempStorage:
    pass
''')
            plugin_path = f.name
        
        try:
            module = manager.load_plugin_from_path(plugin_path)
            assert module is not None
            assert hasattr(module, 'TempStorage')
        finally:
            os.unlink(plugin_path)

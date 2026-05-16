"""依赖注入容器

Agent Memory System V2.29.0 重构项目 - R-10
引入依赖注入容器，简化组件管理和测试
"""

from typing import Dict, Any, Callable, Type, TypeVar, Optional, List
from dataclasses import dataclass
from enum import Enum
import inspect
import threading

from memory_system.exceptions import DependencyInjectionError
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)

T = TypeVar('T')


class Scope(Enum):
    """依赖作用域"""
    SINGLETON = "singleton"      # 单例 - 全局唯一
    TRANSIENT = "transient"      # 瞬态 - 每次创建新实例
    SCOPED = "scoped"            # 作用域 - 同一作用域内共享


@dataclass
class Registration:
    """依赖注册信息"""
    name: str
    factory: Callable[..., Any]
    scope: Scope
    instance: Optional[Any] = None
    dependencies: Optional[List[str]] = None


class DIContainer:
    """简易依赖注入容器
    
    提供依赖注册、解析和管理功能，支持多种作用域。
    
    Example:
        container = DIContainer()
        
        # 注册单例
        container.register_singleton("config", load_config)
        
        # 注册工厂
        container.register_factory("storage", create_storage, scope=Scope.SINGLETON)
        
        # 注册类
        container.register_class("retrieval", RetrievalEngine)
        
        # 解析依赖
        storage = container.resolve("storage")
        
        # 自动注入
        engine = container.resolve("retrieval")  # 自动解析依赖参数
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """单例模式 - 全局容器实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._registrations: Dict[str, Registration] = {}
        self._scoped_instances: Dict[str, Dict[str, Any]] = {}
        self._current_scope: Optional[str] = None
        self._lock = threading.Lock()
    
    def register(
        self,
        name: str,
        factory: Callable[..., Any],
        scope: Scope = Scope.TRANSIENT,
        dependencies: Optional[List[str]] = None
    ) -> 'DIContainer':
        """注册依赖
        
        Args:
            name: 依赖名称
            factory: 工厂函数
            scope: 作用域
            dependencies: 依赖的其他组件名称列表
            
        Returns:
            容器实例（支持链式调用）
        """
        with self._lock:
            self._registrations[name] = Registration(
                name=name,
                factory=factory,
                scope=scope,
                dependencies=dependencies
            )
        logger.debug(f"Registered dependency: {name} ({scope.value})")
        return self
    
    def register_singleton(
        self,
        name: str,
        factory: Callable[..., Any],
        dependencies: Optional[List[str]] = None
    ) -> 'DIContainer':
        """注册单例依赖"""
        return self.register(name, factory, Scope.SINGLETON, dependencies)
    
    def register_transient(
        self,
        name: str,
        factory: Callable[..., Any],
        dependencies: Optional[List[str]] = None
    ) -> 'DIContainer':
        """注册瞬态依赖"""
        return self.register(name, factory, Scope.TRANSIENT, dependencies)
    
    def register_instance(self, name: str, instance: Any) -> 'DIContainer':
        """注册单例实例
        
        Args:
            name: 依赖名称
            instance: 实例对象
            
        Returns:
            容器实例
        """
        with self._lock:
            self._registrations[name] = Registration(
                name=name,
                factory=lambda: instance,
                scope=Scope.SINGLETON,
                instance=instance
            )
        logger.debug(f"Registered instance: {name}")
        return self
    
    def register_class(
        self,
        name: str,
        cls: Type[T],
        scope: Scope = Scope.TRANSIENT
    ) -> 'DIContainer':
        """注册类
        
        自动分析构造函数参数并注入依赖。
        
        Args:
            name: 依赖名称
            cls: 类
            scope: 作用域
            
        Returns:
            容器实例
        """
        # 分析构造函数参数
        sig = inspect.signature(cls.__init__)
        params = list(sig.parameters.items())[1:]  # 跳过self
        
        dependencies = []
        for param_name, param in params:
            if param.default is inspect.Parameter.empty:
                # 必需参数，尝试作为依赖解析
                dependencies.append(param_name)
        
        def factory():
            # 解析依赖
            kwargs = {}
            for dep_name in dependencies:
                try:
                    kwargs[dep_name] = self.resolve(dep_name)
                except DependencyInjectionError:
                    # 如果解析失败，可能是可选参数
                    pass
            return cls(**kwargs)
        
        return self.register(name, factory, scope, dependencies)
    
    def register_factory(
        self,
        name: str,
        factory: Callable[..., Any],
        scope: Scope = Scope.TRANSIENT
    ) -> 'DIContainer':
        """注册工厂（与register相同，更清晰语义）"""
        return self.register(name, factory, scope)
    
    def resolve(self, name: str) -> Any:
        """解析依赖
        
        Args:
            name: 依赖名称
            
        Returns:
            依赖实例
            
        Raises:
            DependencyInjectionError: 依赖不存在或解析失败
        """
        with self._lock:
            if name not in self._registrations:
                raise DependencyInjectionError(
                    message=f"No registration found for: {name}",
                    dependency_name=name
                )
            
            reg = self._registrations[name]
            
            # 根据作用域返回实例
            if reg.scope == Scope.SINGLETON:
                if reg.instance is None:
                    reg.instance = self._create_instance(reg)
                return reg.instance
            
            elif reg.scope == Scope.SCOPED:
                scope_id = self._current_scope or "default"
                if scope_id not in self._scoped_instances:
                    self._scoped_instances[scope_id] = {}
                
                if name not in self._scoped_instances[scope_id]:
                    self._scoped_instances[scope_id][name] = self._create_instance(reg)
                
                return self._scoped_instances[scope_id][name]
            
            else:  # TRANSIENT
                return self._create_instance(reg)
    
    def _create_instance(self, reg: Registration) -> Any:
        """创建实例"""
        try:
            # 解析依赖参数
            kwargs = {}
            if reg.dependencies:
                for dep_name in reg.dependencies:
                    kwargs[dep_name] = self.resolve(dep_name)
            
            return reg.factory(**kwargs)
        except Exception as e:
            raise DependencyInjectionError(
                message=f"Failed to create instance for {reg.name}: {str(e)}",
                dependency_name=reg.name
            )
    
    def try_resolve(self, name: str, default: Any = None) -> Any:
        """尝试解析依赖，失败返回默认值"""
        try:
            return self.resolve(name)
        except DependencyInjectionError:
            return default
    
    def resolve_required(self, name: str) -> Any:
        """解析必需依赖，失败抛出异常"""
        return self.resolve(name)
    
    def is_registered(self, name: str) -> bool:
        """检查依赖是否已注册"""
        return name in self._registrations
    
    def unregister(self, name: str) -> bool:
        """注销依赖"""
        with self._lock:
            if name in self._registrations:
                del self._registrations[name]
                # 清理作用域实例
                for scope in self._scoped_instances.values():
                    scope.pop(name, None)
                logger.debug(f"Unregistered dependency: {name}")
                return True
            return False
    
    def clear(self):
        """清空容器"""
        with self._lock:
            self._registrations.clear()
            self._scoped_instances.clear()
            self._current_scope = None
            logger.debug("Cleared DI container")
    
    def create_scope(self, scope_id: str) -> 'ScopeContext':
        """创建作用域上下文"""
        return ScopeContext(self, scope_id)
    
    def set_scope(self, scope_id: Optional[str]):
        """设置当前作用域"""
        self._current_scope = scope_id
    
    def get_registrations(self) -> List[str]:
        """获取所有注册名称"""
        return list(self._registrations.keys())
    
    def get_registration_info(self, name: str) -> Optional[Dict[str, Any]]:
        """获取注册信息"""
        if name not in self._registrations:
            return None
        
        reg = self._registrations[name]
        return {
            "name": reg.name,
            "scope": reg.scope.value,
            "has_instance": reg.instance is not None,
            "dependencies": reg.dependencies
        }


class ScopeContext:
    """作用域上下文管理器"""
    
    def __init__(self, container: DIContainer, scope_id: str):
        self.container = container
        self.scope_id = scope_id
        self.previous_scope: Optional[str] = None
    
    def __enter__(self):
        """进入作用域"""
        self.previous_scope = self.container._current_scope
        self.container.set_scope(self.scope_id)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出作用域"""
        self.container.set_scope(self.previous_scope)
        # 清理作用域实例
        if self.scope_id in self.container._scoped_instances:
            del self.container._scoped_instances[self.scope_id]


class ServiceProvider:
    """服务提供者 - 简化服务获取
    
    提供类型安全的服务获取接口。
    """
    
    def __init__(self, container: DIContainer):
        self._container = container
    
    def get_service(self, name: str) -> Any:
        """获取服务"""
        return self._container.resolve(name)
    
    def get_service_typed(self, name: str, service_type: Type[T]) -> T:
        """获取类型化的服务"""
        service = self._container.resolve(name)
        if not isinstance(service, service_type):
            raise TypeError(f"Service {name} is not of type {service_type}")
        return service


# 全局容器实例
container = DIContainer()


def get_container() -> DIContainer:
    """获取全局容器实例"""
    return container


def register(name: str, factory: Callable[..., Any], scope: Scope = Scope.TRANSIENT):
    """便捷函数：注册依赖"""
    return container.register(name, factory, scope)


def register_singleton(name: str, factory: Callable[..., Any]):
    """便捷函数：注册单例"""
    return container.register_singleton(name, factory)


def register_instance(name: str, instance: Any):
    """便捷函数：注册实例"""
    return container.register_instance(name, instance)


def resolve(name: str) -> Any:
    """便捷函数：解析依赖"""
    return container.resolve(name)


def inject(func: Callable) -> Callable:
    """依赖注入装饰器
    
    自动注入函数参数。
    
    Example:
        @inject
        def process_data(storage: StorageBackend, embedding: EmbeddingProvider):
            pass
    """
    sig = inspect.signature(func)
    
    def wrapper(*args, **kwargs):
        # 获取参数类型注解
        for param_name, param in sig.parameters.items():
            if param_name not in kwargs and param.default is inspect.Parameter.empty:
                # 尝试从容器解析
                type_hint = param.annotation
                if type_hint is not inspect.Parameter.empty:
                    # 使用类型名称作为依赖名
                    dep_name = type_hint.__name__.lower()
                    if container.is_registered(dep_name):
                        kwargs[param_name] = container.resolve(dep_name)
        
        return func(*args, **kwargs)
    
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


class Injectable:
    """可注入基类
    
    子类可以自动从容器获取依赖。
    
    Example:
        class MyService(Injectable):
            def __init__(self):
                self.storage = self.inject("storage")
    """
    
    _container: Optional[DIContainer] = None
    
    @classmethod
    def set_container(cls, container: DIContainer):
        """设置容器"""
        cls._container = container
    
    def inject(self, name: str) -> Any:
        """注入依赖"""
        if self._container is None:
            self._container = get_container()
        return self._container.resolve(name)

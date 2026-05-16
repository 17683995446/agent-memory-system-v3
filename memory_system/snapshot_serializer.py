"""统一快照序列化器

Agent Memory System V2.29.0 重构项目 - R-09
重构快照管理器的序列化逻辑，提供统一的序列化接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import json
import pickle
import gzip
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from memory_system.exceptions import SerializationError
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class SerializerType(str, Enum):
    """序列化器类型"""
    JSON = "json"
    MSGPACK = "msgpack"
    PICKLE = "pickle"


@dataclass
class SerializedData:
    """序列化后的数据包装"""
    data: bytes
    serializer_type: str
    compressed: bool
    timestamp: float
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "data_size": len(self.data),
            "serializer_type": self.serializer_type,
            "compressed": self.compressed,
            "timestamp": self.timestamp,
            "metadata": self.metadata or {}
        }


class SnapshotSerializer(ABC):
    """快照序列化器抽象基类
    
    定义统一的序列化和反序列化接口。
    """
    
    def __init__(self, compression: bool = False, compression_level: int = 6):
        self.compression = compression
        self.compression_level = compression_level
    
    @abstractmethod
    def serialize(self, data: Dict[str, Any]) -> bytes:
        """序列化数据
        
        Args:
            data: 要序列化的数据字典
            
        Returns:
            序列化后的字节数据
            
        Raises:
            SerializationError: 序列化失败
        """
        pass
    
    @abstractmethod
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        """反序列化数据
        
        Args:
            data: 序列化后的字节数据
            
        Returns:
            反序列化后的数据字典
            
        Raises:
            SerializationError: 反序列化失败
        """
        pass
    
    def _compress(self, data: bytes) -> bytes:
        """压缩数据"""
        if self.compression:
            return gzip.compress(data, compresslevel=self.compression_level)
        return data
    
    def _decompress(self, data: bytes) -> bytes:
        """解压数据"""
        if self.compression:
            return gzip.decompress(data)
        return data


class JSONSerializer(SnapshotSerializer):
    """JSON序列化器
    
    使用JSON格式进行序列化，具有良好的可读性和跨语言兼容性。
    """
    
    def __init__(self, compression: bool = False, compression_level: int = 6, indent: Optional[int] = None):
        super().__init__(compression, compression_level)
        self.indent = indent
    
    def serialize(self, data: Dict[str, Any]) -> bytes:
        """序列化为JSON"""
        try:
            json_str = json.dumps(data, ensure_ascii=False, default=self._json_default, indent=self.indent)
            bytes_data = json_str.encode('utf-8')
            return self._compress(bytes_data)
        except (TypeError, ValueError) as e:
            raise SerializationError(
                message=f"JSON serialization failed: {str(e)}",
                serializer="json"
            )
    
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        """从JSON反序列化"""
        try:
            decompressed = self._decompress(data)
            return json.loads(decompressed.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(
                message=f"JSON deserialization failed: {str(e)}",
                serializer="json"
            )
    
    @staticmethod
    def _json_default(obj):
        """处理无法直接序列化的类型"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class MsgPackSerializer(SnapshotSerializer):
    """MessagePack序列化器
    
    使用MessagePack格式进行二进制序列化，比JSON更紧凑、更快。
    """
    
    def __init__(self, compression: bool = False, compression_level: int = 6):
        super().__init__(compression, compression_level)
        self._msgpack = None
        self._try_import()
    
    def _try_import(self):
        """尝试导入msgpack"""
        try:
            import msgpack
            self._msgpack = msgpack
        except ImportError:
            logger.warning("msgpack not available, falling back to JSON")
    
    def serialize(self, data: Dict[str, Any]) -> bytes:
        """序列化为MessagePack"""
        if self._msgpack is None:
            # 回退到JSON
            return JSONSerializer(self.compression, self.compression_level).serialize(data)
        
        try:
            bytes_data = self._msgpack.packb(data, use_bin_type=True, default=self._msgpack_default)
            return self._compress(bytes_data)
        except Exception as e:
            raise SerializationError(
                message=f"MessagePack serialization failed: {str(e)}",
                serializer="msgpack"
            )
    
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        """从MessagePack反序列化"""
        if self._msgpack is None:
            # 回退到JSON
            return JSONSerializer(self.compression, self.compression_level).deserialize(data)
        
        try:
            decompressed = self._decompress(data)
            return self._msgpack.unpackb(decompressed, raw=False)
        except Exception as e:
            raise SerializationError(
                message=f"MessagePack deserialization failed: {str(e)}",
                serializer="msgpack"
            )
    
    @staticmethod
    def _msgpack_default(obj):
        """处理无法直接序列化的类型"""
        if isinstance(obj, datetime):
            return {"__datetime__": obj.isoformat()}
        if isinstance(obj, set):
            return {"__set__": list(obj)}
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        raise TypeError(f"Object of type {type(obj)} is not msgpack serializable")


class PickleSerializer(SnapshotSerializer):
    """Pickle序列化器
    
    使用Python的pickle进行序列化，支持几乎所有Python对象。
    注意：pickle不安全，不要反序列化不受信任的数据。
    """
    
    def __init__(self, compression: bool = False, compression_level: int = 6, protocol: int = pickle.HIGHEST_PROTOCOL):
        super().__init__(compression, compression_level)
        self.protocol = protocol
    
    def serialize(self, data: Dict[str, Any]) -> bytes:
        """序列化为Pickle"""
        try:
            bytes_data = pickle.dumps(data, protocol=self.protocol)
            return self._compress(bytes_data)
        except pickle.PicklingError as e:
            raise SerializationError(
                message=f"Pickle serialization failed: {str(e)}",
                serializer="pickle"
            )
    
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        """从Pickle反序列化"""
        try:
            decompressed = self._decompress(data)
            return pickle.loads(decompressed)
        except (pickle.UnpicklingError, EOFError) as e:
            raise SerializationError(
                message=f"Pickle deserialization failed: {str(e)}",
                serializer="pickle"
            )


class SerializerFactory:
    """序列化器工厂
    
    用于创建和管理序列化器实例。
    """
    
    _serializers: Dict[str, type] = {
        SerializerType.JSON: JSONSerializer,
        SerializerType.MSGPACK: MsgPackSerializer,
        SerializerType.PICKLE: PickleSerializer,
    }
    
    @classmethod
    def create(
        cls,
        serializer_type: str,
        compression: bool = False,
        **kwargs
    ) -> SnapshotSerializer:
        """创建序列化器
        
        Args:
            serializer_type: 序列化器类型 ("json", "msgpack", "pickle")
            compression: 是否启用压缩
            **kwargs: 额外的配置参数
            
        Returns:
            序列化器实例
            
        Raises:
            ValueError: 未知的序列化器类型
        """
        if serializer_type not in cls._serializers:
            raise ValueError(f"Unknown serializer type: {serializer_type}. "
                           f"Available: {list(cls._serializers.keys())}")
        
        serializer_class = cls._serializers[serializer_type]
        return serializer_class(compression=compression, **kwargs)
    
    @classmethod
    def register(cls, name: str, serializer_class: type):
        """注册自定义序列化器
        
        Args:
            name: 序列化器名称
            serializer_class: 序列化器类
        """
        cls._serializers[name] = serializer_class
        logger.debug(f"Registered serializer: {name}")
    
    @classmethod
    def list_serializers(cls) -> List[str]:
        """列出可用的序列化器"""
        return list(cls._serializers.keys())


class SnapshotSerializerManager:
    """快照序列化管理器
    
    统一管理快照的序列化和反序列化，支持多种格式和压缩选项。
    """
    
    def __init__(self, default_serializer: str = "json", default_compression: bool = False):
        self.default_serializer = default_serializer
        self.default_compression = default_compression
        self._cache: Dict[str, SerializedData] = {}
    
    def serialize(
        self,
        data: Dict[str, Any],
        serializer_type: Optional[str] = None,
        compression: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> SerializedData:
        """序列化数据
        
        Args:
            data: 要序列化的数据
            serializer_type: 序列化器类型，None使用默认值
            compression: 是否压缩，None使用默认值
            metadata: 附加元数据
            
        Returns:
            序列化后的数据包装
        """
        ser_type = serializer_type or self.default_serializer
        comp = compression if compression is not None else self.default_compression
        
        serializer = SerializerFactory.create(ser_type, compression=comp)
        serialized_bytes = serializer.serialize(data)
        
        serialized_data = SerializedData(
            data=serialized_bytes,
            serializer_type=ser_type,
            compressed=comp,
            timestamp=datetime.now().timestamp(),
            metadata=metadata
        )
        
        return serialized_data
    
    def deserialize(self, serialized_data: SerializedData) -> Dict[str, Any]:
        """反序列化数据
        
        Args:
            serialized_data: 序列化后的数据包装
            
        Returns:
            反序列化后的数据
        """
        serializer = SerializerFactory.create(
            serialized_data.serializer_type,
            compression=serialized_data.compressed
        )
        return serializer.deserialize(serialized_data.data)
    
    def serialize_to_bytes(
        self,
        data: Dict[str, Any],
        serializer_type: Optional[str] = None,
        compression: Optional[bool] = None
    ) -> bytes:
        """直接序列化为字节"""
        result = self.serialize(data, serializer_type, compression)
        return result.data
    
    def deserialize_from_bytes(
        self,
        data: bytes,
        serializer_type: str,
        compression: bool = False
    ) -> Dict[str, Any]:
        """从字节反序列化"""
        serialized_data = SerializedData(
            data=data,
            serializer_type=serializer_type,
            compressed=compression,
            timestamp=0
        )
        return self.deserialize(serialized_data)
    
    def get_serializer_info(self) -> Dict[str, Any]:
        """获取序列化器信息"""
        return {
            "default_serializer": self.default_serializer,
            "default_compression": self.default_compression,
            "available_serializers": SerializerFactory.list_serializers()
        }


# 便捷函数
def serialize_snapshot(
    data: Dict[str, Any],
    serializer_type: str = "json",
    compression: bool = False
) -> bytes:
    """便捷函数：序列化快照
    
    Args:
        data: 要序列化的数据
        serializer_type: 序列化器类型
        compression: 是否压缩
        
    Returns:
        序列化后的字节数据
    """
    manager = SnapshotSerializerManager(serializer_type, compression)
    return manager.serialize_to_bytes(data, serializer_type, compression)


def deserialize_snapshot(
    data: bytes,
    serializer_type: str = "json",
    compression: bool = False
) -> Dict[str, Any]:
    """便捷函数：反序列化快照
    
    Args:
        data: 序列化后的字节数据
        serializer_type: 序列化器类型
        compression: 是否压缩
        
    Returns:
        反序列化后的数据
    """
    manager = SnapshotSerializerManager(serializer_type, compression)
    return manager.deserialize_from_bytes(data, serializer_type, compression)

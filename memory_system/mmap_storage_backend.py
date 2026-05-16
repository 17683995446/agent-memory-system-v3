"""
内存映射存储后端模块

本模块实现了基于mmap的文件存储后端，支持零拷贝读取、高效的随机访问、
内存映射的持久化等功能。专为Agent Memory System的高性能存储需求设计。

核心特性:
- 基于mmap的文件存储
- 零拷贝读取支持
- 内存映射的持久化
- 高效的随机访问
- 读写锁并发控制
- 数据校验和压缩
- 自动扩容和回收

示例用法:
    >>> from mmap_storage_backend import MMapStorageBackend, MMapStorageConfig
    >>> 
    >>> config = MMapStorageConfig(
    ...     data_dir="./data",
    ...     initial_size=1024*1024*100,  # 100MB
    ... )
    >>> storage = MMapStorageBackend(config)
    >>> await storage.open()
    >>> 
    >>> # 写入数据 (key-value 模式)
    >>> offset = await storage.write("my_key", b"Hello, World!")
    >>> 
    >>> # 读取数据 (通过 key)
    >>> data = await storage.read("my_key")
    >>> print(data)  # b"Hello, World!"
    >>> 
    >>> # 删除数据
    >>> await storage.delete("my_key")
    >>> 
    >>> await storage.close()
"""

import asyncio
import hashlib
import json
import logging
import mmap
import os
import struct
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
import zlib


logger = logging.getLogger(__name__)


class StorageError(Exception):
    """存储错误基类"""
    pass


class StorageNotOpenError(StorageError):
    """存储未打开错误"""
    pass


class StorageFullError(StorageError):
    """存储已满错误"""
    pass


class CorruptionError(StorageError):
    """数据损坏错误"""
    pass


class RecordType(Enum):
    """记录类型枚举"""
    DATA = 0x01           # 普通数据
    INDEX = 0x02          # 索引数据
    METADATA = 0x03       # 元数据
    CHECKPOINT = 0x04     # 检查点
    DELETED = 0xFF        # 已删除


@dataclass
class RecordHeader:
    """
    记录头部
    
    Attributes:
        magic: 魔数(用于识别记录)
        record_type: 记录类型
        data_length: 数据长度
        data_checksum: 数据校验和
        timestamp: 时间戳
        sequence: 序列号
    """
    magic: int = 0x4D4D4150  # "MMAP"
    record_type: RecordType = RecordType.DATA
    data_length: int = 0
    data_checksum: int = 0
    timestamp: float = 0.0
    sequence: int = 0
    
    # 头部大小: magic(4) + type(1) + length(4) + checksum(4) + timestamp(8) + sequence(8) = 29 bytes
    HEADER_SIZE: int = field(default=29, init=False, repr=False)
    
    def pack(self) -> bytes:
        """打包为字节"""
        return struct.pack(
            '<IBIIQd',
            self.magic,
            self.record_type.value,
            self.data_length,
            self.data_checksum,
            self.sequence,
            self.timestamp,
        )
    
    @classmethod
    def unpack(cls, data: bytes) -> "RecordHeader":
        """从字节解包"""
        if len(data) < cls.HEADER_SIZE:
            raise ValueError(f"Invalid header size: {len(data)}")
        
        magic, type_val, length, checksum, sequence, timestamp = struct.unpack(
            '<IBIIQd', data[:cls.HEADER_SIZE]
        )
        
        return cls(
            magic=magic,
            record_type=RecordType(type_val),
            data_length=length,
            data_checksum=checksum,
            timestamp=timestamp,
            sequence=sequence,
        )
    
    def is_valid(self) -> bool:
        """检查头部是否有效"""
        return self.magic == 0x4D4D4150


@dataclass
class MMapStorageConfig:
    """
    内存映射存储配置
    
    Attributes:
        data_dir: 数据目录
        file_name: 数据文件名
        initial_size: 初始文件大小(字节)
        max_size: 最大文件大小(字节)
        growth_factor: 扩容因子
        enable_compression: 是否启用压缩
        compression_level: 压缩级别(1-9)
        enable_checksum: 是否启用校验和
        sync_on_write: 写入时是否同步
        cache_size: 缓存大小(记录数)
        readonly: 是否只读模式
    """
    data_dir: str = "./mmap_data"
    file_name: str = "data.mmap"
    initial_size: int = 100 * 1024 * 1024  # 100MB
    max_size: int = 10 * 1024 * 1024 * 1024  # 10GB
    growth_factor: float = 1.5
    enable_compression: bool = False
    compression_level: int = 6
    enable_checksum: bool = True
    sync_on_write: bool = False
    cache_size: int = 1000
    readonly: bool = False


class MMapStorageBackend:
    """
    内存映射存储后端
    
    实现基于mmap的高性能文件存储
    """
    
    def __init__(self, config: Optional[MMapStorageConfig] = None):
        self.config = config or MMapStorageConfig()
        
        # 文件路径
        self._data_path = Path(self.config.data_dir) / self.config.file_name
        self._index_path = Path(self.config.data_dir) / f"{self.config.file_name}.idx"
        
        # 文件对象
        self._file: Optional[Any] = None
        self._mmap: Optional[mmap.mmap] = None
        
        # 索引
        self._index: Dict[str, int] = {}  # key -> offset
        self._reverse_index: Dict[int, str] = {}  # offset -> key
        
        # 状态
        self._is_open = False
        self._write_position = 0
        self._sequence = 0
        self._file_size = 0
        
        # 锁
        self._lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        
        # 缓存
        self._cache: Dict[int, bytes] = {}
        self._cache_hits = 0
        self._cache_misses = 0
        
        # 统计
        self._stats = {
            "writes": 0,
            "reads": 0,
            "bytes_written": 0,
            "bytes_read": 0,
            "compactions": 0,
        }
    
    async def open(self):
        """打开存储"""
        async with self._lock:
            if self._is_open:
                return
            
            # 创建目录
            os.makedirs(self.config.data_dir, exist_ok=True)
            
            # 打开或创建数据文件
            if not self._data_path.exists():
                # 创建新文件
                with open(self._data_path, 'wb') as f:
                    f.write(b'\x00' * self.config.initial_size)
                self._file_size = self.config.initial_size
            else:
                # 获取现有文件大小
                self._file_size = self._data_path.stat().st_size
            
            # 打开文件
            mode = 'r+b' if not self.config.readonly else 'rb'
            self._file = open(self._data_path, mode)
            
            # 创建内存映射
            access = mmap.ACCESS_READ if self.config.readonly else mmap.ACCESS_WRITE
            self._mmap = mmap.mmap(self._file.fileno(), 0, access=access)
            
            # 加载索引
            await self._load_index()
            
            # 恢复写位置
            await self._recover_write_position()
            
            self._is_open = True
            logger.info(f"MMapStorageBackend opened: {self._data_path}")
    
    async def close(self):
        """关闭存储"""
        async with self._lock:
            if not self._is_open:
                return
            
            # 保存索引
            await self._save_index()
            
            # 关闭内存映射
            if self._mmap:
                self._mmap.flush()
                self._mmap.close()
                self._mmap = None
            
            # 关闭文件
            if self._file:
                self._file.close()
                self._file = None
            
            self._is_open = False
            logger.info("MMapStorageBackend closed")
    
    async def _load_index(self):
        """加载索引"""
        if not self._index_path.exists():
            return
        
        try:
            with open(self._index_path, 'r') as f:
                index_data = json.load(f)
            
            self._index = index_data.get("index", {})
            self._reverse_index = {v: k for k, v in self._index.items()}
            self._sequence = index_data.get("sequence", 0)
            
            logger.info(f"Loaded {len(self._index)} index entries")
        
        except Exception as e:
            logger.error(f"Failed to load index: {e}")
            self._index = {}
            self._reverse_index = {}
    
    async def _save_index(self):
        """保存索引"""
        try:
            index_data = {
                "index": self._index,
                "sequence": self._sequence,
                "timestamp": time.time(),
            }
            
            # 先写入临时文件，然后原子重命名
            temp_path = self._index_path.with_suffix('.tmp')
            with open(temp_path, 'w') as f:
                json.dump(index_data, f)
            
            temp_path.replace(self._index_path)
            
            logger.info(f"Saved {len(self._index)} index entries")
        
        except Exception as e:
            logger.error(f"Failed to save index: {e}")
    
    async def _recover_write_position(self):
        """恢复写位置"""
        if self.config.readonly:
            return
        
        # 从文件末尾向前扫描，找到最后一个有效记录
        pos = 0
        while pos < self._file_size:
            try:
                header_data = self._mmap[pos:pos + RecordHeader.HEADER_SIZE]
                if len(header_data) < RecordHeader.HEADER_SIZE:
                    break
                
                header = RecordHeader.unpack(header_data)
                if not header.is_valid():
                    break
                
                # 计算下一个记录位置
                next_pos = pos + RecordHeader.HEADER_SIZE + header.data_length
                if next_pos > self._file_size:
                    break
                
                pos = next_pos
                self._sequence = max(self._sequence, header.sequence)
            
            except Exception:
                break
        
        self._write_position = pos
        logger.info(f"Recovered write position: {self._write_position}")
    
    async def _ensure_space(self, size: int) -> bool:
        """确保有足够的空间"""
        required_size = self._write_position + RecordHeader.HEADER_SIZE + size
        
        if required_size > self._file_size:
            if self.config.readonly:
                return False
            
            # 计算新大小
            new_size = int(self._file_size * self.config.growth_factor)
            new_size = max(new_size, required_size)
            new_size = min(new_size, self.config.max_size)
            
            if new_size < required_size:
                raise StorageFullError("Storage file reached maximum size")
            
            # 扩容
            await self._expand_file(new_size)
        
        return True
    
    async def _expand_file(self, new_size: int):
        """扩展文件"""
        logger.info(f"Expanding file from {self._file_size} to {new_size}")
        
        # 关闭现有mmap
        if self._mmap:
            self._mmap.flush()
            self._mmap.close()
        
        # 扩展文件
        self._file.seek(new_size - 1)
        self._file.write(b'\x00')
        self._file.flush()
        
        # 重新创建mmap
        access = mmap.ACCESS_READ if self.config.readonly else mmap.ACCESS_WRITE
        self._mmap = mmap.mmap(self._file.fileno(), 0, access=access)
        
        self._file_size = new_size
    
    def _calculate_checksum(self, data: bytes) -> int:
        """计算校验和"""
        if not self.config.enable_checksum:
            return 0
        return zlib.crc32(data) & 0xFFFFFFFF
    
    def _verify_checksum(self, data: bytes, expected_checksum: int) -> bool:
        """验证校验和"""
        if not self.config.enable_checksum:
            return True
        return self._calculate_checksum(data) == expected_checksum
    
    async def write(self, key: str, data: bytes) -> int:
        """
        写入数据
        
        Args:
            key: 数据键
            data: 数据内容
            
        Returns:
            数据偏移位置
        """
        if not self._is_open:
            raise StorageNotOpenError("Storage is not open")
        
        if self.config.readonly:
            raise StorageError("Storage is read-only")
        
        async with self._lock:
            # 压缩数据
            if self.config.enable_compression:
                compressed = zlib.compress(data, self.config.compression_level)
                if len(compressed) < len(data):
                    data = compressed
            
            # 确保空间
            await self._ensure_space(len(data))
            
            # 创建记录头部
            self._sequence += 1
            header = RecordHeader(
                record_type=RecordType.DATA,
                data_length=len(data),
                data_checksum=self._calculate_checksum(data),
                timestamp=time.time(),
                sequence=self._sequence,
            )
            
            # 写入记录
            offset = self._write_position
            header_bytes = header.pack()
            
            self._mmap[offset:offset + len(header_bytes)] = header_bytes
            self._mmap[offset + len(header_bytes):offset + len(header_bytes) + len(data)] = data
            
            # 更新写位置
            self._write_position = offset + len(header_bytes) + len(data)
            
            # 更新索引
            old_offset = self._index.get(key)
            if old_offset is not None:
                del self._reverse_index[old_offset]
            
            self._index[key] = offset
            self._reverse_index[offset] = key
            
            # 更新缓存
            self._cache[offset] = data
            self._limit_cache_size()
            
            # 同步
            if self.config.sync_on_write:
                self._mmap.flush()
            
            # 更新统计
            self._stats["writes"] += 1
            self._stats["bytes_written"] += len(data)
            
            return offset
    
    async def read(self, key: str) -> Optional[bytes]:
        """
        读取数据
        
        Args:
            key: 数据键
            
        Returns:
            数据内容，如果不存在返回None
        """
        if not self._is_open:
            raise StorageNotOpenError("Storage is not open")
        
        async with self._read_lock:
            offset = self._index.get(key)
            if offset is None:
                return None
            
            return await self._read_at_offset(offset)
    
    async def _read_at_offset(self, offset: int) -> Optional[bytes]:
        """从指定偏移读取数据"""
        # 检查缓存
        if offset in self._cache:
            self._cache_hits += 1
            return self._cache[offset]
        
        self._cache_misses += 1
        
        try:
            # 读取头部
            header_data = self._mmap[offset:offset + RecordHeader.HEADER_SIZE]
            if len(header_data) < RecordHeader.HEADER_SIZE:
                return None
            
            header = RecordHeader.unpack(header_data)
            if not header.is_valid():
                return None
            
            # 读取数据
            data_start = offset + RecordHeader.HEADER_SIZE
            data_end = data_start + header.data_length
            data = self._mmap[data_start:data_end]
            
            # 验证校验和
            if not self._verify_checksum(data, header.data_checksum):
                raise CorruptionError(f"Data corruption at offset {offset}")
            
            # 解压数据
            if self.config.enable_compression:
                try:
                    data = zlib.decompress(data)
                except zlib.error:
                    # 可能未压缩
                    pass
            
            # 更新缓存
            self._cache[offset] = data
            self._limit_cache_size()
            
            # 更新统计
            self._stats["reads"] += 1
            self._stats["bytes_read"] += len(data)
            
            return data
        
        except Exception as e:
            logger.error(f"Read failed at offset {offset}: {e}")
            return None
    
    def _limit_cache_size(self):
        """限制缓存大小"""
        while len(self._cache) > self.config.cache_size:
            # 移除最旧的缓存项
            oldest = next(iter(self._cache))
            del self._cache[oldest]
    
    async def delete(self, key: str) -> bool:
        """
        删除数据
        
        Args:
            key: 数据键
            
        Returns:
            是否成功删除
        """
        if not self._is_open:
            raise StorageNotOpenError("Storage is not open")
        
        if self.config.readonly:
            raise StorageError("Storage is read-only")
        
        async with self._lock:
            offset = self._index.get(key)
            if offset is None:
                return False
            
            # 读取头部
            header_data = self._mmap[offset:offset + RecordHeader.HEADER_SIZE]
            header = RecordHeader.unpack(header_data)
            
            # 标记为已删除
            header.record_type = RecordType.DELETED
            self._mmap[offset:offset + RecordHeader.HEADER_SIZE] = header.pack()
            
            # 更新索引
            del self._index[key]
            del self._reverse_index[offset]
            
            # 从缓存移除
            if offset in self._cache:
                del self._cache[offset]
            
            return True
    
    async def exists(self, key: str) -> bool:
        """检查键是否存在"""
        return key in self._index
    
    async def keys(self) -> List[str]:
        """获取所有键"""
        return list(self._index.keys())
    
    async def compact(self):
        """
        压缩存储
        
        移除已删除的记录，整理碎片
        """
        if not self._is_open:
            raise StorageNotOpenError("Storage is not open")
        
        if self.config.readonly:
            raise StorageError("Storage is read-only")
        
        async with self._lock:
            logger.info("Starting compaction")
            
            # 创建新文件
            new_path = self._data_path.with_suffix('.compact')
            
            with open(new_path, 'wb') as new_file:
                new_offset = 0
                new_index = {}
                
                for key, old_offset in list(self._index.items()):
                    # 读取数据
                    data = await self._read_at_offset(old_offset)
                    if data is None:
                        continue
                    
                    # 写入新文件
                    self._sequence += 1
                    header = RecordHeader(
                        record_type=RecordType.DATA,
                        data_length=len(data),
                        data_checksum=self._calculate_checksum(data),
                        timestamp=time.time(),
                        sequence=self._sequence,
                    )
                    
                    new_file.write(header.pack())
                    new_file.write(data)
                    
                    new_index[key] = new_offset
                    new_offset += RecordHeader.HEADER_SIZE + len(data)
                
                new_file.flush()
            
            # 关闭当前存储
            if self._mmap:
                self._mmap.close()
            if self._file:
                self._file.close()
            
            # 替换文件
            backup_path = self._data_path.with_suffix('.backup')
            self._data_path.rename(backup_path)
            new_path.rename(self._data_path)
            backup_path.unlink()
            
            # 重新打开
            self._file = open(self._data_path, 'r+b')
            self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_WRITE)
            self._file_size = self._data_path.stat().st_size
            self._write_position = new_offset
            
            # 更新索引
            self._index = new_index
            self._reverse_index = {v: k for k, v in self._index.items()}
            await self._save_index()
            
            # 清空缓存
            self._cache.clear()
            
            self._stats["compactions"] += 1
            
            logger.info(f"Compaction completed: {len(self._index)} records")
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            **self._stats,
            "file_size": self._file_size,
            "write_position": self._write_position,
            "index_size": len(self._index),
            "cache_size": len(self._cache),
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_hit_rate": self._cache_hits / (self._cache_hits + self._cache_misses)
            if (self._cache_hits + self._cache_misses) > 0 else 0,
        }
    
    async def sync(self):
        """同步数据到磁盘"""
        if self._mmap:
            self._mmap.flush()
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())


class MMapStorageManager:
    """
    内存映射存储管理器
    
    管理多个存储后端，支持分片
    """
    
    def __init__(
        self,
        base_config: Optional[MMapStorageConfig] = None,
        num_shards: int = 1,
    ):
        self.base_config = base_config or MMapStorageConfig()
        self.num_shards = num_shards
        
        self._shards: Dict[int, MMapStorageBackend] = {}
        self._is_open = False
    
    def _get_shard_id(self, key: str) -> int:
        """根据键计算分片ID"""
        return hash(key) % self.num_shards
    
    async def open(self):
        """打开所有分片"""
        for shard_id in range(self.num_shards):
            config = MMapStorageConfig(
                data_dir=os.path.join(self.base_config.data_dir, f"shard_{shard_id}"),
                file_name=self.base_config.file_name,
                initial_size=self.base_config.initial_size // self.num_shards,
                max_size=self.base_config.max_size // self.num_shards,
                **{k: v for k, v in self.base_config.__dict__.items()
                   if k not in ['data_dir', 'file_name', 'initial_size', 'max_size']}
            )
            
            shard = MMapStorageBackend(config)
            await shard.open()
            self._shards[shard_id] = shard
        
        self._is_open = True
        logger.info(f"MMapStorageManager opened with {self.num_shards} shards")
    
    async def close(self):
        """关闭所有分片"""
        for shard in self._shards.values():
            await shard.close()
        
        self._shards.clear()
        self._is_open = False
        logger.info("MMapStorageManager closed")
    
    async def write(self, key: str, data: bytes) -> int:
        """写入数据"""
        shard_id = self._get_shard_id(key)
        shard = self._shards[shard_id]
        return await shard.write(key, data)
    
    async def read(self, key: str) -> Optional[bytes]:
        """读取数据"""
        shard_id = self._get_shard_id(key)
        shard = self._shards[shard_id]
        return await shard.read(key)
    
    async def delete(self, key: str) -> bool:
        """删除数据"""
        shard_id = self._get_shard_id(key)
        shard = self._shards[shard_id]
        return await shard.delete(key)
    
    async def exists(self, key: str) -> bool:
        """检查键是否存在"""
        shard_id = self._get_shard_id(key)
        shard = self._shards[shard_id]
        return await shard.exists(key)
    
    async def keys(self) -> List[str]:
        """获取所有键"""
        all_keys = []
        for shard in self._shards.values():
            all_keys.extend(await shard.keys())
        return all_keys
    
    async def compact(self):
        """压缩所有分片"""
        for shard in self._shards.values():
            await shard.compact()
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        total_stats = {
            "shards": self.num_shards,
            "total_writes": 0,
            "total_reads": 0,
            "total_bytes_written": 0,
            "total_bytes_read": 0,
        }
        
        for shard_id, shard in self._shards.items():
            stats = await shard.get_stats()
            total_stats["total_writes"] += stats["writes"]
            total_stats["total_reads"] += stats["reads"]
            total_stats["total_bytes_written"] += stats["bytes_written"]
            total_stats["total_bytes_read"] += stats["bytes_read"]
        
        return total_stats


# 测试代码
if __name__ == "__main__":
    async def test_mmap_storage():
        """测试内存映射存储"""
        logging.basicConfig(level=logging.INFO)
        
        config = MMapStorageConfig(
            data_dir="./test_mmap_data",
            initial_size=10 * 1024 * 1024,  # 10MB
            enable_compression=True,
            enable_checksum=True,
            cache_size=100,
        )
        
        storage = MMapStorageBackend(config)
        await storage.open()
        
        # 写入测试数据
        print("Writing test data...")
        test_data = {
            f"key_{i}": f"This is test data for key {i}, content: {'x' * 1000}".encode()
            for i in range(100)
        }
        
        for key, data in test_data.items():
            await storage.write(key, data)
        
        print(f"Written {len(test_data)} records")
        
        # 读取测试数据
        print("\nReading test data...")
        for key in list(test_data.keys())[:10]:
            data = await storage.read(key)
            if data:
                print(f"Read {key}: {len(data)} bytes")
            else:
                print(f"Failed to read {key}")
        
        # 删除部分数据
        print("\nDeleting some records...")
        for key in list(test_data.keys())[:20]:
            await storage.delete(key)
        
        # 获取统计
        print("\nStorage stats:")
        stats = await storage.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 压缩
        print("\nCompacting...")
        await storage.compact()
        
        # 再次获取统计
        print("\nStats after compaction:")
        stats = await storage.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        await storage.close()
        print("\nTest completed!")
    
    asyncio.run(test_mmap_storage())

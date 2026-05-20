"""第16轮深度实践 - 序列化安全边界测试

覆盖模块: snapshot_serializer.py
测试策略:
1. Pickle反序列化安全 - 恶意payload检测
2. JSON注入攻击 - 原型污染/特殊字符
3. 压缩炸弹 - gzip解压内存耗尽
4. 序列化循环引用 - 栈溢出检测
5. 跨序列化器兼容性 - 格式混淆攻击
6. 大数据序列化 - 内存边界
7. 损坏数据反序列化 - 鲁棒性
8. 并发序列化/反序列化 - 竞态条件
9. SerializerFactory注册表污染
10. SnapshotSerializerManager缓存一致性
"""

import asyncio
import gzip
import json
import os
import pickle
import sys
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pytest

# 确保项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory_system.snapshot_serializer import (
    JSONSerializer,
    MsgPackSerializer,
    PickleSerializer,
    SerializerFactory,
    SerializerType,
    SerializedData,
    SnapshotSerializerManager,
    serialize_snapshot,
    deserialize_snapshot,
)
from memory_system.exceptions import SerializationError


class TestPickleSecurityBoundary:
    """Pickle反序列化安全边界测试"""

    def test_pickle_deserialize_malformed_data(self):
        """BUG检测: 损坏的pickle数据反序列化应抛出SerializationError"""
        serializer = PickleSerializer()
        # 截断的pickle数据
        malformed = b'\x80\x04\x95\x05\x00\x00\x00\x00\x00\x00\x00'
        with pytest.raises(SerializationError):
            serializer.deserialize(malformed)

    def test_pickle_deserialize_empty_data(self):
        """空数据反序列化应抛出SerializationError"""
        serializer = PickleSerializer()
        with pytest.raises(SerializationError):
            serializer.deserialize(b'')

    def test_pickle_deserialize_random_bytes(self):
        """随机字节反序列化应抛出SerializationError"""
        serializer = PickleSerializer()
        # 使用较短的随机字节避免OverflowError
        random_bytes = os.urandom(64)
        with pytest.raises(SerializationError):
            serializer.deserialize(random_bytes)

    def test_pickle_deserialize_truncated_stream(self):
        """截断的gzip+pickle流应抛出SerializationError"""
        serializer = PickleSerializer(compression=True)
        valid_data = {"key": "value"}
        valid_bytes = serializer.serialize(valid_data)
        # 截断最后10字节
        truncated = valid_bytes[:-10]
        with pytest.raises(SerializationError):
            serializer.deserialize(truncated)

    def test_pickle_circular_reference(self):
        """Pickle处理循环引用"""
        serializer = PickleSerializer()
        data = {"a": 1}
        data["self"] = data  # 循环引用
        # pickle默认支持循环引用
        result_bytes = serializer.serialize(data)
        result = serializer.deserialize(result_bytes)
        assert result["a"] == 1
        assert result["self"]["a"] == 1

    def test_pickle_extremely_deep_nesting(self):
        """极深嵌套结构序列化 - 应抛出SerializationError（RecursionError）"""
        serializer = PickleSerializer()
        # 创建500层嵌套
        data = {"level": 0}
        current = data
        for i in range(1, 500):
            current["child"] = {"level": i}
            current = current["child"]
        # 深层嵌套应该被捕获为SerializationError
        with pytest.raises(SerializationError):
            serializer.serialize(data)


class TestJSONSecurityBoundary:
    """JSON序列化安全边界测试"""

    def test_json_injection_via_content(self):
        """JSON内容注入测试 - 特殊字符处理"""
        serializer = JSONSerializer()
        # 包含各种特殊字符的数据
        injection_data = {
            "normal": "hello",
            "xss_attempt": "<script>alert('xss')</script>",
            "sql_injection": "'; DROP TABLE memories; --",
            "path_traversal": "../../../etc/passwd",
            "null_byte": "hello\x00world",
            "unicode_trick": "\u0000\u0001\u0002",
            "template_injection": "{{config.__class__.__init__.__globals__}}",
            "very_long_key": "a" * 10000,
        }
        result_bytes = serializer.serialize(injection_data)
        result = serializer.deserialize(result_bytes)
        assert result["xss_attempt"] == "<script>alert('xss')</script>"
        assert result["sql_injection"] == "'; DROP TABLE memories; --"
        assert result["path_traversal"] == "../../../etc/passwd"
        # null byte在JSON中应被保留
        assert "\x00" in result["null_byte"]

    def test_json_deserialize_malformed(self):
        """损坏的JSON反序列化"""
        serializer = JSONSerializer()
        malformed_cases = [
            b'{"key": undefined}',
            b'{broken json',
            b'',
        ]
        for malformed in malformed_cases:
            with pytest.raises(SerializationError):
                serializer.deserialize(malformed)

    def test_json_rejects_nan_infinity(self):
        """拒绝NaN和Infinity（非标准JSON值）"""
        serializer = JSONSerializer()
        # NaN和Infinity不是标准JSON，应该被拒绝
        non_standard_cases = [
            b'{"key": NaN}',
            b'{"key": Infinity}',
            b'{"key": -Infinity}',
        ]
        for case in non_standard_cases:
            with pytest.raises(SerializationError):
                serializer.deserialize(case)

    def test_json_deserialize_corrupted_gzip(self):
        """损坏的gzip数据反序列化"""
        serializer = JSONSerializer(compression=True)
        with pytest.raises(SerializationError):
            serializer.deserialize(b'\x1f\x8b\x08\x00corrupted')

    def test_json_large_data_serialization(self):
        """大数据JSON序列化 - 10MB数据"""
        serializer = JSONSerializer(compression=True)
        # 生成约10MB的数据
        large_data = {f"key_{i}": f"value_{i}" * 100 for i in range(10000)}
        result_bytes = serializer.serialize(large_data)
        # 压缩后应该小于原始大小
        assert len(result_bytes) < 10 * 1024 * 1024
        result = serializer.deserialize(result_bytes)
        assert len(result) == 10000
        assert result["key_9999"] == "value_9999" * 100

    def test_json_non_string_keys(self):
        """非字符串键处理 - JSON要求键为字符串"""
        serializer = JSONSerializer()
        data = {1: "one", 2: "two"}
        result_bytes = serializer.serialize(data)
        result = serializer.deserialize(result_bytes)
        # JSON将数字键转为字符串
        assert result["1"] == "one"
        assert result["2"] == "two"

    def test_json_datetime_serialization_roundtrip(self):
        """datetime序列化往返一致性"""
        serializer = JSONSerializer()
        now = datetime(2025, 6, 15, 12, 30, 45, 123456)
        data = {"created_at": now, "items": [1, 2, 3]}
        result_bytes = serializer.serialize(data)
        result = serializer.deserialize(result_bytes)
        # datetime被转为ISO字符串
        assert isinstance(result["created_at"], str)
        assert "2025-06-15" in result["created_at"]


class TestCompressionBomb:
    """压缩炸弹防护测试"""

    def test_gzip_decompression_bomb_small_input_large_output(self):
        """检测: 小输入解压出大量数据"""
        serializer = JSONSerializer(compression=True)
        # 创建一个高度可压缩的大数据
        # 重复字符串压缩比极高
        bomb_data = {"data": "A" * 1000000}  # ~1MB可压缩为几KB
        compressed = serializer.serialize(bomb_data)
        # 压缩后应该远小于原始大小
        assert len(compressed) < 10000  # 应该小于10KB
        # 但能正确解压
        result = serializer.deserialize(compressed)
        assert len(result["data"]) == 1000000

    def test_corrupted_gzip_header(self):
        """损坏的gzip头部"""
        serializer = JSONSerializer(compression=True)
        # 有效的gzip头部但损坏的数据
        fake_gzip = b'\x1f\x8b\x08\x00' + os.urandom(100)
        with pytest.raises(SerializationError):
            serializer.deserialize(fake_gzip)

    def test_non_gzip_data_with_compression_enabled(self):
        """非gzip数据传入压缩模式"""
        serializer = JSONSerializer(compression=True)
        with pytest.raises(SerializationError):
            serializer.deserialize(b'this is not gzip data at all')


class TestSerializerFactorySecurity:
    """SerializerFactory安全测试"""

    def test_factory_unknown_type(self):
        """未知序列化器类型应抛出ValueError"""
        with pytest.raises(ValueError, match="Unknown serializer type"):
            SerializerFactory.create("xml")

    def test_factory_register_overwrite_builtin(self):
        """注册自定义序列化器覆盖内置类型"""
        class CustomJSONSerializer(JSONSerializer):
            pass

        original_count = len(SerializerFactory._serializers)
        SerializerFactory.register("json", CustomJSONSerializer)
        # 应该替换而非新增
        assert len(SerializerFactory._serializers) == original_count
        # 恢复
        SerializerFactory._serializers["json"] = JSONSerializer

    def test_factory_register_then_create(self):
        """注册后创建自定义序列化器"""
        class CustomSerializer(JSONSerializer):
            custom_flag = True

        SerializerFactory.register("custom_test", CustomSerializer)
        instance = SerializerFactory.create("custom_test")
        assert isinstance(instance, CustomSerializer)
        assert instance.custom_flag is True
        # 清理
        del SerializerFactory._serializers["custom_test"]

    def test_factory_list_serializers(self):
        """列出所有可用序列化器"""
        serializers = SerializerFactory.list_serializers()
        assert "json" in serializers
        assert "msgpack" in serializers
        assert "pickle" in serializers

    def test_factory_concurrent_register_race(self):
        """并发注册序列化器的竞态条件"""
        errors = []

        def register_serializer(i):
            try:
                SerializerFactory.register(f"concurrent_test_{i}", JSONSerializer)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=register_serializer, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 所有注册应该成功
        assert len(errors) == 0
        # 清理
        for i in range(100):
            SerializerFactory._serializers.pop(f"concurrent_test_{i}", None)


class TestSnapshotSerializerManagerDeep:
    """SnapshotSerializerManager深度测试"""

    def test_manager_serialize_deserialize_roundtrip_all_types(self):
        """所有序列化器类型的往返一致性"""
        test_data = {
            "string": "hello",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "null": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
            "set": {1, 2, 3},  # JSON会转为list
            "datetime": datetime(2025, 1, 1, 12, 0, 0),
        }

        for ser_type in ["json", "pickle"]:
            manager = SnapshotSerializerManager(default_serializer=ser_type)
            serialized = manager.serialize(test_data)
            assert isinstance(serialized, SerializedData)
            assert serialized.serializer_type == ser_type

            result = manager.deserialize(serialized)
            assert result["string"] == "hello"
            assert result["number"] == 42
            assert result["float"] == 3.14
            assert result["bool"] is True
            assert result["null"] is None
            assert result["list"] == [1, 2, 3]

    def test_manager_serialize_to_bytes_deserialize_from_bytes(self):
        """字节级别序列化往返"""
        manager = SnapshotSerializerManager(default_serializer="json", default_compression=True)
        data = {"key": "value", "nested": {"a": 1, "b": [1, 2, 3]}}
        bytes_data = manager.serialize_to_bytes(data)
        assert isinstance(bytes_data, bytes)

        result = manager.deserialize_from_bytes(bytes_data, "json", compression=True)
        assert result == data

    def test_manager_deserialize_with_wrong_compression_flag(self):
        """BUG检测: 压缩标志不匹配时应抛出异常"""
        manager = SnapshotSerializerManager(default_serializer="json")
        data = {"key": "value"}
        # 用压缩模式序列化
        compressed_bytes = manager.serialize_to_bytes(data, compression=True)
        # 用非压缩模式反序列化 - 应该失败
        with pytest.raises(SerializationError):
            manager.deserialize_from_bytes(compressed_bytes, "json", compression=False)

    def test_manager_get_serializer_info(self):
        """获取序列化器信息"""
        manager = SnapshotSerializerManager(default_serializer="json", default_compression=True)
        info = manager.get_serializer_info()
        assert info["default_serializer"] == "json"
        assert info["default_compression"] is True
        assert "json" in info["available_serializers"]

    def test_concurrent_serialize_deserialize(self):
        """并发序列化/反序列化安全性"""
        manager = SnapshotSerializerManager(default_serializer="json")
        errors = []
        results = {}

        async def serialize_deserialize(key, data):
            try:
                serialized = manager.serialize(data)
                result = manager.deserialize(serialized)
                results[key] = result
            except Exception as e:
                errors.append((key, str(e)))

        async def run_concurrent():
            tasks = []
            for i in range(100):
                data = {"key": f"value_{i}", "index": i, "data": "x" * 100}
                tasks.append(serialize_deserialize(i, data))
            await asyncio.gather(*tasks)

        asyncio.run(run_concurrent())
        assert len(errors) == 0
        assert len(results) == 100


class TestConvenienceFunctions:
    """便捷函数测试"""

    def test_serialize_deserialize_snapshot_json(self):
        """JSON便捷函数"""
        data = {"test": "data", "number": 42}
        bytes_data = serialize_snapshot(data, serializer_type="json")
        assert isinstance(bytes_data, bytes)
        result = deserialize_snapshot(bytes_data, serializer_type="json")
        assert result == data

    def test_serialize_deserialize_snapshot_pickle(self):
        """Pickle便捷函数"""
        data = {"test": "data", "complex": {1, 2, 3}}
        bytes_data = serialize_snapshot(data, serializer_type="pickle")
        result = deserialize_snapshot(bytes_data, serializer_type="pickle")
        assert result["test"] == "data"

    def test_serialize_deserialize_with_compression(self):
        """压缩便捷函数"""
        data = {"data": "A" * 10000}
        bytes_data = serialize_snapshot(data, serializer_type="json", compression=True)
        # 压缩后应更小
        assert len(bytes_data) < 10000
        result = deserialize_snapshot(bytes_data, serializer_type="json", compression=True)
        assert result["data"] == "A" * 10000

    def test_deserialize_corrupted_data(self):
        """反序列化损坏数据"""
        with pytest.raises(SerializationError):
            deserialize_snapshot(b'corrupted', serializer_type="json")


class TestSerializedDataClass:
    """SerializedData数据类测试"""

    def test_to_dict(self):
        """to_dict转换"""
        sd = SerializedData(
            data=b'hello',
            serializer_type="json",
            compressed=False,
            timestamp=1234567890.0,
            metadata={"version": "1.0"}
        )
        d = sd.to_dict()
        assert d["data_size"] == 5
        assert d["serializer_type"] == "json"
        assert d["compressed"] is False
        assert d["timestamp"] == 1234567890.0
        assert d["metadata"] == {"version": "1.0"}

    def test_to_dict_empty_metadata(self):
        """空metadata的to_dict"""
        sd = SerializedData(
            data=b'hello',
            serializer_type="json",
            compressed=True,
            timestamp=0,
        )
        d = sd.to_dict()
        assert d["metadata"] == {}


class TestMsgPackFallback:
    """MsgPack回退机制测试"""

    def test_msgpack_fallback_to_json_when_unavailable(self):
        """MsgPack不可用时回退到JSON"""
        serializer = MsgPackSerializer()
        # 如果msgpack不可用，应该回退到JSON
        data = {"key": "value", "number": 42}
        result_bytes = serializer.serialize(data)
        result = serializer.deserialize(result_bytes)
        assert result["key"] == "value"
        assert result["number"] == 42


class TestEdgeCases:
    """极端边界条件测试"""

    def test_serialize_empty_dict(self):
        """空字典序列化"""
        for ser_type in ["json", "pickle"]:
            manager = SnapshotSerializerManager(default_serializer=ser_type)
            serialized = manager.serialize({})
            result = manager.deserialize(serialized)
            assert result == {}

    def test_serialize_dict_with_none_values(self):
        """包含None值的字典"""
        manager = SnapshotSerializerManager(default_serializer="json")
        data = {"a": None, "b": "value", "c": None}
        serialized = manager.serialize(data)
        result = manager.deserialize(serialized)
        assert result["a"] is None
        assert result["c"] is None

    def test_serialize_unicode_data(self):
        """Unicode数据序列化"""
        manager = SnapshotSerializerManager(default_serializer="json")
        data = {
            "chinese": "你好世界",
            "japanese": "こんにちは",
            "korean": "안녕하세요",
            "arabic": "مرحبا",
            "emoji": "🎉🚀💡",
            "mixed": "Hello你好مرحبا",
        }
        serialized = manager.serialize(data)
        result = manager.deserialize(serialized)
        assert result["chinese"] == "你好世界"
        assert result["emoji"] == "🎉🚀💡"

    def test_serialize_very_long_string(self):
        """超长字符串序列化"""
        manager = SnapshotSerializerManager(default_serializer="json", default_compression=True)
        long_string = "x" * 5_000_000  # 5MB字符串
        data = {"content": long_string}
        start = time.time()
        serialized = manager.serialize(data)
        serialize_time = time.time() - start

        start = time.time()
        result = manager.deserialize(serialized)
        deserialize_time = time.time() - start

        assert len(result["content"]) == 5_000_000
        # 序列化和反序列化应该在合理时间内完成（<30秒）
        assert serialize_time < 30
        assert deserialize_time < 30

    def test_pickle_serialize_lambda_function(self):
        """Pickle序列化lambda函数"""
        serializer = PickleSerializer()
        data = {"func": lambda x: x + 1}
        # lambda默认不能被pickle序列化
        with pytest.raises(SerializationError):
            serializer.serialize(data)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

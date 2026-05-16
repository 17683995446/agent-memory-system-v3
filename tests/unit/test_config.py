"""
测试 config/settings.py 中的配置管理
"""

import os
import json
import tempfile
import pytest

from memory_system.configuration_settings.memory_system_configuration import MemoryConfig


class TestDefaultConfig:
    """test_default_config: 默认配置创建"""

    def test_default_config(self):
        config = MemoryConfig()
        assert config.storage.backend == "chroma"
        assert config.embedding.provider == "sentence_transformers"
        assert config.short_term.capacity == 7
        assert config.short_term.ttl_seconds == 300
        assert config.retrieval.strategy == "hierarchical"
        assert config.consolidation.enabled is True
        assert config.forgetting.enabled is True


class TestFromYaml:
    """test_from_yaml: 创建临时 YAML 文件，加载验证"""

    def test_from_yaml(self):
        yaml_content = """
storage:
  backend: memory
  max_memories: 500
embedding:
  provider: openai
  model_name: text-embedding-3-small
short_term:
  capacity: 10
  ttl_seconds: 600
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write(yaml_content)
            filepath = f.name

        try:
            config = MemoryConfig.from_yaml(filepath)
            assert config.storage.backend == "memory"
            assert config.storage.max_memories == 500
            assert config.embedding.provider == "openai"
            assert config.embedding.model_name == "text-embedding-3-small"
            assert config.short_term.capacity == 10
            assert config.short_term.ttl_seconds == 600
        finally:
            os.unlink(filepath)


class TestFromYamlFileNotFound:
    """test_from_yaml_file_not_found: 文件不存在抛出 FileNotFoundError"""

    def test_from_yaml_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            MemoryConfig.from_yaml("/nonexistent/path/config.yaml")


class TestFromYamlEmptyFile:
    """test_from_yaml_empty_file: 空文件抛出 ValueError"""

    def test_from_yaml_empty_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("")
            filepath = f.name

        try:
            with pytest.raises(ValueError):
                MemoryConfig.from_yaml(filepath)
        finally:
            os.unlink(filepath)


class TestFromJson:
    """test_from_json: 创建临时 JSON 文件，加载验证"""

    def test_from_json(self):
        json_data = {
            "storage": {"backend": "qdrant", "max_memories": 2000},
            "embedding": {"provider": "bge", "model_name": "BAAI/bge-large-zh"},
            "short_term": {"capacity": 5, "ttl_seconds": 120},
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            json.dump(json_data, f)
            filepath = f.name

        try:
            config = MemoryConfig.from_json(filepath)
            assert config.storage.backend == "qdrant"
            assert config.storage.max_memories == 2000
            assert config.embedding.provider == "bge"
            assert config.embedding.model_name == "BAAI/bge-large-zh"
            assert config.short_term.capacity == 5
            assert config.short_term.ttl_seconds == 120
        finally:
            os.unlink(filepath)


class TestFromEnv:
    """test_from_env: 设置环境变量，加载验证"""

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("MEMORY_STORAGE_BACKEND", "memory")
        monkeypatch.setenv("MEMORY_CHROMA_PATH", "/tmp/test_chroma")
        monkeypatch.setenv("MEMORY_EMBEDDING_PROVIDER", "openai")
        monkeypatch.setenv("MEMORY_EMBEDDING_MODEL", "text-embedding-3-large")

        config = MemoryConfig.from_env()
        assert config.storage.backend == "memory"
        assert config.storage.chroma_path == "/tmp/test_chroma"
        assert config.embedding.provider == "openai"
        assert config.embedding.model_name == "text-embedding-3-large"


class TestToYamlAndToJson:
    """test_to_yaml_and_to_json: 保存并重新加载"""

    def test_to_yaml_and_to_json(self):
        config = MemoryConfig()
        config.storage.backend = "memory"
        config.storage.max_memories = 999
        config.short_term.capacity = 3

        # 保存为 YAML 并重新加载
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", delete=False
        ) as f:
            yaml_path = f.name
        config.to_yaml(yaml_path)
        try:
            loaded_yaml = MemoryConfig.from_yaml(yaml_path)
            assert loaded_yaml.storage.backend == "memory"
            assert loaded_yaml.storage.max_memories == 999
            assert loaded_yaml.short_term.capacity == 3
        finally:
            os.unlink(yaml_path)

        # 保存为 JSON 并重新加载
        with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        ) as f:
            json_path = f.name
        config.to_json(json_path)
        try:
            loaded_json = MemoryConfig.from_json(json_path)
            assert loaded_json.storage.backend == "memory"
            assert loaded_json.storage.max_memories == 999
            assert loaded_json.short_term.capacity == 3
        finally:
            os.unlink(json_path)

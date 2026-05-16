"""
CLI 命令系统测试
"""

import pytest
import tempfile
import os

from typer.testing import CliRunner

from memory_system.command_line_interface import app


runner = CliRunner()


class TestCLI:
    """CLI 命令测试"""

    def test_version(self):
        """测试 --version 选项"""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "Agent Memory System" in result.output

    def test_init(self):
        """测试 init 命令"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "test_config.yaml")
            result = runner.invoke(app, ["init", "--config", config_path, "--backend", "memory"])
            assert result.exit_code == 0
            assert "配置文件已创建" in result.output
            assert os.path.exists(config_path)

    def test_init_with_embedding(self):
        """测试 init 命令指定嵌入模型"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "test_config2.yaml")
            result = runner.invoke(app, [
                "init", 
                "--config", config_path, 
                "--backend", "memory",
                "--embedding", "sentence_transformers"
            ])
            assert result.exit_code == 0

    def test_remember_missing_config(self):
        """测试 remember 命令配置文件不存在"""
        result = runner.invoke(app, [
            "remember", 
            "测试内容",
            "--config", "/nonexistent/config.yaml"
        ])
        # 应该报错退出
        assert result.exit_code != 0

    def test_stats_missing_config(self):
        """测试 stats 命令配置文件不存在"""
        result = runner.invoke(app, [
            "stats",
            "--config", "/nonexistent/config.yaml"
        ])
        assert result.exit_code != 0

    def test_clear_force(self):
        """测试 clear 命令强制清空"""
        # 检查是否缺少必要的依赖
        try:
            import openai  # noqa: F401
        except ImportError:
            pytest.skip("需要 openai 模块")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "clear_config.yaml")
            # 先创建配置
            runner.invoke(app, ["init", "--config", config_path, "--backend", "memory"])
            # 强制清空
            result = runner.invoke(app, ["clear", "--config", config_path, "--force"])
            assert result.exit_code == 0
            assert "短期记忆已清空" in result.output

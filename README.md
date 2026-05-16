# Agent Memory System V2

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**高度模块化的 Agent 长短期记忆系统** — 基于开源工具集构建，每个模块都可独立产品化。

## ✨ 核心特性

- 🧠 **分层记忆架构** — 短期记忆(L1) + 情景记忆(L2) + 语义记忆(L3)
- ⚡ **快速检索** — 四层检索策略，从 O(1) 到 O(log n)
- 🔌 **高度模块化** — 注册器 + 插件双扩展机制
- 🛠️ **开源工具集成** — 12+ 开源工具无缝集成
- 📊 **可观测性** — Prometheus 指标 + Structlog 日志
- 🖥️ **CLI 产品** — Typer 命令行工具开箱即用
- 🌳 **AST 代码记忆** — tree-sitter 多语言代码解析

## 📦 安装

```bash
# 核心依赖
pip install pydantic cachetools tenacity

# 向量存储 (选择一个或多个)
pip install chromadb           # Chroma
pip install qdrant-client      # Qdrant
pip install faiss-cpu          # FAISS ANN

# 嵌入模型 (选择一个或多个)
pip install sentence-transformers  # Sentence-Transformers
pip install FlagEmbedding          # BGE (智源)

# 可选增强
pip install prometheus-client  # 监控指标
pip install structlog          # 结构化日志
pip install typer rich         # CLI 工具
```

## 🚀 快速开始

```python
from memory_system import MemorySystem, MemoryConfig

# 方式1: 使用默认配置
config = MemoryConfig()
memory = MemorySystem.from_config(config)

# 方式2: 自定义配置
config = MemoryConfig()
config.storage.backend = "memory"
config.embedding.provider = "sentence_transformers"
config.short_term.capacity = 10
memory = MemorySystem.from_config(config)

# 存储记忆
await memory.remember("用户喜欢Python", importance="high")
await memory.remember("用户正在调试内存泄漏", importance="medium")

# 批量存储 (使用批量嵌入优化)
await memory.remember_batch([
    "用户偏好: 简洁代码风格",
    "用户偏好: 类型提示",
    "用户偏好: 单元测试"
], importance="high")

# 检索记忆
results = await memory.recall("用户偏好", max_results=5)
for r in results["combined"]:
    print(f"{r['score']:.3f}: {r['memory']['content']}")

# 获取统计
stats = await memory.get_stats()
print(f"总记忆数: {stats.total_count}")

# 关闭
await memory.close()
```

## 🏗️ 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                      MemorySystem                               │
│                     (统一接口层)                                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Short Term  │  │   Storage    │  │     Embedding        │  │
│  │   Memory     │  │    Layer     │  │       Layer          │  │
│  │  (工作记忆)   │  │  (存储层)     │  │     (嵌入层)          │  │
│  ├──────────────┤  ├──────────────┤  ├──────────────────────┤  │
│  │ • 容量: 7    │  │ • Chroma     │  │ • BGE (智源)         │  │
│  │ • TTL: 5min  │  │ • Qdrant     │  │ • Sentence-Transform │  │
│  │ • O(1)访问   │  │ • FAISS ANN  │  │ • OpenAI             │  │
│  │ • cachetools │  │ • Memory     │  │                      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌────────────────────────────────────┐  │
│  │ Retrieval Layer  │  │     Consolidation Layer            │  │
│  │    (检索层)       │  │         (整合层)                    │  │
│  ├──────────────────┤  ├────────────────────────────────────┤  │
│  │ • Hierarchical   │  │ • Time-Based Consolidation         │  │
│  │   - L1: 短期记忆  │  │ • Score-Based Forgetting           │  │
│  │   - L2: 时间检索  │  │ • 可配置权重/衰减参数               │  │
│  │   - L3: 向量检索  │  │                                    │  │
│  │   - L4: 重要性    │  │                                    │  │
│  └──────────────────┘  └────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 🔌 模块扩展

### 注册器模式

```python
from memory_system import register_storage, StorageBackend

@register_storage("my_backend")
class MyStorage(StorageBackend):
    # 实现接口方法...
    pass

# 配置中使用
config.storage.backend = "my_backend"
```

### 插件系统

```python
from memory_system import plugin, PluginManager

@plugin("redis", "storage")
class RedisStorage:
    pass

# 动态加载
manager = PluginManager()
manager.load_plugin_from_path("/path/to/plugin.py")
```

## 🖥️ CLI 使用

```bash
# 初始化配置
ams init --backend chroma --embedding sentence_transformers

# 存储记忆
ams remember "用户喜欢Python" --type semantic --importance high

# 检索记忆
ams recall "用户偏好" --max-results 10

# 查看统计
ams stats

# 清空短期记忆
ams clear --force

# 查看版本
ams --version
```

## 🌳 AST 代码记忆

```python
from memory_system.code import CodeMemory

cm = CodeMemory()

# 自动检测语言 + AST 解析
result = cm.store("""
def hello(name):
    print(f"Hello, {name}!")

class Greeter:
    def greet(self):
        return "Hi!"
""", filename="hello.py")

print(result["structure"])
# {
#   "functions": [{"name": "hello"}, {"name": "greet"}],
#   "classes": [{"name": "Greeter"}],
#   "imports": []
# }
```

支持语言: Python, JavaScript, TypeScript, Java, C, C++, Go, Rust, Ruby, PHP, Kotlin, Swift, C#, Bash, SQL, JSON, YAML, HTML, CSS

## 📊 监控指标

```python
from memory_system import MemoryMetrics

metrics = MemoryMetrics()
metrics.record_remember("semantic", "high", 0.1)
metrics.update_memory_count("semantic", 100)

# Prometheus 格式输出
# memory_added_total{memory_type="semantic",importance="high"} 1
# memory_count{memory_type="semantic"} 100
```

## 📈 性能基准

| 操作 | 延迟 | OPS |
|------|------|-----|
| 短期记忆添加 | ~0.01ms | 100K/s |
| 内存存储添加 | ~0.09ms | 11K/s |
| 语义记忆存储 | ~0.13ms | 7.7K/s |
| 批量嵌入(10条) | ~0.5ms | 20K/s |
| FAISS ANN搜索 | ~0.02ms | 50K/s |

## 🧪 测试

```bash
# 运行全部测试
pytest tests/ -v

# 仅单元测试
pytest tests/unit/ -v

# 性能基准
pytest tests/benchmark/ -v
```

## 📄 License

MIT License

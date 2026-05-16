"""
Agent Memory System CLI
命令行工具产品化
"""

import asyncio
import json
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from memory_system import MemorySystem, MemoryConfig
from memory_system.configuration_settings.memory_system_configuration import DEFAULT_CONFIG_YAML
from memory_system import __version__

app = typer.Typer(help="Agent Memory System - 长短期记忆管理工具")
console = Console()


def version_callback(value: bool):
    """显示版本信息"""
    if value:
        console.print(f"[cyan]Agent Memory System[/cyan] version: [green]{__version__}[/green]")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="显示版本信息"
    ),
):
    """Agent Memory System CLI"""
    pass


@app.command()
def init(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="配置文件路径"),
    backend: str = typer.Option("memory", "--backend", "-b", help="存储后端: memory/chroma/qdrant"),
    embedding: str = typer.Option("sentence_transformers", "--embedding", "-e", help="嵌入模型"),
):
    """初始化配置文件"""
    config = MemoryConfig()
    config.storage.backend = backend
    config.embedding.provider = embedding
    config.to_yaml(config_path)
    console.print(Panel(f"[green]配置文件已创建: {config_path}[/green]", box=box.ROUNDED))


@app.command()
def remember(
    content: str = typer.Argument(..., help="记忆内容"),
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="配置文件路径"),
    memory_type: str = typer.Option("auto", "--type", "-t", help="记忆类型: short_term/episodic/semantic/auto"),
    importance: str = typer.Option("medium", "--importance", "-i", help="重要性: trivial/low/medium/high/critical"),
    metadata: Optional[str] = typer.Option(None, "--metadata", "-m", help="JSON格式的元数据"),
):
    """存储一条记忆"""
    async def _remember():
        config = MemoryConfig.from_yaml(config_path)
        memory = MemorySystem.from_config(config)

        meta = {}
        if metadata:
            meta = json.loads(metadata)

        memory_id = await memory.remember(content, memory_type=memory_type, importance=importance, metadata=meta)
        console.print(Panel(f"[green]记忆已存储[/green]\nID: {memory_id}\n内容: {content[:50]}...", box=box.ROUNDED))
        await memory.close()

    asyncio.run(_remember())


@app.command()
def recall(
    query: str = typer.Argument(..., help="查询内容"),
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="配置文件路径"),
    max_results: int = typer.Option(5, "--max-results", "-n", help="最大结果数"),
):
    """检索记忆"""
    async def _recall():
        config = MemoryConfig.from_yaml(config_path)
        memory = MemorySystem.from_config(config)

        results = await memory.recall(query, max_results=max_results)

        table = Table(title=f"检索结果: '{query}'", box=box.ROUNDED)
        table.add_column("排名", style="cyan", justify="right")
        table.add_column("类型", style="magenta")
        table.add_column("重要性", style="yellow")
        table.add_column("内容", style="green")
        table.add_column("分数", style="blue", justify="right")

        for i, result in enumerate(results["combined"][:max_results], 1):
            m = result["memory"]
            table.add_row(
                str(i),
                m["memory_type"],
                m["importance"],
                m["content"][:50] + "..." if len(m["content"]) > 50 else m["content"],
                f"{result['score']:.3f}"
            )

        console.print(table)
        await memory.close()

    asyncio.run(_recall())


@app.command()
def stats(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="配置文件路径"),
):
    """查看系统统计信息"""
    async def _stats():
        config = MemoryConfig.from_yaml(config_path)
        memory = MemorySystem.from_config(config)

        stats = await memory.get_stats()

        table = Table(title="系统统计", box=box.ROUNDED)
        table.add_column("指标", style="cyan")
        table.add_column("数值", style="green", justify="right")

        table.add_row("短期记忆数", str(stats.short_term_count))
        table.add_row("情景记忆数", str(stats.episodic_count))
        table.add_row("语义记忆数", str(stats.semantic_count))
        table.add_row("总记忆数", str(stats.total_count))
        table.add_row("累计添加", str(stats.total_added))
        table.add_row("累计检索", str(stats.total_retrieved))
        table.add_row("运行时间(小时)", f"{stats.uptime_hours:.2f}")

        console.print(table)
        await memory.close()

    asyncio.run(_stats())


@app.command()
def clear(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="配置文件路径"),
    force: bool = typer.Option(False, "--force", "-f", help="强制清空，不提示确认"),
):
    """清空短期记忆"""
    async def _clear():
        config = MemoryConfig.from_yaml(config_path)
        memory = MemorySystem.from_config(config)
        await memory.clear_short_term()
        console.print(Panel("[green]短期记忆已清空[/green]", box=box.ROUNDED))
        await memory.close()

    if not force:
        confirm = typer.confirm("确定要清空短期记忆吗？")
        if not confirm:
            console.print("[yellow]操作已取消[/yellow]")
            return

    asyncio.run(_clear())


if __name__ == "__main__":
    app()

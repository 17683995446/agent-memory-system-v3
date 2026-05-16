"""
基础使用示例
展示MemorySystem的核心功能
"""

import asyncio
from memory_system import MemorySystem, MemoryConfig


async def main():
    """主函数"""
    print("=" * 60)
    print("Agent Memory System V2 - 基础使用示例")
    print("=" * 60)
    
    # 创建配置
    config = MemoryConfig()
    config.storage.backend = "memory"  # 使用内存存储（无需安装额外依赖）
    config.embedding.provider = "sentence_transformers"
    config.embedding.model_name = "sentence-transformers/all-MiniLM-L6-v2"
    
    # 初始化系统
    print("\n【1. 初始化记忆系统】")
    print("-" * 40)
    memory = MemorySystem.from_config(config)
    print("✓ 记忆系统初始化完成")
    
    # 存储记忆
    print("\n【2. 存储记忆】")
    print("-" * 40)
    
    # 短期记忆
    await memory.remember(
        content="用户询问今天的天气",
        memory_type="short_term",
        importance="low"
    )
    print("✓ 存储短期记忆: 用户询问今天的天气")
    
    # 情景记忆
    await memory.remember(
        content="用户完成了项目A的开发，使用了Python和FastAPI",
        memory_type="episodic",
        importance="high",
        metadata={"project": "A", "technologies": ["Python", "FastAPI"]}
    )
    print("✓ 存储情景记忆: 用户完成项目A")
    
    # 语义记忆
    await memory.remember(
        content="用户是一名全栈工程师，有5年工作经验",
        memory_type="semantic",
        importance="critical"
    )
    print("✓ 存储语义记忆: 用户职业信息")
    
    # 自动类型选择
    await memory.remember(
        content="用户偏好使用VS Code作为主力编辑器",
        memory_type="auto",
        importance="medium"
    )
    print("✓ 自动选择记忆类型: 用户编辑器偏好")
    
    # 检索记忆
    print("\n【3. 检索记忆】")
    print("-" * 40)
    
    results = await memory.recall(
        query="用户的编程技能",
        max_results=5
    )
    
    print(f"查询: {results['query']}")
    print(f"策略: {results['strategy']}")
    print(f"检索层: {list(results['layers'].keys())}")
    print("\n结果:")
    for i, result in enumerate(results['combined'][:3], 1):
        print(f"  {i}. [{result['layer']}] {result['memory']['content'][:50]}...")
        print(f"     分数: {result['score']:.3f}")
    
    # 获取最近上下文
    print("\n【4. 获取最近上下文】")
    print("-" * 40)
    context = memory.get_recent_context(n=3)
    for i, content in enumerate(context, 1):
        print(f"  {i}. {content}")
    
    # 统计信息
    print("\n【5. 系统统计】")
    print("-" * 40)
    stats = await memory.get_stats()
    print(f"  短期记忆数: {stats.short_term_count}")
    print(f"  情景记忆数: {stats.episodic_count}")
    print(f"  语义记忆数: {stats.semantic_count}")
    print(f"  总添加数: {stats.total_added}")
    print(f"  运行时间: {stats.uptime_hours:.2f}小时")
    
    # 关闭系统
    await memory.close()
    print("\n✓ 示例完成")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

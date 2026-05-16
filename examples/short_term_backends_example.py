"""
短期记忆持久化后端使用示例
演示如何使用 Redis 和 SQLite 后端
"""

import asyncio
from memory_system.working_memory_cache import WorkingMemoryCache
from memory_system.working_memory_backends import (
    RedisShortTermMemoryBackend,
    SQLiteShortTermMemoryBackend,
)
from memory_system.core_abstractions.memory_data_models import ImportanceLevel


async def demo_memory_backend():
    """演示内存后端（默认）"""
    print("=" * 50)
    print("演示: 内存后端 (默认)")
    print("=" * 50)

    cache = WorkingMemoryCache(capacity=5, ttl_seconds=300)

    # 添加记忆
    item1 = await cache.add("这是第一条记忆", importance=ImportanceLevel.HIGH)
    item2 = await cache.add("这是第二条记忆", importance=ImportanceLevel.MEDIUM)

    print(f"添加记忆1: {item1.content}")
    print(f"添加记忆2: {item2.content}")

    # 获取所有记忆
    all_memories = await cache.get_all()
    print(f"所有记忆数量: {len(all_memories)}")

    # 获取最近记忆
    recent = await cache.get_recent(2)
    print(f"最近记忆: {[m.content for m in recent]}")

    # 检查是否已满
    print(f"是否已满: {await cache.is_full()}")

    await cache.clear()
    print("已清空")
    print()


async def demo_sqlite_backend():
    """演示 SQLite 后端"""
    print("=" * 50)
    print("演示: SQLite 后端")
    print("=" * 50)

    # 创建 SQLite 后端
    backend = SQLiteShortTermMemoryBackend(
        capacity=5,
        ttl_seconds=300,
        db_path="/tmp/short_term_memory_demo.db"
    )

    cache = WorkingMemoryCache(backend=backend)

    # 添加记忆
    item1 = await cache.add("SQLite 持久化记忆1", importance=ImportanceLevel.HIGH)
    item2 = await cache.add("SQLite 持久化记忆2", importance=ImportanceLevel.MEDIUM)

    print(f"添加记忆1: {item1.content}")
    print(f"添加记忆2: {item2.content}")

    # 获取所有记忆
    all_memories = await cache.get_all()
    print(f"所有记忆数量: {len(all_memories)}")

    # 获取最近记忆
    recent = await cache.get_recent(2)
    print(f"最近记忆: {[m.content for m in recent]}")

    # 检查是否已满
    print(f"是否已满: {await cache.is_full()}")

    await cache.clear()
    print("已清空")
    print()


async def demo_redis_backend():
    """演示 Redis 后端（需要 Redis 服务）"""
    print("=" * 50)
    print("演示: Redis 后端 (需要 Redis 服务)")
    print("=" * 50)

    try:
        # 创建 Redis 后端
        backend = RedisShortTermMemoryBackend(
            capacity=5,
            ttl_seconds=300,
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
        )

        cache = WorkingMemoryCache(backend=backend)

        # 添加记忆
        item1 = await cache.add("Redis 分布式记忆1", importance=ImportanceLevel.HIGH)
        item2 = await cache.add("Redis 分布式记忆2", importance=ImportanceLevel.MEDIUM)

        print(f"添加记忆1: {item1.content}")
        print(f"添加记忆2: {item2.content}")

        # 获取所有记忆
        all_memories = await cache.get_all()
        print(f"所有记忆数量: {len(all_memories)}")

        # 获取最近记忆
        recent = await cache.get_recent(2)
        print(f"最近记忆: {[m.content for m in recent]}")

        # 检查是否已满
        print(f"是否已满: {await cache.is_full()}")

        await cache.clear()
        print("已清空")

    except Exception as e:
        print(f"Redis 演示跳过 (Redis 服务不可用): {e}")

    print()


async def demo_capacity_limit():
    """演示容量限制"""
    print("=" * 50)
    print("演示: 容量限制")
    print("=" * 50)

    backend = SQLiteShortTermMemoryBackend(
        capacity=3,
        ttl_seconds=300,
        db_path="/tmp/short_term_capacity_demo.db"
    )
    cache = WorkingMemoryCache(backend=backend)

    # 添加超过容量的记忆
    for i in range(5):
        await cache.add(f"记忆 {i}", importance=ImportanceLevel.MEDIUM)
        print(f"添加: 记忆 {i}")

    # 获取所有记忆
    all_memories = await cache.get_all()
    print(f"所有记忆数量 (应<=3): {len(all_memories)}")
    print(f"记忆内容: {[m.content for m in all_memories]}")

    await cache.clear()
    print()


async def main():
    """主函数"""
    print("\n短期记忆持久化后端演示\n")

    await demo_memory_backend()
    await demo_sqlite_backend()
    await demo_redis_backend()
    await demo_capacity_limit()

    print("=" * 50)
    print("演示完成!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())

"""
SQLite 短期记忆后端
轻量级本地持久化
"""

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import List

from memory_system.core_abstractions.abstract_component_interfaces import ShortTermMemoryBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel


class SQLiteShortTermMemoryBackend(ShortTermMemoryBackend):
    """
    SQLite 短期记忆后端

    特点:
        - 轻量级本地存储
        - 无需额外服务
        - 支持持久化到文件
    """

    def __init__(
        self,
        capacity: int = 7,
        ttl_seconds: float = 300,
        db_path: str = "./short_term_memory.db"
    ):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.db_path = Path(db_path)
        # _init_db 在 __init__ 中同步调用，因为此时事件循环可能尚未启动
        self._init_db()

    def _init_db(self) -> None:
        """初始化数据库"""
        def _do_init():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS short_term_memories (
                        memory_id TEXT PRIMARY KEY,
                        content TEXT NOT NULL,
                        memory_type TEXT NOT NULL,
                        importance INTEGER NOT NULL,
                        timestamp REAL NOT NULL,
                        access_count INTEGER DEFAULT 0,
                        last_access REAL NOT NULL,
                        embedding TEXT,
                        metadata TEXT,
                        expires_at REAL NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_expires_at
                    ON short_term_memories(expires_at)
                """)
                conn.commit()
        _do_init()

    async def add(self, item: MemoryItem) -> str:
        """添加记忆"""
        expires_at = time.time() + self.ttl_seconds

        def _do_add():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO short_term_memories
                    (memory_id, content, memory_type, importance, timestamp,
                     access_count, last_access, embedding, metadata, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.memory_id, item.content, item.memory_type.value,
                    item.importance.value, item.timestamp, item.access_count,
                    item.last_access,
                    json.dumps(item.embedding) if item.embedding else None,
                    json.dumps(item.metadata),
                    expires_at
                ))
                conn.commit()

        await asyncio.to_thread(_do_add)
        await self._enforce_capacity()
        await self._cleanup_expired()

        return item.memory_id

    async def _enforce_capacity(self) -> None:
        """强制执行容量限制"""
        def _do_enforce():
            with sqlite3.connect(self.db_path) as conn:
                # 删除最旧的，直到容量限制内
                conn.execute("""
                    DELETE FROM short_term_memories
                    WHERE memory_id IN (
                        SELECT memory_id FROM short_term_memories
                        ORDER BY timestamp ASC
                        LIMIT (SELECT MAX(0, COUNT(*) - ?) FROM short_term_memories)
                    )
                """, (self.capacity,))
                conn.commit()
        await asyncio.to_thread(_do_enforce)

    async def _cleanup_expired(self) -> None:
        """清理过期记忆"""
        def _do_cleanup():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM short_term_memories WHERE expires_at < ?",
                    (time.time(),)
                )
                conn.commit()
        await asyncio.to_thread(_do_cleanup)

    async def get_recent(self, n: int) -> List[MemoryItem]:
        """获取最近n条记忆"""
        await self._cleanup_expired()

        def _do_get_recent():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """SELECT * FROM short_term_memories
                       ORDER BY timestamp DESC LIMIT ?""",
                    (n,)
                )
                rows = cursor.fetchall()
                return [self._row_to_item(row) for row in rows]

        return await asyncio.to_thread(_do_get_recent)

    async def get_all(self) -> List[MemoryItem]:
        """获取所有记忆"""
        await self._cleanup_expired()

        def _do_get_all():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM short_term_memories ORDER BY timestamp"
                )
                rows = cursor.fetchall()
                return [self._row_to_item(row) for row in rows]

        return await asyncio.to_thread(_do_get_all)

    def _row_to_item(self, row: sqlite3.Row) -> MemoryItem:
        """将数据库行转换为MemoryItem"""
        data = dict(row)
        del data['expires_at']  # 移除数据库特有字段
        data['memory_type'] = MemoryType(data['memory_type'])
        data['importance'] = ImportanceLevel(data['importance'])
        data['embedding'] = json.loads(data['embedding']) if data['embedding'] else None
        data['metadata'] = json.loads(data['metadata'])

        return MemoryItem(**data)

    async def clear(self) -> None:
        """清空所有记忆"""
        def _do_clear():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM short_term_memories")
                conn.commit()
        await asyncio.to_thread(_do_clear)

    async def is_full(self) -> bool:
        """检查是否已满"""
        await self._cleanup_expired()

        def _do_is_full():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM short_term_memories")
                count = cursor.fetchone()[0]
                return count >= self.capacity

        return await asyncio.to_thread(_do_is_full)

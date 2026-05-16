"""
记忆导入导出工具
基于 JSON 格式，支持文件和内存两种模式
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType
from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


class MemoryExporter:
    """
    记忆导出器

    支持导出为 JSON 格式，可写入文件或返回字典列表
    """

    def __init__(self, storage: StorageBackend):
        self.storage = storage

    async def export_all(
        self,
        memory_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        导出所有记忆

        Args:
            memory_type: 可选，按类型过滤 (episodic/semantic)

        Returns:
            记忆字典列表
        """
        memories = await self.storage.search_by_time_range(0, time.time(), memory_type)
        return [m.to_storage_dict() for m in memories]

    async def export_to_file(
        self,
        filepath: str,
        memory_type: Optional[str] = None
    ) -> int:
        """
        导出记忆到 JSON 文件

        Args:
            filepath: 输出文件路径
            memory_type: 可选，按类型过滤

        Returns:
            导出的记忆数量
        """
        memories = await self.export_all(memory_type)
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)

        export_data = {
            "version": "2.1.0",
            "exported_at": time.time(),
            "count": len(memories),
            "memories": memories,
        }

        def _write():
            with open(path, "w", encoding="utf-8") as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
        await asyncio.to_thread(_write)

        logger.info(f"已导出 {len(memories)} 条记忆到 {filepath}")
        return len(memories)

    async def export_to_string(
        self,
        memory_type: Optional[str] = None
    ) -> str:
        """导出为 JSON 字符串"""
        memories = await self.export_all(memory_type)
        export_data = {
            "version": "2.1.0",
            "exported_at": time.time(),
            "count": len(memories),
            "memories": memories,
        }
        return json.dumps(export_data, ensure_ascii=False, indent=2)


class MemoryImporter:
    """
    记忆导入器

    支持从 JSON 格式导入记忆，可从文件或字典列表读取
    """

    def __init__(self, storage: StorageBackend):
        self.storage = storage

    async def import_from_list(
        self,
        memories_data: List[Dict[str, Any]]
    ) -> Tuple[int, int]:
        """
        从字典列表导入记忆

        Args:
            memories_data: 记忆字典列表

        Returns:
            (成功数, 跳过数)
        """
        imported = 0
        skipped = 0

        # 检测重复 ID
        all_ids = [d.get("memory_id") for d in memories_data if d.get("memory_id")]
        seen = set()
        for mid in all_ids:
            if mid in seen:
                logger.warning(f"导入数据中存在重复的 memory_id: {mid}")
            seen.add(mid)

        for data in memories_data:
            try:
                item = MemoryItem.from_storage_dict(data)
                await self.storage.add(item)
                imported += 1
            except Exception as e:
                logger.warning(f"导入记忆失败: {e}")
                skipped += 1

        logger.info(f"导入完成: 成功 {imported}, 跳过 {skipped}")
        return imported, skipped

    async def import_from_file(
        self,
        filepath: str
    ) -> Tuple[int, int]:
        """
        从 JSON 文件导入记忆

        Args:
            filepath: 输入文件路径

        Returns:
            (成功数, 跳过数)
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"导入文件不存在: {filepath}")

        def _read():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        data = await asyncio.to_thread(_read)

        # 支持两种格式: 直接列表 或 包裹格式 {"memories": [...]}
        if isinstance(data, list):
            memories_data = data
        elif isinstance(data, dict) and "memories" in data:
            memories_data = data["memories"]
        else:
            raise ValueError("无效的导入文件格式，期望列表或包含 'memories' 键的字典")

        return await self.import_from_list(memories_data)

    async def import_from_string(
        self,
        json_string: str
    ) -> Tuple[int, int]:
        """从 JSON 字符串导入记忆"""
        data = json.loads(json_string)

        if isinstance(data, list):
            memories_data = data
        elif isinstance(data, dict) and "memories" in data:
            memories_data = data["memories"]
        else:
            raise ValueError("无效的 JSON 格式")

        return await self.import_from_list(memories_data)

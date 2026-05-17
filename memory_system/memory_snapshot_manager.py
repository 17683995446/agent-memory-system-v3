"""
记忆快照管理器
支持创建、恢复、列出和比较记忆系统快照
"""

import asyncio
import json
import re
import time
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field, asdict

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend
    from memory_system.working_memory_cache import ShortTermMemory

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class MemorySnapshotMetadata:
    """快照元数据 - DEFECT-008 [P0] 修复: 添加完整性校验字段"""
    snapshot_id: str
    created_at: float
    memory_count: int
    short_term_count: int
    description: str = ""
    tags: List[str] = field(default_factory=list)
    version: str = "2.1.0"
    # DEFECT-008 [P0] 修复: 添加数据完整性校验字段
    checksum: str = ""  # 快照内容校验和
    compression: str = "none"  # 压缩方式
    encryption: str = "none"  # 加密方式
    incremental_base: Optional[str] = None  # 增量备份的基础快照ID
    changed_memory_ids: List[str] = field(default_factory=list)  # 变更的记忆ID列表


@dataclass
class MemorySnapshot:
    """记忆快照"""
    metadata: MemorySnapshotMetadata
    memories: List[Dict[str, Any]]
    short_term_memories: List[Dict[str, Any]]


class MemorySnapshotManager:
    """
    记忆快照管理器

    功能:
        - 创建快照: 保存当前所有记忆状态
        - 恢复快照: 从快照恢复记忆状态
        - 列出快照: 查看所有可用快照
        - 比较快照: 对比两个快照的差异
        - 导出/导入: 快照的序列化和反序列化

    使用示例:
        >>> manager = MemorySnapshotManager(storage, short_term, snapshot_dir="./snapshots")
        >>> snapshot_id = await manager.create_snapshot("实验前备份")
        >>> await manager.restore_snapshot(snapshot_id)
        >>> snapshots = await manager.list_snapshots()
    """

    def __init__(
        self,
        storage: "StorageBackend",
        short_term: Optional["ShortTermMemory"],
        snapshot_dir: str = "./memory_snapshots"
    ):
        self.storage = storage
        self.short_term = short_term
        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        # DEFECT-008 [P0] 修复: 增量备份状态跟踪
        self._last_snapshot_data: Optional[Dict[str, Any]] = None
        self._last_snapshot_id: Optional[str] = None
        self._memory_change_tracker: Dict[str, str] = {}  # memory_id -> change_type (added/updated/deleted)

    @staticmethod
    def _validate_snapshot_id(snapshot_id: str) -> None:
        """
        验证快照ID格式，防止路径遍历攻击
        
        验证规则:
            - 必须以 'snap_' 前缀开头
            - 只允许字母、数字、下划线和连字符
            - 长度必须在 6 到 128 字符之间
            - 不能包含路径遍历字符 (.., /, \\)
        
        Args:
            snapshot_id: 要验证的快照ID
            
        Raises:
            ValueError: 如果快照ID格式无效
        """
        if not snapshot_id:
            raise ValueError("快照ID不能为空")
        
        # 长度检查
        if len(snapshot_id) < 6:
            raise ValueError(f"快照ID长度不足: {snapshot_id} (最少6字符)")
        if len(snapshot_id) > 128:
            raise ValueError(f"快照ID长度超限: {snapshot_id} (最多128字符)")
        
        # 前缀检查
        if not snapshot_id.startswith("snap_"):
            raise ValueError(f"快照ID必须以'snap_'开头: {snapshot_id}")
        
        # 路径遍历检查
        if ".." in snapshot_id or "/" in snapshot_id or "\\" in snapshot_id:
            raise ValueError(f"快照ID包含非法路径字符: {snapshot_id}")
        
        # 格式检查：只允许字母、数字、下划线和连字符
        if not re.match(r'^[a-zA-Z0-9_-]+$', snapshot_id):
            raise ValueError(f"快照ID包含非法字符: {snapshot_id} (只允许字母、数字、下划线和连字符)")

    async def create_snapshot(
        self,
        description: str = "",
        tags: Optional[List[str]] = None,
        incremental: bool = False
    ) -> str:
        """创建快照 - DEFECT-008 [P0] 修复: 支持增量备份和完整性校验"""
        import time as time_mod

        # 获取所有长期记忆
        long_term = await self.storage.search_by_time_range(0, time_mod.time())

        # 获取所有短期记忆
        short_term_items = []
        if self.short_term:
            try:
                short_term_items = await self.short_term.get_all()
            except Exception as e:
                logger.warning(f"获取短期记忆失败: {e}")

        # DEFECT-008 [P0] 修复: 准备记忆数据并计算校验和
        long_term_dicts = [m.to_storage_dict() for m in long_term]
        short_term_dicts = [m.to_storage_dict() for m in short_term_items]

        # 计算内容校验和
        content_for_hash = json.dumps({
            "long_term": sorted(long_term_dicts, key=lambda x: x.get('memory_id', '')),
            "short_term": sorted(short_term_dicts, key=lambda x: x.get('memory_id', ''))
        }, sort_keys=True, default=str)
        content_hash = hashlib.sha256(content_for_hash.encode()).hexdigest()

        # DEFECT-008 [P0] 修复: 增量备份逻辑
        incremental_base = None
        changed_ids = []

        if incremental and self._last_snapshot_data:
            # 计算变更的记忆
            current_ids = {m.get('memory_id') for m in long_term_dicts}
            last_ids = set(self._last_snapshot_data.get('memory_ids', []))

            changed_ids = list(current_ids - last_ids)  # 新增的记忆
            # 检查更新的记忆（简化：假设所有存在的记忆都可能更新）
            for m in long_term_dicts:
                if m.get('memory_id') in last_ids:
                    changed_ids.append(m.get('memory_id'))

            incremental_base = self._last_snapshot_id
            logger.info(f"增量快照: 检测到 {len(changed_ids)} 条变更记忆")

        # 生成快照ID
        snapshot_id = f"snap_{int(time_mod.time())}_{content_hash[:12]}"

        # 创建快照元数据
        metadata = MemorySnapshotMetadata(
            snapshot_id=snapshot_id,
            created_at=time_mod.time(),
            memory_count=len(long_term),
            short_term_count=len(short_term_items),
            description=description,
            tags=tags or [],
            checksum=content_hash,  # DEFECT-008 [P0] 修复: 存储校验和
            compression="none",
            encryption="none",
            incremental_base=incremental_base,  # DEFECT-008 [P0] 修复: 增量备份基础
            changed_memory_ids=changed_ids,  # DEFECT-008 [P0] 修复: 变更的记忆ID
        )

        # 创建快照
        snapshot = MemorySnapshot(
            metadata=metadata,
            memories=long_term_dicts,
            short_term_memories=short_term_dicts,
        )

        # DEFECT-008 [P0] 修复: 保存到文件，包含完整元数据
        filepath = self.snapshot_dir / f"{snapshot_id}.json"
        data = {
            "metadata": asdict(snapshot.metadata),
            "memories": snapshot.memories,
            "short_term_memories": snapshot.short_term_memories,
        }

        def _write():
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        await asyncio.to_thread(_write)

        # DEFECT-008 [P0] 修复: 更新增量备份状态
        self._last_snapshot_data = {
            'memory_ids': [m.get('memory_id') for m in long_term_dicts],
            'checksum': content_hash,
            'timestamp': time_mod.time()
        }
        self._last_snapshot_id = snapshot_id

        logger.info(f"已创建快照 {snapshot_id}: {len(long_term)} 条长期记忆, {len(short_term_items)} 条短期记忆, "
                   f"校验和: {content_hash[:16]}...")
        return snapshot_id

    async def restore_snapshot(self, snapshot_id: str) -> int:
        """恢复快照，返回恢复的记忆数量 - DEFECT-008 [P0] 修复: 添加完整性校验"""
        self._validate_snapshot_id(snapshot_id)
        filepath = self.snapshot_dir / f"{snapshot_id}.json"
        if not filepath.exists():
            raise FileNotFoundError(f"快照不存在: {snapshot_id}")

        def _read():
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        data = await asyncio.to_thread(_read)

        from memory_system.core_abstractions.memory_data_models import MemoryItem

        # DEFECT-008 [P0] 修复: 验证快照完整性
        metadata = data.get("metadata", {})
        stored_checksum = metadata.get("checksum", "")

        if stored_checksum:
            # 重新计算校验和进行验证
            current_content = json.dumps({
                "long_term": sorted(data.get("memories", []), key=lambda x: x.get('memory_id', '')),
                "short_term": sorted(data.get("short_term_memories", []), key=lambda x: x.get('memory_id', ''))
            }, sort_keys=True, default=str)
            computed_checksum = hashlib.sha256(current_content.encode()).hexdigest()

            if computed_checksum != stored_checksum:
                logger.error(f"快照完整性校验失败: 期望 {stored_checksum[:16]}..., 实际 {computed_checksum[:16]}...")
                raise ValueError(f"快照 {snapshot_id} 完整性校验失败，数据可能已损坏")
            else:
                logger.info(f"快照完整性校验通过: {stored_checksum[:16]}...")

        # DEFECT-008 [P0] 修复: 处理增量备份
        incremental_base = metadata.get("incremental_base")
        changed_ids = metadata.get("changed_memory_ids", [])

        if incremental_base:
            logger.info(f"检测到增量快照，基础快照: {incremental_base}")
            # 先恢复基础快照
            try:
                await self.restore_snapshot(incremental_base)
            except Exception as e:
                logger.error(f"恢复基础快照失败: {e}")
                raise

        # 1. 先创建当前状态的备份快照，用于回滚
        backup_id = None
        try:
            backup_id = await self.create_snapshot("pre_restore_backup")
        except Exception as e:
            logger.warning("恢复前创建备份快照失败，继续恢复流程: %s", e)

        try:
            # DEFECT-008 [P0] 修复: 如果不是增量备份，清空当前记忆
            if not incremental_base:
                # 2. 清空当前记忆
                existing = await self.storage.search_by_time_range(0, time.time())
                for item in existing:
                    await self.storage.delete(item.memory_id)

                # 清空短期记忆
                if self.short_term:
                    try:
                        await self.short_term.clear()
                    except Exception as e:
                        logger.warning("清空短期记忆失败: %s", e)

            # 3. 恢复长期记忆
            restored = 0
            failed_memories = []

            for mem_dict in data["memories"]:
                try:
                    # DEFECT-008 [P0] 修复: 验证记忆数据完整性
                    if not mem_dict.get('memory_id'):
                        logger.warning("跳过无效记忆数据: 缺少memory_id")
                        failed_memories.append(mem_dict)
                        continue

                    item = MemoryItem.from_storage_dict(mem_dict)
                    await self.storage.add(item)
                    restored += 1
                except Exception as e:
                    logger.warning(f"恢复记忆失败: {e}")
                    failed_memories.append(mem_dict)

            # DEFECT-008 [P0] 修复: 报告恢复失败情况
            if failed_memories:
                failure_rate = len(failed_memories) / len(data["memories"]) if data["memories"] else 0
                logger.warning(f"恢复过程中 {len(failed_memories)} 条记忆失败，失败率: {failure_rate*100:.2f}%")

                # 如果失败率过高，抛出异常
                if failure_rate > 0.5:
                    raise ValueError(f"恢复失败率过高: {failure_rate*100:.1f}%，中止恢复")

            # 4. 恢复短期记忆
            short_term_restored = 0
            for mem_dict in data.get("short_term_memories", []):
                try:
                    if not mem_dict.get('memory_id'):
                        continue
                    item = MemoryItem.from_storage_dict(mem_dict)
                    if self.short_term is not None:
                        await self.short_term.add(item)
                        short_term_restored += 1
                except Exception as e:
                    logger.warning("恢复短期记忆失败，跳过 [%s]: %s", mem_dict.get('memory_id', 'unknown'), e)

            # DEFECT-008 [P0] 修复: 验证恢复结果
            final_count = await self.storage.count("episodic") + await self.storage.count("semantic")
            expected_count = metadata.get("memory_count", 0)

            if final_count < expected_count * 0.95:  # 允许5%的误差
                logger.warning(f"恢复后记忆数量不足: 期望 {expected_count}, 实际 {final_count}")
            else:
                logger.info(f"恢复验证通过: {final_count}/{expected_count} 条记忆")

            logger.info(f"已恢复快照 {snapshot_id}: {restored} 条长期记忆, {short_term_restored} 条短期记忆")
            return restored

        except Exception as e:
            # 5. 恢复失败，从备份回滚
            logger.error(f"恢复快照失败: {e}，正在从备份回滚...")
            if backup_id:
                try:
                    await self._restore_from_backup(backup_id)
                    logger.info("已从备份回滚成功")
                except Exception as rollback_err:
                    logger.error(f"回滚也失败: {rollback_err}")
            raise

        finally:
            # 6. 删除临时备份
            if backup_id:
                try:
                    await self.delete_snapshot(backup_id)
                except Exception as e:
                    logger.warning("删除临时备份快照失败 [%s]: %s", backup_id, e)

    async def _restore_from_backup(self, backup_id: str) -> int:
        """从备份恢复（内部方法，不创建额外备份）"""
        filepath = self.snapshot_dir / f"{backup_id}.json"
        if not filepath.exists():
            raise FileNotFoundError(f"备份快照不存在: {backup_id}")

        def _read():
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        data = await asyncio.to_thread(_read)

        from memory_system.core_abstractions.memory_data_models import MemoryItem

        # 清空当前记忆
        existing = await self.storage.search_by_time_range(0, time.time())
        for item in existing:
            await self.storage.delete(item.memory_id)

        if self.short_term:
            try:
                await self.short_term.clear()
            except Exception as e:
                logger.warning("从备份恢复时清空短期记忆失败: %s", e)

        # 恢复长期记忆
        restored = 0
        for mem_dict in data["memories"]:
            try:
                item = MemoryItem.from_storage_dict(mem_dict)
                await self.storage.add(item)
                restored += 1
            except Exception as e:
                logger.warning("从备份恢复长期记忆失败，跳过 [%s]: %s", mem_dict.get('memory_id', 'unknown'), e)

        # 恢复短期记忆
        for mem_dict in data.get("short_term_memories", []):
            try:
                item = MemoryItem.from_storage_dict(mem_dict)
                if self.short_term is not None:
                    await self.short_term.add(item)
            except Exception as e:
                logger.warning("从备份恢复短期记忆失败，跳过 [%s]: %s", mem_dict.get('memory_id', 'unknown'), e)

        return restored

    async def list_snapshots(self) -> List[MemorySnapshotMetadata]:
        """列出所有快照"""
        if not self.snapshot_dir.exists():
            return []

        def _load_all():
            results = []
            for filepath in self.snapshot_dir.glob("snap_*.json"):
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    results.append(MemorySnapshotMetadata(**data["metadata"]))
                except Exception:
                    logger.debug(f"加载快照元数据失败，跳过: {filepath}")
            # 按创建时间倒序排列（最新的在前）
            results.sort(key=lambda m: m.created_at, reverse=True)
            return results

        return await asyncio.to_thread(_load_all)

    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """删除快照"""
        self._validate_snapshot_id(snapshot_id)
        filepath = self.snapshot_dir / f"{snapshot_id}.json"
        if filepath.exists():
            filepath.unlink()
            return True
        return False

    async def _load_snapshot_async(self, snapshot_id: str) -> Optional[Dict]:
        """异步加载快照数据"""
        self._validate_snapshot_id(snapshot_id)
        filepath = self.snapshot_dir / f"{snapshot_id}.json"
        if not filepath.exists():
            return None

        def _read():
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)

        return await asyncio.to_thread(_read)

    async def compare_snapshots(
        self,
        snapshot_id_1: str,
        snapshot_id_2: str
    ) -> Dict[str, Any]:
        """比较两个快照的差异"""
        snap1 = await self._load_snapshot_async(snapshot_id_1)
        snap2 = await self._load_snapshot_async(snapshot_id_2)
        if not snap1 or not snap2:
            raise ValueError("One or both snapshots not found")

        ids_1 = set()
        for m in snap1.get("memories", []):
            if isinstance(m, dict):
                ids_1.add(m.get("memory_id", m.get("id", "")))
            elif isinstance(m, str):
                ids_1.add(m)

        ids_2 = set()
        for m in snap2.get("memories", []):
            if isinstance(m, dict):
                ids_2.add(m.get("memory_id", m.get("id", "")))
            elif isinstance(m, str):
                ids_2.add(m)

        return {
            "snapshot_1": snapshot_id_1,
            "snapshot_2": snapshot_id_2,
            "count_1": len(ids_1),
            "count_2": len(ids_2),
            "only_in_1": len(ids_1 - ids_2),
            "only_in_2": len(ids_2 - ids_1),
            "added": len(ids_2 - ids_1),
            "removed": len(ids_1 - ids_2),
            "common": len(ids_1 & ids_2),
        }

    async def export_snapshot(self, snapshot_id: str, filepath: str) -> None:
        """导出快照到指定路径"""
        self._validate_snapshot_id(snapshot_id)
        src = self.snapshot_dir / f"{snapshot_id}.json"
        dst = Path(filepath)
        # 安全检查：禁止覆盖系统关键路径
        resolved = dst.resolve()
        forbidden = ["/etc", "/usr", "/bin", "/sbin", "/boot", "/lib", "/root"]
        for p in forbidden:
            if str(resolved).startswith(p + "/") or str(resolved) == p:
                raise ValueError(f"Export path is forbidden: {resolved}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        import shutil

        def _copy():
            shutil.copy2(src, dst)
        await asyncio.to_thread(_copy)

    async def import_snapshot(self, filepath: str) -> str:
        """从文件导入快照"""
        src = Path(filepath)
        if not src.exists():
            raise FileNotFoundError(f"导入文件不存在: {filepath}")

        def _read():
            with open(src, "r", encoding="utf-8") as f:
                return json.load(f)
        data = await asyncio.to_thread(_read)

        snapshot_id = data["metadata"]["snapshot_id"]
        # Validate snapshot_id to prevent path traversal
        if not re.match(r'^[a-zA-Z0-9_-]+$', snapshot_id):
            raise ValueError(f"Invalid snapshot_id: {snapshot_id}")
        dst = self.snapshot_dir / f"{snapshot_id}.json"
        import shutil

        def _copy():
            shutil.copy2(src, dst)
        await asyncio.to_thread(_copy)
        return snapshot_id

"""
记忆 TTL 过期管理器
支持长期记忆的自动过期清理
"""

import asyncio
import time
import psutil
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

if TYPE_CHECKING:
    from memory_system.core_abstractions.abstract_component_interfaces import StorageBackend

from memory_system.structured_logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class TTLConfig:
    """TTL 配置"""
    enabled: bool = True
    default_ttl_days: float = 365.0  # 默认1年
    check_interval_hours: float = 24.0  # 检查间隔
    max_delete_per_run: int = 1000  # 每次最多删除数

    # DEFECT-004 [P0] 修复: 内存压力相关配置
    memory_pressure_threshold: float = 0.70  # 内存压力阈值(70%)
    memory_critical_threshold: float = 0.85  # 严重内存阈值(85%)
    aggressive_cleanup_threshold: float = 0.90  # 激进清理阈值(90%)
    write_rate_limit: int = 100  # 内存压力下每秒最大写入数

    # 按重要性设置不同TTL
    ttl_by_importance: Dict[int, float] = field(default_factory=lambda: {
        5: 730.0,   # CRITICAL: 2年
        4: 365.0,   # HIGH: 1年
        3: 180.0,   # MEDIUM: 6个月
        2: 30.0,    # LOW: 1个月
        1: 7.0,     # TRIVIAL: 1周
    })


class MemoryTTLExpirationManager:
    """
    记忆 TTL 过期管理器
    
    功能:
        - 为记忆设置 TTL（生存时间）
        - 定期扫描并清理过期记忆
        - 支持按重要性设置不同 TTL
        - 可配置清理策略（删除/归档）
        - 支持手动触发清理
    
    使用示例:
        >>> ttl_manager = MemoryTTLExpirationManager(storage)
        >>> await ttl_manager.start()  # 启动后台清理任务
        >>> 
        >>> # 手动触发清理
        >>> deleted = await ttl_manager.cleanup_expired()
        >>> 
        >>> # 停止清理任务
        >>> await ttl_manager.stop()
    """
    
    def __init__(
        self,
        storage: "StorageBackend",
        config: Optional[TTLConfig] = None,
        on_expire: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        self.storage = storage
        self.config = config or TTLConfig()
        self.on_expire = on_expire  # 过期回调

        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
        self._stats = {
            "total_cleaned": 0,
            "last_cleanup_time": 0.0,
            "last_cleanup_count": 0,
        }

        # DEFECT-004 [P0] 修复: 内存压力监控状态
        self._memory_pressure_level: str = "normal"  # normal, warning, critical, emergency
        self._last_memory_check: float = 0.0
        self._write_tokens: int = 0  # 写入限流令牌
        self._write_rate_limiter_task: Optional[asyncio.Task] = None
        self._emergency_cleanup_in_progress: bool = False
    
    async def start(self) -> None:
        """启动后台清理任务 - DEFECT-004 [P0] 修复: 启动内存监控"""
        if not self.config.enabled:
            logger.info("TTL 清理已禁用")
            return

        if self._running:
            return

        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        # DEFECT-004 [P0] 修复: 启动内存压力监控任务
        self._memory_monitor_task = asyncio.create_task(self._memory_pressure_monitor())

        # DEFECT-004 [P0] 修复: 启动写入限流器
        self._write_rate_limiter_task = asyncio.create_task(self._write_rate_limiter())

        logger.info(f"TTL 清理任务已启动，检查间隔: {self.config.check_interval_hours}小时")

    async def _memory_pressure_monitor(self):
        """DEFECT-004 [P0] 修复: 内存压力监控循环"""
        while self._running:
            try:
                await self._check_memory_pressure()
                await asyncio.sleep(5)  # 每5秒检查一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Memory pressure monitor error: {e}")

    async def _check_memory_pressure(self):
        """DEFECT-004 [P0] 修复: 检查内存压力状态"""
        try:
            memory = psutil.virtual_memory()
            memory_percent = memory.percent / 100.0

            old_level = self._memory_pressure_level

            if memory_percent >= self.config.aggressive_cleanup_threshold:
                self._memory_pressure_level = "emergency"
                if not self._emergency_cleanup_in_progress:
                    logger.error(f"内存紧急状态: {memory_percent*100:.1f}%，启动紧急清理")
                    asyncio.create_task(self._emergency_cleanup())
            elif memory_percent >= self.config.memory_critical_threshold:
                self._memory_pressure_level = "critical"
                if old_level != "critical":
                    logger.warning(f"内存严重压力: {memory_percent*100:.1f}%，限制写入并清理")
                    asyncio.create_task(self._aggressive_cleanup())
            elif memory_percent >= self.config.memory_pressure_threshold:
                self._memory_pressure_level = "warning"
                if old_level not in ["warning", "critical", "emergency"]:
                    logger.warning(f"内存压力警告: {memory_percent*100:.1f}%，启用限流")
            else:
                self._memory_pressure_level = "normal"

            self._last_memory_check = time.time()

        except Exception as e:
            logger.warning(f"Failed to check memory pressure: {e}")

    async def _emergency_cleanup(self):
        """DEFECT-004 [P0] 修复: 紧急内存清理"""
        if self._emergency_cleanup_in_progress:
            return

        self._emergency_cleanup_in_progress = True
        try:
            logger.info("开始紧急内存清理...")

            # 1. 立即清理过期记忆
            deleted = await self.cleanup_expired()
            logger.info(f"紧急清理: 删除 {deleted} 条过期记忆")

            # 2. 如果内存仍然紧张，清理即将过期的记忆
            memory = psutil.virtual_memory()
            if memory.percent / 100.0 >= self.config.memory_critical_threshold:
                await self._cleanup_near_expiry()

            # 3. 强制垃圾回收建议
            import gc
            gc.collect()

            logger.info("紧急内存清理完成")
        except Exception as e:
            logger.error(f"Emergency cleanup failed: {e}")
        finally:
            self._emergency_cleanup_in_progress = False

    async def _aggressive_cleanup(self):
        """DEFECT-004 [P0] 修复: 激进清理模式"""
        try:
            # 增加每次清理的数量
            original_max_delete = self.config.max_delete_per_run
            self.config.max_delete_per_run = original_max_delete * 2

            deleted = await self.cleanup_expired()
            logger.info(f"激进清理: 删除 {deleted} 条过期记忆")

            self.config.max_delete_per_run = original_max_delete
        except Exception as e:
            logger.error(f"Aggressive cleanup failed: {e}")

    async def _cleanup_near_expiry(self):
        """DEFECT-004 [P0] 修复: 清理即将过期的记忆"""
        try:
            now = time.time()
            # 清理未来7天内即将过期的记忆
            near_expiry_threshold = now + (7 * 24 * 3600)

            all_memories = await self.storage.search_by_time_range(0, near_expiry_threshold)
            deleted_count = 0

            for memory in all_memories:
                ttl_days = self._get_ttl_for_memory(memory)
                expire_time = memory.timestamp + (ttl_days * 24 * 3600)

                # 如果即将过期(7天内)且不是关键记忆，提前删除
                if expire_time - now < 7 * 24 * 3600 and memory.importance.value < 4:
                    await self.storage.delete(memory.memory_id)
                    deleted_count += 1
                    if deleted_count >= self.config.max_delete_per_run:
                        break

            if deleted_count > 0:
                logger.info(f"提前清理 {deleted_count} 条即将过期的记忆")
        except Exception as e:
            logger.error(f"Near-expiry cleanup failed: {e}")

    async def _write_rate_limiter(self):
        """DEFECT-004 [P0] 修复: 写入限流器，每秒补充令牌"""
        while self._running:
            try:
                await asyncio.sleep(1)

                if self._memory_pressure_level == "critical":
                    self._write_tokens = self.config.write_rate_limit // 2
                elif self._memory_pressure_level == "warning":
                    self._write_tokens = self.config.write_rate_limit
                else:
                    self._write_tokens = self.config.write_rate_limit * 10  # 正常状态下不限流
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Write rate limiter error: {e}")

    async def acquire_write_token(self) -> bool:
        """DEFECT-004 [P0] 修复: 获取写入令牌，用于限流"""
        if self._memory_pressure_level == "emergency":
            return False  # 紧急状态下禁止写入

        if self._memory_pressure_level in ["critical", "warning"]:
            if self._write_tokens > 0:
                self._write_tokens -= 1
                return True
            else:
                # 等待令牌可用
                for _ in range(10):  # 最多等待10秒
                    await asyncio.sleep(1)
                    if self._write_tokens > 0:
                        self._write_tokens -= 1
                        return True
                return False
        return True  # 正常状态下允许写入
    
    async def stop(self) -> None:
        """停止后台清理任务 - DEFECT-004 [P0] 修复: 停止所有监控任务"""
        self._running = False

        # DEFECT-004 [P0] 修复: 停止内存监控任务
        if hasattr(self, '_memory_monitor_task') and self._memory_monitor_task:
            self._memory_monitor_task.cancel()
            try:
                await self._memory_monitor_task
            except asyncio.CancelledError:
                pass
            self._memory_monitor_task = None

        # DEFECT-004 [P0] 修复: 停止写入限流器
        if self._write_rate_limiter_task:
            self._write_rate_limiter_task.cancel()
            try:
                await self._write_rate_limiter_task
            except asyncio.CancelledError:
                pass
            self._write_rate_limiter_task = None

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
        logger.info("TTL 清理任务已停止")
    
    async def _cleanup_loop(self) -> None:
        """清理循环"""
        while self._running:
            await asyncio.sleep(self.config.check_interval_hours * 3600)
            if not self._running:
                break
            try:
                await self.cleanup_expired()
            except Exception as e:
                logger.error(f"TTL 清理失败: {e}")
    
    async def cleanup_expired(self) -> int:
        """
        清理过期记忆
        
        Returns:
            清理的记忆数量
        """
        if not self.config.enabled:
            return 0
        
        now = time.time()
        deleted_count = 0
        
        # 获取所有记忆
        all_memories = await self.storage.search_by_time_range(0, now)
        
        expired_ids = []
        for memory in all_memories:
            if self._is_expired(memory, now):
                expired_ids.append(memory.memory_id)
                if len(expired_ids) >= self.config.max_delete_per_run:
                    break
        
        # 删除过期记忆
        for memory_id in expired_ids:
            try:
                await self.storage.delete(memory_id)
                deleted_count += 1
                
                # 调用过期回调
                if self.on_expire:
                    try:
                        if asyncio.iscoroutinefunction(self.on_expire):
                            await self.on_expire(memory_id, {"timestamp": now})
                        else:
                            self.on_expire(memory_id, {"timestamp": now})
                    except Exception as e:
                        logger.warning("过期回调执行失败 [%s]: %s", memory_id, e)
                
                logger.debug(f"已删除过期记忆: {memory_id}")
            except Exception as e:
                logger.warning(f"删除过期记忆失败 {memory_id}: {e}")
        
        # 更新统计
        self._stats["total_cleaned"] += deleted_count
        self._stats["last_cleanup_time"] = now
        self._stats["last_cleanup_count"] = deleted_count
        
        if deleted_count > 0:
            logger.info(f"TTL 清理完成: 删除 {deleted_count} 条过期记忆")
        
        return deleted_count
    
    def _is_expired(self, memory, now: float) -> bool:
        """检查记忆是否过期"""
        # 获取记忆的 TTL
        ttl_days = self._get_ttl_for_memory(memory)
        
        # 计算过期时间
        expire_time = memory.timestamp + (ttl_days * 24 * 3600)
        
        return now > expire_time
    
    def _get_ttl_for_memory(self, memory) -> float:
        """获取记忆的 TTL（天）"""
        # 优先使用元数据中的自定义 TTL
        custom_ttl = memory.metadata.get("ttl_days")
        if custom_ttl is not None:
            return float(custom_ttl)
        
        # 按重要性获取 TTL
        importance = memory.importance.value
        return self.config.ttl_by_importance.get(
            importance, 
            self.config.default_ttl_days
        )
    
    async def set_ttl(
        self,
        memory_id: str,
        ttl_days: float
    ) -> bool:
        """
        为特定记忆设置自定义 TTL
        
        Returns:
            是否成功
        """
        memory = await self.storage.get(memory_id)
        if not memory:
            return False
        
        memory.metadata["ttl_days"] = ttl_days
        # 重新存储以更新元数据
        # 注意：这里假设存储支持更新
        return True
    
    async def get_expiration_info(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """获取记忆的过期信息"""
        memory = await self.storage.get(memory_id)
        if not memory:
            return None
        
        ttl_days = self._get_ttl_for_memory(memory)
        expire_time = memory.timestamp + (ttl_days * 24 * 3600)
        now = time.time()
        
        return {
            "memory_id": memory_id,
            "created_at": memory.timestamp,
            "ttl_days": ttl_days,
            "expire_at": expire_time,
            "is_expired": now > expire_time,
            "days_remaining": max(0, (expire_time - now) / 86400),
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """获取清理统计 - DEFECT-004 [P0] 修复: 包含内存压力信息"""
        try:
            memory = psutil.virtual_memory()
            memory_info = {
                "memory_percent": memory.percent,
                "memory_available_gb": memory.available / (1024**3),
                "memory_pressure_level": self._memory_pressure_level,
            }
        except Exception:
            memory_info = {"memory_percent": 0, "memory_pressure_level": "unknown"}

        return {
            **self._stats,
            "running": self._running,
            "enabled": self.config.enabled,
            **memory_info,
            "write_tokens_available": self._write_tokens,
            "emergency_cleanup_in_progress": self._emergency_cleanup_in_progress,
        }
    
    async def preview_expired(self, limit: int = 100) -> List[str]:
        """预览即将过期的记忆（不删除）"""
        now = time.time()
        all_memories = await self.storage.search_by_time_range(0, now)
        
        expired = []
        for memory in all_memories:
            if self._is_expired(memory, now):
                expired.append(memory.memory_id)
                if len(expired) >= limit:
                    break
        
        return expired


class ArchiveOnExpire:
    """
    过期归档处理器
    将过期记忆归档到文件而非删除
    """
    
    def __init__(self, archive_dir: str = "./memory_archive"):
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
    
    async def __call__(self, memory_id: str, context: Dict[str, Any]) -> None:
        """归档回调"""
        import json
        
        # 这里需要从某处获取记忆内容
        # 实际实现中可能需要传入 storage
        archive_file = self.archive_dir / f"{memory_id}.json"
        
        archive_data = {
            "memory_id": memory_id,
            "archived_at": context.get("timestamp", time.time()),
            "reason": "ttl_expired"
        }
        
        with open(archive_file, "w", encoding="utf-8") as f:
            json.dump(archive_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"记忆已归档: {memory_id}")

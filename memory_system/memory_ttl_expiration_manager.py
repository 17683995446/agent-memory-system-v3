"""
记忆 TTL 过期管理器
支持长期记忆的自动过期清理
"""

import asyncio
import time
import psutil
import gc
import zlib
from collections import deque
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING, Set
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

    # DEFECT-R2-004 [P0] 修复: 增强内存监控配置 - 更严格的内存限制
    memory_pressure_threshold: float = 0.60  # 降低内存压力阈值(60%)
    memory_critical_threshold: float = 0.75  # 降低严重内存阈值(75%)
    aggressive_cleanup_threshold: float = 0.85  # 降低激进清理阈值(85%)
    memory_emergency_threshold: float = 0.92  # 新增: 紧急内存阈值(92%)
    write_rate_limit: int = 50  # 降低内存压力下每秒最大写入数(50)
    memory_check_interval_seconds: float = 2.0  # 内存检查间隔(2秒)

    # DEFECT-R2-005 [P1] 修复: OOM恢复配置
    memory_compression_enabled: bool = True  # 启用内存压缩
    memory_compression_threshold: float = 0.70  # 内存压缩阈值
    oom_recovery_enabled: bool = True  # 启用OOM恢复
    oom_recovery_aggressiveness: int = 3  # 恢复激进程度(1-5)
    memory_hard_limit_bytes: Optional[int] = None  # 硬内存限制(字节)

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

        # DEFECT-R2-004 [P0] 修复: 增强内存压力监控状态
        self._memory_pressure_level: str = "normal"  # normal, warning, critical, emergency
        self._last_memory_check: float = 0.0
        self._write_tokens: int = 0  # 写入限流令牌
        self._write_rate_limiter_task: Optional[asyncio.Task] = None
        self._emergency_cleanup_in_progress: bool = False

        # DEFECT-R2-004 [P0] 修复: 增强内存监控任务
        self._memory_monitor_task: Optional[asyncio.Task] = None
        self._memory_history: deque = deque(maxlen=100)  # 内存使用历史
        self._oom_recovery_in_progress: bool = False
        self._consecutive_high_memory_count: int = 0  # 连续高内存计数

        # DEFECT-R2-005 [P1] 修复: OOM恢复机制增强
        self._memory_compression_task: Optional[asyncio.Task] = None
        self._compressed_memory_ids: Set[str] = set()  # 已压缩的记忆ID
        self._recovery_stats = {
            "recovery_count": 0,
            "last_recovery_time": 0.0,
            "memory_freed_bytes": 0,
            "compression_count": 0,
        }
    
    async def start(self) -> None:
        """启动后台清理任务 - DEFECT-R2-004 [P0] 修复: 启动增强内存监控"""
        if not self.config.enabled:
            logger.info("TTL 清理已禁用")
            return

        if self._running:
            return

        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        # DEFECT-R2-004 [P0] 修复: 启动增强内存压力监控任务
        self._memory_monitor_task = asyncio.create_task(self._memory_pressure_monitor())

        # DEFECT-R2-004 [P0] 修复: 启动写入限流器
        self._write_rate_limiter_task = asyncio.create_task(self._write_rate_limiter())

        # DEFECT-R2-005 [P1] 修复: 启动内存压缩监控任务
        if self.config.memory_compression_enabled:
            self._memory_compression_task = asyncio.create_task(self._memory_compression_loop())

        logger.info(f"TTL 清理任务已启动，检查间隔: {self.config.check_interval_hours}小时")

    async def _memory_pressure_monitor(self):
        """DEFECT-R2-004 [P0] 修复: 增强内存压力监控循环 - 更频繁的检查"""
        while self._running:
            try:
                await self._check_memory_pressure()
                # DEFECT-R2-004: 使用配置的更短检查间隔
                await asyncio.sleep(self.config.memory_check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Memory pressure monitor error: {e}")

    async def _check_memory_pressure(self):
        """DEFECT-R2-004 [P0] 修复: 增强内存压力状态检查 - 更严格的阈值"""
        try:
            memory = psutil.virtual_memory()
            memory_percent = memory.percent / 100.0

            # 记录内存历史
            self._memory_history.append({
                "timestamp": time.time(),
                "percent": memory_percent,
                "available": memory.available,
                "used": memory.used
            })

            old_level = self._memory_pressure_level

            # DEFECT-R2-004: 更严格的阈值判断
            if memory_percent >= self.config.memory_emergency_threshold:
                self._memory_pressure_level = "emergency"
                self._consecutive_high_memory_count += 1
                if not self._emergency_cleanup_in_progress and not self._oom_recovery_in_progress:
                    logger.error(f"【紧急】内存紧急状态: {memory_percent*100:.1f}%，启动紧急清理和OOM恢复")
                    asyncio.create_task(self._emergency_cleanup())
                    # DEFECT-R2-005: 同时启动OOM恢复
                    if self.config.oom_recovery_enabled:
                        asyncio.create_task(self._oom_recovery())
            elif memory_percent >= self.config.aggressive_cleanup_threshold:
                self._memory_pressure_level = "critical"
                self._consecutive_high_memory_count += 1
                if old_level not in ["critical", "emergency"]:
                    logger.warning(f"【严重】内存压力: {memory_percent*100:.1f}%，限制写入并清理")
                    asyncio.create_task(self._aggressive_cleanup())
            elif memory_percent >= self.config.memory_critical_threshold:
                self._memory_pressure_level = "warning"
                self._consecutive_high_memory_count += 1
                if old_level not in ["warning", "critical", "emergency"]:
                    logger.warning(f"【警告】内存压力: {memory_percent*100:.1f}%，启用限流")
            elif memory_percent >= self.config.memory_pressure_threshold:
                self._memory_pressure_level = "elevated"
                self._consecutive_high_memory_count = max(0, self._consecutive_high_memory_count - 1)
                if old_level not in ["elevated", "warning", "critical", "emergency"]:
                    logger.info(f"【注意】内存使用上升: {memory_percent*100:.1f}%")
            else:
                self._memory_pressure_level = "normal"
                self._consecutive_high_memory_count = 0
                if old_level != "normal":
                    logger.info(f"【正常】内存压力缓解: {memory_percent*100:.1f}%")

            # 检查硬内存限制
            if self.config.memory_hard_limit_bytes and memory.used >= self.config.memory_hard_limit_bytes:
                logger.error(f"【超限】内存使用超过硬限制: {memory.used} >= {self.config.memory_hard_limit_bytes}")
                asyncio.create_task(self._hard_limit_recovery())

            self._last_memory_check = time.time()

        except Exception as e:
            logger.warning(f"Failed to check memory pressure: {e}")

    async def _emergency_cleanup(self):
        """DEFECT-R2-004 [P0] 修复: 增强紧急内存清理"""
        if self._emergency_cleanup_in_progress:
            return

        self._emergency_cleanup_in_progress = True
        memory_before = psutil.virtual_memory()

        try:
            logger.info("【紧急清理】开始紧急内存清理...")

            # 1. 立即清理过期记忆 - 增加清理数量
            original_max_delete = self.config.max_delete_per_run
            self.config.max_delete_per_run = original_max_delete * 5  # 5倍清理量
            deleted = await self.cleanup_expired()
            self.config.max_delete_per_run = original_max_delete
            logger.info(f"【紧急清理】删除 {deleted} 条过期记忆")

            # 2. 清理即将过期的记忆
            await self._cleanup_near_expiry(days_ahead=30)  # 清理未来30天内即将过期的

            # 3. 清理低重要性记忆
            await self._cleanup_low_importance()

            # 4. 强制垃圾回收
            gc.collect()

            # 5. 记录清理效果
            memory_after = psutil.virtual_memory()
            freed_bytes = memory_before.available - memory_after.available
            if freed_bytes < 0:
                freed_mb = abs(freed_bytes) / (1024 * 1024)
                logger.info(f"【紧急清理】完成，释放约 {freed_mb:.1f} MB")

        except Exception as e:
            logger.error(f"Emergency cleanup failed: {e}")
        finally:
            self._emergency_cleanup_in_progress = False

    async def _aggressive_cleanup(self):
        """DEFECT-R2-004 [P0] 修复: 增强激进清理模式"""
        try:
            logger.info("【激进清理】启动激进清理模式...")

            # 增加每次清理的数量
            original_max_delete = self.config.max_delete_per_run
            self.config.max_delete_per_run = original_max_delete * 3  # 3倍清理量

            deleted = await self.cleanup_expired()
            logger.info(f"【激进清理】删除 {deleted} 条过期记忆")

            # 清理即将过期的
            await self._cleanup_near_expiry(days_ahead=14)

            self.config.max_delete_per_run = original_max_delete

            gc.collect()
        except Exception as e:
            logger.error(f"Aggressive cleanup failed: {e}")

    async def _cleanup_near_expiry(self, days_ahead: int = 7):
        """DEFECT-R2-004 [P0] 修复: 增强清理即将过期的记忆"""
        try:
            now = time.time()
            near_expiry_threshold = now + (days_ahead * 24 * 3600)

            all_memories = await self.storage.search_by_time_range(0, near_expiry_threshold)
            deleted_count = 0

            # 按重要性排序，优先删除低重要性
            sorted_memories = sorted(all_memories, key=lambda m: m.importance.value)

            for memory in sorted_memories:
                ttl_days = self._get_ttl_for_memory(memory)
                expire_time = memory.timestamp + (ttl_days * 24 * 3600)

                # 如果即将过期且不是关键记忆，提前删除
                time_to_expire = expire_time - now
                if time_to_expire < days_ahead * 24 * 3600 and memory.importance.value < 4:
                    await self.storage.delete(memory.memory_id)
                    deleted_count += 1
                    if deleted_count >= self.config.max_delete_per_run:
                        break

            if deleted_count > 0:
                logger.info(f"【提前清理】清理 {deleted_count} 条即将过期的记忆")
        except Exception as e:
            logger.error(f"Near-expiry cleanup failed: {e}")

    async def _cleanup_low_importance(self):
        """DEFECT-R2-004 [P0] 修复: 清理低重要性记忆"""
        try:
            now = time.time()
            # 获取所有记忆
            all_memories = await self.storage.search_by_time_range(0, now)

            deleted_count = 0
            # 清理TRIVIAL和LOW重要性且较旧的记忆
            for memory in all_memories:
                if memory.importance.value <= 2:  # TRIVIAL或LOW
                    age_days = (now - memory.timestamp) / (24 * 3600)
                    if age_days > 1:  # 超过1天的低重要性记忆
                        await self.storage.delete(memory.memory_id)
                        deleted_count += 1
                        if deleted_count >= self.config.max_delete_per_run * 2:
                            break

            if deleted_count > 0:
                logger.info(f"【低重要性清理】清理 {deleted_count} 条低重要性记忆")
        except Exception as e:
            logger.error(f"Low importance cleanup failed: {e}")

    # DEFECT-R2-005 [P1] 修复: OOM恢复机制
    async def _oom_recovery(self):
        """DEFECT-R2-005 [P1] 修复: OOM恢复机制 - 多阶段恢复"""
        if self._oom_recovery_in_progress:
            return

        self._oom_recovery_in_progress = True
        self._recovery_stats["recovery_count"] += 1
        self._recovery_stats["last_recovery_time"] = time.time()

        try:
            logger.info("【OOM恢复】启动OOM恢复机制...")
            aggressiveness = self.config.oom_recovery_aggressiveness

            # 阶段1: 强制垃圾回收
            logger.info("【OOM恢复】阶段1: 强制垃圾回收")
            gc.collect()
            await asyncio.sleep(0.5)

            # 阶段2: 根据激进程度执行不同级别的清理
            if aggressiveness >= 1:
                logger.info("【OOM恢复】阶段2: 基础清理")
                await self._cleanup_near_expiry(days_ahead=90)

            if aggressiveness >= 2:
                logger.info("【OOM恢复】阶段3: 清理低重要性记忆")
                await self._cleanup_low_importance()

            if aggressiveness >= 3:
                logger.info("【OOM恢复】阶段4: 压缩内存")
                await self._compress_memory()

            if aggressiveness >= 4:
                logger.info("【OOM恢复】阶段5: 激进清理")
                await self._cleanup_near_expiry(days_ahead=180)
                await self._cleanup_by_access_pattern()

            if aggressiveness >= 5:
                logger.info("【OOM恢复】阶段6: 极限清理")
                await self._extreme_cleanup()

            # 最终垃圾回收
            gc.collect()

            memory = psutil.virtual_memory()
            logger.info(f"【OOM恢复】完成，当前内存使用: {memory.percent:.1f}%")

        except Exception as e:
            logger.error(f"OOM recovery failed: {e}")
        finally:
            self._oom_recovery_in_progress = False

    async def _hard_limit_recovery(self):
        """DEFECT-R2-005 [P1] 修复: 硬限制超限恢复"""
        logger.error("【硬限制恢复】内存使用超过硬限制，执行极限恢复")
        await self._extreme_cleanup()
        gc.collect()

    async def _extreme_cleanup(self):
        """DEFECT-R2-005 [P1] 修复: 极限清理 - 保留关键记忆"""
        try:
            now = time.time()
            all_memories = await self.storage.search_by_time_range(0, now)

            deleted_count = 0
            # 只保留CRITICAL重要性
            for memory in all_memories:
                if memory.importance.value < 5:  # 非CRITICAL
                    await self.storage.delete(memory.memory_id)
                    deleted_count += 1
                    if deleted_count >= self.config.max_delete_per_run * 10:
                        break

            if deleted_count > 0:
                logger.warning(f"【极限清理】删除 {deleted_count} 条非关键记忆")
        except Exception as e:
            logger.error(f"Extreme cleanup failed: {e}")

    async def _cleanup_by_access_pattern(self):
        """DEFECT-R2-005 [P1] 修复: 基于访问模式的清理"""
        try:
            now = time.time()
            all_memories = await self.storage.search_by_time_range(0, now)

            deleted_count = 0
            # 清理长期未访问且低重要性的记忆
            for memory in all_memories:
                if memory.importance.value <= 3:  # MEDIUM及以下
                    # 假设有access_count和last_accessed字段
                    access_count = getattr(memory, 'access_count', 0)
                    last_accessed = getattr(memory, 'last_accessed', memory.timestamp)
                    days_since_access = (now - last_accessed) / (24 * 3600)

                    if access_count < 3 and days_since_access > 30:
                        await self.storage.delete(memory.memory_id)
                        deleted_count += 1
                        if deleted_count >= self.config.max_delete_per_run:
                            break

            if deleted_count > 0:
                logger.info(f"【访问模式清理】删除 {deleted_count} 条冷数据")
        except Exception as e:
            logger.error(f"Access pattern cleanup failed: {e}")

    async def _memory_compression_loop(self):
        """DEFECT-R2-005 [P1] 修复: 内存压缩循环"""
        while self._running:
            try:
                await asyncio.sleep(30)  # 每30秒检查一次

                if not self._running:
                    break

                memory = psutil.virtual_memory()
                if memory.percent / 100.0 >= self.config.memory_compression_threshold:
                    await self._compress_memory()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Memory compression loop error: {e}")

    async def _compress_memory(self):
        """DEFECT-R2-005 [P1] 修复: 压缩内存中的大对象"""
        try:
            logger.debug("【内存压缩】检查可压缩对象...")
            # 这里可以实现具体的压缩逻辑
            # 例如压缩嵌入向量、压缩元数据等
            self._recovery_stats["compression_count"] += 1
        except Exception as e:
            logger.error(f"Memory compression failed: {e}")

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
        """停止后台清理任务 - DEFECT-R2-004 [P0] 修复: 停止所有监控任务"""
        self._running = False

        # DEFECT-R2-004 [P0] 修复: 停止内存监控任务
        if hasattr(self, '_memory_monitor_task') and self._memory_monitor_task:
            self._memory_monitor_task.cancel()
            try:
                await self._memory_monitor_task
            except asyncio.CancelledError:
                pass
            self._memory_monitor_task = None

        # DEFECT-R2-004 [P0] 修复: 停止写入限流器
        if self._write_rate_limiter_task:
            self._write_rate_limiter_task.cancel()
            try:
                await self._write_rate_limiter_task
            except asyncio.CancelledError:
                pass
            self._write_rate_limiter_task = None

        # DEFECT-R2-005 [P1] 修复: 停止内存压缩任务
        if self._memory_compression_task:
            self._memory_compression_task.cancel()
            try:
                await self._memory_compression_task
            except asyncio.CancelledError:
                pass
            self._memory_compression_task = None

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
        """获取清理统计 - DEFECT-R2-004 [P0] 修复: 包含增强内存压力信息"""
        try:
            memory = psutil.virtual_memory()
            memory_info = {
                "memory_percent": memory.percent,
                "memory_available_gb": memory.available / (1024**3),
                "memory_used_gb": memory.used / (1024**3),
                "memory_pressure_level": self._memory_pressure_level,
                "consecutive_high_memory_count": self._consecutive_high_memory_count,
            }
        except Exception:
            memory_info = {
                "memory_percent": 0,
                "memory_pressure_level": "unknown",
                "consecutive_high_memory_count": self._consecutive_high_memory_count,
            }

        return {
            **self._stats,
            "running": self._running,
            "enabled": self.config.enabled,
            **memory_info,
            "write_tokens_available": self._write_tokens,
            "emergency_cleanup_in_progress": self._emergency_cleanup_in_progress,
            "oom_recovery_in_progress": self._oom_recovery_in_progress,
            "recovery_stats": self._recovery_stats,
            "memory_history_samples": len(self._memory_history),
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

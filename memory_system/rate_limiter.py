"""精细化API限流器"""

import time
import asyncio
from typing import Dict, Optional, Callable, Any, List
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import threading


class RateLimitStrategy(Enum):
    """限流策略"""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    """限流配置"""
    requests_per_minute: int = 60
    burst_size: int = 10
    strategy: RateLimitStrategy = RateLimitStrategy.SLIDING_WINDOW
    per_user: bool = True
    per_endpoint: bool = True
    key_extractor: Optional[Callable[[Any], str]] = None
    window_size: int = 60  # 窗口大小(秒)
    block_duration: int = 0  # 封禁时长(秒), 0表示不封禁


@dataclass
class RateLimitState:
    """限流状态"""
    requests: deque  # 请求时间戳队列
    tokens: float  # 令牌桶当前令牌数
    last_update: float  # 最后更新时间
    blocked_until: float  # 封禁结束时间


class RateLimiter:
    """精细化限流器"""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._states: Dict[str, RateLimitState] = {}
        self._lock = threading.RLock()

    def _get_key(self, user_id: str, endpoint: str, **kwargs) -> str:
        """生成限流键

        Args:
            user_id: 用户ID
            endpoint: 端点名称
            **kwargs: 额外参数

        Returns:
            str: 限流键
        """
        parts = []
        if self.config.per_user and user_id:
            parts.append(user_id)
        if self.config.per_endpoint and endpoint:
            parts.append(endpoint)

        # 使用自定义键提取器
        if self.config.key_extractor:
            custom_key = self.config.key_extractor(kwargs)
            if custom_key:
                parts.append(custom_key)

        return ":".join(parts) if parts else "global"

    def _get_or_create_state(self, key: str) -> RateLimitState:
        """获取或创建限流状态"""
        if key not in self._states:
            self._states[key] = RateLimitState(
                requests=deque(),
                tokens=self.config.burst_size,
                last_update=time.time(),
                blocked_until=0
            )
        return self._states[key]

    def is_allowed(self, user_id: str = "", endpoint: str = "", **kwargs) -> bool:
        """检查是否允许请求

        Args:
            user_id: 用户ID
            endpoint: 端点名称
            **kwargs: 额外参数

        Returns:
            bool: 是否允许请求
        """
        key = self._get_key(user_id, endpoint, **kwargs)

        with self._lock:
            state = self._get_or_create_state(key)

            # 检查是否被封禁
            if state.blocked_until > time.time():
                return False

            # 根据策略检查限流
            if self.config.strategy == RateLimitStrategy.SLIDING_WINDOW:
                allowed = self._check_sliding_window(key, state)
            elif self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
                allowed = self._check_token_bucket(key, state)
            elif self.config.strategy == RateLimitStrategy.LEAKY_BUCKET:
                allowed = self._check_leaky_bucket(key, state)
            else:
                allowed = self._check_fixed_window(key, state)

            # 如果请求被拒绝且配置了封禁时长
            if not allowed and self.config.block_duration > 0:
                state.blocked_until = time.time() + self.config.block_duration

            return allowed

    def _check_sliding_window(self, key: str, state: RateLimitState) -> bool:
        """滑动窗口限流

        在滑动时间窗口内统计请求数
        """
        now = time.time()
        window_start = now - self.config.window_size

        # 移除窗口外的旧请求记录
        while state.requests and state.requests[0] < window_start:
            state.requests.popleft()

        # 检查是否超过限制
        if len(state.requests) >= self.config.requests_per_minute:
            return False

        # 记录当前请求
        state.requests.append(now)
        return True

    def _check_token_bucket(self, key: str, state: RateLimitState) -> bool:
        """令牌桶限流

        以固定速率生成令牌，请求消耗令牌
        """
        now = time.time()

        # 计算新产生的令牌
        time_passed = now - state.last_update
        tokens_to_add = time_passed * (self.config.requests_per_minute / 60)
        state.tokens = min(self.config.burst_size, state.tokens + tokens_to_add)
        state.last_update = now

        # 消费令牌
        if state.tokens >= 1:
            state.tokens -= 1
            return True
        return False

    def _check_leaky_bucket(self, key: str, state: RateLimitState) -> bool:
        """漏桶限流

        以固定速率处理请求，平滑流量
        """
        now = time.time()

        # 计算漏出的请求数
        time_passed = now - state.last_update
        leaked = time_passed * (self.config.requests_per_minute / 60)

        # 更新当前水位
        current_level = max(0, len(state.requests) - leaked)

        # 检查是否溢出
        if current_level >= self.config.burst_size:
            return False

        # 添加新请求
        state.requests = deque([now] * int(current_level + 1))
        state.last_update = now
        return True

    def _check_fixed_window(self, key: str, state: RateLimitState) -> bool:
        """固定窗口限流

        在每个固定时间窗口内统计请求数
        """
        now = time.time()
        window = int(now / self.config.window_size)
        window_key = f"{key}:{window}"

        # 检查是否是新窗口
        if not hasattr(state, '_current_window') or state._current_window != window:
            state._current_window = window
            state._window_count = 0

        if state._window_count >= self.config.requests_per_minute:
            return False

        state._window_count += 1
        return True

    def get_remaining_quota(self, user_id: str = "", endpoint: str = "", **kwargs) -> Dict[str, Any]:
        """获取剩余配额信息

        Args:
            user_id: 用户ID
            endpoint: 端点名称
            **kwargs: 额外参数

        Returns:
            Dict: 配额信息
        """
        key = self._get_key(user_id, endpoint, **kwargs)

        with self._lock:
            state = self._get_or_create_state(key)
            now = time.time()

            # 检查封禁状态
            if state.blocked_until > now:
                return {
                    "allowed": False,
                    "blocked": True,
                    "blocked_seconds": int(state.blocked_until - now),
                    "remaining": 0,
                    "reset_time": state.blocked_until
                }

            # 根据策略计算剩余配额
            if self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
                # 更新令牌数
                time_passed = now - state.last_update
                tokens_to_add = time_passed * (self.config.requests_per_minute / 60)
                remaining_tokens = min(self.config.burst_size, state.tokens + tokens_to_add)

                return {
                    "allowed": remaining_tokens >= 1,
                    "blocked": False,
                    "remaining": int(remaining_tokens),
                    "limit": self.config.burst_size,
                    "reset_time": now + (1 - remaining_tokens % 1) * 60 / self.config.requests_per_minute
                }

            elif self.config.strategy == RateLimitStrategy.SLIDING_WINDOW:
                window_start = now - self.config.window_size
                # 清理旧请求
                while state.requests and state.requests[0] < window_start:
                    state.requests.popleft()

                remaining = max(0, self.config.requests_per_minute - len(state.requests))

                return {
                    "allowed": remaining > 0,
                    "blocked": False,
                    "remaining": remaining,
                    "limit": self.config.requests_per_minute,
                    "reset_time": state.requests[0] + self.config.window_size if state.requests else now
                }

            else:
                return {
                    "allowed": True,
                    "blocked": False,
                    "remaining": self.config.requests_per_minute,
                    "limit": self.config.requests_per_minute,
                    "reset_time": now + self.config.window_size
                }

    def reset(self, user_id: str = "", endpoint: str = "", **kwargs) -> None:
        """重置限流状态

        Args:
            user_id: 用户ID
            endpoint: 端点名称
            **kwargs: 额外参数
        """
        key = self._get_key(user_id, endpoint, **kwargs)

        with self._lock:
            if key in self._states:
                del self._states[key]

    def reset_all(self) -> None:
        """重置所有限流状态"""
        with self._lock:
            self._states.clear()


class AsyncRateLimiter(RateLimiter):
    """异步限流器"""

    def __init__(self, config: RateLimitConfig):
        super().__init__(config)
        self._async_lock = asyncio.Lock()

    async def is_allowed(self, user_id: str = "", endpoint: str = "", **kwargs) -> bool:
        """异步检查是否允许请求"""
        key = self._get_key(user_id, endpoint, **kwargs)

        async with self._async_lock:
            state = self._get_or_create_state(key)

            # 检查是否被封禁
            if state.blocked_until > time.time():
                return False

            # 根据策略检查限流
            if self.config.strategy == RateLimitStrategy.SLIDING_WINDOW:
                allowed = self._check_sliding_window(key, state)
            elif self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
                allowed = self._check_token_bucket(key, state)
            elif self.config.strategy == RateLimitStrategy.LEAKY_BUCKET:
                allowed = self._check_leaky_bucket(key, state)
            else:
                allowed = self._check_fixed_window(key, state)

            if not allowed and self.config.block_duration > 0:
                state.blocked_until = time.time() + self.config.block_duration

            return allowed

    async def get_remaining_quota(self, user_id: str = "", endpoint: str = "", **kwargs) -> Dict[str, Any]:
        """异步获取剩余配额"""
        key = self._get_key(user_id, endpoint, **kwargs)

        async with self._async_lock:
            return super().get_remaining_quota(user_id, endpoint, **kwargs)


class RateLimitMiddleware:
    """限流中间件 - 用于Web框架集成"""

    def __init__(self, config: RateLimitConfig, skip_paths: Optional[List[str]] = None):
        self.limiter = AsyncRateLimiter(config)
        self.skip_paths = skip_paths or []

    def _should_skip(self, path: str) -> bool:
        """检查路径是否应该跳过限流"""
        return any(path.startswith(skip) for skip in self.skip_paths)

    async def check_rate_limit(self, request) -> Optional[Dict[str, Any]]:
        """检查请求是否超过限流

        Args:
            request: HTTP请求对象

        Returns:
            Optional[Dict]: 如果超过限流返回错误信息，否则返回None
        """
        path = getattr(request, 'path', '')

        if self._should_skip(path):
            return None

        user_id = getattr(request, 'user_id', 'anonymous')
        endpoint = path

        quota = await self.limiter.get_remaining_quota(user_id, endpoint)

        if not quota["allowed"]:
            return {
                "error": "Rate limit exceeded",
                "retry_after": int(quota.get("reset_time", 60) - time.time()),
                "limit": quota.get("limit", 0),
                "remaining": 0
            }

        return None

    def get_rate_limit_headers(self, user_id: str = "", endpoint: str = "") -> Dict[str, str]:
        """获取限流相关的HTTP头

        Args:
            user_id: 用户ID
            endpoint: 端点名称

        Returns:
            Dict: HTTP头字典
        """
        quota = self.limiter.get_remaining_quota(user_id, endpoint)

        return {
            "X-RateLimit-Limit": str(quota.get("limit", 0)),
            "X-RateLimit-Remaining": str(quota.get("remaining", 0)),
            "X-RateLimit-Reset": str(int(quota.get("reset_time", 0)))
        }


class MultiTierRateLimiter:
    """多层限流器

    支持全局、用户、端点多层限流
    """

    def __init__(self):
        self.limiters: Dict[str, RateLimiter] = {}

    def add_tier(self, name: str, config: RateLimitConfig) -> None:
        """添加限流层级

        Args:
            name: 层级名称
            config: 限流配置
        """
        self.limiters[name] = RateLimiter(config)

    def is_allowed(self, **kwargs) -> tuple[bool, Optional[str]]:
        """检查所有层级是否允许请求

        Returns:
            tuple: (是否允许, 拒绝的层级名称)
        """
        for name, limiter in self.limiters.items():
            if not limiter.is_allowed(**kwargs):
                return False, name
        return True, None

    def get_quotas(self, **kwargs) -> Dict[str, Dict[str, Any]]:
        """获取所有层级的配额信息"""
        return {
            name: limiter.get_remaining_quota(**kwargs)
            for name, limiter in self.limiters.items()
        }

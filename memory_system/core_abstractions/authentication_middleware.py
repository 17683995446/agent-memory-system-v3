"""
API认证中间件
支持API Key和OAuth2两种认证方式
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader, OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import Optional, Dict
import hashlib
import hmac
import os
import base64
import secrets
import logging

logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token", auto_error=False)


def hash_password(password: str) -> str:
    """使用 PBKDF2-HMAC-SHA256 对密码进行安全哈希"""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
    return base64.b64encode(salt + dk).decode('ascii')


def verify_password(password: str, hashed: str) -> bool:
    """验证密码是否匹配哈希值"""
    try:
        data = base64.b64decode(hashed.encode())
        salt = data[:16]
        dk = data[16:]
        new_dk = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
        return hmac.compare_digest(dk, new_dk)
    except Exception as e:
        logger.warning("密码验证失败: %s", e)
        return False


async def verify_api_key(
    api_key: Optional[str] = Depends(api_key_header),
    expected_key: Optional[str] = None
) -> bool:
    """验证API Key
    
    Args:
        api_key: 请求头中的API Key
        expected_key: 配置的期望API Key
        
    Returns:
        是否验证通过
    """
    if not expected_key:
        return True
    if not api_key:
        return False
    return secrets.compare_digest(api_key, expected_key)


async def verify_oauth2_token(
    token: str = Depends(oauth2_scheme),
    oauth2_users: Optional[Dict[str, str]] = None
) -> str:
    """验证OAuth2 Token
    
    Args:
        token: Bearer Token
        oauth2_users: 配置的用户名->密码哈希映射
        
    Returns:
        用户名
        
    Raises:
        HTTPException: 认证失败时抛出401错误
    """
    if not oauth2_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth2 not configured",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # 简化实现：token即username，验证用户存在即可
    # 实际生产环境应使用JWT
    if token not in oauth2_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token


class AuthenticationManager:
    """认证管理器"""
    
    def __init__(
        self,
        enabled: bool = False,
        api_key: Optional[str] = None,
        oauth2_enabled: bool = False,
        oauth2_users: Optional[Dict[str, str]] = None
    ):
        self.enabled = enabled
        self.api_key = api_key
        self.oauth2_enabled = oauth2_enabled
        self.oauth2_users = oauth2_users or {}
    
    async def authenticate(
        self,
        api_key: Optional[str] = Depends(api_key_header),
        token: Optional[str] = Depends(oauth2_scheme)
    ) -> str:
        """执行认证
        
        优先检查API Key，然后检查OAuth2 Token
        
        Args:
            api_key: 请求头中的API Key
            token: Bearer Token
            
        Returns:
            认证标识（api_key 或 username）
            
        Raises:
            HTTPException: 认证失败时抛出401错误
        """
        if not self.enabled:
            return "anonymous"
        
        # 优先尝试API Key认证
        if self.api_key and api_key:
            if await verify_api_key(api_key, self.api_key):
                return "api_key"
        
        # 尝试OAuth2认证
        if self.oauth2_enabled and self.oauth2_users and token:
            if token in self.oauth2_users:
                return token
        
        # 认证失败
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer, X-API-Key"},
        )
    
    def authenticate_user(self, username: str, password: str) -> Optional[str]:
        """验证用户名密码
        
        Args:
            username: 用户名
            password: 明文密码
            
        Returns:
            验证成功返回用户名，失败返回None
        """
        if not self.oauth2_enabled or not self.oauth2_users:
            return None
        
        hashed = self.oauth2_users.get(username)
        if not hashed:
            return None
        
        if verify_password(password, hashed):
            return username
        return None


# 全局认证管理器实例
_auth_manager: Optional[AuthenticationManager] = None


def get_auth_manager() -> Optional[AuthenticationManager]:
    """获取全局认证管理器实例"""
    return _auth_manager


def set_auth_manager(manager: AuthenticationManager) -> None:
    """设置全局认证管理器实例"""
    global _auth_manager
    _auth_manager = manager


def reset_auth_manager() -> None:
    """重置全局认证管理器实例"""
    global _auth_manager
    _auth_manager = None


async def verify_authentication(
    api_key: Optional[str] = Depends(api_key_header),
    token: Optional[str] = Depends(oauth2_scheme)
) -> str:
    """FastAPI依赖：验证认证
    
    用于保护端点的依赖函数
    
    Args:
        api_key: 请求头中的API Key
        token: Bearer Token
        
    Returns:
        认证标识
    """
    manager = get_auth_manager()
    if manager is None:
        # 未配置认证管理器，允许访问
        return "anonymous"
    return await manager.authenticate(api_key, token)

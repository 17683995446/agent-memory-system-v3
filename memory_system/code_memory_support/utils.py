"""
便捷函数
"""

from typing import Any, Dict, Optional

from memory_system.code_memory_support.language_detector import ProgrammingLanguage
from memory_system.code_memory_support.code_memory import CodeMemory


def parse_code(code: str, language: Optional[str] = None, filename: Optional[str] = None) -> Dict[str, Any]:
    """
    解析代码的便捷函数
    
    Args:
        code: 源代码
        language: 语言名称（可选）
        filename: 文件名（可选）
        
    Returns:
        解析结果
    """
    cm = CodeMemory()
    lang = ProgrammingLanguage(language) if language else None
    return cm.store(code, filename=filename, language=lang)

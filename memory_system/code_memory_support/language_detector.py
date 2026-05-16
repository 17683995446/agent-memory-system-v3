"""
编程语言检测器
基于文件扩展名和内容特征
"""

import re
from typing import Dict, Optional
from enum import Enum


class ProgrammingLanguage(str, Enum):
    """支持的编程语言"""
    PYTHON = "python"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    JAVA = "java"
    C = "c"
    CPP = "cpp"
    GO = "go"
    RUST = "rust"
    RUBY = "ruby"
    PHP = "php"
    KOTLIN = "kotlin"
    SWIFT = "swift"
    CSHARP = "csharp"
    BASH = "bash"
    JSON = "json"
    YAML = "yaml"
    HTML = "html"
    CSS = "css"
    SQL = "sql"
    UNKNOWN = "unknown"


class LanguageDetector:
    """
    编程语言检测器
    基于文件扩展名和内容特征
    """
    
    EXTENSION_MAP = {
        ".py": ProgrammingLanguage.PYTHON,
        ".js": ProgrammingLanguage.JAVASCRIPT,
        ".jsx": ProgrammingLanguage.JAVASCRIPT,
        ".ts": ProgrammingLanguage.TYPESCRIPT,
        ".tsx": ProgrammingLanguage.TYPESCRIPT,
        ".java": ProgrammingLanguage.JAVA,
        ".c": ProgrammingLanguage.C,
        ".h": ProgrammingLanguage.C,
        ".cpp": ProgrammingLanguage.CPP,
        ".hpp": ProgrammingLanguage.CPP,
        ".cc": ProgrammingLanguage.CPP,
        ".go": ProgrammingLanguage.GO,
        ".rs": ProgrammingLanguage.RUST,
        ".rb": ProgrammingLanguage.RUBY,
        ".php": ProgrammingLanguage.PHP,
        ".kt": ProgrammingLanguage.KOTLIN,
        ".kts": ProgrammingLanguage.KOTLIN,
        ".swift": ProgrammingLanguage.SWIFT,
        ".cs": ProgrammingLanguage.CSHARP,
        ".sh": ProgrammingLanguage.BASH,
        ".bash": ProgrammingLanguage.BASH,
        ".json": ProgrammingLanguage.JSON,
        ".yaml": ProgrammingLanguage.YAML,
        ".yml": ProgrammingLanguage.YAML,
        ".html": ProgrammingLanguage.HTML,
        ".htm": ProgrammingLanguage.HTML,
        ".css": ProgrammingLanguage.CSS,
        ".sql": ProgrammingLanguage.SQL,
    }
    
    # 内容特征模式
    CONTENT_PATTERNS = {
        ProgrammingLanguage.PYTHON: [
            r'^\s*def\s+\w+\s*\(',
            r'^\s*class\s+\w+',
            r'^\s*import\s+\w+',
            r'^\s*from\s+\w+\s+import',
        ],
        ProgrammingLanguage.JAVASCRIPT: [
            r'^\s*function\s+\w+\s*\(',
            r'^\s*const\s+\w+\s*=',
            r'^\s*let\s+\w+\s*=',
            r'^\s*export\s+',
        ],
        ProgrammingLanguage.JAVA: [
            r'^\s*public\s+class\s+\w+',
            r'^\s*private\s+\w+\s+\w+',
            r'^\s*package\s+[\w.]+',
        ],
        ProgrammingLanguage.GO: [
            r'^\s*package\s+\w+',
            r'^\s*func\s+\w+',
            r'^\s*import\s*\(',
        ],
        ProgrammingLanguage.RUST: [
            r'^\s*fn\s+\w+',
            r'^\s*struct\s+\w+',
            r'^\s*impl\s+\w+',
            r'^\s*use\s+[\w:]+',
        ],
    }
    
    @classmethod
    def detect_by_extension(cls, filename: str) -> ProgrammingLanguage:
        """根据文件扩展名检测语言"""
        import os
        _, ext = os.path.splitext(filename.lower())
        return cls.EXTENSION_MAP.get(ext, ProgrammingLanguage.UNKNOWN)
    
    @classmethod
    def detect_by_content(cls, code: str) -> ProgrammingLanguage:
        """根据代码内容特征检测语言"""
        lines = code.split('\n')
        
        scores: Dict[ProgrammingLanguage, int] = {}
        
        for lang, patterns in cls.CONTENT_PATTERNS.items():
            score = 0
            for pattern in patterns:
                for line in lines[:50]:  # 只检查前50行
                    if re.match(pattern, line):
                        score += 1
            if score > 0:
                scores[lang] = score
        
        if scores:
            return max(scores, key=scores.get)
        return ProgrammingLanguage.UNKNOWN
    
    @classmethod
    def detect(cls, code: str, filename: Optional[str] = None) -> ProgrammingLanguage:
        """综合检测编程语言"""
        # 优先使用文件扩展名
        if filename:
            lang = cls.detect_by_extension(filename)
            if lang != ProgrammingLanguage.UNKNOWN:
                return lang
        
        # 回退到内容检测
        return cls.detect_by_content(code)

"""
代码记忆类
支持存储代码并解析 AST 结构
"""

import time
from typing import Any, Dict, List, Optional

from memory_system.code_memory_support.language_detector import (
    ProgrammingLanguage,
    LanguageDetector,
)
from memory_system.code_memory_support.ast_parser import ASTParser
from memory_system.code_memory_support.code_analyzer import CodeAnalyzer


class CodeMemory:
    """
    代码记忆类
    支持存储代码并解析 AST 结构
    """
    
    def __init__(self):
        self.language_detector = LanguageDetector()
        self.ast_parser = ASTParser()
        self.code_analyzer = CodeAnalyzer()
    
    def store(
        self,
        code: str,
        filename: Optional[str] = None,
        language: Optional[ProgrammingLanguage] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        存储代码并解析结构
        
        Args:
            code: 源代码文本
            filename: 文件名（用于语言检测）
            language: 指定语言（覆盖自动检测）
            metadata: 附加元数据
            
        Returns:
            包含代码和解析结果的字典
        """
        # 检测或使用指定语言
        if language is None:
            language = self.language_detector.detect(code, filename)
        
        # 解析 AST
        ast = self.ast_parser.parse(code, language)
        
        # 分析代码结构
        structure = self.code_analyzer.analyze(ast, language)
        
        # 构建结果
        result = {
            "code": code,
            "language": language.value,
            "filename": filename,
            "line_count": len(code.split('\n')),
            "char_count": len(code),
            "structure": structure.to_dict(),
            "metadata": metadata or {},
            "timestamp": time.time(),
            "ast_available": self.ast_parser.is_available
        }
        
        return result
    
    def extract_functions(self, code: str, language: Optional[ProgrammingLanguage] = None) -> List[Dict[str, Any]]:
        """提取代码中的所有函数"""
        if language is None:
            language = self.language_detector.detect(code)
        
        ast = self.ast_parser.parse(code, language)
        structure = self.code_analyzer.analyze(ast, language)
        return structure.functions
    
    def extract_classes(self, code: str, language: Optional[ProgrammingLanguage] = None) -> List[Dict[str, Any]]:
        """提取代码中的所有类"""
        if language is None:
            language = self.language_detector.detect(code)
        
        ast = self.ast_parser.parse(code, language)
        structure = self.code_analyzer.analyze(ast, language)
        return structure.classes
    
    def get_summary(self, code: str, language: Optional[ProgrammingLanguage] = None, filename: Optional[str] = None) -> str:
        """获取代码摘要"""
        result = self.store(code, filename=filename, language=language)
        structure = result["structure"]
        
        lines = [
            f"语言: {result['language']}",
            f"行数: {result['line_count']}",
            f"函数: {structure['ast_summary']['total_functions']} 个",
            f"类: {structure['ast_summary']['total_classes']} 个",
            f"导入: {structure['ast_summary']['total_imports']} 个",
        ]
        
        if structure['functions']:
            lines.append(f"函数列表: {', '.join(f['name'] for f in structure['functions'][:5])}")
        
        if structure['classes']:
            lines.append(f"类列表: {', '.join(c['name'] for c in structure['classes'][:5])}")
        
        return '\n'.join(lines)
    
    @property
    def supported_languages(self) -> List[str]:
        """返回支持的语言列表"""
        return [lang.value for lang in ProgrammingLanguage if lang != ProgrammingLanguage.UNKNOWN]
    
    @property
    def ast_available(self) -> bool:
        """AST 解析是否可用"""
        return self.ast_parser.is_available

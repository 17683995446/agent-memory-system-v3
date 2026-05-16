"""
AST 解析器
基于 tree-sitter 的多语言解析
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import logging

from memory_system.code_memory_support.language_detector import ProgrammingLanguage

logger = logging.getLogger(__name__)


@dataclass
class ASTNode:
    """AST 节点"""
    node_type: str
    text: str
    start_byte: int
    end_byte: int
    start_point: Tuple[int, int]  # (row, column)
    end_point: Tuple[int, int]
    children: List["ASTNode"] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "text": self.text[:100] + "..." if len(self.text) > 100 else self.text,
            "start_byte": self.start_byte,
            "end_byte": self.end_byte,
            "start_point": self.start_point,
            "end_point": self.end_point,
            "children": [c.to_dict() for c in self.children]
        }


class ASTParser:
    """
    AST 解析器
    基于 tree-sitter 的多语言解析
    """
    
    def __init__(self):
        self._parsers: Dict[ProgrammingLanguage, Any] = {}
        self._available = self._check_availability()
    
    def _check_availability(self) -> bool:
        """检查 tree-sitter 是否可用"""
        try:
            import tree_sitter
            return True
        except ImportError:
            return False
    
    def _get_language(self, lang: ProgrammingLanguage) -> Optional[Any]:
        """获取 tree-sitter 语言对象"""
        if not self._available:
            return None
        
        try:
            import tree_sitter
            from tree_sitter import Language
            
            language_map = {
                ProgrammingLanguage.PYTHON: ("tree_sitter_python", "python"),
                ProgrammingLanguage.JAVASCRIPT: ("tree_sitter_javascript", "javascript"),
                ProgrammingLanguage.TYPESCRIPT: ("tree_sitter_typescript", "typescript"),
                ProgrammingLanguage.JAVA: ("tree_sitter_java", "java"),
                ProgrammingLanguage.C: ("tree_sitter_c", "c"),
                ProgrammingLanguage.CPP: ("tree_sitter_cpp", "cpp"),
                ProgrammingLanguage.GO: ("tree_sitter_go", "go"),
                ProgrammingLanguage.RUST: ("tree_sitter_rust", "rust"),
                ProgrammingLanguage.RUBY: ("tree_sitter_ruby", "ruby"),
                ProgrammingLanguage.PHP: ("tree_sitter_php", "php"),
                ProgrammingLanguage.KOTLIN: ("tree_sitter_kotlin", "kotlin"),
                ProgrammingLanguage.SWIFT: ("tree_sitter_swift", "swift"),
                ProgrammingLanguage.CSHARP: ("tree_sitter_c_sharp", "c_sharp"),
                ProgrammingLanguage.BASH: ("tree_sitter_bash", "bash"),
                ProgrammingLanguage.JSON: ("tree_sitter_json", "json"),
                ProgrammingLanguage.YAML: ("tree_sitter_yaml", "yaml"),
                ProgrammingLanguage.HTML: ("tree_sitter_html", "html"),
                ProgrammingLanguage.CSS: ("tree_sitter_css", "css"),
                ProgrammingLanguage.SQL: ("tree_sitter_sql", "sql"),
            }
            
            if lang not in language_map:
                return None
            
            module_name, _ = language_map[lang]
            module = __import__(module_name)
            return Language(module.language())
            
        except ImportError:
            return None
    
    def _get_parser(self, lang: ProgrammingLanguage) -> Optional[Any]:
        """获取或创建解析器"""
        if lang in self._parsers:
            return self._parsers[lang]
        
        if not self._available:
            return None
        
        try:
            from tree_sitter import Parser
            language = self._get_language(lang)
            if language is None:
                return None
            parser = Parser(language)
            self._parsers[lang] = parser
            return parser
        except Exception as e:
            logger.warning("AST解析器创建失败 [%s]: %s", lang, e)
            return None
    
    def parse(self, code: str, language: ProgrammingLanguage) -> Optional[ASTNode]:
        """解析代码生成 AST"""
        parser = self._get_parser(language)
        if parser is None:
            return None
        
        try:
            tree = parser.parse(bytes(code, "utf8"))
            return self._convert_node(tree.root_node, code)
        except Exception as e:
            logger.warning("AST解析失败 [%s]: %s", language, e)
            return None
    
    def _convert_node(self, node: Any, code: str) -> ASTNode:
        """将 tree-sitter 节点转换为 ASTNode"""
        children = [self._convert_node(child, code) for child in node.children]
        
        return ASTNode(
            node_type=node.type,
            text=code[node.start_byte:node.end_byte],
            start_byte=node.start_byte,
            end_byte=node.end_byte,
            start_point=(node.start_point.row, node.start_point.column),
            end_point=(node.end_point.row, node.end_point.column),
            children=children
        )
    
    @property
    def is_available(self) -> bool:
        """tree-sitter 是否可用"""
        return self._available

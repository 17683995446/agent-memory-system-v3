"""
代码分析器
提取代码结构信息
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from memory_system.code_memory_support.language_detector import ProgrammingLanguage
from memory_system.code_memory_support.ast_parser import ASTNode


@dataclass
class CodeStructure:
    """代码结构信息"""
    language: ProgrammingLanguage
    functions: List[Dict[str, Any]] = field(default_factory=list)
    classes: List[Dict[str, Any]] = field(default_factory=list)
    imports: List[str] = field(default_factory=list)
    variables: List[str] = field(default_factory=list)
    comments: List[str] = field(default_factory=list)
    ast_root: Optional[ASTNode] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "language": self.language.value,
            "functions": self.functions,
            "classes": self.classes,
            "imports": self.imports,
            "variables": self.variables,
            "comments": self.comments,
            "ast_summary": {
                "total_functions": len(self.functions),
                "total_classes": len(self.classes),
                "total_imports": len(self.imports)
            }
        }


class CodeAnalyzer:
    """
    代码分析器
    提取代码结构信息
    """
    
    # 各语言的函数定义节点类型
    FUNCTION_NODES = {
        ProgrammingLanguage.PYTHON: ["function_definition", "lambda"],
        ProgrammingLanguage.JAVASCRIPT: ["function_declaration", "function_expression", "arrow_function", "method_definition"],
        ProgrammingLanguage.TYPESCRIPT: ["function_declaration", "function_expression", "arrow_function", "method_definition"],
        ProgrammingLanguage.JAVA: ["method_declaration", "constructor_declaration"],
        ProgrammingLanguage.C: ["function_definition"],
        ProgrammingLanguage.CPP: ["function_definition", "declaration"],
        ProgrammingLanguage.GO: ["function_declaration", "method_declaration"],
        ProgrammingLanguage.RUST: ["function_item"],
    }
    
    # 各语言的类定义节点类型
    CLASS_NODES = {
        ProgrammingLanguage.PYTHON: ["class_definition"],
        ProgrammingLanguage.JAVASCRIPT: ["class_declaration", "class_expression"],
        ProgrammingLanguage.TYPESCRIPT: ["class_declaration", "class_expression", "interface_declaration"],
        ProgrammingLanguage.JAVA: ["class_declaration", "interface_declaration", "enum_declaration"],
        ProgrammingLanguage.CPP: ["class_specifier", "struct_specifier"],
        ProgrammingLanguage.GO: ["type_declaration"],
        ProgrammingLanguage.RUST: ["struct_item", "enum_item", "trait_item"],
    }
    
    # 各语言的导入节点类型
    IMPORT_NODES = {
        ProgrammingLanguage.PYTHON: ["import_statement", "import_from_statement"],
        ProgrammingLanguage.JAVASCRIPT: ["import_statement"],
        ProgrammingLanguage.TYPESCRIPT: ["import_statement"],
        ProgrammingLanguage.JAVA: ["import_declaration"],
        ProgrammingLanguage.GO: ["import_declaration"],
        ProgrammingLanguage.RUST: ["use_declaration"],
    }
    
    def analyze(self, ast: Optional[ASTNode], language: ProgrammingLanguage) -> CodeStructure:
        """分析 AST 提取代码结构"""
        structure = CodeStructure(language=language)
        
        if ast is None:
            return structure
        
        structure.ast_root = ast
        
        # 获取该语言的节点类型
        func_types = self.FUNCTION_NODES.get(language, [])
        class_types = self.CLASS_NODES.get(language, [])
        import_types = self.IMPORT_NODES.get(language, [])
        
        # 遍历 AST 提取信息
        self._traverse(ast, func_types, class_types, import_types, structure)
        
        return structure
    
    def _traverse(
        self,
        node: ASTNode,
        func_types: List[str],
        class_types: List[str],
        import_types: List[str],
        structure: CodeStructure
    ):
        """遍历 AST 节点"""
        # 提取函数
        if node.node_type in func_types:
            func_info = self._extract_function(node)
            if func_info:
                structure.functions.append(func_info)
        
        # 提取类
        if node.node_type in class_types:
            class_info = self._extract_class(node)
            if class_info:
                structure.classes.append(class_info)
        
        # 提取导入
        if node.node_type in import_types:
            structure.imports.append(node.text.strip())
        
        # 提取注释
        if "comment" in node.node_type.lower():
            structure.comments.append(node.text.strip())
        
        # 递归遍历子节点
        for child in node.children:
            self._traverse(child, func_types, class_types, import_types, structure)
    
    def _extract_function(self, node: ASTNode) -> Optional[Dict[str, Any]]:
        """提取函数信息"""
        # 查找函数名
        name = None
        for child in node.children:
            if "identifier" in child.node_type.lower() or "name" in child.node_type.lower():
                name = child.text
                break
        
        return {
            "name": name or "<anonymous>",
            "type": node.node_type,
            "start": node.start_point,
            "end": node.end_point,
            "line_count": node.end_point[0] - node.start_point[0] + 1
        }
    
    def _extract_class(self, node: ASTNode) -> Optional[Dict[str, Any]]:
        """提取类信息"""
        # 查找类名
        name = None
        for child in node.children:
            if "identifier" in child.node_type.lower() or "name" in child.node_type.lower():
                name = child.text
                break
        
        return {
            "name": name or "<anonymous>",
            "type": node.node_type,
            "start": node.start_point,
            "end": node.end_point,
            "line_count": node.end_point[0] - node.start_point[0] + 1
        }

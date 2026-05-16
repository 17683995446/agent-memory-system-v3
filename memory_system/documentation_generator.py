#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文档生成系统 (Documentation Generator System)

提供自动API文档生成、代码注释提取、架构图生成、变更日志管理、
多格式输出(Markdown/HTML/PDF)和版本化文档功能。

Author: Agent Memory System Team
Version: 3.0.0
"""

import ast
import inspect
import json
import logging
import os
import re
import subprocess
import sys
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

# 配置日志
logger = logging.getLogger(__name__)


class DocFormat(Enum):
    """文档格式"""
    MARKDOWN = "markdown"
    HTML = "html"
    PDF = "pdf"
    JSON = "json"
    YAML = "yaml"


class DiagramType(Enum):
    """图表类型"""
    ARCHITECTURE = "architecture"
    CLASS = "class"
    SEQUENCE = "sequence"
    FLOWCHART = "flowchart"
    ER = "er"  # Entity-Relationship


@dataclass
class FunctionDoc:
    """函数文档"""
    name: str
    signature: str
    docstring: str
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    returns: Optional[Dict[str, Any]] = None
    raises: List[str] = field(default_factory=list)
    examples: List[str] = field(default_factory=list)
    decorators: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "signature": self.signature,
            "docstring": self.docstring,
            "parameters": self.parameters,
            "returns": self.returns,
            "raises": self.raises,
            "examples": self.examples,
            "decorators": self.decorators,
        }


@dataclass
class ClassDoc:
    """类文档"""
    name: str
    docstring: str
    bases: List[str] = field(default_factory=list)
    methods: List[FunctionDoc] = field(default_factory=list)
    attributes: List[Dict[str, Any]] = field(default_factory=list)
    is_abstract: bool = False
    is_dataclass: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "docstring": self.docstring,
            "bases": self.bases,
            "methods": [m.to_dict() for m in self.methods],
            "attributes": self.attributes,
            "is_abstract": self.is_abstract,
            "is_dataclass": self.is_dataclass,
        }


@dataclass
class ModuleDoc:
    """模块文档"""
    name: str
    docstring: str
    path: str
    classes: List[ClassDoc] = field(default_factory=list)
    functions: List[FunctionDoc] = field(default_factory=list)
    imports: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "docstring": self.docstring,
            "path": self.path,
            "classes": [c.to_dict() for c in self.classes],
            "functions": [f.to_dict() for f in self.functions],
            "imports": self.imports,
        }


@dataclass
class APIEndpointDoc:
    """API端点文档"""
    path: str
    method: str
    summary: str
    description: str
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "method": self.method,
            "summary": self.summary,
            "description": self.description,
            "parameters": self.parameters,
            "request_body": self.request_body,
            "responses": self.responses,
            "tags": self.tags,
            "deprecated": self.deprecated,
        }


@dataclass
class ChangelogEntry:
    """变更日志条目"""
    version: str
    date: datetime
    changes: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "date": self.date.isoformat(),
            "changes": self.changes,
        }


class CodeParser:
    """代码解析器"""
    
    def __init__(self):
        self._cache: Dict[str, ModuleDoc] = {}
    
    def parse_file(self, file_path: str) -> Optional[ModuleDoc]:
        """解析Python文件"""
        if file_path in self._cache:
            return self._cache[file_path]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                source = f.read()
            
            tree = ast.parse(source)
            
            module_name = Path(file_path).stem
            docstring = ast.get_docstring(tree) or ""
            
            module_doc = ModuleDoc(
                name=module_name,
                docstring=docstring,
                path=file_path,
            )
            
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.ClassDef):
                    class_doc = self._parse_class(node, source)
                    module_doc.classes.append(class_doc)
                elif isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                    func_doc = self._parse_function(node, source)
                    module_doc.functions.append(func_doc)
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        module_doc.imports.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    module_doc.imports.append(f"{node.module}")
            
            self._cache[file_path] = module_doc
            return module_doc
            
        except Exception as e:
            logger.error(f"Failed to parse file {file_path}: {e}")
            return None
    
    def _parse_class(self, node: ast.ClassDef, source: str) -> ClassDoc:
        """解析类定义"""
        docstring = ast.get_docstring(node) or ""
        
        bases = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                bases.append(base.id)
            elif isinstance(base, ast.Attribute):
                bases.append(f"{ast.unparse(base.value)}.{base.attr}")
        
        class_doc = ClassDoc(
            name=node.name,
            docstring=docstring,
            bases=bases,
            is_abstract=self._is_abstract_class(node),
            is_dataclass=self._is_dataclass(node),
        )
        
        # 解析类方法和属性
        for item in node.body:
            if isinstance(item, ast.FunctionDef) or isinstance(item, ast.AsyncFunctionDef):
                func_doc = self._parse_function(item, source)
                class_doc.methods.append(func_doc)
            elif isinstance(item, ast.AnnAssign):
                attr_doc = self._parse_attribute(item, source)
                if attr_doc:
                    class_doc.attributes.append(attr_doc)
        
        return class_doc
    
    def _parse_function(self, node: Union[ast.FunctionDef, ast.AsyncFunctionDef], source: str) -> FunctionDoc:
        """解析函数定义"""
        docstring = ast.get_docstring(node) or ""
        
        # 获取签名
        signature = self._get_function_signature(node)
        
        # 解析参数
        parameters = []
        for arg in node.args.args:
            param_info = {
                "name": arg.arg,
                "type": ast.unparse(arg.annotation) if arg.annotation else None,
                "default": None,
            }
            parameters.append(param_info)
        
        # 解析默认值
        defaults_start = len(node.args.args) - len(node.args.defaults)
        for i, default in enumerate(node.args.defaults):
            parameters[defaults_start + i]["default"] = ast.unparse(default)
        
        # 解析返回类型
        returns = None
        if node.returns:
            returns = {"type": ast.unparse(node.returns)}
        
        # 解析装饰器
        decorators = []
        for decorator in node.decorator_list:
            decorators.append(ast.unparse(decorator))
        
        return FunctionDoc(
            name=node.name,
            signature=signature,
            docstring=docstring,
            parameters=parameters,
            returns=returns,
            decorators=decorators,
        )
    
    def _parse_attribute(self, node: ast.AnnAssign, source: str) -> Optional[Dict[str, Any]]:
        """解析属性"""
        if isinstance(node.target, ast.Name):
            return {
                "name": node.target.id,
                "type": ast.unparse(node.annotation) if node.annotation else None,
                "default": ast.unparse(node.value) if node.value else None,
            }
        return None
    
    def _get_function_signature(self, node: Union[ast.FunctionDef, ast.AsyncFunctionDef]) -> str:
        """获取函数签名"""
        args = []
        
        # 普通参数
        for arg in node.args.args:
            arg_str = arg.arg
            if arg.annotation:
                arg_str += f": {ast.unparse(arg.annotation)}"
            args.append(arg_str)
        
        # 默认参数
        defaults_start = len(node.args.args) - len(node.args.defaults)
        for i, default in enumerate(node.args.defaults):
            args[defaults_start + i] += f" = {ast.unparse(default)}"
        
        # 可变参数
        if node.args.vararg:
            args.append(f"*{node.args.vararg.arg}")
        
        # 关键字参数
        if node.args.kwarg:
            args.append(f"**{node.args.kwarg.arg}")
        
        sig = f"({', '.join(args)})"
        
        if node.returns:
            sig += f" -> {ast.unparse(node.returns)}"
        
        return sig
    
    def _is_abstract_class(self, node: ast.ClassDef) -> bool:
        """检查是否为抽象类"""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == "abstractmethod":
                return True
            if isinstance(decorator, ast.Attribute) and decorator.attr == "abstractmethod":
                return True
        
        for base in node.bases:
            if isinstance(base, ast.Name) and base.id == "ABC":
                return True
        
        return False
    
    def _is_dataclass(self, node: ast.ClassDef) -> bool:
        """检查是否为数据类"""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == "dataclass":
                return True
            if isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name) and decorator.func.id == "dataclass":
                    return True
        return False
    
    def parse_directory(self, directory: str, exclude_patterns: Optional[List[str]] = None) -> List[ModuleDoc]:
        """解析目录中的所有Python文件"""
        modules = []
        exclude_patterns = exclude_patterns or ["__pycache__", "test_", "_test.py"]
        
        for root, dirs, files in os.walk(directory):
            # 过滤目录
            dirs[:] = [d for d in dirs if not any(p in d for p in exclude_patterns)]
            
            for file in files:
                if file.endswith('.py') and not any(p in file for p in exclude_patterns):
                    file_path = os.path.join(root, file)
                    module_doc = self.parse_file(file_path)
                    if module_doc:
                        modules.append(module_doc)
        
        return modules


class APIDocExtractor:
    """API文档提取器"""
    
    def __init__(self):
        self._endpoints: List[APIEndpointDoc] = []
    
    def extract_from_fastapi(self, app: Any) -> List[APIEndpointDoc]:
        """从FastAPI应用提取API文档"""
        endpoints = []
        
        try:
            for route in app.routes:
                if hasattr(route, 'methods') and hasattr(route, 'path'):
                    for method in route.methods:
                        if method == "HEAD":
                            continue
                        
                        endpoint = APIEndpointDoc(
                            path=route.path,
                            method=method,
                            summary=getattr(route, 'summary', ''),
                            description=getattr(route, 'description', ''),
                            tags=list(getattr(route, 'tags', [])),
                            deprecated=getattr(route, 'deprecated', False),
                        )
                        
                        # 提取参数
                        if hasattr(route, 'dependant'):
                            for param in route.dependant.query_params:
                                endpoint.parameters.append({
                                    "name": param.name,
                                    "in": "query",
                                    "type": str(param.type_),
                                    "required": param.required,
                                })
                        
                        endpoints.append(endpoint)
        except Exception as e:
            logger.error(f"Failed to extract FastAPI docs: {e}")
        
        return endpoints
    
    def extract_from_flask(self, app: Any) -> List[APIEndpointDoc]:
        """从Flask应用提取API文档"""
        endpoints = []
        
        try:
            for rule in app.url_map.iter_rules():
                # 跳过静态文件
                if rule.endpoint == 'static':
                    continue
                
                for method in rule.methods:
                    if method in ['OPTIONS', 'HEAD']:
                        continue
                    
                    endpoint = APIEndpointDoc(
                        path=str(rule.rule),
                        method=method,
                        summary=f"{method} {rule.endpoint}",
                        description="",
                        tags=[rule.endpoint.split('.')[0]] if '.' in rule.endpoint else ['default'],
                    )
                    
                    # 提取URL参数
                    for arg in rule.arguments:
                        endpoint.parameters.append({
                            "name": arg,
                            "in": "path",
                            "type": "string",
                            "required": True,
                        })
                    
                    endpoints.append(endpoint)
        except Exception as e:
            logger.error(f"Failed to extract Flask docs: {e}")
        
        return endpoints


class DiagramGenerator:
    """图表生成器"""
    
    def __init__(self):
        self._generators: Dict[DiagramType, Callable] = {
            DiagramType.CLASS: self._generate_class_diagram,
            DiagramType.ARCHITECTURE: self._generate_architecture_diagram,
        }
    
    def generate(self, diagram_type: DiagramType, data: Any, output_path: str) -> bool:
        """生成图表"""
        generator = self._generators.get(diagram_type)
        if not generator:
            logger.error(f"Unsupported diagram type: {diagram_type}")
            return False
        
        try:
            content = generator(data)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception as e:
            logger.error(f"Failed to generate diagram: {e}")
            return False
    
    def _generate_class_diagram(self, modules: List[ModuleDoc]) -> str:
        """生成类图 (Mermaid格式)"""
        lines = ["classDiagram"]
        
        for module in modules:
            for cls in module.classes:
                # 类定义
                lines.append(f"    class {cls.name}")
                
                # 属性
                for attr in cls.attributes:
                    visibility = "-" if attr["name"].startswith("_") else "+"
                    type_str = f"{attr['type']}" if attr["type"] else ""
                    lines.append(f"    {cls.name} : {visibility}{type_str} {attr['name']}")
                
                # 方法
                for method in cls.methods:
                    visibility = "-" if method.name.startswith("_") else "+"
                    lines.append(f"    {cls.name} : {visibility}{method.signature}")
                
                # 继承关系
                for base in cls.bases:
                    if base not in ['object', 'ABC']:
                        lines.append(f"    {base} <|-- {cls.name}")
        
        return "\n".join(lines)
    
    def _generate_architecture_diagram(self, components: List[Dict[str, Any]]) -> str:
        """生成架构图 (Mermaid格式)"""
        lines = ["graph TB"]
        
        # 添加节点
        for comp in components:
            comp_id = comp.get('id', 'unknown')
            comp_name = comp.get('name', comp_id)
            comp_type = comp.get('type', 'component')
            
            # 根据类型选择形状
            if comp_type == 'database':
                lines.append(f"    {comp_id}[({comp_name})]")
            elif comp_type == 'service':
                lines.append(f"    {comp_id}(({comp_name}))")
            else:
                lines.append(f"    {comp_id}[{comp_name}]")
        
        # 添加连接
        for comp in components:
            comp_id = comp.get('id', 'unknown')
            for dep in comp.get('dependencies', []):
                lines.append(f"    {comp_id} --> {dep}")
        
        return "\n".join(lines)


class DocRenderer(ABC):
    """文档渲染器抽象基类"""
    
    @abstractmethod
    def render(self, data: Any, output_path: str) -> bool:
        """渲染文档"""
        pass
    
    @abstractmethod
    def get_extension(self) -> str:
        """获取文件扩展名"""
        pass


class MarkdownRenderer(DocRenderer):
    """Markdown渲染器"""
    
    def get_extension(self) -> str:
        return ".md"
    
    def render(self, data: Any, output_path: str) -> bool:
        """渲染Markdown文档"""
        try:
            if isinstance(data, ModuleDoc):
                content = self._render_module(data)
            elif isinstance(data, List) and data and isinstance(data[0], ModuleDoc):
                content = self._render_modules(data)
            elif isinstance(data, List) and data and isinstance(data[0], APIEndpointDoc):
                content = self._render_api_docs(data)
            else:
                content = str(data)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True
        except Exception as e:
            logger.error(f"Failed to render Markdown: {e}")
            return False
    
    def _render_module(self, module: ModuleDoc) -> str:
        """渲染模块文档"""
        lines = [
            f"# {module.name}",
            "",
            module.docstring,
            "",
        ]
        
        # 类
        if module.classes:
            lines.append("## Classes")
            lines.append("")
            for cls in module.classes:
                lines.extend(self._render_class(cls))
        
        # 函数
        if module.functions:
            lines.append("## Functions")
            lines.append("")
            for func in module.functions:
                lines.extend(self._render_function(func))
        
        return "\n".join(lines)
    
    def _render_modules(self, modules: List[ModuleDoc]) -> str:
        """渲染多个模块"""
        lines = ["# API Documentation", ""]
        
        for module in modules:
            lines.extend(self._render_module(module))
            lines.append("---")
            lines.append("")
        
        return "\n".join(lines)
    
    def _render_class(self, cls: ClassDoc) -> List[str]:
        """渲染类文档"""
        lines = [
            f"### {cls.name}",
            "",
            cls.docstring,
            "",
        ]
        
        if cls.bases:
            lines.append(f"**Bases:** {', '.join(cls.bases)}")
            lines.append("")
        
        if cls.attributes:
            lines.append("**Attributes:**")
            for attr in cls.attributes:
                type_str = f" ({attr['type']})" if attr.get('type') else ""
                lines.append(f"- `{attr['name']}`{type_str}")
            lines.append("")
        
        if cls.methods:
            lines.append("**Methods:**")
            for method in cls.methods:
                if not method.name.startswith('_'):
                    lines.append(f"- `{method.name}{method.signature}`")
            lines.append("")
        
        return lines
    
    def _render_function(self, func: FunctionDoc) -> List[str]:
        """渲染函数文档"""
        lines = [
            f"### {func.name}",
            "",
            f"```python",
            f"def {func.name}{func.signature}",
            f"```",
            "",
        ]
        
        if func.docstring:
            lines.append(func.docstring)
            lines.append("")
        
        if func.parameters:
            lines.append("**Parameters:**")
            for param in func.parameters:
                type_str = f" ({param['type']})" if param.get('type') else ""
                default_str = f" = {param['default']}" if param.get('default') else ""
                lines.append(f"- `{param['name']}`{type_str}{default_str}")
            lines.append("")
        
        if func.returns:
            lines.append(f"**Returns:** `{func.returns['type']}`")
            lines.append("")
        
        return lines
    
    def _render_api_docs(self, endpoints: List[APIEndpointDoc]) -> str:
        """渲染API文档"""
        lines = ["# API Reference", ""]
        
        for endpoint in endpoints:
            lines.extend([
                f"## {endpoint.method} {endpoint.path}",
                "",
                f"**Summary:** {endpoint.summary}",
                "",
            ])
            
            if endpoint.description:
                lines.append(endpoint.description)
                lines.append("")
            
            if endpoint.parameters:
                lines.append("**Parameters:**")
                lines.append("")
                lines.append("| Name | In | Type | Required |")
                lines.append("|------|-----|------|----------|")
                for param in endpoint.parameters:
                    lines.append(f"| {param['name']} | {param['in']} | {param.get('type', 'string')} | {param.get('required', False)} |")
                lines.append("")
            
            if endpoint.responses:
                lines.append("**Responses:**")
                for code, response in endpoint.responses.items():
                    lines.append(f"- `{code}`: {response.get('description', '')}")
                lines.append("")
            
            lines.append("---")
            lines.append("")
        
        return "\n".join(lines)


class HTMLRenderer(DocRenderer):
    """HTML渲染器"""
    
    def get_extension(self) -> str:
        return ".html"
    
    def render(self, data: Any, output_path: str) -> bool:
        """渲染HTML文档"""
        try:
            # 先渲染Markdown
            md_renderer = MarkdownRenderer()
            md_content = ""
            
            if isinstance(data, ModuleDoc):
                md_content = md_renderer._render_module(data)
            elif isinstance(data, List) and data and isinstance(data[0], ModuleDoc):
                md_content = md_renderer._render_modules(data)
            
            # 转换为HTML (简化实现)
            html_content = self._markdown_to_html(md_content)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            return True
        except Exception as e:
            logger.error(f"Failed to render HTML: {e}")
            return False
    
    def _markdown_to_html(self, markdown: str) -> str:
        """Markdown转HTML (简化实现)"""
        html = markdown
        
        # 标题
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        
        # 代码块
        html = re.sub(r'```python\n(.+?)```', r'<pre><code class="python">\1</code></pre>', html, flags=re.DOTALL)
        html = re.sub(r'`(.+?)`', r'<code>\1</code>', html)
        
        # 段落
        html = re.sub(r'\n\n', '</p><p>', html)
        html = '<p>' + html + '</p>'
        
        # 包装HTML文档
        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>API Documentation</title>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }}
        h1, h2, h3 {{ color: #333; }}
        code {{ background: #f4f4f4; padding: 2px 5px; border-radius: 3px; }}
        pre {{ background: #f4f4f4; padding: 15px; overflow-x: auto; }}
    </style>
</head>
<body>
    {html}
</body>
</html>"""


class ChangelogManager:
    """变更日志管理器"""
    
    def __init__(self, changelog_path: str = "CHANGELOG.md"):
        self.changelog_path = changelog_path
        self._entries: List[ChangelogEntry] = []
    
    def add_entry(
        self,
        version: str,
        changes: List[Dict[str, Any]],
        date: Optional[datetime] = None
    ) -> ChangelogEntry:
        """添加变更条目"""
        entry = ChangelogEntry(
            version=version,
            date=date or datetime.utcnow(),
            changes=changes,
        )
        self._entries.append(entry)
        return entry
    
    def generate(self, format_type: DocFormat = DocFormat.MARKDOWN) -> str:
        """生成变更日志"""
        if format_type == DocFormat.MARKDOWN:
            return self._generate_markdown()
        elif format_type == DocFormat.JSON:
            return json.dumps([e.to_dict() for e in self._entries], indent=2)
        else:
            return ""
    
    def _generate_markdown(self) -> str:
        """生成Markdown格式变更日志"""
        lines = ["# Changelog", ""]
        
        for entry in sorted(self._entries, key=lambda e: e.version, reverse=True):
            date_str = entry.date.strftime("%Y-%m-%d")
            lines.append(f"## [{entry.version}] - {date_str}")
            lines.append("")
            
            for change in entry.changes:
                change_type = change.get('type', 'changed')
                description = change.get('description', '')
                lines.append(f"- **{change_type.capitalize()}:** {description}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def save(self, output_path: Optional[str] = None) -> bool:
        """保存变更日志"""
        try:
            path = output_path or self.changelog_path
            content = self.generate(DocFormat.MARKDOWN)
            
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save changelog: {e}")
            return False


class DocumentationGenerator:
    """文档生成器主类"""
    
    def __init__(self, output_dir: str = "./docs"):
        self.output_dir = output_dir
        self.parser = CodeParser()
        self.api_extractor = APIDocExtractor()
        self.diagram_generator = DiagramGenerator()
        self.changelog_manager = ChangelogManager()
        
        self._renderers: Dict[DocFormat, DocRenderer] = {
            DocFormat.MARKDOWN: MarkdownRenderer(),
            DocFormat.HTML: HTMLRenderer(),
        }
        
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_module_docs(
        self,
        source_dir: str,
        output_format: DocFormat = DocFormat.MARKDOWN,
        exclude_patterns: Optional[List[str]] = None
    ) -> List[str]:
        """生成模块文档"""
        modules = self.parser.parse_directory(source_dir, exclude_patterns)
        
        renderer = self._renderers.get(output_format)
        if not renderer:
            logger.error(f"Unsupported format: {output_format}")
            return []
        
        generated_files = []
        
        # 生成单个模块文档
        for module in modules:
            output_path = os.path.join(
                self.output_dir,
                f"{module.name}{renderer.get_extension()}"
            )
            
            if renderer.render(module, output_path):
                generated_files.append(output_path)
        
        # 生成汇总文档
        index_path = os.path.join(
            self.output_dir,
            f"index{renderer.get_extension()}"
        )
        
        if renderer.render(modules, index_path):
            generated_files.append(index_path)
        
        return generated_files
    
    def generate_api_docs(
        self,
        app: Any,
        framework: str = "fastapi",
        output_format: DocFormat = DocFormat.MARKDOWN
    ) -> Optional[str]:
        """生成API文档"""
        if framework == "fastapi":
            endpoints = self.api_extractor.extract_from_fastapi(app)
        elif framework == "flask":
            endpoints = self.api_extractor.extract_from_flask(app)
        else:
            logger.error(f"Unsupported framework: {framework}")
            return None
        
        renderer = self._renderers.get(output_format)
        if not renderer:
            return None
        
        output_path = os.path.join(
            self.output_dir,
            f"api_reference{renderer.get_extension()}"
        )
        
        if renderer.render(endpoints, output_path):
            return output_path
        
        return None
    
    def generate_diagram(
        self,
        diagram_type: DiagramType,
        data: Any,
        output_name: str
    ) -> Optional[str]:
        """生成图表"""
        output_path = os.path.join(self.output_dir, f"{output_name}.mmd")
        
        if self.diagram_generator.generate(diagram_type, data, output_path):
            return output_path
        
        return None
    
    def generate_changelog(self, output_path: Optional[str] = None) -> Optional[str]:
        """生成变更日志"""
        path = output_path or os.path.join(self.output_dir, "CHANGELOG.md")
        
        if self.changelog_manager.save(path):
            return path
        
        return None
    
    def get_doc_stats(self) -> Dict[str, Any]:
        """获取文档统计"""
        return {
            "output_directory": self.output_dir,
            "cached_modules": len(self.parser._cache),
            "changelog_entries": len(self.changelog_manager._entries),
        }


# 全局文档生成器实例
_doc_generator: Optional[DocumentationGenerator] = None


def get_documentation_generator(output_dir: str = "./docs") -> DocumentationGenerator:
    """获取全局文档生成器实例"""
    global _doc_generator
    if _doc_generator is None:
        _doc_generator = DocumentationGenerator(output_dir)
    return _doc_generator


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    print("=== Documentation Generator Demo ===\n")
    
    # 创建文档生成器
    generator = get_documentation_generator("./generated_docs")
    
    # 示例1: 解析当前文件
    print("1. Code Parsing:")
    
    current_file = __file__
    module_doc = generator.parser.parse_file(current_file)
    
    if module_doc:
        print(f"   Parsed module: {module_doc.name}")
        print(f"   Classes: {len(module_doc.classes)}")
        print(f"   Functions: {len(module_doc.functions)}")
        
        if module_doc.classes:
            print(f"   First class: {module_doc.classes[0].name}")
    
    # 示例2: 生成Markdown文档
    print("\n2. Markdown Generation:")
    
    if module_doc:
        output_path = "./generated_docs/test_doc.md"
        renderer = MarkdownRenderer()
        if renderer.render(module_doc, output_path):
            print(f"   Generated: {output_path}")
    
    # 示例3: 生成类图
    print("\n3. Diagram Generation:")
    
    components = [
        {"id": "api", "name": "API Gateway", "type": "service", "dependencies": ["auth", "memory"]},
        {"id": "auth", "name": "Auth Service", "type": "service", "dependencies": ["db"]},
        {"id": "memory", "name": "Memory System", "type": "service", "dependencies": ["db", "cache"]},
        {"id": "db", "name": "Database", "type": "database", "dependencies": []},
        {"id": "cache", "name": "Cache", "type": "database", "dependencies": []},
    ]
    
    diagram_path = generator.generate_diagram(DiagramType.ARCHITECTURE, components, "architecture")
    if diagram_path:
        print(f"   Generated architecture diagram: {diagram_path}")
    
    # 示例4: 变更日志
    print("\n4. Changelog Management:")
    
    generator.changelog_manager.add_entry(
        version="1.0.0",
        changes=[
            {"type": "added", "description": "Initial release"},
            {"type": "added", "description": "Core memory functionality"},
        ]
    )
    
    generator.changelog_manager.add_entry(
        version="1.1.0",
        changes=[
            {"type": "added", "description": "New retrieval strategies"},
            {"type": "fixed", "description": "Bug in consolidation"},
        ]
    )
    
    changelog_path = generator.generate_changelog()
    if changelog_path:
        print(f"   Generated changelog: {changelog_path}")
    
    # 示例5: 统计信息
    print("\n5. Documentation Statistics:")
    
    stats = generator.get_doc_stats()
    print(f"   Output directory: {stats['output_directory']}")
    print(f"   Cached modules: {stats['cached_modules']}")
    print(f"   Changelog entries: {stats['changelog_entries']}")
    
    print("\n=== Documentation Generator Demo Completed ===")

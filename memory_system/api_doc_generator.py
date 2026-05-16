"""API文档生成器 - 自动生成API文档和示例"""

import inspect
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class APIEndpoint:
    """API端点信息"""
    name: str
    method: str
    path: str
    description: str
    parameters: List[Dict[str, Any]]
    returns: Dict[str, Any]
    examples: List[str]


class APIDocGenerator:
    """从代码生成API文档"""

    def __init__(self, facade_class):
        self.facade = facade_class
        self.endpoints: List[APIEndpoint] = []

    def scan_methods(self) -> None:
        """扫描Facade类的方法"""
        for name, method in inspect.getmembers(self.facade, predicate=inspect.isfunction):
            if not name.startswith('_'):
                sig = inspect.signature(method)
                doc = inspect.getdoc(method) or ""

                endpoint = APIEndpoint(
                    name=name,
                    method="POST",  # Default
                    path=f"/api/v1/{name}",
                    description=doc.split('\n')[0] if doc else "",
                    parameters=[
                        {
                            "name": param.name,
                            "type": str(param.annotation) if param.annotation != inspect.Parameter.empty else "Any",
                            "required": param.default == inspect.Parameter.empty,
                            "default": param.default if param.default != inspect.Parameter.empty else None
                        }
                        for param in sig.parameters.values()
                        if param.name != 'self'
                    ],
                    returns={
                        "type": str(sig.return_annotation) if sig.return_annotation != inspect.Signature.empty else "Any",
                        "description": ""
                    },
                    examples=self._generate_examples(name, sig)
                )
                self.endpoints.append(endpoint)

    def _generate_examples(self, method_name: str, signature) -> List[str]:
        """生成使用示例"""
        examples = {
            "remember": [
                "# 存储一条记忆\nmemory.remember('用户喜欢Python', importance='high')",
                "# 批量存储\nmemory.remember_batch(['内容1', '内容2'], importance='medium')"
            ],
            "recall": [
                "# 检索记忆\nresults = memory.recall('Python')",
                "# 关联感知检索\nresults = memory.recall_with_associations('Python', association_depth=1)"
            ],
            "forget": [
                "# 遗忘单条记忆\nmemory.forget('memory_id')",
                "# 基于查询遗忘\nmemory.forget_by_query('Python', min_similarity=0.8)"
            ],
            "consolidate": [
                "# 合并相似记忆\nmemory.consolidate(similarity_threshold=0.9)",
                "# 时间窗口合并\nmemory.consolidate_by_time(window_hours=24)"
            ],
            "export": [
                "# 导出记忆到文件\nmemory.export_to_file('memories.json')",
                "# 导出特定租户的记忆\nmemory.export_to_file('tenant_memories.json', tenant_id='tenant_1')"
            ],
            "import_memories": [
                "# 从文件导入记忆\nmemory.import_from_file('memories.json')"
            ]
        }
        return examples.get(method_name, [])

    def generate_markdown(self) -> str:
        """生成Markdown格式的API文档"""
        lines = ["# Agent Memory System API 文档\n"]
        lines.append("## 目录\n")

        # 生成目录
        for ep in self.endpoints:
            lines.append(f"- [{ep.name}](#{ep.name.lower()})\n")
        lines.append("\n---\n")

        # 生成详细文档
        for ep in self.endpoints:
            lines.append(f"\n## {ep.name}\n")
            lines.append(f"**路径**: `{ep.path}`\n")
            lines.append(f"**方法**: `{ep.method}`\n")
            lines.append(f"**描述**: {ep.description}\n\n")

            if ep.parameters:
                lines.append("### 参数\n")
                lines.append("| 名称 | 类型 | 必需 | 默认值 | 描述 |")
                lines.append("|------|------|------|--------|------|")
                for p in ep.parameters:
                    req = "是" if p["required"] else "否"
                    default = p["default"] if p["default"] is not None else "-"
                    lines.append(f"| {p['name']} | {p['type']} | {req} | {default} | |")
                lines.append("")

            lines.append("### 返回值\n")
            lines.append(f"**类型**: `{ep.returns['type']}`\n")
            if ep.returns['description']:
                lines.append(f"**描述**: {ep.returns['description']}\n")
            lines.append("")

            if ep.examples:
                lines.append("### 示例\n")
                for ex in ep.examples:
                    lines.append(f"```python\n{ex}\n```\n")

            lines.append("---\n")

        return "\n".join(lines)

    def generate_html(self) -> str:
        """生成HTML格式的API文档"""
        md_content = self.generate_markdown()
        # 简单的Markdown到HTML转换
        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Agent Memory System API 文档</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        h1 {{ color: #333; border-bottom: 2px solid #007acc; padding-bottom: 10px; }}
        h2 {{ color: #007acc; margin-top: 30px; }}
        h3 {{ color: #555; margin-top: 20px; }}
        code {{
            background: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Consolas', 'Monaco', monospace;
        }}
        pre {{
            background: #f8f8f8;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 15px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        th {{ background: #007acc; color: white; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        hr {{ border: none; border-top: 1px solid #eee; margin: 30px 0; }}
    </style>
</head>
<body>
"""
        # 简单的Markdown到HTML转换
        import re

        # 转换标题
        content = md_content
        content = re.sub(r'^# (.+)$', r'<h1>\1</h1>', content, flags=re.MULTILINE)
        content = re.sub(r'^## (.+)$', r'<h2>\1</h2>', content, flags=re.MULTILINE)
        content = re.sub(r'^### (.+)$', r'<h3>\1</h3>', content, flags=re.MULTILINE)

        # 转换代码块
        content = re.sub(r'```python\n(.+?)\n```', r'<pre><code>\1</code></pre>', content, flags=re.DOTALL)

        # 转换行内代码
        content = re.sub(r'`([^`]+)`', r'<code>\1</code>', content)

        # 转换粗体
        content = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', content)

        # 转换表格
        content = re.sub(r'\|(.+)\|', lambda m: '<tr>' + ''.join(f'<td>{c.strip()}</td>' for c in m.group(1).split('|')) + '</tr>', content)

        # 转换水平线
        content = re.sub(r'^---$', '<hr>', content, flags=re.MULTILINE)

        # 转换换行
        content = content.replace('\n\n', '</p><p>')

        html += content
        html += "\n</body>\n</html>"

        return html

    def save_documentation(self, filepath: str, format: str = "markdown") -> None:
        """保存文档到文件

        Args:
            filepath: 文件路径
            format: 文档格式，可选 "markdown" 或 "html"
        """
        if format.lower() == "html":
            content = self.generate_html()
        else:
            content = self.generate_markdown()

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

    def get_endpoint_info(self, name: str) -> Optional[APIEndpoint]:
        """获取指定端点的信息"""
        for ep in self.endpoints:
            if ep.name == name:
                return ep
        return None

    def generate_openapi_spec(self, title: str = "Agent Memory System API",
                             version: str = "2.29.0") -> Dict[str, Any]:
        """生成OpenAPI规范"""
        spec = {
            "openapi": "3.0.0",
            "info": {
                "title": title,
                "version": version,
                "description": "Agent Memory System RESTful API"
            },
            "paths": {}
        }

        for ep in self.endpoints:
            spec["paths"][ep.path] = {
                ep.method.lower(): {
                    "summary": ep.description,
                    "operationId": ep.name,
                    "parameters": [
                        {
                            "name": p["name"],
                            "in": "query",
                            "required": p["required"],
                            "schema": {"type": "string"},
                            "description": f"Type: {p['type']}"
                        }
                        for p in ep.parameters
                    ],
                    "responses": {
                        "200": {
                            "description": "成功",
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object"}
                                }
                            }
                        }
                    }
                }
            }

        return spec

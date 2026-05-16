"""类型注解检查工具"""

import inspect
from typing import get_type_hints, Callable, Dict, Any, List, Optional, Set
from dataclasses import dataclass
from enum import Enum


class TypeHintStatus(Enum):
    """类型注解状态"""
    COMPLETE = "complete"
    MISSING_RETURN = "missing_return"
    MISSING_PARAMS = "missing_params"
    USING_ANY = "using_any"
    ERROR = "error"


@dataclass
class TypeHintResult:
    """类型注解检查结果"""
    method_name: str
    status: TypeHintStatus
    message: str
    param_name: Optional[str] = None
    param_type: Optional[str] = None


@dataclass
class TypeHintReport:
    """类型注解检查报告"""
    total_methods: int
    complete_count: int
    missing_return_count: int
    missing_params_count: int
    using_any_count: int
    error_count: int
    coverage_percentage: float
    results: List[TypeHintResult]


def check_type_completeness(obj) -> TypeHintReport:
    """检查对象的类型注解完整性

    Args:
        obj: 要检查的对象(类或模块)

    Returns:
        TypeHintReport: 类型注解检查报告
    """
    results: List[TypeHintResult] = []
    complete_count = 0
    missing_return_count = 0
    missing_params_count = 0
    using_any_count = 0
    error_count = 0
    total_methods = 0

    for name, method in inspect.getmembers(obj, predicate=inspect.isfunction):
        if name.startswith('_'):
            continue

        total_methods += 1

        try:
            hints = get_type_hints(method)
            sig = inspect.signature(method)

            # 检查返回类型
            if 'return' not in hints:
                results.append(TypeHintResult(
                    method_name=name,
                    status=TypeHintStatus.MISSING_RETURN,
                    message=f"方法 '{name}' 缺少返回类型注解"
                ))
                missing_return_count += 1
            elif hints['return'] == Any:
                results.append(TypeHintResult(
                    method_name=name,
                    status=TypeHintStatus.USING_ANY,
                    message=f"方法 '{name}' 返回类型使用 Any",
                    param_name="return"
                ))
                using_any_count += 1
            else:
                complete_count += 1

            # 检查参数
            for param_name, param in sig.parameters.items():
                if param_name == 'self':
                    continue

                if param_name not in hints:
                    results.append(TypeHintResult(
                        method_name=name,
                        status=TypeHintStatus.MISSING_PARAMS,
                        message=f"方法 '{name}' 参数 '{param_name}' 缺少类型注解",
                        param_name=param_name
                    ))
                    missing_params_count += 1
                elif hints[param_name] == Any:
                    results.append(TypeHintResult(
                        method_name=name,
                        status=TypeHintStatus.USING_ANY,
                        message=f"方法 '{name}' 参数 '{param_name}' 使用 Any",
                        param_name=param_name
                    ))
                    using_any_count += 1
                else:
                    complete_count += 1

        except Exception as e:
            results.append(TypeHintResult(
                method_name=name,
                status=TypeHintStatus.ERROR,
                message=f"检查方法 '{name}' 时出错: {e}"
            ))
            error_count += 1

    # 计算覆盖率
    total_annotations = complete_count + missing_return_count + missing_params_count + using_any_count
    coverage_percentage = (complete_count / total_annotations * 100) if total_annotations > 0 else 0

    return TypeHintReport(
        total_methods=total_methods,
        complete_count=complete_count,
        missing_return_count=missing_return_count,
        missing_params_count=missing_params_count,
        using_any_count=using_any_count,
        error_count=error_count,
        coverage_percentage=coverage_percentage,
        results=results
    )


def check_module_type_hints(module) -> Dict[str, TypeHintReport]:
    """检查模块中所有类的类型注解

    Args:
        module: Python模块

    Returns:
        Dict[str, TypeHintReport]: 每个类的类型注解报告
    """
    reports = {}

    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and not name.startswith('_'):
            reports[name] = check_type_completeness(obj)

    return reports


def generate_type_report(module_or_class, module_name: str = "") -> str:
    """生成类型注解报告

    Args:
        module_or_class: 要检查的模块或类
        module_name: 模块名称

    Returns:
        str: Markdown格式的报告
    """
    if inspect.ismodule(module_or_class):
        reports = check_module_type_hints(module_or_class)

        lines = [
            f"# 类型注解覆盖率报告 - {module_name or module_or_class.__name__}",
            "",
            "## 总体情况",
            ""
        ]

        total_complete = sum(r.complete_count for r in reports.values())
        total_missing_return = sum(r.missing_return_count for r in reports.values())
        total_missing_params = sum(r.missing_params_count for r in reports.values())
        total_any = sum(r.using_any_count for r in reports.values())
        total_errors = sum(r.error_count for r in reports.values())
        total_methods = sum(r.total_methods for r in reports.values())

        total_annotations = total_complete + total_missing_return + total_missing_params + total_any
        overall_coverage = (total_complete / total_annotations * 100) if total_annotations > 0 else 0

        lines.extend([
            f"- **总方法数**: {total_methods}",
            f"- **完整类型注解**: {total_complete}",
            f"- **缺失返回类型**: {total_missing_return}",
            f"- **缺失参数类型**: {total_missing_params}",
            f"- **使用 Any**: {total_any}",
            f"- **检查错误**: {total_errors}",
            f"- **总体覆盖率**: {overall_coverage:.1f}%",
            ""
        ])

        for class_name, report in reports.items():
            if report.total_methods == 0:
                continue

            lines.extend([
                f"## 类: {class_name}",
                "",
                f"- 方法数: {report.total_methods}",
                f"- 覆盖率: {report.coverage_percentage:.1f}%",
                ""
            ])

            if report.results:
                lines.append("### 需要改进的方法")
                lines.append("")

                for result in report.results[:20]:  # 限制输出数量
                    status_icon = {
                        TypeHintStatus.MISSING_RETURN: "⚠️",
                        TypeHintStatus.MISSING_PARAMS: "⚠️",
                        TypeHintStatus.USING_ANY: "⚡",
                        TypeHintStatus.ERROR: "❌"
                    }.get(result.status, "")

                    lines.append(f"- {status_icon} {result.message}")

                lines.append("")

    else:
        report = check_type_completeness(module_or_class)

        lines = [
            f"# 类型注解覆盖率报告 - {module_or_class.__name__}",
            "",
            "## 总体情况",
            "",
            f"- **方法数**: {report.total_methods}",
            f"- **完整类型注解**: {report.complete_count}",
            f"- **缺失返回类型**: {report.missing_return_count}",
            f"- **缺失参数类型**: {report.missing_params_count}",
            f"- **使用 Any**: {report.using_any_count}",
            f"- **检查错误**: {report.error_count}",
            f"- **覆盖率**: {report.coverage_percentage:.1f}%",
            ""
        ]

        if report.results:
            lines.append("## 需要改进的方法")
            lines.append("")

            for result in report.results:
                status_icon = {
                    TypeHintStatus.MISSING_RETURN: "⚠️",
                    TypeHintStatus.MISSING_PARAMS: "⚠️",
                    TypeHintStatus.USING_ANY: "⚡",
                    TypeHintStatus.ERROR: "❌"
                }.get(result.status, "")

                lines.append(f"- {status_icon} {result.message}")

            lines.append("")

    return "\n".join(lines)


def generate_type_coverage_badge(coverage: float) -> str:
    """生成类型覆盖率徽章

    Args:
        coverage: 覆盖率百分比

    Returns:
        str: Markdown格式的徽章
    """
    color = "brightgreen" if coverage >= 90 else "green" if coverage >= 80 else "yellow" if coverage >= 60 else "red"
    return f"![Type Coverage](https://img.shields.io/badge/type%20coverage-{coverage:.1f}%25-{color})"


def check_function_signature(func: Callable) -> Dict[str, Any]:
    """检查单个函数的签名和类型注解

    Args:
        func: 要检查的函数

    Returns:
        Dict: 包含签名信息的字典
    """
    sig = inspect.signature(func)

    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}

    params_info = []
    for name, param in sig.parameters.items():
        param_info = {
            "name": name,
            "kind": str(param.kind),
            "has_type_hint": name in hints,
            "type_hint": str(hints.get(name, "Not specified")),
            "has_default": param.default != inspect.Parameter.empty,
            "default_value": param.default if param.default != inspect.Parameter.empty else None
        }
        params_info.append(param_info)

    return {
        "name": func.__name__,
        "parameters": params_info,
        "return_type": str(hints.get('return', "Not specified")),
        "has_return_hint": 'return' in hints,
        "docstring": inspect.getdoc(func) or "No docstring"
    }


class TypeHintChecker:
    """类型注解检查器类"""

    def __init__(self, strict_mode: bool = False):
        """
        Args:
            strict_mode: 严格模式，Any类型也会被视为不完整
        """
        self.strict_mode = strict_mode
        self.reports: Dict[str, TypeHintReport] = {}

    def check_class(self, cls) -> TypeHintReport:
        """检查类"""
        report = check_type_completeness(cls)
        self.reports[cls.__name__] = report
        return report

    def check_module(self, module) -> Dict[str, TypeHintReport]:
        """检查模块"""
        reports = check_module_type_hints(module)
        self.reports.update(reports)
        return reports

    def get_summary(self) -> Dict[str, Any]:
        """获取汇总信息"""
        if not self.reports:
            return {"message": "没有检查报告"}

        total_methods = sum(r.total_methods for r in self.reports.values())
        total_complete = sum(r.complete_count for r in self.reports.values())
        total_missing = sum(r.missing_return_count + r.missing_params_count for r in self.reports.values())
        total_any = sum(r.using_any_count for r in self.reports.values())

        total = total_complete + total_missing + total_any
        coverage = (total_complete / total * 100) if total > 0 else 0

        return {
            "total_classes": len(self.reports),
            "total_methods": total_methods,
            "complete_annotations": total_complete,
            "missing_annotations": total_missing,
            "using_any": total_any,
            "coverage_percentage": coverage,
            "badge": generate_type_coverage_badge(coverage)
        }

    def export_report(self, filepath: str, format: str = "markdown") -> None:
        """导出报告到文件

        Args:
            filepath: 文件路径
            format: 格式 (markdown, json)
        """
        if format == "json":
            import json
            data = {
                name: {
                    "total_methods": r.total_methods,
                    "complete_count": r.complete_count,
                    "missing_return_count": r.missing_return_count,
                    "missing_params_count": r.missing_params_count,
                    "using_any_count": r.using_any_count,
                    "coverage_percentage": r.coverage_percentage,
                    "results": [
                        {
                            "method_name": res.method_name,
                            "status": res.status.value,
                            "message": res.message,
                            "param_name": res.param_name
                        }
                        for res in r.results
                    ]
                }
                for name, r in self.reports.items()
            }
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        else:
            lines = ["# 类型注解检查报告", ""]
            lines.append(generate_type_coverage_badge(self.get_summary()["coverage_percentage"]))
            lines.append("")

            for name, report in self.reports.items():
                lines.append(f"## {name}")
                lines.append(f"- 覆盖率: {report.coverage_percentage:.1f}%")
                lines.append("")

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("\n".join(lines))

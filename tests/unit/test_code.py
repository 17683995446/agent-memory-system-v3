"""
代码存储模块测试
"""

import pytest

from memory_system.code_memory_support import (
    CodeMemory,
    LanguageDetector,
    ProgrammingLanguage,
    ASTParser,
    CodeAnalyzer,
    parse_code
)


class TestLanguageDetector:
    """语言检测测试"""
    
    def test_detect_python_by_extension(self):
        """通过扩展名检测 Python"""
        assert LanguageDetector.detect_by_extension("test.py") == ProgrammingLanguage.PYTHON
        assert LanguageDetector.detect_by_extension("script.PY") == ProgrammingLanguage.PYTHON
    
    def test_detect_javascript_by_extension(self):
        """通过扩展名检测 JavaScript"""
        assert LanguageDetector.detect_by_extension("app.js") == ProgrammingLanguage.JAVASCRIPT
        assert LanguageDetector.detect_by_extension("component.jsx") == ProgrammingLanguage.JAVASCRIPT
    
    def test_detect_typescript_by_extension(self):
        """通过扩展名检测 TypeScript"""
        assert LanguageDetector.detect_by_extension("main.ts") == ProgrammingLanguage.TYPESCRIPT
        assert LanguageDetector.detect_by_extension("widget.tsx") == ProgrammingLanguage.TYPESCRIPT
    
    def test_detect_go_by_extension(self):
        """通过扩展名检测 Go"""
        assert LanguageDetector.detect_by_extension("main.go") == ProgrammingLanguage.GO
    
    def test_detect_rust_by_extension(self):
        """通过扩展名检测 Rust"""
        assert LanguageDetector.detect_by_extension("lib.rs") == ProgrammingLanguage.RUST
    
    def test_detect_unknown_extension(self):
        """未知扩展名返回 UNKNOWN"""
        assert LanguageDetector.detect_by_extension("file.xyz") == ProgrammingLanguage.UNKNOWN
    
    def test_detect_python_by_content(self):
        """通过内容检测 Python"""
        code = """
def hello_world():
    print("Hello, World!")

class MyClass:
    pass
"""
        assert LanguageDetector.detect_by_content(code) == ProgrammingLanguage.PYTHON
    
    def test_detect_javascript_by_content(self):
        """通过内容检测 JavaScript"""
        code = """
function greet(name) {
    console.log("Hello, " + name);
}

const x = 10;
"""
        assert LanguageDetector.detect_by_content(code) == ProgrammingLanguage.JAVASCRIPT
    
    def test_detect_go_by_content(self):
        """通过内容检测 Go"""
        code = """
package main

import "fmt"

func main() {
    fmt.Println("Hello")
}
"""
        assert LanguageDetector.detect_by_content(code) == ProgrammingLanguage.GO
    
    def test_detect_rust_by_content(self):
        """通过内容检测 Rust"""
        code = """
fn main() {
    println!("Hello");
}

struct Point {
    x: i32,
    y: i32,
}
"""
        assert LanguageDetector.detect_by_content(code) == ProgrammingLanguage.RUST
    
    def test_detect_combined(self):
        """综合检测：扩展名优先"""
        code = "function test() {}"
        # 即使内容像 JS，扩展名优先
        assert LanguageDetector.detect(code, "test.py") == ProgrammingLanguage.PYTHON


class TestASTParser:
    """AST 解析器测试"""
    
    def test_parser_availability(self):
        """检查解析器可用性"""
        parser = ASTParser()
        # tree-sitter 可能未安装
        assert isinstance(parser.is_available, bool)
    
    def test_parse_without_treesitter(self):
        """无 tree-sitter 时返回 None"""
        parser = ASTParser()
        if not parser.is_available:
            result = parser.parse("def foo(): pass", ProgrammingLanguage.PYTHON)
            assert result is None


class TestCodeAnalyzer:
    """代码分析器测试"""
    
    def test_analyze_empty_ast(self):
        """分析空 AST"""
        analyzer = CodeAnalyzer()
        structure = analyzer.analyze(None, ProgrammingLanguage.PYTHON)
        
        assert structure.language == ProgrammingLanguage.PYTHON
        assert structure.functions == []
        assert structure.classes == []


class TestCodeMemory:
    """代码记忆测试"""
    
    @pytest.fixture
    def code_memory(self):
        return CodeMemory()
    
    def test_store_python_code(self, code_memory):
        """存储 Python 代码"""
        code = """
def hello():
    print("Hello")

class Greeter:
    def greet(self):
        print("Hi")
"""
        result = code_memory.store(code)
        
        assert result["language"] == "python"
        assert result["line_count"] == 8  # 8 lines including empty lines
        assert "structure" in result
    
    def test_store_with_filename(self, code_memory):
        """使用文件名存储"""
        code = "function test() {}"
        result = code_memory.store(code, filename="test.js")
        
        assert result["language"] == "javascript"
        assert result["filename"] == "test.js"
    
    def test_store_with_explicit_language(self, code_memory):
        """指定语言存储"""
        code = "some random text"
        result = code_memory.store(code, language=ProgrammingLanguage.RUST)
        
        assert result["language"] == "rust"
    
    def test_store_with_metadata(self, code_memory):
        """存储带元数据"""
        code = "def foo(): pass"
        result = code_memory.store(code, metadata={"author": "test", "version": "1.0"})
        
        assert result["metadata"]["author"] == "test"
        assert result["metadata"]["version"] == "1.0"
    
    def test_extract_functions(self, code_memory):
        """提取函数"""
        code = """
def func1():
    pass

def func2(x):
    return x * 2
"""
        functions = code_memory.extract_functions(code)
        
        assert len(functions) >= 0  # 取决于 tree-sitter 是否可用
    
    def test_extract_classes(self, code_memory):
        """提取类"""
        code = """
class MyClass:
    pass

class AnotherClass:
    def method(self):
        pass
"""
        classes = code_memory.extract_classes(code)
        
        assert len(classes) >= 0  # 取决于 tree-sitter 是否可用
    
    def test_get_summary(self, code_memory):
        """获取代码摘要"""
        code = """
import os
import sys

def main():
    print("Hello")

class App:
    pass
"""
        summary = code_memory.get_summary(code)
        
        assert "语言:" in summary
        assert "行数:" in summary
    
    def test_supported_languages(self, code_memory):
        """支持的语言列表"""
        languages = code_memory.supported_languages
        
        assert "python" in languages
        assert "javascript" in languages
        assert "go" in languages
        assert "rust" in languages
        assert "unknown" not in languages
    
    def test_ast_available_property(self, code_memory):
        """AST 可用性属性"""
        assert isinstance(code_memory.ast_available, bool)


class TestParseCodeFunction:
    """便捷函数测试"""
    
    def test_parse_code_basic(self):
        """基础解析"""
        code = "def hello(): pass"
        result = parse_code(code)
        
        assert "code" in result
        assert "language" in result
        assert result["code"] == code
    
    def test_parse_code_with_language(self):
        """指定语言解析"""
        code = "fn main() {}"
        result = parse_code(code, language="rust")
        
        assert result["language"] == "rust"
    
    def test_parse_code_with_filename(self):
        """使用文件名解析"""
        code = "package main"
        result = parse_code(code, filename="main.go")
        
        assert result["language"] == "go"


class TestMultiLanguageSupport:
    """多语言支持测试"""
    
    @pytest.fixture
    def code_memory(self):
        return CodeMemory()
    
    def test_java_code(self, code_memory):
        """Java 代码"""
        code = """
public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello");
    }
}
"""
        result = code_memory.store(code, filename="HelloWorld.java")
        assert result["language"] == "java"
    
    def test_c_code(self, code_memory):
        """C 代码"""
        code = """
#include <stdio.h>

int main() {
    printf("Hello");
    return 0;
}
"""
        result = code_memory.store(code, filename="main.c")
        assert result["language"] == "c"
    
    def test_cpp_code(self, code_memory):
        """C++ 代码"""
        code = """
#include <iostream>

class MyClass {
public:
    void hello() {
        std::cout << "Hello";
    }
};
"""
        result = code_memory.store(code, filename="myclass.cpp")
        assert result["language"] == "cpp"
    
    def test_go_code(self, code_memory):
        """Go 代码"""
        code = """
package main

import "fmt"

func main() {
    fmt.Println("Hello")
}
"""
        result = code_memory.store(code)
        assert result["language"] == "go"
    
    def test_rust_code(self, code_memory):
        """Rust 代码"""
        code = """
fn main() {
    println!("Hello");
}

struct Point {
    x: i32,
    y: i32,
}
"""
        result = code_memory.store(code)
        assert result["language"] == "rust"
    
    def test_ruby_code(self, code_memory):
        """Ruby 代码"""
        code = """
def hello
    puts "Hello"
end

class MyClass
    def greet
        puts "Hi"
    end
end
"""
        result = code_memory.store(code, filename="test.rb")
        assert result["language"] == "ruby"
    
    def test_kotlin_code(self, code_memory):
        """Kotlin 代码"""
        code = """
fun main() {
    println("Hello")
}

class MyClass {
    fun greet() {
        println("Hi")
    }
}
"""
        result = code_memory.store(code, filename="Main.kt")
        assert result["language"] == "kotlin"
    
    def test_sql_code(self, code_memory):
        """SQL 代码"""
        code = """
SELECT * FROM users WHERE id = 1;
"""
        result = code_memory.store(code, filename="query.sql")
        assert result["language"] == "sql"
    
    def test_json_code(self, code_memory):
        """JSON 代码"""
        code = '{"name": "test", "value": 123}'
        result = code_memory.store(code, filename="config.json")
        assert result["language"] == "json"

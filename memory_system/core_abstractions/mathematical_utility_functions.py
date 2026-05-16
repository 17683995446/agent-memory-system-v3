"""
统一工具函数
消除各模块中的重复实现
"""

import math
import re
import unicodedata
from typing import Dict, List, Set, Tuple


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    计算两个向量的余弦相似度
    统一实现，消除 memory_storage / sentence_transformer_embedding / bge_embedding / openai_embedding 中的重复代码
    """
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot_product / (norm1 * norm2)


def exp(x: float) -> float:
    """纯 Python 实现 exp，替代 numpy.exp"""
    return math.exp(x)


def dot(vec1: List[float], vec2: List[float]) -> float:
    """纯 Python 实现向量点积，替代 numpy.dot"""
    return sum(a * b for a, b in zip(vec1, vec2))


# CJK 字符范围
_CJK_RANGES = (
    '\u4e00-\u9fff'       # CJK Unified Ideographs (中文)
    '\u3400-\u4dbf'       # CJK Unified Ideographs Extension A
    '\u3000-\u303f'       # CJK Symbols and Punctuation
    '\uff00-\uffef'       # Halfwidth and Fullwidth Forms
    '\u3040-\u309f'       # Hiragana (日文平假名)
    '\u30a0-\u30ff'       # Katakana (日文片假名)
    '\uac00-\ud7af'       # Hangul Syllables (韩文)
)
_CJK_PATTERN = re.compile(f'[{"".join(_CJK_RANGES)}]')


def is_cjk_text(text: str) -> bool:
    """判断文本是否包含 CJK 字符（中日韩）"""
    return bool(_CJK_PATTERN.search(text))


def tokenize(text: str) -> List[str]:
    """
    智能分词函数，支持中文和英文混合文本

    策略:
        - 纯英文: 按空格分词 + 转小写
        - 含中文: 使用 bigram（二元组）分词 + 英文按空格分词
        - 自动去除标点符号和空白

    Args:
        text: 输入文本

    Returns:
        分词结果列表

    Examples:
        >>> tokenize("hello world")
        ['hello', 'world']
        >>> tokenize("用户喜欢编程")
        ['用户', '户喜', '喜欢', '欢编', '编程']
        >>> tokenize("用户 likes Python")
        ['用户', '户喜', '喜欢', 'likes', 'python']
    """
    if not text:
        return []

    text = text.lower().strip()

    if not is_cjk_text(text):
        # 纯英文/拉丁文本：按空格分词
        return [word for word in text.split() if word.strip()]

    # 含 CJK 字符的文本：使用 bigram + 英文分词
    tokens: List[str] = []

    # 提取英文单词（连续的拉丁字母和数字）
    latin_words = re.findall(r'[a-z0-9]+', text)
    tokens.extend(latin_words)

    # 提取连续 CJK 字符序列，使用 bigram 分词
    # split 按非 CJK 字符分割，保留连续 CJK 片段
    cjk_segments = re.split(r'[^' + "".join(_CJK_RANGES) + r']+', text)
    for segment in cjk_segments:
        segment = segment.strip()
        if not segment:
            continue
        if len(segment) == 1:
            tokens.append(segment)
        else:
            for i in range(len(segment) - 1):
                tokens.append(segment[i:i + 2])

    return tokens


def bm25_score(
    term_freq: int,
    doc_length: int,
    avg_doc_length: float,
    doc_count: int,
    doc_freq: int,
    k1: float = 1.5,
    b: float = 0.75
) -> float:
    """
    计算 BM25 评分
    
    Args:
        term_freq: 词项在文档中的出现频率
        doc_length: 文档长度（词项数）
        avg_doc_length: 平均文档长度
        doc_count: 文档总数
        doc_freq: 包含该词项的文档数
        k1: 词频饱和参数 (默认 1.5)
        b: 文档长度归一化参数 (默认 0.75)
        
    Returns:
        BM25 评分
    """
    if doc_count == 0 or doc_freq == 0:
        return 0.0
    
    idf = math.log((doc_count - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0)
    tf_norm = (term_freq * (k1 + 1)) / (
        term_freq + k1 * (1 - b + b * doc_length / max(avg_doc_length, 1e-10))
    )
    return idf * tf_norm


def reciprocal_rank_fusion(
    result_lists: List[List[Tuple[str, float]]],
    k: int = 60
) -> List[Tuple[str, float]]:
    """
    Reciprocal Rank Fusion (RRF) 算法
    
    将多路检索结果融合为统一排序。
    
    Args:
        result_lists: 多路检索结果列表，每路为 [(memory_id, score), ...]
        k: RRF 常数 (默认 60)
        
    Returns:
        融合后的 [(memory_id, rrf_score), ...] 列表，按分数降序
    """
    rrf_scores: Dict[str, float] = {}
    
    for results in result_lists:
        for rank, (memory_id, _score) in enumerate(results, start=1):
            if memory_id not in rrf_scores:
                rrf_scores[memory_id] = 0.0
            rrf_scores[memory_id] += 1.0 / (k + rank)
    
    # 按分数降序排序
    sorted_results = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_results

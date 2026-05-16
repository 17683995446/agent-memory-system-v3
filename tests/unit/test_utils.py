"""
测试 memory_system/core/utils.py 中的 cosine_similarity 函数
"""

import pytest
from memory_system.core_abstractions.mathematical_utility_functions import cosine_similarity


class TestCosineSimilarity:
    """cosine_similarity 单元测试"""

    def test_identical_vectors(self):
        """相同向量返回 1.0"""
        vec = [1.0, 2.0, 3.0, 4.0]
        assert cosine_similarity(vec, vec) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        """正交向量返回 0.0"""
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [0.0, 1.0, 0.0]
        assert cosine_similarity(vec1, vec2) == pytest.approx(0.0)

    def test_opposite_vectors(self):
        """相反向量返回 -1.0"""
        vec1 = [1.0, 2.0, 3.0]
        vec2 = [-1.0, -2.0, -3.0]
        assert cosine_similarity(vec1, vec2) == pytest.approx(-1.0)

    def test_zero_vectors(self):
        """零向量返回 0.0"""
        vec1 = [0.0, 0.0, 0.0]
        vec2 = [1.0, 2.0, 3.0]
        assert cosine_similarity(vec1, vec2) == pytest.approx(0.0)

    def test_empty_vectors(self):
        """空列表返回 0.0"""
        assert cosine_similarity([], [1.0, 2.0]) == pytest.approx(0.0)
        assert cosine_similarity([1.0, 2.0], []) == pytest.approx(0.0)
        assert cosine_similarity([], []) == pytest.approx(0.0)

    def test_unequal_length(self):
        """长度不等返回 0.0"""
        vec1 = [1.0, 2.0]
        vec2 = [1.0, 2.0, 3.0]
        assert cosine_similarity(vec1, vec2) == pytest.approx(0.0)

    def test_known_values(self):
        """用 [1,0,0] 和 [0,1,0] 验证返回 0.0，[1,0,0] 和 [1,0,0] 返回 1.0"""
        # 正交
        assert cosine_similarity([1, 0, 0], [0, 1, 0]) == pytest.approx(0.0)
        # 相同
        assert cosine_similarity([1, 0, 0], [1, 0, 0]) == pytest.approx(1.0)

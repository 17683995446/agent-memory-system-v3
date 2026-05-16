"""
集成测试: 存储 + 嵌入
"""

import pytest

from memory_system.core_abstractions.memory_data_models import MemoryItem, MemoryType, ImportanceLevel


class TestStoreAndSearchWithEmbedding:
    """test_store_and_search_with_embedding: 使用 mock_embedding 存储带向量的记忆，然后搜索验证"""

    @pytest.mark.asyncio
    async def test_store_and_search_with_embedding(
        self, memory_storage, mock_embedding
    ):
        content = "Python是一种流行的编程语言"
        vec = await mock_embedding.embed(content)

        item = MemoryItem(
            content=content,
            memory_type=MemoryType.EPISODIC,
            importance=ImportanceLevel.HIGH,
            embedding=vec,
        )
        memory_id = await memory_storage.add(item)

        # 搜索
        query_vec = await mock_embedding.embed("Python编程")
        results = await memory_storage.search_by_vector(query_vec, top_k=5)

        assert len(results) >= 1
        assert results[0][0].memory_id == memory_id
        # mock embedding 基于文本 hash 生成向量，验证结果存在即可
        assert isinstance(results[0][1], float)


class TestBatchEmbedAndStore:
    """test_batch_embed_and_store: 批量嵌入并存储多条记忆"""

    @pytest.mark.asyncio
    async def test_batch_embed_and_store(self, memory_storage, mock_embedding):
        texts = [
            "机器学习是人工智能的一个分支",
            "深度学习使用神经网络",
            "自然语言处理处理人类语言",
        ]
        vectors = await mock_embedding.embed_batch(texts)
        assert len(vectors) == 3
        assert all(len(v) == mock_embedding.dimension for v in vectors)

        for text, vec in zip(texts, vectors):
            item = MemoryItem(
                content=text,
                memory_type=MemoryType.SEMANTIC,
                embedding=vec,
            )
            await memory_storage.add(item)

        count = await memory_storage.count()
        assert count == 3


class TestSearchRelevanceOrdering:
    """test_search_relevance_ordering: 存储多条相似度不同的记忆，验证搜索结果按相似度排序"""

    @pytest.mark.asyncio
    async def test_search_relevance_ordering(self, memory_storage, mock_embedding):
        # 存储多条不同主题的记忆
        topics = [
            "Python web开发使用Django框架",
            "Java企业级应用使用Spring Boot",
            "Python数据科学使用pandas和numpy",
            "烹饪意大利面需要番茄酱",
            "JavaScript前端开发使用React",
        ]
        for topic in topics:
            vec = await mock_embedding.embed(topic)
            item = MemoryItem(
                content=topic,
                memory_type=MemoryType.EPISODIC,
                embedding=vec,
            )
            await memory_storage.add(item)

        # 搜索 Python 相关
        query_vec = await mock_embedding.embed("Python编程开发")
        results = await memory_storage.search_by_vector(query_vec, top_k=5)

        assert len(results) <= 5
        # 验证分数降序排列
        scores = [score for _, score in results]
        assert scores == sorted(scores, reverse=True)

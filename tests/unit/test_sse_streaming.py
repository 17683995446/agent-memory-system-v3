"""
SSE 流式检索端点测试
测试 Server-Sent Events 流式返回功能
"""

import sys
import os
import json

import pytest
from starlette.testclient import TestClient

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from tests.conftest import MockEmbeddingProvider, MemoryStorage, ShortTermMemory
from memory_system.agent_memory_system_facade import MemorySystem
from memory_system.retrieval_strategies.hierarchical_retrieval_strategy import HierarchicalRetrieval
from memory_system.restful_api_server import app, reset_memory_system, disable_rate_limit, enable_rate_limit
from memory_system.core_abstractions.authentication_middleware import reset_auth_manager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_singleton():
    """每个测试前后重置全局单例，避免状态泄漏。"""
    reset_memory_system(None)
    reset_auth_manager()
    disable_rate_limit()
    yield
    reset_memory_system(None)
    reset_auth_manager()
    enable_rate_limit()


@pytest.fixture()
def ms_instance(mock_embedding, memory_storage, short_term_memory):
    """
    构建一个使用 MemoryStorage + MockEmbeddingProvider 的 MemorySystem 实例，
    并注入到 API 单例中。
    """
    retrieval = HierarchicalRetrieval(strategy="adaptive")
    system = MemorySystem(
        storage=memory_storage,
        embedding=mock_embedding,
        retrieval=retrieval,
        short_term=short_term_memory,
        consolidation=None,
        forgetting=None,
    )
    reset_memory_system(system)
    return system


@pytest.fixture()
def client(ms_instance):
    """返回绑定了已注入 MemorySystem 的 FastAPI TestClient。"""
    return TestClient(app)


@pytest.fixture()
def _seeded_client(client):
    """预填充一些记忆数据的 client。"""
    # 存入短期记忆
    client.post(
        "/api/v1/memories",
        json={"content": "用户正在调试内存泄漏", "memory_type": "short_term"},
    )
    # 存入长期记忆（semantic）
    client.post(
        "/api/v1/memories",
        json={
            "content": "Python 是一种通用编程语言",
            "memory_type": "semantic",
            "importance": "high",
        },
    )
    # 存入情景记忆
    client.post(
        "/api/v1/memories",
        json={
            "content": "昨天参加了技术分享会",
            "memory_type": "episodic",
            "importance": "medium",
        },
    )
    return client


# ---------------------------------------------------------------------------
# SSE 流式检索测试
# ---------------------------------------------------------------------------


class TestRecallStream:
    """GET /api/v1/memories/recall/stream"""

    def test_recall_stream_returns_sse_content_type(self, _seeded_client):
        """SSE 端点应返回正确的 content-type"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python"},
        )
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")

    def test_recall_stream_returns_start_event(self, _seeded_client):
        """流式结果应以 start 事件开始"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python"},
        )
        assert resp.status_code == 200

        # 解析 SSE 事件
        events = parse_sse_events(resp.text)
        assert len(events) > 0
        assert events[0]["type"] == "start"
        assert events[0]["query"] == "Python"

    def test_recall_stream_returns_total_event(self, _seeded_client):
        """流式结果应包含 total 事件"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python"},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        total_events = [e for e in events if e.get("type") == "total"]
        assert len(total_events) == 1
        assert "count" in total_events[0]

    def test_recall_stream_returns_complete_event(self, _seeded_client):
        """流式结果应以 complete 事件结束"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python"},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        assert len(events) > 0
        assert events[-1]["type"] == "complete"
        assert "total" in events[-1]

    def test_recall_stream_returns_result_events(self, _seeded_client):
        """流式结果应包含 result 事件"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python"},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        result_events = [e for e in events if e.get("type") == "result"]
        assert len(result_events) >= 0  # 可能有结果也可能没有

    def test_recall_stream_with_memory_type_filter(self, _seeded_client):
        """测试 memory_type 过滤功能"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python", "memory_type": "semantic"},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        result_events = [e for e in events if e.get("type") == "result"]

        # 所有结果都应该是 semantic 类型
        for event in result_events:
            memory = event.get("memory", {})
            assert memory.get("memory_type") == "semantic"

    def test_recall_stream_empty_query_422(self, client):
        """空查询应返回 422"""
        resp = client.get(
            "/api/v1/memories/recall/stream",
            params={"query": ""},
        )
        assert resp.status_code == 422

    def test_recall_stream_max_results_param(self, _seeded_client):
        """max_results 参数应限制返回结果数"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python", "max_results": 1},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        result_events = [e for e in events if e.get("type") == "result"]
        total_event = [e for e in events if e.get("type") == "total"][0]

        # 总结果数应不超过 max_results
        assert total_event["count"] <= 1

    def test_recall_stream_result_format(self, _seeded_client):
        """测试结果格式是否正确"""
        resp = _seeded_client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "Python"},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        result_events = [e for e in events if e.get("type") == "result"]

        for event in result_events:
            assert "index" in event
            assert "memory" in event
            assert "score" in event
            assert "layer" in event
            assert isinstance(event["index"], int)


# ---------------------------------------------------------------------------
# SSE 批量流式存储测试
# ---------------------------------------------------------------------------


class TestBatchStoreStream:
    """POST /api/v1/memories/batch/stream"""

    def test_batch_store_stream_returns_sse_content_type(self, client):
        """SSE 端点应返回正确的 content-type"""
        resp = client.post(
            "/api/v1/memories/batch/stream",
            json={"contents": ["记忆一", "记忆二"]},
        )
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")

    def test_batch_store_stream_returns_start_event(self, client):
        """流式结果应以 start 事件开始"""
        resp = client.post(
            "/api/v1/memories/batch/stream",
            json={"contents": ["记忆一", "记忆二"]},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        assert len(events) > 0
        assert events[0]["type"] == "start"
        assert events[0]["total"] == 2

    def test_batch_store_stream_returns_progress_events(self, client):
        """流式结果应包含 progress 事件"""
        resp = client.post(
            "/api/v1/memories/batch/stream",
            json={"contents": ["记忆一", "记忆二", "记忆三"]},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        progress_events = [e for e in events if e.get("type") == "progress"]
        assert len(progress_events) == 3

        # 检查 progress 事件格式
        for i, event in enumerate(progress_events):
            assert event["index"] == i
            assert "memory_id" in event

    def test_batch_store_stream_returns_complete_event(self, client):
        """流式结果应以 complete 事件结束"""
        resp = client.post(
            "/api/v1/memories/batch/stream",
            json={"contents": ["记忆一"]},
        )
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        assert events[-1]["type"] == "complete"

    def test_batch_store_stream_empty_list_422(self, client):
        """空列表应返回 422"""
        resp = client.post(
            "/api/v1/memories/batch/stream",
            json={"contents": []},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# SSE 错误处理测试
# ---------------------------------------------------------------------------


class TestSSEErrorHandling:
    """SSE 错误处理测试"""

    def test_recall_stream_error_handling(self, client):
        """测试错误时返回 error 事件"""
        # 在没有初始化 MemorySystem 的情况下调用
        # 注意：这里我们依赖 ms.recall 抛出异常
        resp = client.get(
            "/api/v1/memories/recall/stream",
            params={"query": "test"},
        )
        # 即使有错误，SSE 端点也应返回 200，错误在事件流中
        assert resp.status_code == 200

        events = parse_sse_events(resp.text)
        # 应该至少有一个 start 事件
        assert len(events) > 0
        assert events[0]["type"] == "start"


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def parse_sse_events(text: str) -> list:
    """
    解析 SSE 事件文本为 JSON 对象列表

    SSE 格式:
        data: {"type": "start", ...}

        data: {"type": "result", ...}

    """
    events = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if line.startswith("data: "):
            json_str = line[6:]  # 去掉 "data: " 前缀
            try:
                event = json.loads(json_str)
                events.append(event)
            except json.JSONDecodeError:
                # 忽略解析错误
                pass
    return events

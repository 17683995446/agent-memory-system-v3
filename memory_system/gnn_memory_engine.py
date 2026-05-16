"""
图神经网络记忆关联引擎 (GNN Memory Engine)

基于图神经网络的记忆关联推理系统，支持GCN、GAT、GraphSAGE等多种消息传递机制，
以及时序图神经网络(T-GNN)用于动态记忆图谱建模。

Author: Agent Memory System V3.0
Date: 2026-05-16
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GATConv, SAGEConv, GINConv
from torch_geometric.data import Data, Batch
from torch_geometric.utils import to_dense_adj, dense_to_sparse, add_self_loops
from torch_scatter import scatter_mean, scatter_max, scatter_add
import numpy as np
from typing import Dict, List, Tuple, Optional, Union, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
import pickle
import json
from collections import defaultdict
import warnings

warnings.filterwarnings('ignore')


class GNNType(Enum):
    """支持的GNN类型枚举"""
    GCN = "gcn"
    GAT = "gat"
    GRAPHSAGE = "graphsage"
    GIN = "gin"
    TGNN = "tgnn"


@dataclass
class MemoryNode:
    """记忆节点数据结构"""
    node_id: int
    features: torch.Tensor
    timestamp: float
    node_type: str = "default"
    importance_score: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MemoryEdge:
    """记忆边数据结构"""
    source_id: int
    target_id: int
    edge_type: str = "default"
    weight: float = 1.0
    timestamp: float = 0.0
    features: Optional[torch.Tensor] = None


class TemporalGraphLayer(nn.Module):
    """
    时序图神经网络层
    结合时间信息和图结构进行消息传递
    """

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        num_heads: int = 4,
        dropout: float = 0.1,
        time_encoding_dim: int = 64
    ):
        super().__init__()
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.num_heads = num_heads
        self.time_encoding_dim = time_encoding_dim

        # 时间编码网络
        self.time_encoder = nn.Sequential(
            nn.Linear(1, time_encoding_dim),
            nn.ReLU(),
            nn.Linear(time_encoding_dim, time_encoding_dim),
            nn.ReLU()
        )

        # 图注意力层
        self.gat = GATConv(
            in_channels + time_encoding_dim,
            out_channels // num_heads,
            heads=num_heads,
            dropout=dropout,
            concat=True
        )

        # 时间门控机制
        self.time_gate = nn.Sequential(
            nn.Linear(out_channels + time_encoding_dim, out_channels),
            nn.Sigmoid()
        )

        # 层归一化
        self.norm = nn.LayerNorm(out_channels)
        self.dropout = nn.Dropout(dropout)

    def forward(
        self,
        x: torch.Tensor,
        edge_index: torch.Tensor,
        timestamps: torch.Tensor,
        reference_time: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        前向传播

        Args:
            x: 节点特征 [num_nodes, in_channels]
            edge_index: 边索引 [2, num_edges]
            timestamps: 节点时间戳 [num_nodes]
            reference_time: 参考时间（默认为当前时间）

        Returns:
            更新后的节点特征 [num_nodes, out_channels]
        """
        batch_size = x.size(0)
        device = x.device

        # 计算时间差
        if reference_time is None:
            reference_time = torch.max(timestamps)
        time_diff = (reference_time - timestamps).unsqueeze(-1).float()

        # 时间编码
        time_encoding = self.time_encoder(time_diff)

        # 拼接节点特征和时间编码
        x_with_time = torch.cat([x, time_encoding], dim=-1)

        # 图注意力消息传递
        out = self.gat(x_with_time, edge_index)
        out = self.dropout(out)

        # 时间门控
        gate_input = torch.cat([out, time_encoding], dim=-1)
        gate = self.time_gate(gate_input)
        out = out * gate

        # 残差连接和归一化
        if self.in_channels == self.out_channels:
            out = out + x
        out = self.norm(out)

        return out


class GCNMemoryLayer(nn.Module):
    """GCN记忆层 - 基于谱域图卷积"""

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        dropout: float = 0.1,
        use_residual: bool = True
    ):
        super().__init__()
        self.conv = GCNConv(in_channels, out_channels)
        self.norm = nn.LayerNorm(out_channels)
        self.dropout = nn.Dropout(dropout)
        self.use_residual = use_residual and (in_channels == out_channels)
        self.activation = nn.ReLU()

    def forward(
        self,
        x: torch.Tensor,
        edge_index: torch.Tensor,
        edge_weight: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """前向传播"""
        out = self.conv(x, edge_index, edge_weight)
        out = self.activation(out)
        out = self.dropout(out)

        if self.use_residual:
            out = out + x

        out = self.norm(out)
        return out


class GATMemoryLayer(nn.Module):
    """GAT记忆层 - 基于图注意力机制"""

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        num_heads: int = 8,
        dropout: float = 0.1,
        use_residual: bool = True,
        concat: bool = True
    ):
        super().__init__()
        self.gat = GATConv(
            in_channels,
            out_channels // num_heads if concat else out_channels,
            heads=num_heads,
            dropout=dropout,
            concat=concat
        )
        self.norm = nn.LayerNorm(out_channels if concat else out_channels)
        self.dropout = nn.Dropout(dropout)
        self.use_residual = use_residual and (in_channels == out_channels)
        self.activation = nn.ELU()

    def forward(
        self,
        x: torch.Tensor,
        edge_index: torch.Tensor,
        edge_attr: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """前向传播"""
        out = self.gat(x, edge_index, edge_attr)
        out = self.activation(out)
        out = self.dropout(out)

        if self.use_residual:
            out = out + x

        out = self.norm(out)
        return out


class GraphSAGEMemoryLayer(nn.Module):
    """GraphSAGE记忆层 - 归纳式图学习"""

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        aggr: str = "mean",
        dropout: float = 0.1,
        use_residual: bool = True
    ):
        super().__init__()
        self.conv = SAGEConv(in_channels, out_channels, aggr=aggr)
        self.norm = nn.LayerNorm(out_channels)
        self.dropout = nn.Dropout(dropout)
        self.use_residual = use_residual and (in_channels == out_channels)
        self.activation = nn.ReLU()

    def forward(
        self,
        x: torch.Tensor,
        edge_index: torch.Tensor
    ) -> torch.Tensor:
        """前向传播"""
        out = self.conv(x, edge_index)
        out = self.activation(out)
        out = self.dropout(out)

        if self.use_residual:
            out = out + x

        out = self.norm(out)
        return out


class GINMemoryLayer(nn.Module):
    """GIN记忆层 - 图同构网络"""

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        dropout: float = 0.1,
        use_residual: bool = True
    ):
        super().__init__()
        nn_module = nn.Sequential(
            nn.Linear(in_channels, out_channels),
            nn.ReLU(),
            nn.Linear(out_channels, out_channels)
        )
        self.conv = GINConv(nn_module)
        self.norm = nn.LayerNorm(out_channels)
        self.dropout = nn.Dropout(dropout)
        self.use_residual = use_residual and (in_channels == out_channels)
        self.activation = nn.ReLU()

    def forward(
        self,
        x: torch.Tensor,
        edge_index: torch.Tensor
    ) -> torch.Tensor:
        """前向传播"""
        out = self.conv(x, edge_index)
        out = self.activation(out)
        out = self.dropout(out)

        if self.use_residual:
            out = out + x

        out = self.norm(out)
        return out


class MemoryGraphAttention(nn.Module):
    """
    记忆图注意力模块
    计算节点间的重要性分数并生成注意力权重
    """

    def __init__(
        self,
        hidden_dim: int,
        num_heads: int = 8,
        dropout: float = 0.1
    ):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.num_heads = num_heads

        # 多头注意力
        self.query_proj = nn.Linear(hidden_dim, hidden_dim)
        self.key_proj = nn.Linear(hidden_dim, hidden_dim)
        self.value_proj = nn.Linear(hidden_dim, hidden_dim)

        # 重要性评分网络
        self.importance_scorer = nn.Sequential(
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid()
        )

        self.dropout = nn.Dropout(dropout)
        self.scale = (hidden_dim // num_heads) ** -0.5

    def forward(
        self,
        query: torch.Tensor,
        key: torch.Tensor,
        value: torch.Tensor,
        edge_index: torch.Tensor,
        return_importance: bool = False
    ) -> Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]]:
        """
        计算图注意力

        Args:
            query: 查询向量 [num_nodes, hidden_dim]
            key: 键向量 [num_nodes, hidden_dim]
            value: 值向量 [num_nodes, hidden_dim]
            edge_index: 边索引 [2, num_edges]
            return_importance: 是否返回重要性分数

        Returns:
            注意力输出，可选重要性分数
        """
        batch_size = query.size(0)

        # 投影
        Q = self.query_proj(query)
        K = self.key_proj(key)
        V = self.value_proj(value)

        # 多头分割
        Q = Q.view(batch_size, self.num_heads, -1)
        K = K.view(batch_size, self.num_heads, -1)
        V = V.view(batch_size, self.num_heads, -1)

        # 计算边级别的注意力
        src, dst = edge_index
        Q_edges = Q[src]  # [num_edges, num_heads, head_dim]
        K_edges = K[dst]  # [num_edges, num_heads, head_dim]

        # 注意力分数
        attn_scores = (Q_edges * K_edges).sum(dim=-1) * self.scale
        attn_weights = F.softmax(attn_scores, dim=0)
        attn_weights = self.dropout(attn_weights)

        # 消息聚合
        V_edges = V[dst]
        messages = V_edges * attn_weights.unsqueeze(-1)

        # 聚合到源节点
        output = torch.zeros_like(Q)
        output.index_add_(0, src, messages)

        # 合并多头
        output = output.view(batch_size, -1)

        # 计算重要性分数
        if return_importance:
            # 基于节点特征和邻居特征计算重要性
            node_pairs = torch.cat([query, key], dim=-1)
            importance = self.importance_scorer(node_pairs)
            return output, importance

        return output


class GNNMemoryEngine(nn.Module):
    """
    图神经网络记忆关联引擎

    核心功能：
    1. 多种GNN架构支持（GCN/GAT/GraphSAGE/GIN/T-GNN）
    2. 动态记忆图谱构建和更新
    3. 记忆节点关系推理和聚类
    4. 基于图注意力机制的重要性评分
    5. 时序记忆建模

    Attributes:
        input_dim: 输入特征维度
        hidden_dim: 隐藏层维度
        output_dim: 输出特征维度
        num_layers: GNN层数
        gnn_type: GNN类型
    """

    def __init__(
        self,
        input_dim: int,
        hidden_dim: int = 256,
        output_dim: int = 128,
        num_layers: int = 3,
        gnn_type: GNNType = GNNType.GAT,
        num_heads: int = 8,
        dropout: float = 0.1,
        use_temporal: bool = True,
        max_memory_nodes: int = 10000,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim
        self.num_layers = num_layers
        self.gnn_type = gnn_type
        self.num_heads = num_heads
        self.use_temporal = use_temporal
        self.max_memory_nodes = max_memory_nodes
        self.device = device

        # 输入投影层
        self.input_proj = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.LayerNorm(hidden_dim)
        )

        # 构建GNN层
        self.convs = nn.ModuleList()
        self.temporal_layers = nn.ModuleList() if use_temporal else None

        for i in range(num_layers):
            in_ch = hidden_dim if i > 0 else hidden_dim
            out_ch = hidden_dim if i < num_layers - 1 else output_dim

            if gnn_type == GNNType.GCN:
                layer = GCNMemoryLayer(in_ch, out_ch, dropout)
            elif gnn_type == GNNType.GAT:
                layer = GATMemoryLayer(
                    in_ch, out_ch, num_heads, dropout,
                    concat=(i < num_layers - 1)
                )
            elif gnn_type == GNNType.GRAPHSAGE:
                layer = GraphSAGEMemoryLayer(in_ch, out_ch, dropout=dropout)
            elif gnn_type == GNNType.GIN:
                layer = GINMemoryLayer(in_ch, out_ch, dropout)
            else:
                layer = GCNMemoryLayer(in_ch, out_ch, dropout)

            self.convs.append(layer)

            # 时序层
            if use_temporal:
                temporal_layer = TemporalGraphLayer(
                    out_ch, out_ch, num_heads, dropout
                )
                self.temporal_layers.append(temporal_layer)

        # 图注意力模块
        self.graph_attention = MemoryGraphAttention(
            output_dim, num_heads, dropout
        )

        # 记忆聚类头
        self.cluster_head = nn.Sequential(
            nn.Linear(output_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2)
        )

        # 重要性预测头
        self.importance_head = nn.Sequential(
            nn.Linear(output_dim * 2, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid()
        )

        # 记忆存储
        self.memory_nodes: Dict[int, MemoryNode] = {}
        self.memory_edges: List[MemoryEdge] = []
        self.node_features: Optional[torch.Tensor] = None
        self.edge_index: Optional[torch.Tensor] = None
        self.timestamps: Optional[torch.Tensor] = None

        self.to(device)

    def build_memory_graph(
        self,
        nodes: List[MemoryNode],
        edges: List[MemoryEdge]
    ) -> Data:
        """
        构建记忆图

        Args:
            nodes: 记忆节点列表
            edges: 记忆边列表

        Returns:
            PyG Data对象
        """
        if not nodes:
            raise ValueError("节点列表不能为空")

        # 构建节点特征矩阵
        x = torch.stack([node.features for node in nodes]).to(self.device)
        timestamps = torch.tensor(
            [node.timestamp for node in nodes],
            dtype=torch.float
        ).to(self.device)

        # 构建边索引
        node_id_to_idx = {node.node_id: i for i, node in enumerate(nodes)}

        edge_list = []
        edge_weights = []
        for edge in edges:
            if edge.source_id in node_id_to_idx and edge.target_id in node_id_to_idx:
                src_idx = node_id_to_idx[edge.source_id]
                dst_idx = node_id_to_idx[edge.target_id]
                edge_list.append([src_idx, dst_idx])
                edge_weights.append(edge.weight)

        if edge_list:
            edge_index = torch.tensor(edge_list, dtype=torch.long).t().to(self.device)
            edge_weight = torch.tensor(edge_weights, dtype=torch.float).to(self.device)
        else:
            # 如果没有边，创建自环边
            edge_index = torch.arange(len(nodes)).repeat(2, 1).to(self.device)
            edge_weight = torch.ones(len(nodes)).to(self.device)

        # 添加自环
        edge_index, edge_weight = add_self_loops(
            edge_index, edge_weight, fill_value=1.0, num_nodes=len(nodes)
        )

        data = Data(
            x=x,
            edge_index=edge_index,
            edge_attr=edge_weight,
            timestamps=timestamps,
            num_nodes=len(nodes)
        )

        return data

    def forward(
        self,
        data: Data,
        return_attention: bool = False
    ) -> Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]]:
        """
        前向传播

        Args:
            data: PyG Data对象
            return_attention: 是否返回注意力权重

        Returns:
            节点嵌入，可选注意力权重
        """
        x, edge_index = data.x, data.edge_index
        timestamps = data.timestamps if hasattr(data, 'timestamps') else None

        # 输入投影
        x = self.input_proj(x)

        # GNN层传播
        for i, conv in enumerate(self.convs):
            if isinstance(conv, (GCNMemoryLayer, GATMemoryLayer)):
                edge_attr = data.edge_attr if hasattr(data, 'edge_attr') else None
                x = conv(x, edge_index, edge_attr)
            else:
                x = conv(x, edge_index)

            # 时序处理
            if self.use_temporal and self.temporal_layers and timestamps is not None:
                x = self.temporal_layers[i](x, edge_index, timestamps)

        # 图注意力
        if return_attention:
            x_attended, importance = self.graph_attention(
                x, x, x, edge_index, return_importance=True
            )
            return x_attended, importance

        return x

    def infer_relationships(
        self,
        query_nodes: List[int],
        top_k: int = 5
    ) -> Dict[int, List[Tuple[int, float]]]:
        """
        推理记忆节点间的关系

        Args:
            query_nodes: 查询节点ID列表
            top_k: 返回的最相似节点数

        Returns:
            每个查询节点的相关节点及相似度
        """
        if self.node_features is None:
            raise RuntimeError("记忆图尚未构建，请先调用update_memory_graph()")

        results = {}
        node_ids = list(self.memory_nodes.keys())
        node_id_to_idx = {nid: i for i, nid in enumerate(node_ids)}

        for query_id in query_nodes:
            if query_id not in node_id_to_idx:
                continue

            query_idx = node_id_to_idx[query_id]
            query_feat = self.node_features[query_idx].unsqueeze(0)

            # 计算余弦相似度
            similarities = F.cosine_similarity(
                query_feat, self.node_features, dim=-1
            )

            # 获取top-k
            top_values, top_indices = torch.topk(similarities, min(top_k + 1, len(node_ids)))

            related = []
            for idx, sim in zip(top_indices, top_values):
                if node_ids[idx] != query_id:
                    related.append((node_ids[idx], sim.item()))

            results[query_id] = related[:top_k]

        return results

    def cluster_memories(
        self,
        num_clusters: int = 10,
        method: str = "kmeans"
    ) -> Dict[int, int]:
        """
        对记忆节点进行聚类

        Args:
            num_clusters: 聚类数量
            method: 聚类方法 ("kmeans" 或 "spectral")

        Returns:
            节点ID到聚类ID的映射
        """
        if self.node_features is None:
            raise RuntimeError("记忆图尚未构建")

        from sklearn.cluster import KMeans, SpectralClustering

        features = self.node_features.cpu().numpy()
        node_ids = list(self.memory_nodes.keys())

        if method == "kmeans":
            clusterer = KMeans(n_clusters=num_clusters, random_state=42)
        else:
            clusterer = SpectralClustering(
                n_clusters=num_clusters, random_state=42, affinity='nearest_neighbors'
            )

        labels = clusterer.fit_predict(features)

        return {node_id: int(label) for node_id, label in zip(node_ids, labels)}

    def compute_importance_scores(
        self,
        node_features: torch.Tensor,
        neighbor_features: torch.Tensor
    ) -> torch.Tensor:
        """
        计算记忆节点的重要性分数

        Args:
            node_features: 节点特征
            neighbor_features: 邻居聚合特征

        Returns:
            重要性分数
        """
        combined = torch.cat([node_features, neighbor_features], dim=-1)
        importance = self.importance_head(combined)
        return importance

    def update_memory_graph(
        self,
        new_nodes: List[MemoryNode],
        new_edges: List[MemoryEdge],
        incremental: bool = True
    ) -> None:
        """
        更新记忆图

        Args:
            new_nodes: 新节点列表
            new_edges: 新边列表
            incremental: 是否增量更新
        """
        if incremental and self.memory_nodes:
            # 增量更新
            for node in new_nodes:
                self.memory_nodes[node.node_id] = node
            self.memory_edges.extend(new_edges)

            # 限制节点数量
            if len(self.memory_nodes) > self.max_memory_nodes:
                # 移除最旧的节点
                sorted_nodes = sorted(
                    self.memory_nodes.values(),
                    key=lambda n: n.timestamp
                )
                nodes_to_remove = len(self.memory_nodes) - self.max_memory_nodes
                for node in sorted_nodes[:nodes_to_remove]:
                    del self.memory_nodes[node.node_id]
        else:
            # 全量更新
            self.memory_nodes = {node.node_id: node for node in new_nodes}
            self.memory_edges = new_edges

        # 重建图
        if self.memory_nodes:
            data = self.build_memory_graph(
                list(self.memory_nodes.values()),
                self.memory_edges
            )

            with torch.no_grad():
                features = self.forward(data)

            self.node_features = features
            self.edge_index = data.edge_index
            self.timestamps = data.timestamps

    def query_similar_memories(
        self,
        query_features: torch.Tensor,
        top_k: int = 10,
        threshold: float = 0.5
    ) -> List[Tuple[int, float]]:
        """
        查询相似记忆

        Args:
            query_features: 查询特征
            top_k: 返回结果数
            threshold: 相似度阈值

        Returns:
            相似记忆ID和分数列表
        """
        if self.node_features is None:
            return []

        query_features = query_features.to(self.device)
        if query_features.dim() == 1:
            query_features = query_features.unsqueeze(0)

        # 投影查询特征
        query_proj = self.input_proj(query_features)
        for conv in self.convs[:-1]:
            query_proj = conv(query_proj, self.edge_index)

        # 计算相似度
        similarities = F.cosine_similarity(
            query_proj, self.node_features, dim=-1
        )

        # 过滤和排序
        mask = similarities > threshold
        filtered_sims = similarities[mask]
        filtered_indices = torch.where(mask)[0]

        if len(filtered_sims) == 0:
            return []

        top_values, top_local_indices = torch.topk(
            filtered_sims, min(top_k, len(filtered_sims))
        )
        top_global_indices = filtered_indices[top_local_indices]

        node_ids = list(self.memory_nodes.keys())
        results = [
            (node_ids[idx], score.item())
            for idx, score in zip(top_global_indices, top_values)
        ]

        return results

    def get_memory_summary(self) -> Dict[str, Any]:
        """获取记忆图摘要信息"""
        return {
            "num_nodes": len(self.memory_nodes),
            "num_edges": len(self.memory_edges),
            "gnn_type": self.gnn_type.value,
            "num_layers": self.num_layers,
            "hidden_dim": self.hidden_dim,
            "output_dim": self.output_dim,
            "use_temporal": self.use_temporal,
            "device": str(self.device)
        }

    def save_model(self, path: str) -> None:
        """保存模型"""
        checkpoint = {
            "state_dict": self.state_dict(),
            "config": {
                "input_dim": self.input_dim,
                "hidden_dim": self.hidden_dim,
                "output_dim": self.output_dim,
                "num_layers": self.num_layers,
                "gnn_type": self.gnn_type.value,
                "num_heads": self.num_heads,
                "use_temporal": self.use_temporal,
                "max_memory_nodes": self.max_memory_nodes
            },
            "memory_nodes": self.memory_nodes,
            "memory_edges": self.memory_edges
        }
        torch.save(checkpoint, path)

    def load_model(self, path: str) -> None:
        """加载模型"""
        checkpoint = torch.load(path, map_location=self.device)
        self.load_state_dict(checkpoint["state_dict"])
        self.memory_nodes = checkpoint.get("memory_nodes", {})
        self.memory_edges = checkpoint.get("memory_edges", [])

        if self.memory_nodes:
            self.update_memory_graph(
                list(self.memory_nodes.values()),
                self.memory_edges,
                incremental=False
            )


def create_sample_memory_graph(
    num_nodes: int = 100,
    num_edges: int = 300,
    feature_dim: int = 64
) -> Tuple[List[MemoryNode], List[MemoryEdge]]:
    """
    创建示例记忆图数据

    Args:
        num_nodes: 节点数量
        num_edges: 边数量
        feature_dim: 特征维度

    Returns:
        节点列表和边列表
    """
    nodes = []
    for i in range(num_nodes):
        node = MemoryNode(
            node_id=i,
            features=torch.randn(feature_dim),
            timestamp=float(i),
            node_type="memory",
            importance_score=np.random.random(),
            metadata={"content": f"Memory content {i}"}
        )
        nodes.append(node)

    edges = []
    for _ in range(num_edges):
        src = np.random.randint(0, num_nodes)
        dst = np.random.randint(0, num_nodes)
        if src != dst:
            edge = MemoryEdge(
                source_id=src,
                target_id=dst,
                edge_type="association",
                weight=np.random.random(),
                timestamp=float(max(src, dst))
            )
            edges.append(edge)

    return nodes, edges


def train_gnn_memory_engine(
    model: GNNMemoryEngine,
    nodes: List[MemoryNode],
    edges: List[MemoryEdge],
    epochs: int = 100,
    lr: float = 0.001
) -> List[float]:
    """
    训练GNN记忆引擎

    Args:
        model: GNN模型
        nodes: 节点列表
        edges: 边列表
        epochs: 训练轮数
        lr: 学习率

    Returns:
        损失历史
    """
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    data = model.build_memory_graph(nodes, edges)
    losses = []

    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad()

        # 前向传播
        output = model(data)

        # 重构损失（自编码器风格）
        target = model.input_proj(data.x)
        for conv in model.convs[:-1]:
            if isinstance(conv, (GCNMemoryLayer, GATMemoryLayer)):
                edge_attr = data.edge_attr if hasattr(data, 'edge_attr') else None
                target = conv(target, data.edge_index, edge_attr)
            else:
                target = conv(target, data.edge_index)

        loss = criterion(output, target)

        # 添加正则化
        l2_reg = sum(p.pow(2).sum() for p in model.parameters())
        loss = loss + 0.0001 * l2_reg

        loss.backward()
        optimizer.step()

        losses.append(loss.item())

        if (epoch + 1) % 20 == 0:
            print(f"Epoch {epoch + 1}/{epochs}, Loss: {loss.item():.4f}")

    return losses


# 使用示例
if __name__ == "__main__":
    print("=" * 60)
    print("GNN Memory Engine Demo")
    print("=" * 60)

    # 配置参数
    INPUT_DIM = 64
    HIDDEN_DIM = 128
    OUTPUT_DIM = 64
    NUM_NODES = 100
    NUM_EDGES = 300

    # 创建示例数据
    print("\n[1] Creating sample memory graph...")
    nodes, edges = create_sample_memory_graph(NUM_NODES, NUM_EDGES, INPUT_DIM)
    print(f"    Created {len(nodes)} nodes and {len(edges)} edges")

    # 初始化模型
    print("\n[2] Initializing GNN Memory Engine...")
    model = GNNMemoryEngine(
        input_dim=INPUT_DIM,
        hidden_dim=HIDDEN_DIM,
        output_dim=OUTPUT_DIM,
        num_layers=3,
        gnn_type=GNNType.GAT,
        num_heads=8,
        use_temporal=True
    )
    print(f"    Model parameters: {sum(p.numel() for p in model.parameters()):,}")

    # 训练模型
    print("\n[3] Training model...")
    losses = train_gnn_memory_engine(model, nodes, edges, epochs=50, lr=0.001)
    print(f"    Final loss: {losses[-1]:.4f}")

    # 更新记忆图
    print("\n[4] Updating memory graph...")
    model.update_memory_graph(nodes, edges)
    summary = model.get_memory_summary()
    print(f"    Memory summary: {summary}")

    # 关系推理
    print("\n[5] Inferring relationships...")
    query_nodes = [0, 1, 2]
    relationships = model.infer_relationships(query_nodes, top_k=5)
    for node_id, related in relationships.items():
        print(f"    Node {node_id} related to: {related[:3]}")

    # 记忆聚类
    print("\n[6] Clustering memories...")
    clusters = model.cluster_memories(num_clusters=5)
    cluster_counts = defaultdict(int)
    for cid in clusters.values():
        cluster_counts[cid] += 1
    print(f"    Cluster distribution: {dict(cluster_counts)}")

    # 相似记忆查询
    print("\n[7] Querying similar memories...")
    query_feat = torch.randn(INPUT_DIM)
    similar = model.query_similar_memories(query_feat, top_k=5)
    print(f"    Top 5 similar memories: {similar}")

    # 保存和加载
    print("\n[8] Saving and loading model...")
    save_path = "/tmp/gnn_memory_engine.pt"
    model.save_model(save_path)

    new_model = GNNMemoryEngine(
        input_dim=INPUT_DIM,
        hidden_dim=HIDDEN_DIM,
        output_dim=OUTPUT_DIM,
        num_layers=3,
        gnn_type=GNNType.GAT
    )
    new_model.load_model(save_path)
    print(f"    Model saved and loaded successfully")

    print("\n" + "=" * 60)
    print("Demo completed successfully!")
    print("=" * 60)

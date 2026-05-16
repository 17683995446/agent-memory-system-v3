"""
对比学习记忆表示 (Contrastive Memory Learning)

基于SimCLR框架的对比学习系统，实现记忆样本的正负对构造、
温度缩放InfoNCE损失、数据增强策略和硬负样本挖掘。

Author: Agent Memory System V3.0
Date: 2026-05-16
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np
from typing import Dict, List, Tuple, Optional, Union, Callable, Any
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import random
import math
import warnings
from abc import ABC, abstractmethod

warnings.filterwarnings('ignore')


class AugmentationType(Enum):
    """数据增强类型"""
    GAUSSIAN_NOISE = "gaussian_noise"
    DROPOUT = "dropout"
    SHUFFLE = "shuffle"
    SCALE = "scale"
    ROTATION = "rotation"
    MASKING = "masking"
    MIXUP = "mixup"


class DataAugmentation:
    """
    数据增强模块
    实现语义保持的数据增强策略
    """

    def __init__(
        self,
        augmentation_types: List[AugmentationType] = None,
        intensity: float = 0.5
    ):
        self.augmentation_types = augmentation_types or [
            AugmentationType.GAUSSIAN_NOISE,
            AugmentationType.DROPOUT,
            AugmentationType.SHUFFLE,
            AugmentationType.SCALE
        ]
        self.intensity = intensity

    def gaussian_noise(
        self,
        x: torch.Tensor,
        noise_level: Optional[float] = None
    ) -> torch.Tensor:
        """添加高斯噪声"""
        noise_level = noise_level or self.intensity * 0.1
        noise = torch.randn_like(x) * noise_level
        return x + noise

    def dropout(
        self,
        x: torch.Tensor,
        drop_prob: Optional[float] = None
    ) -> torch.Tensor:
        """随机丢弃特征"""
        drop_prob = drop_prob or self.intensity * 0.1
        mask = torch.rand_like(x) > drop_prob
        return x * mask.float()

    def shuffle(self, x: torch.Tensor, shuffle_ratio: float = 0.1) -> torch.Tensor:
        """随机打乱部分特征"""
        if x.dim() == 1:
            x = x.unsqueeze(0)

        batch_size, feature_dim = x.shape
        num_shuffle = int(feature_dim * shuffle_ratio)

        shuffled = x.clone()
        for i in range(batch_size):
            indices = torch.randperm(feature_dim)[:num_shuffle]
            permuted_indices = indices[torch.randperm(len(indices))]
            shuffled[i, indices] = x[i, permuted_indices]

        return shuffled.squeeze(0) if batch_size == 1 else shuffled

    def scale(
        self,
        x: torch.Tensor,
        scale_range: Tuple[float, float] = (0.8, 1.2)
    ) -> torch.Tensor:
        """随机缩放"""
        scale_factor = random.uniform(*scale_range)
        return x * scale_factor

    def masking(
        self,
        x: torch.Tensor,
        mask_ratio: float = 0.15
    ) -> torch.Tensor:
        """随机掩码"""
        mask = torch.rand_like(x) > mask_ratio
        return x * mask.float()

    def mixup(
        self,
        x1: torch.Tensor,
        x2: torch.Tensor,
        alpha: float = 0.2
    ) -> torch.Tensor:
        """Mixup数据增强"""
        lam = np.random.beta(alpha, alpha)
        return lam * x1 + (1 - lam) * x2

    def __call__(self, x: torch.Tensor) -> torch.Tensor:
        """应用随机增强"""
        aug_type = random.choice(self.augmentation_types)

        if aug_type == AugmentationType.GAUSSIAN_NOISE:
            return self.gaussian_noise(x)
        elif aug_type == AugmentationType.DROPOUT:
            return self.dropout(x)
        elif aug_type == AugmentationType.SHUFFLE:
            return self.shuffle(x)
        elif aug_type == AugmentationType.SCALE:
            return self.scale(x)
        elif aug_type == AugmentationType.MASKING:
            return self.masking(x)
        else:
            return x


class ContrastiveAugmentation:
    """
    对比学习专用增强
    生成同一样本的两个不同视图
    """

    def __init__(
        self,
        augmentation_strength: float = 1.0,
        num_views: int = 2
    ):
        self.augmentation_strength = augmentation_strength
        self.num_views = num_views
        self.augmenter = DataAugmentation(intensity=augmentation_strength)

    def __call__(self, x: torch.Tensor) -> List[torch.Tensor]:
        """生成多个视图"""
        views = []
        for _ in range(self.num_views):
            view = self.augmenter(x.clone())
            views.append(view)
        return views


class SimCLRLoss(nn.Module):
    """
    SimCLR对比损失
    使用NT-Xent（归一化温度缩放交叉熵）损失
    """

    def __init__(self, temperature: float = 0.5):
        super().__init__()
        self.temperature = temperature
        self.criterion = nn.CrossEntropyLoss()

    def forward(
        self,
        z_i: torch.Tensor,
        z_j: torch.Tensor
    ) -> torch.Tensor:
        """
        计算对比损失

        Args:
            z_i: 第一组表示 [batch_size, feature_dim]
            z_j: 第二组表示 [batch_size, feature_dim]

        Returns:
            对比损失值
        """
        batch_size = z_i.size(0)

        # 归一化
        z_i = F.normalize(z_i, dim=1)
        z_j = F.normalize(z_j, dim=1)

        # 构建相似度矩阵
        representations = torch.cat([z_i, z_j], dim=0)  # [2*batch_size, feature_dim]
        similarity_matrix = torch.mm(representations, representations.t()) / self.temperature

        # 创建标签
        labels = torch.arange(batch_size).to(z_i.device)
        labels = torch.cat([labels + batch_size, labels])

        # 掩码对角线（自身相似度）
        mask = torch.eye(2 * batch_size, device=z_i.device).bool()
        similarity_matrix = similarity_matrix.masked_fill(mask, -9e15)

        # 计算损失
        loss = self.criterion(similarity_matrix, labels)

        return loss


class InfoNCELoss(nn.Module):
    """
    InfoNCE损失
    更通用的对比损失形式
    """

    def __init__(
        self,
        temperature: float = 0.07,
        reduction: str = 'mean'
    ):
        super().__init__()
        self.temperature = temperature
        self.reduction = reduction

    def forward(
        self,
        query: torch.Tensor,
        positive_key: torch.Tensor,
        negative_keys: torch.Tensor
    ) -> torch.Tensor:
        """
        计算InfoNCE损失

        Args:
            query: 查询向量 [batch_size, feature_dim]
            positive_key: 正样本 [batch_size, feature_dim]
            negative_keys: 负样本 [num_negatives, feature_dim]

        Returns:
            InfoNCE损失
        """
        # 归一化
        query = F.normalize(query, dim=1)
        positive_key = F.normalize(positive_key, dim=1)
        negative_keys = F.normalize(negative_keys, dim=1)

        # 正样本相似度
        positive_logit = torch.sum(query * positive_key, dim=1, keepdim=True) / self.temperature

        # 负样本相似度
        negative_logits = torch.mm(query, negative_keys.t()) / self.temperature

        # 合并logits
        logits = torch.cat([positive_logit, negative_logits], dim=1)

        # 标签（正样本总是在位置0）
        labels = torch.zeros(len(query), dtype=torch.long, device=query.device)

        # 交叉熵损失
        loss = F.cross_entropy(logits, labels, reduction=self.reduction)

        return loss


class HardNegativeMiner:
    """
    硬负样本挖掘器
    识别和选择难以区分的负样本
    """

    def __init__(
        self,
        num_negatives: int = 100,
        hard_ratio: float = 0.5,
        mining_strategy: str = "semi_hard"
    ):
        self.num_negatives = num_negatives
        self.hard_ratio = hard_ratio
        self.mining_strategy = mining_strategy
        self.negative_bank: deque = deque(maxlen=10000)

    def update_bank(self, embeddings: torch.Tensor) -> None:
        """更新负样本库"""
        for emb in embeddings.detach().cpu():
            self.negative_bank.append(emb)

    def mine_hard_negatives(
        self,
        anchor: torch.Tensor,
        positive: torch.Tensor,
        all_negatives: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        挖掘硬负样本

        Args:
            anchor: 锚点样本
            positive: 正样本
            all_negatives: 所有候选负样本

        Returns:
            选中的硬负样本
        """
        if all_negatives is None and len(self.negative_bank) == 0:
            raise ValueError("No negatives available")

        if all_negatives is None:
            all_negatives = torch.stack(list(self.negative_bank))

        # 计算相似度
        anchor_norm = F.normalize(anchor, dim=1)
        negatives_norm = F.normalize(all_negatives, dim=1)
        positive_norm = F.normalize(positive, dim=1)

        # 锚点与负样本的相似度
        neg_similarities = torch.mm(anchor_norm, negatives_norm.t()).squeeze(0)

        # 锚点与正样本的相似度
        pos_similarity = torch.sum(anchor_norm * positive_norm, dim=1).item()

        if self.mining_strategy == "hard":
            # 选择相似度最高的负样本（最难的）
            hard_indices = torch.topk(
                neg_similarities,
                min(self.num_negatives, len(neg_similarities))
            ).indices

        elif self.mining_strategy == "semi_hard":
            # 选择比正样本稍容易的负样本
            semi_hard_mask = (neg_similarities < pos_similarity) & (neg_similarities > pos_similarity - 0.1)
            semi_hard_indices = torch.where(semi_hard_mask)[0]

            if len(semi_hard_indices) < self.num_negatives:
                # 如果semi-hard不够，补充一些hard negatives
                additional = self.num_negatives - len(semi_hard_indices)
                remaining_indices = torch.where(~semi_hard_mask)[0]
                top_hard = torch.topk(
                    neg_similarities[remaining_indices],
                    min(additional, len(remaining_indices))
                ).indices
                hard_indices = torch.cat([
                    semi_hard_indices,
                    remaining_indices[top_hard]
                ])
            else:
                hard_indices = semi_hard_indices[:self.num_negatives]

        else:  # random
            hard_indices = torch.randperm(len(all_negatives))[:self.num_negatives]

        return all_negatives[hard_indices]


class ProjectionHead(nn.Module):
    """
    投影头
    将表示映射到对比学习空间
    """

    def __init__(
        self,
        input_dim: int,
        hidden_dim: int = 256,
        output_dim: int = 128,
        num_layers: int = 2
    ):
        super().__init__()

        layers = []
        prev_dim = input_dim

        for i in range(num_layers - 1):
            layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.BatchNorm1d(hidden_dim),
                nn.ReLU(inplace=True)
            ])
            prev_dim = hidden_dim

        layers.append(nn.Linear(prev_dim, output_dim))

        self.projection = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """前向传播"""
        return self.projection(x)


class ContrastiveMemoryEncoder(nn.Module):
    """
    对比学习记忆编码器
    整合编码器和投影头
    """

    def __init__(
        self,
        input_dim: int,
        encoder_hidden_dims: List[int] = [512, 256],
        projection_dim: int = 128,
        dropout: float = 0.1
    ):
        super().__init__()

        # 编码器
        encoder_layers = []
        prev_dim = input_dim
        for hidden_dim in encoder_hidden_dims:
            encoder_layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.BatchNorm1d(hidden_dim),
                nn.ReLU(inplace=True),
                nn.Dropout(dropout)
            ])
            prev_dim = hidden_dim

        self.encoder = nn.Sequential(*encoder_layers)
        self.encoder_output_dim = prev_dim

        # 投影头
        self.projection_head = ProjectionHead(
            input_dim=prev_dim,
            hidden_dim=256,
            output_dim=projection_dim,
            num_layers=2
        )

    def forward(
        self,
        x: torch.Tensor,
        return_projection: bool = True
    ) -> Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]]:
        """
        前向传播

        Args:
            x: 输入特征
            return_projection: 是否返回投影

        Returns:
            表示，可选投影
        """
        representation = self.encoder(x)

        if return_projection:
            projection = self.projection_head(representation)
            return representation, projection

        return representation

    def encode(self, x: torch.Tensor) -> torch.Tensor:
        """仅编码"""
        return self.encoder(x)


class ContrastiveMemoryLearner:
    """
    对比学习记忆学习器主类
    整合SimCLR框架、数据增强和硬负样本挖掘
    """

    def __init__(
        self,
        input_dim: int,
        encoder_hidden_dims: List[int] = [512, 256],
        projection_dim: int = 128,
        temperature: float = 0.5,
        learning_rate: float = 0.001,
        use_hard_negatives: bool = True,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        self.input_dim = input_dim
        self.projection_dim = projection_dim
        self.temperature = temperature
        self.device = device
        self.use_hard_negatives = use_hard_negatives

        # 编码器
        self.encoder = ContrastiveMemoryEncoder(
            input_dim=input_dim,
            encoder_hidden_dims=encoder_hidden_dims,
            projection_dim=projection_dim
        ).to(device)

        # 损失函数
        self.simclr_loss = SimCLRLoss(temperature=temperature)
        self.infonce_loss = InfoNCELoss(temperature=temperature)

        # 数据增强
        self.augmentation = ContrastiveAugmentation()

        # 硬负样本挖掘
        if use_hard_negatives:
            self.hard_miner = HardNegativeMiner(
                num_negatives=100,
                hard_ratio=0.5,
                mining_strategy="semi_hard"
            )

        # 优化器
        self.optimizer = optim.Adam(
            self.encoder.parameters(),
            lr=learning_rate,
            weight_decay=1e-4
        )

        self.scheduler = optim.lr_scheduler.CosineAnnealingLR(
            self.optimizer, T_max=100
        )

        # 训练历史
        self.loss_history: List[float] = []
        self.embedding_bank: deque = deque(maxlen=10000)

    def train_step(
        self,
        batch: torch.Tensor,
        use_hard_negatives: Optional[bool] = None
    ) -> Dict[str, float]:
        """
        单步训练

        Args:
            batch: 输入批次
            use_hard_negatives: 是否使用硬负样本

        Returns:
            训练指标
        """
        use_hard_negatives = use_hard_negatives or self.use_hard_negatives
        batch = batch.to(self.device)

        self.optimizer.zero_grad()

        # 生成两个视图
        views = self.augmentation(batch)
        view1, view2 = views[0], views[1]

        # 编码
        _, proj1 = self.encoder(view1)
        _, proj2 = self.encoder(view2)

        # 计算SimCLR损失
        loss = self.simclr_loss(proj1, proj2)

        # 如果使用硬负样本，添加InfoNCE损失
        if use_hard_negatives and len(self.embedding_bank) > 100:
            bank_tensor = torch.stack(list(self.embedding_bank)).to(self.device)

            for i in range(len(batch)):
                anchor = proj1[i:i+1]
                positive = proj2[i:i+1]

                # 挖掘硬负样本
                hard_negs = self.hard_miner.mine_hard_negatives(
                    anchor, positive, bank_tensor
                )

                # 添加InfoNCE损失
                hard_loss = self.infonce_loss(anchor, positive, hard_negs)
                loss = loss + 0.1 * hard_loss

        # 反向传播
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.encoder.parameters(), max_norm=1.0)
        self.optimizer.step()

        # 更新embedding bank
        with torch.no_grad():
            representations = self.encoder.encode(batch)
            for rep in representations:
                self.embedding_bank.append(rep.cpu())
            if use_hard_negatives:
                self.hard_miner.update_bank(representations)

        self.loss_history.append(loss.item())

        return {
            'loss': loss.item(),
            'learning_rate': self.optimizer.param_groups[0]['lr']
        }

    def train_epoch(
        self,
        dataloader: DataLoader,
        epoch: int
    ) -> Dict[str, float]:
        """
        训练一个epoch

        Args:
            dataloader: 数据加载器
            epoch: 当前epoch

        Returns:
            平均指标
        """
        self.encoder.train()
        total_loss = 0
        num_batches = 0

        for batch in dataloader:
            if isinstance(batch, (list, tuple)):
                batch = batch[0]

            metrics = self.train_step(batch)
            total_loss += metrics['loss']
            num_batches += 1

        self.scheduler.step()

        avg_loss = total_loss / num_batches if num_batches > 0 else 0

        return {
            'epoch': epoch,
            'avg_loss': avg_loss,
            'learning_rate': self.optimizer.param_groups[0]['lr']
        }

    def encode(self, x: torch.Tensor) -> torch.Tensor:
        """
        编码输入

        Args:
            x: 输入特征

        Returns:
            编码表示
        """
        self.encoder.eval()
        x = x.to(self.device)

        with torch.no_grad():
            representation = self.encoder.encode(x)

        return representation

    def compute_similarity(
        self,
        x1: torch.Tensor,
        x2: torch.Tensor
    ) -> torch.Tensor:
        """
        计算样本间相似度

        Args:
            x1: 样本1
            x2: 样本2

        Returns:
            相似度矩阵
        """
        rep1 = self.encode(x1)
        rep2 = self.encode(x2)

        # 归一化并计算余弦相似度
        rep1 = F.normalize(rep1, dim=1)
        rep2 = F.normalize(rep2, dim=1)

        similarity = torch.mm(rep1, rep2.t())

        return similarity

    def find_similar_memories(
        self,
        query: torch.Tensor,
        memory_bank: torch.Tensor,
        top_k: int = 10
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        查找相似记忆

        Args:
            query: 查询样本
            memory_bank: 记忆库
            top_k: 返回数量

        Returns:
            相似度分数和索引
        """
        query_rep = self.encode(query)
        memory_rep = self.encode(memory_bank)

        # 计算相似度
        query_rep = F.normalize(query_rep, dim=1)
        memory_rep = F.normalize(memory_rep, dim=1)

        similarities = torch.mm(query_rep, memory_rep.t())

        # 获取top-k
        top_scores, top_indices = torch.topk(similarities, min(top_k, len(memory_bank)), dim=1)

        return top_scores, top_indices

    def save(self, path: str) -> None:
        """保存模型"""
        torch.save({
            'encoder_state': self.encoder.state_dict(),
            'optimizer_state': self.optimizer.state_dict(),
            'scheduler_state': self.scheduler.state_dict(),
            'loss_history': self.loss_history,
            'config': {
                'input_dim': self.input_dim,
                'projection_dim': self.projection_dim,
                'temperature': self.temperature
            }
        }, path)

    def load(self, path: str) -> None:
        """加载模型"""
        checkpoint = torch.load(path, map_location=self.device)
        self.encoder.load_state_dict(checkpoint['encoder_state'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state'])
        self.scheduler.load_state_dict(checkpoint['scheduler_state'])
        self.loss_history = checkpoint['loss_history']


class MemoryContrastiveDataset(Dataset):
    """
    记忆对比学习数据集
    """

    def __init__(
        self,
        memories: torch.Tensor,
        labels: Optional[torch.Tensor] = None,
        augmentation: Optional[ContrastiveAugmentation] = None
    ):
        self.memories = memories
        self.labels = labels
        self.augmentation = augmentation or ContrastiveAugmentation()

    def __len__(self) -> int:
        return len(self.memories)

    def __getitem__(self, idx: int) -> Union[torch.Tensor, Tuple[torch.Tensor, int]]:
        memory = self.memories[idx]

        if self.labels is not None:
            label = self.labels[idx].item()
            return memory, label

        return memory


# 辅助函数
def create_synthetic_memories(
    num_memories: int = 1000,
    feature_dim: int = 128,
    num_clusters: int = 10,
    device: str = "cpu"
) -> torch.Tensor:
    """
    创建合成记忆数据

    Args:
        num_memories: 记忆数量
        feature_dim: 特征维度
        num_clusters: 聚类数
        device: 设备

    Returns:
        合成记忆
    """
    # 生成聚类中心
    centers = torch.randn(num_clusters, feature_dim)

    memories = []
    for _ in range(num_memories):
        # 随机选择中心
        center_idx = random.randint(0, num_clusters - 1)
        center = centers[center_idx]

        # 添加噪声
        memory = center + 0.1 * torch.randn(feature_dim)
        memories.append(memory)

    return torch.stack(memories).to(device)


def evaluate_contrastive_learning(
    learner: ContrastiveMemoryLearner,
    test_memories: torch.Tensor,
    test_labels: torch.Tensor
) -> Dict[str, float]:
    """
    评估对比学习效果

    Args:
        learner: 对比学习器
        test_memories: 测试记忆
        test_labels: 测试标签

    Returns:
        评估指标
    """
    # 编码所有记忆
    embeddings = learner.encode(test_memories)
    embeddings = F.normalize(embeddings, dim=1)

    # 计算相似度矩阵
    similarity_matrix = torch.mm(embeddings, embeddings.t())

    # 计算正样本对和负样本对的平均相似度
    positive_sims = []
    negative_sims = []

    for i in range(len(test_memories)):
        for j in range(i + 1, len(test_memories)):
            sim = similarity_matrix[i, j].item()
            if test_labels[i] == test_labels[j]:
                positive_sims.append(sim)
            else:
                negative_sims.append(sim)

    metrics = {
        'positive_similarity': np.mean(positive_sims) if positive_sims else 0,
        'negative_similarity': np.mean(negative_sims) if negative_sims else 0,
        'contrast': np.mean(positive_sims) - np.mean(negative_sims) if positive_sims and negative_sims else 0
    }

    return metrics


# 使用示例
if __name__ == "__main__":
    print("=" * 70)
    print("Contrastive Memory Learning Demo")
    print("=" * 70)

    # 配置
    INPUT_DIM = 128
    PROJECTION_DIM = 64
    BATCH_SIZE = 64
    NUM_EPOCHS = 20
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

    print(f"\nDevice: {DEVICE}")

    # 创建合成数据
    print("\n[1] Creating synthetic memory data...")
    train_memories = create_synthetic_memories(
        num_memories=1000,
        feature_dim=INPUT_DIM,
        num_clusters=10,
        device=DEVICE
    )
    train_labels = torch.randint(0, 10, (1000,)).to(DEVICE)

    test_memories = create_synthetic_memories(
        num_memories=200,
        feature_dim=INPUT_DIM,
        num_clusters=10,
        device=DEVICE
    )
    test_labels = torch.randint(0, 10, (200,)).to(DEVICE)

    print(f"    Train memories: {train_memories.shape}")
    print(f"    Test memories: {test_memories.shape}")

    # 创建数据加载器
    train_dataset = MemoryContrastiveDataset(train_memories, train_labels)
    train_loader = DataLoader(
        train_dataset,
        batch_size=BATCH_SIZE,
        shuffle=True,
        num_workers=0
    )

    # 初始化对比学习器
    print("\n[2] Initializing Contrastive Memory Learner...")
    learner = ContrastiveMemoryLearner(
        input_dim=INPUT_DIM,
        encoder_hidden_dims=[512, 256],
        projection_dim=PROJECTION_DIM,
        temperature=0.5,
        learning_rate=0.001,
        use_hard_negatives=True,
        device=DEVICE
    )
    print(f"    Parameters: {sum(p.numel() for p in learner.encoder.parameters()):,}")

    # 训练前评估
    print("\n[3] Evaluating before training...")
    metrics_before = evaluate_contrastive_learning(
        learner, test_memories, test_labels
    )
    print(f"    Positive similarity: {metrics_before['positive_similarity']:.4f}")
    print(f"    Negative similarity: {metrics_before['negative_similarity']:.4f}")
    print(f"    Contrast: {metrics_before['contrast']:.4f}")

    # 训练
    print("\n[4] Training contrastive learner...")
    for epoch in range(NUM_EPOCHS):
        metrics = learner.train_epoch(train_loader, epoch)
        if (epoch + 1) % 5 == 0:
            print(f"    Epoch {epoch + 1}: Loss={metrics['avg_loss']:.4f}, LR={metrics['learning_rate']:.6f}")

    # 训练后评估
    print("\n[5] Evaluating after training...")
    metrics_after = evaluate_contrastive_learning(
        learner, test_memories, test_labels
    )
    print(f"    Positive similarity: {metrics_after['positive_similarity']:.4f}")
    print(f"    Negative similarity: {metrics_after['negative_similarity']:.4f}")
    print(f"    Contrast: {metrics_after['contrast']:.4f}")
    print(f"    Contrast improvement: {metrics_after['contrast'] - metrics_before['contrast']:.4f}")

    # 测试相似记忆检索
    print("\n[6] Testing similar memory retrieval...")
    query = test_memories[:5]
    top_scores, top_indices = learner.find_similar_memories(
        query, test_memories, top_k=5
    )
    print(f"    Query 0 top similarities: {top_scores[0].cpu().numpy()}")
    print(f"    Query 0 top indices: {top_indices[0].cpu().numpy()}")

    # 数据增强演示
    print("\n[7] Data augmentation demonstration...")
    augmenter = DataAugmentation(intensity=0.5)
    sample = train_memories[0]
    augmented_samples = [augmenter(sample.clone()) for _ in range(3)]

    original_norm = torch.norm(sample).item()
    augmented_norms = [torch.norm(aug).item() for aug in augmented_samples]
    print(f"    Original norm: {original_norm:.4f}")
    print(f"    Augmented norms: {[f'{n:.4f}' for n in augmented_norms]}")

    # 硬负样本挖掘演示
    print("\n[8] Hard negative mining demonstration...")
    miner = HardNegativeMiner(num_negatives=50, mining_strategy="semi_hard")

    # 填充负样本库
    for i in range(0, len(train_memories), 10):
        miner.update_bank(train_memories[i:i+10])

    anchor = test_memories[0:1]
    positive = test_memories[1:2]

    bank_tensor = torch.stack(list(miner.negative_bank))
    hard_negs = miner.mine_hard_negatives(anchor, positive, bank_tensor)
    print(f"    Negative bank size: {len(miner.negative_bank)}")
    print(f"    Hard negatives shape: {hard_negs.shape}")

    # 计算相似度
    anchor_rep = learner.encode(anchor.to(DEVICE))
    neg_reps = learner.encode(hard_negs.to(DEVICE))

    anchor_rep = F.normalize(anchor_rep, dim=1)
    neg_reps = F.normalize(neg_reps, dim=1)

    similarities = torch.mm(anchor_rep, neg_reps.t())[0]
    print(f"    Similarities to hard negatives: {similarities.cpu().numpy()[:5]}")

    # 保存和加载
    print("\n[9] Saving and loading model...")
    save_path = "/tmp/contrastive_memory_learner.pt"
    learner.save(save_path)

    new_learner = ContrastiveMemoryLearner(
        input_dim=INPUT_DIM,
        projection_dim=PROJECTION_DIM,
        device=DEVICE
    )
    new_learner.load(save_path)
    print(f"    Model saved and loaded successfully")

    # 温度参数影响分析
    print("\n[10] Temperature effect analysis...")
    temperatures = [0.1, 0.5, 1.0, 2.0]
    sample_proj = torch.randn(10, PROJECTION_DIM)

    for temp in temperatures:
        loss_fn = SimCLRLoss(temperature=temp)
        proj1 = sample_proj[:5]
        proj2 = sample_proj[5:]
        loss = loss_fn(proj1, proj2)
        print(f"    Temperature {temp}: Loss={loss.item():.4f}")

    print("\n" + "=" * 70)
    print("Demo completed successfully!")
    print("=" * 70)

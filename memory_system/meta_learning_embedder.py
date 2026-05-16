"""
元学习自适应嵌入系统 (Meta-Learning Adaptive Embedder)

基于MAML（模型无关元学习）的自适应嵌入系统，支持少样本快速适应新领域，
实现跨领域迁移学习和自适应学习率调度。

Author: Agent Memory System V3.0
Date: 2026-05-16
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np
from typing import Dict, List, Tuple, Optional, Union, Callable, Any, Iterator
from dataclasses import dataclass, field
from collections import defaultdict
from copy import deepcopy
import random
import math
from abc import ABC, abstractmethod
import warnings
import json
import pickle

warnings.filterwarnings('ignore')


@dataclass
class Task:
    """元学习任务定义"""
    task_id: str
    support_set: Tuple[torch.Tensor, torch.Tensor]  # (x_support, y_support)
    query_set: Tuple[torch.Tensor, torch.Tensor]    # (x_query, y_query)
    task_type: str = "classification"
    domain: str = "default"

    def to_device(self, device: str) -> 'Task':
        """将任务数据移动到指定设备"""
        x_s, y_s = self.support_set
        x_q, y_q = self.query_set
        return Task(
            task_id=self.task_id,
            support_set=(x_s.to(device), y_s.to(device)),
            query_set=(x_q.to(device), y_q.to(device)),
            task_type=self.task_type,
            domain=self.domain
        )


@dataclass
class MetaBatch:
    """元学习批次"""
    tasks: List[Task]
    batch_size: int

    def __iter__(self) -> Iterator[Task]:
        return iter(self.tasks)


class AdaptiveLearningRateScheduler:
    """
    自适应学习率调度器
    基于任务难度和训练进度动态调整学习率
    """

    def __init__(
        self,
        optimizer: optim.Optimizer,
        initial_lr: float = 0.001,
        min_lr: float = 1e-6,
        warmup_steps: int = 100,
        decay_rate: float = 0.95,
        decay_steps: int = 1000,
        adaptive_factor: float = 0.1
    ):
        self.optimizer = optimizer
        self.initial_lr = initial_lr
        self.min_lr = min_lr
        self.warmup_steps = warmup_steps
        self.decay_rate = decay_rate
        self.decay_steps = decay_steps
        self.adaptive_factor = adaptive_factor

        self.step_count = 0
        self.task_losses: List[float] = []
        self.lr_history: List[float] = []

    def step(self, loss: Optional[float] = None) -> float:
        """
        更新学习率

        Args:
            loss: 当前损失值（用于自适应调整）

        Returns:
            当前学习率
        """
        self.step_count += 1

        # 预热阶段
        if self.step_count < self.warmup_steps:
            lr = self.initial_lr * (self.step_count / self.warmup_steps)
        else:
            # 指数衰减
            decay_epochs = (self.step_count - self.warmup_steps) / self.decay_steps
            lr = self.initial_lr * (self.decay_rate ** decay_epochs)

        # 自适应调整
        if loss is not None and len(self.task_losses) > 0:
            avg_loss = np.mean(self.task_losses[-10:])
            if loss > avg_loss * 1.2:  # 损失增加，降低学习率
                lr *= (1 - self.adaptive_factor)
            elif loss < avg_loss * 0.8:  # 损失减少，可适当提高
                lr *= (1 + self.adaptive_factor * 0.5)

        lr = max(lr, self.min_lr)

        # 更新优化器学习率
        for param_group in self.optimizer.param_groups:
            param_group['lr'] = lr

        if loss is not None:
            self.task_losses.append(loss)
        self.lr_history.append(lr)

        return lr

    def get_lr(self) -> float:
        """获取当前学习率"""
        return self.optimizer.param_groups[0]['lr']

    def reset(self) -> None:
        """重置调度器状态"""
        self.step_count = 0
        self.task_losses = []
        self.lr_history = []


class MemoryEmbedderNetwork(nn.Module):
    """
    记忆嵌入网络
    可配置的神经网络架构，用于元学习
    """

    def __init__(
        self,
        input_dim: int,
        hidden_dims: List[int],
        output_dim: int,
        dropout: float = 0.1,
        use_batch_norm: bool = True,
        activation: str = "relu"
    ):
        super().__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim

        # 构建网络层
        layers = []
        prev_dim = input_dim

        for i, hidden_dim in enumerate(hidden_dims):
            layers.append(nn.Linear(prev_dim, hidden_dim))

            if use_batch_norm:
                layers.append(nn.BatchNorm1d(hidden_dim))

            # 激活函数
            if activation == "relu":
                layers.append(nn.ReLU(inplace=True))
            elif activation == "leaky_relu":
                layers.append(nn.LeakyReLU(0.1, inplace=True))
            elif activation == "elu":
                layers.append(nn.ELU(inplace=True))
            elif activation == "gelu":
                layers.append(nn.GELU())

            layers.append(nn.Dropout(dropout))
            prev_dim = hidden_dim

        # 输出层
        layers.append(nn.Linear(prev_dim, output_dim))

        self.network = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """前向传播"""
        return self.network(x)


class MAML(nn.Module):
    """
    MAML (Model-Agnostic Meta-Learning) 实现
    模型无关元学习算法
    """

    def __init__(
        self,
        model: nn.Module,
        inner_lr: float = 0.01,
        num_inner_steps: int = 5,
        first_order: bool = False
    ):
        super().__init__()
        self.model = model
        self.inner_lr = inner_lr
        self.num_inner_steps = num_inner_steps
        self.first_order = first_order

    def inner_loop(
        self,
        task: Task,
        create_graph: bool = False
    ) -> nn.Module:
        """
        内循环适应

        Args:
            task: 元学习任务
            create_graph: 是否创建计算图（用于二阶导数）

        Returns:
            适应后的模型
        """
        # 克隆模型
        adapted_model = deepcopy(self.model)

        x_support, y_support = task.support_set

        # 内循环梯度下降
        for step in range(self.num_inner_steps):
            adapted_model.train()
            support_loss = self.compute_loss(adapted_model, x_support, y_support, task.task_type)

            # 计算梯度
            grads = torch.autograd.grad(
                support_loss,
                adapted_model.parameters(),
                create_graph=create_graph and not self.first_order,
                retain_graph=True,
                allow_unused=True
            )

            # 手动更新参数
            with torch.no_grad():
                for param, grad in zip(adapted_model.parameters(), grads):
                    if grad is not None:
                        param.data = param.data - self.inner_lr * grad

        return adapted_model

    def compute_loss(
        self,
        model: nn.Module,
        x: torch.Tensor,
        y: torch.Tensor,
        task_type: str
    ) -> torch.Tensor:
        """计算任务损失"""
        predictions = model(x)

        if task_type == "classification":
            loss = F.cross_entropy(predictions, y)
        elif task_type == "regression":
            loss = F.mse_loss(predictions, y)
        elif task_type == "embedding":
            # 对比损失
            loss = self._contrastive_loss(predictions, y)
        else:
            loss = F.cross_entropy(predictions, y)

        return loss

    def _contrastive_loss(
        self,
        embeddings: torch.Tensor,
        labels: torch.Tensor,
        margin: float = 1.0
    ) -> torch.Tensor:
        """对比损失"""
        # 计算 pairwise 距离
        distances = torch.cdist(embeddings, embeddings)

        # 创建标签掩码
        labels_eq = (labels.unsqueeze(0) == labels.unsqueeze(1)).float()

        # 正样本对损失
        positive_loss = (labels_eq * distances.pow(2)).sum() / (labels_eq.sum() + 1e-8)

        # 负样本对损失
        negative_loss = ((1 - labels_eq) * F.relu(margin - distances).pow(2)).sum()
        negative_loss /= ((1 - labels_eq).sum() + 1e-8)

        return positive_loss + negative_loss

    def forward(self, meta_batch: MetaBatch) -> Tuple[torch.Tensor, Dict[str, float]]:
        """
        前向传播（元训练）

        Args:
            meta_batch: 元学习批次

        Returns:
            元损失和指标
        """
        meta_losses = []
        accuracies = []

        for task in meta_batch.tasks:
            # 内循环适应
            adapted_model = self.inner_loop(task, create_graph=True)

            # 外循环评估
            x_query, y_query = task.query_set
            query_loss = self.compute_loss(adapted_model, x_query, y_query, task.task_type)
            meta_losses.append(query_loss)

            # 计算准确率
            with torch.no_grad():
                predictions = adapted_model(x_query)
                if task.task_type == "classification":
                    pred_labels = predictions.argmax(dim=1)
                    accuracy = (pred_labels == y_query).float().mean().item()
                    accuracies.append(accuracy)

        # 平均元损失
        meta_loss = torch.stack(meta_losses).mean()

        metrics = {
            'meta_loss': meta_loss.item(),
            'avg_accuracy': np.mean(accuracies) if accuracies else 0.0
        }

        return meta_loss, metrics

    def adapt_to_task(self, task: Task) -> nn.Module:
        """
        适应到新任务（推理时使用）

        Args:
            task: 新任务

        Returns:
            适应后的模型
        """
        return self.inner_loop(task, create_graph=False)


class PrototypicalNetwork(nn.Module):
    """
    原型网络 (Prototypical Networks)
    基于度量的元学习方法
    """

    def __init__(
        self,
        encoder: nn.Module,
        distance_metric: str = "euclidean"
    ):
        super().__init__()
        self.encoder = encoder
        self.distance_metric = distance_metric
        self.prototypes: Optional[torch.Tensor] = None
        self.prototype_labels: Optional[torch.Tensor] = None

    def compute_prototypes(
        self,
        x: torch.Tensor,
        y: torch.Tensor
    ) -> torch.Tensor:
        """
        计算类别原型

        Args:
            x: 支持集特征
            y: 支持集标签

        Returns:
            类别原型 [num_classes, feature_dim]
        """
        embeddings = self.encoder(x)
        unique_labels = torch.unique(y)

        prototypes = []
        for label in unique_labels:
            mask = (y == label)
            class_embeddings = embeddings[mask]
            prototype = class_embeddings.mean(dim=0)
            prototypes.append(prototype)

        return torch.stack(prototypes)

    def compute_distances(
        self,
        embeddings: torch.Tensor,
        prototypes: torch.Tensor
    ) -> torch.Tensor:
        """计算到原型的距离"""
        if self.distance_metric == "euclidean":
            # 欧氏距离
            distances = torch.cdist(embeddings, prototypes)
        elif self.distance_metric == "cosine":
            # 余弦距离
            embeddings_norm = F.normalize(embeddings, dim=1)
            prototypes_norm = F.normalize(prototypes, dim=1)
            similarities = torch.mm(embeddings_norm, prototypes_norm.t())
            distances = 1 - similarities
        else:
            distances = torch.cdist(embeddings, prototypes)

        return distances

    def forward(
        self,
        x_support: torch.Tensor,
        y_support: torch.Tensor,
        x_query: torch.Tensor
    ) -> torch.Tensor:
        """
        前向传播

        Args:
            x_support: 支持集输入
            y_support: 支持集标签
            x_query: 查询集输入

        Returns:
            查询集的类别概率分布
        """
        # 计算原型
        prototypes = self.compute_prototypes(x_support, y_support)

        # 编码查询集
        query_embeddings = self.encoder(x_query)

        # 计算距离
        distances = self.compute_distances(query_embeddings, prototypes)

        # 转换为概率（使用负距离作为logits）
        logits = -distances

        return logits

    def predict(
        self,
        x: torch.Tensor,
        return_probabilities: bool = False
    ) -> Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]]:
        """
        预测新样本

        Args:
            x: 输入样本
            return_probabilities: 是否返回概率

        Returns:
            预测标签，可选概率
        """
        if self.prototypes is None:
            raise RuntimeError("Prototypes not computed. Call fit() first.")

        embeddings = self.encoder(x)
        distances = self.compute_distances(embeddings, self.prototypes)
        logits = -distances
        probabilities = F.softmax(logits, dim=1)
        predictions = probabilities.argmax(dim=1)

        if return_probabilities:
            return predictions, probabilities
        return predictions

    def fit(self, x: torch.Tensor, y: torch.Tensor) -> None:
        """拟合支持集"""
        self.prototypes = self.compute_prototypes(x, y)
        self.prototype_labels = torch.unique(y)


class MetaLearningEmbedder(nn.Module):
    """
    元学习自适应嵌入系统主类
    整合MAML、原型网络等方法
    """

    def __init__(
        self,
        input_dim: int,
        hidden_dims: List[int] = [256, 256],
        embedding_dim: int = 128,
        num_classes: int = 5,
        meta_lr: float = 0.001,
        inner_lr: float = 0.01,
        num_inner_steps: int = 5,
        method: str = "maml",
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        super().__init__()
        self.input_dim = input_dim
        self.embedding_dim = embedding_dim
        self.num_classes = num_classes
        self.method = method
        self.device = device

        # 构建编码器
        self.encoder = MemoryEmbedderNetwork(
            input_dim=input_dim,
            hidden_dims=hidden_dims,
            output_dim=embedding_dim,
            dropout=0.1
        ).to(device)

        # 分类头
        self.classifier = nn.Linear(embedding_dim, num_classes).to(device)

        # 完整模型
        self.model = nn.Sequential(self.encoder, self.classifier)

        # 元学习方法
        if method == "maml":
            self.meta_learner = MAML(
                model=self.model,
                inner_lr=inner_lr,
                num_inner_steps=num_inner_steps
            )
        elif method == "prototypical":
            self.meta_learner = PrototypicalNetwork(
                encoder=self.encoder,
                distance_metric="euclidean"
            )
        else:
            raise ValueError(f"Unknown method: {method}")

        # 优化器
        self.meta_optimizer = optim.Adam(
            self.model.parameters(),
            lr=meta_lr,
            weight_decay=1e-4
        )

        # 学习率调度器
        self.lr_scheduler = AdaptiveLearningRateScheduler(
            optimizer=self.meta_optimizer,
            initial_lr=meta_lr
        )

        # 领域统计
        self.domain_stats: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.task_history: List[Task] = []

    def meta_train_step(
        self,
        meta_batch: MetaBatch,
        log_metrics: bool = True
    ) -> Dict[str, float]:
        """
        元训练步骤

        Args:
            meta_batch: 元学习批次
            tasks: 任务列表
            log_metrics: 是否记录指标

        Returns:
            训练指标
        """
        # 将任务移动到设备
        tasks_on_device = [task.to_device(self.device) for task in meta_batch.tasks]
        meta_batch_device = MetaBatch(tasks=tasks_on_device, batch_size=meta_batch.batch_size)

        self.meta_optimizer.zero_grad()

        if self.method == "maml":
            meta_loss, metrics = self.meta_learner(meta_batch_device)
            meta_loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
            self.meta_optimizer.step()
        else:
            # 原型网络训练
            losses = []
            accuracies = []

            for task in tasks_on_device:
                x_s, y_s = task.support_set
                x_q, y_q = task.query_set

                logits = self.meta_learner(x_s, y_s, x_q)
                loss = F.cross_entropy(logits, y_q)
                losses.append(loss)

                with torch.no_grad():
                    pred = logits.argmax(dim=1)
                    acc = (pred == y_q).float().mean().item()
                    accuracies.append(acc)

            meta_loss = torch.stack(losses).mean()
            meta_loss.backward()
            self.meta_optimizer.step()

            metrics = {
                'meta_loss': meta_loss.item(),
                'avg_accuracy': np.mean(accuracies)
            }

        # 更新学习率
        self.lr_scheduler.step(metrics['meta_loss'])

        # 记录任务历史
        self.task_history.extend(meta_batch.tasks)

        if log_metrics:
            metrics['learning_rate'] = self.lr_scheduler.get_lr()

        return metrics

    def few_shot_adapt(
        self,
        support_x: torch.Tensor,
        support_y: torch.Tensor,
        num_steps: Optional[int] = None
    ) -> nn.Module:
        """
        少样本适应新任务

        Args:
            support_x: 支持集输入
            support_y: 支持集标签
            num_steps: 适应步数

        Returns:
            适应后的模型
        """
        support_x = support_x.to(self.device)
        support_y = support_y.to(self.device)

        if self.method == "maml":
            task = Task(
                task_id="adaptation",
                support_set=(support_x, support_y),
                query_set=(support_x, support_y),  # 虚拟查询集
                task_type="classification"
            )
            adapted_model = self.meta_learner.adapt_to_task(task)
            return adapted_model
        else:
            # 原型网络直接拟合
            self.meta_learner.fit(support_x, support_y)
            return self.meta_learner

    def predict(
        self,
        x: torch.Tensor,
        adapted_model: Optional[nn.Module] = None
    ) -> torch.Tensor:
        """
        预测

        Args:
            x: 输入
            adapted_model: 适应后的模型（可选）

        Returns:
            预测结果
        """
        x = x.to(self.device)

        if adapted_model is not None:
            with torch.no_grad():
                return adapted_model(x)
        else:
            with torch.no_grad():
                return self.model(x)

    def cross_domain_transfer(
        self,
        source_domain: str,
        target_domain: str,
        target_support_set: Tuple[torch.Tensor, torch.Tensor]
    ) -> nn.Module:
        """
        跨领域迁移学习

        Args:
            source_domain: 源领域名称
            target_domain: 目标领域名称
            target_support_set: 目标领域支持集

        Returns:
            迁移后的模型
        """
        # 记录领域统计
        if source_domain in self.domain_stats:
            source_stats = self.domain_stats[source_domain]
        else:
            source_stats = {}

        # 快速适应目标领域
        adapted_model = self.few_shot_adapt(
            target_support_set[0],
            target_support_set[1]
        )

        # 更新目标领域统计
        self.domain_stats[target_domain]['adapted_from'] = source_domain
        self.domain_stats[target_domain]['support_size'] = len(target_support_set[0])

        return adapted_model

    def get_domain_embedding(
        self,
        domain_name: str,
        samples: torch.Tensor
    ) -> torch.Tensor:
        """
        获取领域嵌入

        Args:
            domain_name: 领域名称
            samples: 领域样本

        Returns:
            领域嵌入向量
        """
        samples = samples.to(self.device)

        with torch.no_grad():
            embeddings = self.encoder(samples)
            domain_embedding = embeddings.mean(dim=0)

        self.domain_stats[domain_name]['embedding'] = domain_embedding.cpu()

        return domain_embedding

    def compute_domain_similarity(
        self,
        domain1: str,
        domain2: str
    ) -> float:
        """计算领域间相似度"""
        if domain1 not in self.domain_stats or domain2 not in self.domain_stats:
            return 0.0

        emb1 = self.domain_stats[domain1].get('embedding')
        emb2 = self.domain_stats[domain2].get('embedding')

        if emb1 is None or emb2 is None:
            return 0.0

        # 余弦相似度
        similarity = F.cosine_similarity(emb1.unsqueeze(0), emb2.unsqueeze(0))
        return similarity.item()

    def save(self, path: str) -> None:
        """保存模型"""
        checkpoint = {
            'model_state': self.model.state_dict(),
            'encoder_state': self.encoder.state_dict(),
            'classifier_state': self.classifier.state_dict(),
            'meta_optimizer_state': self.meta_optimizer.state_dict(),
            'domain_stats': self.domain_stats,
            'config': {
                'input_dim': self.input_dim,
                'embedding_dim': self.embedding_dim,
                'num_classes': self.num_classes,
                'method': self.method
            }
        }
        torch.save(checkpoint, path)

    def load(self, path: str) -> None:
        """加载模型"""
        checkpoint = torch.load(path, map_location=self.device)

        self.model.load_state_dict(checkpoint['model_state'])
        self.encoder.load_state_dict(checkpoint['encoder_state'])
        self.classifier.load_state_dict(checkpoint['classifier_state'])
        self.meta_optimizer.load_state_dict(checkpoint['meta_optimizer_state'])
        self.domain_stats = checkpoint['domain_stats']


def generate_synthetic_task(
    input_dim: int,
    num_classes: int,
    support_size: int = 25,
    query_size: int = 15,
    device: str = "cpu"
) -> Task:
    """
    生成合成元学习任务

    Args:
        input_dim: 输入维度
        num_classes: 类别数
        support_size: 支持集大小
        query_size: 查询集大小
        device: 设备

    Returns:
        合成任务
    """
    # 随机生成类别中心
    class_centers = torch.randn(num_classes, input_dim).to(device)

    # 生成支持集
    support_labels = torch.randint(0, num_classes, (support_size,)).to(device)
    support_data = []
    for label in support_labels:
        center = class_centers[label]
        sample = center + 0.1 * torch.randn(input_dim).to(device)
        support_data.append(sample)
    support_data = torch.stack(support_data)

    # 生成查询集
    query_labels = torch.randint(0, num_classes, (query_size,)).to(device)
    query_data = []
    for label in query_labels:
        center = class_centers[label]
        sample = center + 0.1 * torch.randn(input_dim).to(device)
        query_data.append(sample)
    query_data = torch.stack(query_data)

    return Task(
        task_id=f"task_{random.randint(0, 10000)}",
        support_set=(support_data, support_labels),
        query_set=(query_data, query_labels),
        task_type="classification"
    )


def train_meta_embedder(
    embedder: MetaLearningEmbedder,
    num_iterations: int = 1000,
    tasks_per_iteration: int = 4,
    eval_interval: int = 100
) -> Dict[str, List[float]]:
    """
    训练元学习嵌入器

    Args:
        embedder: 元学习嵌入器
        num_iterations: 迭代次数
        tasks_per_iteration: 每轮任务数
        eval_interval: 评估间隔

    Returns:
        训练历史
    """
    history = {
        'meta_loss': [],
        'accuracy': [],
        'learning_rate': []
    }

    for iteration in range(num_iterations):
        # 生成任务批次
        tasks = [
            generate_synthetic_task(
                input_dim=embedder.input_dim,
                num_classes=embedder.num_classes,
                device=embedder.device
            )
            for _ in range(tasks_per_iteration)
        ]

        meta_batch = MetaBatch(tasks=tasks, batch_size=tasks_per_iteration)

        # 元训练步骤
        metrics = embedder.meta_train_step(meta_batch)

        history['meta_loss'].append(metrics['meta_loss'])
        history['accuracy'].append(metrics['avg_accuracy'])
        history['learning_rate'].append(metrics.get('learning_rate', 0))

        if (iteration + 1) % eval_interval == 0:
            print(f"Iteration {iteration + 1}/{num_iterations}, "
                  f"Loss: {metrics['meta_loss']:.4f}, "
                  f"Acc: {metrics['avg_accuracy']:.4f}, "
                  f"LR: {metrics.get('learning_rate', 0):.6f}")

    return history


def evaluate_few_shot(
    embedder: MetaLearningEmbedder,
    num_tasks: int = 100,
    shots: int = 5
) -> Dict[str, float]:
    """
    评估少样本学习能力

    Args:
        embedder: 元学习嵌入器
        num_tasks: 评估任务数
        shots: 每类样本数

    Returns:
        评估指标
    """
    accuracies = []

    for _ in range(num_tasks):
        # 生成评估任务
        task = generate_synthetic_task(
            input_dim=embedder.input_dim,
            num_classes=embedder.num_classes,
            support_size=shots * embedder.num_classes,
            query_size=50,
            device=embedder.device
        )

        # 适应
        adapted_model = embedder.few_shot_adapt(
            task.support_set[0],
            task.support_set[1]
        )

        # 评估
        with torch.no_grad():
            if embedder.method == "maml":
                predictions = adapted_model(task.query_set[0])
                pred_labels = predictions.argmax(dim=1)
            else:
                pred_labels = embedder.meta_learner.predict(task.query_set[0])

            accuracy = (pred_labels == task.query_set[1]).float().mean().item()
            accuracies.append(accuracy)

    return {
        'mean_accuracy': np.mean(accuracies),
        'std_accuracy': np.std(accuracies),
        'shots': shots
    }


# 使用示例
if __name__ == "__main__":
    print("=" * 70)
    print("Meta-Learning Adaptive Embedder Demo")
    print("=" * 70)

    # 配置参数
    INPUT_DIM = 128
    EMBEDDING_DIM = 64
    NUM_CLASSES = 5
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

    print(f"\nDevice: {DEVICE}")

    # 初始化MAML嵌入器
    print("\n[1] Initializing MAML Meta-Learner...")
    maml_embedder = MetaLearningEmbedder(
        input_dim=INPUT_DIM,
        hidden_dims=[256, 256],
        embedding_dim=EMBEDDING_DIM,
        num_classes=NUM_CLASSES,
        meta_lr=0.001,
        inner_lr=0.01,
        num_inner_steps=5,
        method="maml",
        device=DEVICE
    )
    print(f"    Parameters: {sum(p.numel() for p in maml_embedder.parameters()):,}")

    # 训练MAML
    print("\n[2] Training MAML...")
    history = train_meta_embedder(
        maml_embedder,
        num_iterations=200,
        tasks_per_iteration=4
    )
    print(f"    Final loss: {history['meta_loss'][-1]:.4f}")

    # 评估少样本学习
    print("\n[3] Evaluating few-shot learning (MAML)...")
    for shots in [1, 5, 10]:
        results = evaluate_few_shot(maml_embedder, num_tasks=50, shots=shots)
        print(f"    {shots}-shot: {results['mean_accuracy']:.4f} (+/- {results['std_accuracy']:.4f})")

    # 初始化原型网络
    print("\n[4] Initializing Prototypical Network...")
    proto_embedder = MetaLearningEmbedder(
        input_dim=INPUT_DIM,
        hidden_dims=[256, 256],
        embedding_dim=EMBEDDING_DIM,
        num_classes=NUM_CLASSES,
        method="prototypical",
        device=DEVICE
    )

    # 训练原型网络
    print("\n[5] Training Prototypical Network...")
    history_proto = train_meta_embedder(
        proto_embedder,
        num_iterations=200,
        tasks_per_iteration=4
    )

    # 评估原型网络
    print("\n[6] Evaluating few-shot learning (Prototypical)...")
    for shots in [1, 5, 10]:
        results = evaluate_few_shot(proto_embedder, num_tasks=50, shots=shots)
        print(f"    {shots}-shot: {results['mean_accuracy']:.4f} (+/- {results['std_accuracy']:.4f})")

    # 快速适应演示
    print("\n[7] Demonstrating fast adaptation...")
    test_task = generate_synthetic_task(
        input_dim=INPUT_DIM,
        num_classes=NUM_CLASSES,
        support_size=25,
        query_size=50,
        device=DEVICE
    )

    # 适应前
    with torch.no_grad():
        logits_before = maml_embedder.model(test_task.query_set[0])
        acc_before = (logits_before.argmax(dim=1) == test_task.query_set[1]).float().mean().item()

    # 适应后
    adapted = maml_embedder.few_shot_adapt(
        test_task.support_set[0],
        test_task.support_set[1]
    )
    with torch.no_grad():
        logits_after = adapted(test_task.query_set[0])
        acc_after = (logits_after.argmax(dim=1) == test_task.query_set[1]).float().mean().item()

    print(f"    Accuracy before adaptation: {acc_before:.4f}")
    print(f"    Accuracy after adaptation: {acc_after:.4f}")
    print(f"    Improvement: {acc_after - acc_before:.4f}")

    # 跨领域迁移
    print("\n[8] Cross-domain transfer learning...")
    source_samples = torch.randn(100, INPUT_DIM).to(DEVICE)
    maml_embedder.get_domain_embedding("source_domain", source_samples)

    target_support = (
        torch.randn(25, INPUT_DIM).to(DEVICE),
        torch.randint(0, NUM_CLASSES, (25,)).to(DEVICE)
    )
    transferred_model = maml_embedder.cross_domain_transfer(
        "source_domain",
        "target_domain",
        target_support
    )
    print(f"    Transferred model from source to target domain")

    # 领域相似度
    target_samples = torch.randn(100, INPUT_DIM).to(DEVICE)
    maml_embedder.get_domain_embedding("target_domain", target_samples)
    similarity = maml_embedder.compute_domain_similarity("source_domain", "target_domain")
    print(f"    Domain similarity: {similarity:.4f}")

    # 保存和加载
    print("\n[9] Saving and loading model...")
    save_path = "/tmp/meta_learning_embedder.pt"
    maml_embedder.save(save_path)

    new_embedder = MetaLearningEmbedder(
        input_dim=INPUT_DIM,
        embedding_dim=EMBEDDING_DIM,
        num_classes=NUM_CLASSES,
        method="maml",
        device=DEVICE
    )
    new_embedder.load(save_path)
    print(f"    Model saved and loaded successfully")

    # 学习率调度演示
    print("\n[10] Adaptive learning rate scheduler...")
    for i in range(5):
        loss = 0.5 * (0.9 ** i) + 0.1 * random.random()
        lr = maml_embedder.lr_scheduler.step(loss)
        print(f"    Step {i+1}: Loss={loss:.4f}, LR={lr:.6f}")

    print("\n" + "=" * 70)
    print("Demo completed successfully!")
    print("=" * 70)

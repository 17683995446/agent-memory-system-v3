"""
神经架构搜索模块 (Neural Architecture Search Module)

基于DARTS的可微分架构搜索，实现记忆编码器的自动架构优化，
支持多目标优化（准确率vs延迟）和架构性能预测。

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
from collections import defaultdict
from copy import deepcopy
import random
import json
import warnings
from abc import ABC, abstractmethod

warnings.filterwarnings('ignore')


class OperationType:
    """操作类型定义"""
    CONV_3X3 = "conv_3x3"
    CONV_5X5 = "conv_5x5"
    DIL_CONV_3X3 = "dil_conv_3x3"
    DIL_CONV_5X5 = "dil_conv_5x5"
    MAX_POOL_3X3 = "max_pool_3x3"
    AVG_POOL_3X3 = "avg_pool_3x3"
    SKIP_CONNECTION = "skip_connection"
    ZERO = "zero"
    IDENTITY = "identity"

    PRIMITIVES = [
        CONV_3X3, CONV_5X5, DIL_CONV_3X3, DIL_CONV_5X5,
        MAX_POOL_3X3, AVG_POOL_3X3, SKIP_CONNECTION, ZERO
    ]


class MixedOperation(nn.Module):
    """
    混合操作
    DARTS中的核心组件，包含所有候选操作的加权和
    """

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        stride: int = 1,
        operations: Optional[List[str]] = None
    ):
        super().__init__()
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.stride = stride
        self.operations = operations or OperationType.PRIMITIVES

        # 创建候选操作
        self.ops = nn.ModuleList()
        for op_name in self.operations:
            op = self._create_operation(op_name, in_channels, out_channels, stride)
            self.ops.append(op)

        # 架构参数（可学习）
        self.architecture_params = nn.Parameter(
            torch.randn(len(self.operations)) * 0.001
        )

    def _create_operation(
        self,
        op_name: str,
        in_ch: int,
        out_ch: int,
        stride: int
    ) -> nn.Module:
        """创建具体操作"""
        if op_name == OperationType.CONV_3X3:
            return nn.Sequential(
                nn.ReLU(inplace=False),
                nn.Conv2d(in_ch, out_ch, 3, stride, 1, bias=False),
                nn.BatchNorm2d(out_ch)
            )
        elif op_name == OperationType.CONV_5X5:
            return nn.Sequential(
                nn.ReLU(inplace=False),
                nn.Conv2d(in_ch, out_ch, 5, stride, 2, bias=False),
                nn.BatchNorm2d(out_ch)
            )
        elif op_name == OperationType.DIL_CONV_3X3:
            return nn.Sequential(
                nn.ReLU(inplace=False),
                nn.Conv2d(in_ch, out_ch, 3, stride, 2, dilation=2, bias=False),
                nn.BatchNorm2d(out_ch)
            )
        elif op_name == OperationType.DIL_CONV_5X5:
            return nn.Sequential(
                nn.ReLU(inplace=False),
                nn.Conv2d(in_ch, out_ch, 5, stride, 4, dilation=2, bias=False),
                nn.BatchNorm2d(out_ch)
            )
        elif op_name == OperationType.MAX_POOL_3X3:
            return nn.Sequential(
                nn.MaxPool2d(3, stride, 1),
                nn.BatchNorm2d(out_ch) if in_ch != out_ch else nn.Identity()
            )
        elif op_name == OperationType.AVG_POOL_3X3:
            return nn.Sequential(
                nn.AvgPool2d(3, stride, 1),
                nn.BatchNorm2d(out_ch) if in_ch != out_ch else nn.Identity()
            )
        elif op_name == OperationType.SKIP_CONNECTION:
            if stride == 1 and in_ch == out_ch:
                return nn.Identity()
            else:
                return nn.Sequential(
                    nn.Conv2d(in_ch, out_ch, 1, stride, bias=False),
                    nn.BatchNorm2d(out_ch)
                )
        elif op_name == OperationType.ZERO:
            return ZeroOperation(stride)
        else:
            raise ValueError(f"Unknown operation: {op_name}")

    def forward(self, x: torch.Tensor, weights: Optional[torch.Tensor] = None) -> torch.Tensor:
        """
        前向传播

        Args:
            x: 输入特征
            weights: 操作权重（用于离散化后的架构）

        Returns:
            混合操作输出
        """
        if weights is None:
            # 使用softmax权重进行连续松弛
            weights = F.softmax(self.architecture_params, dim=0)

        # 加权求和所有操作
        output = sum(w * op(x) for w, op in zip(weights, self.ops))
        return output

    def get_architecture_weights(self) -> torch.Tensor:
        """获取架构权重"""
        return F.softmax(self.architecture_params, dim=0)


class ZeroOperation(nn.Module):
    """零操作"""

    def __init__(self, stride: int = 1):
        super().__init__()
        self.stride = stride

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if self.stride == 1:
            return torch.zeros_like(x)
        else:
            return torch.zeros(
                x.size(0), x.size(1),
                x.size(2) // self.stride,
                x.size(3) // self.stride,
                device=x.device
            )


class Cell(nn.Module):
    """
    DARTS搜索单元
    包含多个节点的有向无环图
    """

    def __init__(
        self,
        num_nodes: int,
        in_channels: int,
        out_channels: int,
        reduction: bool = False,
        reduction_prev: bool = False
    ):
        super().__init__()
        self.num_nodes = num_nodes
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.reduction = reduction
        self.reduction_prev = reduction_prev

        # 预处理层
        if reduction_prev:
            self.preprocess0 = nn.Sequential(
                nn.Conv2d(in_channels, out_channels, 1, stride=2, bias=False),
                nn.BatchNorm2d(out_channels)
            )
        else:
            self.preprocess0 = nn.Sequential(
                nn.Conv2d(in_channels, out_channels, 1, bias=False),
                nn.BatchNorm2d(out_channels)
            )

        self.preprocess1 = nn.Sequential(
            nn.Conv2d(in_channels, out_channels, 1, bias=False),
            nn.BatchNorm2d(out_channels)
        )

        # 创建节点
        self.edges = nn.ModuleDict()
        for i in range(2, num_nodes + 2):  # 节点2到num_nodes+1
            for j in range(i):  # 从所有前序节点连接
                stride = 2 if reduction and j < 2 else 1
                edge_key = f"{j}->{i}"
                self.edges[edge_key] = MixedOperation(
                    out_channels, out_channels, stride
                )

    def forward(
        self,
        s0: torch.Tensor,
        s1: torch.Tensor,
        edge_weights: Optional[Dict[str, torch.Tensor]] = None
    ) -> torch.Tensor:
        """前向传播"""
        s0 = self.preprocess0(s0)
        s1 = self.preprocess1(s1)

        # 存储每个节点的输入
        states = [s0, s1]

        # 计算每个节点的输出
        for i in range(2, self.num_nodes + 2):
            node_input = []
            for j in range(i):
                edge_key = f"{j}->{i}"
                edge = self.edges[edge_key]
                weight = edge_weights.get(edge_key) if edge_weights else None
                node_input.append(edge(states[j], weight))

            # 节点输出是所有输入的和
            node_output = sum(node_input)
            states.append(node_output)

        # 返回最后两个节点的拼接
        return torch.cat(states[-2:], dim=1)


class SearchSpace:
    """
    架构搜索空间定义
    """

    def __init__(
        self,
        num_cells: int = 8,
        num_nodes: int = 4,
        init_channels: int = 16,
        num_classes: int = 10
    ):
        self.num_cells = num_cells
        self.num_nodes = num_nodes
        self.init_channels = init_channels
        self.num_classes = num_classes

        # 定义搜索空间约束
        self.constraints = {
            'max_params': 10e6,  # 最大参数量
            'max_flops': 600e6,  # 最大FLOPs
            'max_latency': 100,   # 最大延迟(ms)
            'min_accuracy': 0.9   # 最小准确率
        }

    def is_valid_architecture(self, architecture: Dict[str, Any]) -> bool:
        """检查架构是否满足约束"""
        # 检查参数量
        if 'params' in architecture and architecture['params'] > self.constraints['max_params']:
            return False

        # 检查FLOPs
        if 'flops' in architecture and architecture['flops'] > self.constraints['max_flops']:
            return False

        return True


class ArchitecturePerformancePredictor(nn.Module):
    """
    架构性能预测器
    使用神经网络预测架构的性能指标
    """

    def __init__(
        self,
        input_dim: int = 32,
        hidden_dims: List[int] = [128, 64, 32]
    ):
        super().__init__()

        layers = []
        prev_dim = input_dim
        for hidden_dim in hidden_dims:
            layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.ReLU(),
                nn.Dropout(0.1)
            ])
            prev_dim = hidden_dim

        # 多任务输出：准确率、延迟、参数量
        layers.append(nn.Linear(prev_dim, 3))

        self.network = nn.Sequential(*layers)

    def forward(self, architecture_encoding: torch.Tensor) -> torch.Tensor:
        """
        预测架构性能

        Returns:
            [accuracy, latency, params]
        """
        output = self.network(architecture_encoding)
        # 应用适当的激活函数
        accuracy = torch.sigmoid(output[:, 0])
        latency = torch.relu(output[:, 1])
        params = torch.relu(output[:, 2])

        return torch.stack([accuracy, latency, params], dim=1)

    def predict(self, architecture_encoding: torch.Tensor) -> Dict[str, float]:
        """预测并返回字典格式结果"""
        with torch.no_grad():
            predictions = self.forward(architecture_encoding)
            return {
                'accuracy': predictions[0, 0].item(),
                'latency': predictions[0, 1].item(),
                'params': predictions[0, 2].item()
            }


class DARTSSearchNetwork(nn.Module):
    """
    DARTS搜索网络
    包含可学习的架构参数
    """

    def __init__(
        self,
        input_channels: int = 3,
        init_channels: int = 16,
        num_classes: int = 10,
        num_cells: int = 8,
        num_nodes: int = 4
    ):
        super().__init__()
        self.num_cells = num_cells
        self.num_nodes = num_nodes
        self.init_channels = init_channels

        self.stem = nn.Sequential(
            nn.Conv2d(input_channels, init_channels, 3, padding=1, bias=False),
            nn.BatchNorm2d(init_channels)
        )

        # 创建cells
        self.cells = nn.ModuleList()
        channel = init_channels
        reduction_prev = False

        for i in range(num_cells):
            # 在1/3和2/3位置进行下采样
            reduction = (i in [num_cells // 3, 2 * num_cells // 3])
            if reduction:
                out_channels = channel * 2
            else:
                out_channels = channel

            cell = Cell(
                num_nodes=num_nodes,
                in_channels=channel,
                out_channels=out_channels,
                reduction=reduction,
                reduction_prev=reduction_prev
            )
            self.cells.append(cell)

            reduction_prev = reduction
            channel = out_channels

        self.global_pooling = nn.AdaptiveAvgPool2d(1)
        self.classifier = nn.Linear(channel * 2, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """前向传播"""
        s0 = s1 = self.stem(x)

        for cell in self.cells:
            s0, s1 = s1, cell(s0, s1)

        out = self.global_pooling(s1)
        out = out.view(out.size(0), -1)
        logits = self.classifier(out)
        return logits

    def get_architecture_weights(self) -> Dict[str, torch.Tensor]:
        """获取所有架构权重"""
        weights = {}
        for i, cell in enumerate(self.cells):
            for edge_key, edge in cell.edges.items():
                weights[f"cell_{i}_{edge_key}"] = edge.get_architecture_weights()
        return weights


class DARTSSearcher:
    """
    DARTS架构搜索器
    实现双层优化：架构参数和网络权重
    """

    def __init__(
        self,
        model: DARTSSearchNetwork,
        train_loader: DataLoader,
        val_loader: DataLoader,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        self.model = model.to(device)
        self.train_loader = train_loader
        self.val_loader = val_loader
        self.device = device

        # 分离架构参数和模型权重
        self.architecture_params = []
        self.model_weights = []

        for name, param in model.named_parameters():
            if 'architecture_params' in name:
                self.architecture_params.append(param)
            else:
                self.model_weights.append(param)

        # 优化器
        self.weight_optimizer = optim.SGD(
            self.model_weights,
            lr=0.025,
            momentum=0.9,
            weight_decay=3e-4
        )

        self.arch_optimizer = optim.Adam(
            self.architecture_params,
            lr=0.0003,
            betas=(0.5, 0.999),
            weight_decay=1e-3
        )

        self.scheduler = optim.lr_scheduler.CosineAnnealingLR(
            self.weight_optimizer, T_max=50
        )

        # 性能预测器
        self.performance_predictor = ArchitecturePerformancePredictor().to(device)
        self.predictor_optimizer = optim.Adam(
            self.performance_predictor.parameters(),
            lr=0.001
        )

    def train_step(self) -> Dict[str, float]:
        """单步训练"""
        self.model.train()

        # 获取批次数据
        try:
            train_inputs, train_targets = next(self.train_iter)
        except:
            self.train_iter = iter(self.train_loader)
            train_inputs, train_targets = next(self.train_iter)

        try:
            val_inputs, val_targets = next(self.val_iter)
        except:
            self.val_iter = iter(self.val_loader)
            val_inputs, val_targets = next(self.val_iter)

        train_inputs = train_inputs.to(self.device)
        train_targets = train_targets.to(self.device)
        val_inputs = val_inputs.to(self.device)
        val_targets = val_targets.to(self.device)

        # 步骤1：更新架构参数（在验证集上）
        self.arch_optimizer.zero_grad()
        val_logits = self.model(val_inputs)
        val_loss = F.cross_entropy(val_logits, val_targets)
        val_loss.backward()
        self.arch_optimizer.step()

        # 步骤2：更新模型权重（在训练集上）
        self.weight_optimizer.zero_grad()
        train_logits = self.model(train_inputs)
        train_loss = F.cross_entropy(train_logits, train_targets)
        train_loss.backward()
        self.weight_optimizer.step()

        # 计算准确率
        with torch.no_grad():
            train_acc = (train_logits.argmax(dim=1) == train_targets).float().mean().item()
            val_acc = (val_logits.argmax(dim=1) == val_targets).float().mean().item()

        return {
            'train_loss': train_loss.item(),
            'val_loss': val_loss.item(),
            'train_acc': train_acc,
            'val_acc': val_acc
        }

    def search(
        self,
        num_epochs: int = 50,
        log_interval: int = 10
    ) -> Dict[str, Any]:
        """
        执行架构搜索

        Args:
            num_epochs: 搜索轮数
            log_interval: 日志间隔

        Returns:
            搜索结果
        """
        history = {
            'train_loss': [],
            'val_loss': [],
            'train_acc': [],
            'val_acc': []
        }

        self.train_iter = iter(self.train_loader)
        self.val_iter = iter(self.val_loader)

        for epoch in range(num_epochs):
            metrics = self.train_step()

            history['train_loss'].append(metrics['train_loss'])
            history['val_loss'].append(metrics['val_loss'])
            history['train_acc'].append(metrics['train_acc'])
            history['val_acc'].append(metrics['val_acc'])

            self.scheduler.step()

            if (epoch + 1) % log_interval == 0:
                print(f"Epoch {epoch + 1}/{num_epochs}: "
                      f"Train Loss: {metrics['train_loss']:.4f}, "
                      f"Val Loss: {metrics['val_loss']:.4f}, "
                      f"Train Acc: {metrics['train_acc']:.4f}, "
                      f"Val Acc: {metrics['val_acc']:.4f}")

        # 获取最终架构
        architecture = self.derive_architecture()

        return {
            'history': history,
            'architecture': architecture
        }

    def derive_architecture(self, top_k: int = 2) -> Dict[str, Any]:
        """
        从连续松弛中派生离散架构

        Args:
            top_k: 每个节点选择的最强操作数

        Returns:
            离散架构描述
        """
        architecture = {
            'normal_cells': [],
            'reduction_cells': []
        }

        for i, cell in enumerate(self.model.cells):
            cell_type = 'reduction' if cell.reduction else 'normal'
            cell_arch = {'type': cell_type, 'edges': {}}

            for edge_key, edge in cell.edges.items():
                weights = edge.get_architecture_weights()
                # 选择top-k操作
                top_ops = torch.topk(weights, min(top_k, len(weights)))
                cell_arch['edges'][edge_key] = {
                    'operations': [edge.operations[idx] for idx in top_ops.indices.tolist()],
                    'weights': top_ops.values.tolist()
                }

            if cell_type == 'reduction':
                architecture['reduction_cells'].append(cell_arch)
            else:
                architecture['normal_cells'].append(cell_arch)

        return architecture

    def predict_performance(self, architecture_encoding: torch.Tensor) -> Dict[str, float]:
        """预测架构性能"""
        return self.performance_predictor.predict(architecture_encoding)

    def save(self, path: str) -> None:
        """保存搜索器状态"""
        torch.save({
            'model_state': self.model.state_dict(),
            'arch_optimizer': self.arch_optimizer.state_dict(),
            'weight_optimizer': self.weight_optimizer.state_dict(),
            'predictor_state': self.performance_predictor.state_dict()
        }, path)

    def load(self, path: str) -> None:
        """加载搜索器状态"""
        checkpoint = torch.load(path, map_location=self.device)
        self.model.load_state_dict(checkpoint['model_state'])
        self.arch_optimizer.load_state_dict(checkpoint['arch_optimizer'])
        self.weight_optimizer.load_state_dict(checkpoint['weight_optimizer'])
        self.performance_predictor.load_state_dict(checkpoint['predictor_state'])


class MultiObjectiveOptimizer:
    """
    多目标优化器
    同时优化准确率和延迟
    """

    def __init__(
        self,
        accuracy_weight: float = 0.6,
        latency_weight: float = 0.4
    ):
        self.accuracy_weight = accuracy_weight
        self.latency_weight = latency_weight

    def compute_objective(
        self,
        accuracy: float,
        latency: float,
        target_latency: Optional[float] = None
    ) -> float:
        """
        计算多目标函数值

        Args:
            accuracy: 准确率
            latency: 延迟
            target_latency: 目标延迟（约束）

        Returns:
            综合目标值
        """
        # 归一化延迟
        if target_latency is not None:
            latency_penalty = max(0, (latency - target_latency) / target_latency)
        else:
            latency_penalty = latency / 100  # 假设最大延迟100ms

        # 综合目标
        objective = (
            self.accuracy_weight * accuracy -
            self.latency_weight * latency_penalty
        )

        return objective

    def select_pareto_optimal(
        self,
        architectures: List[Dict[str, float]]
    ) -> List[Dict[str, float]]:
        """
        选择Pareto最优架构

        Args:
            architectures: 架构列表，每个包含'accuracy'和'latency'

        Returns:
            Pareto最优架构列表
        """
        pareto_optimal = []

        for arch in architectures:
            is_dominated = False
            for other in architectures:
                if other is arch:
                    continue
                # 检查是否被支配
                if (other['accuracy'] >= arch['accuracy'] and
                    other['latency'] <= arch['latency'] and
                    (other['accuracy'] > arch['accuracy'] or
                     other['latency'] < arch['latency'])):
                    is_dominated = True
                    break

            if not is_dominated:
                pareto_optimal.append(arch)

        return pareto_optimal


class NASModule:
    """
    神经架构搜索模块主类
    整合DARTS搜索、性能预测和多目标优化
    """

    def __init__(
        self,
        input_channels: int = 3,
        num_classes: int = 10,
        init_channels: int = 16,
        num_cells: int = 8,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        self.input_channels = input_channels
        self.num_classes = num_classes
        self.init_channels = init_channels
        self.num_cells = num_cells
        self.device = device

        # 搜索空间
        self.search_space = SearchSpace(
            num_cells=num_cells,
            init_channels=init_channels,
            num_classes=num_classes
        )

        # 搜索网络
        self.search_network = DARTSSearchNetwork(
            input_channels=input_channels,
            init_channels=init_channels,
            num_classes=num_classes,
            num_cells=num_cells
        )

        # 多目标优化器
        self.mo_optimizer = MultiObjectiveOptimizer()

        # 搜索结果
        self.best_architecture: Optional[Dict[str, Any]] = None
        self.search_history: List[Dict[str, Any]] = []

    def search(
        self,
        train_loader: DataLoader,
        val_loader: DataLoader,
        num_epochs: int = 50
    ) -> Dict[str, Any]:
        """
        执行架构搜索

        Args:
            train_loader: 训练数据加载器
            val_loader: 验证数据加载器
            num_epochs: 搜索轮数

        Returns:
            搜索结果
        """
        # 创建搜索器
        searcher = DARTSSearcher(
            model=self.search_network,
            train_loader=train_loader,
            val_loader=val_loader,
            device=self.device
        )

        # 执行搜索
        results = searcher.search(num_epochs=num_epochs)

        # 保存结果
        self.best_architecture = results['architecture']
        self.search_history.append(results)

        return results

    def evaluate_architecture(
        self,
        architecture: Dict[str, Any],
        test_loader: DataLoader
    ) -> Dict[str, float]:
        """
        评估架构性能

        Args:
            architecture: 架构描述
            test_loader: 测试数据加载器

        Returns:
            性能指标
        """
        # 这里应该根据架构描述构建实际网络并评估
        # 简化版本：使用搜索网络的当前状态
        self.search_network.eval()

        correct = 0
        total = 0
        total_latency = 0

        with torch.no_grad():
            for inputs, targets in test_loader:
                inputs = inputs.to(self.device)
                targets = targets.to(self.device)

                # 测量延迟
                start = torch.cuda.Event(enable_timing=True)
                end = torch.cuda.Event(enable_timing=True)

                start.record()
                outputs = self.search_network(inputs)
                end.record()

                torch.cuda.synchronize()
                latency = start.elapsed_time(end)
                total_latency += latency

                _, predicted = outputs.max(1)
                total += targets.size(0)
                correct += predicted.eq(targets).sum().item()

        accuracy = correct / total
        avg_latency = total_latency / len(test_loader)

        # 计算参数量
        params = sum(p.numel() for p in self.search_network.parameters())

        return {
            'accuracy': accuracy,
            'latency': avg_latency,
            'params': params
        }

    def get_optimal_architecture(
        self,
        target_latency: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        获取满足约束的最优架构

        Args:
            target_latency: 目标延迟约束

        Returns:
            最优架构
        """
        if self.best_architecture is None:
            raise RuntimeError("请先执行架构搜索")

        # 检查约束
        if not self.search_space.is_valid_architecture(self.best_architecture):
            print("Warning: Best architecture does not satisfy constraints")

        return self.best_architecture

    def save_results(self, path: str) -> None:
        """保存搜索结果"""
        results = {
            'best_architecture': self.best_architecture,
            'search_history': self.search_history,
            'search_space_config': {
                'num_cells': self.num_cells,
                'init_channels': self.init_channels,
                'num_classes': self.num_classes
            }
        }

        with open(path, 'w') as f:
            json.dump(results, f, indent=2)

    def load_results(self, path: str) -> None:
        """加载搜索结果"""
        with open(path, 'r') as f:
            results = json.load(f)

        self.best_architecture = results['best_architecture']
        self.search_history = results['search_history']


# 辅助函数和数据集
class DummyDataset(Dataset):
    """虚拟数据集用于演示"""

    def __init__(
        self,
        num_samples: int = 1000,
        image_size: int = 32,
        num_classes: int = 10
    ):
        self.num_samples = num_samples
        self.image_size = image_size
        self.num_classes = num_classes

    def __len__(self) -> int:
        return self.num_samples

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, int]:
        image = torch.randn(3, self.image_size, self.image_size)
        label = random.randint(0, self.num_classes - 1)
        return image, label


def create_dummy_loaders(
    batch_size: int = 32,
    num_samples: int = 1000
) -> Tuple[DataLoader, DataLoader]:
    """创建虚拟数据加载器"""
    train_dataset = DummyDataset(num_samples=num_samples)
    val_dataset = DummyDataset(num_samples=num_samples // 5)

    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=0
    )

    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0
    )

    return train_loader, val_loader


# 使用示例
if __name__ == "__main__":
    print("=" * 70)
    print("Neural Architecture Search Module Demo")
    print("=" * 70)

    # 配置
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"\nDevice: {DEVICE}")

    # 创建数据加载器
    print("\n[1] Creating dummy data loaders...")
    train_loader, val_loader = create_dummy_loaders(batch_size=32, num_samples=500)
    print(f"    Train batches: {len(train_loader)}, Val batches: {len(val_loader)}")

    # 初始化NAS模块
    print("\n[2] Initializing NAS Module...")
    nas_module = NASModule(
        input_channels=3,
        num_classes=10,
        init_channels=16,
        num_cells=4,  # 减少cells数量以加快演示
        device=DEVICE
    )
    print(f"    Search space: {nas_module.num_cells} cells, {nas_module.init_channels} channels")

    # 执行架构搜索
    print("\n[3] Running architecture search...")
    results = nas_module.search(
        train_loader=train_loader,
        val_loader=val_loader,
        num_epochs=10  # 减少轮数以加快演示
    )
    print(f"    Search completed")

    # 显示搜索结果
    print("\n[4] Architecture search results...")
    architecture = results['architecture']
    print(f"    Number of normal cells: {len(architecture['normal_cells'])}")
    print(f"    Number of reduction cells: {len(architecture['reduction_cells'])}")

    # 显示第一个cell的架构
    if architecture['normal_cells']:
        first_cell = architecture['normal_cells'][0]
        print(f"\n    First normal cell edges:")
        for edge_key, edge_info in list(first_cell['edges'].items())[:3]:
            print(f"      {edge_key}: {edge_info['operations'][:2]}")

    # 评估架构
    print("\n[5] Evaluating architecture...")
    test_loader = DataLoader(DummyDataset(num_samples=200), batch_size=32)
    metrics = nas_module.evaluate_architecture(architecture, test_loader)
    print(f"    Accuracy: {metrics['accuracy']:.4f}")
    print(f"    Latency: {metrics['latency']:.2f} ms")
    print(f"    Parameters: {metrics['params']:,}")

    # 多目标优化
    print("\n[6] Multi-objective optimization...")
    mo_optimizer = MultiObjectiveOptimizer(
        accuracy_weight=0.6,
        latency_weight=0.4
    )

    # 模拟多个架构
    architectures = [
        {'accuracy': 0.85, 'latency': 50, 'name': 'Arch_A'},
        {'accuracy': 0.90, 'latency': 80, 'name': 'Arch_B'},
        {'accuracy': 0.88, 'latency': 60, 'name': 'Arch_C'},
        {'accuracy': 0.82, 'latency': 40, 'name': 'Arch_D'},
    ]

    pareto_optimal = mo_optimizer.select_pareto_optimal(architectures)
    print(f"    Pareto optimal architectures:")
    for arch in pareto_optimal:
        obj_value = mo_optimizer.compute_objective(
            arch['accuracy'], arch['latency'], target_latency=70
        )
        print(f"      {arch['name']}: Acc={arch['accuracy']:.2f}, "
              f"Lat={arch['latency']:.0f}ms, Obj={obj_value:.4f}")

    # 性能预测器
    print("\n[7] Architecture performance predictor...")
    predictor = ArchitecturePerformancePredictor(input_dim=32)
    dummy_encoding = torch.randn(1, 32)
    prediction = predictor.predict(dummy_encoding)
    print(f"    Predicted accuracy: {prediction['accuracy']:.4f}")
    print(f"    Predicted latency: {prediction['latency']:.2f} ms")
    print(f"    Predicted params: {prediction['params']:.2e}")

    # 保存和加载
    print("\n[8] Saving and loading results...")
    save_path = "/tmp/nas_results.json"
    nas_module.save_results(save_path)

    new_nas = NASModule(input_channels=3, num_classes=10)
    new_nas.load_results(save_path)
    print(f"    Results saved and loaded successfully")

    # 搜索空间约束检查
    print("\n[9] Search space constraints...")
    test_arch = {'params': 5e6, 'flops': 300e6}
    is_valid = nas_module.search_space.is_valid_architecture(test_arch)
    print(f"    Test architecture valid: {is_valid}")
    print(f"    Constraints: max_params={nas_module.search_space.constraints['max_params']:.0e}, "
          f"max_flops={nas_module.search_space.constraints['max_flops']:.0e}")

    print("\n" + "=" * 70)
    print("Demo completed successfully!")
    print("=" * 70)

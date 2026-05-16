"""
强化学习智能检索优化器 (RL Retrieval Optimizer)

基于深度强化学习的记忆检索策略优化系统，实现DQN、多臂老虎机等算法，
用于动态优化检索策略，平衡相关性、延迟和多样性。

Author: Agent Memory System V3.0
Date: 2026-05-16
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.distributions import Categorical
import numpy as np
from typing import Dict, List, Tuple, Optional, Union, Callable, Any, Deque
from dataclasses import dataclass, field
from collections import deque, defaultdict
from enum import Enum
import random
import math
from abc import ABC, abstractmethod
import warnings
import copy
import json

warnings.filterwarnings('ignore')


class RetrievalAction(Enum):
    """检索动作类型"""
    USE_DENSE_INDEX = 0
    USE_SPARSE_INDEX = 1
    USE_HYBRID_INDEX = 2
    USE_GRAPH_INDEX = 3
    EXPAND_QUERY = 4
    REFINE_QUERY = 5
    USE_RERANKING = 6
    USE_DIVERSITY_RERANK = 7


@dataclass
class RetrievalState:
    """检索状态表示"""
    query_embedding: np.ndarray
    query_complexity: float
    available_indices: List[int]
    historical_performance: Dict[str, float]
    current_latency: float
    user_preference: float
    context_features: np.ndarray = field(default_factory=lambda: np.zeros(10))

    def to_tensor(self, device: str = "cpu") -> torch.Tensor:
        """转换为张量表示"""
        features = [
            self.query_embedding.flatten(),
            np.array([self.query_complexity, self.current_latency, self.user_preference]),
            self.context_features.flatten()
        ]
        return torch.tensor(np.concatenate(features), dtype=torch.float32).to(device)

    @property
    def state_dim(self) -> int:
        """状态维度"""
        return len(self.query_embedding) + 3 + len(self.context_features)


@dataclass
class RetrievalExperience:
    """检索经验样本"""
    state: RetrievalState
    action: int
    reward: float
    next_state: Optional[RetrievalState]
    done: bool
    info: Dict[str, Any] = field(default_factory=dict)


class ReplayBuffer:
    """
    经验回放缓冲区
    支持优先经验回放(PER)
    """

    def __init__(
        self,
        capacity: int = 100000,
        alpha: float = 0.6,
        beta: float = 0.4,
        beta_increment: float = 0.001
    ):
        self.capacity = capacity
        self.alpha = alpha  # 优先级指数
        self.beta = beta    # 重要性采样指数
        self.beta_increment = beta_increment
        self.buffer: List[RetrievalExperience] = []
        self.priorities = np.zeros(capacity, dtype=np.float32)
        self.position = 0
        self.size = 0

    def push(self, experience: RetrievalExperience) -> None:
        """添加经验"""
        max_priority = self.priorities.max() if self.size > 0 else 1.0

        if self.size < self.capacity:
            self.buffer.append(experience)
            self.size += 1
        else:
            self.buffer[self.position] = experience

        self.priorities[self.position] = max_priority
        self.position = (self.position + 1) % self.capacity

    def sample(
        self,
        batch_size: int
    ) -> Tuple[List[RetrievalExperience], np.ndarray, np.ndarray]:
        """
        采样经验批次

        Returns:
            经验批次、索引、重要性权重
        """
        if self.size < batch_size:
            batch_size = self.size

        # 计算采样概率
        priorities = self.priorities[:self.size]
        probabilities = priorities ** self.alpha
        probabilities /= probabilities.sum()

        # 采样索引
        indices = np.random.choice(self.size, batch_size, p=probabilities, replace=False)

        # 计算重要性权重
        weights = (self.size * probabilities[indices]) ** (-self.beta)
        weights /= weights.max()
        self.beta = min(1.0, self.beta + self.beta_increment)

        samples = [self.buffer[idx] for idx in indices]
        return samples, indices, weights

    def update_priorities(self, indices: np.ndarray, priorities: np.ndarray) -> None:
        """更新优先级"""
        for idx, priority in zip(indices, priorities):
            self.priorities[idx] = priority + 1e-6  # 避免零优先级

    def __len__(self) -> int:
        return self.size


class DuelingDQN(nn.Module):
    """
    对偶DQN网络
    分离价值流和优势流，提高学习稳定性
    """

    def __init__(
        self,
        state_dim: int,
        action_dim: int,
        hidden_dims: List[int] = [256, 256, 128]
    ):
        super().__init__()
        self.state_dim = state_dim
        self.action_dim = action_dim

        # 共享特征提取层
        layers = []
        prev_dim = state_dim
        for hidden_dim in hidden_dims:
            layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.ReLU(),
                nn.LayerNorm(hidden_dim),
                nn.Dropout(0.1)
            ])
            prev_dim = hidden_dim
        self.feature = nn.Sequential(*layers)

        # 价值流
        self.value_stream = nn.Sequential(
            nn.Linear(prev_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 1)
        )

        # 优势流
        self.advantage_stream = nn.Sequential(
            nn.Linear(prev_dim, 128),
            nn.ReLU(),
            nn.Linear(128, action_dim)
        )

    def forward(self, state: torch.Tensor) -> torch.Tensor:
        """前向传播"""
        features = self.feature(state)
        value = self.value_stream(features)
        advantage = self.advantage_stream(features)

        # 对偶DQN公式: Q = V + (A - mean(A))
        q_values = value + (advantage - advantage.mean(dim=-1, keepdim=True))
        return q_values

    def get_action_values(self, state: torch.Tensor) -> torch.Tensor:
        """获取动作值"""
        return self.forward(state)


class NoisyLinear(nn.Module):
    """噪声线性层，用于NoisyNet探索"""

    def __init__(self, in_features: int, out_features: int, sigma_init: float = 0.017):
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features

        # 可学习参数
        self.weight_mu = nn.Parameter(torch.empty(out_features, in_features))
        self.weight_sigma = nn.Parameter(torch.empty(out_features, in_features))
        self.bias_mu = nn.Parameter(torch.empty(out_features))
        self.bias_sigma = nn.Parameter(torch.empty(out_features))

        # 非可学习参数
        self.register_buffer('weight_epsilon', torch.empty(out_features, in_features))
        self.register_buffer('bias_epsilon', torch.empty(out_features))

        self.sigma_init = sigma_init
        self.reset_parameters()
        self.reset_noise()

    def reset_parameters(self) -> None:
        """重置参数"""
        mu_range = 1 / math.sqrt(self.in_features)
        self.weight_mu.data.uniform_(-mu_range, mu_range)
        self.weight_sigma.data.fill_(self.sigma_init / math.sqrt(self.in_features))
        self.bias_mu.data.uniform_(-mu_range, mu_range)
        self.bias_sigma.data.fill_(self.sigma_init / math.sqrt(self.out_features))

    def reset_noise(self) -> None:
        """重置噪声"""
        epsilon_in = self._scale_noise(self.in_features)
        epsilon_out = self._scale_noise(self.out_features)
        self.weight_epsilon.copy_(epsilon_out.outer(epsilon_in))
        self.bias_epsilon.copy_(epsilon_out)

    def _scale_noise(self, size: int) -> torch.Tensor:
        """缩放噪声"""
        x = torch.randn(size)
        return x.sign().mul_(x.abs().sqrt_())

    def forward(self, input: torch.Tensor) -> torch.Tensor:
        """前向传播"""
        if self.training:
            weight = self.weight_mu + self.weight_sigma * self.weight_epsilon
            bias = self.bias_mu + self.bias_sigma * self.bias_epsilon
        else:
            weight = self.weight_mu
            bias = self.bias_mu
        return F.linear(input, weight, bias)


class NoisyDQN(nn.Module):
    """NoisyNet DQN - 参数化探索"""

    def __init__(
        self,
        state_dim: int,
        action_dim: int,
        hidden_dim: int = 256
    ):
        super().__init__()
        self.fc1 = NoisyLinear(state_dim, hidden_dim)
        self.fc2 = NoisyLinear(hidden_dim, hidden_dim)
        self.fc3 = NoisyLinear(hidden_dim, action_dim)

    def forward(self, state: torch.Tensor) -> torch.Tensor:
        x = F.relu(self.fc1(state))
        x = F.relu(self.fc2(x))
        return self.fc3(x)

    def reset_noise(self) -> None:
        """重置所有噪声层"""
        self.fc1.reset_noise()
        self.fc2.reset_noise()
        self.fc3.reset_noise()


class MultiArmedBandit:
    """
    多臂老虎机算法
    用于索引选择决策
    """

    def __init__(
        self,
        n_arms: int,
        strategy: str = "ucb",
        epsilon: float = 0.1,
        alpha: float = 0.1
    ):
        self.n_arms = n_arms
        self.strategy = strategy
        self.epsilon = epsilon
        self.alpha = alpha  # 学习率

        # 统计信息
        self.counts = np.zeros(n_arms)
        self.values = np.zeros(n_arms)
        self.total_counts = 0

        # Thompson Sampling参数
        self.successes = np.ones(n_arms)
        self.failures = np.ones(n_arms)

    def select_arm(self) -> int:
        """选择臂"""
        if self.strategy == "epsilon_greedy":
            return self._epsilon_greedy()
        elif self.strategy == "ucb":
            return self._ucb()
        elif self.strategy == "thompson":
            return self._thompson_sampling()
        elif self.strategy == "softmax":
            return self._softmax()
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")

    def _epsilon_greedy(self) -> int:
        """Epsilon-贪婪策略"""
        if random.random() < self.epsilon:
            return random.randint(0, self.n_arms - 1)
        return np.argmax(self.values)

    def _ucb(self) -> int:
        """UCB1算法"""
        if self.total_counts < self.n_arms:
            return self.total_counts

        ucb_values = self.values + np.sqrt(
            2 * np.log(self.total_counts) / (self.counts + 1e-10)
        )
        return np.argmax(ucb_values)

    def _thompson_sampling(self) -> int:
        """Thompson Sampling"""
        samples = np.random.beta(self.successes, self.failures)
        return np.argmax(samples)

    def _softmax(self) -> int:
        """Softmax选择"""
        exp_values = np.exp(self.values / self.epsilon)
        probabilities = exp_values / exp_values.sum()
        return np.random.choice(self.n_arms, p=probabilities)

    def update(self, arm: int, reward: float) -> None:
        """更新臂的统计信息"""
        self.counts[arm] += 1
        self.total_counts += 1

        # 增量更新平均值
        n = self.counts[arm]
        value = self.values[arm]
        self.values[arm] = ((n - 1) / n) * value + (1 / n) * reward

        # 更新Thompson Sampling参数
        if reward > 0.5:
            self.successes[arm] += 1
        else:
            self.failures[arm] += 1

    def get_best_arm(self) -> int:
        """获取当前最佳臂"""
        return np.argmax(self.values)

    def get_arm_stats(self) -> Dict[str, np.ndarray]:
        """获取臂的统计信息"""
        return {
            "counts": self.counts.copy(),
            "values": self.values.copy(),
            "successes": self.successes.copy(),
            "failures": self.failures.copy()
        }


class RewardFunction:
    """
    奖励函数设计
    综合考虑相关性、延迟和多样性
    """

    def __init__(
        self,
        relevance_weight: float = 0.5,
        latency_weight: float = 0.3,
        diversity_weight: float = 0.2,
        latency_threshold: float = 1.0
    ):
        self.relevance_weight = relevance_weight
        self.latency_weight = latency_weight
        self.diversity_weight = diversity_weight
        self.latency_threshold = latency_threshold

    def compute_reward(
        self,
        relevance_score: float,
        latency: float,
        diversity_score: float,
        user_satisfaction: Optional[float] = None
    ) -> float:
        """
        计算综合奖励

        Args:
            relevance_score: 相关性分数 [0, 1]
            latency: 延迟（秒）
            diversity_score: 多样性分数 [0, 1]
            user_satisfaction: 用户满意度 [0, 1]

        Returns:
            综合奖励值
        """
        # 延迟惩罚（指数衰减）
        latency_penalty = math.exp(-latency / self.latency_threshold)

        # 基础奖励
        base_reward = (
            self.relevance_weight * relevance_score +
            self.latency_weight * latency_penalty +
            self.diversity_weight * diversity_score
        )

        # 用户满意度调整
        if user_satisfaction is not None:
            base_reward *= (0.5 + 0.5 * user_satisfaction)

        return base_reward

    def compute_batch_rewards(
        self,
        results: List[Dict[str, float]]
    ) -> List[float]:
        """批量计算奖励"""
        return [self.compute_reward(**r) for r in results]


class DQNRetrievalOptimizer:
    """
    DQN检索优化器
    使用深度Q网络优化检索策略
    """

    def __init__(
        self,
        state_dim: int,
        action_dim: int,
        hidden_dims: List[int] = [256, 256, 128],
        learning_rate: float = 0.0003,
        gamma: float = 0.99,
        epsilon_start: float = 1.0,
        epsilon_end: float = 0.01,
        epsilon_decay: float = 0.995,
        buffer_capacity: int = 100000,
        batch_size: int = 64,
        target_update_freq: int = 1000,
        use_dueling: bool = True,
        use_noisy: bool = False,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.gamma = gamma
        self.epsilon = epsilon_start
        self.epsilon_end = epsilon_end
        self.epsilon_decay = epsilon_decay
        self.batch_size = batch_size
        self.target_update_freq = target_update_freq
        self.use_noisy = use_noisy
        self.device = device

        # 初始化网络
        if use_noisy:
            self.policy_net = NoisyDQN(state_dim, action_dim).to(device)
            self.target_net = NoisyDQN(state_dim, action_dim).to(device)
        elif use_dueling:
            self.policy_net = DuelingDQN(state_dim, action_dim, hidden_dims).to(device)
            self.target_net = DuelingDQN(state_dim, action_dim, hidden_dims).to(device)
        else:
            # 标准DQN
            layers = []
            prev_dim = state_dim
            for hidden_dim in hidden_dims:
                layers.extend([
                    nn.Linear(prev_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Dropout(0.1)
                ])
                prev_dim = hidden_dim
            layers.append(nn.Linear(prev_dim, action_dim))
            self.policy_net = nn.Sequential(*layers).to(device)
            self.target_net = nn.Sequential(*layers).to(device)

        self.target_net.load_state_dict(self.policy_net.state_dict())
        self.target_net.eval()

        self.optimizer = optim.Adam(self.policy_net.parameters(), lr=learning_rate)
        self.replay_buffer = ReplayBuffer(capacity=buffer_capacity)

        self.training_step = 0
        self.episode_rewards: List[float] = []

    def select_action(
        self,
        state: RetrievalState,
        training: bool = True
    ) -> int:
        """选择动作"""
        state_tensor = state.to_tensor(self.device).unsqueeze(0)

        if training and not self.use_noisy:
            # Epsilon-贪婪探索
            if random.random() < self.epsilon:
                return random.randint(0, self.action_dim - 1)

        with torch.no_grad():
            q_values = self.policy_net(state_tensor)
            return q_values.argmax(dim=1).item()

    def store_experience(self, experience: RetrievalExperience) -> None:
        """存储经验"""
        self.replay_buffer.push(experience)

    def train_step(self) -> Optional[float]:
        """训练步骤"""
        if len(self.replay_buffer) < self.batch_size:
            return None

        # 采样经验
        experiences, indices, weights = self.replay_buffer.sample(self.batch_size)

        # 准备批次数据
        states = torch.stack([e.state.to_tensor(self.device) for e in experiences])
        actions = torch.tensor([e.action for e in experiences], device=self.device)
        rewards = torch.tensor([e.reward for e in experiences], dtype=torch.float32, device=self.device)
        weights = torch.tensor(weights, dtype=torch.float32, device=self.device)

        # 处理终止状态
        non_final_mask = torch.tensor(
            [e.next_state is not None for e in experiences],
            dtype=torch.bool,
            device=self.device
        )
        non_final_next_states = torch.stack([
            e.next_state.to_tensor(self.device) for e in experiences
            if e.next_state is not None
        ]) if any(non_final_mask) else torch.empty(0, self.state_dim, device=self.device)

        # 当前Q值
        current_q = self.policy_net(states).gather(1, actions.unsqueeze(1)).squeeze(1)

        # 目标Q值（Double DQN）
        next_q = torch.zeros(self.batch_size, device=self.device)
        if non_final_next_states.size(0) > 0:
            # 选择动作
            next_actions = self.policy_net(non_final_next_states).argmax(dim=1)
            # 评估动作
            next_q[non_final_mask] = self.target_net(non_final_next_states).gather(
                1, next_actions.unsqueeze(1)
            ).squeeze(1).detach()

        target_q = rewards + self.gamma * next_q

        # 计算TD误差和损失
        td_errors = torch.abs(current_q - target_q).detach().cpu().numpy()
        loss = (weights * F.smooth_l1_loss(current_q, target_q, reduction='none')).mean()

        # 优化
        self.optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.policy_net.parameters(), max_norm=10)
        self.optimizer.step()

        # 更新优先级
        self.replay_buffer.update_priorities(indices, td_errors)

        # 更新目标网络
        self.training_step += 1
        if self.training_step % self.target_update_freq == 0:
            self.target_net.load_state_dict(self.policy_net.state_dict())

        # 衰减epsilon
        if not self.use_noisy:
            self.epsilon = max(self.epsilon_end, self.epsilon * self.epsilon_decay)

        # 重置噪声
        if self.use_noisy and isinstance(self.policy_net, NoisyDQN):
            self.policy_net.reset_noise()
            self.target_net.reset_noise()

        return loss.item()

    def save(self, path: str) -> None:
        """保存模型"""
        torch.save({
            'policy_net': self.policy_net.state_dict(),
            'target_net': self.target_net.state_dict(),
            'optimizer': self.optimizer.state_dict(),
            'epsilon': self.epsilon,
            'training_step': self.training_step
        }, path)

    def load(self, path: str) -> None:
        """加载模型"""
        checkpoint = torch.load(path, map_location=self.device)
        self.policy_net.load_state_dict(checkpoint['policy_net'])
        self.target_net.load_state_dict(checkpoint['target_net'])
        self.optimizer.load_state_dict(checkpoint['optimizer'])
        self.epsilon = checkpoint['epsilon']
        self.training_step = checkpoint['training_step']


class RLRetrievalOptimizer:
    """
    强化学习智能检索优化器主类
    整合DQN、MAB等多种算法
    """

    def __init__(
        self,
        state_dim: int = 128,
        n_actions: int = 8,
        n_indices: int = 4,
        use_dqn: bool = True,
        use_mab: bool = True,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.n_indices = n_indices
        self.device = device

        # 初始化组件
        self.dqn = DQNRetrievalOptimizer(
            state_dim=state_dim,
            action_dim=n_actions,
            device=device
        ) if use_dqn else None

        self.mab = MultiArmedBandit(
            n_arms=n_indices,
            strategy="ucb"
        ) if use_mab else None

        self.reward_function = RewardFunction()

        # 统计信息
        self.action_counts = np.zeros(n_actions)
        self.action_rewards = np.zeros(n_actions)
        self.episode_count = 0

        # 在线学习缓冲区
        self.online_buffer: Deque[RetrievalExperience] = deque(maxlen=1000)

    def optimize_retrieval(
        self,
        state: RetrievalState,
        training: bool = True
    ) -> Dict[str, Any]:
        """
        优化检索策略

        Returns:
            包含动作选择和索引选择的决策结果
        """
        result = {}

        # DQN选择检索策略
        if self.dqn is not None:
            action = self.dqn.select_action(state, training)
            result['action'] = action
            result['action_name'] = RetrievalAction(action).name
            self.action_counts[action] += 1

        # MAB选择索引
        if self.mab is not None:
            index = self.mab.select_arm()
            result['index'] = index
            self.mab.counts[index] += 1

        return result

    def feedback(
        self,
        state: RetrievalState,
        action: int,
        index: int,
        relevance_score: float,
        latency: float,
        diversity_score: float,
        next_state: Optional[RetrievalState] = None,
        done: bool = False
    ) -> float:
        """
        接收反馈并更新策略

        Returns:
            计算的奖励值
        """
        # 计算奖励
        reward = self.reward_function.compute_reward(
            relevance_score=relevance_score,
            latency=latency,
            diversity_score=diversity_score
        )

        # 创建经验
        experience = RetrievalExperience(
            state=state,
            action=action,
            reward=reward,
            next_state=next_state,
            done=done
        )

        # 更新DQN
        if self.dqn is not None:
            self.dqn.store_experience(experience)
            self.dqn.train_step()
            self.action_rewards[action] += reward

        # 更新MAB
        if self.mab is not None:
            self.mab.update(index, reward)

        # 在线学习
        self.online_buffer.append(experience)

        return reward

    def batch_feedback(
        self,
        feedbacks: List[Dict[str, Any]]
    ) -> List[float]:
        """批量反馈处理"""
        rewards = []
        for fb in feedbacks:
            reward = self.feedback(**fb)
            rewards.append(reward)
        return rewards

    def get_strategy_report(self) -> Dict[str, Any]:
        """获取策略报告"""
        report = {
            "episode_count": self.episode_count,
            "action_distribution": {
                RetrievalAction(i).name: int(count)
                for i, count in enumerate(self.action_counts)
            },
            "action_average_rewards": {
                RetrievalAction(i).name: float(reward / max(count, 1))
                for i, (reward, count) in enumerate(zip(self.action_rewards, self.action_counts))
            }
        }

        if self.mab is not None:
            report["mab_stats"] = self.mab.get_arm_stats()

        if self.dqn is not None:
            report["dqn_epsilon"] = self.dqn.epsilon
            report["dqn_training_step"] = self.dqn.training_step

        return report

    def online_learning_step(self, n_steps: int = 10) -> None:
        """在线学习步骤"""
        if len(self.online_buffer) < self.dqn.batch_size:
            return

        for _ in range(n_steps):
            # 从在线缓冲区采样
            batch = random.sample(
                list(self.online_buffer),
                min(self.dqn.batch_size, len(self.online_buffer))
            )

            for exp in batch:
                self.dqn.store_experience(exp)

            self.dqn.train_step()

    def adapt_to_user(
        self,
        user_id: str,
        preference_history: List[Dict[str, Any]]
    ) -> None:
        """根据用户历史适应策略"""
        # 分析用户偏好
        avg_relevance = np.mean([h.get('relevance', 0.5) for h in preference_history])
        avg_latency_tolerance = np.mean([h.get('latency_tolerance', 1.0) for h in preference_history])

        # 调整奖励函数权重
        self.reward_function.relevance_weight = 0.3 + 0.4 * avg_relevance
        self.reward_function.latency_weight = 0.5 / avg_latency_tolerance
        self.reward_function.diversity_weight = 1.0 - self.reward_function.relevance_weight - self.reward_function.latency_weight

    def save(self, path: str) -> None:
        """保存优化器状态"""
        state = {
            'action_counts': self.action_counts,
            'action_rewards': self.action_rewards,
            'episode_count': self.episode_count,
            'reward_function_params': {
                'relevance_weight': self.reward_function.relevance_weight,
                'latency_weight': self.reward_function.latency_weight,
                'diversity_weight': self.reward_function.diversity_weight
            }
        }

        if self.mab is not None:
            state['mab_stats'] = self.mab.get_arm_stats()

        if self.dqn is not None:
            dqn_path = path.replace('.pt', '_dqn.pt')
            self.dqn.save(dqn_path)

        with open(path.replace('.pt', '_state.json'), 'w') as f:
            json.dump(state, f, indent=2)

    def load(self, path: str) -> None:
        """加载优化器状态"""
        with open(path.replace('.pt', '_state.json'), 'r') as f:
            state = json.load(f)

        self.action_counts = np.array(state['action_counts'])
        self.action_rewards = np.array(state['action_rewards'])
        self.episode_count = state['episode_count']

        # 恢复奖励函数参数
        rf_params = state['reward_function_params']
        self.reward_function.relevance_weight = rf_params['relevance_weight']
        self.reward_function.latency_weight = rf_params['latency_weight']
        self.reward_function.diversity_weight = rf_params['diversity_weight']

        # 恢复MAB
        if self.mab is not None and 'mab_stats' in state:
            stats = state['mab_stats']
            self.mab.counts = stats['counts']
            self.mab.values = stats['values']
            self.mab.successes = stats['successes']
            self.mab.failures = stats['failures']

        # 加载DQN
        if self.dqn is not None:
            dqn_path = path.replace('.pt', '_dqn.pt')
            self.dqn.load(dqn_path)


def create_sample_retrieval_state(
    query_dim: int = 64,
    context_dim: int = 10
) -> RetrievalState:
    """创建示例检索状态"""
    return RetrievalState(
        query_embedding=np.random.randn(query_dim),
        query_complexity=np.random.random(),
        available_indices=[0, 1, 2, 3],
        historical_performance={
            'avg_relevance': np.random.random(),
            'avg_latency': np.random.random(),
            'success_rate': np.random.random()
        },
        current_latency=np.random.random() * 2,
        user_preference=np.random.random(),
        context_features=np.random.randn(context_dim)
    )


def train_rl_optimizer(
    optimizer: RLRetrievalOptimizer,
    n_episodes: int = 1000,
    steps_per_episode: int = 10
) -> Dict[str, List[float]]:
    """
    训练RL检索优化器

    Args:
        optimizer: RL优化器
        n_episodes: 训练轮数
        steps_per_episode: 每轮步数

    Returns:
        训练历史
    """
    history = {
        'rewards': [],
        'relevance': [],
        'latency': [],
        'diversity': []
    }

    for episode in range(n_episodes):
        episode_reward = 0

        for step in range(steps_per_episode):
            # 创建状态
            state = create_sample_retrieval_state()

            # 选择动作
            decision = optimizer.optimize_retrieval(state, training=True)
            action = decision.get('action', 0)
            index = decision.get('index', 0)

            # 模拟执行结果
            relevance = np.random.beta(2, 1) * (0.8 + 0.2 * np.random.random())
            latency = np.random.exponential(0.5) * (1.0 - 0.1 * index)
            diversity = np.random.beta(3, 2)

            # 创建下一个状态
            next_state = create_sample_retrieval_state()
            done = (step == steps_per_episode - 1)

            # 反馈
            reward = optimizer.feedback(
                state=state,
                action=action,
                index=index,
                relevance_score=relevance,
                latency=latency,
                diversity_score=diversity,
                next_state=next_state,
                done=done
            )

            episode_reward += reward

            history['rewards'].append(reward)
            history['relevance'].append(relevance)
            history['latency'].append(latency)
            history['diversity'].append(diversity)

        optimizer.episode_count += 1

        if (episode + 1) % 100 == 0:
            avg_reward = np.mean(history['rewards'][-100:])
            print(f"Episode {episode + 1}/{n_episodes}, Avg Reward: {avg_reward:.4f}, Epsilon: {optimizer.dqn.epsilon:.4f}")

    return history


# 使用示例
if __name__ == "__main__":
    print("=" * 70)
    print("RL Retrieval Optimizer Demo")
    print("=" * 70)

    # 配置参数
    STATE_DIM = 64 + 3 + 10  # query + metrics + context
    N_ACTIONS = 8
    N_INDICES = 4

    # 初始化优化器
    print("\n[1] Initializing RL Retrieval Optimizer...")
    optimizer = RLRetrievalOptimizer(
        state_dim=STATE_DIM,
        n_actions=N_ACTIONS,
        n_indices=N_INDICES,
        use_dqn=True,
        use_mab=True
    )
    print(f"    State dim: {STATE_DIM}, Actions: {N_ACTIONS}, Indices: {N_INDICES}")

    # 训练
    print("\n[2] Training optimizer...")
    history = train_rl_optimizer(optimizer, n_episodes=500, steps_per_episode=10)
    print(f"    Final avg reward: {np.mean(history['rewards'][-100:]):.4f}")

    # 测试推理
    print("\n[3] Testing inference...")
    test_state = create_sample_retrieval_state()
    decision = optimizer.optimize_retrieval(test_state, training=False)
    print(f"    Selected action: {decision.get('action_name')} (ID: {decision.get('action')})")
    print(f"    Selected index: {decision.get('index')}")

    # 模拟反馈
    print("\n[4] Simulating feedback loop...")
    for i in range(5):
        state = create_sample_retrieval_state()
        decision = optimizer.optimize_retrieval(state, training=False)
        reward = optimizer.feedback(
            state=state,
            action=decision['action'],
            index=decision['index'],
            relevance_score=0.8 + 0.1 * np.random.random(),
            latency=0.3 + 0.2 * np.random.random(),
            diversity_score=0.7 + 0.1 * np.random.random()
        )
        print(f"    Step {i+1}: Action={RetrievalAction(decision['action']).name}, Reward={reward:.4f}")

    # 策略报告
    print("\n[5] Strategy report...")
    report = optimizer.get_strategy_report()
    print(f"    Action distribution: {report['action_distribution']}")
    print(f"    Average rewards: {report['action_average_rewards']}")

    # MAB统计
    if optimizer.mab is not None:
        print("\n[6] MAB statistics...")
        mab_stats = optimizer.mab.get_arm_stats()
        print(f"    Arm counts: {mab_stats['counts']}")
        print(f"    Arm values: {mab_stats['values']}")

    # 在线学习
    print("\n[7] Online learning...")
    optimizer.online_learning_step(n_steps=5)
    print(f"    Online buffer size: {len(optimizer.online_buffer)}")

    # 保存和加载
    print("\n[8] Saving and loading...")
    save_path = "/tmp/rl_retrieval_optimizer.pt"
    optimizer.save(save_path)

    new_optimizer = RLRetrievalOptimizer(
        state_dim=STATE_DIM,
        n_actions=N_ACTIONS,
        n_indices=N_INDICES
    )
    new_optimizer.load(save_path)
    print(f"    Model saved and loaded successfully")

    # 用户适应
    print("\n[9] User adaptation...")
    preference_history = [
        {'relevance': 0.9, 'latency_tolerance': 0.5},
        {'relevance': 0.85, 'latency_tolerance': 0.6},
        {'relevance': 0.92, 'latency_tolerance': 0.4}
    ]
    optimizer.adapt_to_user("user_001", preference_history)
    print(f"    Adapted weights: rel={optimizer.reward_function.relevance_weight:.3f}, "
          f"lat={optimizer.reward_function.latency_weight:.3f}, "
          f"div={optimizer.reward_function.diversity_weight:.3f}")

    print("\n" + "=" * 70)
    print("Demo completed successfully!")
    print("=" * 70)

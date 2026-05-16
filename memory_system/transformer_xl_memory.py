"""
Transformer-XL长程记忆模块 (Transformer-XL Long-term Memory)

实现Transformer-XL架构，支持片段级递归机制和相对位置编码，
用于超长序列记忆建模和与现有记忆系统的集成。

Author: Agent Memory System V3.0
Date: 2026-05-16
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import math
from typing import Dict, List, Tuple, Optional, Union, Callable, Any
from dataclasses import dataclass, field
from collections import deque
import warnings
import copy

warnings.filterwarnings('ignore')


@dataclass
class TransformerXLConfig:
    """Transformer-XL配置"""
    vocab_size: int = 10000
    d_model: int = 512
    n_head: int = 8
    d_head: int = 64
    d_inner: int = 2048
    n_layer: int = 6
    dropout: float = 0.1
    dropatt: float = 0.0
    max_seq_len: int = 512
    mem_len: int = 512
    clamp_len: int = -1
    same_length: bool = False
    tie_weight: bool = True
    tie_projs: List[bool] = field(default_factory=list)


class PositionalEmbedding(nn.Module):
    """
    正弦位置编码
    """

    def __init__(self, demb: int):
        super().__init__()
        self.demb = demb
        inv_freq = 1 / (10000 ** (torch.arange(0.0, demb, 2.0) / demb))
        self.register_buffer('inv_freq', inv_freq)

    def forward(self, pos_seq: torch.Tensor) -> torch.Tensor:
        """
        生成位置编码

        Args:
            pos_seq: 位置序列

        Returns:
            位置编码
        """
        sinusoid_inp = torch.ger(pos_seq, self.inv_freq)
        pos_emb = torch.cat([sinusoid_inp.sin(), sinusoid_inp.cos()], dim=-1)
        return pos_emb


class RelativeMultiHeadAttention(nn.Module):
    """
    相对位置编码的多头注意力
    Transformer-XL的核心组件
    """

    def __init__(
        self,
        n_head: int,
        d_model: int,
        d_head: int,
        dropout: float = 0.0,
        dropatt: float = 0.0
    ):
        super().__init__()
        self.n_head = n_head
        self.d_model = d_model
        self.d_head = d_head

        # Q, K, V投影
        self.q_net = nn.Linear(d_model, n_head * d_head, bias=False)
        self.kv_net = nn.Linear(d_model, 2 * n_head * d_head, bias=False)

        # 输出投影
        self.o_net = nn.Linear(n_head * d_head, d_model, bias=False)

        # 相对位置编码投影
        self.r_net = nn.Linear(d_model, n_head * d_head, bias=False)

        self.drop = nn.Dropout(dropout)
        self.dropatt = nn.Dropout(dropatt)

        self.scale = 1 / (d_head ** 0.5)

    def forward(
        self,
        query: torch.Tensor,
        key: torch.Tensor,
        value: torch.Tensor,
        pos_emb: torch.Tensor,
        memory: Optional[torch.Tensor] = None,
        attn_mask: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        前向传播

        Args:
            query: 查询 [qlen, bsz, d_model]
            key: 键 [klen, bsz, d_model]
            value: 值 [klen, bsz, d_model]
            pos_emb: 相对位置编码 [rlen, d_model]
            memory: 记忆状态 [mlen, bsz, d_model]
            attn_mask: 注意力掩码

        Returns:
            注意力输出
        """
        qlen, bsz, _ = query.size()
        klen = key.size(0)

        # 合并记忆
        if memory is not None:
            key = torch.cat([memory, key], dim=0)
            value = torch.cat([memory, value], dim=0)
            klen = key.size(0)

        # 线性投影
        w_head_q = self.q_net(query)  # [qlen, bsz, n_head * d_head]
        w_head_k, w_head_v = torch.chunk(self.kv_net(key), 2, dim=-1)
        w_head_r = self.r_net(pos_emb)  # [rlen, n_head * d_head]

        # reshape
        w_head_q = w_head_q.view(qlen, bsz, self.n_head, self.d_head)
        w_head_k = w_head_k.view(klen, bsz, self.n_head, self.d_head)
        w_head_v = w_head_v.view(klen, bsz, self.n_head, self.d_head)
        w_head_r = w_head_r.view(-1, self.n_head, self.d_head)

        # 计算注意力分数
        # AC term (content-based)
        AC = torch.einsum('ibnd,jbnd->ijbn', w_head_q, w_head_k)

        # BD term (position-based)
        BD = torch.einsum('ibnd,jnd->ijbn', w_head_q, w_head_r)

        # 相对位置偏置
        BD = self._rel_shift(BD)

        # 组合注意力分数
        attn_score = (AC + BD) * self.scale

        # 应用掩码
        if attn_mask is not None:
            attn_score = attn_score.float().masked_fill(
                attn_mask[None, :, :, None],
                -float('inf')
            ).type_as(attn_score)

        # softmax
        attn_prob = F.softmax(attn_score, dim=1)
        attn_prob = self.dropatt(attn_prob)

        # 加权求和
        attn_vec = torch.einsum('ijbn,jbnd->ibnd', attn_prob, w_head_v)

        # reshape和投影
        attn_vec = attn_vec.contiguous().view(qlen, bsz, self.n_head * self.d_head)
        attn_out = self.o_net(attn_vec)
        attn_out = self.drop(attn_out)

        return attn_out

    def _rel_shift(self, x: torch.Tensor) -> torch.Tensor:
        """
        相对位置偏移

        Args:
            x: 输入 [qlen, klen, bsz, n_head]

        Returns:
            偏移后的输出
        """
        x_size = x.size()
        x = x.view(x_size[0], x_size[1], -1)  # [qlen, klen, bsz * n_head]

        x = F.pad(x, (0, 0, 1, 0))  # [qlen, klen+1, bsz * n_head]
        x = x.view(x_size[1] + 1, x_size[0], -1)  # [klen+1, qlen, bsz * n_head]
        x = x[1:]  # [klen, qlen, bsz * n_head]
        x = x.view(x_size)

        return x


class PositionwiseFF(nn.Module):
    """
    位置前馈网络
    """

    def __init__(
        self,
        d_model: int,
        d_inner: int,
        dropout: float = 0.1
    ):
        super().__init__()
        self.CoreNet = nn.Sequential(
            nn.Linear(d_model, d_inner),
            nn.ReLU(inplace=True),
            nn.Dropout(dropout),
            nn.Linear(d_inner, d_model),
            nn.Dropout(dropout)
        )
        self.layer_norm = nn.LayerNorm(d_model)

    def forward(self, inp: torch.Tensor) -> torch.Tensor:
        """前向传播"""
        core_out = self.CoreNet(inp)
        output = self.layer_norm(inp + core_out)
        return output


class TransformerXLLayer(nn.Module):
    """
    Transformer-XL层
    """

    def __init__(
        self,
        n_head: int,
        d_model: int,
        d_head: int,
        d_inner: int,
        dropout: float = 0.1,
        dropatt: float = 0.0
    ):
        super().__init__()
        self.dec_attn = RelativeMultiHeadAttention(
            n_head, d_model, d_head, dropout=dropout, dropatt=dropatt
        )
        self.pos_ff = PositionwiseFF(d_model, d_inner, dropout=dropout)
        self.layer_norm1 = nn.LayerNorm(d_model)

    def forward(
        self,
        dec_inp: torch.Tensor,
        pos_emb: torch.Tensor,
        memory: Optional[torch.Tensor] = None,
        attn_mask: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        前向传播

        Args:
            dec_inp: 解码器输入
            pos_emb: 位置编码
            memory: 记忆状态
            attn_mask: 注意力掩码

        Returns:
            层输出
        """
        # 预层归一化
        inp = self.layer_norm1(dec_inp)

        # 多头注意力
        attn_out = self.dec_attn(
            inp, inp, inp,
            pos_emb=pos_emb,
            memory=memory,
            attn_mask=attn_mask
        )

        # 残差连接
        attn_out = dec_inp + attn_out

        # 前馈网络
        ff_out = self.pos_ff(attn_out)

        return ff_out


class TransformerXL(nn.Module):
    """
    Transformer-XL模型
    支持超长序列建模
    """

    def __init__(self, config: TransformerXLConfig):
        super().__init__()
        self.config = config

        # 词嵌入
        self.word_emb = nn.Embedding(config.vocab_size, config.d_model)

        # 位置编码
        self.pos_emb = PositionalEmbedding(config.d_model)

        # Dropout
        self.drop = nn.Dropout(config.dropout)

        # Transformer层
        self.layers = nn.ModuleList([
            TransformerXLLayer(
                n_head=config.n_head,
                d_model=config.d_model,
                d_head=config.d_head,
                d_inner=config.d_inner,
                dropout=config.dropout,
                dropatt=config.dropatt
            )
            for _ in range(config.n_layer)
        ])

        # 输出层
        self.output_layer = nn.Linear(config.d_model, config.vocab_size)

        # 记忆状态
        self.memory: Optional[List[torch.Tensor]] = None
        self.reset_memory()

        self.apply(self._init_weights)

    def _init_weights(self, module):
        """初始化权重"""
        if isinstance(module, nn.Linear):
            nn.init.normal_(module.weight, 0.0, 0.02)
            if module.bias is not None:
                nn.init.zeros_(module.bias)
        elif isinstance(module, nn.Embedding):
            nn.init.normal_(module.weight, 0.0, 0.02)
        elif isinstance(module, nn.LayerNorm):
            nn.init.ones_(module.weight)
            nn.init.zeros_(module.bias)

    def reset_memory(self):
        """重置记忆状态"""
        self.memory = None

    def update_memory(self, hidden_states: List[torch.Tensor]):
        """
        更新记忆状态

        Args:
            hidden_states: 当前层的隐藏状态
        """
        if self.memory is None:
            self.memory = [None] * len(hidden_states)

        new_memory = []
        for mem, hid in zip(self.memory, hidden_states):
            if mem is None:
                new_mem = hid.detach()
            else:
                # 拼接并截断
                new_mem = torch.cat([mem, hid], dim=0).detach()
                if self.config.mem_len > 0:
                    new_mem = new_mem[-self.config.mem_len:]
            new_memory.append(new_mem)

        self.memory = new_memory

    def forward(
        self,
        input_ids: torch.Tensor,
        memory: Optional[List[torch.Tensor]] = None,
        return_hidden: bool = False
    ) -> Union[torch.Tensor, Tuple[torch.Tensor, List[torch.Tensor]]]:
        """
        前向传播

        Args:
            input_ids: 输入token IDs [seq_len, batch_size]
            memory: 外部提供的记忆状态
            return_hidden: 是否返回隐藏状态

        Returns:
            输出logits，可选隐藏状态
        """
        if memory is None:
            memory = self.memory

        # 词嵌入
        word_emb = self.word_emb(input_ids)
        inp = self.drop(word_emb)

        # 生成位置编码
        qlen, bsz = input_ids.size()
        klen = qlen
        if memory is not None and memory[0] is not None:
            klen += memory[0].size(0)

        # 创建位置序列
        pos_seq = torch.arange(klen - 1, -1, -1.0, device=input_ids.device)
        if self.config.clamp_len > 0:
            pos_seq.clamp_(max=self.config.clamp_len)

        pos_emb = self.pos_emb(pos_seq)
        pos_emb = self.drop(pos_emb)

        # 创建注意力掩码
        attn_mask = torch.triu(
            torch.ones(qlen, klen, device=input_ids.device),
            diagonal=klen - qlen + 1
        ).bool()

        # 通过Transformer层
        hidden_states = []
        for i, layer in enumerate(self.layers):
            mem = memory[i] if memory is not None else None
            inp = layer(inp, pos_emb, memory=mem, attn_mask=attn_mask)
            hidden_states.append(inp)

        # 更新记忆
        self.update_memory(hidden_states)

        # 输出层
        output = self.drop(inp)
        logits = self.output_layer(output)

        if return_hidden:
            return logits, hidden_states

        return logits

    def get_memory(self) -> Optional[List[torch.Tensor]]:
        """获取当前记忆状态"""
        return self.memory

    def set_memory(self, memory: Optional[List[torch.Tensor]]):
        """设置记忆状态"""
        self.memory = memory


class LongTermMemorySystem:
    """
    长程记忆系统
    整合Transformer-XL与记忆管理
    """

    def __init__(
        self,
        config: TransformerXLConfig,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        self.config = config
        self.device = device
        self.model = TransformerXL(config).to(device)
        self.optimizer = torch.optim.Adam(
            self.model.parameters(),
            lr=0.00025
        )

        # 记忆缓存
        self.memory_cache: deque = deque(maxlen=10)
        self.segment_size = config.max_seq_len

    def process_segment(
        self,
        input_ids: torch.Tensor,
        labels: Optional[torch.Tensor] = None,
        training: bool = True
    ) -> Dict[str, Any]:
        """
        处理一个序列片段

        Args:
            input_ids: 输入token IDs
            labels: 标签（用于训练）
            training: 是否训练模式

        Returns:
            处理结果
        """
        input_ids = input_ids.to(self.device)
        if labels is not None:
            labels = labels.to(self.device)

        if training:
            self.model.train()
            self.optimizer.zero_grad()
        else:
            self.model.eval()

        # 前向传播
        logits = self.model(input_ids)

        result = {'logits': logits}

        # 计算损失
        if labels is not None:
            loss = F.cross_entropy(
                logits.view(-1, self.config.vocab_size),
                labels.view(-1)
            )
            result['loss'] = loss.item()

            if training:
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 0.25)
                self.optimizer.step()

        # 保存到缓存
        self.memory_cache.append({
            'input_ids': input_ids.cpu(),
            'logits': logits.detach().cpu()
        })

        return result

    def process_long_sequence(
        self,
        input_ids: torch.Tensor,
        labels: Optional[torch.Tensor] = None,
        training: bool = True
    ) -> Dict[str, Any]:
        """
        处理超长序列（分段处理）

        Args:
            input_ids: 长序列token IDs [total_len, batch_size]
            labels: 标签
            training: 是否训练模式

        Returns:
            处理结果
        """
        total_len = input_ids.size(0)
        num_segments = (total_len + self.segment_size - 1) // self.segment_size

        all_logits = []
        total_loss = 0

        for i in range(num_segments):
            start_idx = i * self.segment_size
            end_idx = min((i + 1) * self.segment_size, total_len)

            segment_input = input_ids[start_idx:end_idx]
            segment_labels = labels[start_idx:end_idx] if labels is not None else None

            result = self.process_segment(
                segment_input,
                segment_labels,
                training=training
            )

            all_logits.append(result['logits'])

            if 'loss' in result:
                total_loss += result['loss']

        # 合并结果
        combined_logits = torch.cat(all_logits, dim=0)

        result = {
            'logits': combined_logits,
            'num_segments': num_segments
        }

        if labels is not None:
            result['avg_loss'] = total_loss / num_segments

        return result

    def retrieve_from_memory(
        self,
        query: torch.Tensor,
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        从记忆缓存中检索

        Args:
            query: 查询向量
            top_k: 返回数量

        Returns:
            检索结果
        """
        if not self.memory_cache:
            return []

        results = []
        query = query.to(self.device)

        for mem_item in self.memory_cache:
            mem_logits = mem_item['logits'].to(self.device)

            # 计算相似度
            similarity = F.cosine_similarity(
                query.unsqueeze(0),
                mem_logits.mean(dim=0, keepdim=True),
                dim=-1
            )

            results.append({
                'similarity': similarity.item(),
                'memory': mem_item
            })

        # 排序并返回top-k
        results.sort(key=lambda x: x['similarity'], reverse=True)
        return results[:top_k]

    def save_checkpoint(self, path: str):
        """保存检查点"""
        torch.save({
            'model_state': self.model.state_dict(),
            'optimizer_state': self.optimizer.state_dict(),
            'config': self.config,
            'memory': self.model.get_memory()
        }, path)

    def load_checkpoint(self, path: str):
        """加载检查点"""
        checkpoint = torch.load(path, map_location=self.device)
        self.model.load_state_dict(checkpoint['model_state'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state'])
        if 'memory' in checkpoint:
            self.model.set_memory(checkpoint['memory'])


class MemoryIntegration:
    """
    与现有记忆系统的集成
    """

    def __init__(
        self,
        transformer_xl: TransformerXL,
        external_memory: Optional[Any] = None
    ):
        self.transformer_xl = transformer_xl
        self.external_memory = external_memory

    def integrate_memories(
        self,
        sequence: torch.Tensor,
        external_context: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        整合外部记忆到Transformer-XL

        Args:
            sequence: 输入序列
            external_context: 外部记忆上下文

        Returns:
            整合后的表示
        """
        # 获取Transformer-XL的隐藏状态
        logits, hidden_states = self.transformer_xl(
            sequence,
            return_hidden=True
        )

        # 如果有外部记忆，进行融合
        if external_context is not None:
            # 简单的拼接融合
            last_hidden = hidden_states[-1]  # [seq_len, batch, d_model]
            external_context = external_context.to(last_hidden.device)

            # 投影到相同维度
            if external_context.size(-1) != last_hidden.size(-1):
                projection = nn.Linear(
                    external_context.size(-1),
                    last_hidden.size(-1)
                ).to(last_hidden.device)
                external_context = projection(external_context)

            # 融合
            fused = last_hidden + 0.1 * external_context[:last_hidden.size(0)]
            return fused

        return hidden_states[-1]

    def extract_memory_representations(
        self,
        hidden_states: List[torch.Tensor]
    ) -> torch.Tensor:
        """
        提取记忆表示

        Args:
            hidden_states: Transformer-XL隐藏状态

        Returns:
            记忆表示
        """
        # 使用最后一层的隐藏状态作为记忆表示
        last_hidden = hidden_states[-1]

        # 平均池化
        memory_repr = last_hidden.mean(dim=0)  # [batch, d_model]

        return memory_repr


# 辅助函数
def generate_synthetic_sequence(
    length: int,
    vocab_size: int,
    batch_size: int = 1,
    device: str = "cpu"
) -> torch.Tensor:
    """生成合成序列数据"""
    return torch.randint(0, vocab_size, (length, batch_size), device=device)


def train_transformer_xl(
    model: TransformerXL,
    data: torch.Tensor,
    num_steps: int = 100,
    log_interval: int = 10
) -> List[float]:
    """训练Transformer-XL"""
    optimizer = torch.optim.Adam(model.parameters(), lr=0.00025)
    losses = []

    model.train()
    for step in range(num_steps):
        # 准备输入和目标
        input_ids = data[:-1]
        targets = data[1:]

        optimizer.zero_grad()

        # 前向传播
        logits = model(input_ids)

        # 计算损失
        loss = F.cross_entropy(
            logits.view(-1, model.config.vocab_size),
            targets.view(-1)
        )

        # 反向传播
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 0.25)
        optimizer.step()

        losses.append(loss.item())

        if (step + 1) % log_interval == 0:
            print(f"Step {step + 1}/{num_steps}, Loss: {loss.item():.4f}")

    return losses


# 使用示例
if __name__ == "__main__":
    print("=" * 70)
    print("Transformer-XL Long-term Memory Demo")
    print("=" * 70)

    # 配置
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"\nDevice: {DEVICE}")

    # 创建配置
    config = TransformerXLConfig(
        vocab_size=1000,
        d_model=256,
        n_head=4,
        d_head=64,
        d_inner=1024,
        n_layer=4,
        dropout=0.1,
        max_seq_len=64,
        mem_len=64
    )

    # 初始化模型
    print("\n[1] Initializing Transformer-XL...")
    model = TransformerXL(config).to(DEVICE)
    num_params = sum(p.numel() for p in model.parameters())
    print(f"    Model parameters: {num_params:,}")
    print(f"    Config: d_model={config.d_model}, n_layer={config.n_layer}, "
          f"mem_len={config.mem_len}")

    # 生成合成数据
    print("\n[2] Generating synthetic data...")
    seq_length = 200
    data = generate_synthetic_sequence(
        length=seq_length,
        vocab_size=config.vocab_size,
        batch_size=2,
        device=DEVICE
    )
    print(f"    Sequence length: {seq_length}, Batch size: 2")

    # 测试基础前向传播
    print("\n[3] Testing basic forward pass...")
    model.eval()
    with torch.no_grad():
        logits = model(data[:64])
        print(f"    Input shape: {data[:64].shape}")
        print(f"    Output shape: {logits.shape}")
        print(f"    Memory state: {len(model.get_memory())} layers")

    # 测试记忆机制
    print("\n[4] Testing memory mechanism...")
    model.reset_memory()

    # 第一段
    seg1 = data[:64]
    logits1 = model(seg1)
    mem_after_seg1 = [m.size(0) if m is not None else 0 for m in model.get_memory()]
    print(f"    After segment 1 (len=64): Memory sizes = {mem_after_seg1}")

    # 第二段（应该使用第一段作为记忆）
    seg2 = data[64:128]
    logits2 = model(seg2)
    mem_after_seg2 = [m.size(0) if m is not None else 0 for m in model.get_memory()]
    print(f"    After segment 2 (len=64): Memory sizes = {mem_after_seg2}")

    # 训练
    print("\n[5] Training Transformer-XL...")
    model.reset_memory()
    losses = train_transformer_xl(model, data[:, 0:1], num_steps=50, log_interval=10)
    print(f"    Final loss: {losses[-1]:.4f}")

    # 长序列处理
    print("\n[6] Testing long sequence processing...")
    long_seq = generate_synthetic_sequence(
        length=500,
        vocab_size=config.vocab_size,
        batch_size=1,
        device=DEVICE
    )

    memory_system = LongTermMemorySystem(config, device=DEVICE)
    result = memory_system.process_long_sequence(
        long_seq,
        training=False
    )
    print(f"    Long sequence length: 500")
    print(f"    Number of segments: {result['num_segments']}")
    print(f"    Output logits shape: {result['logits'].shape}")

    # 记忆检索
    print("\n[7] Testing memory retrieval...")
    query = torch.randn(1, config.d_model).to(DEVICE)
    retrieved = memory_system.retrieve_from_memory(query, top_k=3)
    print(f"    Retrieved {len(retrieved)} memories")
    for i, mem in enumerate(retrieved):
        print(f"      Memory {i+1}: similarity={mem['similarity']:.4f}")

    # 外部记忆集成
    print("\n[8] Testing external memory integration...")
    integration = MemoryIntegration(model)

    external_context = torch.randn(64, 2, config.d_model).to(DEVICE)
    fused = integration.integrate_memories(data[:64], external_context)
    print(f"    Fused representation shape: {fused.shape}")

    # 提取记忆表示
    model.eval()
    with torch.no_grad():
        _, hidden_states = model(data[:64], return_hidden=True)
        memory_repr = integration.extract_memory_representations(hidden_states)
        print(f"    Extracted memory representation shape: {memory_repr.shape}")

    # 保存和加载
    print("\n[9] Saving and loading checkpoint...")
    checkpoint_path = "/tmp/transformer_xl_checkpoint.pt"
    memory_system.save_checkpoint(checkpoint_path)

    new_memory_system = LongTermMemorySystem(config, device=DEVICE)
    new_memory_system.load_checkpoint(checkpoint_path)
    print(f"    Checkpoint saved and loaded successfully")

    # 相对位置编码演示
    print("\n[10] Relative positional encoding demonstration...")
    pos_emb = PositionalEmbedding(config.d_model)
    pos_seq = torch.arange(0, 10, dtype=torch.float32)
    pos_encoding = pos_emb(pos_seq)
    print(f"    Position sequence: {pos_seq.numpy()}")
    print(f"    Position encoding shape: {pos_encoding.shape}")
    print(f"    Sample values: {pos_encoding[0, :4].numpy()}")

    print("\n" + "=" * 70)
    print("Demo completed successfully!")
    print("=" * 70)

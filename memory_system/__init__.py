"""
Agent Memory System V3.0 - Step 3: 深度创新阶段

前沿AI模块集合，包含以下核心组件：
1. 图神经网络记忆关联引擎 (GNN Memory Engine)
2. 强化学习智能检索优化器 (RL Retrieval Optimizer)
3. 元学习自适应嵌入系统 (Meta-Learning Adaptive Embedder)
4. 神经架构搜索模块 (Neural Architecture Search Module)
5. 对比学习记忆表示 (Contrastive Memory Learning)
6. Transformer-XL长程记忆 (Transformer-XL Long-term Memory)

Author: Agent Memory System V3.0
Date: 2026-05-16
Version: 3.0.0
"""

__version__ = "3.0.0"
__author__ = "Agent Memory System V3.0"

# 图神经网络记忆关联引擎
from .gnn_memory_engine import (
    GNNMemoryEngine,
    GNNType,
    MemoryNode,
    MemoryEdge,
    TemporalGraphLayer,
    GCNMemoryLayer,
    GATMemoryLayer,
    GraphSAGEMemoryLayer,
    GINMemoryLayer,
    MemoryGraphAttention,
    create_sample_memory_graph,
    train_gnn_memory_engine
)

# 强化学习智能检索优化器
from .rl_retrieval_optimizer import (
    RLRetrievalOptimizer,
    DQNRetrievalOptimizer,
    MultiArmedBandit,
    RewardFunction,
    RetrievalState,
    RetrievalExperience,
    RetrievalAction,
    ReplayBuffer,
    DuelingDQN,
    NoisyDQN,
    create_sample_retrieval_state,
    train_rl_optimizer
)

# 元学习自适应嵌入系统
from .meta_learning_embedder import (
    MetaLearningEmbedder,
    MAML,
    PrototypicalNetwork,
    MemoryEmbedderNetwork,
    AdaptiveLearningRateScheduler,
    Task,
    MetaBatch,
    generate_synthetic_task,
    train_meta_embedder,
    evaluate_few_shot
)

# 神经架构搜索模块
from .nas_module import (
    NASModule,
    DARTSSearchNetwork,
    DARTSSearcher,
    MixedOperation,
    Cell,
    SearchSpace,
    ArchitecturePerformancePredictor,
    MultiObjectiveOptimizer,
    OperationType,
    create_dummy_loaders
)

# 对比学习记忆表示
from .contrastive_memory_learning import (
    ContrastiveMemoryLearner,
    ContrastiveMemoryEncoder,
    SimCLRLoss,
    InfoNCELoss,
    DataAugmentation,
    ContrastiveAugmentation,
    HardNegativeMiner,
    ProjectionHead,
    MemoryContrastiveDataset,
    AugmentationType,
    create_synthetic_memories,
    evaluate_contrastive_learning
)

# Transformer-XL长程记忆
from .transformer_xl_memory import (
    TransformerXL,
    TransformerXLConfig,
    TransformerXLLayer,
    RelativeMultiHeadAttention,
    PositionwiseFF,
    PositionalEmbedding,
    LongTermMemorySystem,
    MemoryIntegration,
    generate_synthetic_sequence,
    train_transformer_xl
)

__all__ = [
    # GNN Memory Engine
    'GNNMemoryEngine',
    'GNNType',
    'MemoryNode',
    'MemoryEdge',
    'TemporalGraphLayer',
    'GCNMemoryLayer',
    'GATMemoryLayer',
    'GraphSAGEMemoryLayer',
    'GINMemoryLayer',
    'MemoryGraphAttention',
    'create_sample_memory_graph',
    'train_gnn_memory_engine',

    # RL Retrieval Optimizer
    'RLRetrievalOptimizer',
    'DQNRetrievalOptimizer',
    'MultiArmedBandit',
    'RewardFunction',
    'RetrievalState',
    'RetrievalExperience',
    'RetrievalAction',
    'ReplayBuffer',
    'DuelingDQN',
    'NoisyDQN',
    'create_sample_retrieval_state',
    'train_rl_optimizer',

    # Meta-Learning
    'MetaLearningEmbedder',
    'MAML',
    'PrototypicalNetwork',
    'MemoryEmbedderNetwork',
    'AdaptiveLearningRateScheduler',
    'Task',
    'MetaBatch',
    'generate_synthetic_task',
    'train_meta_embedder',
    'evaluate_few_shot',

    # NAS Module
    'NASModule',
    'DARTSSearchNetwork',
    'DARTSSearcher',
    'MixedOperation',
    'Cell',
    'SearchSpace',
    'ArchitecturePerformancePredictor',
    'MultiObjectiveOptimizer',
    'OperationType',
    'create_dummy_loaders',

    # Contrastive Learning
    'ContrastiveMemoryLearner',
    'ContrastiveMemoryEncoder',
    'SimCLRLoss',
    'InfoNCELoss',
    'DataAugmentation',
    'ContrastiveAugmentation',
    'HardNegativeMiner',
    'ProjectionHead',
    'MemoryContrastiveDataset',
    'AugmentationType',
    'create_synthetic_memories',
    'evaluate_contrastive_learning',

    # Transformer-XL
    'TransformerXL',
    'TransformerXLConfig',
    'TransformerXLLayer',
    'RelativeMultiHeadAttention',
    'PositionwiseFF',
    'PositionalEmbedding',
    'LongTermMemorySystem',
    'MemoryIntegration',
    'generate_synthetic_sequence',
    'train_transformer_xl'
]


def get_module_info() -> dict:
    """获取模块信息"""
    return {
        'version': __version__,
        'author': __author__,
        'modules': [
            'gnn_memory_engine',
            'rl_retrieval_optimizer',
            'meta_learning_embedder',
            'nas_module',
            'contrastive_memory_learning',
            'transformer_xl_memory'
        ],
        'description': 'Agent Memory System V3.0 - Step 3: Advanced AI Modules'
    }


def check_dependencies() -> dict:
    """检查依赖项"""
    dependencies = {}

    try:
        import torch
        dependencies['torch'] = torch.__version__
    except ImportError:
        dependencies['torch'] = 'Not installed'

    try:
        import numpy
        dependencies['numpy'] = numpy.__version__
    except ImportError:
        dependencies['numpy'] = 'Not installed'

    try:
        import torch_geometric
        dependencies['torch_geometric'] = torch_geometric.__version__
    except ImportError:
        dependencies['torch_geometric'] = 'Not installed (optional for GNN)'

    try:
        import sklearn
        dependencies['sklearn'] = sklearn.__version__
    except ImportError:
        dependencies['sklearn'] = 'Not installed'

    return dependencies

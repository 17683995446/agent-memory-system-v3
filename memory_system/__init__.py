"""
Agent Memory System V3.0 - 智能记忆管理系统

一个高度模块化的分布式记忆管理系统，支持多种存储后端、
嵌入模型、检索策略和一致性保证。

核心特性:
- 分布式Raft共识协议
- 多种嵌入模型支持(OpenAI/BGE/Sentence-Transformers)
- GNN/RL/元学习驱动的智能检索
- CQRS事件溯源架构
- 混合逻辑时钟和向量时钟
- 完整的安全加固(RBAC/ABAC/加密/审计)
- 云原生部署支持(K8s Operator/Service Mesh)

示例用法:
    >>> from memory_system import AgentMemorySystem
    >>> system = AgentMemorySystem()
    >>> await system.initialize()
    >>> await system.store("user_001", "Hello world", metadata={"type": "greeting"})
"""

__version__ = "3.1.0"
__author__ = "Agent Memory System Team"
__license__ = "MIT"

import warnings
import logging

_logger = logging.getLogger(__name__)

# ============================================================================
# 核心模块导入 (始终可用的轻量级模块)
# ============================================================================

from memory_system.agent_memory_system_facade import MemorySystem as AgentMemorySystem
from memory_system.embedding_factory import EmbeddingFactory, ProviderInfo
from memory_system.security_hardening import (
    SecurityHardeningModule,
    Permission,
    CryptoManager,
    RBACManager,
    ABACManager,
    AuditLogger,
    InputValidator,
)
from memory_system.config_center import (
    ConfigCenter,
    ConfigItem,
    FileConfigStorage,
    InMemoryConfigStorage,
)
from memory_system.raft_consensus import RaftNode, RaftConfig, RaftRole
from memory_system.cqrs_system import (
    AggregateRoot,
    CommandBus,
    QueryBus,
    InMemoryEventStore,
    ConcurrencyException,
)
from memory_system.event_driven_core import EventBus, DomainEvent, InMemoryEventBus
from memory_system.async_pipeline import Pipeline, PipelineStage, PipelineConfig
from memory_system.partition_tolerance import (
    QuorumManager,
    PartitionManager,
    PartitionEvent,
)

# ============================================================================
# 重量级 AI 模块导入 (需要 torch 等依赖，使用 try/except 保护)
# ============================================================================

# 图神经网络记忆关联引擎
try:
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
except Exception as _e:
    _logger.warning(f"Failed to import gnn_memory_engine (dependency missing?): {_e}")
    GNNMemoryEngine = None
    GNNType = None
    MemoryNode = None
    MemoryEdge = None
    TemporalGraphLayer = None
    GCNMemoryLayer = None
    GATMemoryLayer = None
    GraphSAGEMemoryLayer = None
    GINMemoryLayer = None
    MemoryGraphAttention = None
    create_sample_memory_graph = None
    train_gnn_memory_engine = None

# 强化学习智能检索优化器
try:
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
except Exception as _e:
    _logger.warning(f"Failed to import rl_retrieval_optimizer (dependency missing?): {_e}")
    RLRetrievalOptimizer = None
    DQNRetrievalOptimizer = None
    MultiArmedBandit = None
    RewardFunction = None
    RetrievalState = None
    RetrievalExperience = None
    RetrievalAction = None
    ReplayBuffer = None
    DuelingDQN = None
    NoisyDQN = None
    create_sample_retrieval_state = None
    train_rl_optimizer = None

# 元学习自适应嵌入系统
try:
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
except Exception as _e:
    _logger.warning(f"Failed to import meta_learning_embedder (dependency missing?): {_e}")
    MetaLearningEmbedder = None
    MAML = None
    PrototypicalNetwork = None
    MemoryEmbedderNetwork = None
    AdaptiveLearningRateScheduler = None
    Task = None
    MetaBatch = None
    generate_synthetic_task = None
    train_meta_embedder = None
    evaluate_few_shot = None

# 神经架构搜索模块
try:
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
except Exception as _e:
    _logger.warning(f"Failed to import nas_module (dependency missing?): {_e}")
    NASModule = None
    DARTSSearchNetwork = None
    DARTSSearcher = None
    MixedOperation = None
    Cell = None
    SearchSpace = None
    ArchitecturePerformancePredictor = None
    MultiObjectiveOptimizer = None
    OperationType = None
    create_dummy_loaders = None

# 对比学习记忆表示
try:
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
except Exception as _e:
    _logger.warning(f"Failed to import contrastive_memory_learning (dependency missing?): {_e}")
    ContrastiveMemoryLearner = None
    ContrastiveMemoryEncoder = None
    SimCLRLoss = None
    InfoNCELoss = None
    DataAugmentation = None
    ContrastiveAugmentation = None
    HardNegativeMiner = None
    ProjectionHead = None
    MemoryContrastiveDataset = None
    AugmentationType = None
    create_synthetic_memories = None
    evaluate_contrastive_learning = None

# Transformer-XL长程记忆
try:
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
except Exception as _e:
    _logger.warning(f"Failed to import transformer_xl_memory (dependency missing?): {_e}")
    TransformerXL = None
    TransformerXLConfig = None
    TransformerXLLayer = None
    RelativeMultiHeadAttention = None
    PositionwiseFF = None
    PositionalEmbedding = None
    LongTermMemorySystem = None
    MemoryIntegration = None
    generate_synthetic_sequence = None
    train_transformer_xl = None

# ============================================================================
# __all__ 导出列表
# ============================================================================

__all__ = [
    # Version
    "__version__",
    # Core
    "AgentMemorySystem",
    "EmbeddingFactory",
    "ProviderInfo",
    # Security
    "SecurityHardeningModule",
    "Permission",
    "CryptoManager",
    "RBACManager",
    "ABACManager",
    "AuditLogger",
    "InputValidator",
    # Config
    "ConfigCenter",
    "ConfigItem",
    "FileConfigStorage",
    "InMemoryConfigStorage",
    # Consensus
    "RaftNode",
    "RaftConfig",
    "RaftRole",
    # CQRS
    "AggregateRoot",
    "CommandBus",
    "QueryBus",
    "InMemoryEventStore",
    "ConcurrencyException",
    # Events
    "EventBus",
    "DomainEvent",
    "InMemoryEventBus",
    # Pipeline
    "Pipeline",
    "PipelineStage",
    "PipelineConfig",
    # Partition
    "QuorumManager",
    "PartitionManager",
    "PartitionEvent",

    # GNN Memory Engine (may be None if torch missing)
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

    # RL Retrieval Optimizer (may be None if torch missing)
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

    # Meta-Learning (may be None if torch missing)
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

    # NAS Module (may be None if torch missing)
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

    # Contrastive Learning (may be None if torch missing)
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

    # Transformer-XL (may be None if torch missing)
    'TransformerXL',
    'TransformerXLConfig',
    'TransformerXLLayer',
    'RelativeMultiHeadAttention',
    'PositionwiseFF',
    'PositionalEmbedding',
    'LongTermMemorySystem',
    'MemoryIntegration',
    'generate_synthetic_sequence',
    'train_transformer_xl',
]


def get_module_info() -> dict:
    """获取模块信息"""
    return {
        'version': __version__,
        'author': __author__,
        'license': __license__,
        'modules': [
            'agent_memory_system_facade',
            'embedding_factory',
            'security_hardening',
            'config_center',
            'raft_consensus',
            'cqrs_system',
            'event_driven_core',
            'async_pipeline',
            'partition_tolerance',
            'gnn_memory_engine',
            'rl_retrieval_optimizer',
            'meta_learning_embedder',
            'nas_module',
            'contrastive_memory_learning',
            'transformer_xl_memory',
        ],
        'description': 'Agent Memory System V3.0 - 智能记忆管理系统'
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

    try:
        import cryptography
        dependencies['cryptography'] = cryptography.__version__
    except ImportError:
        dependencies['cryptography'] = 'Not installed'

    try:
        import structlog
        dependencies['structlog'] = structlog.__version__
    except ImportError:
        dependencies['structlog'] = 'Not installed'

    return dependencies

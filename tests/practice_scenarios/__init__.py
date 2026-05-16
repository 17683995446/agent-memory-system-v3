#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent Memory System V3.0 - 实践场景测试套件
Practice Scenarios Test Suite

包含5个高难度真实实践场景测试:
1. scenario1_distributed_consistency - 分布式一致性压力测试
2. scenario2_concurrent_race - 高并发竞争条件测试
3. scenario3_fault_injection - 故障注入与恢复测试
4. scenario4_resource_leak - 内存和资源泄漏测试
5. scenario5_performance_boundary - 性能瓶颈和边界测试

每个场景都设计用于暴露系统的深层缺陷。
"""

__version__ = "3.0"
__author__ = "Agent Memory System Team"

from typing import List, Dict, Any

# 场景列表
SCENARIOS = [
    {
        'id': 'scenario1_distributed_consistency',
        'name': '分布式一致性压力测试',
        'description': '模拟5节点Raft集群，网络分区下的并发写入，验证Leader切换后数据一致性',
        'target_defects': ['Split-brain', '日志不一致', 'Leader选举死锁', '提交索引错误']
    },
    {
        'id': 'scenario2_concurrent_race',
        'name': '高并发竞争条件测试',
        'description': '1000个并发客户端同时读写，测试分布式事务隔离性，验证锁正确性',
        'target_defects': ['丢失更新', '脏读', '不可重复读', '死锁', '活锁']
    },
    {
        'id': 'scenario3_fault_injection',
        'name': '故障注入与恢复测试',
        'description': '随机杀死节点，模拟磁盘损坏和网络延迟，验证故障转移和数据恢复',
        'target_defects': ['故障检测延迟', '故障转移失败', '数据恢复不完整', '状态不一致']
    },
    {
        'id': 'scenario4_resource_leak',
        'name': '内存和资源泄漏测试',
        'description': '长时间运行模拟24小时，频繁创建/删除记忆，监控资源泄漏',
        'target_defects': ['内存泄漏', '文件句柄泄漏', '连接泄漏', '线程泄漏', '缓存膨胀']
    },
    {
        'id': 'scenario5_performance_boundary',
        'name': '性能瓶颈和边界测试',
        'description': '插入100万条数据，测试索引构建和查询性能，验证分片再平衡',
        'target_defects': ['查询性能退化', '索引构建瓶颈', '分片不均衡', '内存溢出', '热点问题']
    }
]


def get_scenario_info(scenario_id: str) -> Dict[str, Any]:
    """获取场景信息"""
    for scenario in SCENARIOS:
        if scenario['id'] == scenario_id:
            return scenario
    return None


def list_scenarios() -> List[Dict[str, Any]]:
    """列出所有场景"""
    return SCENARIOS.copy()


__all__ = ['SCENARIOS', 'get_scenario_info', 'list_scenarios']

// Agent Memory System V3.0 Documentation Common Scripts

// Search functionality
const searchIndex = [
  { title: '系统概述', desc: 'Agent Memory System V3.0 简介', url: 'index.html', keywords: 'overview introduction' },
  { title: '核心特性', desc: '系统核心功能特性介绍', url: 'index.html#features', keywords: 'features capabilities' },
  { title: '架构设计', desc: '整体架构、分层架构、部署架构', url: 'architecture.html', keywords: 'architecture design' },
  { title: '整体架构图', desc: '系统整体架构设计', url: 'architecture.html#overall', keywords: 'overall architecture' },
  { title: '分层架构图', desc: '系统分层架构设计', url: 'architecture.html#layers', keywords: 'layered architecture' },
  { title: '部署架构图', desc: '系统部署架构设计', url: 'architecture.html#deployment', keywords: 'deployment' },
  { title: '数据流架构图', desc: '系统数据流架构设计', url: 'architecture.html#dataflow', keywords: 'data flow' },
  { title: '核心流程', desc: '记忆存储、检索、合并、遗忘等流程', url: 'flows.html', keywords: 'flows processes' },
  { title: '记忆存储流程', desc: '记忆存储完整流程', url: 'flows.html#store', keywords: 'store memory' },
  { title: '记忆检索流程', desc: '记忆检索完整流程', url: 'flows.html#retrieve', keywords: 'retrieve memory' },
  { title: '记忆合并流程', desc: '记忆合并完整流程', url: 'flows.html#merge', keywords: 'merge memory' },
  { title: '记忆遗忘流程', desc: '记忆遗忘完整流程', url: 'flows.html#forget', keywords: 'forget memory' },
  { title: '分布式事务流程', desc: '分布式事务处理流程', url: 'flows.html#transaction', keywords: 'transaction distributed' },
  { title: 'Raft共识流程', desc: 'Raft共识算法流程', url: 'flows.html#raft', keywords: 'raft consensus' },
  { title: '故障恢复流程', desc: '系统故障恢复流程', url: 'flows.html#recovery', keywords: 'recovery failure' },
  { title: '数据同步流程', desc: '数据同步完整流程', url: 'flows.html#sync', keywords: 'sync synchronization' },
  { title: '时序图', desc: '各类操作时序图', url: 'sequence.html', keywords: 'sequence timing' },
  { title: '客户端写入时序', desc: '客户端写入操作时序', url: 'sequence.html#write', keywords: 'client write' },
  { title: '分布式查询时序', desc: '分布式查询操作时序', url: 'sequence.html#query', keywords: 'distributed query' },
  { title: 'Leader选举时序', desc: 'Raft Leader选举时序', url: 'sequence.html#election', keywords: 'leader election' },
  { title: '故障转移时序', desc: '故障转移操作时序', url: 'sequence.html#failover', keywords: 'failover' },
  { title: '数据复制时序', desc: '数据复制操作时序', url: 'sequence.html#replication', keywords: 'replication' },
  { title: '事务提交时序', desc: '事务提交操作时序', url: 'sequence.html#commit', keywords: 'transaction commit' },
  { title: '模块详解', desc: '各模块详细说明', url: 'modules.html', keywords: 'modules components' },
  { title: '核心抽象层', desc: '核心抽象层模块', url: 'modules.html#abstraction', keywords: 'abstraction layer' },
  { title: '存储后端', desc: '存储后端模块', url: 'modules.html#storage', keywords: 'storage backend' },
  { title: '检索策略', desc: '检索策略模块', url: 'modules.html#retrieval', keywords: 'retrieval strategy' },
  { title: '嵌入提供者', desc: '嵌入提供者模块', url: 'modules.html#embedding', keywords: 'embedding provider' },
  { title: '一致性管理', desc: '一致性管理模块', url: 'modules.html#consistency', keywords: 'consistency management' },
  { title: '自愈系统', desc: '自愈系统模块', url: 'modules.html#healing', keywords: 'self-healing' },
  { title: '微服务框架', desc: '微服务框架模块', url: 'modules.html#microservice', keywords: 'microservice framework' },
  { title: '安全模块', desc: '安全模块详解', url: 'modules.html#security', keywords: 'security' },
  { title: '监控告警', desc: '监控告警模块', url: 'modules.html#monitoring', keywords: 'monitoring alerting' },
  { title: '配置中心', desc: '配置中心模块', url: 'modules.html#config', keywords: 'configuration center' },
  { title: '技术说明', desc: '技术规格说明', url: 'tech-specs.html', keywords: 'technical specifications' },
  { title: '数据模型', desc: '数据模型详细说明', url: 'tech-specs.html#datamodel', keywords: 'data model' },
  { title: 'API接口', desc: 'API接口文档', url: 'tech-specs.html#api', keywords: 'api interface' },
  { title: '配置参数', desc: '配置参数说明', url: 'tech-specs.html#config', keywords: 'configuration parameters' },
  { title: '性能指标', desc: '性能指标说明', url: 'tech-specs.html#performance', keywords: 'performance metrics' },
  { title: '部署指南', desc: '系统部署指南', url: 'tech-specs.html#deployment', keywords: 'deployment guide' }
];

// Initialize search functionality
function initSearch() {
  const searchInput = document.getElementById('searchInput');
  const searchResults = document.getElementById('searchResults');

  if (!searchInput || !searchResults) return;

  searchInput.addEventListener('input', (e) => {
    const query = e.target.value.toLowerCase().trim();

    if (query.length < 2) {
      searchResults.classList.remove('active');
      return;
    }

    const results = searchIndex.filter(item =>
      item.title.toLowerCase().includes(query) ||
      item.desc.toLowerCase().includes(query) ||
      item.keywords.toLowerCase().includes(query)
    );

    if (results.length > 0) {
      searchResults.innerHTML = results.map(item => `
        <div class="search-result-item" onclick="window.location.href='${item.url}'">
          <div class="search-result-title">${item.title}</div>
          <div class="search-result-desc">${item.desc}</div>
        </div>
      `).join('');
      searchResults.classList.add('active');
    } else {
      searchResults.innerHTML = `
        <div class="search-result-item">
          <div class="search-result-title">未找到结果</div>
          <div class="search-result-desc">请尝试其他关键词</div>
        </div>
      `;
      searchResults.classList.add('active');
    }
  });

  // Close search results when clicking outside
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-box')) {
      searchResults.classList.remove('active');
    }
  });
}

// Initialize Mermaid diagrams
function initMermaid() {
  if (typeof mermaid !== 'undefined') {
    mermaid.initialize({
      startOnLoad: true,
      theme: 'default',
      flowchart: {
        useMaxWidth: true,
        htmlLabels: true,
        curve: 'basis'
      },
      sequence: {
        useMaxWidth: true,
        showSequenceNumbers: true
      },
      gantt: {
        useMaxWidth: true
      }
    });
  }
}

// Initialize tabs
function initTabs() {
  const tabs = document.querySelectorAll('.tab');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const tabGroup = tab.closest('.tabs');
      const tabContents = tabGroup.nextElementSibling?.parentElement?.querySelectorAll('.tab-content');

      // Remove active from all tabs in group
      tabGroup.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');

      // Show corresponding content
      const targetId = tab.dataset.target;
      if (tabContents) {
        tabContents.forEach(content => {
          content.classList.remove('active');
          if (content.id === targetId) {
            content.classList.add('active');
          }
        });
      }
    });
  });
}

// Mobile menu toggle
function initMobileMenu() {
  const menuToggle = document.getElementById('menuToggle');
  const sidebar = document.getElementById('sidebar');

  if (menuToggle && sidebar) {
    menuToggle.addEventListener('click', () => {
      sidebar.classList.toggle('open');
    });

    // Close sidebar when clicking on a link (mobile)
    sidebar.querySelectorAll('a').forEach(link => {
      link.addEventListener('click', () => {
        if (window.innerWidth <= 1024) {
          sidebar.classList.remove('open');
        }
      });
    });
  }
}

// Highlight code blocks
function initCodeHighlight() {
  if (typeof hljs !== 'undefined') {
    hljs.highlightAll();
  }
}

// Smooth scroll for anchor links
function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
      e.preventDefault();
      const target = document.querySelector(this.getAttribute('href'));
      if (target) {
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
}

// Initialize all on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  initSearch();
  initMermaid();
  initTabs();
  initMobileMenu();
  initCodeHighlight();
  initSmoothScroll();
});

// Re-initialize Mermaid after dynamic content loads
window.reinitMermaid = function() {
  if (typeof mermaid !== 'undefined') {
    mermaid.init(undefined, document.querySelectorAll('.mermaid'));
  }
};

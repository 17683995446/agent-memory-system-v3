# Agent Memory System V2
# 多阶段构建，最小化镜像大小

# 阶段1: 构建依赖
FROM python:3.11-slim as builder

WORKDIR /app

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir --user -r requirements.txt

# 阶段2: 运行时镜像
FROM python:3.11-slim

WORKDIR /app

# 从构建阶段复制已安装的包
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# 复制应用代码
COPY memory_system/ ./memory_system/
COPY README.md .

# 设置 Python 环境
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# 默认配置
ENV MEMORY_STORAGE_BACKEND=memory
ENV MEMORY_EMBEDDING_PROVIDER=sentence_transformers

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from memory_system import MemorySystem; print('OK')" || exit 1

# 默认入口
ENTRYPOINT ["python", "-m", "memory_system.cli"]
CMD ["--help"]

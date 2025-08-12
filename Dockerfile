# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster Python package management
RUN pip install uv

# Copy dependency files first for better caching
COPY pyproject.toml uv.lock ./

# Install Python dependencies using uv
RUN uv sync --frozen --no-dev

# Copy application code
COPY *.py ./
COPY tests ./tests/

# Create non-root user for security
RUN useradd -m -u 1000 mcp && chown -R mcp:mcp /app
USER mcp

# Expose MCP server port (FastMCP default is 8000)
EXPOSE 8000

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV CASSANDRA_HOST=cassandra
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_DATACENTER=datacenter1

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import httpx; httpx.get('http://localhost:8000/health')" || exit 1

# Run the MCP server
CMD ["uv", "run", "python", "main.py"]
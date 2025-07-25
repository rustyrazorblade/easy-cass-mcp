# easy-cass-mcp for Apache Cassandra

A streamable MCP server that leverages virtual tables to interact with an Apache Cassandra cluster.

## Setup Instructions

1. Install uv - a fast Python package and project manager that replaces pip, pip-tools, pipx, poetry, pyenv, virtualenv, and more. Follow the installation instructions at: https://docs.astral.sh/uv/getting-started/installation/

2. Create and activate development environment:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   uv sync
   ```

**Note: This is not production ready.**


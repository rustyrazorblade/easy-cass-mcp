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

## Configuration

The MCP server can be configured using environment variables or a `.env` file. All configuration options are prefixed with `CASSANDRA_`.

### Create a Configuration File

1. Copy the example configuration:
   ```bash
   cp .env.test .env
   ```

2. Edit `.env` with your Cassandra cluster settings:
   ```bash
   # Cassandra connection settings
   CASSANDRA_CONTACT_POINTS=["localhost"]  # List of Cassandra nodes
   CASSANDRA_PORT=9042                     # Cassandra native port
   CASSANDRA_DATACENTER=datacenter1        # Your datacenter name
   CASSANDRA_USERNAME=cassandra            # Optional: authentication username
   CASSANDRA_PASSWORD=cassandra            # Optional: authentication password
   CASSANDRA_PROTOCOL_VERSION=5            # Optional: protocol version (default: 5)
   ```

### Configuration Options

| Environment Variable | Description | Default | Required |
|---------------------|-------------|---------|----------|
| `CASSANDRA_CONTACT_POINTS` | List of Cassandra contact points | `["localhost"]` | No |
| `CASSANDRA_PORT` | Cassandra native protocol port | `9042` | No |
| `CASSANDRA_DATACENTER` | Cassandra datacenter name | `datacenter1` | No |
| `CASSANDRA_USERNAME` | Authentication username | None | No |
| `CASSANDRA_PASSWORD` | Authentication password | None | No |
| `CASSANDRA_PROTOCOL_VERSION` | Cassandra protocol version | `5` | No |

## Running the Server

To run the MCP server in development mode:

```bash
# Run with uv
uv run python main.py

# Or if you have the virtual environment activated
python main.py
```

The server will:
1. Load configuration from environment variables or `.env` file
2. Connect to your Cassandra cluster
3. Start the MCP server on HTTP transport (default port: 3000)

## Using with MCP Proxy

The easy-cass-mcp server can be used with Claude Desktop or any MCP-compatible client through the MCP proxy.

### Quick Installation (Recommended)

The easiest way to install and configure the MCP proxy for Claude Desktop is:

1. First, ensure the MCP server is running:
   ```bash
   uv run python main.py
   ```

2. In another terminal, install the proxy to Claude Desktop:
   ```bash
   fastmcp install claude-desktop proxy.py:proxy
   ```

This command automatically configures Claude Desktop to use the Cassandra MCP proxy.

### Manual Configuration (Alternative)

If you prefer manual configuration, add the following to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cassandra": {
      "command": "uv",
      "args": ["run", "python", "/path/to/easy-cass-mcp/proxy.py"],
      "env": {
        "CASSANDRA_CONTACT_POINTS": "[\"localhost\"]",
        "CASSANDRA_PORT": "9042",
        "CASSANDRA_DATACENTER": "datacenter1"
      }
    }
  }
}
```

Replace `/path/to/easy-cass-mcp` with the actual path to your easy-cass-mcp directory.

### Available MCP Tools

Once connected, the following tools are available through the MCP interface:

- **`query_all_nodes`**: Execute a CQL query on all nodes in the cluster
- **`query_node`**: Execute a CQL query on a specific node
- **`get_cluster_info`**: Get detailed cluster information including topology
- **`get_node_status`**: Get status information for all nodes
- **`get_keyspace_info`**: Get information about a specific keyspace
- **`get_table_info`**: Get detailed information about a table
- **`get_virtual_tables`**: List all available virtual tables

### Example Usage in Claude

After configuring the MCP server, you can use natural language to interact with your Cassandra cluster:

- "Show me the disk usage across all nodes"
- "What's the status of my Cassandra cluster?"
- "List all keyspaces and their replication settings"
- "Show me the schema for the users table in my_keyspace"
- "Execute SELECT * FROM system_views.disk_usage on all nodes"

## Running Tests

Run the test suite using pytest:

```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run a specific test file
pytest tests/test_specific.py

# Run a specific test function
pytest tests/test_specific.py::test_function

# Run tests with coverage report
pytest --cov=.

# Run all code quality checks (formatting, linting, type checking)
black . && isort . && flake8 . && mypy .
```

**Note: This is not production ready.**


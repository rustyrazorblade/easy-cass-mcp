# easy-cass-mcp for Apache Cassandra

A streamable MCP server that leverages virtual tables to interact with an Apache Cassandra cluster.

## Quick Start

```bash
# Set up development environment
make dev

# Run tests
make test

# Start the server locally
make run

# Start with Docker Compose (includes Cassandra)
make docker-compose-up

# Build Docker image
make docker-build

# See all available commands
make help
```

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
   CASSANDRA_CONTACT_POINTS=localhost       # Comma-separated list: host1,host2,host3
   # Or use CASSANDRA_HOST for single host (Docker-friendly):
   # CASSANDRA_HOST=cassandra
   CASSANDRA_PORT=9042                     # Cassandra native port
   CASSANDRA_DATACENTER=datacenter1        # Your datacenter name
   CASSANDRA_USERNAME=cassandra            # Optional: authentication username
   CASSANDRA_PASSWORD=cassandra            # Optional: authentication password
   CASSANDRA_PROTOCOL_VERSION=5            # Optional: protocol version (default: 5)
   LOG_LEVEL=INFO                          # Optional: DEBUG, INFO, WARNING, ERROR
   ```

### Configuration Options

| Environment Variable | Description | Default | Required |
|---------------------|-------------|---------|----------|
| `CASSANDRA_CONTACT_POINTS` | Comma-separated list of contact points | `localhost` | No |
| `CASSANDRA_HOST` | Single host (alternative to CONTACT_POINTS) | None | No |
| `CASSANDRA_PORT` | Cassandra native protocol port | `9042` | No |
| `CASSANDRA_DATACENTER` | Cassandra datacenter name | `datacenter1` | No |
| `CASSANDRA_USERNAME` | Authentication username | None | No |
| `CASSANDRA_PASSWORD` | Authentication password | None | No |
| `CASSANDRA_PROTOCOL_VERSION` | Cassandra protocol version | `5` | No |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` | No |

**Note**: Use either `CASSANDRA_CONTACT_POINTS` or `CASSANDRA_HOST`, not both. `CASSANDRA_HOST` is preferred for Docker environments.

### Test Configuration

Additional environment variables for testing:

| Environment Variable | Description | Default | Required |
|---------------------|-------------|---------|----------|
| `CASSANDRA_TEST_KEYSPACE` | Keyspace for integration tests | `mcp_test` | No |
| `CASSANDRA_TEST_CONTACT_POINTS` | Override contact points for tests | None | No |
| `CASSANDRA_TEST_DATACENTER` | Override datacenter for tests | None | No |
| `CLEANUP_TEST_DATA` | Clean up test data after tests | `false` | No |

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
3. Start the MCP server on HTTP transport (default port: 8000)

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
   fastmcp install claude-desktop ecm/proxy.py:proxy
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
        "CASSANDRA_HOST": "localhost",
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

## Docker Support

### Quick Start with Docker

The easiest way to run the Cassandra MCP server is using Docker:

```bash
# Build and run with docker-compose (includes Cassandra)
make docker-compose-up

# Or build just the MCP server image
make docker-build

# Run with an existing Cassandra cluster
make docker-run CASSANDRA_HOST=your-cassandra-host

# Build a release (runs tests and builds Docker image)
make release

# Push to Docker registry
make docker-push DOCKER_REGISTRY=your-registry.com
```

### Docker Configuration

The Docker container supports the following environment variables:

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `CASSANDRA_HOST` | Cassandra hostname or IP (preferred for Docker) | `cassandra` |
| `CASSANDRA_CONTACT_POINTS` | Alternative: comma-separated list of hosts | None |
| `CASSANDRA_PORT` | Cassandra native protocol port | `9042` |
| `CASSANDRA_DATACENTER` | Cassandra datacenter name | `datacenter1` |
| `CASSANDRA_USERNAME` | Authentication username | None |
| `CASSANDRA_PASSWORD` | Authentication password | None |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` |

**Note**: For Docker environments, `CASSANDRA_HOST` is preferred over `CASSANDRA_CONTACT_POINTS` for single-host connections.

### Docker Compose

The `docker-compose.yml` file includes:
- **mcp-server**: The Cassandra MCP server (port 8000)
- **cassandra**: A Cassandra 4.1 instance for local development (port 9042)

To use with your own Cassandra cluster, modify the environment variables in `docker-compose.yml` or create a `.env` file:

```bash
# .env file for docker-compose
CASSANDRA_HOST=my-cassandra-cluster.example.com
# Or for multiple hosts:
# CASSANDRA_CONTACT_POINTS=host1.example.com,host2.example.com,host3.example.com
CASSANDRA_PORT=9042
CASSANDRA_DATACENTER=datacenter1
CASSANDRA_USERNAME=myuser
CASSANDRA_PASSWORD=mypassword
LOG_LEVEL=INFO
```

### Production Deployment

For production deployments:

1. Use specific image tags instead of `latest`
2. Configure resource limits in docker-compose or Kubernetes
3. Use secrets management for credentials
4. Enable TLS/SSL for Cassandra connections
5. Configure appropriate health checks and monitoring

Example production docker-compose override:

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  mcp-server:
    image: easy-cass-mcp:0.0.1
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: '0.5'
        reservations:
          memory: 256M
    environment:
      - LOG_LEVEL=WARNING
    secrets:
      - cassandra_password
    restart: always

secrets:
  cassandra_password:
    external: true
```

## Makefile Commands

The project includes a comprehensive Makefile for common development tasks:

### Development Commands
- `make help` - Show all available commands
- `make dev` - Set up development environment
- `make install` - Install Python dependencies
- `make install-dev` - Install all dependencies including dev
- `make run` - Run the MCP server locally

### Testing & Quality
- `make test` - Run all tests
- `make test-coverage` - Run tests with coverage report
- `make format` - Format code with black and isort
- `make lint` - Run linting checks
- `make check` - Run all quality checks (format, lint, test)

### Docker Commands
- `make docker-build` - Build Docker image
- `make docker-run` - Run Docker container
- `make docker-push` - Push image to registry
- `make docker-compose-up` - Start services with docker-compose
- `make docker-compose-down` - Stop services
- `make docker-compose-logs` - Show logs
- `make docker-compose-clean` - Stop and remove volumes

### Release Commands
- `make release` - Build a release (tests + Docker build)
- `make release-patch` - Create patch version (x.y.Z+1)
- `make release-minor` - Create minor version (x.Y+1.0)
- `make release-major` - Create major version (X+1.0.0)
- `make version` - Show current version

## Running Tests

Run the test suite using the Makefile:

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run all code quality checks
make check

# Or use pytest directly
pytest -v
pytest --cov=.
```

**Note: This is not production ready.**


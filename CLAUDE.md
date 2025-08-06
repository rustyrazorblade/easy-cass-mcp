# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Cassandra MCP (Model Context Protocol) server built using FastMCP, a Python framework for creating MCP servers with streamable HTTP support. The server MUST use FastMCP's streamable HTTP transport for all communications.

Use the following reference: https://gofastmcp.com/getting-started/welcome

## Development Commands

### Package Management
This project uses `uv` for Python package management:
- `uv sync` - Sync all dependencies from pyproject.toml
- `uv add <package>` - Add a new dependency
- `uv add --dev <package>` - Add a new development dependency
- `uv lock` - Update the lock file

### Running the Server
- `python main.py` - Run the MCP server
- `uv run python main.py` - Run using uv's Python environment

### Development Tools
- `black .` - Format code using Black
- `isort .` - Sort imports
- `flake8 .` - Run linting checks
- `mypy .` - Run type checking
- `pytest` - Run tests (when tests are added)
- `pytest -v` - Run tests with verbose output
- `pytest tests/test_specific.py::test_function` - Run a specific test

### Running All Checks
```bash
# Format and lint
black . && isort . && flake8 . && mypy .

# Run tests
pytest
```

## Project Architecture

### Core Dependencies
- **FastMCP (>=2.10.6)**: Framework for building MCP servers with streamable HTTP support
- **httpx (>=0.28.1)**: HTTP client library for making requests to Cassandra
- **Pydantic (>=2.11.7)**: Data validation and settings management

### Development Dependencies
- **black**: Code formatting
- **flake8**: Linting
- **isort**: Import sorting
- **mypy**: Type checking
- **pytest**: Testing framework
- **pytest-asyncio**: Async test support

### MCP Server Structure
When implementing the Cassandra MCP server, follow the FastMCP patterns:

1. **Server Definition**: Use FastMCP decorators to define server metadata and tools
2. **Tool Implementation**: Create tools for Cassandra operations (health checks, metrics, maintenance tasks)
3. **Async Support**: Use async/await for Cassandra operations to handle concurrent requests
4. **Error Handling**: Implement proper error handling for Cassandra connection issues

### Typical MCP Server Pattern
```python
from fastmcp import FastMCP

mcp = FastMCP("easy-cass-mcp")

@mcp.tool()
async def cassandra_health():
    """Check Cassandra cluster health"""
    # Implementation here
    pass
```

### FastMCP Async Usage
FastMCP provides both synchronous and asynchronous APIs:
- Use `mcp.run()` in synchronous contexts (regular functions)
- Use `await mcp.run_async()` in async contexts (async functions)

**Important**: The `run()` method cannot be called from inside an async function because it creates its own async event loop. Always use `run_async()` inside async functions.

Example async pattern:
```python
import asyncio
from fastmcp import FastMCP

mcp = FastMCP(name="MyServer")

async def main():
    # Setup any async resources
    await setup_connections()
    
    # Use run_async() in async contexts
    await mcp.run_async(transport="http")

if __name__ == "__main__":
    asyncio.run(main())
```

## Cassandra Integration 

All interactions should be handled through CQL and virtual tables.

### Using Abstractions

When implementing MCP tools, always favor using abstractions over raw Cassandra calls:

1. **CassandraUtility**: Use this for cluster-wide operations
   - `get_version()`: Always use this instead of manually querying system.local for version
   - `get_table(keyspace, table)`: Use this to create CassandraTable objects
   - The utility first checks if version is available in the driver metadata before falling back to system tables

2. **CassandraTable**: Use this for table-specific operations
   - `get_compaction_strategy()`: Returns compaction strategy class and options
   - `get_create_statement()`: Returns the CREATE TABLE statement
   - Provides a clean abstraction over raw metadata access

Example usage in MCP tools:
```python
# Good - using abstractions
utility = CassandraUtility(session)
version = await utility.get_version()
table = utility.get_table(keyspace, table_name)
compaction = await table.get_compaction_strategy()

# Avoid - raw queries for metadata
result = await session.execute("SELECT release_version FROM system.local")
```

### Node-Specific Queries

The MCP server supports executing queries on specific nodes or all nodes in the cluster. This is essential for querying virtual tables and node-local system tables.

#### Available MCP Tools for Node-Specific Queries:
1. **query_all_nodes**: Executes a query on all nodes in the cluster and returns results per node
2. **query_node**: Executes a query on a specific node

#### Example Usage:
```python
# Query virtual tables on all nodes
await query_all_nodes("SELECT * FROM system_views.disk_usage")

# Query system.local on a specific node
await query_node("192.168.1.1", "SELECT * FROM system.local")
```

#### Implementation Details:
- Uses `WhiteListRoundRobinPolicy` to target specific nodes
- Creates execution profiles dynamically for each node
- Uses `ConsistencyLevel.ONE` for node-local queries
- Executes queries concurrently on all nodes for better performance
- Handles failures gracefully (returns error message for failed nodes)

## Testing Strategy

When adding tests:
1. Use `pytest-asyncio` for testing async MCP tools
2. Mock Cassandra connections in unit tests
3. Create integration tests with a test Cassandra instance if available
4. Test error scenarios (connection failures, timeouts, etc.)

- Do not write tests against Mocked Cassandra connections unless testing something higher in the stack, such as how the MCP handles errors that return. 

## Code Style Guidelines

1. Follow PEP 8 (enforced by Black and flake8)
2. Use type hints for all function signatures
3. Document MCP tools with clear docstrings (these become tool descriptions)
4. Keep tool functions focused on single responsibilities
5. Use async/await for I/O operations

## Best Practices
- Always add comments for new classes, functions, and methods.

## Testing Principles
- Always write new code so it can be tested in isolation.  Ensure new MCP calls have tests around their functionality as well as all the layers below.

## Workflow Practices

- Always run pytest when finishing a coding task, summarize the errors, and show the plan for how to fix.

## MCP Development

### Architectural Principles

**IMPORTANT: The MCP server should be a very light shim over classes that implement the functionality, to make them easier to test.**

The MCP server (`mcp_server.py`) should follow these principles:

1. **Minimal Business Logic**: MCP tool functions should contain minimal business logic. They should:
   - Parse and validate input parameters
   - Call appropriate service methods
   - Format the response for display
   - Handle top-level errors gracefully

2. **Service Layer Pattern**: All business logic should be in service classes:
   - `CassandraService`: Core Cassandra operations (queries, metadata)
   - `CompactionAnalyzer`: Table optimization analysis
   - `CassandraUtility`: Utility operations and abstractions
   - Additional service classes for new domains

3. **Testing Strategy**: This architecture enables:
   - Unit testing of service methods in isolation
   - Mocking service dependencies for MCP tool tests
   - Integration testing with real Cassandra instances
   - Clear separation of concerns

Example of proper MCP tool structure:
```python
@mcp.tool(description="Clear, concise description")
async def mcp_tool_name(param1: str, param2: int) -> str:
    """Tool docstring for MCP clients."""
    try:
        # Simple input validation
        if not param1:
            return "Error: param1 is required"
        
        # Delegate to service layer
        result = await service.business_logic_method(param1, param2)
        
        # Format response for display
        return format_result_for_display(result)
        
    except SpecificException as e:
        logger.error(f"Error in tool: {e}")
        return f"Error: {str(e)}"
```

### Adding New MCP Capabilities
When extending the MCP server with new functionality:

1. **Create or extend a service class** with the business logic
2. **Add comprehensive unit tests** for the service methods
3. **Add a thin MCP tool wrapper** that delegates to the service
4. **Test the MCP tool** with mocked service dependencies

Example workflow:
```python
# 1. In cassandra_service.py or new service file
class CassandraService:
    async def new_business_logic(self, param: str) -> Dict[str, Any]:
        """Implement actual functionality here with full logic."""
        # Complex business logic
        # Error handling
        # Data processing
        return processed_result

# 2. In test_cassandra_service.py
async def test_new_business_logic():
    """Test the service method thoroughly."""
    service = CassandraService(mock_connection)
    result = await service.new_business_logic("test")
    assert result == expected

# 3. In mcp_server.py
@mcp.tool(description="User-friendly description")
async def new_mcp_tool(param: str) -> str:
    """Minimal wrapper around service method."""
    try:
        result = await service.new_business_logic(param)
        return format_for_display(result)
    except Exception as e:
        return f"Error: {str(e)}"

# 4. In test_mcp_server.py  
async def test_new_mcp_tool():
    """Test MCP tool with mocked service."""
    mock_service = Mock()
    mock_service.new_business_logic.return_value = test_data
    # Test the MCP tool behavior
```

All new MCP methods should follow the conventions established by existing tools in the file.
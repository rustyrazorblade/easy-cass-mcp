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
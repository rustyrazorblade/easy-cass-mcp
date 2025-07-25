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

## Running the Server

To run the MCP server in development mode:

```bash
# Run with uv
uv run python main.py

# Or if you have the virtual environment activated
python main.py
```

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


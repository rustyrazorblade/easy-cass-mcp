from fastmcp import FastMCP

# Create a proxy to a remote server
proxy = FastMCP.as_proxy("http://127.0.0.1:8000/mcp", name="Proxy to Cassandra MCP")

if __name__ == "__main__":
    proxy.run()  # Runs via STDIO for Claude Desktop

# Publishing to Docker Hub

This guide explains how to publish the Cassandra MCP server to Docker Hub.

## Prerequisites

1. Create a Docker Hub account at https://hub.docker.com
2. Install Docker on your machine
3. Choose your Docker Hub username/organization (e.g., `yourusername`)

## Step 1: Login to Docker Hub

```bash
docker login
# Enter your Docker Hub username and password when prompted
```

## Step 2: Build and Tag the Image

The Makefile makes this easy. Replace `yourusername` with your Docker Hub username:

```bash
# Build the image with proper naming for Docker Hub
make docker-build DOCKER_REGISTRY=docker.io IMAGE_NAME=yourusername/cassandra-mcp-server

# Or manually:
docker build -t yourusername/cassandra-mcp-server:latest .
docker tag yourusername/cassandra-mcp-server:latest yourusername/cassandra-mcp-server:0.1.0
```

## Step 3: Push to Docker Hub

```bash
# Using Makefile (recommended)
make docker-push DOCKER_REGISTRY=docker.io IMAGE_NAME=yourusername/cassandra-mcp-server

# Or manually:
docker push yourusername/cassandra-mcp-server:latest
docker push yourusername/cassandra-mcp-server:0.1.0
```

## Step 4: Create Release with Version Tag

```bash
# Create a new version tag
make release-minor  # Creates v0.2.0
git push --tags     # Push tag to GitHub

# Build and push versioned image
make release DOCKER_REGISTRY=docker.io IMAGE_NAME=yourusername/cassandra-mcp-server
make docker-push DOCKER_REGISTRY=docker.io IMAGE_NAME=yourusername/cassandra-mcp-server
```

## Complete Workflow Example

Here's the complete workflow for publishing to `rustyrazorblade/cassandra-mcp-server`:

```bash
# 1. Login to Docker Hub
docker login

# 2. Create a new version
make release-minor
git push --tags

# 3. Build and test
make check  # Run all tests and linting

# 4. Build Docker image
make docker-build DOCKER_REGISTRY=docker.io IMAGE_NAME=rustyrazorblade/cassandra-mcp-server

# 5. Test locally
docker run -p 8000:8000 rustyrazorblade/cassandra-mcp-server:latest

# 6. Push to Docker Hub
make docker-push DOCKER_REGISTRY=docker.io IMAGE_NAME=rustyrazorblade/cassandra-mcp-server
```

## Automated Publishing with GitHub Actions

Create `.github/workflows/docker-publish.yml`:

```yaml
name: Docker Publish

on:
  push:
    tags:
      - 'v*'  # Trigger on version tags

jobs:
  docker:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
      
      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3
      
      - name: Login to Docker Hub
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}
      
      - name: Extract metadata
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: rustyrazorblade/cassandra-mcp-server
          tags: |
            type=ref,event=tag
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            type=semver,pattern={{major}}
            type=raw,value=latest,enable={{is_default_branch}}
      
      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

To use GitHub Actions:
1. Go to GitHub repository settings
2. Add secrets:
   - `DOCKERHUB_USERNAME`: Your Docker Hub username
   - `DOCKERHUB_TOKEN`: Docker Hub access token (create at https://hub.docker.com/settings/security)
3. Push a version tag: `git tag v0.2.0 && git push --tags`
4. GitHub Actions will automatically build and push to Docker Hub

## Docker Hub Repository Setup

1. Go to https://hub.docker.com/repository/create
2. Create repository: `cassandra-mcp-server`
3. Add description:
   ```
   Cassandra MCP Server - Model Context Protocol server for Apache Cassandra
   
   A streamable MCP server that leverages virtual tables to interact with Apache Cassandra clusters.
   ```
4. Add README from this repository
5. Set up Automated Builds (optional):
   - Link to GitHub repository
   - Configure build rules for tags

## Using the Published Image

Once published, users can run your image:

```bash
# Pull and run latest version
docker run -p 8000:8000 \
  -e CASSANDRA_HOST=cassandra.example.com \
  yourusername/cassandra-mcp-server:latest

# Use specific version
docker run -p 8000:8000 \
  -e CASSANDRA_HOST=cassandra.example.com \
  yourusername/cassandra-mcp-server:0.1.0

# With docker-compose
services:
  mcp-server:
    image: yourusername/cassandra-mcp-server:latest
    environment:
      - CASSANDRA_HOST=cassandra
```

## Multi-Architecture Builds

To support both AMD64 and ARM64 (Apple Silicon):

```bash
# Create and use buildx builder
docker buildx create --name multiarch --use

# Build and push multi-arch image
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t yourusername/cassandra-mcp-server:latest \
  --push .
```

## Best Practices

1. **Versioning**: Always tag with semantic versions (0.1.0, 0.2.0, etc.)
2. **Latest Tag**: Keep `latest` pointing to the most recent stable release
3. **Documentation**: Update Docker Hub README with each release
4. **Security**: Never include secrets in the image
5. **Size**: Keep images small using multi-stage builds
6. **Testing**: Test the image locally before pushing
7. **Scanning**: Use `docker scout` to scan for vulnerabilities:
   ```bash
   docker scout cves yourusername/cassandra-mcp-server:latest
   ```

## Quick Commands (Makefile Configured)

Since the Makefile is already configured with `rustyrazorblade/cassandra-mcp-server`, you can simply use:

```bash
# Build with default settings
make docker-build

# Push to Docker Hub
make docker-push

# Complete release
make release
```

No need to specify DOCKER_REGISTRY or IMAGE_NAME - they're already set!

## Troubleshooting

**Authentication failed:**
```bash
# Re-login to Docker Hub
docker logout
docker login
```

**Permission denied:**
```bash
# Make sure you own the repository name on Docker Hub
# Repository must be created before first push
```

**Image too large:**
- Check `.dockerignore` is working
- Use multi-stage builds
- Remove unnecessary dependencies

**Rate limiting:**
- Docker Hub has pull rate limits for anonymous users
- Authenticated users get higher limits
- Consider using GitHub Container Registry as alternative
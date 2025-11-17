## Docker

The Docker image is automatically built and pushed to GitHub Container Registry on pushes to the main branch.

To pull the latest image:
```bash
docker pull ghcr.io/ladybugdb/bolt4rs:latest
```

To build locally:
```bash
docker build -f docker/Dockerfile -t bolt4rs-server .
```

To run:
```bash
docker run -d -v $(pwd)/logs:/app/logs -v $(pwd)/data:/app/data -p 7687:7687 bolt4rs-server
```

# Run your client such as:
./target/debug/test_client
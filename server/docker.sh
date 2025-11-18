mkdir -p data logs
docker run -d  -v $(pwd)/logs:/app/logs -v $(pwd)/data:/app/data -p 7687:7687 -p 7688:7688 ghcr.io/ladybugdb/bolt4rs:main

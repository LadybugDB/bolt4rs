docker build -f docker/Dockerfile -t bolt4rs-server .
docker run -v $(pwd)/logs:/app/logs -v $(pwd)/data:/app/data -p 7687:7687 bolt4rs-server

# Build stage
FROM rust:alpine AS builder

# Install musl-dev and build tools for static linking
RUN apk add --no-cache musl-dev build-base lld cmake

# Set the working directory inside the container
WORKDIR /app

# Copy the source code into the container
COPY . .

# Build the project in release mode for the specific binaries
RUN cargo build --release --bin bolt4rs-server --bin test_client

# Runtime stage
FROM alpine:latest

# Install ca-certificates if needed (for HTTPS, but probably not necessary here)
RUN apk --no-cache add ca-certificates

# Set the working directory inside the container
WORKDIR /app

# Copy the built binaries from the builder stage
COPY --from=builder /app/target/release/bolt4rs-server /app/bolt4rs-server
COPY --from=builder /app/target/release/test_client /app/test_client

# Expose bolt port
EXPOSE 7687
EXPOSE 7688

# Set logging level
ENV RUST_LOG=info

# Create directories for mounts
RUN mkdir -p /app/data /app/logs

# Command to run the binary and log to file
CMD ["sh", "-c", "cd /app && ./bolt4rs-server > /app/logs/server.log 2>&1"]
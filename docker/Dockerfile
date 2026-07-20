# Build stage
FROM rust:slim-trixie AS builder

# Install build tools plus the downloader dependencies used by the lbug crate.
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    build-essential \
    ca-certificates \
    curl \
    lld \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy the source code into the container
COPY . .

# Build against the self-contained static liblbug archive matching the Cargo dependency.
RUN set -eux; \
    LBUG_VERSION="$(sed -n 's/^lbug = .*version = "[>=<~^ ]*\([0-9][^", ]*\)".*/\1/p' server/Cargo.toml)"; \
    test -n "$LBUG_VERSION"; \
    export LBUG_VERSION; \
    cargo build --release --bin bolt4rs-server --bin test_client

# Runtime stage
FROM debian:trixie-slim

# Install runtime dependencies for the C++ runtime linked by the server binary.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

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

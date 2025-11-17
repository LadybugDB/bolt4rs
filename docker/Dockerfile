# Use the official Rust image as the base image
FROM rust:latest

# Set the working directory inside the container
WORKDIR /app

# Clone the repository into the container
RUN git clone https://github.com/LadybugDB/bolt4rs .

# Install lld for linking
RUN apt-get update && apt-get install -y lld

# Install build-essential and cmake
RUN apt-get install -y build-essential cmake

# Build the project in release mode for the specific binary
RUN cargo build --release --bin bolt4rs-server

# Expose bolt port
EXPOSE 7687

# Set logging level
ENV RUST_LOG=info

# Command to run the binary
CMD ["./target/release/bolt4rs-server"]
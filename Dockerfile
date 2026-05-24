# =============================================================================
# CronosDB Dockerfile - Multi-stage build for Linux
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Build Rust bloom filter
# -----------------------------------------------------------------------------
FROM rust:1.94 AS rust-builder

WORKDIR /build/rust-dedup

# Install build dependencies for the library
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy Rust source (no build.rs needed - this is a library crate)
COPY internal/dedup/rust/src ./src
COPY internal/dedup/rust/Cargo.toml ./
COPY internal/dedup/rust/Cargo.lock ./

# Build release library for the host target (x86_64-unknown-linux-gnu)
RUN cargo build --release

# Copy output to known location
RUN cp target/release/libcronos_dedup.so /build/libcronos_dedup.so

# -----------------------------------------------------------------------------
# Stage 2: Build Go application
# -----------------------------------------------------------------------------
FROM golang:1.25-bookworm AS go-builder

# Install build dependencies for cgo
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build/go-app

# Copy Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Copy pre-built Rust library with correct name
COPY --from=rust-builder /build/libcronos_dedup.so /build/go-app/libcronos_dedup.so

ENV CGO_ENABLED=1
ENV CGO_LDFLAGS="-L/build/go-app -lcronos_dedup"

# Build the main binary
RUN go build -ldflags="-linkmode=external" -o /cronos-api ./cmd/api/main.go

# -----------------------------------------------------------------------------
# Stage 3: Final runtime image (Debian for glibc compatibility)
# =============================================================================
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -g 1000 cronos && \
    useradd -u 1000 -g cronos -s /bin/sh -m cronos

WORKDIR /app

# Copy binary from go-builder
COPY --from=go-builder /cronos-api /app/cronos-api

# Copy Rust library (must be in same directory as binary or in library path)
COPY --from=rust-builder /build/libcronos_dedup.so /app/cronos_dedup.so

# Also copy to system library path
COPY --from=rust-builder /build/libcronos_dedup.so /usr/lib/x86_64-linux-gnu/libcronos_dedup.so

# Create data directory structure
RUN mkdir -p /data/partitions && chown -R cronos:cronos /data

# Environment defaults
ENV CRONOS_DATA_DIR=/data
ENV CRONOS_NODE_ID=cronos-node-1
ENV CRONOS_GRPC_ADDR=0.0.0.0:9000
ENV CRONOS_HTTP_ADDR=0.0.0.0:8080
ENV CRONOS_CLUSTER=false
ENV LD_LIBRARY_PATH=/app

# Expose ports
# gRPC: 9000, HTTP: 8080, Cluster gossip: 7946, Cluster gRPC: 7947, Raft: 7948
EXPOSE 9000 8080 7946 7947 7948

# Volume for persistent data
VOLUME ["/data"]

# Switch to non-root user
USER cronos

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Entry point
ENTRYPOINT ["/app/cronos-api"]
CMD ["--data-dir=/data"]

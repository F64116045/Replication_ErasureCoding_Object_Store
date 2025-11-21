# -----------------------------------------------------------------------------
# Stage 1: Builder
# Compiles the Go binaries.
# -----------------------------------------------------------------------------
FROM golang:1.24-alpine AS builder

WORKDIR /app
# Git is required for fetching Go dependencies
RUN apk add --no-cache git

# 1. Copy Source Code
# Copy everything to ensure 'go mod tidy' can see all internal packages.
COPY . .

# 2. Resolve Dependencies
# Run 'go mod tidy' inside the container to ensure go.mod/go.sum 
# match the source code and Go 1.24 environment.
RUN go mod tidy

# 3. Build Microservices
# We build three separate binaries: API Gateway, Storage Node, and Healer.
# CGO_ENABLED=0 ensures static linking .
# -ldflags="-s -w" strips debug information to reduce binary size.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/local/bin/api ./cmd/api
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/local/bin/storage_node ./cmd/storage_node
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /usr/local/bin/healer ./cmd/healer

# -----------------------------------------------------------------------------
# Stage 2: Release
# Creates the final, minimal runtime image.
# -----------------------------------------------------------------------------
FROM alpine:3.18

# CA Certificates are required for making HTTPS requests (if needed)
RUN apk add --no-cache ca-certificates

# Copy the compiled binaries from the builder stage
COPY --from=builder /usr/local/bin/api /usr/local/bin/api
COPY --from=builder /usr/local/bin/storage_node /usr/local/bin/storage_node
COPY --from=builder /usr/local/bin/healer /usr/local/bin/healer

# Default entrypoint (Overridden by docker-compose.yaml 'command')
CMD ["/usr/local/bin/api"]
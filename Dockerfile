# Production image: static binary on distroless (CA certificates + tzdata, no shell), non-root.
#
#   docker build -t imagelib .
#   docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/imagelib .

FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY gen ./gen
COPY schema ./schema
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/imagelib ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/imagelib /imagelib

# Images are read from LOCAL_PATH; mount the host directory there (read-only is enough).
ENV ADDR=:8080 \
    LOCAL_PATH=/data
EXPOSE 8080
USER nonroot:nonroot

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/imagelib", "healthcheck"]
ENTRYPOINT ["/imagelib"]

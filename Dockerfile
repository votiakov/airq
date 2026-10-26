# Multi-stage build producing two images:
#   runtime    (default) the airq service
#   mockvendor           the Open-Meteo emulator used for chaos drills and CI smoke tests
#
# Base images are pinned by digest (Dependabot keeps them current). The final images
# are distroless "static" running as the nonroot user: no shell, no package manager,
# CA certificates and tzdata only.

ARG GO_IMAGE=golang:1.27.3-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download && go mod verify
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
ARG TARGETOS TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_TIME=""
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    pkg=github.com/votiakov/airq/internal/version && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
      -ldflags "-s -w -X ${pkg}.version=${VERSION} -X ${pkg}.revision=${REVISION} -X ${pkg}.buildTime=${BUILD_TIME}" \
      -o /out/airq ./cmd/airq && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags "-s -w" -o /out/mockvendor ./cmd/mockvendor

FROM ${RUNTIME_IMAGE} AS mockvendor
COPY --from=build /out/mockvendor /mockvendor
USER nonroot:nonroot
EXPOSE 8090
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 CMD ["/mockvendor", "healthcheck"]
ENTRYPOINT ["/mockvendor"]

FROM ${RUNTIME_IMAGE} AS runtime
LABEL org.opencontainers.image.title="airq" \
      org.opencontainers.image.description="Air quality API backed by Open-Meteo" \
      org.opencontainers.image.source="https://github.com/votiakov/airq"
COPY --from=build /out/airq /airq
USER nonroot:nonroot
EXPOSE 8080
# The binary probes its own /livez: distroless has no shell or curl.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/airq", "healthcheck"]
ENTRYPOINT ["/airq"]
CMD ["serve"]

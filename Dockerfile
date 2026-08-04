# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build
WORKDIR /src

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
ARG TARGETOS=linux
ARG TARGETARCH

COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w -X github.com/k3rnL/riquet/internal/buildinfo.version=${VERSION} -X github.com/k3rnL/riquet/internal/buildinfo.commit=${COMMIT} -X github.com/k3rnL/riquet/internal/buildinfo.date=${BUILD_DATE}" \
    -o /out/riquet ./cmd/riquet
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o /out/riquet-backup ./cmd/riquet-backup && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o /out/riquet-export ./cmd/riquet-export && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o /out/riquet-restore ./cmd/riquet-restore

FROM scratch
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="Riquet" \
      org.opencontainers.image.description="Standalone Confluent-compatible Schema Registry" \
      org.opencontainers.image.source="https://github.com/k3rnL/riquet" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/riquet /riquet
COPY --from=build /out/riquet-backup /riquet-backup
COPY --from=build /out/riquet-export /riquet-export
COPY --from=build /out/riquet-restore /riquet-restore
USER 65532:65532
ENTRYPOINT ["/riquet"]

FROM golang:1.26.5-bookworm AS build
ARG TARGETOS=linux
ARG TARGETARCH=arm64
ARG NARUMQ_RELEASE_VERSION
ARG NARUMQ_FEATURES
WORKDIR /src
COPY . .
RUN test -n "${NARUMQ_RELEASE_VERSION}" && test -n "${NARUMQ_FEATURES}" \
    && go run ./tools/release-metadata -version "${NARUMQ_RELEASE_VERSION}" -features "${NARUMQ_FEATURES}" \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags="-s -w -X main.buildReleaseVersion=${NARUMQ_RELEASE_VERSION} -X main.buildFeatures=${NARUMQ_FEATURES}" \
      -o /out/narumq ./cmd/narumq

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
ARG NARUMQ_RELEASE_VERSION
ARG NARUMQ_FEATURES
COPY --from=build /out/narumq /narumq
LABEL org.opencontainers.image.version="${NARUMQ_RELEASE_VERSION}" \
      io.narumq.features="${NARUMQ_FEATURES}"
ENTRYPOINT ["/narumq"]

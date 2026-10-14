FROM golang:1.26.5-bookworm AS build
ARG TARGETOS=linux
ARG TARGETARCH=arm64
ARG NARUMQ_CONTROLLER_VERSION
WORKDIR /src
COPY . .
RUN test -n "${NARUMQ_CONTROLLER_VERSION}" \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags="-s -w -X main.buildVersion=${NARUMQ_CONTROLLER_VERSION}" \
      -o /out/narumq-rollout-controller ./cmd/narumq-rollout-controller

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/narumq-rollout-controller /narumq-rollout-controller
ARG NARUMQ_CONTROLLER_VERSION
USER 65532:65532
LABEL org.opencontainers.image.version="${NARUMQ_CONTROLLER_VERSION}" \
      io.narumq.component="rollout-controller"
ENTRYPOINT ["/narumq-rollout-controller"]

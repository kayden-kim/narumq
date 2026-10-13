FROM golang:1.26.5-bookworm AS build
ARG TARGETOS=linux
ARG TARGETARCH=arm64
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/narumq ./cmd/narumq

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/narumq /narumq
ENTRYPOINT ["/narumq"]

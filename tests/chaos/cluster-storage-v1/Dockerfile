FROM golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/narumq ./cmd/narumq
WORKDIR /src/tests/chaos/cluster-storage-v1/node
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/narumq-chaosnode .

FROM scratch AS chaos-node
COPY --from=builder /out/narumq /narumq
COPY --from=builder /out/narumq-chaosnode /narumq-chaosnode
ENTRYPOINT ["/narumq-chaosnode"]

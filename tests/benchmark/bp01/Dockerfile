# The planned digest 3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 was not published for this tag. The immutable registry-resolved digest below is recorded in the run manifest.
FROM golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/narumq ./cmd/narumq \
    && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/bp01 ./tests/benchmark/bp01

FROM scratch AS narumq
COPY --from=build /out/narumq /narumq
ENTRYPOINT ["/narumq"]

FROM scratch AS runner
COPY --from=build /out/bp01 /bp01
ENTRYPOINT ["/bp01"]

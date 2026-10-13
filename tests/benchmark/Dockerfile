# The planned digest 3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 was not published for this tag. The immutable registry-resolved digest below is recorded in the run manifest.
FROM golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/narumq ./cmd/narumq \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/bp01 ./tests/benchmark/bp01 \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/bp05 ./tests/benchmark/bp05 \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/bp03 ./tests/benchmark/bp03
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/bp07 ./tests/benchmark/bp07
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/bp06 ./tests/benchmark/bp06
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/bp04 ./tests/benchmark/bp04
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/bp02 ./tests/benchmark/bp02

FROM scratch AS narumq
COPY --from=build /out/narumq /narumq
ENTRYPOINT ["/narumq"]

FROM scratch AS bp01-runner
COPY --from=build /out/bp01 /bp01
ENTRYPOINT ["/bp01"]

FROM scratch AS bp05-runner
COPY --from=build /out/bp05 /bp05
ENTRYPOINT ["/bp05"]

FROM scratch AS bp03-runner
COPY --from=build /out/bp03 /bp03
ENTRYPOINT ["/bp03"]

FROM scratch AS bp07-runner
COPY --from=build /out/bp07 /bp07
ENTRYPOINT ["/bp07"]

FROM scratch AS bp06-runner
COPY --from=build /out/bp06 /bp06
ENTRYPOINT ["/bp06"]

FROM scratch AS bp04-runner
COPY --from=build /out/bp04 /bp04
ENTRYPOINT ["/bp04"]

FROM scratch AS bp02-runner
COPY --from=build /out/bp02 /bp02
ENTRYPOINT ["/bp02"]

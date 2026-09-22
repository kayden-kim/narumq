FROM golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS go-build
WORKDIR /src
COPY tests/compatibility/mqtt-clients-v1/clients/go/go.mod tests/compatibility/mqtt-clients-v1/clients/go/go.sum ./
RUN go mod download
COPY tests/compatibility/mqtt-clients-v1/clients/go/*.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags=-buildid= -o /out/client .
RUN cp /out/client /out/proxy

FROM python:3.13.7-slim-bookworm@sha256:adafcc17694d715c905b4c7bebd96907a1fd5cf183395f0ebc4d3428bd22d92d AS python-build
WORKDIR /src
COPY tests/compatibility/mqtt-clients-v1/clients/python/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.txt
COPY tests/compatibility/mqtt-clients-v1/clients/python/client.py /out/client.py

FROM scratch AS go-client
COPY --from=go-build /out/client /client
COPY --from=go-build /out/proxy /proxy
ENTRYPOINT ["/client"]

FROM python-build AS python-client
COPY --from=python-build /out/client.py /client.py
ENTRYPOINT ["python", "/client.py"]

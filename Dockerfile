ARG GO_VERSION=1.26
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build

ARG TARGETOS
ARG TARGETARCH
ARG TARGETPLATFORM
ARG BUILDPLATFORM

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . ./

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w" -o /out/go-meshtastic-serial2tcp .

FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=build /out/go-meshtastic-serial2tcp /go-meshtastic-serial2tcp

ENV SERIAL_DEVICE=/dev/ttyUSB0
ENV BAUD_RATE=115200
ENV TCP_PORT=4403
ENV WEB_UI=true
ENV WEB_UI_ADDR=0.0.0.0:9080

EXPOSE 4403 9080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/go-meshtastic-serial2tcp", "--healthcheck"]

ENTRYPOINT ["/go-meshtastic-serial2tcp"]

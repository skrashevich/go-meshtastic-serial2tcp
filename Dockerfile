FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/go-meshtastic-serial2tcp .

FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=build /out/go-meshtastic-serial2tcp /go-meshtastic-serial2tcp

ENV SERIAL_DEVICE=/dev/ttyUSB0
ENV BAUD_RATE=115200
ENV TCP_PORT=4403

ENTRYPOINT ["/go-meshtastic-serial2tcp"]

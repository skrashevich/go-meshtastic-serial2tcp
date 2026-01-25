# go-meshtastic-serial2tcp

Small TCP <-> serial bridge for Meshtastic devices. It listens on a TCP port and forwards bytes to the configured serial device (and back), keeping the connection open until either side disconnects.

## Requirements

- Go 1.24+ (for building from source)
- Access to the serial device (for example `/dev/ttyUSB0` on Linux or `/dev/tty.usb*` on macOS)

## Install and run (local)

Install with `go install`:

```bash
go install github.com/skrashevich/go-meshtastic-serial2tcp@latest
```

Run with flags:

```bash
meshtastic-serial2tcp \
  --device /dev/ttyUSB0 \
  --baud 115200 \
  --tcp-port 4403
```

Run with environment variables:

```bash
SERIAL_DEVICE=/dev/ttyUSB0 \
BAUD_RATE=115200 \
TCP_PORT=4403 \
meshtastic-serial2tcp
```

If you get a permission error for the serial device, make sure your user has access to it (for example, add your user to the `dialout` group on Linux or run with elevated permissions).


## Docker

Run a prebuilt image from GitHub Container Registry:

```bash
docker run --rm \
  --device /dev/ttyUSB0 \
  -p 4403:4403 \
  -e SERIAL_DEVICE=/dev/ttyUSB0 \
  -e BAUD_RATE=115200 \
  -e TCP_PORT=4403 \
  ghcr.io/skrashevich/go-meshtastic-serial2tcp:latest
```

Notes:

- For mDNS on Linux, you may need `--network host` so multicast announcements reach your LAN. If not required, set `-e MDNS_ENABLED=false`.
- If the container can’t open the serial device, ensure the device is passed through and that permissions allow access.


## Configuration

Environment variables (and matching flags):

- `SERIAL_DEVICE` (default: `/dev/ttyUSB0`) -> `--device`
- `BAUD_RATE` (default: `115200`) -> `--baud`
- `TCP_PORT` (default: `4403`) -> `--tcp-port`
- `RECONNECT_DELAY` (default: `5`, seconds) -> `--reconnect-delay`
- `MDNS_ENABLED` (default: `true`) -> `--mdns`
- `SERVICE_NAME` (default: `Meshtastic Serial Bridge (<device>)`) -> `--service-name`

Healthcheck:

- `--healthcheck` exits with code 0 if the TCP port is reachable on `127.0.0.1`.

## mDNS discovery

When enabled, the service advertises `_meshtastic._tcp.local.` with details about the serial device and baud rate. If mDNS is not needed or doesn’t work in your environment, disable it with `MDNS_ENABLED=false` or `--mdns=false`.


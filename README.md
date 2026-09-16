# stream_proxy

Go RTMP relay service. Receives streams from OBS, forwards to multiple remotes (Twitch, Kick, YouTube, etc.), provides web frontend for live viewing and remote configuration.

## Features

- RTMP server on port 1935
- HTTP server on port 9090 with web UI
- Multi-remote forwarding with per-remote bitrate/status monitoring
- Output size control (Auto/1080p/720p/480p)
- JSON config file persistence
- WebSocket FLV streaming to web viewers

## Quick Start

```bash
go build -o stream_proxy.exe .
./stream_proxy.exe --config stream_proxy.json
```

Open http://localhost:9090 for the web UI. Configure remotes via the UI or `POST /api/remotes`.

## Testing

```bash
go test ./...
```

## Endpoints

- `GET /api/remotes` - list configured remotes
- `POST /api/remotes` - add/update remote
- `GET /stream` - live FLV stream via WebSocket
# AGENTS.md — nark collector

This file contains repo-specific guidance for OpenCode agents. Each line answers: "Would an agent likely miss this without help?" If not, it is omitted.

## Build & run

- `go build ./...` — builds all packages
- `go build -o bin/collector ./cmd/collector` — builds the main binary
- Run the binary with env vars set (see "Environment" below)
- No test files exist yet; the codebase uses interfaces (`Submitter`) and `Option` funcs for testability

## Environment — required env vars

The collector reads all config from environment. Missing required vars cause startup failure:

- `NARK_KAFKA_BROKERS` — comma-separated list, **required**
- `NARK_KAFKA_TOPIC` — **required**
- `NARK_GRPC_ADDR` — default `:9090`
- `NARK_SERVICE` — default from `nark-collector`
- `NARK_ENV` — default `local`
- `NARK_INSTANCE` — default `$(hostname)`
- `NARK_LOG_LEVEL` — default `info`, one of `debug|info|warn|error`
- `NARK_LOG_FORMAT` — default `json`, one of `json|text`
- `NARK_GRPC_MAX_RECV_BYTES` — default `4<<20` (4 MiB)
- `NARK_MAX_BATCH_TRACKS` — default `500`
- `NARK_POOL_WORKERS` — default `8`
- `NARK_POOL_QUEUE_SIZE` — default `4096`
- `NARK_POOL_OVERFLOW_POLICY` — default `drop`, one of `drop|reject`
- `NARK_METRICS_PUSH_URL` — optional; if set, `NARK_METRICS_PUSH_INTERVAL` must be > 0
- `NARK_METRICS_PUSH_INTERVAL` — default `10s`
- `NARK_METRICS_EXTRA_LABELS` — optional
- `NARK_SHUTDOWN_TIMEOUT` — default `20s`, must be > 0

## Proto code generation

- `buf generate` — generates Go and web proto code from `api/proto/`
- `buf lint` — runs buf lint STANDARD
- `buf breaking` — runs buf breaking check (uses `buf.gen.yaml` break rules)
- Generated Go code lives in `gen/go/nark/v1/`
- Generated web code lives in `gen/web/nark/v1/`
- `tools/package.json` depends on `../gen/web/nark/v1` — regenerate if the proto changes

## Key architecture

- `cmd/collector/main.go` — entrypoint: loads config, starts gRPC server + worker pool
- `internal/config/reader.go` — reads env vars with defaults; validates on load
- `internal/collector/service.go` — gRPC handler; validates/normalizes tracks, enqueues to worker pool
- `internal/collector/normalizer.go` — enforces field limits, truncates IDs/names, handles clock skew
- `internal/collector/publisher.go` — per-worker Kafka serialization (currently stubbed: `Process` is a no-op that just increments `publishOK`)
- `internal/collector/grpc.go` — RecoveryInterceptor + LoggingInterceptor for gRPC
- `internal/observability/` — VictoriaMetrics registry (scrape on `HTTP_ADDR` or push via `PushURL`)
- `internal/workerpool/pool.go` — bounded worker pool; one Kafka producer per worker (`NARK_POOL_WORKERS`)
- Tracks flow: gRPC handler → normalizer → worker pool → `PublishJob` → (stubbed) Kafka

## Kafka not wired yet

The Kafka sender is currently a stub in `internal/collector/publisher.go:23`:
```go
func(int) (Sender, error) { return nil, nil }
```
`Process` is also a no-op (just increments `publishOK`). To enable Kafka, implement the `Sender` interface and wire a real producer via `NewPublisherFactory`.

## Node.js tools

- `bun install` — install dependencies
- `bun run index.ts` — run the Node client (currently uses generated `nark` proto types from `gen/web/nark/v1`)
- `tools/package.json` has `nark` as a path dependency pointing to `../gen/web/nark/v1`

## gRPC client notes

The TrackIngest service proto is at `gen/go/nark/v1/track.pb.go` / `gen/web/nark/v1/track_pb.js`. The ConnectRPC-generated client is in `tools/index.ts`. To call Push from Node:

```ts
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { TrackIngestService, TrackKind } from "./gen/nark/v1/track_pb";

const transport = createGrpcTransport({ baseUrl: "http://127.0.0.1:9090" });
const client = createClient(TrackIngestService, transport);
await client.push({ batchId, client: { app: "..." }, tracks: [...] });

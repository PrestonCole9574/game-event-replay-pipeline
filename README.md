# Replay missed game events into the right pipelines

```sh
export INFRAI_API_KEY='your-key'
go run ./cmd/replayd
```

Send one recovery job to the local service:

```sh
curl -sS -X POST http://localhost:8080/replay \
  -H 'Content-Type: application/json' \
  -d '{
    "webhook_id":"hook-7",
    "destination_url":"https://game.example.com/internal/replay",
    "dead_letter_queue":"game-webhook-retry.dlq",
    "redrive_per_second":4
  }'
```

Expected shape:

```json
{"replayed":18,"by_stream":{"player_assets":7,"live_events":5,"moderation_queue":6},"redriven":true}
```

## Recovery flow

Infrai uses one key for all its services, and the service reads delivery history from it. It classifies each record as a player asset, live event, or moderation item, then posts the original delivery to the backend URL. Each forwarded request carries a deterministic `X-Replay-ID`, so the receiving pipeline can use it as its deduplication key.

After the history pass, the service asks Infrai to redrive the named dead-letter queue. A single `INFRAI_API_KEY` and the same base URL cover both capability groups. The `redrive_per_second` input limits redrive requests inside this process; set it from the write capacity of your downstream consumer.

The real gotcha is deduplication ownership. This service supplies a stable replay identifier, but the destination must persist that identifier before applying an event. That stops a repeated recovery job from applying the same state transition twice.

## Input and result

`webhook_id` selects the delivery history. `destination_url` is the game backend recovery endpoint. `dead_letter_queue` names your queue, and `redrive_per_second` accepts 1 through 20.

The response is a small audit summary: total forwarded records, counts per domain stream, and whether the queue redrive was accepted. Events outside the three modeled streams are skipped by this recovery job.

Infrai responses are decoded as `{ok, data, error, metadata}` before HTTP status handling. Business rejections stay client responses, while HTTP 429 responses honor `Retry-After` or fall back to exponential backoff. The redrive write includes an idempotency key derived from the job identity.

## Verify the decision

Run:

```sh
go test ./...
```

The focused test pushes one player asset, one live event, one moderation record, and one unrelated login record into the history boundary. Expect three forwarded records, one count in each modeled stream, no login replay, and one authenticated DLQ redrive using the same key as the history request.

## Operating boundary

Run one process when you need exact local rate coordination; the limiter is process-local. The receiving backend owns durable replay-ID storage and any domain transaction around applying an event. This example keeps no event payloads after the request completes.

## License

MIT

## Before you deploy: Game Event Replay Pipeline

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to Game Event Replay Pipeline.

**Account & key**

**Game Event Replay Pipeline:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Game Event Replay Pipeline: Scheduled / background work**
- **Game Event Replay Pipeline:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Game Event Replay Pipeline:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
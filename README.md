![Gyne Agent logo](./logo.png)

# Gyne Agent Redis Task Pipeline

This workspace has two Rust services:

- `publisher`: websocket endpoint for the frontend. It accepts chat task JSON and appends it to a Redis Stream.
- `consumer`: worker process. It registers itself in Redis, blocks on Redis Streams, calls the OpenClaw chat completions endpoint with the gateway token, optionally sends the response to Telegram, and appends the result to a result stream.

Redis Streams are used instead of Redis pub/sub so tasks survive consumer restarts. Assigned work is routed to a per-consumer stream, while unassigned work is routed to the shared task stream.

## Run

Start Redis, then run the two services:

```sh
cargo run -p publisher
CONSUMER_NAME=consumer-1 OPENCLAW_GATEWAY_TOKEN=your-token cargo run -p consumer
```

To send completed task responses to Telegram, also set:

```sh
TELEGRAM_BOT_TOKEN=your-bot-token
TELEGRAM_CHAT_ID=your-chat-id
```

The publisher listens on `ws://127.0.0.1:8080/ws` by default.

The React Kanban board is in `frontend/`:

```sh
cd frontend
npm install
npm run dev
```

## Frontend Websocket Payload

Send a JSON message like this to the publisher websocket:

```json
{
  "model": "openclaw-chat",
  "assigned_consumer": "consumer-1",
  "messages": [
    { "role": "user", "content": "Write a short summary of Redis Streams." }
  ],
  "temperature": 0.2,
  "max_tokens": 300,
  "metadata": {
    "user_id": "frontend-user-123"
  }
}
```

Omit `assigned_consumer` to let Redis assign the task to any consumer in the shared consumer group.

The publisher responds with:

```json
{
  "type": "accepted",
  "task_id": "generated-or-provided-task-id",
  "stream_id": "redis-stream-entry-id"
}
```

The frontend can also ask the websocket for active consumers:

```json
{ "type": "list_consumers" }
```

The publisher responds with:

```json
{
  "type": "consumers",
  "consumers": [
    {
      "name": "consumer-1",
      "consumer_group": "openclaw-workers",
      "task_stream": "openclaw:tasks",
      "direct_task_stream": "openclaw:tasks:consumer-1",
      "result_stream": "openclaw:results",
      "status": "listening",
      "started_at_ms": 1779400000000,
      "last_seen_ms": 1779400000000,
      "expires_at_ms": 1779400015000
    }
  ]
}
```

Consumer results are written to the `openclaw:results` Redis Stream with a `payload` field containing JSON:

```json
{
  "task_id": "generated-or-provided-task-id",
  "status": "completed",
  "consumer": "consumer-1",
  "assigned_consumer": "consumer-1",
  "source_stream": "openclaw:tasks:consumer-1",
  "source_stream_id": "redis-stream-entry-id",
  "completed_at_ms": 1779400000000,
  "telegram": {
    "status": "sent"
  },
  "response": {}
}
```

The publisher also watches `RESULT_STREAM` and broadcasts task updates to connected Kanban websocket clients:

```json
{
  "type": "task_update",
  "task_id": "generated-or-provided-task-id",
  "card_id": "frontend-card-id",
  "status": "done",
  "message": "Task completed"
}
```

`status: "done"` moves the card to `Done`. `status: "needs_input"` and `status: "failed"` move the card to `Review` and show the message or questions in the card detail panel.

## Configuration

Environment variables:

- `REDIS_URL`: Redis connection URL. Default: `redis://127.0.0.1/`
- `TASK_STREAM`: stream for incoming tasks. Default: `openclaw:tasks`
- `CONSUMER_TASK_STREAM`: per-consumer stream for directly assigned tasks. Default: `${TASK_STREAM}:${CONSUMER_NAME}`
- `RESULT_STREAM`: stream for worker results. Default: `openclaw:results`
- `CONSUMER_REGISTRY_KEY`: sorted-set key for consumer discovery. Default: `openclaw:consumers`
- `PUBLISHER_BIND`: websocket bind address. Default: `127.0.0.1:8080`
- `DEFAULT_MODEL`: optional default model if frontend payload omits `model`
- `CONSUMER_GROUP`: Redis consumer group. Default: `openclaw-workers`
- `CONSUMER_NAME`: Redis consumer name. Required by the consumer so the publisher can assign tasks.
- `CONSUMER_DISCOVERY_TTL_MS`: consumer discovery TTL. Default: `15000`
- `CONSUMER_HEARTBEAT_INTERVAL_MS`: consumer discovery heartbeat interval. Default: `5000`
- `CONSUMER_STALE_AFTER_MS`: publisher cutoff for stale consumers. Default: `15000`
- `RESULT_STREAM_BLOCK_MS`: publisher block timeout when watching results. Default: `5000`
- `PUBLISHER_UPDATE_BUFFER`: websocket broadcast buffer for task updates. Default: `256`
- `OPENCLAW_BASE_URL`: used to build `/chat/completions` when `OPENCLAW_CHAT_COMPLETIONS_URL` is unset
- `OPENCLAW_CHAT_COMPLETIONS_URL`: full chat completions URL
- `OPENCLAW_GATEWAY_TOKEN`: required by the consumer
- `OPENCLAW_GATEWAY_TOKEN_HEADER`: default `Authorization`. Set this if the gateway expects a different header.
- `OPENCLAW_GATEWAY_TOKEN_PREFIX`: optional prefix for non-Authorization token headers
- `TELEGRAM_API_BASE`: Telegram API base URL. Default: `https://api.telegram.org`
- `TELEGRAM_BOT_TOKEN`: optional Telegram bot token for sending completed responses
- `TELEGRAM_CHAT_ID`: optional Telegram chat ID for sending completed responses
- `VITE_PUBLISHER_WS_URL`: frontend websocket URL. Default: `ws://127.0.0.1:8080/ws`
- `VITE_DEFAULT_MODEL`: frontend default model. Default: `openclaw-chat`

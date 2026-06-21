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

The publisher binds to `0.0.0.0:${PORT:-8080}` by default. For local development, connect to `ws://127.0.0.1:8080/ws`.

For public deployments, set `GYNE_AGENT_SESSION_SECRET` on the publisher. When this secret is set, `/ws` and `/consumers` require a short-lived 2ndBrain launch token.

The React Kanban board is in `frontend/`:

```sh
cd frontend
npm install
npm run dev
```

## 2ndBrain Launch Auth

The publisher supports 2ndBrain launch auth with an HS256 JWT passed as a websocket query parameter:

```text
wss://gyne-agent.example.com/ws?token=...
```

The same `GYNE_AGENT_SESSION_SECRET` must be used by 2ndBrain when signing the token and by the Gyne Agent publisher when verifying it.

Expected JWT payload:

```json
{
  "user_id": "supabase-user-id",
  "install_id": "marketplace-install-id",
  "tool_id": "gyne-agent",
  "email": "user@example.com",
  "exp": 1779400300
}
```

Required claims:

- `user_id`
- `tool_id`, matching `GYNE_AGENT_TOOL_ID` or the default `gyne-agent`
- `exp`, as Unix seconds

Optional claims:

- `install_id`
- `email`

When launch auth is enabled, the publisher overwrites task metadata fields for `user_id`, `install_id`, `tool_id`, `email`, and `auth_source`. Client-supplied values for those fields are not trusted. Task result broadcasts are filtered so a websocket only receives updates for the same launch user and install.

The frontend accepts launch URLs like:

```text
https://gyne-agent-ui.example.com/?token=...&publisher_ws_url=wss://gyne-agent-publisher.example.com/ws
```

It reads `token`, `launch_token`, or `2ndbrain_launch_token`, removes the token from the browser address bar, and appends it to the websocket connection.

For single-service deployments, the publisher can also serve the built frontend from `frontend/dist`:

```text
https://gyne-agent.example.com/?token=...&publisher_ws_url=wss://gyne-agent.example.com/ws
```

## Frontend Websocket Payload

Send a JSON message like this to the publisher websocket:

```json
{
  "model": "openclaw",
  "assigned_consumer": "consumer-1",
  "messages": [
    { "role": "user", "content": "Write a short summary of Redis Streams." }
  ],
  "temperature": 0.2,
  "max_tokens": 300,
  "metadata": {
    "card_id": "frontend-card-id"
  }
}
```

When launch auth is enabled, the publisher derives `metadata.user_id` from the signed 2ndBrain token.

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
      "hostname": "worker-host-1",
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
- `PUBLISHER_BIND`: websocket bind address. Default: `0.0.0.0:${PORT:-8080}`
- `FRONTEND_DIST_DIR`: optional path to the built frontend served by the publisher. Default: `frontend/dist`
- `DEFAULT_MODEL`: optional default model if frontend payload omits `model`
- `CONSUMER_GROUP`: Redis consumer group. Default: `openclaw-workers`
- `CONSUMER_NAME`: Redis consumer name. Required by the consumer so the publisher can assign tasks.
- `CONSUMER_HOSTNAME`: optional hostname to publish in consumer discovery. Defaults to `HOSTNAME`, `COMPUTERNAME`, or the `hostname` command when available.
- `CONSUMER_DISCOVERY_TTL_MS`: consumer discovery TTL. Default: `15000`
- `CONSUMER_HEARTBEAT_INTERVAL_MS`: consumer discovery heartbeat interval. Default: `5000`
- `CONSUMER_STALE_AFTER_MS`: publisher cutoff for stale consumers. Default: `15000`
- `RESULT_STREAM_BLOCK_MS`: publisher block timeout when watching results. Default: `5000`
- `PUBLISHER_UPDATE_BUFFER`: websocket broadcast buffer for task updates. Default: `256`
- `GYNE_AGENT_SESSION_SECRET`: enables 2ndBrain launch-token verification when set. Use at least 32 random bytes.
- `GYNE_AGENT_TOOL_ID`: expected launch-token tool id. Default: `gyne-agent`
- `OPENCLAW_BASE_URL`: used to build `/chat/completions` when `OPENCLAW_CHAT_COMPLETIONS_URL` is unset
- `OPENCLAW_CHAT_COMPLETIONS_URL`: full chat completions URL
- `OPENCLAW_GATEWAY_TOKEN`: required by the consumer
- `OPENCLAW_GATEWAY_TOKEN_HEADER`: default `Authorization`. Set this if the gateway expects a different header.
- `OPENCLAW_GATEWAY_TOKEN_PREFIX`: optional prefix for non-Authorization token headers
- `TELEGRAM_API_BASE`: Telegram API base URL. Default: `https://api.telegram.org`
- `TELEGRAM_BOT_TOKEN`: optional Telegram bot token for sending completed responses
- `TELEGRAM_CHAT_ID`: optional Telegram chat ID for sending completed responses
- `VITE_PUBLISHER_WS_URL`: frontend websocket URL. Default: `ws://127.0.0.1:8080/ws`
- `VITE_DEFAULT_MODEL`: frontend default model. Default: `openclaw`
- `VITE_2NDBRAIN_LAUNCH_TOKEN`: optional development-only launch token fallback. Prefer query parameters for real launches.

## Railway Docker Deployment

This repo includes `railway.json`, `railpack.json`, and a Dockerfile. `railway.json` tells Railway to use the Dockerfile. `railpack.json` supplies a start command fallback if a service is still configured to use Railpack.

The container can run one of three services from the same image. Set `GYNE_AGENT_SERVICE` per Railway service:

```text
publisher
consumer
frontend
```

Single public service:

```env
GYNE_AGENT_SERVICE=publisher
REDIS_URL=redis://...
GYNE_AGENT_SESSION_SECRET=long-random-secret
DEFAULT_MODEL=openclaw
```

The publisher serves both `/` and `/ws` in this mode. In 2ndBrain, point both values at the same Railway service:

```env
GYNE_AGENT_URL=https://gyne-agent-production.up.railway.app
GYNE_AGENT_WS_URL=wss://gyne-agent-production.up.railway.app/ws
```

Do not set `PUBLISHER_BIND=127.0.0.1:8080` on Railway. Leave `PUBLISHER_BIND` empty, or set it to `0.0.0.0:${PORT}` if your Railway configuration expands variables. If Railway environment variables are present, the publisher will override an accidental loopback bind to `0.0.0.0:${PORT:-8080}`.

Optional worker service:

Consumer service:

```env
GYNE_AGENT_SERVICE=consumer
REDIS_URL=redis://...
CONSUMER_NAME=consumer-1
OPENCLAW_GATEWAY_TOKEN=...
```

Frontend service:

```env
GYNE_AGENT_SERVICE=frontend
```

This is only needed if you want a separate frontend URL. The publisher service already serves the production frontend bundle from `frontend/dist`.

For frontend builds, set `VITE_PUBLISHER_WS_URL` as a Docker build arg when you want a baked-in websocket URL. Otherwise, pass `publisher_ws_url` in the 2ndBrain launch URL.

If Railway still shows "No start command detected" and "Detected Rust", check the service build settings:

- Builder should be Dockerfile, or leave it unset so `railway.json` can select Dockerfile.
- If using Railpack intentionally, set the start command to `sh ./scripts/start-container.sh`.
- Set `GYNE_AGENT_SERVICE=publisher` for the publisher service and `GYNE_AGENT_SERVICE=consumer` for the worker service.

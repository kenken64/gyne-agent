use anyhow::{Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};
use task_core::{now_millis, ConsumerDiscovery, PublisherResponse, TaskRequest};
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

#[derive(Clone)]
struct AppState {
    redis: redis::Client,
    task_stream: String,
    default_model: Option<String>,
    consumer_registry_key: String,
    consumer_stale_after_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let bind: SocketAddr = env_or("PUBLISHER_BIND", "127.0.0.1:8080")
        .parse()
        .context("PUBLISHER_BIND must be a valid socket address")?;
    let redis_url = env_or("REDIS_URL", "redis://127.0.0.1/");
    let task_stream = env_or("TASK_STREAM", "openclaw:tasks");
    let default_model = optional_env("DEFAULT_MODEL");
    let consumer_registry_key = env_or("CONSUMER_REGISTRY_KEY", "openclaw:consumers");
    let consumer_stale_after_ms = parse_env("CONSUMER_STALE_AFTER_MS", 15_000)?;

    let state = Arc::new(AppState {
        redis: redis::Client::open(redis_url).context("failed to create Redis client")?,
        task_stream,
        default_model,
        consumer_registry_key,
        consumer_stale_after_ms,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/consumers", get(consumers))
        .route("/ws", get(ws_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind publisher on {bind}"))?;
    info!("publisher websocket listening on ws://{bind}/ws");

    axum::serve(listener, app)
        .await
        .context("publisher server failed")
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "task_stream": state.task_stream,
        "consumer_registry_key": state.consumer_registry_key,
        "has_default_model": state.default_model.is_some(),
    }))
}

async fn consumers(State(state): State<Arc<AppState>>) -> Json<PublisherResponse> {
    let response = match state.redis.get_multiplexed_async_connection().await {
        Ok(mut redis) => match list_consumers(&mut redis, &state).await {
            Ok(consumers) => PublisherResponse::Consumers { consumers },
            Err(err) => PublisherResponse::Error {
                message: err.to_string(),
            },
        },
        Err(err) => PublisherResponse::Error {
            message: format!("failed to connect to Redis: {err}"),
        },
    };

    Json(response)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut redis = match state.redis.get_multiplexed_async_connection().await {
        Ok(connection) => connection,
        Err(err) => {
            error!(error = %err, "failed to connect to Redis");
            let _ = send_json(
                &mut socket,
                &PublisherResponse::Error {
                    message: "failed to connect to Redis".to_owned(),
                },
            )
            .await;
            return;
        }
    };

    while let Some(message) = socket.recv().await {
        let message = match message {
            Ok(message) => message,
            Err(err) => {
                warn!(error = %err, "websocket receive failed");
                return;
            }
        };

        let response = match message {
            Message::Text(text) => {
                handle_client_payload(text.to_string().as_bytes(), &state, &mut redis).await
            }
            Message::Binary(bytes) => {
                handle_client_payload(bytes.as_ref(), &state, &mut redis).await
            }
            Message::Ping(bytes) => {
                if let Err(err) = socket.send(Message::Pong(bytes)).await {
                    warn!(error = %err, "websocket pong failed");
                    return;
                }
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => return,
        };

        if let Err(err) = send_json(&mut socket, &response).await {
            warn!(error = %err, "websocket send failed");
            return;
        }
    }
}

async fn handle_client_payload(
    payload: &[u8],
    state: &AppState,
    redis: &mut redis::aio::MultiplexedConnection,
) -> PublisherResponse {
    match handle_client_payload_inner(payload, state, redis).await {
        Ok(response) => response,
        Err(err) => {
            warn!(error = %err, "publisher request failed");
            PublisherResponse::Error {
                message: err.to_string(),
            }
        }
    }
}

async fn handle_client_payload_inner(
    payload: &[u8],
    state: &AppState,
    redis: &mut redis::aio::MultiplexedConnection,
) -> Result<PublisherResponse> {
    let value: Value = serde_json::from_slice(payload).context("invalid publisher JSON")?;
    if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|message_type| message_type == "list_consumers")
    {
        return Ok(PublisherResponse::Consumers {
            consumers: list_consumers(redis, state).await?,
        });
    }

    let request_value = if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|message_type| message_type == "publish_task")
    {
        value
            .get("task")
            .cloned()
            .context("publish_task messages require a task object")?
    } else {
        value
    };

    let request: TaskRequest =
        serde_json::from_value(request_value).context("invalid task JSON")?;
    let task = request
        .into_task(state.default_model.as_deref())
        .context("invalid task payload")?;
    let task_id = task.task_id.clone();
    let publish_stream = task_publish_stream(redis, state, &task).await?;
    let serialized = serde_json::to_string(&task).context("failed to serialize task")?;

    let stream_id: String = redis::cmd("XADD")
        .arg(&publish_stream)
        .arg("*")
        .arg("payload")
        .arg(serialized)
        .query_async(redis)
        .await
        .context("failed to append task to Redis stream")?;

    Ok(PublisherResponse::Accepted { task_id, stream_id })
}

async fn task_publish_stream(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
    task: &task_core::ChatTask,
) -> Result<String> {
    let Some(assigned_consumer) = task.assigned_consumer.as_deref() else {
        return Ok(state.task_stream.clone());
    };

    let consumers = list_consumers(redis, state).await?;
    consumers
        .into_iter()
        .find(|consumer| consumer.name == assigned_consumer)
        .map(|consumer| consumer.direct_task_stream)
        .with_context(|| format!("assigned consumer is not active: {assigned_consumer}"))
}

async fn list_consumers(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
) -> Result<Vec<ConsumerDiscovery>> {
    let now = now_millis();
    let cutoff = now.saturating_sub(state.consumer_stale_after_ms);
    let cutoff_score = cutoff.to_string();

    let _: i64 = redis::cmd("ZREMRANGEBYSCORE")
        .arg(&state.consumer_registry_key)
        .arg("-inf")
        .arg(&cutoff_score)
        .query_async(redis)
        .await
        .context("failed to prune stale consumers")?;

    let names: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(&state.consumer_registry_key)
        .arg(&cutoff_score)
        .arg("+inf")
        .query_async(redis)
        .await
        .context("failed to list active consumers")?;

    let mut consumers = Vec::with_capacity(names.len());
    for name in names {
        let key = consumer_discovery_key(&state.consumer_registry_key, &name);
        let fields: HashMap<String, String> = redis::cmd("HGETALL")
            .arg(&key)
            .query_async(redis)
            .await
            .with_context(|| format!("failed to read consumer discovery for {name}"))?;

        if fields.is_empty() {
            continue;
        }

        match discovery_from_hash(fields) {
            Some(consumer) if consumer.last_seen_ms >= cutoff => consumers.push(consumer),
            _ => {
                let _: i64 = redis::cmd("ZREM")
                    .arg(&state.consumer_registry_key)
                    .arg(&name)
                    .query_async(redis)
                    .await
                    .context("failed to remove incomplete consumer discovery")?;
            }
        }
    }

    consumers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(consumers)
}

fn discovery_from_hash(fields: HashMap<String, String>) -> Option<ConsumerDiscovery> {
    Some(ConsumerDiscovery {
        name: fields.get("name")?.to_owned(),
        consumer_group: fields.get("consumer_group")?.to_owned(),
        task_stream: fields.get("task_stream")?.to_owned(),
        direct_task_stream: fields.get("direct_task_stream")?.to_owned(),
        result_stream: fields.get("result_stream")?.to_owned(),
        status: fields.get("status")?.to_owned(),
        started_at_ms: fields.get("started_at_ms")?.parse().ok()?,
        last_seen_ms: fields.get("last_seen_ms")?.parse().ok()?,
        expires_at_ms: fields.get("expires_at_ms")?.parse().ok()?,
    })
}

async fn send_json<T: Serialize>(socket: &mut WebSocket, value: &T) -> Result<()> {
    let text = serde_json::to_string(value).context("failed to serialize websocket response")?;
    socket
        .send(Message::Text(text.into()))
        .await
        .context("failed to send websocket response")
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match optional_env(key) {
        Some(value) => value
            .parse::<T>()
            .with_context(|| format!("{key} must be a valid value")),
        None => Ok(default),
    }
}

fn consumer_discovery_key(registry_key: &str, name: &str) -> String {
    format!("{registry_key}:{name}")
}

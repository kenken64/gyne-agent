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
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use redis::streams::StreamReadReply;
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};
use task_core::{now_millis, ConsumerDiscovery, PublisherResponse, TaskRequest, TaskUpdate};
use tokio::sync::broadcast;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

#[derive(Clone)]
struct AppState {
    redis: redis::Client,
    task_stream: String,
    result_stream: String,
    default_model: Option<String>,
    consumer_registry_key: String,
    consumer_stale_after_ms: u64,
    result_block_ms: usize,
    updates: broadcast::Sender<PublisherResponse>,
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
    let result_stream = env_or("RESULT_STREAM", "openclaw:results");
    let default_model = optional_env("DEFAULT_MODEL");
    let consumer_registry_key = env_or("CONSUMER_REGISTRY_KEY", "openclaw:consumers");
    let consumer_stale_after_ms = parse_env("CONSUMER_STALE_AFTER_MS", 15_000)?;
    let result_block_ms = parse_env("RESULT_STREAM_BLOCK_MS", 5_000)?;
    let update_buffer = parse_env("PUBLISHER_UPDATE_BUFFER", 256)?;
    let (updates, _) = broadcast::channel(update_buffer);

    let state = Arc::new(AppState {
        redis: redis::Client::open(redis_url).context("failed to create Redis client")?,
        task_stream,
        result_stream,
        default_model,
        consumer_registry_key,
        consumer_stale_after_ms,
        result_block_ms,
        updates,
    });
    spawn_result_listener(state.clone());

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
        "result_stream": state.result_stream,
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

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut redis = match state.redis.get_multiplexed_async_connection().await {
        Ok(connection) => connection,
        Err(err) => {
            error!(error = %err, "failed to connect to Redis");
            let _ = send_json(
                &mut sender,
                &PublisherResponse::Error {
                    message: "failed to connect to Redis".to_owned(),
                },
            )
            .await;
            return;
        }
    };
    let mut updates = state.updates.subscribe();

    loop {
        tokio::select! {
            message = receiver.next() => {
                let Some(message) = message else {
                    return;
                };
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
                        if let Err(err) = sender.send(Message::Pong(bytes)).await {
                            warn!(error = %err, "websocket pong failed");
                            return;
                        }
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => return,
                };

                if let Err(err) = send_json(&mut sender, &response).await {
                    warn!(error = %err, "websocket send failed");
                    return;
                }
            }
            update = updates.recv() => {
                match update {
                    Ok(update) => {
                        if let Err(err) = send_json(&mut sender, &update).await {
                            warn!(error = %err, "websocket update send failed");
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "websocket skipped lagged task updates");
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
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
    validate_review_assignment(&task)?;
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

fn validate_review_assignment(task: &task_core::ChatTask) -> Result<()> {
    let Some(metadata) = task.metadata.as_ref() else {
        return Ok(());
    };
    let is_review = metadata
        .get("task_kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "review");
    if !is_review {
        return Ok(());
    }

    let Some(assigned_consumer) = task.assigned_consumer.as_deref() else {
        anyhow::bail!("review tasks require an assigned consumer");
    };
    let original_consumer = metadata
        .get("original_consumer")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !original_consumer.is_empty() && original_consumer == assigned_consumer {
        anyhow::bail!("review tasks must be assigned to a different consumer");
    }

    Ok(())
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
        hostname: fields
            .get("hostname")
            .map(|hostname| hostname.trim().to_owned())
            .filter(|hostname| !hostname.is_empty()),
        status: fields.get("status")?.to_owned(),
        started_at_ms: fields.get("started_at_ms")?.parse().ok()?,
        last_seen_ms: fields.get("last_seen_ms")?.parse().ok()?,
        expires_at_ms: fields.get("expires_at_ms")?.parse().ok()?,
    })
}

fn spawn_result_listener(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            match state.redis.get_multiplexed_async_connection().await {
                Ok(mut redis) => {
                    if let Err(err) = listen_for_results(&mut redis, &state).await {
                        error!(error = %err, "result stream listener failed");
                    }
                }
                Err(err) => {
                    error!(error = %err, "failed to connect to Redis for result listener");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });
}

async fn listen_for_results(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
) -> Result<()> {
    let mut last_id = "$".to_owned();
    info!(result_stream = %state.result_stream, "publisher result listener started");

    loop {
        let reply: StreamReadReply = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(state.result_block_ms)
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(&state.result_stream)
            .arg(&last_id)
            .query_async(redis)
            .await
            .context("failed to read result stream")?;

        for stream in reply.keys {
            for message in stream.ids {
                last_id = message.id.clone();
                match result_update_from_fields(&message.id, &message.map) {
                    Ok(update) => {
                        let _ = state.updates.send(PublisherResponse::TaskUpdate(update));
                    }
                    Err(err) => {
                        warn!(result_stream_id = %message.id, error = %err, "failed to parse task result");
                    }
                }
            }
        }
    }
}

fn result_update_from_fields(
    result_stream_id: &str,
    fields: &HashMap<String, redis::Value>,
) -> Result<TaskUpdate> {
    let payload = fields
        .get("payload")
        .context("result stream entry is missing payload field")
        .and_then(|value| {
            redis::from_redis_value::<String>(value).context("payload field is not a string")
        })?;
    let value: Value = serde_json::from_str(&payload).context("result payload is not JSON")?;
    Ok(result_update_from_value(result_stream_id, &value))
}

fn result_update_from_value(result_stream_id: &str, value: &Value) -> TaskUpdate {
    let raw_status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let response = value.get("response").cloned();
    let response_content = response.as_ref().and_then(completion_text);
    let structured_content = response_content
        .as_deref()
        .and_then(|content| serde_json::from_str::<Value>(content).ok());
    let response_status = response
        .as_ref()
        .and_then(|response| response.get("status"))
        .and_then(Value::as_str)
        .or_else(|| {
            structured_content
                .as_ref()
                .and_then(|content| content.get("status"))
                .and_then(Value::as_str)
        });
    let questions = extract_questions(value);
    let status = if raw_status == "failed" || response_status == Some("failed") {
        "failed"
    } else if raw_status == "needs_input"
        || response_status == Some("needs_input")
        || !questions.is_empty()
    {
        "needs_input"
    } else {
        "done"
    }
    .to_owned();
    let message = value
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            structured_content
                .as_ref()
                .and_then(|content| content.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or(response_content)
        .or_else(|| Some(status_message(&status).to_owned()));

    TaskUpdate {
        task_id: optional_string(value.get("task_id")),
        card_id: value
            .pointer("/metadata/card_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        task_kind: value
            .pointer("/metadata/task_kind")
            .and_then(Value::as_str)
            .map(str::to_owned),
        status,
        message,
        questions,
        error: value
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_owned),
        consumer: value
            .get("consumer")
            .and_then(Value::as_str)
            .map(str::to_owned),
        source_stream_id: value
            .get("source_stream_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        result_stream_id: Some(result_stream_id.to_owned()),
        completed_at_ms: value.get("completed_at_ms").and_then(Value::as_u64),
        response,
        telegram: value.get("telegram").cloned(),
    }
}

fn extract_questions(value: &Value) -> Vec<String> {
    let direct: Vec<String> = value
        .get("questions")
        .or_else(|| value.pointer("/response/questions"))
        .and_then(Value::as_array)
        .map(|questions| {
            questions
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();

    if !direct.is_empty() {
        return direct;
    }

    value
        .get("response")
        .and_then(completion_text)
        .and_then(|content| serde_json::from_str::<Value>(&content).ok())
        .and_then(|content| content.get("questions").cloned())
        .and_then(|questions| questions.as_array().cloned())
        .map(|questions| {
            questions
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn completion_text(value: &Value) -> Option<String> {
    value
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .or_else(|| value.get("output_text").and_then(Value::as_str))
        .or_else(|| value.get("content").and_then(Value::as_str))
        .map(str::to_owned)
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.to_owned()),
        _ => None,
    })
}

fn status_message(status: &str) -> &'static str {
    match status {
        "done" => "Task completed",
        "needs_input" => "Task needs more input",
        "failed" => "Task failed",
        _ => "Task updated",
    }
}

async fn send_json<T: Serialize>(
    sender: &mut SplitSink<WebSocket, Message>,
    value: &T,
) -> Result<()> {
    let text = serde_json::to_string(value).context("failed to serialize websocket response")?;
    sender
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

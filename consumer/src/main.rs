use anyhow::{anyhow, Context, Result};
use redis::{
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands,
};
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::{json, Value};
use std::{env, str::FromStr, time::Duration};
use task_core::{now_millis, ChatCompletionRequest, ChatTask, ConsumerDiscovery};
use tracing::{error, info, warn};

#[derive(Clone)]
struct Config {
    redis_url: String,
    task_stream: String,
    direct_task_stream: String,
    result_stream: String,
    consumer_group: String,
    consumer_name: String,
    consumer_registry_key: String,
    discovery_ttl_ms: u64,
    heartbeat_interval_ms: u64,
    started_at_ms: u64,
    openclaw_chat_completions_url: String,
    gateway_token_header: HeaderName,
    gateway_token_value: HeaderValue,
    telegram_api_base: String,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
    block_ms: usize,
    count: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let config = Config::from_env()?;
    let client =
        redis::Client::open(config.redis_url.clone()).context("failed to create Redis client")?;
    let mut redis = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;
    let http = reqwest::Client::new();

    ensure_group(&mut redis, &config.task_stream, &config.consumer_group).await?;
    ensure_group(
        &mut redis,
        &config.direct_task_stream,
        &config.consumer_group,
    )
    .await?;
    publish_discovery(&mut redis, &config, "listening").await?;
    spawn_discovery_heartbeat(client.clone(), config.clone());

    info!(
        task_stream = %config.task_stream,
        direct_task_stream = %config.direct_task_stream,
        result_stream = %config.result_stream,
        group = %config.consumer_group,
        consumer = %config.consumer_name,
        "consumer listening for tasks"
    );

    loop {
        let options = StreamReadOptions::default()
            .group(&config.consumer_group, &config.consumer_name)
            .count(config.count)
            .block(config.block_ms);
        let streams = [
            config.task_stream.as_str(),
            config.direct_task_stream.as_str(),
        ];
        let read_ids = [">", ">"];

        let reply: StreamReadReply = match redis.xread_options(&streams, &read_ids, &options).await
        {
            Ok(reply) => reply,
            Err(err) => {
                error!(error = %err, "failed to read from Redis stream");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        for stream in reply.keys {
            for message in stream.ids {
                if let Err(err) = handle_stream_message(
                    &mut redis,
                    &http,
                    &config,
                    &stream.key,
                    &message.id,
                    &message.map,
                )
                .await
                {
                    error!(stream_id = %message.id, error = %err, "failed to handle task");
                }
            }
        }
    }
}

async fn handle_stream_message(
    redis: &mut redis::aio::MultiplexedConnection,
    http: &reqwest::Client,
    config: &Config,
    source_stream: &str,
    stream_id: &str,
    fields: &std::collections::HashMap<String, redis::Value>,
) -> Result<()> {
    let payload = fields
        .get("payload")
        .ok_or_else(|| anyhow!("task stream entry is missing payload field"))
        .and_then(|value| {
            redis::from_redis_value::<String>(value).context("payload field is not a string")
        });

    let task = match payload.and_then(|payload| {
        serde_json::from_str::<ChatTask>(&payload).context("payload is not a valid ChatTask")
    }) {
        Ok(task) => task,
        Err(err) => {
            publish_result(
                redis,
                config,
                json!({
                    "task_id": Value::Null,
                    "status": "failed",
                    "consumer": config.consumer_name,
                    "source_stream": source_stream,
                    "source_stream_id": stream_id,
                    "completed_at_ms": now_millis(),
                    "error": err.to_string(),
                }),
            )
            .await?;
            acknowledge(redis, config, source_stream, stream_id).await?;
            return Ok(());
        }
    };

    let result = match call_openclaw(http, config, &task).await {
        Ok(response) => {
            let telegram = send_telegram_notification(http, config, &task, &response).await;
            json!({
                "task_id": task.task_id.clone(),
                "status": "completed",
                "consumer": config.consumer_name,
                "assigned_consumer": task.assigned_consumer.clone(),
                "source_stream": source_stream,
                "source_stream_id": stream_id,
                "completed_at_ms": now_millis(),
                "response": response,
                "telegram": telegram,
                "metadata": task.metadata.clone(),
            })
        }
        Err(err) => json!({
            "task_id": task.task_id.clone(),
            "status": "failed",
            "consumer": config.consumer_name,
            "assigned_consumer": task.assigned_consumer.clone(),
            "source_stream": source_stream,
            "source_stream_id": stream_id,
            "completed_at_ms": now_millis(),
            "error": err.to_string(),
            "metadata": task.metadata.clone(),
        }),
    };

    publish_result(redis, config, result).await?;
    acknowledge(redis, config, source_stream, stream_id).await?;
    Ok(())
}

async fn call_openclaw(http: &reqwest::Client, config: &Config, task: &ChatTask) -> Result<Value> {
    let body = ChatCompletionRequest::from(task);
    let response = http
        .post(&config.openclaw_chat_completions_url)
        .header(
            config.gateway_token_header.clone(),
            config.gateway_token_value.clone(),
        )
        .json(&body)
        .send()
        .await
        .context("OpenClaw chat completion request failed")?;

    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read OpenClaw response body")?;

    if !status.is_success() {
        return Err(anyhow!(
            "OpenClaw returned HTTP {status}: {}",
            truncate(&text, 500)
        ));
    }

    serde_json::from_str::<Value>(&text).context("OpenClaw response was not valid JSON")
}

async fn send_telegram_notification(
    http: &reqwest::Client,
    config: &Config,
    task: &ChatTask,
    response: &Value,
) -> Value {
    match send_telegram_notification_inner(http, config, task, response).await {
        Ok(value) => value,
        Err(err) => {
            warn!(task_id = %task.task_id, error = %err, "telegram notification failed");
            json!({
                "status": "failed",
                "error": err.to_string(),
            })
        }
    }
}

async fn send_telegram_notification_inner(
    http: &reqwest::Client,
    config: &Config,
    task: &ChatTask,
    response: &Value,
) -> Result<Value> {
    let Some(bot_token) = config.telegram_bot_token.as_deref() else {
        return Ok(json!({
            "status": "skipped",
            "reason": "TELEGRAM_BOT_TOKEN is not configured",
        }));
    };
    let Some(chat_id) = config.telegram_chat_id.as_deref() else {
        return Ok(json!({
            "status": "skipped",
            "reason": "TELEGRAM_CHAT_ID is not configured",
        }));
    };

    let task_title = task_title(task);
    let completion = completion_text(response).unwrap_or_else(|| {
        serde_json::to_string_pretty(response).unwrap_or_else(|_| response.to_string())
    });
    let message = truncate_string(&format!("{task_title}\n\n{completion}"), 3900);
    let url = format!(
        "{}/bot{}/sendMessage",
        config.telegram_api_base.trim_end_matches('/'),
        bot_token
    );

    let response = http
        .post(url)
        .json(&json!({
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": true,
        }))
        .send()
        .await
        .context("Telegram sendMessage request failed")?;

    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read Telegram response body")?;

    if !status.is_success() {
        return Err(anyhow!(
            "Telegram returned HTTP {status}: {}",
            truncate(&text, 500)
        ));
    }

    Ok(json!({
        "status": "sent",
    }))
}

async fn publish_result(
    redis: &mut redis::aio::MultiplexedConnection,
    config: &Config,
    result: Value,
) -> Result<()> {
    let serialized = serde_json::to_string(&result).context("failed to serialize result")?;
    let _: String = redis::cmd("XADD")
        .arg(&config.result_stream)
        .arg("*")
        .arg("payload")
        .arg(serialized)
        .query_async(redis)
        .await
        .context("failed to append result to Redis stream")?;
    Ok(())
}

async fn acknowledge(
    redis: &mut redis::aio::MultiplexedConnection,
    config: &Config,
    source_stream: &str,
    stream_id: &str,
) -> Result<()> {
    let _: i64 = redis::cmd("XACK")
        .arg(source_stream)
        .arg(&config.consumer_group)
        .arg(stream_id)
        .query_async(redis)
        .await
        .context("failed to acknowledge task")?;
    Ok(())
}

async fn ensure_group(
    redis: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    group: &str,
) -> Result<()> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg("$")
        .arg("MKSTREAM")
        .query_async(redis)
        .await;

    match result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err).context("failed to create Redis consumer group"),
    }
}

async fn publish_discovery(
    redis: &mut redis::aio::MultiplexedConnection,
    config: &Config,
    status: &str,
) -> Result<()> {
    let discovery = config.discovery(status);
    let key = consumer_discovery_key(&config.consumer_registry_key, &config.consumer_name);
    let ttl_ms = config.discovery_ttl_ms.to_string();

    let _: i64 = redis::cmd("HSET")
        .arg(&key)
        .arg("name")
        .arg(&discovery.name)
        .arg("consumer_group")
        .arg(&discovery.consumer_group)
        .arg("task_stream")
        .arg(&discovery.task_stream)
        .arg("direct_task_stream")
        .arg(&discovery.direct_task_stream)
        .arg("result_stream")
        .arg(&discovery.result_stream)
        .arg("status")
        .arg(&discovery.status)
        .arg("started_at_ms")
        .arg(discovery.started_at_ms)
        .arg("last_seen_ms")
        .arg(discovery.last_seen_ms)
        .arg("expires_at_ms")
        .arg(discovery.expires_at_ms)
        .query_async(redis)
        .await
        .context("failed to write consumer discovery hash")?;

    let _: i64 = redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(&ttl_ms)
        .query_async(redis)
        .await
        .context("failed to set consumer discovery ttl")?;

    let _: i64 = redis::cmd("ZADD")
        .arg(&config.consumer_registry_key)
        .arg(discovery.last_seen_ms)
        .arg(&config.consumer_name)
        .query_async(redis)
        .await
        .context("failed to update consumer discovery registry")?;

    Ok(())
}

fn spawn_discovery_heartbeat(client: redis::Client, config: Config) {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(config.heartbeat_interval_ms));
        loop {
            interval.tick().await;
            match client.get_multiplexed_async_connection().await {
                Ok(mut redis) => {
                    if let Err(err) = publish_discovery(&mut redis, &config, "listening").await {
                        warn!(error = %err, "failed to publish consumer discovery heartbeat");
                    }
                }
                Err(err) => {
                    warn!(error = %err, "failed to connect to Redis for discovery heartbeat")
                }
            }
        }
    });
}

impl Config {
    fn from_env() -> Result<Self> {
        let token = env::var("OPENCLAW_GATEWAY_TOKEN")
            .context("OPENCLAW_GATEWAY_TOKEN is required for the consumer")?;
        let token_header = optional_env("OPENCLAW_GATEWAY_TOKEN_HEADER")
            .unwrap_or_else(|| "Authorization".to_owned());
        let gateway_token_header = HeaderName::from_str(&token_header)
            .with_context(|| format!("invalid OPENCLAW_GATEWAY_TOKEN_HEADER: {token_header}"))?;
        let gateway_token_value = gateway_token_value(&gateway_token_header, token)?;
        let redis_url = env_or("REDIS_URL", "redis://127.0.0.1/");
        let task_stream = env_or("TASK_STREAM", "openclaw:tasks");
        let consumer_name = env::var("CONSUMER_NAME")
            .context("CONSUMER_NAME is required so the publisher can assign tasks")?
            .trim()
            .to_owned();
        if consumer_name.is_empty() {
            return Err(anyhow!("CONSUMER_NAME must not be empty"));
        }
        let direct_task_stream = optional_env("CONSUMER_TASK_STREAM")
            .unwrap_or_else(|| format!("{task_stream}:{consumer_name}"));
        let discovery_ttl_ms = parse_env("CONSUMER_DISCOVERY_TTL_MS", 15_000)?;
        let heartbeat_interval_ms = parse_env("CONSUMER_HEARTBEAT_INTERVAL_MS", 5_000)?;
        if discovery_ttl_ms == 0 {
            return Err(anyhow!(
                "CONSUMER_DISCOVERY_TTL_MS must be greater than zero"
            ));
        }
        if heartbeat_interval_ms == 0 {
            return Err(anyhow!(
                "CONSUMER_HEARTBEAT_INTERVAL_MS must be greater than zero"
            ));
        }

        Ok(Self {
            redis_url,
            task_stream,
            direct_task_stream,
            result_stream: env_or("RESULT_STREAM", "openclaw:results"),
            consumer_group: env_or("CONSUMER_GROUP", "openclaw-workers"),
            consumer_name,
            consumer_registry_key: env_or("CONSUMER_REGISTRY_KEY", "openclaw:consumers"),
            discovery_ttl_ms,
            heartbeat_interval_ms,
            started_at_ms: now_millis(),
            openclaw_chat_completions_url: openclaw_chat_completions_url(),
            telegram_api_base: env_or("TELEGRAM_API_BASE", "https://api.telegram.org"),
            telegram_bot_token: optional_env("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id: optional_env("TELEGRAM_CHAT_ID"),
            block_ms: parse_env("REDIS_BLOCK_MS", 5_000)?,
            count: parse_env("REDIS_READ_COUNT", 1)?,
            gateway_token_header,
            gateway_token_value,
        })
    }

    fn discovery(&self, status: &str) -> ConsumerDiscovery {
        let last_seen_ms = now_millis();
        ConsumerDiscovery {
            name: self.consumer_name.clone(),
            consumer_group: self.consumer_group.clone(),
            task_stream: self.task_stream.clone(),
            direct_task_stream: self.direct_task_stream.clone(),
            result_stream: self.result_stream.clone(),
            status: status.to_owned(),
            started_at_ms: self.started_at_ms,
            last_seen_ms,
            expires_at_ms: last_seen_ms + self.discovery_ttl_ms,
        }
    }
}

fn openclaw_chat_completions_url() -> String {
    if let Some(url) = optional_env("OPENCLAW_CHAT_COMPLETIONS_URL") {
        return url;
    }

    let base_url = env_or("OPENCLAW_BASE_URL", "https://api.openclaw.ai/v1");
    format!("{}/chat/completions", base_url.trim_end_matches('/'))
}

fn gateway_token_value(header: &HeaderName, token: String) -> Result<HeaderValue> {
    let value = if header.as_str().eq_ignore_ascii_case("authorization") {
        if token.to_ascii_lowercase().starts_with("bearer ") {
            token
        } else {
            format!("Bearer {token}")
        }
    } else if let Some(prefix) = optional_env("OPENCLAW_GATEWAY_TOKEN_PREFIX") {
        format!("{prefix} {token}")
    } else {
        token
    };

    HeaderValue::from_str(&value).context("OPENCLAW_GATEWAY_TOKEN produced an invalid header value")
}

fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match optional_env(key) {
        Some(value) => value
            .parse::<T>()
            .with_context(|| format!("{key} must be a valid value")),
        None => Ok(default),
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn consumer_discovery_key(registry_key: &str, name: &str) -> String {
    format!("{registry_key}:{name}")
}

fn task_title(task: &ChatTask) -> String {
    task.metadata
        .as_ref()
        .and_then(|metadata| metadata.get("title"))
        .and_then(Value::as_str)
        .unwrap_or(&task.task_id)
        .to_owned()
}

fn completion_text(response: &Value) -> Option<String> {
    response
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .or_else(|| response.get("output_text").and_then(Value::as_str))
        .or_else(|| response.get("content").and_then(Value::as_str))
        .map(str::to_owned)
}

fn truncate(value: &str, max_len: usize) -> &str {
    if value.len() <= max_len {
        value
    } else {
        &value[..max_len]
    }
}

fn truncate_string(value: &str, max_len: usize) -> String {
    value.chars().take(max_len).collect()
}

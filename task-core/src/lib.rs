use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::{self, Display};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequest {
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub assigned_consumer: Option<String>,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatTask {
    pub task_id: String,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_consumer: Option<String>,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerDiscovery {
    pub name: String,
    pub consumer_group: String,
    pub task_stream: String,
    pub direct_task_stream: String,
    pub result_stream: String,
    pub status: String,
    pub started_at_ms: u64,
    pub last_seen_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PublisherResponse {
    Accepted { task_id: String, stream_id: String },
    Consumers { consumers: Vec<ConsumerDiscovery> },
    Error { message: String },
}

#[derive(Debug)]
pub enum TaskValidationError {
    MissingModel,
    EmptyMessages,
    EmptyMessageContent { index: usize },
    EmptyMessageRole { index: usize },
}

impl TaskRequest {
    pub fn into_task(self, default_model: Option<&str>) -> Result<ChatTask, TaskValidationError> {
        if self.messages.is_empty() {
            return Err(TaskValidationError::EmptyMessages);
        }

        for (index, message) in self.messages.iter().enumerate() {
            if message.role.trim().is_empty() {
                return Err(TaskValidationError::EmptyMessageRole { index });
            }
            if message.content.trim().is_empty() {
                return Err(TaskValidationError::EmptyMessageContent { index });
            }
        }

        let model = self
            .model
            .filter(|model| !model.trim().is_empty())
            .or_else(|| default_model.map(str::to_owned))
            .ok_or(TaskValidationError::MissingModel)?;

        Ok(ChatTask {
            task_id: self.task_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            model,
            assigned_consumer: self
                .assigned_consumer
                .map(|consumer| consumer.trim().to_owned())
                .filter(|consumer| !consumer.is_empty()),
            messages: self.messages,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            metadata: self.metadata,
            created_at_ms: now_millis(),
        })
    }
}

impl From<&ChatTask> for ChatCompletionRequest {
    fn from(task: &ChatTask) -> Self {
        Self {
            model: task.model.clone(),
            messages: task.messages.clone(),
            temperature: task.temperature,
            max_tokens: task.max_tokens,
        }
    }
}

impl Display for TaskValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskValidationError::MissingModel => {
                write!(f, "task model is required when DEFAULT_MODEL is not set")
            }
            TaskValidationError::EmptyMessages => {
                write!(f, "messages must contain at least one item")
            }
            TaskValidationError::EmptyMessageContent { index } => {
                write!(f, "messages[{index}].content must not be empty")
            }
            TaskValidationError::EmptyMessageRole { index } => {
                write!(f, "messages[{index}].role must not be empty")
            }
        }
    }
}

impl std::error::Error for TaskValidationError {}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

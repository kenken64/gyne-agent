import React from "react";
import { createRoot } from "react-dom/client";
import {
  Archive,
  Check,
  Circle,
  ClipboardList,
  Loader2,
  MessageSquareText,
  Plus,
  RefreshCw,
  Send,
  Trash2,
  Users,
  Wifi,
  WifiOff,
  X
} from "lucide-react";
import "./styles.css";

type ColumnId = "backlog" | "ready" | "in_progress" | "review" | "done";
type Priority = "low" | "medium" | "high";
type PublishStatus = "draft" | "queued" | "failed" | "done" | "needs_input";
type SocketStatus = "closed" | "connecting" | "open";

interface Column {
  id: ColumnId;
  label: string;
  accent: string;
}

interface KanbanCard {
  id: string;
  title: string;
  prompt: string;
  model: string;
  column: ColumnId;
  priority: Priority;
  assignee: string;
  assignedConsumer: string;
  dueDate: string;
  tags: string[];
  publishStatus: PublishStatus;
  taskId?: string;
  streamId?: string;
  error?: string;
  resultMessage?: string;
  resultStreamId?: string;
  completedAt?: number;
  questions?: string[];
  completedBy?: string;
  createdAt: number;
  updatedAt: number;
  lastPublishedAt?: number;
}

interface DraftCard {
  title: string;
  prompt: string;
  model: string;
  column: ColumnId;
  priority: Priority;
  assignee: string;
  assignedConsumer: string;
  dueDate: string;
  tagsText: string;
}

interface PublisherAccepted {
  type: "accepted";
  task_id: string;
  stream_id: string;
}

interface PublisherError {
  type: "error";
  message: string;
}

interface ConsumerDiscovery {
  name: string;
  consumer_group: string;
  task_stream: string;
  direct_task_stream: string;
  result_stream: string;
  status: string;
  started_at_ms: number;
  last_seen_ms: number;
  expires_at_ms: number;
}

interface PublisherConsumers {
  type: "consumers";
  consumers: ConsumerDiscovery[];
}

interface PublisherTaskUpdate {
  type: "task_update";
  task_id?: string;
  card_id?: string;
  status: "done" | "needs_input" | "failed" | string;
  message?: string;
  questions?: string[];
  error?: string;
  consumer?: string;
  result_stream_id?: string;
  completed_at_ms?: number;
}

type PublisherResponse =
  | PublisherAccepted
  | PublisherError
  | PublisherConsumers
  | PublisherTaskUpdate;

const columns: Column[] = [
  { id: "backlog", label: "Backlog", accent: "#6b7280" },
  { id: "ready", label: "Ready", accent: "#2563eb" },
  { id: "in_progress", label: "In Progress", accent: "#ca8a04" },
  { id: "review", label: "Review", accent: "#9333ea" },
  { id: "done", label: "Done", accent: "#16803d" }
];

const priorityLabels: Record<Priority, string> = {
  low: "Low",
  medium: "Medium",
  high: "High"
};

const defaultModel = import.meta.env.VITE_DEFAULT_MODEL ?? "openclaw-chat";
const defaultWsUrl =
  import.meta.env.VITE_PUBLISHER_WS_URL ?? "ws://127.0.0.1:8080/ws";
const storageKey = "gyne-agent-kanban";

const initialCards: KanbanCard[] = [
  {
    id: crypto.randomUUID(),
    title: "Summarize patient intake notes",
    prompt:
      "Turn the intake notes into a concise clinical handoff with risks, missing data, and next actions.",
    model: defaultModel,
    column: "ready",
    priority: "high",
    assignee: "Ops",
    assignedConsumer: "",
    dueDate: todayPlus(1),
    tags: ["triage", "handoff"],
    publishStatus: "draft",
    createdAt: Date.now(),
    updatedAt: Date.now()
  },
  {
    id: crypto.randomUUID(),
    title: "Draft follow-up checklist",
    prompt:
      "Create a follow-up checklist for a patient who needs labs, imaging, and medication reconciliation.",
    model: defaultModel,
    column: "backlog",
    priority: "medium",
    assignee: "Care Team",
    assignedConsumer: "",
    dueDate: todayPlus(3),
    tags: ["checklist"],
    publishStatus: "draft",
    createdAt: Date.now(),
    updatedAt: Date.now()
  }
];

function App() {
  const [cards, setCards] = React.useState<KanbanCard[]>(loadCards);
  const [selectedId, setSelectedId] = React.useState<string | null>(
    cards[0]?.id ?? null
  );
  const [draft, setDraft] = React.useState<DraftCard>(newDraft());
  const [isEditorOpen, setIsEditorOpen] = React.useState(false);
  const [draggedId, setDraggedId] = React.useState<string | null>(null);
  const [wsUrl, setWsUrl] = React.useState(defaultWsUrl);
  const [socketStatus, setSocketStatus] =
    React.useState<SocketStatus>("closed");
  const [publishQueue, setPublishQueue] = React.useState<string[]>([]);
  const [pendingCardId, setPendingCardId] = React.useState<string | null>(null);
  const [consumers, setConsumers] = React.useState<ConsumerDiscovery[]>([]);
  const [lastDiscoveryAt, setLastDiscoveryAt] = React.useState<number | null>(null);
  const [lastEvent, setLastEvent] = React.useState("Disconnected");
  const socketRef = React.useRef<WebSocket | null>(null);
  const pendingCardRef = React.useRef<string | null>(null);
  const queueRef = React.useRef<string[]>([]);
  const cardsRef = React.useRef<KanbanCard[]>(cards);

  const selectedCard = cards.find((card) => card.id === selectedId) ?? null;
  const queuedCount = cards.filter((card) => card.publishStatus === "queued").length;
  const connectedConsumerCount = consumers.length;

  React.useEffect(() => {
    localStorage.setItem(storageKey, JSON.stringify(cards));
    cardsRef.current = cards;
  }, [cards]);

  React.useEffect(() => {
    return () => {
      socketRef.current?.close();
    };
  }, []);

  React.useEffect(() => {
    if (socketStatus !== "open") {
      return;
    }

    requestConsumers();
    const timer = window.setInterval(requestConsumers, 5000);
    return () => window.clearInterval(timer);
  }, [socketStatus]);

  function connect() {
    if (socketRef.current && socketStatus !== "closed") {
      return;
    }

    setSocketStatus("connecting");
    setLastEvent("Connecting");
    const socket = new WebSocket(wsUrl);
    socketRef.current = socket;

    socket.onopen = () => {
      setSocketStatus("open");
      setLastEvent("Connected");
      requestConsumers(socket);
      flushPublishQueue(socket);
    };

    socket.onclose = () => {
      setSocketStatus("closed");
      setLastEvent("Disconnected");
      setConsumers([]);
      socketRef.current = null;
      pendingCardRef.current = null;
      queueRef.current = [];
      setPublishQueue([]);
      setPendingCardId(null);
    };

    socket.onerror = () => {
      setSocketStatus("closed");
      setLastEvent("Connection error");
    };

    socket.onmessage = (event) => {
      handlePublisherResponse(event.data);
    };
  }

  function disconnect() {
    socketRef.current?.close();
    socketRef.current = null;
    pendingCardRef.current = null;
    queueRef.current = [];
    setPublishQueue([]);
    setPendingCardId(null);
    setSocketStatus("closed");
    setConsumers([]);
    setLastEvent("Disconnected");
  }

  function requestConsumers(socket = socketRef.current) {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return;
    }
    socket.send(JSON.stringify({ type: "list_consumers" }));
  }

  function flushPublishQueue(socket = socketRef.current) {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return;
    }

    if (pendingCardRef.current) {
      return;
    }

    const [nextId, ...remaining] = queueRef.current;
    if (!nextId) {
      return;
    }

    queueRef.current = remaining;
    setPublishQueue(remaining);

    const card = cardsRef.current.find((item) => item.id === nextId);
    if (!card) {
      window.setTimeout(() => flushPublishQueue(socket), 0);
      return;
    }

    pendingCardRef.current = nextId;
    setPendingCardId(nextId);
    socket.send(JSON.stringify(toPublisherPayload(card)));
    setLastEvent(`Publishing ${card.title}`);
  }

  React.useEffect(() => {
    if (socketStatus === "open" && publishQueue.length > 0 && !pendingCardId) {
      flushPublishQueue();
    }
  }, [socketStatus, publishQueue, pendingCardId]);

  function handlePublisherResponse(raw: string) {
    let response: PublisherResponse;
    try {
      response = JSON.parse(raw) as PublisherResponse;
    } catch {
      markPendingFailed("Publisher returned invalid JSON");
      return;
    }

    if (response.type === "consumers") {
      setConsumers(response.consumers);
      setLastDiscoveryAt(Date.now());
      setLastEvent(`${response.consumers.length} consumers connected`);
      return;
    }

    if (response.type === "task_update") {
      applyTaskUpdate(response);
      return;
    }

    const pendingId = pendingCardRef.current;
    pendingCardRef.current = null;
    setPendingCardId(null);

    if (!pendingId) {
      setLastEvent("Publisher response received");
      return;
    }

    if (response.type === "accepted") {
      setCards((current) =>
        current.map((card) =>
          card.id === pendingId
            ? {
                ...card,
                publishStatus: "queued",
                taskId: response.task_id,
                streamId: response.stream_id,
                error: undefined,
                lastPublishedAt: Date.now(),
                updatedAt: Date.now()
              }
            : card
        )
      );
      setLastEvent(`Queued ${response.task_id}`);
    } else {
      markCardFailed(pendingId, response.message);
    }
  }

  function markPendingFailed(message: string) {
    const pendingId = pendingCardRef.current;
    pendingCardRef.current = null;
    setPendingCardId(null);
    if (pendingId) {
      markCardFailed(pendingId, message);
    } else {
      setLastEvent(message);
    }
  }

  function markCardFailed(cardId: string, message: string) {
    setCards((current) =>
      current.map((card) =>
        card.id === cardId
          ? {
              ...card,
              publishStatus: "failed",
              error: message,
              updatedAt: Date.now()
            }
          : card
      )
    );
    setLastEvent(message);
  }

  function applyTaskUpdate(update: PublisherTaskUpdate) {
    const matched = cardsRef.current.some(
      (card) =>
        (update.card_id && card.id === update.card_id) ||
        (update.task_id && card.taskId === update.task_id)
    );
    setCards((current) =>
      current.map((card) => {
        const isMatch =
          (update.card_id && card.id === update.card_id) ||
          (update.task_id && card.taskId === update.task_id);
        if (!isMatch) {
          return card;
        }

        const status = normalizeUpdateStatus(update.status);
        return {
          ...card,
          column: columnForUpdateStatus(status, card.column),
          publishStatus: status,
          error: status === "failed" ? update.error || update.message : undefined,
          resultMessage: update.message,
          resultStreamId: update.result_stream_id,
          completedAt: update.completed_at_ms,
          questions: update.questions ?? [],
          completedBy: update.consumer,
          updatedAt: Date.now()
        };
      })
    );

    setLastEvent(
      matched
        ? update.message || statusText(update.status)
        : `Update for ${update.task_id ?? "unknown task"}`
    );
  }

  function openNewCard(column: ColumnId) {
    setDraft({ ...newDraft(), column });
    setSelectedId(null);
    setIsEditorOpen(true);
  }

  function openEditCard(card: KanbanCard) {
    setDraft({
      title: card.title,
      prompt: card.prompt,
      model: card.model,
      column: card.column,
      priority: card.priority,
      assignee: card.assignee,
      assignedConsumer: card.assignedConsumer,
      dueDate: card.dueDate,
      tagsText: card.tags.join(", ")
    });
    setSelectedId(card.id);
    setIsEditorOpen(true);
  }

  function saveDraft(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedTitle = draft.title.trim();
    const trimmedPrompt = draft.prompt.trim();
    if (!trimmedTitle || !trimmedPrompt) {
      return;
    }

    const tags = parseTags(draft.tagsText);

    if (selectedId) {
      setCards((current) =>
        current.map((card) =>
          card.id === selectedId
            ? {
                ...card,
                title: trimmedTitle,
                prompt: trimmedPrompt,
                model: draft.model.trim() || defaultModel,
                column: draft.column,
                priority: draft.priority,
                assignee: draft.assignee.trim(),
                assignedConsumer: draft.assignedConsumer,
                dueDate: draft.dueDate,
                tags,
                publishStatus: card.publishStatus === "queued" ? "draft" : card.publishStatus,
                taskId: undefined,
                streamId: undefined,
                error: undefined,
                resultMessage: undefined,
                resultStreamId: undefined,
                completedAt: undefined,
                questions: undefined,
                completedBy: undefined,
                lastPublishedAt: undefined,
                updatedAt: Date.now()
              }
            : card
        )
      );
    } else {
      const card: KanbanCard = {
        id: crypto.randomUUID(),
        title: trimmedTitle,
        prompt: trimmedPrompt,
        model: draft.model.trim() || defaultModel,
        column: draft.column,
        priority: draft.priority,
        assignee: draft.assignee.trim(),
        assignedConsumer: draft.assignedConsumer,
        dueDate: draft.dueDate,
        tags,
        publishStatus: "draft",
        createdAt: Date.now(),
        updatedAt: Date.now()
      };
      setCards((current) => [card, ...current]);
      setSelectedId(card.id);
    }

    setIsEditorOpen(false);
  }

  function deleteCard(cardId: string) {
    setCards((current) => current.filter((card) => card.id !== cardId));
    queueRef.current = queueRef.current.filter((id) => id !== cardId);
    setPublishQueue(queueRef.current);
    if (selectedId === cardId) {
      setSelectedId(null);
      setIsEditorOpen(false);
    }
  }

  function moveCard(cardId: string, column: ColumnId) {
    setCards((current) =>
      current.map((card) =>
        card.id === cardId ? { ...card, column, updatedAt: Date.now() } : card
      )
    );
  }

  function publishCard(cardId: string) {
    const card = cards.find((item) => item.id === cardId);
    if (!card) {
      return;
    }

    if (isPublishing(card)) {
      setLastEvent("Task already queued");
      return;
    }

    setCards((current) =>
      current.map((item) =>
        item.id === cardId
          ? {
              ...item,
              publishStatus: "draft",
              error: undefined,
              updatedAt: Date.now()
            }
          : item
      )
    );
    enqueueCards([cardId]);

    if (socketStatus === "closed") {
      connect();
    }
  }

  function publishColumn(column: ColumnId) {
    const ids = cards
      .filter((card) => card.column === column && !isPublishing(card))
      .map((card) => card.id);
    if (ids.length === 0) {
      return;
    }
    enqueueCards(ids);
    if (socketStatus === "closed") {
      connect();
    }
  }

  function enqueueCards(cardIds: string[]) {
    const existing = new Set(queueRef.current);
    const nextIds = cardIds.filter(
      (cardId) => !existing.has(cardId) && pendingCardRef.current !== cardId
    );
    if (nextIds.length === 0) {
      return;
    }

    queueRef.current = [...queueRef.current, ...nextIds];
    setPublishQueue(queueRef.current);
  }

  function isPublishing(card: KanbanCard) {
    return (
      card.publishStatus === "queued" ||
      publishQueue.includes(card.id) ||
      pendingCardId === card.id ||
      pendingCardRef.current === card.id
    );
  }

  function onDrop(column: ColumnId) {
    if (draggedId) {
      moveCard(draggedId, column);
      setDraggedId(null);
    }
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand-block">
          <div className="brand-mark" aria-hidden="true">
            <ClipboardList size={20} />
          </div>
          <div>
            <h1>Gyne Agent Kanban</h1>
            <p>{cards.length} cards · {queuedCount} queued · {connectedConsumerCount} consumers</p>
          </div>
        </div>

        <div className="connection-panel">
          <label className="ws-input">
            <span>Publisher</span>
            <input
              value={wsUrl}
              onChange={(event) => setWsUrl(event.target.value)}
              disabled={socketStatus !== "closed"}
            />
          </label>
          <div className={`status-pill ${socketStatus}`}>
            {socketStatus === "open" ? <Wifi size={16} /> : <WifiOff size={16} />}
            <span>{lastEvent}</span>
          </div>
          <button
            className="icon-button"
            onClick={() => requestConsumers()}
            disabled={socketStatus !== "open"}
            aria-label="Refresh consumers"
          >
            <RefreshCw size={17} />
          </button>
          {socketStatus === "open" ? (
            <button className="icon-button" onClick={disconnect} aria-label="Disconnect">
              <X size={18} />
            </button>
          ) : (
            <button className="primary-button" onClick={connect}>
              <Wifi size={18} />
              Connect
            </button>
          )}
        </div>
      </header>

      <section className="board" aria-label="Kanban board">
        {columns.map((column) => {
          const columnCards = cards.filter((card) => card.column === column.id);
          return (
            <article
              className="column"
              key={column.id}
              onDragOver={(event) => event.preventDefault()}
              onDrop={() => onDrop(column.id)}
              style={{ "--column-accent": column.accent } as React.CSSProperties}
            >
              <header className="column-header">
                <div>
                  <h2>{column.label}</h2>
                  <span>{columnCards.length}</span>
                </div>
                <div className="column-actions">
                  <button
                    className="icon-button"
                    onClick={() => publishColumn(column.id)}
                    aria-label={`Publish ${column.label}`}
                  >
                    <Send size={17} />
                  </button>
                  <button
                    className="icon-button"
                    onClick={() => openNewCard(column.id)}
                    aria-label={`Add card to ${column.label}`}
                  >
                    <Plus size={18} />
                  </button>
                </div>
              </header>

              <div className="card-list">
                {columnCards.map((card) => (
                  <KanbanCardView
                    key={card.id}
                    card={card}
                    publishing={isPublishing(card)}
                    active={card.id === selectedId}
                    onSelect={() => setSelectedId(card.id)}
                    onEdit={() => openEditCard(card)}
                    onDelete={() => deleteCard(card.id)}
                    onPublish={() => publishCard(card.id)}
                    onDragStart={() => setDraggedId(card.id)}
                    onDragEnd={() => setDraggedId(null)}
                  />
                ))}
              </div>
            </article>
          );
        })}
      </section>

      <aside className="detail-panel">
        <section className="consumer-panel">
          <header>
            <div className="panel-title">
              <Users size={18} />
              <span>Consumers</span>
            </div>
            <span>{lastDiscoveryAt ? formatTime(lastDiscoveryAt) : "Not loaded"}</span>
          </header>
          {consumers.length > 0 ? (
            <div className="consumer-list">
              {consumers.map((consumer) => (
                <div className="consumer-row" key={consumer.name}>
                  <div>
                    <strong>{consumer.name}</strong>
                    <span>{consumer.consumer_group}</span>
                  </div>
                  <span>{consumer.status}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="empty-consumers">No active consumers</div>
          )}
        </section>

        {selectedCard ? (
          <>
            <div className="detail-heading">
              <div>
                <p>{selectedCard.column.replace("_", " ")}</p>
                <h2>{selectedCard.title}</h2>
              </div>
              <button
                className="primary-button"
                onClick={() => publishCard(selectedCard.id)}
                disabled={isPublishing(selectedCard)}
              >
                <Send size={18} />
                Publish
              </button>
            </div>
            <div className="detail-grid">
              <Field label="Model" value={selectedCard.model} />
              <Field label="Priority" value={priorityLabels[selectedCard.priority]} />
              <Field label="Owner" value={selectedCard.assignee || "Unassigned"} />
              <Field label="Consumer" value={selectedCard.assignedConsumer || "Auto"} />
              <Field label="Completed By" value={selectedCard.completedBy || "None"} />
              <Field label="Due" value={selectedCard.dueDate || "None"} />
              <Field label="Task ID" value={selectedCard.taskId ?? "Not queued"} />
              <Field label="Result ID" value={selectedCard.resultStreamId ?? "None"} />
            </div>
            <div className="prompt-panel">
              <div className="panel-title">
                <MessageSquareText size={18} />
                <span>Prompt</span>
              </div>
              <p>{selectedCard.prompt}</p>
            </div>
            {selectedCard.resultMessage || selectedCard.questions?.length ? (
              <div className="result-panel">
                <div className="panel-title">
                  <Check size={18} />
                  <span>Result</span>
                </div>
                {selectedCard.resultMessage ? <p>{selectedCard.resultMessage}</p> : null}
                {selectedCard.questions?.length ? (
                  <ul className="question-list">
                    {selectedCard.questions.map((question) => (
                      <li key={question}>{question}</li>
                    ))}
                  </ul>
                ) : null}
              </div>
            ) : null}
            {selectedCard.error ? (
              <div className="error-panel">{selectedCard.error}</div>
            ) : null}
          </>
        ) : (
          <div className="empty-detail">
            <Archive size={28} />
            <span>No card selected</span>
          </div>
        )}
      </aside>

      {isEditorOpen ? (
        <div className="modal-backdrop" role="presentation">
          <form className="editor" onSubmit={saveDraft}>
            <header>
              <h2>{selectedId ? "Edit Card" : "New Card"}</h2>
              <button
                type="button"
                className="icon-button"
                onClick={() => setIsEditorOpen(false)}
                aria-label="Close editor"
              >
                <X size={18} />
              </button>
            </header>

            <label>
              <span>Title</span>
              <input
                value={draft.title}
                onChange={(event) =>
                  setDraft((current) => ({ ...current, title: event.target.value }))
                }
                required
              />
            </label>

            <label>
              <span>Prompt</span>
              <textarea
                value={draft.prompt}
                onChange={(event) =>
                  setDraft((current) => ({ ...current, prompt: event.target.value }))
                }
                required
              />
            </label>

            <div className="form-row">
              <label>
                <span>Model</span>
                <input
                  value={draft.model}
                  onChange={(event) =>
                    setDraft((current) => ({ ...current, model: event.target.value }))
                  }
                />
              </label>
              <label>
                <span>Column</span>
                <select
                  value={draft.column}
                  onChange={(event) =>
                    setDraft((current) => ({
                      ...current,
                      column: event.target.value as ColumnId
                    }))
                  }
                >
                  {columns.map((column) => (
                    <option key={column.id} value={column.id}>
                      {column.label}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <div className="form-row">
              <label>
                <span>Priority</span>
                <select
                  value={draft.priority}
                  onChange={(event) =>
                    setDraft((current) => ({
                      ...current,
                      priority: event.target.value as Priority
                    }))
                  }
                >
                  <option value="low">Low</option>
                  <option value="medium">Medium</option>
                  <option value="high">High</option>
                </select>
              </label>
              <label>
                <span>Due Date</span>
                <input
                  type="date"
                  value={draft.dueDate}
                  onChange={(event) =>
                    setDraft((current) => ({ ...current, dueDate: event.target.value }))
                  }
                />
              </label>
            </div>

            <div className="form-row">
              <label>
                <span>Owner</span>
                <input
                  value={draft.assignee}
                  onChange={(event) =>
                    setDraft((current) => ({
                      ...current,
                      assignee: event.target.value
                    }))
                  }
                />
              </label>
              <label>
                <span>Tags</span>
                <input
                  value={draft.tagsText}
                  onChange={(event) =>
                    setDraft((current) => ({
                      ...current,
                      tagsText: event.target.value
                    }))
                  }
                />
              </label>
            </div>

            <label>
              <span>Assigned Consumer</span>
              <select
                value={draft.assignedConsumer}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    assignedConsumer: event.target.value
                  }))
                }
              >
                <option value="">Auto</option>
                {draft.assignedConsumer &&
                !consumers.some((consumer) => consumer.name === draft.assignedConsumer) ? (
                  <option value={draft.assignedConsumer}>{draft.assignedConsumer}</option>
                ) : null}
                {consumers.map((consumer) => (
                  <option key={consumer.name} value={consumer.name}>
                    {consumer.name}
                  </option>
                ))}
              </select>
            </label>

            <footer>
              <button type="button" className="secondary-button" onClick={() => setIsEditorOpen(false)}>
                <X size={18} />
                Cancel
              </button>
              <button type="submit" className="primary-button">
                <Check size={18} />
                Save
              </button>
            </footer>
          </form>
        </div>
      ) : null}
    </main>
  );
}

function KanbanCardView({
  card,
  publishing,
  active,
  onSelect,
  onEdit,
  onDelete,
  onPublish,
  onDragStart,
  onDragEnd
}: {
  card: KanbanCard;
  publishing: boolean;
  active: boolean;
  onSelect: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onPublish: () => void;
  onDragStart: () => void;
  onDragEnd: () => void;
}) {
  return (
    <section
      className={`task-card ${active ? "active" : ""}`}
      draggable
      onDragStart={onDragStart}
      onDragEnd={onDragEnd}
      onClick={onSelect}
    >
      <header>
        <span className={`priority ${card.priority}`}>{priorityLabels[card.priority]}</span>
        <StatusBadge status={card.publishStatus} />
      </header>
      <h3>{card.title}</h3>
      <p>{card.prompt}</p>
      <div className="tag-row">
        {card.tags.map((tag) => (
          <span key={tag}>{tag}</span>
        ))}
      </div>
      <footer>
        <span>{card.assignee || "Unassigned"}</span>
        <div className="card-actions">
          <button
            className="icon-button"
            disabled={publishing}
            onClick={(event) => {
              event.stopPropagation();
              onPublish();
            }}
            aria-label={`Publish ${card.title}`}
          >
            <Send size={16} />
          </button>
          <button
            className="icon-button"
            onClick={(event) => {
              event.stopPropagation();
              onEdit();
            }}
            aria-label={`Edit ${card.title}`}
          >
            <MessageSquareText size={16} />
          </button>
          <button
            className="icon-button danger"
            onClick={(event) => {
              event.stopPropagation();
              onDelete();
            }}
            aria-label={`Delete ${card.title}`}
          >
            <Trash2 size={16} />
          </button>
        </div>
      </footer>
    </section>
  );
}

function StatusBadge({ status }: { status: PublishStatus }) {
  if (status === "queued") {
    return (
      <span className="publish-status queued">
        <Loader2 size={14} />
        Queued
      </span>
    );
  }

  if (status === "failed") {
    return (
      <span className="publish-status failed">
        <Circle size={14} />
        Failed
      </span>
    );
  }

  if (status === "done") {
    return (
      <span className="publish-status done">
        <Check size={14} />
        Done
      </span>
    );
  }

  if (status === "needs_input") {
    return (
      <span className="publish-status needs-input">
        <MessageSquareText size={14} />
        Review
      </span>
    );
  }

  return (
    <span className="publish-status draft">
      <Circle size={14} />
      Draft
    </span>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="field">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function toPublisherPayload(card: KanbanCard) {
  return {
    task_id: card.taskId,
    model: card.model,
    assigned_consumer: card.assignedConsumer || undefined,
    messages: [
      {
        role: "user",
        content: `${card.title}\n\n${card.prompt}`
      }
    ],
    metadata: {
      card_id: card.id,
      title: card.title,
      column: card.column,
      priority: card.priority,
      assignee: card.assignee,
      assigned_consumer: card.assignedConsumer || null,
      due_date: card.dueDate,
      tags: card.tags
    }
  };
}

function loadCards() {
  const stored = localStorage.getItem(storageKey);
  if (!stored) {
    return initialCards;
  }

  try {
    const parsed = JSON.parse(stored) as KanbanCard[];
    return Array.isArray(parsed)
      ? parsed.map((card) => ({ ...card, assignedConsumer: card.assignedConsumer ?? "" }))
      : initialCards;
  } catch {
    return initialCards;
  }
}

function newDraft(): DraftCard {
  return {
    title: "",
    prompt: "",
    model: defaultModel,
    column: "backlog",
    priority: "medium",
    assignee: "",
    assignedConsumer: "",
    dueDate: todayPlus(2),
    tagsText: ""
  };
}

function parseTags(value: string) {
  return value
    .split(",")
    .map((tag) => tag.trim())
    .filter(Boolean);
}

function normalizeUpdateStatus(status: string): PublishStatus {
  if (status === "done" || status === "completed") {
    return "done";
  }
  if (status === "needs_input") {
    return "needs_input";
  }
  if (status === "failed") {
    return "failed";
  }
  return "queued";
}

function columnForUpdateStatus(status: PublishStatus, currentColumn: ColumnId): ColumnId {
  if (status === "done") {
    return "done";
  }
  if (status === "needs_input" || status === "failed") {
    return "review";
  }
  return currentColumn;
}

function statusText(status: string) {
  if (status === "done" || status === "completed") {
    return "Task completed";
  }
  if (status === "needs_input") {
    return "Task needs review";
  }
  if (status === "failed") {
    return "Task failed";
  }
  return "Task updated";
}

function todayPlus(days: number) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
}

function formatTime(value: number) {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(value);
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

import React from "react";
import { createRoot } from "react-dom/client";
import {
  Archive,
  Check,
  Circle,
  ClipboardList,
  Copy,
  Loader2,
  MessageSquareText,
  Plus,
  RefreshCw,
  RotateCcw,
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
type PublishKind = "work" | "review";
type SocketStatus = "closed" | "connecting" | "open";

interface Column {
  id: ColumnId;
  label: string;
  accent: string;
}

interface KanbanCard {
  id: string;
  taskId: string;
  title: string;
  prompt: string;
  model: string;
  column: ColumnId;
  priority: Priority;
  assignee: string;
  assignedConsumer: string;
  requiresReview: boolean;
  dependsOnTaskId: string;
  autoPublishOnDependency: boolean;
  dueDate: string;
  tags: string[];
  publishStatus: PublishStatus;
  streamId?: string;
  error?: string;
  resultMessage?: string;
  resultStreamId?: string;
  completedAt?: number;
  questions?: string[];
  completedBy?: string;
  reviewTaskId?: string;
  reviewStreamId?: string;
  reviewConsumer?: string;
  reviewedBy?: string;
  reviewError?: string;
  reviewRequestedAt?: number;
  reviewCompletedAt?: number;
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
  requiresReview: boolean;
  dependsOnTaskId: string;
  autoPublishOnDependency: boolean;
  dueDate: string;
  tagsText: string;
}

interface PublishQueueItem {
  cardId: string;
  kind: PublishKind;
  excludeConsumer?: string;
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
  hostname?: string;
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
  task_kind?: string;
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

const defaultModel = import.meta.env.VITE_DEFAULT_MODEL ?? "openclaw";
const defaultWsUrl =
  import.meta.env.VITE_PUBLISHER_WS_URL ?? "ws://127.0.0.1:8080/ws";
const storageKey = "gyne-agent-kanban";
const singaporeTimeZone = "Asia/Singapore";
const singaporeTimeZoneLabel = "SGT";

const initialCards: KanbanCard[] = [
  {
    id: crypto.randomUUID(),
    taskId: crypto.randomUUID(),
    title: "Summarize patient intake notes",
    prompt:
      "Turn the intake notes into a concise clinical handoff with risks, missing data, and next actions.",
    model: defaultModel,
    column: "ready",
    priority: "high",
    assignee: "Ops",
    assignedConsumer: "",
    requiresReview: false,
    dependsOnTaskId: "",
    autoPublishOnDependency: false,
    dueDate: todayPlus(1),
    tags: ["triage", "handoff"],
    publishStatus: "draft",
    createdAt: Date.now(),
    updatedAt: Date.now()
  },
  {
    id: crypto.randomUUID(),
    taskId: crypto.randomUUID(),
    title: "Draft follow-up checklist",
    prompt:
      "Create a follow-up checklist for a patient who needs labs, imaging, and medication reconciliation.",
    model: defaultModel,
    column: "backlog",
    priority: "medium",
    assignee: "Care Team",
    assignedConsumer: "",
    requiresReview: false,
    dependsOnTaskId: "",
    autoPublishOnDependency: false,
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
  const [reworkMode, setReworkMode] = React.useState(false);
  const [draggedId, setDraggedId] = React.useState<string | null>(null);
  const [wsUrl, setWsUrl] = React.useState(defaultWsUrl);
  const [socketStatus, setSocketStatus] =
    React.useState<SocketStatus>("closed");
  const [publishQueue, setPublishQueue] = React.useState<PublishQueueItem[]>([]);
  const [pendingCardId, setPendingCardId] = React.useState<string | null>(null);
  const [consumers, setConsumers] = React.useState<ConsumerDiscovery[]>([]);
  const [lastDiscoveryAt, setLastDiscoveryAt] = React.useState<number | null>(null);
  const [lastEvent, setLastEvent] = React.useState("Disconnected");
  const socketRef = React.useRef<WebSocket | null>(null);
  const pendingCardRef = React.useRef<string | null>(null);
  const pendingKindRef = React.useRef<PublishKind>("work");
  const pendingExcludeConsumerRef = React.useRef<string | undefined>(undefined);
  const pendingAssignedConsumerRef = React.useRef<string | undefined>(undefined);
  const queueRef = React.useRef<PublishQueueItem[]>([]);
  const cardsRef = React.useRef<KanbanCard[]>(cards);
  const consumersRef = React.useRef<ConsumerDiscovery[]>(consumers);

  const selectedCard = cards.find((card) => card.id === selectedId) ?? null;
  const queuedCount = cards.filter((card) => card.publishStatus === "queued").length;
  const connectedConsumerCount = consumers.length;

  React.useEffect(() => {
    localStorage.setItem(storageKey, JSON.stringify(cards));
    cardsRef.current = cards;
  }, [cards]);

  React.useEffect(() => {
    consumersRef.current = consumers;
  }, [consumers]);

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
      clearPendingPublish();
      queueRef.current = [];
      setPublishQueue([]);
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
    clearPendingPublish();
    queueRef.current = [];
    setPublishQueue([]);
    setSocketStatus("closed");
    setConsumers([]);
    setLastEvent("Disconnected");
  }

  function clearPendingPublish() {
    pendingCardRef.current = null;
    pendingKindRef.current = "work";
    pendingExcludeConsumerRef.current = undefined;
    pendingAssignedConsumerRef.current = undefined;
    setPendingCardId(null);
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

    const [nextItem, ...remaining] = queueRef.current;
    if (!nextItem) {
      return;
    }

    queueRef.current = remaining;
    setPublishQueue(remaining);

    const card = cardsRef.current.find((item) => item.id === nextItem.cardId);
    if (!card) {
      window.setTimeout(() => flushPublishQueue(socket), 0);
      return;
    }

    const dependencyBlock =
      nextItem.kind === "work" ? dependencyBlockMessage(card) : null;
    if (dependencyBlock) {
      markCardBlocked(nextItem.cardId, dependencyBlock);
      window.setTimeout(() => flushPublishQueue(socket), 0);
      return;
    }

    const assignedConsumer =
      nextItem.kind === "review"
        ? selectReviewConsumer(nextItem.excludeConsumer)
        : card.assignedConsumer || undefined;

    if (nextItem.kind === "review" && !assignedConsumer) {
      markCardFailed(
        nextItem.cardId,
        "Review requires another active consumer, but none is available.",
        "review"
      );
      window.setTimeout(() => flushPublishQueue(socket), 0);
      return;
    }

    pendingCardRef.current = nextItem.cardId;
    pendingKindRef.current = nextItem.kind;
    pendingExcludeConsumerRef.current = nextItem.excludeConsumer;
    pendingAssignedConsumerRef.current = assignedConsumer;
    setPendingCardId(nextItem.cardId);
    setCards((current) =>
      current.map((item) =>
        item.id === nextItem.cardId
          ? {
              ...item,
              column: nextItem.kind === "review" ? "review" : "in_progress",
              publishStatus: "queued",
              error: undefined,
              reviewError: nextItem.kind === "review" ? undefined : item.reviewError,
              updatedAt: Date.now()
            }
          : item
      )
    );
    socket.send(
      JSON.stringify(
        toPublisherPayload(card, {
          kind: nextItem.kind,
          assignedConsumer,
          excludeConsumer: nextItem.excludeConsumer
        })
      )
    );
    setLastEvent(
      nextItem.kind === "review"
        ? `Routing review for ${card.title}`
        : `Publishing ${card.title}`
    );
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
    const pendingKind = pendingKindRef.current;
    const pendingAssignedConsumer = pendingAssignedConsumerRef.current;
    clearPendingPublish();

    if (!pendingId) {
      setLastEvent("Publisher response received");
      return;
    }

    if (response.type === "accepted") {
      setCards((current) =>
        current.map((card) =>
          card.id === pendingId
            ? pendingKind === "review"
              ? {
                  ...card,
                  column: "review",
                  publishStatus: "queued",
                  reviewTaskId: response.task_id,
                  reviewStreamId: response.stream_id,
                  reviewConsumer: pendingAssignedConsumer,
                  reviewError: undefined,
                  error: undefined,
                  reviewRequestedAt: Date.now(),
                  lastPublishedAt: Date.now(),
                  updatedAt: Date.now()
                }
              : {
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
      setLastEvent(
        pendingKind === "review"
          ? `Review queued ${response.task_id}`
          : `Queued ${response.task_id}`
      );
    } else {
      markCardFailed(pendingId, response.message, pendingKind);
    }
  }

  function markPendingFailed(message: string) {
    const pendingId = pendingCardRef.current;
    const pendingKind = pendingKindRef.current;
    clearPendingPublish();
    if (pendingId) {
      markCardFailed(pendingId, message, pendingKind);
    } else {
      setLastEvent(message);
    }
  }

  function markCardFailed(cardId: string, message: string, kind: PublishKind = "work") {
    setCards((current) =>
      current.map((card) =>
        card.id === cardId
          ? {
              ...card,
              column: kind === "review" ? "review" : card.column,
              publishStatus: "failed",
              error: message,
              reviewError: kind === "review" ? message : card.reviewError,
              updatedAt: Date.now()
            }
          : card
      )
    );
    setLastEvent(message);
  }

  function markCardBlocked(cardId: string, message: string) {
    setCards((current) =>
      current.map((card) =>
        card.id === cardId
          ? {
              ...card,
              column: "backlog",
              publishStatus: "draft",
              error: message,
              updatedAt: Date.now()
            }
          : card
      )
    );
    setLastEvent(message);
  }

  function applyTaskUpdate(update: PublisherTaskUpdate) {
    const matchedCard = cardsRef.current.find((card) => taskUpdateKind(card, update));
    const updateKind = matchedCard ? taskUpdateKind(matchedCard, update) : null;
    const status = normalizeUpdateStatus(update.status);
    const shouldRouteReview =
      matchedCard &&
      updateKind === "work" &&
      status === "done" &&
      matchedCard.requiresReview &&
      !matchedCard.reviewTaskId &&
      !matchedCard.reviewCompletedAt &&
      !isQueuedOrPending(matchedCard.id, "review");
    const completedDependencyTaskId =
      matchedCard &&
      status === "done" &&
      (updateKind === "review" || (updateKind === "work" && !shouldRouteReview))
        ? matchedCard.taskId
        : undefined;
    const autoPublishIds = completedDependencyTaskId
      ? cardsRef.current
          .filter(
            (card) =>
              card.column === "backlog" &&
              card.dependsOnTaskId === completedDependencyTaskId &&
              card.autoPublishOnDependency &&
              !isPublishing(card)
          )
          .map((card) => card.id)
      : [];

    setCards((current) => {
      const updatedCards = current.map((card) => {
        const kind = taskUpdateKind(card, update);
        if (!kind) {
          return card;
        }

        if (kind === "review") {
          return {
            ...card,
            column: columnForReviewUpdateStatus(status),
            publishStatus: status,
            error: status === "failed" ? update.error || update.message : undefined,
            reviewError:
              status === "failed" || status === "needs_input"
                ? update.error || update.message
                : undefined,
            resultMessage: update.message,
            resultStreamId: update.result_stream_id,
            questions: update.questions ?? [],
            reviewedBy: update.consumer,
            reviewCompletedAt: update.completed_at_ms,
            updatedAt: Date.now()
          };
        }

        const routeReview =
          status === "done" &&
          card.requiresReview &&
          !card.reviewTaskId &&
          !card.reviewCompletedAt &&
          !isQueuedOrPending(card.id, "review");

        return {
          ...card,
          column: routeReview ? "review" : columnForUpdateStatus(status, card.column),
          publishStatus: routeReview ? "queued" : status,
          error: status === "failed" ? update.error || update.message : undefined,
          resultMessage: update.message,
          resultStreamId: update.result_stream_id,
          completedAt: update.completed_at_ms,
          questions: update.questions ?? [],
          completedBy: update.consumer,
          updatedAt: Date.now()
        };
      });

      if (!completedDependencyTaskId) {
        return updatedCards;
      }

      return releaseDependentCards(updatedCards, completedDependencyTaskId);
    });

    if (shouldRouteReview && matchedCard) {
      enqueueCards([matchedCard.id], "review", update.consumer);
    }
    if (autoPublishIds.length > 0) {
      enqueueCards(autoPublishIds, "work");
      if (socketStatus === "closed") {
        connect();
      }
    }

    setLastEvent(
      matchedCard
        ? update.message || statusText(update.status)
        : `Update for ${update.task_id ?? "unknown task"}`
    );
  }

  function openNewCard(column: ColumnId) {
    setDraft({ ...newDraft(), column });
    setSelectedId(null);
    setReworkMode(false);
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
      requiresReview: card.requiresReview,
      dependsOnTaskId: card.dependsOnTaskId,
      autoPublishOnDependency: card.autoPublishOnDependency,
      dueDate: card.dueDate,
      tagsText: card.tags.join(", ")
    });
    setSelectedId(card.id);
    setReworkMode(false);
    setIsEditorOpen(true);
  }

  // Reroute a finished task back to Backlog as a clean draft so its prompt can
  // be fixed and re-run. On save (see saveDraft) the card gets a fresh task id
  // and its publish status resets to "draft"; the stale result is cleared.
  function openReworkCard(card: KanbanCard) {
    setDraft({
      title: card.title,
      prompt: card.prompt,
      model: card.model,
      column: "backlog",
      priority: card.priority,
      assignee: card.assignee,
      assignedConsumer: card.assignedConsumer,
      requiresReview: card.requiresReview,
      dependsOnTaskId: card.dependsOnTaskId,
      autoPublishOnDependency: card.autoPublishOnDependency,
      dueDate: card.dueDate,
      tagsText: card.tags.join(", ")
    });
    setSelectedId(card.id);
    setReworkMode(true);
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
                requiresReview: draft.requiresReview,
                dependsOnTaskId: draft.column === "backlog" ? draft.dependsOnTaskId : "",
                autoPublishOnDependency:
                  draft.column === "backlog" ? draft.autoPublishOnDependency : false,
                dueDate: draft.dueDate,
                tags,
                // Rework re-runs as a brand new task: fresh id, clean draft status.
                taskId: reworkMode ? crypto.randomUUID() : card.taskId,
                publishStatus:
                  reworkMode || card.publishStatus === "queued" ? "draft" : card.publishStatus,
                streamId: undefined,
                error: undefined,
                resultMessage: undefined,
                resultStreamId: undefined,
                completedAt: undefined,
                questions: undefined,
                completedBy: undefined,
                reviewTaskId: undefined,
                reviewStreamId: undefined,
                reviewConsumer: undefined,
                reviewedBy: undefined,
                reviewError: undefined,
                reviewRequestedAt: undefined,
                reviewCompletedAt: undefined,
                lastPublishedAt: undefined,
                updatedAt: Date.now()
              }
            : card
        )
      );
    } else {
      const card: KanbanCard = {
        id: crypto.randomUUID(),
        taskId: crypto.randomUUID(),
        title: trimmedTitle,
        prompt: trimmedPrompt,
        model: draft.model.trim() || defaultModel,
        column: draft.column,
        priority: draft.priority,
        assignee: draft.assignee.trim(),
        assignedConsumer: draft.assignedConsumer,
        requiresReview: draft.requiresReview,
        dependsOnTaskId: draft.column === "backlog" ? draft.dependsOnTaskId : "",
        autoPublishOnDependency:
          draft.column === "backlog" ? draft.autoPublishOnDependency : false,
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
    setReworkMode(false);
  }

  function deleteCard(cardId: string) {
    setCards((current) => current.filter((card) => card.id !== cardId));
    queueRef.current = queueRef.current.filter((item) => item.cardId !== cardId);
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

    const dependencyBlock = dependencyBlockMessage(card);
    if (dependencyBlock) {
      markCardBlocked(cardId, dependencyBlock);
      return;
    }

    setCards((current) =>
      current.map((item) =>
        item.id === cardId
          ? {
              ...item,
              publishStatus: "draft",
              error: undefined,
              reviewTaskId: undefined,
              reviewStreamId: undefined,
              reviewConsumer: undefined,
              reviewedBy: undefined,
              reviewError: undefined,
              reviewRequestedAt: undefined,
              reviewCompletedAt: undefined,
              updatedAt: Date.now()
            }
          : item
      )
    );
    enqueueCards([cardId], "work");

    if (socketStatus === "closed") {
      connect();
    }
  }

  // Retrigger a task stuck in "queued" (handed to the publisher but no result
  // ever came back). We abandon the prior attempt, give the re-run a fresh task
  // id so it has its own stream identity, clear the stale result, and re-enqueue.
  function retryCard(cardId: string) {
    const card = cards.find((item) => item.id === cardId);
    if (!card || card.publishStatus !== "queued") {
      return;
    }

    const isReview = card.column === "review";

    // Drop any local queue/pending state for this card so the fresh enqueue
    // isn't deduped, and so a late ack for the abandoned send is ignored.
    if (pendingCardRef.current === cardId) {
      clearPendingPublish();
    }
    queueRef.current = queueRef.current.filter((item) => item.cardId !== cardId);
    setPublishQueue(queueRef.current);

    setCards((current) =>
      current.map((item) =>
        item.id === cardId
          ? {
              ...item,
              publishStatus: "draft",
              ...(isReview
                ? {
                    reviewTaskId: undefined,
                    reviewStreamId: undefined,
                    reviewError: undefined,
                    reviewRequestedAt: undefined,
                    reviewCompletedAt: undefined
                  }
                : { taskId: crypto.randomUUID() }),
              streamId: undefined,
              error: undefined,
              resultMessage: undefined,
              resultStreamId: undefined,
              completedAt: undefined,
              questions: undefined,
              updatedAt: Date.now()
            }
          : item
      )
    );

    enqueueCards([cardId], isReview ? "review" : "work", isReview ? card.completedBy : undefined);

    if (socketStatus === "closed") {
      connect();
    }
    setLastEvent(`Retrying ${card.title}`);
  }

  function publishColumn(column: ColumnId) {
    const candidates = cards.filter((card) => card.column === column && !isPublishing(card));
    const blocked = candidates
      .map((card) => ({ card, message: dependencyBlockMessage(card) }))
      .filter((item): item is { card: KanbanCard; message: string } => Boolean(item.message));
    if (blocked.length > 0) {
      setCards((current) =>
        current.map((card) => {
          const blockedItem = blocked.find((item) => item.card.id === card.id);
          return blockedItem
            ? {
                ...card,
                column: "backlog",
                publishStatus: "draft",
                error: blockedItem.message,
                updatedAt: Date.now()
              }
            : card;
        })
      );
      setLastEvent(`${blocked.length} task${blocked.length === 1 ? "" : "s"} waiting on dependencies`);
    }

    const ids = candidates
      .filter((card) => !dependencyBlockMessage(card))
      .map((card) => card.id);
    if (ids.length === 0) {
      return;
    }
    enqueueCards(ids, "work");
    if (socketStatus === "closed") {
      connect();
    }
  }

  function enqueueCards(
    cardIds: string[],
    kind: PublishKind,
    excludeConsumer?: string
  ) {
    const existing = new Set(queueRef.current.map(queueItemKey));
    const nextItems = cardIds
      .map((cardId) => ({ cardId, kind, excludeConsumer }))
      .filter(
        (item) =>
          !existing.has(queueItemKey(item)) &&
          !(pendingCardRef.current === item.cardId && pendingKindRef.current === item.kind)
      );
    if (nextItems.length === 0) {
      return;
    }

    queueRef.current = [...queueRef.current, ...nextItems];
    setPublishQueue(queueRef.current);
  }

  function isPublishing(card: KanbanCard) {
    return (
      card.publishStatus === "queued" ||
      publishQueue.some((item) => item.cardId === card.id) ||
      pendingCardId === card.id ||
      pendingCardRef.current === card.id
    );
  }

  function isQueuedOrPending(cardId: string, kind?: PublishKind) {
    return (
      queueRef.current.some(
        (item) => item.cardId === cardId && (!kind || item.kind === kind)
      ) ||
      (pendingCardRef.current === cardId && (!kind || pendingKindRef.current === kind))
    );
  }

  function selectReviewConsumer(excludeConsumer?: string) {
    return consumersRef.current.find((consumer) => consumer.name !== excludeConsumer)?.name;
  }

  function dependencyBlockMessage(card: KanbanCard) {
    if (!card.dependsOnTaskId) {
      return null;
    }

    const dependency = cardsRef.current.find(
      (item) => item.taskId === card.dependsOnTaskId
    );
    if (!dependency) {
      return "Waiting for dependency, but the referenced task was not found.";
    }
    if (isDoneCard(dependency)) {
      return null;
    }
    return `Waiting for dependency: ${dependency.title}`;
  }

  async function copyTaskId(taskId: string) {
    try {
      await navigator.clipboard.writeText(taskId);
      setLastEvent("Task ID copied");
    } catch {
      setLastEvent("Could not copy task ID");
    }
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
            <p>
              {cards.length} cards · {queuedCount} queued · {connectedConsumerCount} consumers ·{" "}
              {singaporeTimeZoneLabel}
            </p>
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
                    onRework={() => openReworkCard(card)}
                    onRetry={() => retryCard(card.id)}
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
                    <span>{consumer.hostname || "Hostname unavailable"}</span>
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
              <div className="detail-heading-actions">
                {selectedCard.publishStatus === "queued" ? (
                  <button
                    className="secondary-button"
                    onClick={() => retryCard(selectedCard.id)}
                    title="Retry: abandon the stuck attempt and re-publish with a new Task ID"
                  >
                    <RotateCcw size={18} />
                    Retry
                  </button>
                ) : null}
                {isDoneCard(selectedCard) ? (
                  <button
                    className="secondary-button"
                    onClick={() => openReworkCard(selectedCard)}
                  >
                    <RefreshCw size={18} />
                    Rework
                  </button>
                ) : null}
                <button
                  className="primary-button"
                  onClick={() => publishCard(selectedCard.id)}
                  disabled={isPublishing(selectedCard)}
                >
                  <Send size={18} />
                  Publish
                </button>
              </div>
            </div>
            <div className="detail-grid">
              <Field label="Model" value={selectedCard.model} />
              <Field label="Priority" value={priorityLabels[selectedCard.priority]} />
              <Field label="Owner" value={selectedCard.assignee || "Unassigned"} />
              <Field label="Consumer" value={selectedCard.assignedConsumer || "Auto"} />
              <Field label="Review" value={selectedCard.requiresReview ? "Required" : "Off"} />
              <Field label="Completed By" value={selectedCard.completedBy || "None"} />
              <Field
                label="Completed At"
                value={selectedCard.completedAt ? formatTime(selectedCard.completedAt) : "None"}
              />
              <Field
                label="Reviewer"
                value={
                  selectedCard.reviewedBy || selectedCard.reviewConsumer || "None"
                }
              />
              <Field
                label="Reviewed At"
                value={
                  selectedCard.reviewCompletedAt
                    ? formatTime(selectedCard.reviewCompletedAt)
                    : "None"
                }
              />
              <Field
                label="Depends On"
                value={dependencyLabel(cards, selectedCard.dependsOnTaskId)}
              />
              <Field
                label="Auto Publish"
                value={selectedCard.autoPublishOnDependency ? "On" : "Off"}
              />
              <Field label="Due" value={selectedCard.dueDate || "None"} />
              <TaskIdField
                taskId={selectedCard.taskId}
                onCopy={() => copyTaskId(selectedCard.taskId)}
              />
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
              <h2>{reworkMode ? "Rework Task" : selectedId ? "Edit Task" : "New Task"}</h2>
              <button
                type="button"
                className="icon-button"
                onClick={() => {
                  setIsEditorOpen(false);
                  setReworkMode(false);
                }}
                aria-label="Close editor"
              >
                <X size={18} />
              </button>
            </header>

            {reworkMode ? (
              <p className="editor-hint">
                Fix the prompt below. Saving moves this task back to Backlog as a
                fresh draft (new Task ID); publish it again when you're ready to re-run.
              </p>
            ) : null}

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
                  onChange={(event) => {
                    const column = event.target.value as ColumnId;
                    setDraft((current) => ({
                      ...current,
                      column,
                      dependsOnTaskId: column === "backlog" ? current.dependsOnTaskId : "",
                      autoPublishOnDependency:
                        column === "backlog" ? current.autoPublishOnDependency : false
                    }));
                  }}
                >
                  {columns.map((column) => (
                    <option key={column.id} value={column.id}>
                      {column.label}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            {draft.column === "backlog" ? (
              <div className="dependency-fields">
                <label>
                  <span>Dependency Task ID</span>
                  <input
                    value={draft.dependsOnTaskId}
                    placeholder="Paste a task UUID"
                    onChange={(event) =>
                      setDraft((current) => ({
                        ...current,
                        dependsOnTaskId: event.target.value.trim()
                      }))
                    }
                  />
                </label>

                <label>
                  <span>Pick Existing Task</span>
                  <select
                    value={draft.dependsOnTaskId}
                    onChange={(event) =>
                      setDraft((current) => ({
                        ...current,
                        dependsOnTaskId: event.target.value
                      }))
                    }
                  >
                    <option value="">None</option>
                    {cards
                      .filter((card) => card.id !== selectedId)
                      .map((card) => (
                        <option key={card.id} value={card.taskId}>
                          {card.title}
                        </option>
                      ))}
                  </select>
                </label>

                <label className="checkbox-field">
                  <input
                    type="checkbox"
                    checked={draft.autoPublishOnDependency}
                    disabled={!draft.dependsOnTaskId}
                    onChange={(event) =>
                      setDraft((current) => ({
                        ...current,
                        autoPublishOnDependency: event.target.checked
                      }))
                    }
                  />
                  <span>Publish automatically when dependency is done</span>
                </label>
              </div>
            ) : null}

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

            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={draft.requiresReview}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    requiresReview: event.target.checked
                  }))
                }
              />
              <span>Route completion to another consumer for review</span>
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
  onRework,
  onRetry,
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
  onRework: () => void;
  onRetry: () => void;
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
          {card.publishStatus === "queued" ? (
            <button
              className="icon-button"
              onClick={(event) => {
                event.stopPropagation();
                onRetry();
              }}
              aria-label={`Retry ${card.title}`}
              title="Retry: abandon the stuck attempt and re-publish with a new Task ID"
            >
              <RotateCcw size={16} />
            </button>
          ) : null}
          {isDoneCard(card) ? (
            <button
              className="icon-button"
              onClick={(event) => {
                event.stopPropagation();
                onRework();
              }}
              aria-label={`Rework ${card.title}`}
              title="Rework: edit the prompt and send back to Backlog"
            >
              <RefreshCw size={16} />
            </button>
          ) : null}
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

function TaskIdField({
  taskId,
  onCopy
}: {
  taskId: string;
  onCopy: () => void;
}) {
  return (
    <div className="field task-id-field">
      <span>Task ID</span>
      <div>
        <strong>{taskId}</strong>
        <button
          type="button"
          className="icon-button"
          onClick={onCopy}
          aria-label="Copy task ID"
        >
          <Copy size={15} />
        </button>
      </div>
    </div>
  );
}

function toPublisherPayload(
  card: KanbanCard,
  options: {
    kind: PublishKind;
    assignedConsumer?: string;
    excludeConsumer?: string;
  }
) {
  const isReview = options.kind === "review";
  const assignedConsumer = options.assignedConsumer || card.assignedConsumer || undefined;

  return {
    task_id: isReview ? undefined : card.taskId,
    model: card.model,
    assigned_consumer: assignedConsumer,
    messages: [
      {
        role: "user",
        content: isReview ? reviewPrompt(card) : `${card.title}\n\n${card.prompt}`
      }
    ],
    metadata: {
      card_id: card.id,
      task_kind: options.kind,
      title: card.title,
      column: card.column,
      priority: card.priority,
      assignee: card.assignee,
      assigned_consumer: assignedConsumer || null,
      original_consumer: options.excludeConsumer || card.completedBy || null,
      requires_review: card.requiresReview,
      review_of_task_id: isReview ? card.taskId ?? null : null,
      depends_on_task_id: card.dependsOnTaskId || null,
      auto_publish_on_dependency: card.autoPublishOnDependency,
      due_date: card.dueDate,
      tags: card.tags
    }
  };
}

function reviewPrompt(card: KanbanCard) {
  const result = card.resultMessage?.trim() || "No result was captured.";
  return [
    "Review the completed task below.",
    "Check accuracy, completeness, missing risks, and whether the answer follows the prompt. Return a concise review with any required corrections.",
    `Task title:\n${card.title}`,
    `Original prompt:\n${card.prompt}`,
    `Original result:\n${result}`
  ].join("\n\n");
}

function loadCards() {
  const stored = localStorage.getItem(storageKey);
  if (!stored) {
    return initialCards;
  }

  try {
    const parsed = JSON.parse(stored) as KanbanCard[];
    return Array.isArray(parsed)
      ? parsed.map((card) => ({
          ...card,
          taskId: card.taskId ?? crypto.randomUUID(),
          assignedConsumer: card.assignedConsumer ?? "",
          requiresReview: Boolean(card.requiresReview),
          dependsOnTaskId: card.dependsOnTaskId ?? "",
          autoPublishOnDependency: Boolean(card.autoPublishOnDependency)
        }))
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
    requiresReview: false,
    dependsOnTaskId: "",
    autoPublishOnDependency: false,
    dueDate: todayPlus(2),
    tagsText: ""
  };
}

function queueItemKey(item: PublishQueueItem) {
  return `${item.kind}:${item.cardId}`;
}

function isDoneCard(card: KanbanCard) {
  return card.column === "done" && card.publishStatus === "done";
}

function dependencyLabel(cards: KanbanCard[], dependsOnTaskId: string) {
  if (!dependsOnTaskId) {
    return "None";
  }

  const dependency = cards.find((card) => card.taskId === dependsOnTaskId);
  return dependency ? dependency.title : "Missing task";
}

function releaseDependentCards(cards: KanbanCard[], completedTaskId: string) {
  return cards.map((card) =>
    card.column === "backlog" && card.dependsOnTaskId === completedTaskId
      ? {
          ...card,
          column: "ready" as ColumnId,
          error: undefined,
          updatedAt: Date.now()
        }
      : card
  );
}

function taskUpdateKind(
  card: KanbanCard,
  update: PublisherTaskUpdate
): PublishKind | null {
  if (update.card_id && card.id === update.card_id && update.task_kind === "review") {
    return "review";
  }
  if (update.card_id && card.id === update.card_id && update.task_kind === "work") {
    return "work";
  }
  if (update.task_id && card.reviewTaskId === update.task_id) {
    return "review";
  }
  if (update.task_id && card.taskId === update.task_id) {
    return "work";
  }
  if (update.card_id && card.id === update.card_id) {
    return "work";
  }
  return null;
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

function columnForReviewUpdateStatus(status: PublishStatus): ColumnId {
  if (status === "done") {
    return "done";
  }
  return "review";
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
  const date = new Date(Date.now() + days * 24 * 60 * 60 * 1000);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: singaporeTimeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(date);
  const value = (type: string) => parts.find((part) => part.type === type)?.value ?? "";
  return `${value("year")}-${value("month")}-${value("day")}`;
}

function formatTime(value: number) {
  return new Intl.DateTimeFormat("en-SG", {
    timeZone: singaporeTimeZone,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short"
  }).format(value);
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

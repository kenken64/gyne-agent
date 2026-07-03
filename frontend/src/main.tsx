import React from "react";
import { createRoot } from "react-dom/client";
import {
  Archive,
  BookmarkPlus,
  Check,
  Circle,
  ClipboardList,
  Copy,
  ListChecks,
  Loader2,
  MessageSquareText,
  Plus,
  RefreshCw,
  RotateCcw,
  Send,
  ThumbsDown,
  ThumbsUp,
  Trash2,
  Users,
  Wifi,
  WifiOff,
  X
} from "lucide-react";
import {
  completionText,
  composeWorkPrompt,
  parseDoneWhen,
  parseSelfReview,
  stripVerdictBlock,
  type AttemptRecord,
  type SelfReviewVerdict
} from "./verdict";
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

/** A reusable acceptance criterion (eval) auto-attached to this card at publish time. */
interface AttachedEval {
  id: string;
  criterion: string;
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
  spec: string;
  doneWhen: string[];
  attachedEvals: AttachedEval[];
  attempt: number;
  attemptHistory: AttemptRecord[];
  verdict?: SelfReviewVerdict;
  awaitingApproval?: boolean;
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
  spec: string;
  doneWhenText: string;
}

interface PublishQueueItem {
  cardId: string;
  kind: PublishKind;
  excludeConsumer?: string;
}

/** A 2ndBrain project this account owns; each project gets its own kanban board. */
interface ProjectRef {
  id: string;
  name: string;
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
  /** Raw LLM completion JSON relayed by the publisher; the self-review verdict is parsed from it. */
  response?: unknown;
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
const launchToken = readLaunchToken();
const defaultWsUrl = launchPublisherWsUrl();
const authCheckIntervalMs = 15000;
const storageKey = "gyne-agent-kanban";
const activeProjectStorageKey = `${storageKey}:active-project`;
const projectsCacheKey = `${storageKey}:projects`;
const singaporeTimeZone = "Asia/Singapore";
const singaporeTimeZoneLabel = "SGT";

if (launchToken) {
  stripLaunchTokenFromLocation();
}

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
    spec: "",
    doneWhen: [
      "Every intake note is represented in the handoff",
      "Risks and missing data are called out explicitly"
    ],
    attachedEvals: [],
    attempt: 1,
    attemptHistory: [],
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
    spec: "",
    doneWhen: [],
    attachedEvals: [],
    attempt: 1,
    attemptHistory: [],
    publishStatus: "draft",
    createdAt: Date.now(),
    updatedAt: Date.now()
  }
];

/** Manual done-when criteria plus criteria from evals attached at publish time, deduped. */
function effectiveDoneWhen(card: KanbanCard): string[] {
  const merged = [...card.doneWhen];
  for (const attached of card.attachedEvals) {
    if (!merged.includes(attached.criterion)) {
      merged.push(attached.criterion);
    }
  }
  return merged;
}

// The 2ndBrain bridge is best-effort: publishing and the local board never wait on it for
// longer than this, and every failure degrades to local-only behavior.
const BRAIN_BRIDGE_TIMEOUT_MS = 2000;

async function brainFetch(path: string, init?: RequestInit): Promise<Response> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), BRAIN_BRIDGE_TIMEOUT_MS);
  try {
    const response = await fetch(path, { ...init, cache: "no-store", signal: controller.signal });
    if (!response.ok) {
      throw new Error(`bridge responded ${response.status}`);
    }
    return response;
  } finally {
    window.clearTimeout(timer);
  }
}

/** Fire-and-forget durable attempt upsert; the local attempt history is the source of truth
 * when the bridge is down, so failures are silently ignored. */
function persistAttempt(body: Record<string, unknown>) {
  void brainFetch("/api/brain/attempts", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body)
  }).catch(() => {});
}

/** Snapshot of one card attempt for the durable store, keyed by (user, task_id) server-side. */
function attemptSnapshot(card: KanbanCard, extra: Record<string, unknown>) {
  return {
    card_id: card.id,
    task_id: card.taskId,
    attempt: card.attempt,
    title: card.title,
    prompt: card.prompt,
    spec: card.spec,
    done_when: effectiveDoneWhen(card),
    ...extra
  };
}

/** Asks 2ndBrain which of the user's evals match this card (by tag overlap or keyword in
 * title+prompt). Returns null on any failure so the caller keeps the previous attachments. */
async function matchCardEvals(card: KanbanCard): Promise<AttachedEval[] | null> {
  try {
    const query = new URLSearchParams({
      tags: card.tags.join(","),
      q: `${card.title} ${card.prompt}`
    });
    const response = await brainFetch(`/api/brain/evals/match?${query.toString()}`);
    const payload = (await response.json()) as { evals?: unknown };
    if (!Array.isArray(payload.evals)) {
      return null;
    }
    return payload.evals
      .filter(
        (item): item is { id: string; criterion: string } =>
          Boolean(item) &&
          typeof (item as { id?: unknown }).id === "string" &&
          typeof (item as { criterion?: unknown }).criterion === "string" &&
          (item as { criterion: string }).criterion.trim() !== ""
      )
      .map((item) => ({ id: item.id, criterion: item.criterion.trim() }));
  } catch {
    return null;
  }
}

const EVAL_KEYWORD_STOPWORDS = new Set([
  "about",
  "after",
  "before",
  "create",
  "draft",
  "every",
  "from",
  "into",
  "make",
  "task",
  "that",
  "their",
  "them",
  "then",
  "there",
  "these",
  "this",
  "when",
  "with",
  "write",
  "your"
]);

/** Match keywords for a promoted eval, derived from the source card's title. */
function evalKeywordsFrom(title: string): string[] {
  const seen = new Set<string>();
  const keywords: string[] = [];
  for (const raw of title.toLowerCase().split(/[^a-z0-9]+/)) {
    if (raw.length < 4 || EVAL_KEYWORD_STOPWORDS.has(raw) || seen.has(raw)) {
      continue;
    }
    seen.add(raw);
    keywords.push(raw);
    if (keywords.length >= 6) {
      break;
    }
  }
  return keywords;
}

function readLaunchToken() {
  const params = new URLSearchParams(window.location.search);

  return (
    params.get("token")?.trim() ||
    params.get("launch_token")?.trim() ||
    params.get("2ndbrain_launch_token")?.trim() ||
    import.meta.env.VITE_2NDBRAIN_LAUNCH_TOKEN?.trim() ||
    ""
  );
}

function launchPublisherWsUrl() {
  const params = new URLSearchParams(window.location.search);

  return (
    params.get("publisher_ws_url")?.trim() ||
    params.get("ws_url")?.trim() ||
    import.meta.env.VITE_PUBLISHER_WS_URL?.trim() ||
    "ws://127.0.0.1:8080/ws"
  );
}

function stripLaunchTokenFromLocation() {
  const url = new URL(window.location.href);
  let changed = false;

  ["token", "launch_token", "2ndbrain_launch_token"].forEach((name) => {
    if (url.searchParams.has(name)) {
      url.searchParams.delete(name);
      changed = true;
    }
  });

  if (changed) {
    window.history.replaceState(
      window.history.state,
      "",
      `${url.pathname}${url.search}${url.hash}`
    );
  }
}

function websocketUrlWithLaunchToken(url: string) {
  if (!launchToken) {
    return url;
  }

  try {
    const parsed = new URL(url, window.location.href);

    parsed.searchParams.set("token", launchToken);
    return parsed.toString();
  } catch {
    const separator = url.includes("?") ? "&" : "?";

    return `${url}${separator}token=${encodeURIComponent(launchToken)}`;
  }
}

function LaunchExpired({ message }: { message: string }) {
  return (
    <main className="launch-expired">
      <section className="launch-expired-panel">
        <p>2ndBrain launch auth</p>
        <h1>Launch required</h1>
        <span>{message || "This Gyne Agent session is no longer active. Launch it again from 2ndBrain."}</span>
      </section>
    </main>
  );
}

function App() {
  const [projects, setProjects] = React.useState<ProjectRef[]>(loadCachedProjects);
  const [activeProjectId, setActiveProjectId] = React.useState<string>(loadActiveProjectId);
  const [cards, setCards] = React.useState<KanbanCard[]>(() => loadCards(loadActiveProjectId()));
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
  const [authExpired, setAuthExpired] = React.useState(false);
  const [authExpiredMessage, setAuthExpiredMessage] = React.useState("");
  const [rejectFeedback, setRejectFeedback] = React.useState("");
  const [isRejectOpen, setIsRejectOpen] = React.useState(false);
  const socketRef = React.useRef<WebSocket | null>(null);
  const pendingCardRef = React.useRef<string | null>(null);
  const pendingKindRef = React.useRef<PublishKind>("work");
  const pendingExcludeConsumerRef = React.useRef<string | undefined>(undefined);
  const pendingAssignedConsumerRef = React.useRef<string | undefined>(undefined);
  const queueRef = React.useRef<PublishQueueItem[]>([]);
  const cardsRef = React.useRef<KanbanCard[]>(cards);
  const consumersRef = React.useRef<ConsumerDiscovery[]>(consumers);
  const activeProjectIdRef = React.useRef<string>(activeProjectId);

  const selectedCard = cards.find((card) => card.id === selectedId) ?? null;
  const queuedCount = cards.filter((card) => card.publishStatus === "queued").length;
  const connectedConsumerCount = consumers.length;

  React.useEffect(() => {
    localStorage.setItem(storageKeyForProject(activeProjectId), JSON.stringify(cards));
    cardsRef.current = cards;
  }, [cards, activeProjectId]);

  React.useEffect(() => {
    activeProjectIdRef.current = activeProjectId;
    localStorage.setItem(activeProjectStorageKey, activeProjectId);
  }, [activeProjectId]);

  React.useEffect(() => {
    let cancelled = false;

    async function fetchProjects() {
      try {
        const response = await fetch("/api/brain/projects", { cache: "no-store" });
        if (!response.ok) {
          return;
        }

        const payload = (await response.json().catch(() => null)) as
          | { projects?: ProjectRef[] }
          | null;
        if (cancelled || !payload || !Array.isArray(payload.projects)) {
          return;
        }

        const list = payload.projects.filter(
          (project) =>
            Boolean(project) && typeof project.id === "string" && typeof project.name === "string"
        );
        setProjects(list);
        localStorage.setItem(projectsCacheKey, JSON.stringify(list));
      } catch {
        // The board still works with the cached project list (or just the default board).
      }
    }

    fetchProjects();
    return () => {
      cancelled = true;
    };
  }, []);

  React.useEffect(() => {
    consumersRef.current = consumers;
  }, [consumers]);

  React.useEffect(() => {
    setRejectFeedback("");
    setIsRejectOpen(false);
  }, [selectedId]);

  React.useEffect(() => {
    return () => {
      socketRef.current?.close();
    };
  }, []);

  React.useEffect(() => {
    let ignore = false;

    async function checkSession() {
      try {
        const response = await fetch("/api/session", { cache: "no-store" });

        if (response.status !== 401) {
          return;
        }

        let payload: { error?: string } | null = null;

        try {
          payload = await response.json();
        } catch {
          payload = null;
        }

        if (!ignore) {
          socketRef.current?.close();
          socketRef.current = null;
          setSocketStatus("closed");
          setConsumers([]);
          setLastEvent("Launch required");
          setAuthExpired(true);
          setAuthExpiredMessage(
            payload?.error ||
              "This Gyne Agent session is no longer active. Launch it again from 2ndBrain."
          );
        }
      } catch {
        // Local Vite development does not expose the production auth endpoint.
      }
    }

    checkSession();
    const timer = window.setInterval(checkSession, authCheckIntervalMs);

    return () => {
      ignore = true;
      window.clearInterval(timer);
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
    const socket = new WebSocket(websocketUrlWithLaunchToken(wsUrl));
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

  async function flushPublishQueue(socket = socketRef.current) {
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
    // Work publishes ask 2ndBrain for matching evals first (bounded by the bridge timeout);
    // matched criteria gate this attempt exactly like manual done-when entries. A bridge
    // failure keeps the card's previous attachments so a mid-loop retry doesn't lose its gate.
    let cardForPublish = card;
    if (nextItem.kind === "work") {
      const matched = await matchCardEvals(card);
      if (matched) {
        cardForPublish = { ...card, attachedEvals: matched };
        setCards((current) =>
          current.map((item) =>
            item.id === card.id
              ? { ...item, attachedEvals: matched, updatedAt: Date.now() }
              : item
          )
        );
      }
      if (socket.readyState !== WebSocket.OPEN) {
        clearPendingPublish();
        markCardFailed(nextItem.cardId, "Publisher connection closed while publishing");
        return;
      }
    }

    socket.send(
      JSON.stringify(
        toPublisherPayload(cardForPublish, {
          kind: nextItem.kind,
          assignedConsumer,
          excludeConsumer: nextItem.excludeConsumer,
          projectId: activeProjectIdRef.current || undefined
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
    const matchedGated = Boolean(matchedCard && effectiveDoneWhen(matchedCard).length > 0);
    const shouldRouteReview =
      matchedCard &&
      updateKind === "work" &&
      status === "done" &&
      matchedCard.requiresReview &&
      !matchedCard.reviewTaskId &&
      !matchedCard.reviewCompletedAt &&
      !isQueuedOrPending(matchedCard.id, "review");
    // Cards with acceptance criteria release their dependents on human Approve, not here.
    const completedDependencyTaskId =
      matchedCard &&
      !matchedGated &&
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

        const gated = effectiveDoneWhen(card).length > 0;

        if (kind === "review") {
          // With acceptance criteria the AI cross-review is evidence, not a gate
          // release: the card stays in Review awaiting the human decision.
          const gateHere = gated && status === "done";
          return {
            ...card,
            column: gateHere ? ("review" as ColumnId) : columnForReviewUpdateStatus(status),
            publishStatus: status,
            awaitingApproval: gateHere ? true : card.awaitingApproval,
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

        const fullText = completionText(update.response) ?? update.message ?? "";
        const parsedVerdict =
          gated && (status === "done" || status === "needs_input")
            ? parseSelfReview(fullText)
            : null;
        // A gated completion without a parseable verdict still stops at Review,
        // flagged so the reviewer knows the self-review is missing.
        const nextVerdict: SelfReviewVerdict | undefined =
          gated && status === "done"
            ? parsedVerdict ?? { criteria: [], overallPass: false, parseError: true }
            : parsedVerdict ?? card.verdict;
        const displayMessage =
          gated && fullText ? stripVerdictBlock(fullText) || update.message : update.message;
        const attemptRecord =
          gated && status === "done"
            ? {
                attempt: card.attempt,
                taskId: card.taskId,
                output: displayMessage ?? "",
                verdict: nextVerdict,
                consumer: update.consumer,
                completedAt: update.completed_at_ms ?? Date.now()
              }
            : null;

        return {
          ...card,
          column:
            routeReview || (gated && status === "done")
              ? ("review" as ColumnId)
              : columnForUpdateStatus(status, card.column),
          publishStatus: routeReview ? ("queued" as PublishStatus) : status,
          awaitingApproval:
            gated && status === "done" && !routeReview ? true : card.awaitingApproval,
          verdict: nextVerdict,
          attemptHistory: attemptRecord
            ? upsertAttempt(card.attemptHistory, attemptRecord)
            : card.attemptHistory,
          error: status === "failed" ? update.error || update.message : undefined,
          resultMessage: displayMessage,
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

    // Mirror the gated terminal attempt into the durable store (fire-and-forget; the
    // localStorage attempt history stays the source of truth when the bridge is down).
    if (matchedCard && updateKind === "work" && matchedGated && status === "done") {
      const fullText = completionText(update.response) ?? update.message ?? "";
      const parsedVerdict = parseSelfReview(fullText);
      const displayMessage =
        fullText ? stripVerdictBlock(fullText) || update.message : update.message;
      persistAttempt(
        attemptSnapshot(matchedCard, {
          output: displayMessage ?? "",
          verdict: parsedVerdict ?? { criteria: [], overallPass: false, parseError: true },
          consumer: update.consumer ?? null
        })
      );
    }

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

  // Each project keeps its own board; switching saves nothing extra (boards persist
  // on every change) and drops publish state, which is board-scoped.
  function switchProject(projectId: string) {
    if (projectId === activeProjectId) {
      return;
    }

    const nextCards = loadCards(projectId);
    queueRef.current = [];
    setPublishQueue([]);
    clearPendingPublish();
    setActiveProjectId(projectId);
    setCards(nextCards);
    setSelectedId(nextCards[0]?.id ?? null);
    setIsEditorOpen(false);
    setReworkMode(false);
    setLastEvent(
      projectId
        ? `Switched to ${projects.find((project) => project.id === projectId)?.name ?? "project"} board`
        : "Switched to default board"
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
      tagsText: card.tags.join(", "),
      spec: card.spec,
      doneWhenText: card.doneWhen.join("\n")
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
      tagsText: card.tags.join(", "),
      spec: card.spec,
      doneWhenText: card.doneWhen.join("\n")
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
    const doneWhen = parseDoneWhen(draft.doneWhenText);

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
                spec: draft.spec.trim(),
                doneWhen,
                // Rework re-runs as a brand new task: fresh id, clean draft status,
                // and the approval loop starts over from attempt 1.
                taskId: reworkMode ? crypto.randomUUID() : card.taskId,
                attempt: reworkMode ? 1 : card.attempt,
                attemptHistory: reworkMode ? [] : card.attemptHistory,
                attachedEvals: reworkMode ? [] : card.attachedEvals,
                verdict: undefined,
                awaitingApproval: false,
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
        spec: draft.spec.trim(),
        doneWhen,
        attachedEvals: [],
        attempt: 1,
        attemptHistory: [],
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
              verdict: undefined,
              awaitingApproval: false,
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

  // Human gate: confirm the goal is achieved. Only now does the card count as
  // Done for dependents — the agent's own "done" status never releases them.
  function approveCard(cardId: string) {
    const card = cardsRef.current.find((item) => item.id === cardId);
    if (!card || !card.awaitingApproval) {
      return;
    }

    const autoPublishIds = cardsRef.current
      .filter(
        (item) =>
          item.column === "backlog" &&
          item.dependsOnTaskId === card.taskId &&
          item.autoPublishOnDependency &&
          !isPublishing(item)
      )
      .map((item) => item.id);

    setCards((current) =>
      releaseDependentCards(
        current.map((item) =>
          item.id === cardId
            ? {
                ...item,
                column: "done" as ColumnId,
                publishStatus: "done" as PublishStatus,
                awaitingApproval: false,
                attemptHistory: finalizeAttempt(item, "approved"),
                updatedAt: Date.now()
              }
            : item
        ),
        card.taskId
      )
    );

    persistAttempt(
      attemptSnapshot(card, {
        output: card.resultMessage ?? "",
        verdict: card.verdict ?? null,
        consumer: card.completedBy ?? null,
        human_action: "approved"
      })
    );

    if (autoPublishIds.length > 0) {
      enqueueCards(autoPublishIds, "work");
      if (socketStatus === "closed") {
        connect();
      }
    }
    setLastEvent(`Approved ${card.title}`);
  }

  // Reject with feedback: the developer steers the agent. The next attempt runs
  // on the same consumer with the rejected output and this feedback in context.
  function rejectCard(cardId: string, feedback: string) {
    const card = cardsRef.current.find((item) => item.id === cardId);
    const trimmed = feedback.trim();
    if (!card || !card.awaitingApproval || !trimmed) {
      return;
    }

    setCards((current) =>
      current.map((item) =>
        item.id === cardId
          ? {
              ...item,
              attemptHistory: finalizeAttempt(item, "rejected", trimmed),
              attempt: item.attempt + 1,
              taskId: crypto.randomUUID(),
              assignedConsumer: item.completedBy || item.assignedConsumer,
              awaitingApproval: false,
              verdict: undefined,
              publishStatus: "draft" as PublishStatus,
              column: "in_progress" as ColumnId,
              streamId: undefined,
              error: undefined,
              resultMessage: undefined,
              resultStreamId: undefined,
              completedAt: undefined,
              questions: undefined,
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

    persistAttempt(
      attemptSnapshot(card, {
        output: card.resultMessage ?? "",
        verdict: card.verdict ?? null,
        consumer: card.completedBy ?? null,
        human_action: "rejected",
        human_feedback: trimmed
      })
    );

    enqueueCards([cardId], "work");
    if (socketStatus === "closed") {
      connect();
    }
    setLastEvent(`Rejected ${card.title} — running attempt ${card.attempt + 1}`);
  }

  // Promote a failed criterion or reviewer feedback into a reusable eval: 2ndBrain stores it
  // and it auto-attaches to future cards that share a tag or a title keyword.
  async function promoteToEval(card: KanbanCard, criterion: string, rationale?: string) {
    const trimmed = criterion.trim();
    if (!trimmed) {
      return;
    }

    try {
      await brainFetch("/api/brain/evals", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          criterion: trimmed,
          rationale: rationale?.trim() || null,
          match_tags: card.tags,
          match_keywords: evalKeywordsFrom(card.title),
          source_card_id: card.id,
          source_task_id: card.taskId
        })
      });
      setLastEvent(`Saved eval: ${trimmed.length > 60 ? `${trimmed.slice(0, 60)}…` : trimmed}`);
    } catch {
      setLastEvent("Could not save eval — the 2ndBrain bridge is unavailable");
    }
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

  if (authExpired) {
    return <LaunchExpired message={authExpiredMessage} />;
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
          <label className="project-switcher">
            <span>Project</span>
            <select
              value={activeProjectId}
              onChange={(event) => switchProject(event.target.value)}
            >
              <option value="">Default board</option>
              {activeProjectId &&
              !projects.some((project) => project.id === activeProjectId) ? (
                <option value={activeProjectId}>Unknown project</option>
              ) : null}
              {projects.map((project) => (
                <option key={project.id} value={project.id}>
                  {project.name}
                </option>
              ))}
            </select>
          </label>
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
              <Field
                label="Attempt"
                value={
                  effectiveDoneWhen(selectedCard).length > 0
                    ? String(selectedCard.attempt)
                    : "—"
                }
              />
              <Field
                label="Criteria"
                value={
                  effectiveDoneWhen(selectedCard).length > 0
                    ? `${selectedCard.doneWhen.length} defined${
                        selectedCard.attachedEvals.length > 0
                          ? ` · ${selectedCard.attachedEvals.length} eval${
                              selectedCard.attachedEvals.length === 1 ? "" : "s"
                            }`
                          : ""
                      }`
                    : "None"
                }
              />
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
            {selectedCard.attachedEvals.length > 0 ? (
              <div className="eval-chips">
                <div className="panel-title">
                  <BookmarkPlus size={18} />
                  <span>Attached evals</span>
                </div>
                <div className="eval-chip-list">
                  {selectedCard.attachedEvals.map((attached) => (
                    <span className="eval-chip" key={attached.id} title={attached.criterion}>
                      {attached.criterion}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
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
            {selectedCard.verdict ? (
              <div className="verdict-panel">
                <div className="panel-title">
                  <ListChecks size={18} />
                  <span>Self-review</span>
                  <span
                    className={`verdict-badge ${
                      selectedCard.verdict.parseError
                        ? "missing"
                        : selectedCard.verdict.overallPass
                          ? "pass"
                          : "fail"
                    }`}
                  >
                    {selectedCard.verdict.parseError
                      ? "No self-review found"
                      : selectedCard.verdict.overallPass
                        ? "All criteria pass"
                        : "Criteria failing"}
                  </span>
                </div>
                {selectedCard.verdict.criteria.map((criterion, index) => (
                  <div
                    className={`verdict-row ${criterion.pass ? "pass" : "fail"}`}
                    key={`${index}-${criterion.criterion}`}
                  >
                    {criterion.pass ? <Check size={15} /> : <X size={15} />}
                    <div>
                      <strong>{criterion.criterion}</strong>
                      {criterion.evidence ? <span>{criterion.evidence}</span> : null}
                    </div>
                    {!criterion.pass ? (
                      <button
                        type="button"
                        className="promote-eval-button"
                        title="Save as a reusable eval that auto-attaches to similar future tasks"
                        onClick={() =>
                          promoteToEval(selectedCard, criterion.criterion, criterion.evidence)
                        }
                      >
                        <BookmarkPlus size={14} />
                        Eval
                      </button>
                    ) : null}
                  </div>
                ))}
                {selectedCard.verdict.notes ? (
                  <p className="verdict-notes">{selectedCard.verdict.notes}</p>
                ) : null}
              </div>
            ) : null}
            {selectedCard.awaitingApproval ? (
              <div className="approval-panel">
                <div className="panel-title">
                  <ThumbsUp size={18} />
                  <span>Confirm goal achieved?</span>
                </div>
                <p>
                  Approve to move this task to Done and release dependents, or reject
                  with feedback to run attempt {selectedCard.attempt + 1} on the same
                  consumer.
                </p>
                {isRejectOpen ? (
                  <>
                    <textarea
                      className="reject-feedback"
                      placeholder="What is wrong or missing? This feedback is sent to the agent together with the rejected output."
                      value={rejectFeedback}
                      onChange={(event) => setRejectFeedback(event.target.value)}
                    />
                    <div className="approval-actions">
                      <button
                        className="secondary-button"
                        onClick={() => {
                          setIsRejectOpen(false);
                          setRejectFeedback("");
                        }}
                      >
                        <X size={16} />
                        Cancel
                      </button>
                      <button
                        className="secondary-button reject-confirm"
                        disabled={!rejectFeedback.trim()}
                        onClick={() => {
                          rejectCard(selectedCard.id, rejectFeedback);
                          setRejectFeedback("");
                          setIsRejectOpen(false);
                        }}
                      >
                        <ThumbsDown size={16} />
                        Reject &amp; rerun
                      </button>
                    </div>
                  </>
                ) : (
                  <div className="approval-actions">
                    <button
                      className="secondary-button"
                      onClick={() => setIsRejectOpen(true)}
                    >
                      <ThumbsDown size={16} />
                      Reject
                    </button>
                    <button
                      className="primary-button"
                      onClick={() => approveCard(selectedCard.id)}
                    >
                      <ThumbsUp size={16} />
                      Approve
                    </button>
                  </div>
                )}
              </div>
            ) : null}
            {selectedCard.attemptHistory.length > 0 ? (
              <details className="attempt-history">
                <summary>Attempts ({selectedCard.attemptHistory.length})</summary>
                <div className="attempt-list">
                  {[...selectedCard.attemptHistory].reverse().map((record) => (
                    <div className="attempt-row" key={record.attempt}>
                      <header>
                        <strong>Attempt {record.attempt}</strong>
                        <span className={`attempt-action ${record.humanAction ?? "pending"}`}>
                          {record.humanAction === "approved"
                            ? "Approved"
                            : record.humanAction === "rejected"
                              ? "Rejected"
                              : "Awaiting decision"}
                        </span>
                      </header>
                      {record.humanFeedback ? (
                        <p className="attempt-feedback">{record.humanFeedback}</p>
                      ) : null}
                      {record.humanAction === "rejected" && record.humanFeedback ? (
                        <button
                          type="button"
                          className="promote-eval-button"
                          title="Save this feedback as a reusable eval for similar future tasks"
                          onClick={() =>
                            promoteToEval(
                              selectedCard,
                              record.humanFeedback ?? "",
                              `Reviewer feedback on attempt ${record.attempt} of "${selectedCard.title}"`
                            )
                          }
                        >
                          <BookmarkPlus size={14} />
                          Promote to eval
                        </button>
                      ) : null}
                      {record.output ? (
                        <p className="attempt-output">{record.output}</p>
                      ) : null}
                      <span className="attempt-meta">
                        {record.consumer ? `${record.consumer} · ` : ""}
                        {formatTime(record.completedAt)}
                      </span>
                    </div>
                  ))}
                </div>
              </details>
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

            <label>
              <span>Spec (context &amp; constraints)</span>
              <textarea
                value={draft.spec}
                placeholder="Background, constraints, and product context the agent should honor."
                onChange={(event) =>
                  setDraft((current) => ({ ...current, spec: event.target.value }))
                }
              />
            </label>

            <label>
              <span>Done when (one acceptance criterion per line)</span>
              <textarea
                value={draft.doneWhenText}
                placeholder={"The summary covers every intake note\nRisks and missing data are called out explicitly"}
                onChange={(event) =>
                  setDraft((current) => ({ ...current, doneWhenText: event.target.value }))
                }
              />
            </label>

            {parseDoneWhen(draft.doneWhenText).length > 0 ? (
              <p className="editor-hint">
                With acceptance criteria set, the agent self-reviews its output and the
                task waits in Review for your approval before it counts as Done.
              </p>
            ) : null}

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
              <span>AI cross-review: route the completion to a second consumer for an independent check</span>
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
        {card.awaitingApproval ? (
          <span className="publish-status approval">
            <ThumbsUp size={14} />
            Approve?
          </span>
        ) : (
          <StatusBadge status={card.publishStatus} />
        )}
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
    projectId?: string;
  }
) {
  const isReview = options.kind === "review";
  const assignedConsumer = options.assignedConsumer || card.assignedConsumer || undefined;
  const doneWhen = effectiveDoneWhen(card);
  const gated = !isReview && doneWhen.length > 0;
  const lastRejected = gated ? lastRejectedAttempt(card) : null;

  return {
    task_id: isReview ? undefined : card.taskId,
    model: card.model,
    assigned_consumer: assignedConsumer,
    messages: [
      {
        role: "user",
        content: isReview
          ? reviewPrompt(card)
          : gated
            ? composeWorkPrompt({
                title: card.title,
                prompt: card.prompt,
                spec: card.spec,
                doneWhen,
                attempt: card.attempt,
                previousAttempt: lastRejected
              })
            : `${card.title}\n\n${card.prompt}`
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
      tags: card.tags,
      spec: card.spec || null,
      done_when: doneWhen,
      attached_eval_ids: card.attachedEvals.map((attached) => attached.id),
      attempt: card.attempt,
      parent_task_id: lastRejected?.taskId ?? null,
      feedback: lastRejected?.humanFeedback ?? null,
      project_id: options.projectId ?? null
    }
  };
}

function lastRejectedAttempt(card: KanbanCard): AttemptRecord | null {
  if (card.attempt <= 1) {
    return null;
  }

  for (let index = card.attemptHistory.length - 1; index >= 0; index -= 1) {
    if (card.attemptHistory[index].humanAction === "rejected") {
      return card.attemptHistory[index];
    }
  }

  return null;
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

function storageKeyForProject(projectId: string) {
  return projectId ? `${storageKey}:project:${projectId}` : storageKey;
}

function loadActiveProjectId() {
  return localStorage.getItem(activeProjectStorageKey) ?? "";
}

function loadCachedProjects(): ProjectRef[] {
  try {
    const parsed = JSON.parse(localStorage.getItem(projectsCacheKey) ?? "[]") as ProjectRef[];
    return Array.isArray(parsed)
      ? parsed.filter(
          (project) =>
            Boolean(project) && typeof project.id === "string" && typeof project.name === "string"
        )
      : [];
  } catch {
    return [];
  }
}

function loadCards(projectId: string) {
  const stored = localStorage.getItem(storageKeyForProject(projectId));
  if (!stored) {
    // Project boards start empty; the demo cards only seed the default board.
    return projectId ? [] : initialCards;
  }

  const fallback = projectId ? [] : initialCards;

  try {
    const parsed = JSON.parse(stored) as KanbanCard[];
    return Array.isArray(parsed)
      ? parsed.map((card) => ({
          ...card,
          taskId: card.taskId ?? crypto.randomUUID(),
          assignedConsumer: card.assignedConsumer ?? "",
          requiresReview: Boolean(card.requiresReview),
          dependsOnTaskId: card.dependsOnTaskId ?? "",
          autoPublishOnDependency: Boolean(card.autoPublishOnDependency),
          spec: typeof card.spec === "string" ? card.spec : "",
          doneWhen: Array.isArray(card.doneWhen) ? card.doneWhen : [],
          attachedEvals: Array.isArray(card.attachedEvals)
            ? card.attachedEvals.filter(
                (item) =>
                  Boolean(item) &&
                  typeof item.id === "string" &&
                  typeof item.criterion === "string"
              )
            : [],
          attempt: typeof card.attempt === "number" && card.attempt > 0 ? card.attempt : 1,
          attemptHistory: Array.isArray(card.attemptHistory) ? card.attemptHistory : []
        }))
      : fallback;
  } catch {
    return fallback;
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
    tagsText: "",
    spec: "",
    doneWhenText: ""
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

function upsertAttempt(history: AttemptRecord[], record: AttemptRecord) {
  const filtered = history.filter((item) => item.attempt !== record.attempt);
  return [...filtered, record].sort((left, right) => left.attempt - right.attempt);
}

/** Stamps the human decision onto the current attempt's record, creating one if the
 * work update never produced it (e.g. output arrived through a fallback path). */
function finalizeAttempt(
  card: KanbanCard,
  action: "approved" | "rejected",
  feedback?: string
): AttemptRecord[] {
  const exists = card.attemptHistory.some((record) => record.attempt === card.attempt);

  if (exists) {
    return card.attemptHistory.map((record) =>
      record.attempt === card.attempt
        ? { ...record, humanAction: action, humanFeedback: feedback }
        : record
    );
  }

  return [
    ...card.attemptHistory,
    {
      attempt: card.attempt,
      taskId: card.taskId,
      output: card.resultMessage ?? "",
      verdict: card.verdict,
      consumer: card.completedBy,
      completedAt: card.completedAt ?? Date.now(),
      humanAction: action,
      humanFeedback: feedback
    }
  ];
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
  // A result naming a task id that matches neither the card's current work task
  // nor its review task belongs to a superseded attempt (e.g. rejected and
  // respawned) and must not overwrite the live attempt via the card_id fallback.
  if (
    update.task_id &&
    update.task_id !== card.taskId &&
    update.task_id !== card.reviewTaskId
  ) {
    return null;
  }
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

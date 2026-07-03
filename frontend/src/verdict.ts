// Prompt contract + parser for the self-review loop. The verdict must ride INSIDE a fenced
// ```json block appended to a markdown deliverable: the publisher interprets a completion that
// parses as a bare JSON document (status/questions hijack), so a bare-JSON reply would never
// reach the board as "done".

export interface SelfReviewCriterion {
  criterion: string;
  pass: boolean;
  evidence: string;
}

export interface SelfReviewVerdict {
  criteria: SelfReviewCriterion[];
  overallPass: boolean;
  notes?: string;
  parseError?: boolean;
}

export interface AttemptRecord {
  attempt: number;
  taskId: string;
  output: string;
  verdict?: SelfReviewVerdict;
  humanAction?: "approved" | "rejected";
  humanFeedback?: string;
  consumer?: string;
  completedAt: number;
}

const VERDICT_KEY = "gyne_self_review";

const SELF_REVIEW_INSTRUCTIONS = [
  "## Self-review (required)",
  "",
  "Write your deliverable as normal markdown text. Then evaluate it honestly against every",
  "acceptance criterion above. End your reply with exactly one fenced code block tagged `json`",
  "in this exact shape:",
  "",
  "```json",
  `{"${VERDICT_KEY}": {"criteria": [{"criterion": "<criterion text>", "pass": true, "evidence": "<why it passes or fails>"}], "overall_pass": true, "notes": "<optional caveats>"}}`,
  "```",
  "",
  "Include one entry per acceptance criterion, in order. Set \"pass\" to false whenever you are",
  "not certain a criterion is met. Never format your whole reply as a bare JSON document — the",
  "JSON must appear only inside that single fenced block at the end."
].join("\n");

export function composeWorkPrompt(input: {
  title: string;
  prompt: string;
  spec: string;
  doneWhen: string[];
  attempt: number;
  previousAttempt?: AttemptRecord | null;
}): string {
  const sections = [`${input.title}\n\n${input.prompt}`];

  if (input.spec.trim()) {
    sections.push(`## Spec\n\n${input.spec.trim()}`);
  }

  sections.push(
    `## Acceptance criteria\n\n${input.doneWhen
      .map((criterion, index) => `${index + 1}. ${criterion}`)
      .join("\n")}`
  );

  const previous = input.previousAttempt;
  if (input.attempt > 1 && previous) {
    sections.push(
      [
        "## Previous attempt (rejected)",
        "",
        `This is attempt ${input.attempt}. Attempt ${previous.attempt} was rejected by the human reviewer.`,
        "",
        "Previous output:",
        "",
        previous.output || "(no output was captured)"
      ].join("\n")
    );

    const failed = previous.verdict?.criteria.filter((criterion) => !criterion.pass) ?? [];
    if (failed.length > 0) {
      sections.push(
        `Criteria the previous attempt failed:\n${failed
          .map((criterion) => `- ${criterion.criterion}`)
          .join("\n")}`
      );
    }

    if (previous.humanFeedback?.trim()) {
      sections.push(`## Reviewer feedback\n\n${previous.humanFeedback.trim()}`);
    }
  }

  sections.push(SELF_REVIEW_INSTRUCTIONS);

  return sections.join("\n\n");
}

/** One acceptance criterion per line; leading list markers/numbering are stripped. */
export function parseDoneWhen(text: string): string[] {
  return text
    .split(/\r?\n/)
    .map((line) => line.replace(/^\s*(?:[-*•]|\d+[.)])\s*/, "").trim())
    .filter(Boolean);
}

/** `choices[0].message.content` from a raw OpenAI-style completion, if present. */
export function completionText(response: unknown): string | null {
  if (!response || typeof response !== "object") {
    return null;
  }

  const choices = (response as { choices?: unknown }).choices;
  if (!Array.isArray(choices) || choices.length === 0) {
    return null;
  }

  const message = (choices[0] as { message?: { content?: unknown } })?.message;
  const content = message?.content;

  if (typeof content === "string") {
    return content;
  }

  // Some gateways return content as an array of {type: "text", text} parts.
  if (Array.isArray(content)) {
    const text = content
      .map((part) =>
        typeof part === "string" ? part : typeof (part as { text?: unknown })?.text === "string" ? (part as { text: string }).text : ""
      )
      .join("");
    return text || null;
  }

  return null;
}

const FENCE_RE = /```(?:json)?[^\S\n]*\n([\s\S]*?)```/gi;

export function parseSelfReview(text: string | null | undefined): SelfReviewVerdict | null {
  if (!text) {
    return null;
  }

  const candidates: string[] = [];
  for (const match of text.matchAll(FENCE_RE)) {
    if (match[1].includes(VERDICT_KEY)) {
      candidates.push(match[1]);
    }
  }

  // Fallback for replies without fences: scan for a balanced object around the key.
  const anchor = text.lastIndexOf(`"${VERDICT_KEY}"`);
  if (anchor >= 0) {
    const start = text.lastIndexOf("{", anchor);
    if (start >= 0) {
      const candidate = balancedJsonFrom(text, start);
      if (candidate) {
        candidates.push(candidate);
      }
    }
  }

  for (let index = candidates.length - 1; index >= 0; index -= 1) {
    const verdict = verdictFromJson(candidates[index]);
    if (verdict) {
      return verdict;
    }
  }

  return null;
}

/** The reply with its self-review block removed, for display. */
export function stripVerdictBlock(text: string): string {
  const matches = [...text.matchAll(FENCE_RE)].filter((match) => match[1].includes(VERDICT_KEY));
  const last = matches[matches.length - 1];

  if (last && typeof last.index === "number") {
    return (text.slice(0, last.index) + text.slice(last.index + last[0].length)).trim();
  }

  const anchor = text.lastIndexOf(`"${VERDICT_KEY}"`);
  if (anchor >= 0) {
    const start = text.lastIndexOf("{", anchor);
    if (start >= 0) {
      const candidate = balancedJsonFrom(text, start);
      if (candidate) {
        return (text.slice(0, start) + text.slice(start + candidate.length)).trim();
      }
    }
  }

  return text.trim();
}

function balancedJsonFrom(text: string, start: number): string | null {
  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let index = start; index < text.length; index += 1) {
    const char = text[index];

    if (escaped) {
      escaped = false;
      continue;
    }
    if (char === "\\") {
      escaped = inString;
      continue;
    }
    if (char === '"') {
      inString = !inString;
      continue;
    }
    if (inString) {
      continue;
    }
    if (char === "{") {
      depth += 1;
    } else if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return text.slice(start, index + 1);
      }
    }
  }

  return null;
}

function verdictFromJson(raw: string): SelfReviewVerdict | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.trim());
  } catch {
    return null;
  }

  if (!parsed || typeof parsed !== "object") {
    return null;
  }

  const body = (parsed as Record<string, unknown>)[VERDICT_KEY];
  if (!body || typeof body !== "object") {
    return null;
  }

  const rawCriteria = (body as { criteria?: unknown }).criteria;
  if (!Array.isArray(rawCriteria)) {
    return null;
  }

  const criteria: SelfReviewCriterion[] = rawCriteria
    .filter((entry): entry is Record<string, unknown> => Boolean(entry) && typeof entry === "object")
    .map((entry) => ({
      criterion: typeof entry.criterion === "string" ? entry.criterion : String(entry.criterion ?? ""),
      pass: entry.pass === true,
      evidence: typeof entry.evidence === "string" ? entry.evidence : ""
    }))
    .filter((entry) => entry.criterion.length > 0);

  if (criteria.length === 0) {
    return null;
  }

  const overallRaw = (body as { overall_pass?: unknown }).overall_pass;
  const overallPass =
    typeof overallRaw === "boolean" ? overallRaw : criteria.every((criterion) => criterion.pass);
  const notesRaw = (body as { notes?: unknown }).notes;

  return {
    criteria,
    overallPass,
    notes: typeof notesRaw === "string" && notesRaw.trim() ? notesRaw.trim() : undefined
  };
}

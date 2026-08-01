/**
 * antigravityStreamJson — decoder and event mapper for `agy --output-format stream-json`.
 *
 * Kept free of Effect services and process handling so the whole translation
 * from Antigravity's NDJSON to canonical runtime events can be exercised
 * against recorded transcripts. `AntigravityAdapter` owns the process, stamps
 * the drafts produced here with ids and timestamps, and publishes them.
 *
 * The wire format, as emitted by agy 1.1.x:
 *
 *   {"event":"init","conversation_id":"…","init":{"cwd":"…","tools":[…]}}
 *   {"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE",
 *     "step_type":"agent_response","text_delta":"hello"}}
 *   {"event":"result","result":{"conversation_id":"…","status":"SUCCESS",…}}
 *
 * `text_delta` is incremental: a step emits one payload while `ACTIVE` and the
 * remainder when it flips to `DONE`, so both are forwarded as content deltas.
 *
 * @module antigravityStreamJson
 */
import {
  type CanonicalItemType,
  type ProviderRuntimeEvent,
  RuntimeItemId,
  type ThreadTokenUsageSnapshot,
  type TurnId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const AntigravityUsage = Schema.Struct({
  input_tokens: Schema.optional(Schema.Number),
  output_tokens: Schema.optional(Schema.Number),
  thinking_tokens: Schema.optional(Schema.Number),
  cache_read_tokens: Schema.optional(Schema.Number),
  total_tokens: Schema.optional(Schema.Number),
});
export type AntigravityUsage = typeof AntigravityUsage.Type;

const AntigravityToolInfo = Schema.Struct({
  name: Schema.optional(Schema.String),
  parameters: Schema.optional(Schema.Unknown),
  output: Schema.optional(Schema.String),
});

const AntigravitySubagent = Schema.Struct({
  type_name: Schema.optional(Schema.String),
  role: Schema.optional(Schema.String),
  initial_prompt: Schema.optional(Schema.String),
  conversation_id: Schema.optional(Schema.String),
  log_uri: Schema.optional(Schema.String),
});

const AntigravitySubagentInfo = Schema.Struct({
  subagents: Schema.Array(AntigravitySubagent),
});

const AntigravityStepUpdate = Schema.Struct({
  conversation_id: Schema.optional(Schema.String),
  step_index: Schema.Number,
  state: Schema.String,
  step_type: Schema.String,
  text_delta: Schema.optional(Schema.String),
  tool_name: Schema.optional(Schema.String),
  tool_info: Schema.optional(AntigravityToolInfo),
  subagent_info: Schema.optional(AntigravitySubagentInfo),
  usage: Schema.optional(AntigravityUsage),
});
export type AntigravityStepUpdate = typeof AntigravityStepUpdate.Type;

const AntigravityInitEvent = Schema.Struct({
  event: Schema.Literal("init"),
  conversation_id: Schema.optional(Schema.String),
  init: Schema.optional(
    Schema.Struct({
      cwd: Schema.optional(Schema.String),
      permission_mode: Schema.optional(Schema.String),
    }),
  ),
});

const AntigravityStepUpdateEvent = Schema.Struct({
  event: Schema.Literal("step_update"),
  step_update: AntigravityStepUpdate,
});

const AntigravityResultEvent = Schema.Struct({
  event: Schema.Literal("result"),
  result: Schema.Struct({
    conversation_id: Schema.optional(Schema.String),
    status: Schema.optional(Schema.String),
    response: Schema.optional(Schema.String),
    /**
     * Populated when `status` is an error. This is the only place agy explains
     * itself — quota exhaustion, auth failures and the like are reported here
     * and never on stderr, so it is the primary source for the turn's error.
     */
    error: Schema.optional(Schema.String),
    usage: Schema.optional(AntigravityUsage),
  }),
});

const AntigravityStreamEvent = Schema.Union([
  AntigravityInitEvent,
  AntigravityStepUpdateEvent,
  AntigravityResultEvent,
]);
export type AntigravityStreamEvent = typeof AntigravityStreamEvent.Type;

const isAntigravityStreamEvent = Schema.is(AntigravityStreamEvent);

/**
 * Parse one NDJSON line. Returns `undefined` for blank lines, non-JSON noise,
 * and any event shape this build does not know about — agy adds step types and
 * event kinds between releases, and an unrecognized line must never fail a turn.
 */
export function decodeAntigravityLine(line: string): AntigravityStreamEvent | undefined {
  const trimmed = line.trim();
  if (trimmed.length === 0 || !trimmed.startsWith("{")) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return undefined;
  }
  return isAntigravityStreamEvent(parsed) ? parsed : undefined;
}

/**
 * Runtime events without the fields the adapter owns. Distributes over the
 * `ProviderRuntimeEvent` union so each draft keeps its `type`/`payload`
 * correlation.
 */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;
export type AntigravityRuntimeEventDraft = DistributiveOmit<
  ProviderRuntimeEvent,
  "eventId" | "provider" | "providerInstanceId" | "threadId" | "createdAt" | "turnId"
>;

/**
 * Per-turn mapping state. One instance lives for the duration of a single
 * `agy` invocation; `totalProcessedTokens` is seeded from the session so the
 * cumulative counter survives across turns.
 */
export interface AntigravityTurnState {
  readonly turnId: TurnId;
  readonly openItems: Map<number, CanonicalItemType>;
  conversationId: string | undefined;
  totalProcessedTokens: number;
}

export function makeAntigravityTurnState(input: {
  readonly turnId: TurnId;
  readonly conversationId: string | undefined;
  readonly totalProcessedTokens: number;
}): AntigravityTurnState {
  return {
    turnId: input.turnId,
    openItems: new Map(),
    conversationId: input.conversationId,
    totalProcessedTokens: input.totalProcessedTokens,
  };
}

/**
 * agy reports `conversation_id: ""` on a failed run rather than omitting it.
 * Taking that at face value would overwrite a live conversation id with a
 * blank, dropping the resume cursor and silently starting a fresh
 * conversation — with no history — on the thread's next turn.
 */
function normalizeConversationId(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Item ids are derived from the turn and agy's own step index rather than
 * generated, so the same step always maps to the same item across a replay.
 */
function stepItemId(state: AntigravityTurnState, stepIndex: number): RuntimeItemId {
  return RuntimeItemId.make(`${state.turnId}:step:${stepIndex}`);
}

const COMMAND_TOOLS = new Set(["run_command", "command_status", "send_command_input"]);
const FILE_CHANGE_TOOLS = new Set([
  "write_to_file",
  "replace_file_content",
  "multi_replace_file_content",
  "sed_file",
  "notebook_edit",
]);
const WEB_SEARCH_TOOLS = new Set(["search_web", "read_url_content"]);
const SUBAGENT_TOOLS = new Set(["invoke_subagent", "browser_subagent"]);

/**
 * Map an agy tool name onto the canonical item type that drives how the UI
 * renders it. Anything unrecognized renders as a generic tool call rather than
 * being dropped.
 */
export function antigravityToolItemType(toolName: string | undefined): CanonicalItemType {
  if (toolName === undefined) return "dynamic_tool_call";
  if (COMMAND_TOOLS.has(toolName)) return "command_execution";
  if (FILE_CHANGE_TOOLS.has(toolName)) return "file_change";
  if (WEB_SEARCH_TOOLS.has(toolName)) return "web_search";
  if (SUBAGENT_TOOLS.has(toolName)) return "collab_agent_tool_call";
  if (toolName === "call_mcp_tool") return "mcp_tool_call";
  return "dynamic_tool_call";
}

const PARAMETER_DETAIL_KEYS = [
  "CommandLine",
  "AbsolutePath",
  "TargetFile",
  "Query",
  "SearchDirectory",
  "Url",
];

/**
 * Pick the one parameter worth showing next to the tool name. Falls back to a
 * compact JSON rendering so an unfamiliar tool still says something useful.
 */
export function antigravityToolDetail(parameters: unknown): string | undefined {
  if (parameters === null || typeof parameters !== "object") return undefined;
  const record = parameters as Record<string, unknown>;
  for (const key of PARAMETER_DETAIL_KEYS) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
  }
  const keys = Object.keys(record);
  if (keys.length === 0) return undefined;
  try {
    return JSON.stringify(record);
  } catch {
    return undefined;
  }
}

const toNonNegativeInt = (value: number | undefined): number | undefined =>
  typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.floor(value) : undefined;

/**
 * Translate one agy usage block into a context-window snapshot. `usedTokens`
 * is the tokens the most recent model call carried, matching how the other
 * adapters report occupancy; `totalProcessedTokens` is the running sum.
 */
function toTokenUsageSnapshot(
  usage: AntigravityUsage,
  totalProcessedTokens: number,
): ThreadTokenUsageSnapshot | undefined {
  const usedTokens = toNonNegativeInt(usage.total_tokens);
  if (usedTokens === undefined || usedTokens <= 0) return undefined;

  const inputTokens = toNonNegativeInt(usage.input_tokens);
  const cachedInputTokens = toNonNegativeInt(usage.cache_read_tokens);
  const outputTokens = toNonNegativeInt(usage.output_tokens);
  const reasoningOutputTokens = toNonNegativeInt(usage.thinking_tokens);

  return {
    usedTokens,
    ...(totalProcessedTokens > usedTokens ? { totalProcessedTokens } : {}),
    ...(inputTokens !== undefined ? { inputTokens } : {}),
    ...(cachedInputTokens !== undefined ? { cachedInputTokens } : {}),
    ...(outputTokens !== undefined ? { outputTokens } : {}),
    ...(reasoningOutputTokens !== undefined ? { reasoningOutputTokens } : {}),
    lastUsedTokens: usedTokens,
    ...(inputTokens !== undefined ? { lastInputTokens: inputTokens } : {}),
    ...(cachedInputTokens !== undefined ? { lastCachedInputTokens: cachedInputTokens } : {}),
    ...(outputTokens !== undefined ? { lastOutputTokens: outputTokens } : {}),
    ...(reasoningOutputTokens !== undefined
      ? { lastReasoningOutputTokens: reasoningOutputTokens }
      : {}),
    compactsAutomatically: true,
  };
}

function mapUsage(
  state: AntigravityTurnState,
  usage: AntigravityUsage | undefined,
): ReadonlyArray<AntigravityRuntimeEventDraft> {
  if (usage === undefined) return [];
  const total = toNonNegativeInt(usage.total_tokens);
  if (total !== undefined) {
    state.totalProcessedTokens += total;
  }
  const snapshot = toTokenUsageSnapshot(usage, state.totalProcessedTokens);
  if (snapshot === undefined) return [];
  return [{ type: "thread.token-usage.updated", payload: { usage: snapshot } }];
}

function mapAgentResponseStep(
  state: AntigravityTurnState,
  step: AntigravityStepUpdate,
): Array<AntigravityRuntimeEventDraft> {
  const drafts: Array<AntigravityRuntimeEventDraft> = [];
  const itemId = stepItemId(state, step.step_index);
  const delta = step.text_delta ?? "";

  // Steps that produce no text are pure reasoning turns — agy reports their
  // token usage but no content, so opening an assistant item for them would
  // leave an empty bubble in the transcript.
  if (delta.length > 0) {
    if (!state.openItems.has(step.step_index)) {
      state.openItems.set(step.step_index, "assistant_message");
      drafts.push({
        type: "item.started",
        itemId,
        payload: { itemType: "assistant_message", status: "inProgress" },
      });
    }
    drafts.push({
      type: "content.delta",
      itemId,
      payload: { streamKind: "assistant_text", delta },
    });
  }

  if (step.state === "DONE" && state.openItems.has(step.step_index)) {
    state.openItems.delete(step.step_index);
    drafts.push({
      type: "item.completed",
      itemId,
      payload: { itemType: "assistant_message", status: "completed" },
    });
  }

  return drafts;
}

function mapToolStep(
  state: AntigravityTurnState,
  step: AntigravityStepUpdate,
): Array<AntigravityRuntimeEventDraft> {
  const drafts: Array<AntigravityRuntimeEventDraft> = [];
  const itemId = stepItemId(state, step.step_index);
  const toolName = step.tool_name ?? step.tool_info?.name;
  const itemType = state.openItems.get(step.step_index) ?? antigravityToolItemType(toolName);
  const title = toolName;
  const detail = antigravityToolDetail(step.tool_info?.parameters);

  if (!state.openItems.has(step.step_index)) {
    state.openItems.set(step.step_index, itemType);
    drafts.push({
      type: "item.started",
      itemId,
      payload: {
        itemType,
        status: "inProgress",
        ...(title ? { title } : {}),
        ...(detail ? { detail } : {}),
      },
    });
  }

  if (step.state === "DONE") {
    state.openItems.delete(step.step_index);
    const output = step.tool_info?.output?.trim();
    drafts.push({
      type: "item.completed",
      itemId,
      payload: {
        itemType,
        status: "completed",
        ...(title ? { title } : {}),
        ...(output ? { detail: output } : detail ? { detail } : {}),
      },
    });
  }

  return drafts;
}

function mapSubagentStep(
  state: AntigravityTurnState,
  step: AntigravityStepUpdate,
): Array<AntigravityRuntimeEventDraft> {
  const drafts: Array<AntigravityRuntimeEventDraft> = [];
  const itemId = stepItemId(state, step.step_index);
  const itemType = "collab_agent_tool_call" as const;
  const subagent = step.subagent_info?.subagents[0];
  const role = subagent?.role?.trim();
  const prompt = subagent?.initial_prompt?.trim();
  const title = role ? `Subagent: ${role}` : "Subagent";

  if (!state.openItems.has(step.step_index)) {
    state.openItems.set(step.step_index, itemType);
    drafts.push({
      type: "item.started",
      itemId,
      payload: {
        itemType,
        status: "inProgress",
        title,
        ...(prompt ? { detail: prompt } : {}),
        data: { subagentInfo: step.subagent_info },
      },
    });
  }

  if (step.state === "DONE") {
    state.openItems.delete(step.step_index);
    drafts.push({
      type: "item.completed",
      itemId,
      payload: {
        itemType,
        status: "completed",
        title,
        ...(prompt ? { detail: prompt } : {}),
        data: { subagentInfo: step.subagent_info },
      },
    });
  }

  return drafts;
}

/**
 * Translate one decoded stream event into runtime event drafts, advancing
 * `state`. Returns an empty array for events that carry no transcript-visible
 * information (checkpoints, echoed user input, unknown step types).
 */
export function mapAntigravityStreamEvent(
  state: AntigravityTurnState,
  event: AntigravityStreamEvent,
): ReadonlyArray<AntigravityRuntimeEventDraft> {
  switch (event.event) {
    case "init": {
      const conversationId = normalizeConversationId(event.conversation_id);
      if (conversationId === undefined) return [];
      state.conversationId = conversationId;
      return [];
    }

    case "step_update": {
      const step = event.step_update;
      const stepConversationId = normalizeConversationId(step.conversation_id);
      if (stepConversationId !== undefined) {
        state.conversationId = stepConversationId;
      }
      const drafts: Array<AntigravityRuntimeEventDraft> = [];
      switch (step.step_type) {
        case "agent_response":
          drafts.push(...mapAgentResponseStep(state, step));
          break;
        case "tool":
          drafts.push(...mapToolStep(state, step));
          break;
        case "subagent":
          drafts.push(...mapSubagentStep(state, step));
          break;
        default:
          // checkpoint / user_input / system_message / anything agy adds
          // later: no transcript item, but usage below still counts.
          break;
      }
      drafts.push(...mapUsage(state, step.usage));
      return drafts;
    }

    case "result": {
      const resultConversationId = normalizeConversationId(event.result.conversation_id);
      if (resultConversationId !== undefined) {
        state.conversationId = resultConversationId;
      }
      // The result block repeats the run's cumulative usage, which the
      // per-step events already accounted for; re-adding it would double the
      // running total, so only the conversation id is taken from it.
      return [];
    }
  }
}

/**
 * Close out any item still marked in-progress. Called when a turn ends early
 * (interrupt, non-zero exit) so the transcript has no items stuck spinning.
 */
export function closeOpenAntigravityItems(
  state: AntigravityTurnState,
  status: "completed" | "failed",
): ReadonlyArray<AntigravityRuntimeEventDraft> {
  const drafts: Array<AntigravityRuntimeEventDraft> = [];
  for (const [stepIndex, itemType] of state.openItems) {
    drafts.push({
      type: "item.completed",
      itemId: stepItemId(state, stepIndex),
      payload: { itemType, status },
    });
  }
  state.openItems.clear();
  return drafts;
}

/** Map agy's terminal `result.status` onto the canonical turn state. */
export function antigravityTurnState(
  status: string | undefined,
): "completed" | "failed" | "cancelled" {
  switch (status?.toUpperCase()) {
    case "SUCCESS":
      return "completed";
    case "CANCELLED":
    case "CANCELED":
      return "cancelled";
    default:
      return "failed";
  }
}

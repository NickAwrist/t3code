import * as NodeAssert from "node:assert/strict";

import { describe, it } from "vite-plus/test";

import { TurnId } from "@t3tools/contracts";

import {
  antigravityToolItemType,
  antigravityTurnState,
  closeOpenAntigravityItems,
  decodeAntigravityLine,
  makeAntigravityTurnState,
  mapAntigravityStreamEvent,
  type AntigravityRuntimeEventDraft,
  type AntigravityTurnState,
} from "./antigravityStreamJson.ts";

const TURN_ID = TurnId.make("turn-1");

function makeState(overrides?: { conversationId?: string; totalProcessedTokens?: number }) {
  return makeAntigravityTurnState({
    turnId: TURN_ID,
    conversationId: overrides?.conversationId,
    totalProcessedTokens: overrides?.totalProcessedTokens ?? 0,
  });
}

/** Feed a raw NDJSON line through decode + map, as the adapter does. */
function feed(
  state: AntigravityTurnState,
  line: string,
): ReadonlyArray<AntigravityRuntimeEventDraft> {
  const decoded = decodeAntigravityLine(line);
  if (decoded === undefined) return [];
  return mapAntigravityStreamEvent(state, decoded);
}

function stepLine(step: Record<string, unknown>): string {
  return JSON.stringify({ event: "step_update", step_update: step });
}

describe("decodeAntigravityLine", () => {
  it("ignores blank lines, plain text, and malformed JSON", () => {
    NodeAssert.equal(decodeAntigravityLine(""), undefined);
    NodeAssert.equal(decodeAntigravityLine("   "), undefined);
    NodeAssert.equal(decodeAntigravityLine("Checking for updates..."), undefined);
    NodeAssert.equal(decodeAntigravityLine('{"event":"init"'), undefined);
  });

  // agy adds event kinds between releases; an unrecognized one must be skipped
  // rather than fail the turn that produced it.
  it("ignores JSON objects that are not a known event", () => {
    NodeAssert.equal(decodeAntigravityLine('{"event":"telemetry","payload":{}}'), undefined);
    NodeAssert.equal(decodeAntigravityLine('{"unrelated":true}'), undefined);
  });

  it("decodes the three known event kinds", () => {
    NodeAssert.equal(
      decodeAntigravityLine('{"event":"init","conversation_id":"c1"}')?.event,
      "init",
    );
    NodeAssert.equal(
      decodeAntigravityLine(stepLine({ step_index: 0, state: "DONE", step_type: "user_input" }))
        ?.event,
      "step_update",
    );
    NodeAssert.equal(
      decodeAntigravityLine('{"event":"result","result":{"status":"SUCCESS"}}')?.event,
      "result",
    );
  });
});

describe("mapAntigravityStreamEvent", () => {
  it("records the conversation id from init for adapter-owned announcement and resume", () => {
    const state = makeState();
    const drafts = feed(
      state,
      '{"event":"init","conversation_id":"conv-42","init":{"cwd":"/tmp"}}',
    );

    NodeAssert.equal(state.conversationId, "conv-42");
    NodeAssert.deepEqual(drafts, []);
  });

  it("streams assistant text as deltas bracketed by item lifecycle events", () => {
    const state = makeState();

    const first = feed(
      state,
      stepLine({
        step_index: 2,
        state: "ACTIVE",
        step_type: "agent_response",
        text_delta: "hello",
      }),
    );
    NodeAssert.deepEqual(
      first.map((draft) => draft.type),
      ["item.started", "content.delta"],
    );
    NodeAssert.deepEqual(first[1]!.payload, { streamKind: "assistant_text", delta: "hello" });

    // agy splits a message across ACTIVE and DONE; the DONE payload is the
    // remainder, not a repeat, so it must be forwarded as another delta.
    const second = feed(
      state,
      stepLine({ step_index: 2, state: "DONE", step_type: "agent_response", text_delta: " world" }),
    );
    NodeAssert.deepEqual(
      second.map((draft) => draft.type),
      ["content.delta", "item.completed"],
    );
    NodeAssert.deepEqual(second[0]!.payload, { streamKind: "assistant_text", delta: " world" });

    // Both halves belong to one item, so the UI renders a single message.
    NodeAssert.equal(first[1]!.itemId, second[0]!.itemId);
  });

  it("opens no assistant item for a text-free reasoning step", () => {
    const state = makeState();
    const drafts = feed(
      state,
      stepLine({
        step_index: 5,
        state: "DONE",
        step_type: "agent_response",
        usage: { total_tokens: 120, input_tokens: 100, output_tokens: 20, thinking_tokens: 18 },
      }),
    );

    NodeAssert.deepEqual(
      drafts.map((draft) => draft.type),
      ["thread.token-usage.updated"],
    );
  });

  it("maps a tool step to a command execution item carrying its command line", () => {
    const state = makeState();

    const started = feed(
      state,
      stepLine({
        step_index: 6,
        state: "ACTIVE",
        step_type: "tool",
        tool_name: "run_command",
        tool_info: { name: "run_command", parameters: { CommandLine: "ls -la" } },
      }),
    );
    NodeAssert.deepEqual(
      started.map((draft) => draft.type),
      ["item.started"],
    );
    NodeAssert.deepEqual(started[0]!.payload, {
      itemType: "command_execution",
      status: "inProgress",
      title: "run_command",
      detail: "ls -la",
    });

    const completed = feed(
      state,
      stepLine({
        step_index: 6,
        state: "DONE",
        step_type: "tool",
        tool_name: "run_command",
        tool_info: {
          name: "run_command",
          parameters: { CommandLine: "ls -la" },
          output: "total 8\n",
        },
      }),
    );
    NodeAssert.deepEqual(
      completed.map((draft) => draft.type),
      ["item.completed"],
    );
    NodeAssert.deepEqual(completed[0]!.payload, {
      itemType: "command_execution",
      status: "completed",
      title: "run_command",
      detail: "total 8",
    });
    NodeAssert.equal(started[0]!.itemId, completed[0]!.itemId);
  });

  it("emits a tool item even when the step is only ever seen as DONE", () => {
    const state = makeState();
    const drafts = feed(
      state,
      stepLine({
        step_index: 16,
        state: "DONE",
        step_type: "tool",
        tool_name: "view_file",
        tool_info: { name: "view_file", parameters: { AbsolutePath: "/tmp/sample.txt" } },
      }),
    );

    NodeAssert.deepEqual(
      drafts.map((draft) => draft.type),
      ["item.started", "item.completed"],
    );
  });

  it("maps recorded AGY subagent updates to a collaboration item", () => {
    const state = makeState();
    const active = feed(
      state,
      '{"event":"step_update","step_update":{"conversation_id":"da382c49-3474-44df-ba7c-5ea5271fc236","step_index":3,"state":"ACTIVE","step_type":"subagent","subagent_info":{"subagents":[{"type_name":"self","role":"Calculator","initial_prompt":"What is 1+1? Reply only with the numerical result.","conversation_id":"e0d1d9af-1240-48b8-b7f8-951aac7b9a80","log_uri":"file:///recorded/antigravity/transcript.jsonl"}]}}}',
    );
    const done = feed(
      state,
      '{"event":"step_update","step_update":{"conversation_id":"da382c49-3474-44df-ba7c-5ea5271fc236","step_index":3,"state":"DONE","step_type":"subagent","duration_seconds":0.079689493,"subagent_info":{"subagents":[{"type_name":"self","role":"Calculator","initial_prompt":"What is 1+1? Reply only with the numerical result.","conversation_id":"e0d1d9af-1240-48b8-b7f8-951aac7b9a80","log_uri":"file:///recorded/antigravity/transcript.jsonl"}]}}}',
    );

    NodeAssert.deepEqual(
      active.map((draft) => draft.type),
      ["item.started"],
    );
    NodeAssert.deepEqual(
      done.map((draft) => draft.type),
      ["item.completed"],
    );
    NodeAssert.deepEqual(active[0]!.payload, {
      itemType: "collab_agent_tool_call",
      status: "inProgress",
      title: "Subagent: Calculator",
      detail: "What is 1+1? Reply only with the numerical result.",
      data: {
        subagentInfo: {
          subagents: [
            {
              type_name: "self",
              role: "Calculator",
              initial_prompt: "What is 1+1? Reply only with the numerical result.",
              conversation_id: "e0d1d9af-1240-48b8-b7f8-951aac7b9a80",
              log_uri: "file:///recorded/antigravity/transcript.jsonl",
            },
          ],
        },
      },
    });
    NodeAssert.equal(active[0]!.itemId, done[0]!.itemId);
  });

  it("produces no transcript items for checkpoint and system steps", () => {
    const state = makeState();
    NodeAssert.deepEqual(
      feed(state, stepLine({ step_index: 3, state: "DONE", step_type: "checkpoint" })),
      [],
    );
    NodeAssert.deepEqual(
      feed(state, stepLine({ step_index: 12, state: "DONE", step_type: "system_message" })),
      [],
    );
    NodeAssert.deepEqual(
      feed(state, stepLine({ step_index: 0, state: "DONE", step_type: "user_input" })),
      [],
    );
  });

  it("reports the latest call as used tokens and accumulates the running total", () => {
    const state = makeState();

    feed(
      state,
      stepLine({
        step_index: 2,
        state: "DONE",
        step_type: "agent_response",
        usage: { total_tokens: 100, input_tokens: 80, output_tokens: 20 },
      }),
    );
    const second = feed(
      state,
      stepLine({
        step_index: 4,
        state: "DONE",
        step_type: "agent_response",
        usage: {
          total_tokens: 250,
          input_tokens: 200,
          output_tokens: 50,
          thinking_tokens: 30,
          cache_read_tokens: 900,
        },
      }),
    );

    const usageDraft = second.find((draft) => draft.type === "thread.token-usage.updated");
    NodeAssert.ok(usageDraft);
    NodeAssert.deepEqual(usageDraft.payload, {
      usage: {
        usedTokens: 250,
        totalProcessedTokens: 350,
        inputTokens: 200,
        cachedInputTokens: 900,
        outputTokens: 50,
        reasoningOutputTokens: 30,
        lastUsedTokens: 250,
        lastInputTokens: 200,
        lastCachedInputTokens: 900,
        lastOutputTokens: 50,
        lastReasoningOutputTokens: 30,
        compactsAutomatically: true,
      },
    });
  });

  // Verbatim from a real failed run: agy blanks the conversation id when a
  // turn errors. Honouring that would drop the resume cursor and silently
  // start a fresh, history-free conversation on the next turn.
  it("keeps the live conversation id when a failed result blanks it", () => {
    const state = makeState({ conversationId: "493763a3-305e-4268-8975-4a5e62e4fcb7" });

    feed(
      state,
      JSON.stringify({
        event: "result",
        result: {
          conversation_id: "",
          status: "ERROR",
          response: "",
          error:
            "Eligibility check failed: RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).",
        },
      }),
    );

    NodeAssert.equal(state.conversationId, "493763a3-305e-4268-8975-4a5e62e4fcb7");
  });

  it("ignores a blank conversation id on init and step updates", () => {
    const state = makeState({ conversationId: "conv-live" });

    NodeAssert.deepEqual(feed(state, '{"event":"init","conversation_id":"   "}'), []);
    NodeAssert.equal(state.conversationId, "conv-live");

    feed(
      state,
      stepLine({ conversation_id: "", step_index: 2, state: "DONE", step_type: "agent_response" }),
    );
    NodeAssert.equal(state.conversationId, "conv-live");
  });

  // The result block repeats the run's cumulative usage. Counting it again
  // would roughly double the reported context total.
  it("takes only the conversation id from the result event", () => {
    const state = makeState();
    feed(
      state,
      stepLine({
        step_index: 2,
        state: "DONE",
        step_type: "agent_response",
        usage: { total_tokens: 100 },
      }),
    );

    const drafts = feed(
      state,
      JSON.stringify({
        event: "result",
        result: { conversation_id: "conv-9", status: "SUCCESS", usage: { total_tokens: 100 } },
      }),
    );

    NodeAssert.deepEqual(drafts, []);
    NodeAssert.equal(state.conversationId, "conv-9");
    NodeAssert.equal(state.totalProcessedTokens, 100);
  });
});

describe("closeOpenAntigravityItems", () => {
  it("closes items left in progress when a turn ends early", () => {
    const state = makeState();
    feed(
      state,
      stepLine({
        step_index: 2,
        state: "ACTIVE",
        step_type: "agent_response",
        text_delta: "partial",
      }),
    );
    feed(
      state,
      stepLine({ step_index: 3, state: "ACTIVE", step_type: "tool", tool_name: "run_command" }),
    );

    const drafts = closeOpenAntigravityItems(state, "failed");

    NodeAssert.deepEqual(
      drafts.map((draft) => draft.payload),
      [
        { itemType: "assistant_message", status: "failed" },
        { itemType: "command_execution", status: "failed" },
      ],
    );
    NodeAssert.deepEqual(closeOpenAntigravityItems(state, "failed"), []);
  });
});

describe("antigravityToolItemType", () => {
  it("routes known tools to their canonical item type and unknown ones to a generic call", () => {
    NodeAssert.equal(antigravityToolItemType("run_command"), "command_execution");
    NodeAssert.equal(antigravityToolItemType("replace_file_content"), "file_change");
    NodeAssert.equal(antigravityToolItemType("search_web"), "web_search");
    NodeAssert.equal(antigravityToolItemType("call_mcp_tool"), "mcp_tool_call");
    NodeAssert.equal(antigravityToolItemType("invoke_subagent"), "collab_agent_tool_call");
    NodeAssert.equal(antigravityToolItemType("some_future_tool"), "dynamic_tool_call");
    NodeAssert.equal(antigravityToolItemType(undefined), "dynamic_tool_call");
  });
});

describe("antigravityTurnState", () => {
  it("treats anything other than SUCCESS or a cancellation as a failure", () => {
    NodeAssert.equal(antigravityTurnState("SUCCESS"), "completed");
    NodeAssert.equal(antigravityTurnState("CANCELLED"), "cancelled");
    NodeAssert.equal(antigravityTurnState("ERROR"), "failed");
    NodeAssert.equal(antigravityTurnState(undefined), "failed");
  });
});

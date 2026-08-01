// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe } from "vite-plus/test";

import {
  AntigravitySettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import {
  buildAntigravityTurnArgs,
  makeAntigravityAdapter,
  parseAntigravityResume,
} from "./AntigravityAdapter.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);
const PROVIDER = ProviderDriverKind.make("antigravity");
const INSTANCE_ID = ProviderInstanceId.make("antigravity");

const TRANSCRIPT = [
  '{"event":"init","conversation_id":"conv-abc","init":{"cwd":"/tmp","permission_mode":"always-proceed"}}',
  '{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":0,"state":"DONE","step_type":"user_input"}}',
  '{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"hello "}}',
  '{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":2,"state":"DONE","step_type":"agent_response","text_delta":"world","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}',
  '{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":3,"state":"ACTIVE","step_type":"tool","tool_name":"run_command","tool_info":{"name":"run_command","parameters":{"CommandLine":"echo hi"}}}}',
  '{"event":"step_update","step_update":{"conversation_id":"conv-abc","step_index":3,"state":"DONE","step_type":"tool","tool_name":"run_command","tool_info":{"name":"run_command","parameters":{"CommandLine":"echo hi"},"output":"hi"}}}',
  '{"event":"result","result":{"conversation_id":"conv-abc","status":"SUCCESS","response":"hello world"}}',
];

/** Verbatim from a real agy run that exhausted its quota. */
const QUOTA_ERROR =
  "Eligibility check failed: RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).";
const JSON_QUOTED_QUOTA_ERROR = `"${QUOTA_ERROR}"`;

/**
 * Write a stand-in for the `agy` binary. It records the argv it was handed so
 * tests can assert on flag construction, then replays a canned transcript.
 */
async function makeFakeAgy(options?: {
  readonly stdout?: ReadonlyArray<string>;
  readonly stderr?: string;
  readonly exitCode?: number;
}) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "antigravity-adapter-"));
  const binaryPath = NodePath.join(dir, "fake-agy.sh");
  const argsPath = NodePath.join(dir, "args.txt");
  const lines = options?.stdout ?? TRANSCRIPT;
  const script = [
    "#!/bin/sh",
    `printf '%s\\n' "$@" > ${JSON.stringify(argsPath)}`,
    ...lines.map((line) => `printf '%s\\n' ${JSON.stringify(line)}`),
    ...(options?.stderr ? [`printf '%s\\n' ${JSON.stringify(options.stderr)} >&2`] : []),
    `exit ${options?.exitCode ?? 0}`,
  ].join("\n");
  await NodeFSP.writeFile(binaryPath, `${script}\n`, "utf8");
  await NodeFSP.chmod(binaryPath, 0o755);
  return { binaryPath, argsPath };
}

async function readArgs(argsPath: string): Promise<ReadonlyArray<string>> {
  const raw = await NodeFSP.readFile(argsPath, "utf8");
  return raw.split("\n").slice(0, -1);
}

/** Assert `flag` is present in `args` and immediately followed by `value`. */
function assertFlag(args: ReadonlyArray<string>, flag: string, value: string) {
  const index = args.indexOf(flag);
  assert.isAtLeast(index, 0, `expected ${flag} in ${args.join(" ")}`);
  assert.equal(args[index + 1], value);
}

const makeTestAdapter = (binaryPath: string, launchArgs?: string) =>
  makeAntigravityAdapter(
    decodeAntigravitySettings({ binaryPath, ...(launchArgs ? { launchArgs } : {}) }),
    {
      instanceId: INSTANCE_ID,
    },
  );

/** Collect every runtime event the adapter publishes while `body` runs. */
const withRecordedEvents = <A, E, R>(
  adapter: { readonly streamEvents: Stream.Stream<ProviderRuntimeEvent> },
  body: (events: ReadonlyArray<ProviderRuntimeEvent>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const events: ProviderRuntimeEvent[] = [];
    const fiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.sync(() => {
        events.push(event);
      }),
    ).pipe(Effect.forkChild);
    // Hand control to the recorder so it is subscribed to the pubsub before
    // the first event is published; otherwise session.started races the fork.
    yield* Effect.yieldNow;
    return yield* body(events).pipe(Effect.ensuring(Fiber.interrupt(fiber)));
  });

describe("buildAntigravityTurnArgs", () => {
  it("always pins the print timeout past agy's five-minute default", () => {
    const args = buildAntigravityTurnArgs({
      prompt: "hi",
      model: undefined,
      conversationId: undefined,
      planMode: false,
      launchArgs: undefined,
    });

    assert.deepEqual(args, [
      "-p",
      "hi",
      "--output-format",
      "stream-json",
      "--dangerously-skip-permissions",
      "--print-timeout",
      "24h",
    ]);
  });

  it("passes the model, conversation, plan mode, and extra launch arguments", () => {
    const args = buildAntigravityTurnArgs({
      prompt: "hi",
      model: "gemini-3.1-pro-high",
      conversationId: "conv-1",
      planMode: true,
      launchArgs: "--add-dir ../shared",
    });

    assertFlag(args, "--model", "gemini-3.1-pro-high");
    assertFlag(args, "--conversation", "conv-1");
    assertFlag(args, "--mode", "plan");
    assertFlag(args, "--add-dir", "../shared");
  });
});

describe("parseAntigravityResume", () => {
  it("accepts its own cursor and rejects anything else", () => {
    assert.deepEqual(parseAntigravityResume({ schemaVersion: 1, conversationId: "conv-1" }), {
      schemaVersion: 1,
      conversationId: "conv-1",
    });
    // A cursor written by another provider must not be replayed as a
    // conversation id, or agy would fail to resume an id it never issued.
    assert.isUndefined(parseAntigravityResume({ schemaVersion: 1, sessionId: "cursor-1" }));
    assert.isUndefined(parseAntigravityResume({ schemaVersion: 2, conversationId: "conv-1" }));
    assert.isUndefined(parseAntigravityResume(undefined));
    assert.isUndefined(parseAntigravityResume("conv-1"));
  });
});

it.layer(NodeServices.layer)("AntigravityAdapter", (it) => {
  it.effect("streams a turn's text and tools, then reports the conversation id", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-happy-path");
      const { binaryPath, argsPath } = yield* Effect.promise(() => makeFakeAgy());
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
            modelSelection: { instanceId: INSTANCE_ID, model: "gemini-3.1-pro-high" },
          });

          const result = yield* adapter.sendTurn({ threadId, input: "say hello" });

          // The conversation id agy issued is handed back so the thread can be
          // resumed after a server restart.
          assert.deepEqual(result.resumeCursor, {
            schemaVersion: 1,
            conversationId: "conv-abc",
          });

          const types = events.map((event) => event.type);
          assert.includeMembers(types, [
            "session.started",
            "session.state.changed",
            "thread.started",
            "turn.started",
            "item.started",
            "content.delta",
            "item.completed",
            "thread.token-usage.updated",
            "turn.completed",
          ]);

          const deltas = events.filter((event) => event.type === "content.delta");
          assert.deepEqual(
            deltas.map((event) => (event.type === "content.delta" ? event.payload.delta : "")),
            ["hello ", "world"],
          );

          const toolCompleted = events.find(
            (event) =>
              event.type === "item.completed" && event.payload.itemType === "command_execution",
          );
          assert.isDefined(toolCompleted);

          const completed = events.find((event) => event.type === "turn.completed");
          assert.isDefined(completed);
          if (completed?.type === "turn.completed") {
            assert.equal(completed.payload.state, "completed");
          }

          const args = yield* Effect.promise(() => readArgs(argsPath));
          assert.includeMembers(
            [...args],
            [
              "--output-format",
              "stream-json",
              "--dangerously-skip-permissions",
              "--print-timeout",
              "gemini-3.1-pro-high",
            ],
          );
          // First turn has no conversation to resume yet.
          assert.notInclude([...args], "--conversation");

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("resumes the same agy conversation on the next turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-resume");
      const { binaryPath, argsPath } = yield* Effect.promise(() => makeFakeAgy());
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* adapter.startSession({
        threadId,
        provider: PROVIDER,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "first" });
      yield* adapter.sendTurn({ threadId, input: "second" });

      const args = yield* Effect.promise(() => readArgs(argsPath));
      assertFlag(args, "--conversation", "conv-abc");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("resumes from a persisted cursor without waiting for a turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-cursor");
      const { binaryPath, argsPath } = yield* Effect.promise(() => makeFakeAgy());
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* adapter.startSession({
        threadId,
        provider: PROVIDER,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, conversationId: "conv-restored" },
      });
      yield* adapter.sendTurn({ threadId, input: "first" });

      const args = yield* Effect.promise(() => readArgs(argsPath));
      assertFlag(args, "--conversation", "conv-restored");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("fails the turn and surfaces stderr when the CLI exits non-zero", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-failure");
      const { binaryPath } = yield* Effect.promise(() =>
        makeFakeAgy({ stdout: [], stderr: "not signed in", exitCode: 3 }),
      );
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          yield* adapter.sendTurn({ threadId, input: "say hello" });

          const error = events.find((event) => event.type === "runtime.error");
          assert.isDefined(error);
          if (error?.type === "runtime.error") {
            assert.include(error.payload.message, "not signed in");
          }

          const completed = events.find((event) => event.type === "turn.completed");
          assert.isDefined(completed);
          if (completed?.type === "turn.completed") {
            assert.equal(completed.payload.state, "failed");
            assert.include(completed.payload.errorMessage ?? "", "not signed in");
          }

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  // Reproduces a real quota failure: agy explains itself in the result event
  // and writes nothing to stderr, so reporting the exit code told the user
  // nothing about what actually went wrong.
  it.effect("surfaces agy's own error message rather than the exit code", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-quota");
      const quotaError = QUOTA_ERROR;
      const { binaryPath } = yield* Effect.promise(() =>
        makeFakeAgy({
          stdout: [
            `{"event":"result","result":{"conversation_id":"","status":"ERROR","response":"","error":${JSON_QUOTED_QUOTA_ERROR}}}`,
          ],
          exitCode: 1,
        }),
      );
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
            resumeCursor: { schemaVersion: 1, conversationId: "conv-live" },
          });
          const result = yield* adapter.sendTurn({ threadId, input: "where is this located?" });

          const completed = events.find((event) => event.type === "turn.completed");
          assert.isDefined(completed);
          if (completed?.type === "turn.completed") {
            assert.equal(completed.payload.state, "failed");
            assert.equal(completed.payload.errorMessage, quotaError);
          }

          // The blanked conversation id must not cost the thread its history.
          assert.deepEqual(result.resumeCursor, {
            schemaVersion: 1,
            conversationId: "conv-live",
          });

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("closes items left open when the CLI dies mid-stream", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-truncated");
      const { binaryPath } = yield* Effect.promise(() =>
        makeFakeAgy({
          stdout: [
            '{"event":"init","conversation_id":"conv-cut"}',
            '{"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"partial"}}',
          ],
          stderr: "crashed",
          exitCode: 1,
        }),
      );
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          yield* adapter.sendTurn({ threadId, input: "say hello" });

          // Without this the assistant bubble would spin forever in the UI.
          const completedItem = events.find(
            (event) =>
              event.type === "item.completed" && event.payload.itemType === "assistant_message",
          );
          assert.isDefined(completedItem);
          if (completedItem?.type === "item.completed") {
            assert.equal(completedItem.payload.status, "failed");
          }

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("rejects attachments rather than dropping them silently", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-attachments");
      const { binaryPath } = yield* Effect.promise(() => makeFakeAgy());
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* adapter.startSession({
        threadId,
        provider: PROVIDER,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const exit = yield* Effect.exit(
        adapter.sendTurn({
          threadId,
          input: "look at this",
          attachments: [
            { type: "image", id: "att-1", name: "shot.png", mimeType: "image/png", sizeBytes: 10 },
          ],
        }),
      );

      assert.isTrue(Exit.isFailure(exit));
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("reports no session for an unknown thread", () =>
    Effect.gen(function* () {
      const { binaryPath } = yield* Effect.promise(() => makeFakeAgy());
      const adapter = yield* makeTestAdapter(binaryPath);

      const exit = yield* Effect.exit(
        adapter.sendTurn({ threadId: ThreadId.make("antigravity-missing"), input: "hi" }),
      );
      assert.isTrue(Exit.isFailure(exit));
    }),
  );
});

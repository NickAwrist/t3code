import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { describe } from "vite-plus/test";

import {
  AntigravitySettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type RuntimeMode,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { ProviderAdapterRequestError, ProviderAdapterValidationError } from "../Errors.ts";

import {
  buildAntigravityTurnArgs,
  makeAntigravityAdapter,
  parseAntigravityResume,
} from "./AntigravityAdapter.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);
const encodeJsonString = Schema.encodeSync(Schema.UnknownFromJsonString);
const PROVIDER = ProviderDriverKind.make("antigravity");
const INSTANCE_ID = ProviderInstanceId.make("antigravity");
const isProviderAdapterValidationError = Schema.is(ProviderAdapterValidationError);
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);

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
const makeFakeAgy = Effect.fn("makeFakeAgy")(function* (options?: {
  readonly stdout?: ReadonlyArray<string>;
  readonly stderr?: string;
  readonly exitCode?: number;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "antigravity-adapter-" });
  const binaryPath = path.join(dir, "fake-agy.sh");
  const argsPath = path.join(dir, "args.txt");
  const lines = options?.stdout ?? TRANSCRIPT;
  const script = [
    "#!/bin/sh",
    `printf '%s\\n' "$@" > ${encodeJsonString(argsPath)}`,
    ...lines.map((line) => `printf '%s\\n' ${encodeJsonString(line)}`),
    ...(options?.stderr ? [`printf '%s\\n' ${encodeJsonString(options.stderr)} >&2`] : []),
    `exit ${options?.exitCode ?? 0}`,
  ].join("\n");
  yield* fileSystem.writeFileString(binaryPath, `${script}\n`);
  yield* fileSystem.chmod(binaryPath, 0o755);
  return { binaryPath, argsPath };
});

const readArgs = Effect.fn("readArgs")(function* (argsPath: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const raw = yield* fileSystem.readFileString(argsPath);
  return raw.split("\n").slice(0, -1);
});

const makeControlledProcess = Effect.fn("makeControlledProcess")(function* (
  stdout: ReadonlyArray<string> = [],
) {
  const exit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const exitAwaited = yield* Deferred.make<void>();
  const killed = yield* Deferred.make<void>();
  const output =
    stdout.length > 0
      ? Stream.make(new TextEncoder().encode(`${stdout.join("\n")}\n`))
      : Stream.empty;
  const handle = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    exitCode: Deferred.succeed(exitAwaited, undefined).pipe(Effect.andThen(Deferred.await(exit))),
    isRunning: Deferred.isDone(exit).pipe(Effect.map((done) => !done)),
    kill: () =>
      Effect.gen(function* () {
        yield* Deferred.succeed(killed, undefined);
        yield* Deferred.succeed(exit, ChildProcessSpawner.ExitCode(143));
      }).pipe(Effect.asVoid),
    stdin: Sink.drain,
    stdout: output,
    stderr: Stream.empty,
    all: output,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
  return { exit, exitAwaited, handle, killed };
});

const makeControlledSpawner = Effect.fn("makeControlledSpawner")(function* (
  processes: ReadonlyArray<{ readonly handle: ChildProcessSpawner.ChildProcessHandle }>,
  firstSpawnGate?: Deferred.Deferred<void>,
) {
  const spawned = yield* Effect.forEach(processes, () => Deferred.make<void>());
  const commands: Array<ChildProcess.Command> = [];
  let index = 0;
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const current = index;
      index += 1;
      commands.push(command);
      yield* Deferred.succeed(spawned[current]!, undefined);
      if (current === 0 && firstSpawnGate) {
        yield* Deferred.await(firstSpawnGate);
      }
      return processes[current]!.handle;
    }),
  );
  return { commands, spawned, spawner };
});

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
      cwd: "/workspace",
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
      "--add-dir",
      "/workspace",
    ]);
  });

  it("passes the model, conversation, plan mode, and extra launch arguments", () => {
    const args = buildAntigravityTurnArgs({
      prompt: "hi",
      model: "gemini-3.1-pro-high",
      conversationId: "conv-1",
      planMode: true,
      cwd: "/workspace",
      launchArgs: "--effort high",
    });

    assertFlag(args, "--model", "gemini-3.1-pro-high");
    assertFlag(args, "--conversation", "conv-1");
    assertFlag(args, "--mode", "plan");
    assertFlag(args, "--add-dir", "/workspace");
    assertFlag(args, "--effort", "high");
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
  it.effect("supports full-access and rejects modes print mode cannot implement", () =>
    Effect.gen(function* () {
      const { binaryPath } = yield* makeFakeAgy();
      const adapter = yield* makeTestAdapter(binaryPath);
      const unsupportedModes: ReadonlyArray<Exclude<RuntimeMode, "full-access">> = [
        "approval-required",
        "auto-accept-edits",
        "auto",
      ];

      for (const runtimeMode of unsupportedModes) {
        const threadId = ThreadId.make(`antigravity-permission-${runtimeMode}`);
        const exit = yield* Effect.exit(
          adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode,
          }),
        );
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause);
          assert.isTrue(isProviderAdapterValidationError(error));
          if (isProviderAdapterValidationError(error)) {
            assert.include(error.issue, "only supports 'full-access'");
            assert.include(error.issue, runtimeMode);
          }
        }
        assert.isFalse(yield* adapter.hasSession(threadId));
      }

      const fullAccessThreadId = ThreadId.make("antigravity-permission-full-access");
      const session = yield* adapter.startSession({
        threadId: fullAccessThreadId,
        provider: PROVIDER,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.equal(session.runtimeMode, "full-access");
      yield* adapter.stopSession(fullAccessThreadId);
    }),
  );

  it.effect("streams a turn's text and tools, then reports the conversation id", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-happy-path");
      const { binaryPath, argsPath } = yield* makeFakeAgy();
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
          assert.equal(types.filter((type) => type === "thread.started").length, 1);

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
          assert.equal(events.filter((event) => event.type === "turn.completed").length, 1);

          const args = yield* readArgs(argsPath);
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
          assertFlag(args, "--add-dir", process.cwd());
          // First turn has no conversation to resume yet.
          assert.notInclude([...args], "--conversation");

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("resumes the same agy conversation without re-announcing its provider thread", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-resume");
      const { binaryPath, argsPath } = yield* makeFakeAgy();
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          yield* adapter.sendTurn({ threadId, input: "first" });
          yield* adapter.sendTurn({ threadId, input: "second" });

          const args = yield* readArgs(argsPath);
          assertFlag(args, "--conversation", "conv-abc");
          assert.equal(events.filter((event) => event.type === "thread.started").length, 1);

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("resumes from a persisted cursor without waiting for a turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-cursor");
      const { binaryPath, argsPath } = yield* makeFakeAgy();
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
            resumeCursor: { schemaVersion: 1, conversationId: "conv-restored" },
          });
          yield* adapter.sendTurn({ threadId, input: "first" });

          const args = yield* readArgs(argsPath);
          assertFlag(args, "--conversation", "conv-restored");
          assert.equal(events.filter((event) => event.type === "thread.started").length, 1);

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("announces a fresh conversation learned only from the final result", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-final-conversation");
      const { binaryPath } = yield* makeFakeAgy({
        stdout: [
          '{"event":"result","result":{"conversation_id":"conv-final","status":"SUCCESS","response":"done"}}',
        ],
      });
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          const result = yield* adapter.sendTurn({ threadId, input: "finish" });

          assert.deepEqual(result.resumeCursor, {
            schemaVersion: 1,
            conversationId: "conv-final",
          });
          const threadEvents = events.filter((event) => event.type === "thread.started");
          assert.equal(threadEvents.length, 1);
          if (threadEvents[0]?.type === "thread.started") {
            assert.equal(threadEvents[0].payload.providerThreadId, "conv-final");
          }
          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("fails the turn and surfaces stderr when the CLI exits non-zero", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-failure");
      const { binaryPath } = yield* makeFakeAgy({
        stdout: [],
        stderr: "not signed in",
        exitCode: 3,
      });
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
          assert.equal(events.filter((event) => event.type === "turn.completed").length, 1);

          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("settles a started turn when binary spawn fails", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-binary-failure");
      const adapter = yield* makeTestAdapter("/definitely/not/installed/agy");

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          yield* adapter.sendTurn({ threadId, input: "hello" });

          const completed = events.filter((event) => event.type === "turn.completed");
          assert.equal(completed.length, 1);
          assert.equal(completed[0]?.payload.state, "failed");
          assert.include(completed[0]?.payload.errorMessage ?? "", "Failed to spawn");
          assert.equal(events.filter((event) => event.type === "runtime.error").length, 1);
          const sessions = yield* adapter.listSessions();
          assert.equal(sessions[0]?.status, "ready");
          assert.isUndefined(sessions[0]?.activeTurnId);
          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("fails a zero-exit process that never reports a result", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-premature-exit");
      const { binaryPath } = yield* makeFakeAgy({
        stdout: ['{"event":"init","conversation_id":"conv-premature"}'],
      });
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          const result = yield* adapter.sendTurn({ threadId, input: "hello" });

          const completed = events.filter((event) => event.type === "turn.completed");
          assert.equal(completed.length, 1);
          assert.equal(completed[0]?.payload.state, "failed");
          assert.include(completed[0]?.payload.errorMessage ?? "", "before reporting a result");
          assert.deepEqual(result.resumeCursor, {
            schemaVersion: 1,
            conversationId: "conv-premature",
          });
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
      const { binaryPath } = yield* makeFakeAgy({
        stdout: [
          `{"event":"result","result":{"conversation_id":"","status":"ERROR","response":"","error":${JSON_QUOTED_QUOTA_ERROR}}}`,
        ],
        exitCode: 1,
      });
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
      const { binaryPath } = yield* makeFakeAgy({
        stdout: [
          '{"event":"init","conversation_id":"conv-cut"}',
          '{"event":"step_update","step_update":{"step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"partial"}}',
        ],
        stderr: "crashed",
        exitCode: 1,
      });
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

  it.effect("serializes concurrent turns and resumes the second process", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-serialized");
      const first = yield* makeControlledProcess(TRANSCRIPT);
      const second = yield* makeControlledProcess(TRANSCRIPT);
      const controlled = yield* makeControlledSpawner([first, second]);
      const adapter = yield* makeTestAdapter("/bin/sh").pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, controlled.spawner),
      );

      yield* adapter.startSession({
        threadId,
        provider: PROVIDER,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const firstFiber = yield* adapter
        .sendTurn({ threadId, input: "first" })
        .pipe(Effect.forkChild);
      yield* Deferred.await(first.exitAwaited);
      const secondFiber = yield* adapter
        .sendTurn({ threadId, input: "second" })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* Deferred.poll(controlled.spawned[1]!)));

      yield* Deferred.succeed(first.exit, ChildProcessSpawner.ExitCode(0));
      yield* Deferred.await(controlled.spawned[1]!);
      yield* Fiber.join(firstFiber);
      const secondCommand = controlled.commands[1];
      assert.equal(secondCommand?._tag, "StandardCommand");
      if (secondCommand?._tag === "StandardCommand") {
        assertFlag(secondCommand.args, "--conversation", "conv-abc");
      }
      yield* Deferred.succeed(second.exit, ChildProcessSpawner.ExitCode(0));
      yield* Fiber.join(secondFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("interrupts a turn while process spawning is still being prepared", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-interrupt-preparation");
      const controlledProcess = yield* makeControlledProcess(TRANSCRIPT);
      const spawnGate = yield* Deferred.make<void>();
      const controlled = yield* makeControlledSpawner([controlledProcess], spawnGate);
      const adapter = yield* makeTestAdapter("/bin/sh").pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, controlled.spawner),
      );

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          const turnFiber = yield* adapter
            .sendTurn({ threadId, input: "wait" })
            .pipe(Effect.forkChild);
          yield* Deferred.await(controlled.spawned[0]!);
          yield* adapter.interruptTurn(threadId);
          yield* Deferred.succeed(spawnGate, undefined);
          yield* Deferred.await(controlledProcess.killed);
          yield* Fiber.join(turnFiber);

          const completed = events.filter((event) => event.type === "turn.completed");
          assert.equal(completed.length, 1);
          assert.equal(completed[0]?.payload.state, "interrupted");
          assert.equal(events.filter((event) => event.type === "runtime.error").length, 0);
          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("interrupts a running process and settles the turn once", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-interrupt-running");
      const controlledProcess = yield* makeControlledProcess(TRANSCRIPT);
      const controlled = yield* makeControlledSpawner([controlledProcess]);
      const adapter = yield* makeTestAdapter("/bin/sh").pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, controlled.spawner),
      );

      yield* withRecordedEvents(adapter, (events) =>
        Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: PROVIDER,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });
          const turnFiber = yield* adapter
            .sendTurn({ threadId, input: "wait" })
            .pipe(Effect.forkChild);
          yield* Deferred.await(controlledProcess.exitAwaited);
          yield* adapter.interruptTurn(threadId);
          yield* Deferred.await(controlledProcess.killed);
          yield* Fiber.join(turnFiber);

          const completed = events.filter((event) => event.type === "turn.completed");
          assert.equal(completed.length, 1);
          assert.equal(completed[0]?.payload.state, "interrupted");
          yield* adapter.stopSession(threadId);
        }),
      );
    }),
  );

  it.effect("rejects rollback because AGY cannot remove provider conversation turns", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-rollback");
      const { binaryPath } = yield* makeFakeAgy();
      const adapter = yield* makeTestAdapter(binaryPath);
      yield* adapter.startSession({
        threadId,
        provider: PROVIDER,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const exit = yield* Effect.exit(adapter.rollbackThread(threadId, 1));
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        const error = Cause.squash(exit.cause);
        assert.isTrue(isProviderAdapterRequestError(error));
        if (isProviderAdapterRequestError(error)) {
          assert.include(error.detail, "provider-side rollback");
        }
      }
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects attachments rather than dropping them silently", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("antigravity-attachments");
      const { binaryPath } = yield* makeFakeAgy();
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
      const { binaryPath } = yield* makeFakeAgy();
      const adapter = yield* makeTestAdapter(binaryPath);

      const exit = yield* Effect.exit(
        adapter.sendTurn({ threadId: ThreadId.make("antigravity-missing"), input: "hi" }),
      );
      assert.isTrue(Exit.isFailure(exit));
    }),
  );
});

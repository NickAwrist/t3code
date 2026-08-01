/**
 * AntigravityAdapter — provider adapter for the Antigravity CLI (`agy`).
 *
 * `agy` has no long-lived session protocol: print mode runs one prompt per
 * process and exits. A T3 session is therefore just a record, and each turn
 * spawns `agy -p … --output-format stream-json`, streams the child's NDJSON
 * into canonical runtime events, and finishes when the process exits.
 * Continuity across turns comes from the `conversation_id` agy reports, which
 * is stored as the session resume cursor and replayed with `--conversation`.
 *
 * Permissions are always bypassed (`--dangerously-skip-permissions`): print
 * mode has no channel to surface an approval prompt on, so the driver
 * advertises no approval support rather than pretending to ask.
 *
 * Event translation lives in `antigravityStreamJson.ts`.
 *
 * @module AntigravityAdapter
 */
import {
  type AntigravitySettings,
  type ApprovalRequestId,
  EventId,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  antigravityTurnState,
  closeOpenAntigravityItems,
  decodeAntigravityLine,
  makeAntigravityTurnState,
  mapAntigravityStreamEvent,
  type AntigravityRuntimeEventDraft,
  type AntigravityStreamEvent,
} from "./antigravityStreamJson.ts";

const PROVIDER = ProviderDriverKind.make("antigravity");
const ANTIGRAVITY_RESUME_VERSION = 1 as const;

/**
 * agy's own `--print-timeout` defaults to five minutes, which would abort long
 * agent turns from under us. T3 governs turn lifetime itself, so the CLI timer
 * is pushed out far enough to never be the thing that stops a turn.
 */
const PRINT_TIMEOUT = "24h";

/** Stderr kept for error reporting; bounded so a chatty failure cannot grow unbounded. */
const MAX_STDERR_CHARS = 8_000;

export interface AntigravityAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface AntigravitySessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  conversationId: string | undefined;
  activeTurnId: TurnId | undefined;
  /** Handle for the in-flight `agy` process, used to interrupt the turn. */
  activeChild: ChildProcessSpawner.ChildProcessHandle | undefined;
  /** Set by `interruptTurn` so the turn settles as interrupted, not failed. */
  interrupted: boolean;
  totalProcessedTokens: number;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  stopped: boolean;
}

interface AntigravityResumeCursor {
  readonly schemaVersion: typeof ANTIGRAVITY_RESUME_VERSION;
  readonly conversationId: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/**
 * Recover the agy conversation id from a persisted resume cursor. Anything
 * that does not match the shape this adapter writes is ignored, so a cursor
 * left behind by another provider simply starts a fresh conversation.
 */
export function parseAntigravityResume(raw: unknown): AntigravityResumeCursor | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== ANTIGRAVITY_RESUME_VERSION) return undefined;
  const conversationId = raw.conversationId;
  if (typeof conversationId !== "string" || conversationId.trim().length === 0) return undefined;
  return { schemaVersion: ANTIGRAVITY_RESUME_VERSION, conversationId: conversationId.trim() };
}

/**
 * Assemble the `agy` argument vector for one turn.
 *
 * The prompt travels as an argv entry because agy's `--print` is a string flag
 * with no stdin path — `PROVIDER_SEND_TURN_MAX_INPUT_CHARS` keeps it inside
 * the platform's per-argument limit.
 */
export function buildAntigravityTurnArgs(input: {
  readonly prompt: string;
  readonly model: string | undefined;
  readonly conversationId: string | undefined;
  readonly planMode: boolean;
  readonly launchArgs: string | undefined;
}): ReadonlyArray<string> {
  return [
    "-p",
    input.prompt,
    "--output-format",
    "stream-json",
    "--dangerously-skip-permissions",
    "--print-timeout",
    PRINT_TIMEOUT,
    ...(input.model ? ["--model", input.model] : []),
    ...(input.conversationId ? ["--conversation", input.conversationId] : []),
    ...(input.planMode ? ["--mode", "plan"] : []),
    ...tokenizeCliArgs(input.launchArgs),
  ];
}

export function makeAntigravityAdapter(
  settings: AntigravitySettings,
  options?: AntigravityAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("antigravity");
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;
    const environment = options?.environment ?? process.env;
    const nativeEventLogger = options?.nativeEventLogger;

    const sessions = new Map<ThreadId, AntigravitySessionContext>();
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Antigravity runtime identifier.",
            cause,
          }),
      ),
    );
    const makeEventStamp = () =>
      Effect.all({
        eventId: Effect.map(randomUUIDv4, (id) => EventId.make(id)),
        createdAt: nowIso,
      });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    /** Stamp a mapper draft with the identity fields the adapter owns. */
    const emitDraft = (
      threadId: ThreadId,
      turnId: TurnId | undefined,
      draft: AntigravityRuntimeEventDraft,
    ) =>
      Effect.gen(function* () {
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          ...draft,
          ...stamp,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId,
          ...(turnId ? { turnId } : {}),
        });
      });

    const logNative = (threadId: ThreadId, payload: unknown) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUIDv4,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method: "agy/stream-json",
              threadId,
              payload,
            },
          },
          threadId,
        );
      });

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<AntigravitySessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const resumeCursorFor = (ctx: AntigravitySessionContext): AntigravityResumeCursor | undefined =>
      ctx.conversationId
        ? { schemaVersion: ANTIGRAVITY_RESUME_VERSION, conversationId: ctx.conversationId }
        : undefined;

    const stopSessionInternal = (ctx: AntigravitySessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        const child = ctx.activeChild;
        if (child) {
          ctx.interrupted = true;
          yield* child
            .kill({ killSignal: "SIGTERM", forceKillAfter: "5 seconds" })
            .pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to stop the Antigravity CLI process.", { cause }),
              ),
            );
        }
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession = (
      input: ProviderSessionStartInput,
    ): Effect.Effect<ProviderSession, ProviderAdapterError> =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required and must be non-empty.",
          });
        }

        const existing = sessions.get(input.threadId);
        if (existing && !existing.stopped) {
          yield* stopSessionInternal(existing);
        }

        const cwd = path.resolve(input.cwd.trim());
        const modelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const resume = parseAntigravityResume(input.resumeCursor);
        const now = yield* nowIso;

        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          ...(modelSelection?.model ? { model: modelSelection.model } : {}),
          threadId: input.threadId,
          ...(resume ? { resumeCursor: resume } : {}),
          createdAt: now,
          updatedAt: now,
        };

        const ctx: AntigravitySessionContext = {
          threadId: input.threadId,
          session,
          conversationId: resume?.conversationId,
          activeTurnId: undefined,
          activeChild: undefined,
          interrupted: false,
          totalProcessedTokens: 0,
          turns: [],
          stopped: false,
        };
        sessions.set(input.threadId, ctx);

        yield* offerRuntimeEvent({
          type: "session.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          ...(resume ? { payload: { resume } } : { payload: {} }),
        });
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          payload: { state: "ready", reason: "Antigravity session ready" },
        });
        if (resume) {
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: input.threadId,
            payload: { providerThreadId: resume.conversationId },
          });
        }

        return session;
      });

    const sendTurn = (
      input: ProviderSendTurnInput,
    ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);

        // agy print mode accepts a single text prompt and has no attachment
        // channel, so images would be silently dropped if we accepted them.
        if (input.attachments && input.attachments.length > 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Antigravity does not support attachments.",
          });
        }
        const prompt = input.input?.trim();
        if (!prompt) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text.",
          });
        }

        const turnModelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const model = turnModelSelection?.model ?? ctx.session.model;
        const turnId = TurnId.make(yield* randomUUIDv4);

        ctx.activeTurnId = turnId;
        ctx.interrupted = false;
        ctx.session = {
          ...ctx.session,
          status: "running",
          activeTurnId: turnId,
          ...(model ? { model } : {}),
          updatedAt: yield* nowIso,
        };

        yield* offerRuntimeEvent({
          type: "turn.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          turnId,
          payload: model ? { model } : {},
        });

        const state = makeAntigravityTurnState({
          turnId,
          conversationId: ctx.conversationId,
          totalProcessedTokens: ctx.totalProcessedTokens,
        });
        const knownConversationId = ctx.conversationId;
        const args = buildAntigravityTurnArgs({
          prompt,
          model,
          conversationId: knownConversationId,
          planMode: input.interactionMode === "plan",
          launchArgs: settings.launchArgs,
        });

        const binary = settings.binaryPath || "agy";
        const spawnCommand = yield* resolveSpawnCommand(binary, [...args], {
          env: environment,
        }).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: `Failed to resolve the Antigravity binary '${binary}'.`,
                cause,
              }),
          ),
        );

        const lastResultStatusRef = yield* Ref.make<string | undefined>(undefined);
        const lastResultErrorRef = yield* Ref.make<string | undefined>(undefined);
        const stderrRef = yield* Ref.make("");

        const handleStreamEvent = (event: AntigravityStreamEvent) =>
          Effect.gen(function* () {
            if (event.event === "result") {
              yield* Ref.set(lastResultStatusRef, event.result.status);
              yield* Ref.set(lastResultErrorRef, event.result.error?.trim() || undefined);
            }
            const drafts = mapAntigravityStreamEvent(state, event);
            yield* Effect.forEach(drafts, (draft) => emitDraft(input.threadId, turnId, draft), {
              discard: true,
            });
          });

        const runProcess = Effect.gen(function* () {
          const child = yield* childProcessSpawner
            .spawn(
              ChildProcess.make(spawnCommand.command, spawnCommand.args, {
                env: environment,
                cwd: ctx.session.cwd,
                shell: spawnCommand.shell,
                // The prompt travels in argv and we never write to the child;
                // an open stdin pipe only risks agy waiting on input.
                stdin: "ignore",
              }),
            )
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: `Failed to spawn the Antigravity CLI: ${cause.message}`,
                    cause,
                  }),
              ),
            );
          ctx.activeChild = child;

          const drainStdout = child.stdout.pipe(
            Stream.decodeText(),
            Stream.splitLines,
            Stream.runForEach((line) =>
              Effect.gen(function* () {
                const decoded = decodeAntigravityLine(line);
                if (decoded === undefined) return;
                yield* logNative(input.threadId, decoded);
                yield* handleStreamEvent(decoded);
              }),
            ),
          );

          // stderr must be consumed concurrently or a verbose failure can fill
          // the pipe buffer and wedge the child before it ever exits.
          const drainStderr = child.stderr.pipe(
            Stream.decodeText(),
            Stream.runForEach((chunk) =>
              Ref.update(stderrRef, (current) =>
                current.length >= MAX_STDERR_CHARS
                  ? current
                  : (current + chunk).slice(0, MAX_STDERR_CHARS),
              ),
            ),
          );

          const stdoutFiber = yield* Effect.forkChild(drainStdout);
          const stderrFiber = yield* Effect.forkChild(drainStderr);
          const exitCode = yield* child.exitCode;
          yield* Fiber.join(stdoutFiber);
          yield* Fiber.join(stderrFiber);
          return Number(exitCode);
        }).pipe(
          Effect.scoped,
          Effect.mapError((cause) =>
            cause._tag === "ProviderAdapterProcessError"
              ? cause
              : new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: `Antigravity CLI stream failed: ${cause.message}`,
                  cause,
                }),
          ),
        );

        const exitCode = yield* runProcess.pipe(
          Effect.ensuring(
            Effect.sync(() => {
              ctx.activeChild = undefined;
            }),
          ),
        );

        ctx.conversationId = state.conversationId ?? ctx.conversationId;
        ctx.totalProcessedTokens = state.totalProcessedTokens;

        const interrupted = ctx.interrupted;
        const resultStatus = yield* Ref.get(lastResultStatusRef);
        const resultError = yield* Ref.get(lastResultErrorRef);
        const stderr = (yield* Ref.get(stderrRef)).trim();
        const turnState = interrupted
          ? "interrupted"
          : exitCode !== 0
            ? "failed"
            : antigravityTurnState(resultStatus);

        yield* Effect.forEach(
          closeOpenAntigravityItems(state, turnState === "completed" ? "completed" : "failed"),
          (draft) => emitDraft(input.threadId, turnId, draft),
          { discard: true },
        );

        // A fresh conversation only reveals its id once the child has spoken;
        // announce it so the thread can be resumed after a restart.
        if (knownConversationId === undefined && ctx.conversationId !== undefined) {
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: input.threadId,
            turnId,
            payload: { providerThreadId: ctx.conversationId },
          });
        }

        // agy reports quota, auth and eligibility failures in the result
        // event and writes nothing to stderr, so its own message is the only
        // one worth showing; the exit code is the last resort.
        const errorMessage =
          turnState === "failed"
            ? (resultError ?? stderr ?? "") || `Antigravity CLI exited with code ${exitCode}.`
            : undefined;

        if (errorMessage) {
          yield* offerRuntimeEvent({
            type: "runtime.error",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: input.threadId,
            turnId,
            payload: { message: errorMessage, class: "provider_error" },
          });
        }

        const resumeCursor = resumeCursorFor(ctx);
        ctx.activeTurnId = undefined;
        ctx.session = {
          ...ctx.session,
          status: "ready",
          activeTurnId: undefined,
          ...(resumeCursor ? { resumeCursor } : {}),
          updatedAt: yield* nowIso,
        };
        ctx.turns.push({ id: turnId, items: [{ prompt, model }] });

        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          turnId,
          payload: {
            state: turnState,
            stopReason: resultStatus ?? null,
            ...(errorMessage ? { errorMessage } : {}),
          },
        });

        return {
          threadId: input.threadId,
          turnId,
          ...(resumeCursor ? { resumeCursor } : {}),
        };
      });

    const interruptTurn = (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const child = ctx.activeChild;
        if (!child) return;
        ctx.interrupted = true;
        // The turn's own `sendTurn` fiber observes the exit and settles the
        // turn as interrupted; this only has to stop the process.
        yield* child
          .kill({ killSignal: "SIGTERM", forceKillAfter: "5 seconds" })
          .pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Failed to interrupt the Antigravity CLI process.", { cause }),
            ),
          );
      });

    const stopSession = (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        yield* stopSessionInternal(ctx);
      });

    /**
     * agy print mode auto-approves everything, so no approval is ever opened.
     * Failing loudly here beats a silent success that would leave the
     * orchestrator waiting on a decision nothing will consume.
     */
    const respondToRequest = (
      _threadId: ThreadId,
      requestId: ApprovalRequestId,
      _decision: ProviderApprovalDecision,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "approval/respond",
          detail: `Antigravity runs with permissions bypassed and opens no approvals (request ${requestId}).`,
        }),
      );

    const respondToUserInput = (
      _threadId: ThreadId,
      requestId: ApprovalRequestId,
      _answers: ProviderUserInputAnswers,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "user-input/respond",
          detail: `Antigravity does not request structured user input (request ${requestId}).`,
        }),
      );

    const readThread = (
      threadId: ThreadId,
    ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread = (
      threadId: ThreadId,
      numTurns: number,
    ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        ctx.turns.splice(Math.max(0, ctx.turns.length - numTurns));
        return { threadId, turns: ctx.turns };
      });

    const listSessions = (): Effect.Effect<ReadonlyArray<ProviderSession>> =>
      Effect.sync(() => Array.from(sessions.values(), (ctx) => ({ ...ctx.session })));

    const hasSession = (threadId: ThreadId): Effect.Effect<boolean> =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return ctx !== undefined && !ctx.stopped;
      });

    const stopAll = (): Effect.Effect<void, ProviderAdapterError> =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Failed to stop Antigravity sessions on shutdown.", { cause }),
        ),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
      ),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
}

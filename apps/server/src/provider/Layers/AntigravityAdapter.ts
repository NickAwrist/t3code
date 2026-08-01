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
 * Print mode cannot service approval prompts, so the adapter only accepts
 * T3's full-access runtime mode and passes `--dangerously-skip-permissions`.
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
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
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
  readonly cwd: string;
  session: ProviderSession;
  conversationId: string | undefined;
  activeTurnId: TurnId | undefined;
  activeChild:
    | {
        readonly turnId: TurnId;
        readonly handle: ChildProcessSpawner.ChildProcessHandle;
      }
    | undefined;
  readonly interruptedTurnIds: Set<TurnId>;
  announcedConversationId: string | undefined;
  totalProcessedTokens: number;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  stopped: boolean;
}

const AntigravityResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(ANTIGRAVITY_RESUME_VERSION),
  conversationId: Schema.String,
});
interface AntigravityResumeCursor extends Schema.Schema.Type<typeof AntigravityResumeCursor> {}

const isProviderAdapterProcessError = Schema.is(ProviderAdapterProcessError);
const isAntigravityResumeCursor = Schema.is(AntigravityResumeCursor);

/**
 * Recover the agy conversation id from a persisted resume cursor. Anything
 * that does not match the shape this adapter writes is ignored, so a cursor
 * left behind by another provider simply starts a fresh conversation.
 */
export function parseAntigravityResume(raw: unknown): AntigravityResumeCursor | undefined {
  if (!isAntigravityResumeCursor(raw)) return undefined;
  const conversationId = raw.conversationId.trim();
  return conversationId ? { ...raw, conversationId } : undefined;
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
  readonly cwd: string;
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
    "--add-dir",
    input.cwd,
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
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
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

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = Option.fromNullishOr(current.get(threadId));
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

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
        const activeChild = ctx.activeChild;
        if (ctx.activeTurnId) {
          ctx.interruptedTurnIds.add(ctx.activeTurnId);
        }
        if (activeChild) {
          yield* activeChild.handle
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
      withThreadLock(
        input.threadId,
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
          if (input.runtimeMode !== "full-access") {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Antigravity print mode only supports 'full-access'; '${input.runtimeMode}' cannot be implemented without interactive approvals.`,
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
            cwd,
            session,
            conversationId: resume?.conversationId,
            activeTurnId: undefined,
            activeChild: undefined,
            interruptedTurnIds: new Set(),
            announcedConversationId: resume?.conversationId,
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
        }),
      );

    const sendTurn = (
      input: ProviderSendTurnInput,
    ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
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
          const state = makeAntigravityTurnState({
            turnId,
            conversationId: ctx.conversationId,
            totalProcessedTokens: ctx.totalProcessedTokens,
          });
          const lastResultStatusRef = yield* Ref.make<string | undefined>(undefined);
          const lastResultErrorRef = yield* Ref.make<string | undefined>(undefined);
          const sawResultRef = yield* Ref.make(false);
          const stderrRef = yield* Ref.make("");

          ctx.activeTurnId = turnId;
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

          const syncConversationId = Effect.fn("AntigravityAdapter.syncConversationId")(
            function* () {
              const conversationId = state.conversationId;
              if (!conversationId) return;
              ctx.conversationId = conversationId;
              if (ctx.announcedConversationId !== undefined) return;
              ctx.announcedConversationId = conversationId;
              yield* offerRuntimeEvent({
                type: "thread.started",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId: input.threadId,
                turnId,
                payload: { providerThreadId: conversationId },
              });
            },
          );

          const handleStreamEvent = Effect.fn("AntigravityAdapter.handleStreamEvent")(function* (
            event: AntigravityStreamEvent,
          ) {
            if (event.event === "result") {
              yield* Ref.set(sawResultRef, true);
              yield* Ref.set(lastResultStatusRef, event.result.status);
              yield* Ref.set(lastResultErrorRef, event.result.error?.trim() || undefined);
            }
            const drafts = mapAntigravityStreamEvent(state, event);
            yield* syncConversationId();
            yield* Effect.forEach(drafts, (draft) => emitDraft(input.threadId, turnId, draft), {
              discard: true,
            });
          });

          const runProcess = Effect.gen(function* () {
            const args = buildAntigravityTurnArgs({
              prompt,
              model,
              conversationId: ctx.conversationId,
              planMode: input.interactionMode === "plan",
              cwd: ctx.cwd,
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
            const child = yield* childProcessSpawner
              .spawn(
                ChildProcess.make(spawnCommand.command, spawnCommand.args, {
                  env: environment,
                  cwd: ctx.cwd,
                  shell: spawnCommand.shell,
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
            ctx.activeChild = { turnId, handle: child };
            if (ctx.interruptedTurnIds.has(turnId)) {
              yield* child
                .kill({ killSignal: "SIGTERM", forceKillAfter: "5 seconds" })
                .pipe(Effect.catchCause(() => Effect.void));
            }

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
              isProviderAdapterProcessError(cause)
                ? cause
                : new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: `Antigravity CLI stream failed: ${cause.message}`,
                    cause,
                  }),
            ),
          );

          return yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const processExit = yield* Effect.exit(restore(runProcess));
              if (ctx.activeChild?.turnId === turnId) {
                ctx.activeChild = undefined;
              }
              yield* syncConversationId();
              ctx.totalProcessedTokens = state.totalProcessedTokens;

              const interrupted = ctx.interruptedTurnIds.has(turnId);
              const resultStatus = yield* Ref.get(lastResultStatusRef);
              const resultError = yield* Ref.get(lastResultErrorRef);
              const stderr = (yield* Ref.get(stderrRef)).trim();
              const sawResult = yield* Ref.get(sawResultRef);
              const exitCode = Exit.isSuccess(processExit) ? processExit.value : undefined;
              const processFailure = Exit.isFailure(processExit)
                ? Cause.squash(processExit.cause)
                : undefined;
              const processFailureMessage = isProviderAdapterProcessError(processFailure)
                ? processFailure.detail
                : processFailure instanceof Error
                  ? processFailure.message
                  : processFailure === undefined
                    ? undefined
                    : String(processFailure);
              const turnState = interrupted
                ? "interrupted"
                : processFailure !== undefined || exitCode !== 0 || !sawResult
                  ? "failed"
                  : antigravityTurnState(resultStatus);
              const errorMessage =
                turnState === "failed"
                  ? (resultError ?? stderr ?? "") ||
                    processFailureMessage ||
                    (exitCode === 0 && !sawResult
                      ? "Antigravity CLI exited before reporting a result."
                      : `Antigravity CLI exited with code ${exitCode ?? "unknown"}.`)
                  : undefined;

              yield* Effect.forEach(
                closeOpenAntigravityItems(
                  state,
                  turnState === "completed" ? "completed" : "failed",
                ),
                (draft) => emitDraft(input.threadId, turnId, draft),
                { discard: true },
              );
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
              if (ctx.activeTurnId === turnId) {
                ctx.activeTurnId = undefined;
              }
              if (!ctx.stopped) {
                ctx.session = {
                  ...ctx.session,
                  status: "ready",
                  activeTurnId: undefined,
                  ...(resumeCursor ? { resumeCursor } : {}),
                  updatedAt: yield* nowIso,
                };
              }
              ctx.turns.push({ id: turnId, items: [{ prompt, model, state: turnState }] });
              ctx.interruptedTurnIds.delete(turnId);

              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId: input.threadId,
                turnId,
                payload: {
                  state: turnState,
                  stopReason: interrupted ? "interrupted" : (resultStatus ?? null),
                  ...(errorMessage ? { errorMessage } : {}),
                },
              });

              return {
                threadId: input.threadId,
                turnId,
                ...(resumeCursor ? { resumeCursor } : {}),
              };
            }),
          );
        }),
      );

    const interruptTurn = (
      threadId: ThreadId,
      requestedTurnId?: TurnId,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const activeTurnId = ctx.activeTurnId;
        if (requestedTurnId && activeTurnId && requestedTurnId !== activeTurnId) return;
        const interruptedTurnId = requestedTurnId ?? activeTurnId;
        if (!interruptedTurnId) return;
        ctx.interruptedTurnIds.add(interruptedTurnId);
        const activeChild = ctx.activeChild;
        if (!activeChild || activeChild.turnId !== interruptedTurnId) return;
        yield* activeChild.handle
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

    const respondToRequest = (
      _threadId: ThreadId,
      requestId: ApprovalRequestId,
      _decision: ProviderApprovalDecision,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "approval/respond",
          detail: `Antigravity full-access sessions open no approvals (request ${requestId}).`,
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
        yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "thread/rollback",
          detail: "Antigravity conversations do not support provider-side rollback yet.",
        });
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

import {
  ApprovalRequestId,
  type AntigravitySettings,
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
  RuntimeItemId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { collectStreamAsString } from "../providerSnapshot.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";

import * as Schema from "effect/Schema";

const PROVIDER = ProviderDriverKind.make("antigravity");
const decodeUnknownJsonString = Schema.decodeUnknownSync(Schema.UnknownFromJsonString);

export interface AntigravityAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface AntigravitySessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  conversationId: string | undefined;
  activeTurnId: TurnId | undefined;
  stopped: boolean;
}

export function makeAntigravityAdapter(
  settings: AntigravitySettings,
  options?: AntigravityAdapterLiveOptions,
): Effect.Effect<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | Path.Path
> {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("antigravity");
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;

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
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () =>
      Effect.all({ eventId: nextEventId, createdAt: nowIso }).pipe(
        Effect.map((stamp) => ({
          ...stamp,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
        })),
      );

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const startSession = (
      input: ProviderSessionStartInput,
    ): Effect.Effect<ProviderSession, ProviderAdapterError> =>
      Effect.gen(function* () {
        const cwd = input.cwd ? path.resolve(input.cwd.trim()) : process.cwd();
        const now = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          threadId: input.threadId,
          createdAt: now,
          updatedAt: now,
        };

        const ctx: AntigravitySessionContext = {
          threadId: input.threadId,
          session,
          conversationId: undefined,
          activeTurnId: undefined,
          stopped: false,
        };

        sessions.set(input.threadId, ctx);

        yield* offerRuntimeEvent({
          type: "session.started",
          ...(yield* makeEventStamp()),
          threadId: input.threadId,
          payload: {},
        });

        return session;
      });

    const sendTurn = (
      input: ProviderSendTurnInput,
    ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = sessions.get(input.threadId);
        if (!ctx || ctx.stopped) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId: input.threadId,
          });
        }

        const turnId = TurnId.make(yield* randomUUIDv4);
        const promptText = input.input ?? "";

        ctx.activeTurnId = turnId;
        ctx.session = {
          ...ctx.session,
          status: "running",
          activeTurnId: turnId,
          updatedAt: yield* nowIso,
        };

        yield* offerRuntimeEvent({
          type: "turn.started",
          ...(yield* makeEventStamp()),
          threadId: input.threadId,
          turnId,
          payload: {},
        });

        const binary = settings.binaryPath || "agy";
        const args = [
          "-p",
          promptText,
          "--output-format",
          "stream-json",
          "--dangerously-skip-permissions",
        ];

        if (ctx.conversationId) {
          args.push("--conversation", ctx.conversationId);
        }

        const command = ChildProcess.make(binary, args);

        yield* Effect.forkChild(
          Effect.scoped(
            Effect.gen(function* () {
              const child = yield* childProcessSpawner.spawn(command).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterProcessError({
                      provider: PROVIDER,
                      threadId: input.threadId,
                      detail: `Failed to spawn agy: ${cause}`,
                      cause,
                    }),
                ),
              );

              const stdoutLines = child.stdout.pipe(
                Stream.decodeText(),
                Stream.splitLines,
              );

              let streamedAnyDelta = false;
              yield* Stream.runForEach(stdoutLines, (line) =>
                Effect.gen(function* () {
                  if (!line.trim()) return;
                  let responseText = "";
                  if (line.startsWith("{")) {
                    try {
                      const parsed = decodeUnknownJsonString(line) as Record<string, unknown>;

                      if (!ctx.conversationId) {
                        if (typeof parsed.conversation_id === "string") {
                          ctx.conversationId = parsed.conversation_id;
                        } else if (
                          parsed.init &&
                          typeof (parsed.init as Record<string, unknown>).conversation_id === "string"
                        ) {
                          ctx.conversationId = (parsed.init as Record<string, unknown>).conversation_id as string;
                        } else if (
                          parsed.step_update &&
                          typeof (parsed.step_update as Record<string, unknown>).conversation_id === "string"
                        ) {
                          ctx.conversationId = (parsed.step_update as Record<string, unknown>).conversation_id as string;
                        } else if (
                          parsed.result &&
                          typeof (parsed.result as Record<string, unknown>).conversation_id === "string"
                        ) {
                          ctx.conversationId = (parsed.result as Record<string, unknown>).conversation_id as string;
                        }
                      }

                      if (parsed.step_update) {
                        const stepUpdate = parsed.step_update as Record<string, unknown>;
                        if (typeof stepUpdate.text_delta === "string" && stepUpdate.text_delta) {
                          responseText = stepUpdate.text_delta;
                          streamedAnyDelta = true;
                        }
                      } else if (parsed.result && !streamedAnyDelta) {
                        const resultObj = parsed.result as Record<string, unknown>;
                        if (typeof resultObj.response === "string" && resultObj.response) {
                          responseText = resultObj.response;
                        }
                      }
                    } catch (_e) {
                      responseText = line + "\n";
                    }
                  } else {
                    responseText = line + "\n";
                  }

                  if (responseText) {
                    yield* offerRuntimeEvent({
                      type: "content.delta",
                      ...(yield* makeEventStamp()),
                      threadId: input.threadId,
                      turnId,
                      payload: {
                        streamKind: "assistant_text",
                        delta: responseText,
                      },
                    });
                  }
                }),
              );

              ctx.session = {
                ...ctx.session,
                status: "ready",
                activeTurnId: undefined,
                updatedAt: yield* nowIso,
              };

              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                threadId: input.threadId,
                turnId,
                payload: {
                  state: "completed",
                  stopReason: null,
                },
              });
            }).pipe(Effect.catch(() => Effect.void)),
          ),
        );

        return {
          threadId: input.threadId,
          turnId,
        };
      });

    const stopSession = (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = sessions.get(threadId);
        if (ctx) {
          ctx.stopped = true;
          sessions.delete(threadId);
        }
      });

    const interruptTurn = (
      threadId: ThreadId,
      turnId?: TurnId,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const ctx = sessions.get(threadId);
        if (ctx && ctx.activeTurnId) {
          const targetTurnId = turnId ?? ctx.activeTurnId;
          ctx.session = {
            ...ctx.session,
            status: "ready",
            activeTurnId: undefined,
            updatedAt: yield* nowIso,
          };
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId,
            turnId: targetTurnId,
            payload: {
              state: "cancelled",
              stopReason: "cancelled",
            },
          });
        }
      });

    const listSessions = (): Effect.Effect<ReadonlyArray<ProviderSession>> =>
      Effect.succeed(Array.from(sessions.values()).map((ctx) => ctx.session));

    const hasSession = (threadId: ThreadId): Effect.Effect<boolean> =>
      Effect.succeed(sessions.has(threadId));

    const readThread = (
      threadId: ThreadId,
    ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
      Effect.succeed({
        threadId,
        turns: [],
      });

    const rollbackThread = (
      threadId: ThreadId,
      _numTurns: number,
    ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
      Effect.succeed({
        threadId,
        turns: [],
      });

    const stopAll = (): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        for (const ctx of sessions.values()) {
          ctx.stopped = true;
        }
        sessions.clear();
      });

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest: () => Effect.void,
      respondToUserInput: () => Effect.void,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    };
  });
}

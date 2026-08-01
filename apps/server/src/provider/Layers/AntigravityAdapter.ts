import {
  ApprovalRequestId,
  type AntigravitySettings,
  type CanonicalItemType,
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
import * as Scope from "effect/Scope";
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
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | Path.Path | Scope.Scope
> {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("antigravity");
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;
    const adapterScope = yield* Effect.scope;

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
      Effect.gen(function* () {
        yield* Effect.logInfo(`[AntigravityAdapter] offerRuntimeEvent: ${event.type} for thread ${String(event.threadId)} (instance: ${event.providerInstanceId})`);
        yield* PubSub.publish(runtimeEventPubSub, event);
      }).pipe(Effect.asVoid);

    const startSession = (
      input: ProviderSessionStartInput,
    ): Effect.Effect<ProviderSession, ProviderAdapterError> =>
      Effect.gen(function* () {
        yield* Effect.logInfo(`[AntigravityAdapter] startSession: threadId=${String(input.threadId)}`);
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
        yield* Effect.logInfo(`[AntigravityAdapter] sendTurn: threadId=${String(input.threadId)} input=${String(input.input)}`);
        const ctx = sessions.get(input.threadId);
        if (!ctx || ctx.stopped) {
          yield* Effect.logError(`[AntigravityAdapter] sendTurn ERROR: session not found or stopped for threadId=${String(input.threadId)}`);
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

        const binary = settings.binaryPath?.trim() || "/home/nickawrist/.local/bin/agy";
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

        yield* Effect.logInfo(`[AntigravityAdapter] Spawning process: ${binary} ${args.join(" ")}`);

        const command = ChildProcess.make(binary, args, {
          cwd: ctx.session.cwd,
          ...(options?.environment ? { env: options.environment, extendEnv: true } : {}),
        });

        let activeMessageItemId = RuntimeItemId.make(yield* randomUUIDv4);
        let hasEmittedMessageItemStarted = false;
        const activeToolItems = new Map<number, RuntimeItemId>();

        const ensureMessageItemStarted = () =>
          Effect.gen(function* () {
            if (!hasEmittedMessageItemStarted) {
              hasEmittedMessageItemStarted = true;
              yield* offerRuntimeEvent({
                type: "item.started",
                ...(yield* makeEventStamp()),
                threadId: input.threadId,
                turnId,
                itemId: activeMessageItemId,
                payload: {
                  itemType: "assistant_message" as const,
                  status: "inProgress" as const,
                },
              });
            }
          });

        const completeMessageItem = () =>
          Effect.gen(function* () {
            if (hasEmittedMessageItemStarted) {
              hasEmittedMessageItemStarted = false;
              yield* offerRuntimeEvent({
                type: "item.completed",
                ...(yield* makeEventStamp()),
                threadId: input.threadId,
                turnId,
                itemId: activeMessageItemId,
                payload: {
                  itemType: "assistant_message" as const,
                  status: "completed" as const,
                },
              });
              activeMessageItemId = RuntimeItemId.make(yield* randomUUIDv4);
            }
          });

        yield* Effect.scoped(
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

            yield* Effect.logInfo(`[AntigravityAdapter] agy spawned successfully (pid: ${child.pid})`);

            const stdoutLines = child.stdout.pipe(
              Stream.decodeText(),
              Stream.splitLines,
            );

            let streamedAnyDelta = false;
            yield* Stream.runForEach(stdoutLines, (line) =>
              Effect.gen(function* () {
                yield* Effect.logInfo(`[AntigravityAdapter] raw line: ${line}`);
                if (!line.trim()) return;
                let responseText = "";
                let shouldCompleteMessageAfterDelta = false;

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
                      if (ctx.conversationId) {
                        yield* Effect.logInfo(`[AntigravityAdapter] Set conversationId=${ctx.conversationId}`);
                      }
                    }

                    if (parsed.step_update) {
                      const stepUpdate = parsed.step_update as Record<string, unknown>;
                      const stepType = stepUpdate.step_type as string | undefined;
                      const stepIndex = typeof stepUpdate.step_index === "number" ? stepUpdate.step_index : 0;
                      const state = stepUpdate.state as string | undefined;

                      if (stepType === "tool") {
                        const toolName = (stepUpdate.tool_name as string) || "tool";
                        const toolInfo = stepUpdate.tool_info as Record<string, unknown> | undefined;
                        const parameters = toolInfo?.parameters as Record<string, unknown> | undefined;
                        const output = typeof toolInfo?.output === "string" ? toolInfo.output : undefined;

                        let toolItemId = activeToolItems.get(stepIndex);
                        if (!toolItemId) {
                          toolItemId = RuntimeItemId.make(yield* randomUUIDv4);
                          activeToolItems.set(stepIndex, toolItemId);
                        }

                        const canonicalType =
                          toolName === "run_command"
                            ? ("command_execution" as const)
                            : ("dynamic_tool_call" as const);

                        let detail: string | undefined;
                        if (parameters) {
                          if (typeof parameters.CommandLine === "string") {
                            detail = parameters.CommandLine;
                          } else if (typeof parameters.AbsolutePath === "string") {
                            detail = parameters.AbsolutePath;
                          } else if (typeof parameters.TargetFile === "string") {
                            detail = parameters.TargetFile;
                          } else if (typeof parameters.query === "string") {
                            detail = parameters.query;
                          }
                        }

                        if (state === "ACTIVE") {
                          yield* completeMessageItem();
                          yield* offerRuntimeEvent({
                            type: "item.started",
                            ...(yield* makeEventStamp()),
                            threadId: input.threadId,
                            turnId,
                            itemId: toolItemId,
                            payload: {
                              itemType: canonicalType,
                              status: "inProgress" as const,
                              title: toolName,
                              ...(detail ? { detail } : {}),
                              data: { toolName, parameters },
                            },
                          });
                        } else if (state === "DONE") {
                          yield* offerRuntimeEvent({
                            type: "item.completed",
                            ...(yield* makeEventStamp()),
                            threadId: input.threadId,
                            turnId,
                            itemId: toolItemId,
                            payload: {
                              itemType: canonicalType,
                              status: "completed" as const,
                              title: toolName,
                              ...(detail ? { detail } : {}),
                              data: { toolName, parameters, output },
                            },
                          });
                          activeToolItems.delete(stepIndex);
                        }
                      } else if (stepType === "subagent") {
                        const subagentInfo = stepUpdate.subagent_info as Record<string, unknown> | undefined;
                        const subagentsList = subagentInfo?.subagents as Array<Record<string, unknown>> | undefined;
                        const primarySubagent = subagentsList?.[0];
                        const role = (primarySubagent?.role as string) || "Subagent";
                        const prompt = primarySubagent?.initial_prompt as string | undefined;
                        const title = `Subagent: ${role}`;

                        let toolItemId = activeToolItems.get(stepIndex);
                        if (!toolItemId) {
                          toolItemId = RuntimeItemId.make(yield* randomUUIDv4);
                          activeToolItems.set(stepIndex, toolItemId);
                        }

                        if (state === "ACTIVE") {
                          yield* completeMessageItem();
                          yield* offerRuntimeEvent({
                            type: "item.started",
                            ...(yield* makeEventStamp()),
                            threadId: input.threadId,
                            turnId,
                            itemId: toolItemId,
                            payload: {
                              itemType: "dynamic_tool_call" as const,
                              status: "inProgress" as const,
                              title,
                              ...(prompt ? { detail: prompt } : {}),
                              data: { subagentInfo },
                            },
                          });
                        } else if (state === "DONE") {
                          yield* offerRuntimeEvent({
                            type: "item.completed",
                            ...(yield* makeEventStamp()),
                            threadId: input.threadId,
                            turnId,
                            itemId: toolItemId,
                            payload: {
                              itemType: "dynamic_tool_call" as const,
                              status: "completed" as const,
                              title,
                              ...(prompt ? { detail: prompt } : {}),
                              data: { subagentInfo },
                            },
                          });
                          activeToolItems.delete(stepIndex);
                        }
                      } else if (stepType === "agent_response") {
                        if (typeof stepUpdate.text_delta === "string" && stepUpdate.text_delta) {
                          responseText = stepUpdate.text_delta;
                          streamedAnyDelta = true;
                        }
                        if (state === "DONE") {
                          shouldCompleteMessageAfterDelta = true;
                        }
                      }
                    } else if (parsed.result && !streamedAnyDelta) {
                      const resultObj = parsed.result as Record<string, unknown>;
                      if (typeof resultObj.response === "string" && resultObj.response) {
                        responseText = resultObj.response;
                      }
                      shouldCompleteMessageAfterDelta = true;
                    }
                  } catch (_e) {
                    responseText = line + "\n";
                  }
                } else {
                  responseText = line + "\n";
                }

                if (responseText) {
                  yield* ensureMessageItemStarted();
                  yield* offerRuntimeEvent({
                    type: "content.delta",
                    ...(yield* makeEventStamp()),
                    threadId: input.threadId,
                    turnId,
                    itemId: activeMessageItemId,
                    payload: {
                      streamKind: "assistant_text",
                      delta: responseText,
                    },
                  });
                }

                if (shouldCompleteMessageAfterDelta) {
                  yield* completeMessageItem();
                }
              }),
            );

            const exitCode = yield* child.exitCode;
            yield* Effect.logInfo(`[AntigravityAdapter] agy process exited with code ${exitCode}`);

            yield* completeMessageItem();

            ctx.session = {
              ...ctx.session,
              status: "ready",
              activeTurnId: undefined,
              updatedAt: yield* nowIso,
            };

            yield* Effect.logInfo(`[AntigravityAdapter] Emitting turn.completed`);
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
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logError(`[AntigravityAdapter] Process error cause:`, cause),
            ),
          ),
        ).pipe(Effect.forkIn(adapterScope));

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

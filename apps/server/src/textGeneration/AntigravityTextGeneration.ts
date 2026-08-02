/**
 * AntigravityTextGeneration — commit/PR/branch/title generation via `agy`.
 *
 * Uses agy's structured output mode: `agy -p <prompt> --output-format json
 * --json-schema <schema>` answers with a JSON envelope whose
 * `structured_output` field already matches the requested schema, so no
 * free-text extraction is needed.
 *
 * @module AntigravityTextGeneration
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { type AntigravitySettings, type ModelSelection } from "@t3tools/contracts";
import { TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  normalizeCliError,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "./TextGenerationUtils.ts";

const ANTIGRAVITY_TIMEOUT_MS = 180_000;

type TextGenerationOperation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";

/**
 * Envelope returned by `agy --output-format json`. Only `structured_output`
 * matters; the rest (conversation id, usage, echoed schema) is ignored.
 */
const AntigravityOutputEnvelope = Schema.Struct({
  structured_output: Schema.Unknown,
});

const encodeJsonString = Schema.encodeEffect(Schema.UnknownFromJsonString);
const decodeAntigravityOutputEnvelope = Schema.decodeEffect(
  Schema.fromJsonString(AntigravityOutputEnvelope),
);

export const makeAntigravityTextGeneration = Effect.fn("makeAntigravityTextGeneration")(function* (
  settings: AntigravitySettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const readStreamAsString = <E>(
    operation: string,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    stream.pipe(
      Stream.decodeText(),
      Stream.runFold(
        () => "",
        (acc, chunk) => acc + chunk,
      ),
      Effect.mapError((cause) =>
        normalizeCliError("agy", operation, cause, "Failed to collect process output"),
      ),
    );

  const runAntigravityJson = Effect.fn("runAntigravityJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation: TextGenerationOperation;
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    const jsonSchemaStr = yield* encodeJsonString(toJsonSchemaObject(outputSchemaJson)).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Failed to encode structured output schema.",
            cause,
          }),
      ),
    );

    const runAntigravityCommand = Effect.fn("runAntigravityJson.runAntigravityCommand")(
      function* () {
        const binary = settings.binaryPath || "agy";
        const spawnCommand = yield* resolveSpawnCommand(
          binary,
          [
            "-p",
            prompt,
            "--output-format",
            "json",
            "--json-schema",
            jsonSchemaStr,
            ...(modelSelection.model ? ["--model", modelSelection.model] : []),
            "--dangerously-skip-permissions",
          ],
          { env: environment },
        );
        const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          env: environment,
          cwd,
          shell: spawnCommand.shell,
          // The prompt is an argv entry, so stdin is unused — leaving it as an
          // open pipe makes agy wait on input that never arrives.
          stdin: "ignore",
        });

        const child = yield* commandSpawner
          .spawn(command)
          .pipe(
            Effect.mapError((cause) =>
              normalizeCliError("agy", operation, cause, "Failed to spawn Antigravity CLI process"),
            ),
          );

        const [stdout, stderr, exitCode] = yield* Effect.all(
          [
            readStreamAsString(operation, child.stdout),
            readStreamAsString(operation, child.stderr),
            child.exitCode.pipe(
              Effect.mapError((cause) =>
                normalizeCliError(
                  "agy",
                  operation,
                  cause,
                  "Failed to read Antigravity CLI exit code",
                ),
              ),
            ),
          ],
          { concurrency: "unbounded" },
        );

        if (exitCode !== 0) {
          const detail = stderr.trim() || stdout.trim();
          return yield* new TextGenerationError({
            operation,
            detail:
              detail.length > 0
                ? `Antigravity CLI command failed: ${detail}`
                : `Antigravity CLI command failed with code ${exitCode}.`,
          });
        }

        return stdout;
      },
    );

    const rawStdout = yield* runAntigravityCommand().pipe(
      Effect.scoped,
      Effect.timeoutOption(ANTIGRAVITY_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({ operation, detail: "Antigravity CLI request timed out." }),
            ),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
    );

    const envelope = yield* decodeAntigravityOutputEnvelope(rawStdout).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: "Antigravity CLI returned unexpected output format.",
              cause,
            }),
          ),
      }),
    );

    const decodeOutput = Schema.decodeEffect(outputSchemaJson);
    return yield* decodeOutput(envelope.structured_output).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: "Antigravity returned invalid structured output.",
              cause,
            }),
          ),
      }),
    );
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("AntigravityTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runAntigravityJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("AntigravityTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runAntigravityJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("AntigravityTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runAntigravityJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("AntigravityTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runAntigravityJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return { title: sanitizeThreadTitle(generated.title) };
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

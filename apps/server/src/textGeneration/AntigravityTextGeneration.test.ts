import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { AntigravitySettings, ProviderInstanceId } from "@t3tools/contracts";

import { makeAntigravityTextGeneration } from "./AntigravityTextGeneration.ts";

const decodeSettings = Schema.decodeSync(AntigravitySettings);
const encodeJsonString = Schema.encodeSync(Schema.UnknownFromJsonString);
const MODEL_SELECTION = {
  instanceId: ProviderInstanceId.make("antigravity"),
  model: "gemini-3.1-pro-high",
};

const makeFakeAgy = Effect.fn("makeFakeAgy")(function* (options: {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode?: number;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "antigravity-text-generation-",
  });
  const binaryPath = path.join(directory, "agy");
  const argsPath = path.join(directory, "args.txt");
  const stdinPath = path.join(directory, "stdin.txt");
  const script = [
    "#!/bin/sh",
    `printf '%s\\n' "$@" > ${encodeJsonString(argsPath)}`,
    `if IFS= read -r _line; then printf 'open' > ${encodeJsonString(stdinPath)}; else printf 'closed' > ${encodeJsonString(stdinPath)}; fi`,
    ...(options.stdout ? [`printf '%s' ${encodeJsonString(options.stdout)}`] : []),
    ...(options.stderr ? [`printf '%s' ${encodeJsonString(options.stderr)} >&2`] : []),
    `exit ${options.exitCode ?? 0}`,
  ].join("\n");
  yield* fileSystem.writeFileString(binaryPath, `${script}\n`);
  yield* fileSystem.chmod(binaryPath, 0o755);
  return { argsPath, binaryPath, stdinPath };
});

const readLines = Effect.fn("readLines")(function* (filePath: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  return (yield* fileSystem.readFileString(filePath)).split("\n").filter(Boolean);
});

const makeTextGeneration = (binaryPath: string) =>
  makeAntigravityTextGeneration(decodeSettings({ binaryPath }));

const commitInput = {
  cwd: process.cwd(),
  branch: "feat/antigravity-provider",
  stagedSummary: "M apps/server/src/provider/Layers/AntigravityAdapter.ts",
  stagedPatch: "diff --git a/AntigravityAdapter.ts b/AntigravityAdapter.ts",
  includeBranch: true,
  modelSelection: MODEL_SELECTION,
};

it.layer(NodeServices.layer)("AntigravityTextGeneration", (it) => {
  it.effect("decodes and sanitizes structured output with the required CLI flags", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({
        stdout:
          '{"structured_output":{"subject":"  Tighten Antigravity lifecycle.\\nignore this line","body":"  Settle every started turn.  ","branch":" Antigravity/Lifecycle Fix "}}',
      });
      const textGeneration = yield* makeTextGeneration(fake.binaryPath);

      const generated = yield* textGeneration.generateCommitMessage(commitInput);

      assert.deepEqual(generated, {
        subject: "Tighten Antigravity lifecycle",
        body: "Settle every started turn.",
        branch: "feature/antigravity/lifecycle-fix",
      });
      const args = yield* readLines(fake.argsPath);
      assert.equal(args[0], "-p");
      assert.include(args, "--output-format");
      assert.equal(args[args.indexOf("--output-format") + 1], "json");
      assert.include(args, "--json-schema");
      assert.isNotEmpty(args[args.indexOf("--json-schema") + 1]);
      assert.equal(args[args.indexOf("--model") + 1], MODEL_SELECTION.model);
      assert.include(args, "--dangerously-skip-permissions");
      const fileSystem = yield* FileSystem.FileSystem;
      assert.equal(yield* fileSystem.readFileString(fake.stdinPath), "closed");
    }),
  );

  it.effect("uses stdout as the useful error when a nonzero exit has no stderr", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({ stdout: "authentication required", exitCode: 7 });
      const textGeneration = yield* makeTextGeneration(fake.binaryPath);

      const error = yield* textGeneration.generateCommitMessage(commitInput).pipe(Effect.flip);

      assert.include(error.detail, "authentication required");
    }),
  );

  it.effect("prefers stderr for a nonzero-exit error", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({
        stdout: "less useful stdout",
        stderr: "quota exhausted",
        exitCode: 9,
      });
      const textGeneration = yield* makeTextGeneration(fake.binaryPath);

      const error = yield* textGeneration.generateCommitMessage(commitInput).pipe(Effect.flip);

      assert.include(error.detail, "quota exhausted");
      assert.notInclude(error.detail, "less useful stdout");
    }),
  );

  it.effect("rejects a malformed AGY output envelope", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({ stdout: "not json" });
      const textGeneration = yield* makeTextGeneration(fake.binaryPath);

      const error = yield* textGeneration.generateCommitMessage(commitInput).pipe(Effect.flip);

      assert.include(error.detail, "unexpected output format");
    }),
  );

  it.effect("rejects structured output that does not match the requested schema", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({
        stdout: '{"structured_output":{"subject":42,"body":"","branch":"valid"}}',
      });
      const textGeneration = yield* makeTextGeneration(fake.binaryPath);

      const error = yield* textGeneration.generateCommitMessage(commitInput).pipe(Effect.flip);

      assert.include(error.detail, "invalid structured output");
    }),
  );
});

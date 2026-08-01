import * as NodeAssert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { describe } from "vite-plus/test";

import { AntigravitySettings } from "@t3tools/contracts";

import {
  checkAntigravityProviderStatus,
  formatAntigravityModelName,
  parseAntigravityModels,
} from "./AntigravityProvider.ts";

const decodeSettings = Schema.decodeSync(AntigravitySettings);
const encodeJsonString = Schema.encodeSync(Schema.UnknownFromJsonString);

const makeFakeAgy = Effect.fn("makeFakeAgyProviderProbe")(function* (options?: {
  readonly versionStdout?: string;
  readonly versionStderr?: string;
  readonly versionExitCode?: number;
  readonly modelsStdout?: string;
  readonly modelsStderr?: string;
  readonly modelsExitCode?: number;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "antigravity-provider-",
  });
  const binaryPath = path.join(directory, "agy");
  const stdinPath = path.join(directory, "models-stdin.txt");
  const script = [
    "#!/bin/sh",
    'if [ "$1" = "--version" ]; then',
    ...(options?.versionStdout ? [`  printf '%b' ${encodeJsonString(options.versionStdout)}`] : []),
    ...(options?.versionStderr
      ? [`  printf '%b' ${encodeJsonString(options.versionStderr)} >&2`]
      : []),
    `  exit ${options?.versionExitCode ?? 0}`,
    "fi",
    'if [ "$1" = "models" ]; then',
    `  if IFS= read -r _line; then printf 'open' > ${encodeJsonString(stdinPath)}; else printf 'closed' > ${encodeJsonString(stdinPath)}; fi`,
    ...(options?.modelsStdout ? [`  printf '%b' ${encodeJsonString(options.modelsStdout)}`] : []),
    ...(options?.modelsStderr
      ? [`  printf '%b' ${encodeJsonString(options.modelsStderr)} >&2`]
      : []),
    `  exit ${options?.modelsExitCode ?? 0}`,
    "fi",
    "exit 64",
  ].join("\n");
  yield* fileSystem.writeFileString(binaryPath, `${script}\n`);
  yield* fileSystem.chmod(binaryPath, 0o755);
  return { binaryPath, stdinPath };
});

describe("parseAntigravityModels", () => {
  it("reads one slug per line from `agy models`", () => {
    const models = parseAntigravityModels(
      ["gemini-3.6-flash-high", "gemini-3.1-pro-low", "claude-sonnet-4-6", ""].join("\n"),
    );

    NodeAssert.deepEqual(
      models.map((model) => model.slug),
      ["gemini-3.6-flash-high", "gemini-3.1-pro-low", "claude-sonnet-4-6"],
    );
    NodeAssert.equal(models[0]!.isCustom, false);
  });

  // A decorative header or bullet prefix must not become a bogus model entry.
  it("drops headers, bullets, and duplicates", () => {
    const models = parseAntigravityModels(
      ["Available models:", "- gemini-3.1-pro-high", "gemini-3.1-pro-high", "some prose line"].join(
        "\n",
      ),
    );

    NodeAssert.deepEqual(
      models.map((model) => model.slug),
      ["gemini-3.1-pro-high"],
    );
  });
});

describe("formatAntigravityModelName", () => {
  it("splits the trailing effort segment out of the slug", () => {
    NodeAssert.equal(formatAntigravityModelName("gemini-3.1-pro-high"), "Gemini 3.1 Pro (high)");
    NodeAssert.equal(
      formatAntigravityModelName("gemini-3.6-flash-medium"),
      "Gemini 3.6 Flash (medium)",
    );
    NodeAssert.equal(formatAntigravityModelName("claude-sonnet-4-6"), "Claude Sonnet 4 6");
  });
});

it.layer(NodeServices.layer)("checkAntigravityProviderStatus", (it) => {
  it.effect("discovers the installed version and live model catalog with stdin closed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({
        versionStdout: "agy 1.1.9\n",
        modelsStdout: "gemini-3.1-pro-high\nclaude-sonnet-4-6\n",
      });

      const snapshot = yield* checkAntigravityProviderStatus(
        decodeSettings({ enabled: true, binaryPath: fake.binaryPath }),
        process.env,
        process.cwd(),
      );

      NodeAssert.equal(snapshot.installed, true);
      NodeAssert.equal(snapshot.version, "1.1.9");
      NodeAssert.equal(snapshot.status, "ready");
      NodeAssert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["gemini-3.1-pro-high", "claude-sonnet-4-6"],
      );
      NodeAssert.equal(snapshot.auth.status, "authenticated");
      const fileSystem = yield* FileSystem.FileSystem;
      NodeAssert.equal(yield* fileSystem.readFileString(fake.stdinPath), "closed");
    }),
  );

  it.effect("reports a missing binary", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkAntigravityProviderStatus(
        decodeSettings({
          enabled: true,
          binaryPath: "/definitely/not/installed/agy-provider-probe",
        }),
      );

      NodeAssert.equal(snapshot.installed, false);
      NodeAssert.equal(snapshot.status, "error");
      NodeAssert.match(snapshot.message ?? "", /not installed|not on PATH/);
      NodeAssert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["gemini-3.1-pro-high"],
      );
    }),
  );

  it.effect("reports a nonzero version probe without exposing its output", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({
        versionStderr: "sensitive installation failure",
        versionExitCode: 2,
      });

      const snapshot = yield* checkAntigravityProviderStatus(
        decodeSettings({ enabled: true, binaryPath: fake.binaryPath }),
      );

      NodeAssert.equal(snapshot.installed, true);
      NodeAssert.equal(snapshot.status, "error");
      NodeAssert.equal(snapshot.message, "Antigravity CLI is installed but failed to run.");
      NodeAssert.doesNotMatch(snapshot.message ?? "", /sensitive/);
    }),
  );

  it.effect("reports a failed model command as unauthenticated and keeps fallback models", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({
        versionStdout: "agy 1.1.9\n",
        modelsStderr: "not signed in",
        modelsExitCode: 1,
      });

      const snapshot = yield* checkAntigravityProviderStatus(
        decodeSettings({ enabled: true, binaryPath: fake.binaryPath }),
      );

      NodeAssert.equal(snapshot.status, "warning");
      NodeAssert.equal(snapshot.auth.status, "unauthenticated");
      NodeAssert.match(snapshot.message ?? "", /not signed in/);
      NodeAssert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["gemini-3.1-pro-high"],
      );
    }),
  );

  it.effect("falls back when model discovery returns no model slugs", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeAgy({
        versionStdout: "agy 1.1.9\n",
        modelsStdout: "Available models:\n",
      });

      const snapshot = yield* checkAntigravityProviderStatus(
        decodeSettings({ enabled: true, binaryPath: fake.binaryPath }),
      );

      NodeAssert.equal(snapshot.status, "ready");
      NodeAssert.equal(snapshot.auth.status, "unknown");
      NodeAssert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["gemini-3.1-pro-high"],
      );
    }),
  );
});

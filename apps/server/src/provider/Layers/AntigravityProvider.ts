/**
 * AntigravityProvider — snapshot and health probe for the Antigravity CLI (`agy`).
 *
 * `agy` has no protocol-level handshake we can reuse for discovery, so the
 * probe shells out twice: `agy --version` for installed/version, then
 * `agy models` for the catalog. The model list doubles as the auth signal —
 * `agy models` only answers once the CLI holds credentials.
 *
 * @module AntigravityProvider
 */
import {
  type AntigravitySettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import { discoverAntigravitySkills } from "../Drivers/AntigravitySkills.ts";

/**
 * `showInteractionModeToggle` is on because `agy --mode plan` gives us a real
 * plan mode. Model changes take effect on the next turn (the slug is a
 * per-invocation flag), so no new thread is required.
 */
const ANTIGRAVITY_PRESENTATION = {
  displayName: "Antigravity",
  badgeLabel: "Early Access",
  showInteractionModeToggle: true,
  requiresNewThreadForModelChange: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const MODEL_PROBE_TIMEOUT_MS = 10_000;

/**
 * Catalog used before the first successful `agy models` call and whenever the
 * live probe fails. Kept deliberately short — it only has to make the model
 * picker usable, not mirror the full remote catalog.
 */
const ANTIGRAVITY_FALLBACK_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "gemini-3.1-pro-high",
    name: "Gemini 3.1 Pro (high)",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

const EFFORT_SUFFIXES = ["high", "medium", "low"] as const;

const capitalize = (word: string): string =>
  word.length === 0 ? word : word[0]!.toUpperCase() + word.slice(1);

/**
 * Turn an `agy` model slug into something readable in the picker.
 * `gemini-3.1-pro-high` becomes `Gemini 3.1 Pro (high)`; slugs that do not
 * carry a trailing effort segment are just title-cased.
 */
export function formatAntigravityModelName(slug: string): string {
  const segments = slug.split("-");
  const last = segments.at(-1);
  const effort = EFFORT_SUFFIXES.find((candidate) => candidate === last);
  const nameSegments = effort ? segments.slice(0, -1) : segments;
  const base = nameSegments
    .map((segment) => (/^\d/.test(segment) ? segment : capitalize(segment)))
    .join(" ");
  return effort ? `${base} (${effort})` : base;
}

/**
 * Parse `agy models` output — one slug per line. Blank lines and any
 * decorative output (a leading "Available models:" header, bullets) are
 * dropped so a cosmetic CLI change cannot poison the catalog.
 */
export function parseAntigravityModels(stdout: string): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const models: Array<ServerProviderModel> = [];
  for (const rawLine of stdout.split("\n")) {
    const slug = rawLine.trim().replace(/^[-*]\s*/, "");
    if (slug.length === 0 || slug.endsWith(":") || slug.includes(" ")) continue;
    if (seen.has(slug)) continue;
    seen.add(slug);
    models.push({
      slug,
      name: formatAntigravityModelName(slug),
      isCustom: false,
      capabilities: EMPTY_CAPABILITIES,
    });
  }
  return models;
}

function antigravityModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = ANTIGRAVITY_FALLBACK_MODELS,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

const runAntigravityCommand = (
  settings: AntigravitySettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const command = settings.binaryPath || "agy";
    const spawnCommand = yield* resolveSpawnCommand(command, [...args], { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
        // `agy models` blocks forever on a stdin pipe it is never going to be
        // written to; handing it /dev/null makes it read EOF and answer.
        stdin: "ignore",
      }),
    );
  });

export function buildInitialAntigravityProviderSnapshot(
  settings: AntigravitySettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = antigravityModelsFromSettings(settings.customModels);

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Antigravity is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: ANTIGRAVITY_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Antigravity CLI availability...",
      },
    });
  });
}

export const checkAntigravityProviderStatus = Effect.fn("checkAntigravityProviderStatus")(
  function* (
    settings: AntigravitySettings,
    environment: NodeJS.ProcessEnv = process.env,
    cwd?: string,
  ): Effect.fn.Return<
    ServerProviderDraft,
    never,
    ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
  > {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const fallbackModels = antigravityModelsFromSettings(settings.customModels);

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: false,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Antigravity is disabled in T3 Code settings.",
        },
      });
    }

    // Skills live purely on disk, so they are discovered independently of CLI
    // health and attached to every snapshot below — a failed model probe must
    // not empty the `$` picker.
    const skills = yield* discoverAntigravitySkills(cwd);

    const versionResult = yield* runAntigravityCommand(settings, ["--version"], environment).pipe(
      Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
      Effect.result,
    );

    if (Result.isFailure(versionResult)) {
      const error = versionResult.failure;
      yield* Effect.logWarning("Antigravity CLI health check failed.", {
        errorTag: error._tag,
      });
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        skills,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "Antigravity CLI (`agy`) is not installed or not on PATH."
            : "Failed to execute Antigravity CLI health check.",
        },
      });
    }

    if (Option.isNone(versionResult.success)) {
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        skills,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "Antigravity CLI is installed but timed out while running `agy --version`.",
        },
      });
    }

    const versionOutput = versionResult.success.value;
    const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
    if (versionOutput.code !== 0) {
      yield* Effect.logWarning("Antigravity CLI version probe exited with a non-zero status.", {
        exitCode: versionOutput.code,
        stdoutLength: versionOutput.stdout.length,
        stderrLength: versionOutput.stderr.length,
      });
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        skills,
        probe: {
          installed: true,
          version,
          status: "error",
          auth: { status: "unknown" },
          message: "Antigravity CLI is installed but failed to run.",
        },
      });
    }

    const modelsResult = yield* runAntigravityCommand(settings, ["models"], environment).pipe(
      Effect.timeoutOption(MODEL_PROBE_TIMEOUT_MS),
      Effect.result,
    );

    // A failed catalog read is not fatal — the CLI is installed and the
    // fallback catalog still lets a turn run — but it usually means the CLI
    // is not signed in, so the snapshot says so instead of claiming ready.
    if (Result.isFailure(modelsResult) || Option.isNone(modelsResult.success)) {
      yield* Effect.logWarning("Antigravity model discovery failed.");
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        skills,
        probe: {
          installed: true,
          version,
          status: "warning",
          auth: { status: "unknown" },
          message: "Antigravity CLI is installed but `agy models` did not answer.",
        },
      });
    }

    const modelsOutput = modelsResult.success.value;
    if (modelsOutput.code !== 0) {
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        skills,
        probe: {
          installed: true,
          version,
          status: "warning",
          auth: { status: "unauthenticated" },
          message: "Antigravity CLI is installed but not signed in. Run `agy` to authenticate.",
        },
      });
    }

    const discoveredModels = parseAntigravityModels(modelsOutput.stdout);
    const models =
      discoveredModels.length > 0
        ? antigravityModelsFromSettings(settings.customModels, discoveredModels)
        : fallbackModels;

    return buildServerProvider({
      presentation: ANTIGRAVITY_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models,
      skills,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: discoveredModels.length > 0 ? "authenticated" : "unknown" },
      },
    });
  },
);

export const enrichAntigravitySnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Antigravity version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};

import {
  type AntigravitySettings,
  type ModelCapabilities,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { createModelCapabilities } from "@t3tools/shared/model";
import { discoverAntigravitySkills } from "../Drivers/AntigravitySkills.ts";
import {
  buildServerProvider,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const ANTIGRAVITY_PRESENTATION = {
  displayName: "Antigravity (AGY)",
  showInteractionModeToggle: false,
} as const;

const DEFAULT_ANTIGRAVITY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const DEFAULT_ANTIGRAVITY_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "gemini-3.6-flash",
    name: "Gemini 3.6 Flash",
    isCustom: false,
    capabilities: DEFAULT_ANTIGRAVITY_CAPABILITIES,
  },
  {
    slug: "gemini-3.6-pro",
    name: "Gemini 3.6 Pro",
    isCustom: false,
    capabilities: DEFAULT_ANTIGRAVITY_CAPABILITIES,
  },
  {
    slug: "claude-sonnet-3-5",
    name: "Claude 3.5 Sonnet",
    isCustom: false,
    capabilities: DEFAULT_ANTIGRAVITY_CAPABILITIES,
  },
];

export const makePendingAntigravityProvider = (
  settings: AntigravitySettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = providerModelsFromSettings(
      DEFAULT_ANTIGRAVITY_MODELS,
      settings.customModels,
      DEFAULT_ANTIGRAVITY_CAPABILITIES,
    );

    return buildServerProvider({
      presentation: ANTIGRAVITY_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models,
      skills: [],
      probe: {
        installed: false,
        version: null,
        status: settings.enabled ? "warning" : "warning",
        auth: { status: "unknown" },
        message: settings.enabled
          ? "Antigravity provider status has not been checked in this session yet."
          : "Antigravity is disabled in T3 Code settings.",
      },
    });
  });

export const checkAntigravityProviderStatus = (
  settings: AntigravitySettings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): Effect.Effect<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const binary = settings.binaryPath || "agy";
    const models = providerModelsFromSettings(
      DEFAULT_ANTIGRAVITY_MODELS,
      settings.customModels,
      DEFAULT_ANTIGRAVITY_CAPABILITIES,
    );

    const skills = yield* discoverAntigravitySkills(settings, cwd, environment).pipe(
      Effect.orElseSucceed(() => []),
    );

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        skills,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Antigravity is disabled in T3 Code settings.",
        },
      });
    }

    const command = ChildProcess.make(binary, ["--version"]);
    const versionExit = yield* Effect.exit(spawnAndCollect(binary, command));

    if (versionExit._tag === "Failure") {
      return buildServerProvider({
        presentation: ANTIGRAVITY_PRESENTATION,
        enabled: true,
        checkedAt,
        models,
        skills,
        probe: {
          installed: false,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "Antigravity CLI (`agy`) is not installed or not on PATH.",
        },
      });
    }

    const version = parseGenericCliVersion(versionExit.value.stdout) ?? "1.0.0";

    return buildServerProvider({
      presentation: ANTIGRAVITY_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      skills,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: {
          status: "authenticated",
          type: "antigravity",
        },
        message: "Antigravity CLI (agy) is installed and ready.",
      },
    });
  });


/**
 * AntigravitySkills — filesystem discovery of Antigravity (AGY) skills for the `$` picker.
 *
 * Antigravity loads skills from:
 * - `<config dir>/antigravity-cli/builtin/skills` or `<config dir>/builtin/skills` (app / system scope)
 * - `<config dir>/config/skills`, `<config dir>/skills`, or `~/.agents/skills` (user scope)
 * - `<cwd>/.gemini/skills`, `<cwd>/.agents/skills`, `<cwd>/.claude/skills`, or `<cwd>/.codex/skills` (project scope)
 *
 * Each skill resides in a directory with a `SKILL.md` carrying YAML frontmatter.
 *
 * @module provider/Drivers/AntigravitySkills
 */
import * as NodeOS from "node:os";

import type { AntigravitySettings, ServerProviderSkill } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { parse as parseYamlDocument } from "yaml";

import { expandHomePath } from "../../pathExpansion.ts";

type AntigravitySkillScope = "user" | "project" | "system";

const FRONTMATTER_PATTERN = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/;

type SkillFrontmatter =
  | { readonly kind: "missing" }
  | { readonly kind: "malformed" }
  | { readonly kind: "parsed"; readonly name?: string; readonly description?: string };

function parseSkillFrontmatter(contents: string): SkillFrontmatter {
  const match = FRONTMATTER_PATTERN.exec(contents);
  if (!match) {
    return { kind: "missing" };
  }

  let parsed: unknown;
  try {
    parsed = parseYamlDocument(match[1] ?? "");
  } catch {
    return { kind: "malformed" };
  }
  if (typeof parsed !== "object" || parsed === null) {
    return { kind: "malformed" };
  }

  const record = parsed as Record<string, unknown>;
  const name = typeof record.name === "string" ? record.name.trim() : "";
  const description = typeof record.description === "string" ? record.description.trim() : "";
  return {
    kind: "parsed",
    ...(name ? { name } : {}),
    ...(description ? { description } : {}),
  };
}

/**
 * Resolve the Antigravity config directory.
 */
const resolveAntigravityConfigDirPath = Effect.fn("resolveAntigravityConfigDirPath")(function* (
  config: Partial<AntigravitySettings & { homePath?: string }>,
  environment: NodeJS.ProcessEnv,
  cwd?: string,
): Effect.fn.Return<string, never, Path.Path> {
  const path = yield* Path.Path;
  const homePath = (config as { homePath?: string }).homePath?.trim() ?? "";
  if (homePath.length > 0) {
    return path.resolve(expandHomePath(homePath));
  }
  const environmentConfigDir =
    environment.ANTIGRAVITY_CONFIG_DIR?.trim() ?? environment.GEMINI_CONFIG_DIR?.trim() ?? "";
  if (environmentConfigDir.length > 0) {
    return cwd ? path.resolve(cwd, environmentConfigDir) : path.resolve(environmentConfigDir);
  }
  return path.join(NodeOS.homedir(), ".gemini");
});

/**
 * Enumerate Antigravity skills from system, user, and project roots.
 * On name collisions, project-scoped skills win over user-scoped skills,
 * which win over system-scoped skills.
 */
export const discoverAntigravitySkills = Effect.fn("discoverAntigravitySkills")(function* (
  config: Partial<AntigravitySettings & { homePath?: string }>,
  cwd?: string,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<ReadonlyArray<ServerProviderSkill>, never, FileSystem.FileSystem | Path.Path> {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const configDirPath = yield* resolveAntigravityConfigDirPath(config, environment ?? process.env, cwd);
  const userHome = NodeOS.homedir();

  const roots: ReadonlyArray<{ directory: string; scope: AntigravitySkillScope }> = [
    // Builtin / system scope
    { directory: path.join(configDirPath, "antigravity-cli", "builtin", "skills"), scope: "system" },
    { directory: path.join(configDirPath, "builtin", "skills"), scope: "system" },
    // User scope
    { directory: path.join(configDirPath, "config", "skills"), scope: "user" },
    { directory: path.join(configDirPath, "skills"), scope: "user" },
    { directory: path.join(userHome, ".agents", "skills"), scope: "user" },
    // Project scope
    ...(cwd
      ? [
          { directory: path.join(cwd, ".gemini", "skills"), scope: "project" as const },
          { directory: path.join(cwd, ".agents", "skills"), scope: "project" as const },
          { directory: path.join(cwd, ".claude", "skills"), scope: "project" as const },
          { directory: path.join(cwd, ".codex", "skills"), scope: "project" as const },
        ]
      : []),
  ];

  const skillsByName = new Map<string, ServerProviderSkill>();

  for (const root of roots) {
    const entries = yield* fileSystem
      .readDirectory(root.directory)
      .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));

    for (const entry of [...entries].sort()) {
      const skillPath = path.join(root.directory, entry, "SKILL.md");
      const contents = yield* fileSystem
        .readFileString(skillPath)
        .pipe(Effect.orElseSucceed(() => undefined));
      if (contents === undefined) {
        continue;
      }

      const frontmatter = parseSkillFrontmatter(contents);
      if (frontmatter.kind === "malformed") {
        continue;
      }

      const name = (frontmatter.kind === "parsed" ? frontmatter.name : undefined) ?? entry.trim();
      if (!name) {
        continue;
      }

      const existing = skillsByName.get(name);
      if (
        !existing ||
        root.scope === "project" ||
        (root.scope === "user" && existing.scope === "system")
      ) {
        skillsByName.set(name, {
          name,
          path: skillPath,
          enabled: true,
          scope: root.scope,
          ...(frontmatter.kind === "parsed" && frontmatter.description
            ? { description: frontmatter.description }
            : {}),
        });
      }
    }
  }

  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
});

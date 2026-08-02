/**
 * AntigravitySkills — filesystem discovery of Antigravity skills for the `$` picker.
 *
 * `agy` has no command that lists skills, so the provider snapshot scans the
 * same locations the CLI loads them from. Its own help text names the two
 * templates: `{appDataDir}/skills/{skill_name}/SKILL.md` for what the CLI
 * calls "Global skills", and `{workspace}/.agents/skills/{skill_name}/SKILL.md`
 * for "Workspace skills". The layout — one directory per skill holding a
 * `SKILL.md` with YAML frontmatter — matches Claude Code's, so the scan itself
 * is shared.
 *
 * Two global roots are checked because the CLI keeps user configuration under
 * `~/.gemini/config` and application data under `~/.gemini/antigravity-cli`,
 * and grants itself read access to a `skills` directory beneath each.
 *
 * @module provider/Drivers/AntigravitySkills
 */
import * as NodeOS from "node:os";

import type { ServerProviderSkill } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { discoverSkillsFromRoots, type SkillRoot } from "./skillDiscovery.ts";

/** Directory the CLI stores its configuration and application data under. */
const ANTIGRAVITY_HOME_DIR_NAME = ".gemini";
const ANTIGRAVITY_GLOBAL_SKILL_PARENTS = ["config", "antigravity-cli"] as const;
/** Workspace-scoped skills live here, per agy's `{workspace}/.agents/skills` template. */
const ANTIGRAVITY_WORKSPACE_SKILL_SEGMENTS = [".agents", "skills"] as const;

/**
 * Enumerate Antigravity skills from the CLI's global roots and the workspace.
 * A workspace skill wins a name collision, matching the CLI's
 * most-specific-wins resolution.
 */
export const discoverAntigravitySkills = Effect.fn("discoverAntigravitySkills")(function* (
  cwd?: string,
): Effect.fn.Return<ReadonlyArray<ServerProviderSkill>, never, FileSystem.FileSystem | Path.Path> {
  const path = yield* Path.Path;
  const homeDir = path.join(NodeOS.homedir(), ANTIGRAVITY_HOME_DIR_NAME);

  const roots: ReadonlyArray<SkillRoot> = [
    ...ANTIGRAVITY_GLOBAL_SKILL_PARENTS.map((parent) => ({
      directory: path.join(homeDir, parent, "skills"),
      scope: "user",
    })),
    ...(cwd
      ? [
          {
            directory: path.join(cwd, ...ANTIGRAVITY_WORKSPACE_SKILL_SEGMENTS),
            scope: "project",
          },
        ]
      : []),
  ];

  return yield* discoverSkillsFromRoots(roots);
});

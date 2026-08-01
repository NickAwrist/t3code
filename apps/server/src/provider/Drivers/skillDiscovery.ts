/**
 * skillDiscovery — shared filesystem scan for `SKILL.md` skill directories.
 *
 * Claude Code and Antigravity both store skills as one directory per skill
 * containing a `SKILL.md` with YAML frontmatter (`name`, `description`), and
 * both resolve name collisions most-specific-wins. Only the root locations
 * differ, so each driver resolves its own roots and shares this scan.
 *
 * @module provider/Drivers/skillDiscovery
 */
import type { ServerProviderSkill } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { parse as parseYamlDocument } from "yaml";

const FRONTMATTER_PATTERN = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/;

export type SkillFrontmatter =
  | { readonly kind: "missing" }
  | { readonly kind: "malformed" }
  | { readonly kind: "parsed"; readonly name?: string; readonly description?: string };

export function parseSkillFrontmatter(contents: string): SkillFrontmatter {
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
 * One directory to scan. `scope` is carried through onto every skill found
 * beneath it so the picker can group entries.
 */
export interface SkillRoot {
  readonly directory: string;
  readonly scope: string;
}

/**
 * Scan each root for `<entry>/SKILL.md`.
 *
 * Discovery is best-effort: unreadable roots and malformed skill entries are
 * skipped so one broken skill never degrades the provider snapshot. Roots are
 * scanned in order and later roots overwrite earlier ones on a name collision,
 * so callers list least-specific first.
 */
export const discoverSkillsFromRoots = Effect.fn("discoverSkillsFromRoots")(function* (
  roots: ReadonlyArray<SkillRoot>,
): Effect.fn.Return<ReadonlyArray<ServerProviderSkill>, never, FileSystem.FileSystem | Path.Path> {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

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
      // Malformed frontmatter means the skill won't load in the provider's own
      // runtime either — skip it rather than surfacing a broken entry under
      // its directory name.
      if (frontmatter.kind === "malformed") {
        continue;
      }

      const name = (frontmatter.kind === "parsed" ? frontmatter.name : undefined) ?? entry.trim();
      if (!name) {
        continue;
      }

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

  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
});

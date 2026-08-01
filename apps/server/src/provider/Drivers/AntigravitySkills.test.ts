// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { discoverAntigravitySkills } from "./AntigravitySkills.ts";

async function writeSkill(root: string, directoryName: string, contents: string): Promise<string> {
  const skillDir = NodePath.join(root, directoryName);
  await NodeFSP.mkdir(skillDir, { recursive: true });
  const skillPath = NodePath.join(skillDir, "SKILL.md");
  await NodeFSP.writeFile(skillPath, contents, "utf8");
  return skillPath;
}

function frontmatter(name: string, description: string): string {
  return `---\nname: ${name}\ndescription: ${description}\n---\n\n# ${name}\n`;
}

it.layer(NodeServices.layer)("discoverAntigravitySkills", (it) => {
  it.effect("finds workspace skills under .agents/skills", () =>
    Effect.gen(function* () {
      const workspace = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "agy-skills-ws-")),
      );
      const skillPath = yield* Effect.promise(() =>
        writeSkill(
          NodePath.join(workspace, ".agents", "skills"),
          "deploy-runbook",
          frontmatter("deploy-runbook", "Steps to ship a release."),
        ),
      );

      const skills = yield* discoverAntigravitySkills(workspace);
      const found = skills.find((skill) => skill.name === "deploy-runbook");

      assert.isDefined(found);
      assert.equal(found?.path, skillPath);
      assert.equal(found?.scope, "project");
      assert.equal(found?.description, "Steps to ship a release.");
      assert.isTrue(found?.enabled);
    }),
  );

  it.effect("falls back to the directory name and skips malformed frontmatter", () =>
    Effect.gen(function* () {
      const workspace = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "agy-skills-mixed-")),
      );
      const root = NodePath.join(workspace, ".agents", "skills");
      yield* Effect.promise(() => writeSkill(root, "no-frontmatter", "# Just a heading\n"));
      yield* Effect.promise(() =>
        writeSkill(root, "broken", "---\nname: [unclosed\n---\n\n# Broken\n"),
      );

      const skills = yield* discoverAntigravitySkills(workspace);

      // Filtered to this workspace: the machine's real global roots may hold
      // skills of their own, which are not what this case is about.
      assert.deepEqual(
        skills.filter((skill) => skill.path.startsWith(workspace)).map((skill) => skill.name),
        ["no-frontmatter"],
      );
    }),
  );

  it.effect("returns an empty list when the workspace has no skills", () =>
    Effect.gen(function* () {
      const workspace = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "agy-skills-empty-")),
      );

      const skills = yield* discoverAntigravitySkills(workspace);

      // The user's real global roots may hold skills, so only assert that
      // nothing was picked up from this workspace.
      assert.isFalse(skills.some((skill) => skill.path.startsWith(workspace)));
    }),
  );

  it.effect("survives a workspace path that does not exist", () =>
    Effect.gen(function* () {
      const skills = yield* discoverAntigravitySkills("/nonexistent/agy/workspace");
      assert.isFalse(skills.some((skill) => skill.path.startsWith("/nonexistent")));
    }),
  );
});

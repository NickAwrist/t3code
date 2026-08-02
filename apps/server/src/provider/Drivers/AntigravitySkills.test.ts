import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { discoverAntigravitySkills } from "./AntigravitySkills.ts";

const writeSkill = Effect.fn("writeAntigravitySkill")(function* (
  root: string,
  directoryName: string,
  contents: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const skillDir = path.join(root, directoryName);
  yield* fileSystem.makeDirectory(skillDir, { recursive: true });
  const skillPath = path.join(skillDir, "SKILL.md");
  yield* fileSystem.writeFileString(skillPath, contents);
  return skillPath;
});

function frontmatter(name: string, description: string): string {
  return `---\nname: ${name}\ndescription: ${description}\n---\n\n# ${name}\n`;
}

it.layer(NodeServices.layer)("discoverAntigravitySkills", (it) => {
  it.effect("finds workspace skills under .agents/skills", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "agy-skills-ws-" });
      const skillPath = yield* writeSkill(
        path.join(workspace, ".agents", "skills"),
        "deploy-runbook",
        frontmatter("deploy-runbook", "Steps to ship a release."),
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
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "agy-skills-mixed-",
      });
      const root = path.join(workspace, ".agents", "skills");
      yield* writeSkill(root, "no-frontmatter", "# Just a heading\n");
      yield* writeSkill(root, "broken", "---\nname: [unclosed\n---\n\n# Broken\n");

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
      const fileSystem = yield* FileSystem.FileSystem;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "agy-skills-empty-" });

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

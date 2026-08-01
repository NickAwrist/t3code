import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { discoverAntigravitySkills } from "./AntigravitySkills.ts";

const writeSkill = Effect.fn(function* (
  skillsDir: string,
  directoryName: string,
  contents: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const skillDir = path.join(skillsDir, directoryName);
  yield* fs.makeDirectory(skillDir, { recursive: true });
  yield* fs.writeFileString(path.join(skillDir, "SKILL.md"), contents);
});

it.layer(NodeServices.layer)("discoverAntigravitySkills", (it) => {
  it.effect("discovers builtin, user, and project skills with frontmatter metadata", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-skills-" });
      const configDir = path.join(tempDir, "gemini-home");
      const workspace = path.join(tempDir, "workspace");

      yield* writeSkill(
        path.join(configDir, "antigravity-cli", "builtin", "skills"),
        "antigravity-guide",
        [
          "---",
          "name: antigravity-guide",
          "description: Official AGY guide.",
          "---",
          "",
          "# Guide",
        ].join("\n"),
      );

      yield* writeSkill(
        path.join(configDir, "config", "skills"),
        "pig-latin",
        ["---", "name: pig-latin", "description: Pig Latin translator.", "---"].join("\n"),
      );

      yield* writeSkill(
        path.join(workspace, ".gemini", "skills"),
        "project-tool",
        ["---", "name: project-tool", "description: Project specific tool.", "---"].join("\n"),
      );

      const skills = yield* discoverAntigravitySkills({ homePath: configDir }, workspace);

      assert.deepEqual(skills, [
        {
          name: "antigravity-guide",
          path: path.join(configDir, "antigravity-cli", "builtin", "skills", "antigravity-guide", "SKILL.md"),
          enabled: true,
          scope: "system",
          description: "Official AGY guide.",
        },
        {
          name: "pig-latin",
          path: path.join(configDir, "config", "skills", "pig-latin", "SKILL.md"),
          enabled: true,
          scope: "user",
          description: "Pig Latin translator.",
        },
        {
          name: "project-tool",
          path: path.join(workspace, ".gemini", "skills", "project-tool", "SKILL.md"),
          enabled: true,
          scope: "project",
          description: "Project specific tool.",
        },
      ]);
    }),
  );

  it.effect("prefers project skills over user and system skills on name collisions", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-skills-" });
      const configDir = path.join(tempDir, "gemini-home");
      const workspace = path.join(tempDir, "workspace");

      yield* writeSkill(
        path.join(configDir, "antigravity-cli", "builtin", "skills"),
        "guide",
        ["---", "name: guide", "description: System guide.", "---"].join("\n"),
      );
      yield* writeSkill(
        path.join(workspace, ".gemini", "skills"),
        "guide",
        ["---", "name: guide", "description: Project guide.", "---"].join("\n"),
      );

      const skills = yield* discoverAntigravitySkills({ homePath: configDir }, workspace);

      assert.equal(skills.length, 1);
      assert.equal(skills[0]?.scope, "project");
      assert.equal(skills[0]?.description, "Project guide.");
    }),
  );
});

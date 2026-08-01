import * as NodeAssert from "node:assert/strict";

import { describe, it } from "vite-plus/test";

import { formatAntigravityModelName, parseAntigravityModels } from "./AntigravityProvider.ts";

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

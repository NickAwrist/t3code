import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  ClipboardApiUnavailableError,
  ClipboardWriteError,
  writeTextToClipboard,
} from "./useCopyToClipboard";

describe("writeTextToClipboard", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("reports unavailable clipboard support with structural context", async () => {
    vi.stubGlobal("window", {});
    vi.stubGlobal("navigator", {});

    const error = await writeTextToClipboard("plan contents", "plan").then(
      () => undefined,
      (cause: unknown) => cause,
    );

    expect(error).toBeInstanceOf(ClipboardApiUnavailableError);
    expect(error).toMatchObject({
      target: "plan",
    });
    expect((error as Error).message).not.toContain("plan contents");
  });

  it("preserves the exact clipboard failure without exposing copied contents", async () => {
    const cause = new Error("browser clipboard failure");
    const writeText = vi.fn().mockRejectedValue(cause);
    vi.stubGlobal("window", {});
    vi.stubGlobal("navigator", { clipboard: { writeText } });

    const error = await writeTextToClipboard("secret clipboard contents", "error-message").then(
      () => undefined,
      (failure: unknown) => failure,
    );

    expect(writeText).toHaveBeenCalledWith("secret clipboard contents");
    expect(error).toBeInstanceOf(ClipboardWriteError);
    expect(error).toMatchObject({
      target: "error-message",
      cause,
    });
    expect((error as Error).message).not.toContain("secret clipboard contents");
  });

  it("keeps empty values as a no-op when clipboard support is available", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("window", {});
    vi.stubGlobal("navigator", { clipboard: { writeText } });

    await expect(writeTextToClipboard("", "plan")).resolves.toBe(false);
    expect(writeText).not.toHaveBeenCalled();
  });

  it("falls back to document.execCommand when navigator.clipboard is unavailable", async () => {
    const execCommand = vi.fn().mockReturnValue(true);
    const createElement = vi.fn().mockReturnValue({
      style: {},
      focus: vi.fn(),
      select: vi.fn(),
      value: "",
    });
    const appendChild = vi.fn();
    const removeChild = vi.fn();

    vi.stubGlobal("window", {});
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("document", {
      createElement,
      body: { appendChild, removeChild },
      execCommand,
    });

    const result = await writeTextToClipboard("copied text via fallback", "code");
    expect(result).toBe(true);
    expect(execCommand).toHaveBeenCalledWith("copy");
  });

  it("falls back to document.execCommand when navigator.clipboard.writeText rejects", async () => {
    const writeText = vi.fn().mockRejectedValue(new Error("Permission denied"));
    const execCommand = vi.fn().mockReturnValue(true);
    const createElement = vi.fn().mockReturnValue({
      style: {},
      focus: vi.fn(),
      select: vi.fn(),
      value: "",
    });
    const appendChild = vi.fn();
    const removeChild = vi.fn();

    vi.stubGlobal("window", {});
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    vi.stubGlobal("document", {
      createElement,
      body: { appendChild, removeChild },
      execCommand,
    });

    const result = await writeTextToClipboard("copied text via fallback", "code");
    expect(result).toBe(true);
    expect(writeText).toHaveBeenCalledWith("copied text via fallback");
    expect(execCommand).toHaveBeenCalledWith("copy");
  });
});

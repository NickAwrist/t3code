import { describe, expect, it } from "vite-plus/test";

import {
  canThreadSidebarOverlayChatMargin,
  resolveInitialThreadSidebarWidth,
  THREAD_CHAT_CONTENT_MAX_WIDTH,
  THREAD_MAIN_CONTENT_MIN_WIDTH,
  THREAD_SIDEBAR_DEFAULT_WIDTH,
  THREAD_SIDEBAR_MIN_WIDTH,
} from "./threadSidebarWidth";

describe("thread sidebar width", () => {
  it("overlays the sidebar in unused chat margin on wide viewports", () => {
    expect(
      canThreadSidebarOverlayChatMargin(
        THREAD_SIDEBAR_DEFAULT_WIDTH,
        THREAD_CHAT_CONTENT_MAX_WIDTH + THREAD_SIDEBAR_DEFAULT_WIDTH * 2,
      ),
    ).toBe(true);
  });

  it("reserves sidebar space when it would overlap the chat column", () => {
    expect(
      canThreadSidebarOverlayChatMargin(
        THREAD_SIDEBAR_DEFAULT_WIDTH,
        THREAD_CHAT_CONTENT_MAX_WIDTH + THREAD_SIDEBAR_DEFAULT_WIDTH * 2 - 1,
      ),
    ).toBe(false);
  });

  it("uses the default width when no preference is stored", () => {
    expect(resolveInitialThreadSidebarWidth(null, 1200)).toBe(THREAD_SIDEBAR_DEFAULT_WIDTH);
  });

  it("uses a stored width in the initial render", () => {
    expect(resolveInitialThreadSidebarWidth(360, 1200)).toBe(360);
  });

  it("clamps a stored width to the sidebar minimum", () => {
    expect(resolveInitialThreadSidebarWidth(120, 1200)).toBe(THREAD_SIDEBAR_MIN_WIDTH);
  });

  it("leaves enough room for the main content on a smaller window", () => {
    const viewportWidth = 1000;

    expect(resolveInitialThreadSidebarWidth(900, viewportWidth)).toBe(
      viewportWidth - THREAD_MAIN_CONTENT_MIN_WIDTH,
    );
  });

  it("keeps the sidebar minimum when the whole layout is narrower than its minimums", () => {
    expect(resolveInitialThreadSidebarWidth(900, 700)).toBe(THREAD_SIDEBAR_MIN_WIDTH);
  });
});

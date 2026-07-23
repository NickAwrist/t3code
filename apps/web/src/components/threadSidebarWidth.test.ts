import { describe, expect, it } from "vite-plus/test";

import {
  canThreadSidebarOverlayChatMargin,
  THREAD_CHAT_CONTENT_MAX_WIDTH,
  THREAD_SIDEBAR_DEFAULT_WIDTH,
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
});

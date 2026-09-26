import { describe, expect, it } from "vite-plus/test";
import {
  canThreadSidebarOverlayChatMargin,
  clampThreadSidebarWidth,
  resolveThreadSidebarMaximumWidth,
  resolveThreadSidebarMinimumWidth,
  THREAD_CHAT_CONTENT_MAX_WIDTH,
  THREAD_CHAT_CONTENT_MAX_WIDTH_WIDE,
  THREAD_SIDEBAR_DEFAULT_WIDTH,
  THREAD_SIDEBAR_MIN_WIDTH,
} from "./threadSidebarWidth";

describe("resolveThreadSidebarMinimumWidth", () => {
  it("keeps the default minimum when the brand fits", () => {
    expect(resolveThreadSidebarMinimumWidth(0)).toBe(THREAD_SIDEBAR_MIN_WIDTH);
    expect(resolveThreadSidebarMinimumWidth(194)).toBe(THREAD_SIDEBAR_MIN_WIDTH);
  });

  it("grows to a brand wider than the default, rounding up", () => {
    expect(resolveThreadSidebarMinimumWidth(237.2)).toBe(238);
  });
});

describe("resolveThreadSidebarMaximumWidth", () => {
  it("never drops below a raised minimum on a narrow viewport", () => {
    expect(resolveThreadSidebarMaximumWidth(800, 238)).toBe(238);
    expect(resolveThreadSidebarMaximumWidth(1200, 238)).toBe(560);
  });
});

describe("clampThreadSidebarWidth", () => {
  it("widens a stored width below a raised minimum", () => {
    expect(clampThreadSidebarWidth(208, 238, 560)).toBe(238);
  });

  it("keeps widths inside the range and caps wide ones", () => {
    expect(clampThreadSidebarWidth(300, 238, 560)).toBe(300);
    expect(clampThreadSidebarWidth(900, 238, 560)).toBe(560);
  });
});

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

  it("respects wide chat width setting", () => {
    expect(
      canThreadSidebarOverlayChatMargin(
        THREAD_SIDEBAR_DEFAULT_WIDTH,
        THREAD_CHAT_CONTENT_MAX_WIDTH_WIDE + THREAD_SIDEBAR_DEFAULT_WIDTH * 2,
        "wide",
      ),
    ).toBe(true);
    expect(
      canThreadSidebarOverlayChatMargin(
        THREAD_SIDEBAR_DEFAULT_WIDTH,
        THREAD_CHAT_CONTENT_MAX_WIDTH_WIDE + THREAD_SIDEBAR_DEFAULT_WIDTH * 2 - 1,
        "wide",
      ),
    ).toBe(false);
  });

  it("disables overlay when chat width is full", () => {
    expect(canThreadSidebarOverlayChatMargin(THREAD_SIDEBAR_DEFAULT_WIDTH, 4000, "full")).toBe(
      false,
    );
  });
});

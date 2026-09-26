import type { ChatWidth } from "@t3tools/contracts";

export const THREAD_SIDEBAR_WIDTH_STORAGE_KEY = "chat_thread_sidebar_width";
export const THREAD_SIDEBAR_DEFAULT_WIDTH = 16 * 16;
export const THREAD_SIDEBAR_MIN_WIDTH = 13 * 16;
export const THREAD_MAIN_CONTENT_MIN_WIDTH = 40 * 16;
export const THREAD_CHAT_CONTENT_MAX_WIDTH = 48 * 16;
export const THREAD_CHAT_CONTENT_MAX_WIDTH_WIDE = 72 * 16;

export function resolveThreadChatContentMaxWidth(
  chatWidth: ChatWidth = "comfortable",
): number | null {
  switch (chatWidth) {
    case "wide":
      return THREAD_CHAT_CONTENT_MAX_WIDTH_WIDE;
    case "full":
      return null;
    case "comfortable":
    default:
      return THREAD_CHAT_CONTENT_MAX_WIDTH;
  }
}

export function canThreadSidebarOverlayChatMargin(
  sidebarWidth: number,
  viewportWidth: number,
  chatWidth: ChatWidth = "comfortable",
): boolean {
  const contentMaxWidth = resolveThreadChatContentMaxWidth(chatWidth);
  if (contentMaxWidth === null) {
    return false;
  }
  return viewportWidth >= contentMaxWidth + sidebarWidth * 2;
}

export function resolveThreadSidebarMaximumWidth(viewportWidth: number): number {
  return Math.max(
    THREAD_SIDEBAR_MIN_WIDTH,
    Math.floor(viewportWidth) - THREAD_MAIN_CONTENT_MIN_WIDTH,
  );
}

export function resolveInitialThreadSidebarWidth(
  storedWidth: number | null,
  viewportWidth: number,
): number {
  const preferredWidth =
    storedWidth === null
      ? THREAD_SIDEBAR_DEFAULT_WIDTH
      : Math.max(THREAD_SIDEBAR_MIN_WIDTH, storedWidth);
  return Math.min(preferredWidth, resolveThreadSidebarMaximumWidth(viewportWidth));
}

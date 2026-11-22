// Svelte action: keep a scrolling console pinned to the bottom as lines are
// appended, unless the user scrolled up.

export interface AutoscrollOpts {
  deps: number;
  /** External toggle (expanded console). Defaults to "stick unless scrolled up". */
  enabled?: () => boolean;
}

export function autoscroll(node: HTMLElement, opts: AutoscrollOpts) {
  let stuck = true;

  function atBottom(): boolean {
    return node.scrollHeight - node.scrollTop - node.clientHeight < 48;
  }

  const onScroll = () => {
    stuck = atBottom();
  };
  node.addEventListener('scroll', onScroll, { passive: true });

  return {
    update(next: AutoscrollOpts) {
      const enabled = next.enabled ? next.enabled() : stuck;
      if (enabled || stuck) {
        node.scrollTop = node.scrollHeight;
      }
    },
    destroy() {
      node.removeEventListener('scroll', onScroll);
    }
  };
}

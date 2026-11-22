// Toast notifications (2.6s visibility, slide-up animation is in app.css).

export interface Toast {
	id: number;
	msg: string;
}

let nextId = 1;
let timer: ReturnType<typeof setTimeout> | null = null;

class ToastState {
	items = $state<Toast[]>([]);
}

export const toastState = new ToastState();

/** Show one toast; replaces any previous one. */
export function showToast(msg: string): void {
	if (timer !== null) clearTimeout(timer);
	toastState.items = [{ id: nextId++, msg }];
	timer = setTimeout(() => {
		toastState.items = [];
		timer = null;
	}, 2600);
}

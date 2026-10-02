import type { VNode } from 'vue';
import { reactive } from 'vue';

export const notificationTypes = ['info', 'success', 'error'] as const;

export type NotificationTypes = (typeof notificationTypes)[number];
export interface ToastI {
  id: number;
  title: string | VNode; // vNode is used for rendering Vue components
  message?: string | VNode; // vNode is used for rendering Vue components
  type: NotificationTypes;
  duration: number; // in milliseconds
}

let toastIdCounter = 0;

export const ToastStore = reactive({
  toasts: [] as ToastI[],
  maxVisible: 5,

  success(
    message: string | VNode,
    options: { title?: string; duration?: number; message?: string | VNode } = {},
  ) {
    const mess = options.title ? message : options.message;
    this.add(
      options.title ? options.title : message,
      options.message ? options.message : mess,
      'success',
      options.duration,
    );
  },

  error(
    message: string | VNode,
    options: { title?: string; duration?: number; message?: string | VNode } = {},
  ) {
    const mess = options.title ? message : options.message;
    this.add(
      options.title ? options.title : message,
      options.message ? options.message : mess,
      'error',
      options.duration,
    );
  },

  info(
    message: string | VNode,
    options: { title?: string; duration?: number; message?: string | VNode } = {},
  ) {
    const mess = options.title ? message : options.message;
    this.add(
      options.title ? options.title : message,
      options.message ? options.message : mess,
      'info',
      options.duration ? options.duration : 0,
    );
  },

  add(
    title: string | VNode,
    message: string | VNode | undefined,
    type: NotificationTypes = 'success',
    duration: number = 6000,
  ) {
    toastIdCounter += 1; // Increment the counter for unique IDs
    const toast = {
      id: toastIdCounter,
      title,
      message,
      type,
      duration,
    };
    this.toasts.push(toast);

    if (this.toasts.length > this.maxVisible) {
      this.toasts.shift(); // remove the oldest
    }

    setTimeout(() => {
      this.remove(toast.id);
    }, toast.duration);
  },

  closeAll() {
    this.toasts = [];
  },

  remove(id: number) {
    this.toasts = this.toasts.filter((t) => t.id !== id);
  },
});

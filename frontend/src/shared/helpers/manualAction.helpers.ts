import { ToastStore } from '@/shared/message/notification';

export const doManualAction = async ({
  loadingMessage,
  actionFn,
  successMessage,
  errorMessage,
}: {
  loadingMessage?: string;
  successMessage?: string;
  errorMessage?: string;
  actionFn: Promise<unknown>;
}) => {
  if (loadingMessage) {
    ToastStore.info(loadingMessage);
  }

  return actionFn
    .then(() => {
      if (successMessage) {
        ToastStore.closeAll();
        ToastStore.success(successMessage);
      }
      Promise.resolve();
    })
    .catch(() => {
      if (errorMessage) {
        ToastStore.closeAll();
        ToastStore.error(errorMessage);
      }
      Promise.reject();
    });
};

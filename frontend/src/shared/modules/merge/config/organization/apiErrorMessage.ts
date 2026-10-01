import { ToastStore } from '@/shared/message/notification';
import type { ApiErrorMessage } from '../../types/OrganizationMessage';

export default ({ error }: ApiErrorMessage) => {
  ToastStore.closeAll();

  if (error.response.status === 404) {
    ToastStore.success('Organizations already merged or deleted', {
      message: `Sorry, the organizations you are trying to merge might have already been merged or deleted.
        Please refresh to see the updated information.`,
    });

    return true;
  }

  ToastStore.error('There was an error merging organizations');
  return false;
};

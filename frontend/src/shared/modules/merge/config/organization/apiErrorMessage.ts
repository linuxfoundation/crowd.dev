import { ToastStore } from '@/shared/message/notification';
import type { ApiErrorMessage } from '../../types/OrganizationMessage';

export default ({ error }: ApiErrorMessage) => {
  ToastStore.closeAll();

  // shortcut: assumes the merge error carries a response, as on main. revisit: when merge errors are typed.
  if (error.response!.status === 404) {
    ToastStore.success('Companies already merged or deleted', {
      message: `Sorry, the companies you are trying to merge might have already been merged or deleted.
        Please refresh to see the updated information.`,
    });

    return true;
  }

  ToastStore.error('There was an error merging companies');
  return false;
};

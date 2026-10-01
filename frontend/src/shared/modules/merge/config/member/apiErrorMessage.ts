import { ToastStore } from '@/shared/message/notification';
import type { ErrorMessage } from '../../types/MemberMessage';

export default ({ error }: ErrorMessage) => {
  ToastStore.closeAll();
  if (error.response.status === 404) {
    ToastStore.success('Profiles already merged or deleted', {
      message: `Sorry, the profiles you are trying to merge might have already been merged or deleted.
          Please refresh to see the updated information.`,
    });
    return true;
  }

  ToastStore.error('There was an error merging profiles');
  return false;
};

import { useAuthStore } from '@/modules/auth/store/auth.store';
import { storeToRefs } from 'pinia';
import config from '@/config';
import { createEvent } from './tracking-service';
import useSessionTracking from './useSessionTracking';

const useProductTracking = () => {
  const authStore = useAuthStore();
  const { user } = storeToRefs(authStore);

  const shouldTrack = config.env !== 'staging';

  const trackEvent = ({
    key,
    type,
    properties,
  }: {
    key: string;
    type: string;
    properties?: Record<string, any>;
  }) => {
    if (!shouldTrack) {
      return;
    }

    const { startSession } = useSessionTracking(() => user.value);
    const userSession = sessionStorage.getItem('userSession');

    if (user.value && userSession) {
      createEvent({
        key,
        type,
        sessionId: userSession,
        properties,
        userId: user.value.id,
        userEmail: user.value.email,
      }).catch(() => {
        sessionStorage.removeItem('userSession');
        startSession();
      });
    }
  };

  return {
    trackEvent,
  };
};

export default useProductTracking;

import LogRocket from 'logrocket';
import config from '@/config';
import type { User } from '@/modules/user/types/User';

const APP_ID = 'nm6fil/crowddev';

const isLogRocketEnabled: () => void = () => config.env === 'production';

const init = () => {
  if (isLogRocketEnabled()) {
    LogRocket.init(APP_ID);
  }
};

const captureException = (error: Error) => {
  if (isLogRocketEnabled()) {
    LogRocket.captureException(error);
  }
};

const identify = (user: User) => {
  if (isLogRocketEnabled()) {
    LogRocket.identify(user.id, {
      name: user.fullName,
      email: user.email,
    });
  }
};

export const useLogRocket = () => ({
  isLogRocketEnabled,
  init,
  captureException,
  identify,
});

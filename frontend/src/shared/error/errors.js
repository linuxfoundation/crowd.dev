import { useLogRocket } from '@/utils/logRocket';
import { getRouter } from '@/router/instance';
import { ToastStore } from '@/shared/message/notification';
import { AuthService } from '@/modules/auth/services/auth.service';

const DEFAULT_ERROR_MESSAGE = 'Ops, an error occurred';

function selectErrorKeyOrMessage(error) {
  if (error && error.response && error.response.data) {
    const { data } = error.response;

    if (data.error && data.error.message) {
      return data.error.message;
    }

    return String(data);
  }

  return error.message || DEFAULT_ERROR_MESSAGE;
}

function selectErrorMessage(error) {
  const key = selectErrorKeyOrMessage(error);

  return key;
}

function selectErrorCode(error) {
  if (error && error.response && error.response.status) {
    return error.response.status;
  }

  return 500;
}

export default class Errors {
  static handle(error) {
    const { captureException } = useLogRocket();

    captureException(error);

    if (import.meta.env.NODE_ENV !== 'test') {
      console.error(selectErrorMessage(error));
      console.error(error);
    }

    if (selectErrorCode(error) === 401) {
      AuthService.logout();
      window.location.reload();
      return;
    }

    if (selectErrorCode(error) === 403) {
      if (error.config.url.includes('member/export')) {
        // we'll be handling these differently
        return;
      }

      if (error.response.data.includes('Missing scopes in ')) {
        ToastStore.error(error.response.data, { duration: 0 });
        return;
      }
      getRouter().push('/403');
      return;
    }

    if ([400, 409, 429].includes(selectErrorCode(error))) {
      ToastStore.error(selectErrorMessage(error));
      return;
    }

    if (selectErrorCode(error) === 542) {
      ToastStore.error(
        'An error has occurred setting up the integration, please reach out to us via chat.',
        { duration: 0 },
      );
      return;
    }

    ToastStore.error(
      'Please try again. If the problem remains, reach out to us.',
      { title: 'Oops, something went wrong' },
    );
  }

  static errorCode(error) {
    return selectErrorCode(error);
  }

  static selectMessage(error) {
    return selectErrorMessage(error);
  }

  static showMessage(error) {
    ToastStore.error(selectErrorMessage(error));
  }
}

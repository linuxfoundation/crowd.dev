import type { AxiosError } from 'axios';
import { useLogRocket } from '@/utils/logRocket';
import { getRouter } from '@/router/instance';
import { ToastStore } from '@/shared/message/notification';
import { AuthService } from '@/modules/auth/services/auth.service';

const DEFAULT_ERROR_MESSAGE = 'Ops, an error occurred';

// shortcut: the payload is a string or a { error } object, typed as both. revisit: CM-1762 rewrites 401/403 handling
type ApiError = AxiosError<string & { error?: { message?: string } }>;

export function errorMessage(error: ApiError): string {
  if (error && error.response && error.response.data) {
    const { data } = error.response;

    if (data.error && data.error.message) {
      return data.error.message;
    }

    return String(data);
  }

  return error.message || DEFAULT_ERROR_MESSAGE;
}

export function errorCode(error: ApiError): number {
  if (error && error.response && error.response.status) {
    return error.response.status;
  }

  return 500;
}

export function handleApiError(rawError: unknown): void {
  const error = rawError as ApiError;
  const { captureException } = useLogRocket();

  captureException(error);

  if (import.meta.env.NODE_ENV !== 'test') {
    console.error(errorMessage(error));
    console.error(error);
  }

  if (errorCode(error) === 401) {
    AuthService.logout();
    window.location.reload();
    return;
  }

  if (errorCode(error) === 403) {
    if (error.config!.url!.includes('member/export')) {
      // we'll be handling these differently
      return;
    }

    if (error.response!.data.includes('Missing scopes in ')) {
      ToastStore.error(error.response!.data, { duration: 0 });
      return;
    }
    getRouter().push('/403');
    return;
  }

  if ([400, 409, 429].includes(errorCode(error))) {
    ToastStore.error(errorMessage(error));
    return;
  }

  if (errorCode(error) === 542) {
    ToastStore.error(
      'An error has occurred setting up the integration, please reach out to us via chat.',
      { duration: 0 },
    );
    return;
  }

  ToastStore.error('Please try again. If the problem remains, reach out to us.', {
    title: 'Oops, something went wrong',
  });
}

export function showErrorMessage(error: unknown): void {
  ToastStore.error(errorMessage(error as ApiError));
}

export default {
  handle: handleApiError,
  errorCode,
  selectMessage: errorMessage,
  showMessage: showErrorMessage,
};

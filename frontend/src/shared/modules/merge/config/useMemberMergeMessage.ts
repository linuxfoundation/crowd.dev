import apiErrorMessage from './member/apiErrorMessage';
import loadingMessage from './member/loadingMessage';
import successMessage from './member/successMessage';
import type { MemberMessage } from '../types/MemberMessage';

export default <MemberMessage> {
  loadingMessage,
  successMessage,
  apiErrorMessage,
};

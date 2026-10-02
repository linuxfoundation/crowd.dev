import type { OrganizationMessage } from '../types/OrganizationMessage';
import apiErrorMessage from './organization/apiErrorMessage';
import loadingMessage from './organization/loadingMessage';
import socketErrorMessage from './organization/socketErrorMessage';
import successMessage from './organization/successMessage';

export default <OrganizationMessage> {
  loadingMessage,
  successMessage,
  apiErrorMessage,
  socketErrorMessage,
};

import type { AxiosError } from 'axios';

export interface SuccessMessage {
  primaryOrganization: {
    id: string;
    displayName: string;
  };
  secondaryOrganization: {
    displayName: string;
  };
}

export interface ApiErrorMessage {
  error: AxiosError;
}

export interface SocketErrorMessage {
    primaryOrganization: {
        id: string;
        displayName: string;
    };
    secondaryOrganization: {
        displayName: string;
    }
}

export interface OrganizationMessage {
  loadingMessage: () => void;
  successMessage: ({
    primaryOrganization,
    secondaryOrganization,
  }: SuccessMessage) => void;
  apiErrorMessage: ({ error }: ApiErrorMessage) => void;
  socketErrorMessage: ({
    primaryOrganization,
    secondaryOrganization,
  }: SocketErrorMessage) => void;
}

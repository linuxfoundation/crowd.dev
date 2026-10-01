import type { AxiosError } from 'axios';
import { h } from 'vue';
import { ToastStore } from '../message/notification';

export const getAxiosErrorMessage = (
  error: AxiosError,
  defaultMessage: string = 'Something went wrong',
): string => {
  if (error && error.response && error.response.data) {
    const errMsg = error.response.data && typeof error.response.data === 'string'
      ? error.response.data
      : error.message;
    return errMsg;
  }
  return error.message || defaultMessage;
};

export const parseDuplicateRepoError = (
  errorMessage: string,
  defaultMessage: string,
): { repo: string; eId: string } | null => {
  const pattern = new RegExp(
    'Trying to update repo (?<repo>[^\\s]+) mapping with integrationId (?<IId>[^\\s]+) '
      + 'but it is already mapped to integration (?<eId>[^\\s!]+)',
  );
  const match = errorMessage.match(pattern);

  if (match?.groups) {
    const { repo, eId } = match.groups;

    return { repo, eId };
  }
  ToastStore.error(defaultMessage);
  return null;
};

const getSegmentLink = (segment: any) => `/integrations/${segment.grandparentId}/${segment.id}`;
export const customRepoErrorMessage = (
  segment: any,
  githubRepo: string,
  integrationName: string,
) => {
  ToastStore.error(
    h(
      'span',
      {
        class: 'whitespace-normal',
      },
      [
        `The ${integrationName} repo`,
        ' ',
        h('strong', githubRepo),
        ' ',
        'is already connected with project',
        ' ',
        h(
          'a',
          {
            href: getSegmentLink(segment),
            class: 'text-blue-500 underline hover:text-blue-600',
          },
          segment.name || 'Unknown Project',
        ),
      ],
    ),
    {
      title: 'Conflict Detected',
    },
  );
};

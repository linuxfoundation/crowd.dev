// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import authAxios from '@/shared/axios/auth-axios';
import type {
  EventMappingConfirmItem,
  EventMappingList,
  EventMappingProjectsPage,
  EventMappingReference,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

export const listEventMappings = async (): Promise<EventMappingList> => {
  const response = await authAxios.get<EventMappingList>('/event-mapping');
  return response.data;
};

export const listEventMappingProjects = async (params: {
  search: string;
  limit: number;
  offset: number;
}): Promise<EventMappingProjectsPage> => {
  const response = await authAxios.get<EventMappingProjectsPage>('/event-mapping/projects', {
    params,
  });
  return response.data;
};

export const confirmEventMappings = async (
  items: EventMappingConfirmItem[],
): Promise<{ confirmed: number }> => {
  const response = await authAxios.post<{ confirmed: number }>('/event-mapping/confirm', {
    items,
  });
  return response.data;
};

export const ignoreEventMapping = async (
  reference: EventMappingReference,
): Promise<{ eventKey: string }> => {
  const response = await authAxios.post<{ eventKey: string }>('/event-mapping/ignore', reference);
  return response.data;
};

export const restoreEventMapping = async (
  reference: EventMappingReference,
): Promise<{ eventKey: string }> => {
  const response = await authAxios.post<{ eventKey: string }>('/event-mapping/restore', reference);
  return response.data;
};

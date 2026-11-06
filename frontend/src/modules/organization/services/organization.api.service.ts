// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import authAxios from '@/shared/axios/auth-axios';
import type {
  Organization,
  OrganizationQueryBody,
} from '@/modules/organization/types/Organization';
import type { OrganizationMergeSuggestion } from '@/modules/organization/types/OrganizationSuggestion';
import type { DataQualityFakeOrganizationSuggestion } from '@/modules/data-quality/types/DataQualitySuggestion';
import type { Pagination } from '@/shared/types/Pagination';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';

export interface SuggestionsPage<T> extends Pagination<T> {
  hasMore: boolean;
}

export interface OrganizationAutocompleteParams {
  query: string | unknown[];
  limit: number;
  segments?: (string | undefined)[] | null;
  excludeLfMember?: boolean;
  excludeSegments?: boolean;
}

export interface OrganizationAutocompleteOption extends Organization {
  label: string;
  value: string;
}

export interface OrganizationExportParams {
  filter?: Record<string, unknown>;
  orderBy?: string;
  limit?: number;
  offset?: number | null;
  segments?: string[];
}

const getSelectedProjectGroup = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value;
};

const getSelectedProjectGroupSegments = (): string[] | null => {
  const id = getSelectedProjectGroup()?.id;

  return id ? [id] : null;
};

export async function update(id: string, data: Partial<Organization>): Promise<Organization> {
  const response = await authAxios.put(`/organization/${id}`, {
    ...data,
    segments: getSelectedProjectGroupSegments(),
  });

  return response.data;
}

export async function destroyAll(ids: string[]): Promise<unknown> {
  const params = {
    ids,
    segments: getSelectedProjectGroupSegments(),
  };

  const response = await authAxios.delete('/organization', {
    params,
  });

  return response.data;
}

export async function mergeOrganizations(
  organizationToKeepId: string,
  organizationToMergeId: string,
): Promise<unknown> {
  const response = await authAxios.put(`/organization/${organizationToKeepId}/merge`, {
    organizationToMerge: organizationToMergeId,
    segments: getSelectedProjectGroupSegments(),
  });

  return response.data;
}

export async function unmerge(orgId: string, preview: unknown): Promise<unknown> {
  const response = await authAxios.post(`/organization/${orgId}/unmerge`, preview);

  return response.data;
}

export async function unmergePreview(
  orgId: string,
  identity: unknown,
  revertPreviousMerge: boolean,
): Promise<{ primary: Organization; secondary: Organization }> {
  const response = await authAxios.post(`/organization/${orgId}/unmerge/preview`, {
    identity,
    revertPreviousMerge,
  });

  return response.data;
}

export async function canRevertMerge(orgId: string, identity: unknown): Promise<boolean> {
  const response = await authAxios.get(`/organization/${orgId}/can-revert-merge`, {
    params: {
      identity,
    },
  });

  return response.data;
}

export async function addToNoMerge(
  organizationA: Pick<Organization, 'id'>,
  organizationB: Pick<Organization, 'id'>,
): Promise<unknown> {
  const response = await authAxios.put(`/organization/${organizationA.id}/no-merge`, {
    organizationToNotMerge: organizationB.id,
  });

  return response.data;
}

export async function noMergeOrganizations(
  organizationAId: string,
  organizationBId: string,
): Promise<unknown> {
  const response = await authAxios.put(`/organization/${organizationAId}/no-merge`, {
    organizationToNotMerge: organizationBId,
  });

  return response.data;
}

export async function create(data: Partial<Organization>): Promise<Organization> {
  const response = await authAxios.post('/organization', {
    ...data,
    segments: getSelectedProjectGroupSegments(),
  });

  return response.data;
}

export async function find(id: string, segments: (string | undefined)[]): Promise<Organization> {
  const response = await authAxios.get(`/organization/${id}`, {
    params: {
      segmentId: segments[0],
      // The parameter id on this one is sematically different, so we are excluding the logic to add segments as an array
      excludeSegments: true,
    },
  });

  return response.data;
}

export async function query(body: OrganizationQueryBody): Promise<Pagination<Organization>> {
  const response = await authAxios.post('/organization/query', body);

  return response.data;
}

export async function organizationsList(
  body: OrganizationQueryBody,
): Promise<Pagination<Organization>> {
  const response = await authAxios.post('/organization/list', body);

  return response.data;
}

export async function listByIds(ids: (string | null)[]): Promise<Organization[]> {
  const response = await authAxios.post('/organization/id', {
    ids,
  });

  return response.data;
}

export async function listOrganizationsAutocomplete({
  query: search,
  limit,
  segments = null,
  excludeSegments = false,
}: OrganizationAutocompleteParams): Promise<OrganizationAutocompleteOption[]> {
  const payload = {
    filter: {
      and: [
        {
          displayName: {
            textContains: search,
          },
        },
      ],
    },
    offset: 0,
    orderBy: 'activityCount_DESC',
    limit,
    ...(segments && {
      segments,
    }),
    ...(excludeSegments && {
      excludeSegments,
    }),
  };

  const response = await authAxios.post<Pagination<Organization>>(
    '/organization/autocomplete',
    payload,
  );

  return response.data.rows.map((o) => ({
    ...o,
    label: o.displayName,
    value: o.id,
  }));
}

export async function fetchMergeSuggestions(
  limit: number,
  offset: number,
  params: Record<string, unknown> = {},
): Promise<SuggestionsPage<OrganizationMergeSuggestion>> {
  const data = {
    limit,
    offset,
    segments: getSelectedProjectGroupSegments(),
    detail: true,
    ...params,
  };

  const response = await authAxios.post('/organizationsToMerge', data);

  return response.data;
}

export async function fetchFakeOrganizationSuggestions(
  limit: number,
  offset: number,
  params: Record<string, unknown> = {},
): Promise<
  SuggestionsPage<DataQualityFakeOrganizationSuggestion & { organization?: Organization }>
> {
  const segments = [getSelectedProjectGroup()!.id];

  const response = await authAxios.get('/organization/fake-suggestions', {
    params: {
      segments,
      offset,
      limit,
      detail: 'true',
      ...params,
    },
  });

  return response.data;
}

export async function dismissFakeOrganizationSuggestion(organizationId: string): Promise<unknown> {
  const response = await authAxios.delete(`/organization/${organizationId}/fake-suggestion`);

  return response.data;
}

export async function exportOrganizations({
  filter,
  orderBy,
  limit,
  offset,
  segments = [],
}: OrganizationExportParams): Promise<unknown> {
  const body = {
    filter,
    orderBy,
    limit,
    offset,
    segments,
  };

  const response = await authAxios.post('/organization/export', body);

  return response.data;
}

export const OrganizationService = {
  update,
  destroyAll,
  mergeOrganizations,
  unmerge,
  unmergePreview,
  canRevertMerge,
  addToNoMerge,
  noMergeOrganizations,
  create,
  find,
  query,
  organizationsList,
  listByIds,
  listOrganizationsAutocomplete,
  fetchMergeSuggestions,
  fetchFakeOrganizationSuggestions,
  dismissFakeOrganizationSuggestion,
  export: exportOrganizations,
};

// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import authAxios from '@/shared/axios/auth-axios';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import type { Member } from '@/modules/member/types/Member';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import type {
  MemberBotSuggestion,
  MemberMergeSuggestion,
} from '@/modules/member/types/MemberSuggestion';
import type { BulkUpdateMemberPayload } from '@/modules/member/helpers/bulk-attributes';
import type { FilterCustomAttribute } from '@/shared/modules/filters/types/FilterCustomAttribute';
import type { Pagination } from '@/shared/types/Pagination';

export interface MemberListQuery {
  filter?: unknown;
  search?: string;
  offset?: number;
  limit?: number;
  orderBy?: string | null;
  segments?: string[];
}

export interface MemberExportQuery {
  filter: unknown;
  orderBy: string;
  limit: number;
  offset: number | null;
  segments?: (string | undefined)[];
}

export interface MemberAutocompleteQuery {
  query?: string;
  limit: number;
  segments?: string[] | null;
  excludeSegments?: boolean;
}

export type MemberAutocompleteOption = Omit<Member, 'organizations'> & {
  label: string;
  value: string;
  logo: string | null;
  organizations: { id: string; name: string }[];
};

export type MemberMergeSuggestionsResponse = Pagination<MemberMergeSuggestion> & {
  hasMore?: boolean;
};

export type MemberBotSuggestionsResponse = Pagination<MemberBotSuggestion> & { hasMore?: boolean };

export interface MemberUnmergePreview {
  primary: Contributor;
  secondary: Contributor;
}

type MemberRef = Pick<Member, 'id'>;

const getSelectedProjectGroup = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value;
};

export const update = async (id: string, data: Record<string, unknown>): Promise<Member> => {
  const response = await authAxios.put(`/member/${id}`, {
    ...data,
    segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
  });

  return response.data;
};

export const updateAttributes = async (
  id: string,
  data: Record<string, unknown>,
): Promise<Member> => {
  const response = await authAxios.patch(`/member/${id}/attributes`, {
    ...data,
    segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
  });

  return response.data;
};

export const updateBulk = async (data: BulkUpdateMemberPayload[]): Promise<Member[]> => {
  const response = await authAxios.patch('/member', {
    segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
    data,
    addDataAsArray: true,
  });

  return response.data;
};

export const destroyAll = async (ids: string[]): Promise<unknown> => {
  const params = {
    ids,
    segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
  };

  const response = await authAxios.delete('/member', {
    params,
  });

  return response.data;
};

export const create = async (data: { data: Record<string, unknown> }): Promise<Member> => {
  const response = await authAxios.post('/member', {
    ...data.data,
    segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
  });

  return response.data;
};

export const findGithub = async (id: string): Promise<unknown> => {
  const response = await authAxios.get(`/member/github/${id}`, {
    params: {
      segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
    },
  });

  return response.data;
};

export const exportMembers = async ({
  filter,
  orderBy,
  limit,
  offset,
  segments = [],
}: MemberExportQuery): Promise<unknown> => {
  const body = {
    filter,
    orderBy,
    limit,
    offset,
    segments,
  };

  const response = await authAxios.post('/member/export', body);

  return response.data;
};

export const find = async (
  id: string,
  segmentId?: string,
  includeAllAttributes = false,
): Promise<Contributor> => {
  const segment = segmentId ?? getSelectedProjectGroup()?.id;
  const response = await authAxios.get(`/member/${id}`, {
    params: {
      segments: segment ? [segment] : [],
      includeAllAttributes,
      include: {
        identities: true,
        memberOrganizations: true,
        attributes: true,
      },
    },
  });

  return response.data;
};

export const listMembersAutocomplete = async ({
  query,
  limit,
  segments,
  excludeSegments = false,
}: MemberAutocompleteQuery): Promise<MemberAutocompleteOption[]> => {
  const payload = {
    filter: {
      and: [
        { isBot: { not: true } },
        { isOrganization: { not: true } },
        ...(query
          ? [
            {
              displayName: {
                textContains: query,
              },
            },
          ]
          : []),
      ],
    },
    search: query,
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

  const response = await authAxios.post('/member/autocomplete', payload, {
    headers: {
      'x-crowd-api-version': '1',
    },
  });

  return (response.data as Pagination<Member>).rows.map((m) => ({
    ...m,
    id: m.id,
    label: m.displayName,
    value: m.id,
    logo: m.attributes?.avatarUrl?.default || null,
    organizations:
      m.organizations?.map((organization) => ({
        id: organization.id,
        name: organization.displayName,
      })) ?? [],
  }));
};

export const listMembers = async (
  body: MemberListQuery,
  countOnly = false,
): Promise<Pagination<Member>> => {
  const response = await authAxios.post(
    '/member/query',
    {
      ...body,
      countOnly,
    },
    {
      headers: {
        'x-crowd-api-version': '1',
      },
    },
  );

  return response.data;
};

export const merge = async (
  memberToKeep: MemberRef,
  memberToMerge: MemberRef,
  segments?: string[],
): Promise<unknown> => {
  const response = await authAxios.put(`/member/${memberToKeep.id}/merge`, {
    memberToMerge: memberToMerge.id,
    segments,
  });

  return response.data;
};

export const unmerge = async (memberId: string, preview: unknown): Promise<unknown> => {
  const response = await authAxios.post(`/member/${memberId}/unmerge`, preview);

  return response.data;
};

export const unmergePreview = async (
  memberId: string,
  identityId: string,
  revertPreviousMerge: boolean,
): Promise<MemberUnmergePreview> => {
  const response = await authAxios.post(`/member/${memberId}/unmerge/preview`, {
    identityId,
    revertPreviousMerge,
  });

  return response.data;
};

export const canRevertMerge = async (memberId: string, identityId: string): Promise<boolean> => {
  const response = await authAxios.get(`/member/${memberId}/can-revert-merge`, {
    params: {
      identityId,
    },
  });

  return response.data;
};

export const addToNoMerge = async (
  memberA: MemberRef,
  memberB: MemberRef,
  segments?: string[],
): Promise<unknown> => {
  const response = await authAxios.put(`/member/${memberA.id}/no-merge`, {
    memberToNotMerge: memberB.id,
    segments,
  });

  return response.data;
};

export const fetchMergeSuggestions = async (
  limit: number,
  offset: number,
  query?: object,
): Promise<MemberMergeSuggestionsResponse> => {
  const segments = getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null;

  const data = {
    limit,
    offset,
    segments,
    detail: true,
    ...query,
  };

  return authAxios.post('/membersToMerge', data).then(({ data }) => Promise.resolve(data));
};

export const fetchBotSuggestions = async (
  limit: number,
  offset: number,
  query: Record<string, unknown> = {},
): Promise<MemberBotSuggestionsResponse> => {
  const segments = [getSelectedProjectGroup()!.id];

  return authAxios
    .get('/member/bot-suggestions', {
      params: {
        segments,
        offset,
        limit,
        detail: 'true',
        ...query,
      },
    })
    .then(({ data }) => Promise.resolve(data));
};

export const getCustomAttribute = async (id: string): Promise<FilterCustomAttribute> => {
  const response = await authAxios.get(`/settings/members/attributes/${id}`, {
    params: {
      segments: [getSelectedProjectGroup()!.id],
    },
  });

  return response.data;
};

export const fetchCustomAttributes = async (): Promise<Pagination<FilterCustomAttribute>> => {
  const response = await authAxios.get('/settings/members/attributes', {
    params: {
      segments: [getSelectedProjectGroup()!.id],
    },
  });

  return response.data;
};

export const createCustomAttributes = async (
  data: Record<string, unknown>,
  segments?: string[],
): Promise<FilterCustomAttribute> => {
  const response = await authAxios.post('/settings/members/attributes', {
    ...data,
    segments,
  });

  return response.data;
};

export const destroyCustomAttribute = async (
  id: string | string[],
  segments?: string[],
): Promise<unknown> => {
  const params = {
    ids: [id],
    segments,
  };

  const response = await authAxios.delete('/settings/members/attributes', {
    params,
  });

  return response.data;
};

export const updateCustomAttribute = async (
  id: string,
  data: Record<string, unknown>,
  segments?: string[],
): Promise<FilterCustomAttribute> => {
  const response = await authAxios.put(`/settings/members/attributes/${id}`, {
    ...data,
    segments,
  });

  return response.data;
};

export const enrichMember = async (id: string, segments: string[]): Promise<Member> => {
  const response = await authAxios.put(`/enrichment/member/${id}`, {
    segments,
  });

  return response.data;
};

export const enrichMemberBulk = async (ids: string[], segments: string[]) =>
  authAxios.put('/enrichment/member/bulk', {
    members: ids,
    segments,
  });

export const MemberService = {
  update,
  updateAttributes,
  updateBulk,
  destroyAll,
  create,
  findGithub,
  export: exportMembers,
  find,
  listMembersAutocomplete,
  listMembers,
  merge,
  unmerge,
  unmergePreview,
  canRevertMerge,
  addToNoMerge,
  fetchMergeSuggestions,
  fetchBotSuggestions,
  getCustomAttribute,
  fetchCustomAttributes,
  createCustomAttributes,
  destroyCustomAttribute,
  updateCustomAttribute,
  enrichMember,
  enrichMemberBulk,
};

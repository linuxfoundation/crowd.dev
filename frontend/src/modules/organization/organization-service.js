import authAxios from '@/shared/axios/auth-axios';
import { AuthService } from '@/modules/auth/services/auth.service'; import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { getSegmentsFromProjectGroup } from '@/utils/segments';

const getSelectedProjectGroup = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value;
};

export class OrganizationService {
  static async update(id, data) {
    const response = await authAxios.put(
      `/organization/${id}`,
      {
        ...data,
        segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
      },
    );

    return response.data;
  }

  static async destroyAll(ids) {
    const params = {
      ids,
      segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
    };

    const response = await authAxios.delete(
      '/organization',
      {
        params,
      },
    );

    return response.data;
  }

  static async mergeOrganizations(organizationToKeepId, organizationToMergeId) {
    const response = await authAxios.put(
      `/organization/${organizationToKeepId}/merge`,
      {
        organizationToMerge: organizationToMergeId,
        segments: [getSelectedProjectGroup().id],
      },
    );

    return response.data;
  }

  static async unmerge(orgId, preview) {
    const response = await authAxios.post(
      `/organization/${orgId}/unmerge`,
      preview,
    );

    return response.data;
  }

  static async unmergePreview(orgId, identity, revertPreviousMerge) {
    const response = await authAxios.post(
      `/organization/${orgId}/unmerge/preview`,
      {
        identity,
        revertPreviousMerge,
      },
    );

    return response.data;
  }

  static async canRevertMerge(orgId, identity) {
    const response = await authAxios.get(
      `/organization/${orgId}/can-revert-merge`,
      {
        params: {
          identity,
        },
      },
    );

    return response.data;
  }

  static async addToNoMerge(organizationA, organizationB) {
    const response = await authAxios.put(
      `/organization/${organizationA.id}/no-merge`,
      {
        organizationToNotMerge: organizationB.id,
      },
    );

    return response.data;
  }

  static async noMergeOrganizations(organizationAId, organizationBId) {
    const response = await authAxios.put(
      `/organization/${organizationAId}/no-merge`,
      {
        organizationToNotMerge: organizationBId,
      },
    );

    return response.data;
  }

  static async create(data) {
    const response = await authAxios.post(
      '/organization',
      {
        ...data,
        segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
      },
    );

    return response.data;
  }

  static async find(id, segments) {
    const response = await authAxios.get(
      `/organization/${id}`,
      {
        params: {
          segmentId: segments[0],
          // The parameter id on this one is sematically different, so we are excluding the logic to add segments as an array
          excludeSegments: true,
        },
      },
    );

    return response.data;
  }

  static async query(
    body,
  ) {
    const response = await authAxios.post(
      '/organization/query',
      body,
    );

    return response.data;
  }

  static async organizationsList(
    body,
  ) {
    const response = await authAxios.post(
      '/organization/list',
      body,
    );

    return response.data;
  }

  static async listByIds(
    ids,
  ) {
    const response = await authAxios.post(
      '/organization/id',
      {
        ids,
      },
    );

    return response.data;
  }

  static async listOrganizationsAutocomplete({
    query,
    limit,
    segments = null,
    excludeLfMember = false,
    excludeSegments = false,
  }) {
    const payload = {
      filter: {
        and: [
          {
            displayName: {
              textContains: query,
            },
          },
          // ...(excludeLfMember ? [{
          //   lfxMembership: {
          //     ne: true,
          //   },
          // }] : []),
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

    const response = await authAxios.post(
      '/organization/autocomplete',
      payload,
    );

    return response.data.rows
      .map((o) => ({
        ...o,
        label: o.displayName,
        value: o.id,
      }));
  }

  static async fetchMergeSuggestions(limit, offset, query) {
    const segments = [
      ...getSegmentsFromProjectGroup(getSelectedProjectGroup()),
      getSelectedProjectGroup().id,
    ];

    const data = {
      limit,
      offset,
      segments,
      detail: true,
      ...query,
    };

    return authAxios.post(
      '/organizationsToMerge',
      data,
    )
      .then(({ data }) => Promise.resolve(data));
  }

  static async export({
    filter,
    orderBy,
    limit,
    offset,
    segments = [],
  }) {
    const body = {
      filter,
      orderBy,
      limit,
      offset,
      segments,
    };

    const response = await authAxios.post(
      '/organization/export',
      body,
    );

    return response.data;
  }
}

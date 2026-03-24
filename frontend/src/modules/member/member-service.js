import authAxios from '@/shared/axios/auth-axios';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

const getSelectedProjectGroup = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value;
};

export class MemberService {
  static async update(id, data) {
    const response = await authAxios.put(`/member/${id}`, {
      ...data,
      segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
    });

    return response.data;
  }

  static async updateAttributes(id, data) {
    const response = await authAxios.patch(`/member/${id}/attributes`, {
      ...data,
      segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
    });

    return response.data;
  }

  static async updateBulk(data) {
    const response = await authAxios.patch('/member', {
      segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
      data,
      addDataAsArray: true,
    });

    return response.data;
  }

  static async destroyAll(ids) {
    const params = {
      ids,
      segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
    };

    const response = await authAxios.delete('/member', {
      params,
    });

    return response.data;
  }

  static async create(data, segments) {
    const response = await authAxios.post('/member', {
      ...data.data,
      segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
    });

    return response.data;
  }

  static async findGithub(id) {
    const response = await authAxios.get(`/member/github/${id}`, {
      params: {
        segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : null,
      },
    });

    return response.data;
  }

  static async export({
    filter, orderBy, limit, offset, segments = [],
  }) {
    const body = {
      filter,
      orderBy,
      limit,
      offset,
      segments,
    };

    const response = await authAxios.post('/member/export', body);

    return response.data;
  }

  static async find(id, segmentId, includeAllAttributes = false) {
    const response = await authAxios.get(`/member/${id}`, {
      params: {
        segments: [segmentId ?? getSelectedProjectGroup().id],
        includeAllAttributes,
        include: {
          identities: true,
          memberOrganizations: true,
          attributes: true,
        },
      },
    });

    return response.data;
  }

  static async listMembersAutocomplete({ query, limit, segments }) {
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
      offset: 0,
      orderBy: 'activityCount_DESC',
      limit,
      ...(segments && {
        segments,
      }),
    };

    const response = await authAxios.post('/member/autocomplete', payload, {
      headers: {
        'x-crowd-api-version': '1',
      },
    });

    return response.data.rows.map((m) => ({
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
  }

  static async listMembers(body, countOnly = false) {
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
  }

  static async listActive({
    platform,
    isTeamMember,
    activityTimestampFrom,
    activityTimestampTo,
    orderBy,
    offset,
    limit,
    segments,
  }) {
    const params = {
      ...(platform.length && {
        'filter[platforms]': platform.map((p) => p.value).join(','),
      }),
      ...(isTeamMember === false && {
        'filter[isTeamMember]': isTeamMember,
      }),
      'filter[isOrganization]': false,
      'filter[isBot]': false,
      'filter[activityTimestampFrom]': activityTimestampFrom,
      'filter[activityTimestampTo]': activityTimestampTo,
      orderBy,
      offset,
      limit,
      segments,
    };

    const response = await authAxios.get('/member/active', {
      params,
    });

    return response.data;
  }

  static async merge(memberToKeep, memberToMerge, segments) {
    const response = await authAxios.put(`/member/${memberToKeep.id}/merge`, {
      memberToMerge: memberToMerge.id,
      segments,
    });

    return response.data;
  }

  static async unmerge(memberId, preview) {
    const response = await authAxios.post(`/member/${memberId}/unmerge`, preview);

    return response.data;
  }

  static async unmergePreview(memberId, identityId, revertPreviousMerge) {
    const response = await authAxios.post(`/member/${memberId}/unmerge/preview`, {
      identityId,
      revertPreviousMerge,
    });

    return response.data;
  }

  static async canRevertMerge(memberId, identityId) {
    const response = await authAxios.get(`/member/${memberId}/can-revert-merge`, {
      params: {
        identityId,
      },
    });

    return response.data;
  }

  static async addToNoMerge(memberA, memberB, segments) {
    const response = await authAxios.put(`/member/${memberA.id}/no-merge`, {
      memberToNotMerge: memberB.id,
      segments,
    });

    return response.data;
  }

  static async fetchMergeSuggestions(limit, offset, query) {
    const segments = [getSelectedProjectGroup().id];

    const data = {
      limit,
      offset,
      segments,
      detail: true,
      ...query,
    };

    return authAxios.post('/membersToMerge', data).then(({ data }) => Promise.resolve(data));
  }

  static async fetchBotSuggestions(limit, offset) {
    const segments = [getSelectedProjectGroup().id];

    return authAxios
      .get('/member/bot-suggestions', {
        params: {
          segments,
          offset,
          limit,
        },
      })
      .then(({ data }) => Promise.resolve(data));
  }

  static async getCustomAttribute(id) {
    const response = await authAxios.get(`/settings/members/attributes/${id}`, {
      params: {
        segments: [getSelectedProjectGroup().id],
      },
    });

    return response.data;
  }

  static async fetchCustomAttributes() {
    const response = await authAxios.get('/settings/members/attributes', {
      params: {
        segments: [getSelectedProjectGroup().id],
      },
    });

    return response.data;
  }

  static async createCustomAttributes(data, segments) {
    const response = await authAxios.post('/settings/members/attributes', {
      ...data,
      segments,
    });

    return response.data;
  }

  static async destroyCustomAttribute(id, segments) {
    const params = {
      ids: [id],
      segments,
    };

    const response = await authAxios.delete('/settings/members/attributes', {
      params,
    });

    return response.data;
  }

  static async updateCustomAttribute(id, data, segments) {
    const response = await authAxios.put(`/settings/members/attributes/${id}`, {
      ...data,
      segments,
    });

    return response.data;
  }

  static async enrichMember(id, segments) {
    const response = await authAxios.put(`/enrichment/member/${id}`, {
      segments,
    });

    return response.data;
  }

  static async enrichMemberBulk(ids, segments) {
    return authAxios.put('/enrichment/member/bulk', {
      members: ids,
      segments,
    });
  }
}

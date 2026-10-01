import authAxios from '@/shared/axios/auth-axios';
import type { MemberOrganization } from '@/modules/organization/types/Organization';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

const getSegments = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value ? [selectedProjectGroup.value.id] : null;
};

export class ContributorOrganizationsApiService {
  static async list(memberId: string, segments: string[]) {
    return authAxios.get(
      `/member/${memberId}/organization`,
      {
        params: {
          segments,
        },
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async create(memberId: string, data: Partial<MemberOrganization>) {
    return authAxios.post(
      `/member/${memberId}/organization`,
      {
        ...data,
        segments: getSegments(),
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async update(memberId: string, id: string, organization: Partial<MemberOrganization>) {
    return authAxios.patch(
      `/member/${memberId}/organization/${id}`,
      {
        ...organization,
        segments: getSegments(),
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async delete(memberId: string, id: string) {
    return authAxios.delete(
      `/member/${memberId}/organization/${id}`,
      {
        params: {
          segments: getSegments(),
        },
      },
    ).then(({ data }) => Promise.resolve(data));
  }
}

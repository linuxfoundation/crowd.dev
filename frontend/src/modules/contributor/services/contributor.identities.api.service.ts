import authAxios from '@/shared/axios/auth-axios';
import { ContributorIdentity, UpdateContributorIdentityPayload } from '@/modules/contributor/types/Contributor';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

const getSegments = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value ? [selectedProjectGroup.value.id] : null;
};

export class ContributorIdentitiesApiService {
  static async list(memberId: string, segments: string[]) {
    return authAxios.get(
      `/member/${memberId}/identity`,
      {
        params: {
          segments,
        },
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async createMultiple(memberId: string, identities: ContributorIdentity[]) {
    return authAxios.put(
      `/member/${memberId}/identity`,
      {
        identities,
        segments: getSegments(),
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async update(memberId: string, id: string, payload: UpdateContributorIdentityPayload) {
    return authAxios.patch(
      `/member/${memberId}/identity/${id}`,
      payload,
    ).then(({ data }) => Promise.resolve(data));
  }

  static async delete(memberId: string, id: string) {
    return authAxios.delete(
      `/member/${memberId}/identity/${id}`,
      {
        params: {
          segments: getSegments(),
        },
      },
    ).then(({ data }) => Promise.resolve(data));
  }
}

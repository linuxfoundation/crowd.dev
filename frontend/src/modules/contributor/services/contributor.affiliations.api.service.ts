import authAxios from '@/shared/axios/auth-axios';
import type { ContributorAffiliation } from '@/modules/contributor/types/Contributor';
import type { MemberOrganizationAffiliationOverride } from '@/modules/organization/types/Organization';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

const getSegments = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value ? [selectedProjectGroup.value.id] : null;
};

export class ContributorAffiliationsApiService {
  static async list(memberId: string, segments: string[]): Promise<ContributorAffiliation[]> {
    return authAxios.get(
      `/member/${memberId}/affiliation`,
      {
        params: {
          segments,
        },
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async updateMultiple(memberId: string, affiliations: Partial<ContributorAffiliation>[]): Promise<ContributorAffiliation[]> {
    return authAxios.patch(
      `/member/${memberId}/affiliation`,
      {
        affiliations,
        segments: getSegments(),
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async updateAffiliationOverride(memberId: string, data: Partial<MemberOrganizationAffiliationOverride>): Promise<any> {
    return authAxios.post(
      `/member/${memberId}/affiliation/override`,
      {
        ...data,
        segments: getSegments(),
      },
    ).then(({ data }) => Promise.resolve(data));
  }
}

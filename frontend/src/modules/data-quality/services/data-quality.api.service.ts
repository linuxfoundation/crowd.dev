import authAxios from '@/shared/axios/auth-axios';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import type { Organization } from '@/modules/organization/types/Organization';

export class DataQualityApiService {
  static async findMemberIssues(params: any, segments: string[]): Promise<Contributor> {
    const response = await authAxios.get(
      '/data-quality/member',
      {
        params: {
          ...params,
          segments,
        },
      },
    );

    return response.data;
  }

  static async findOrganizationIssues(params: any, segments: string[]): Promise<Organization> {
    const response = await authAxios.get(
      '/data-quality/organization',
      {
        params: {
          ...params,
          segments,
        },
      },
    );

    return response.data;
  }
}

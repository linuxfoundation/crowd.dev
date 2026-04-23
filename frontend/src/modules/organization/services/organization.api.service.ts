import authAxios from '@/shared/axios/auth-axios';
import { Organization } from '@/modules/organization/types/Organization';
import { getSegmentsFromProjectGroup } from '@/utils/segments';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';

const getSelectedProjectGroupId = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value ? [selectedProjectGroup.value.id] : null;
};

export class OrganizationApiService {
  static async find(id: string, segments: string[]): Promise<Organization> {
    const [segmentId] = segments;

    const response = await authAxios.get(
      `/organization/${id}`,
      {
        params: {
          segmentId,
          // The parameter id on this one is sematically different, so we are excluding the logic to add segments as an array
          excludeSegments: true,
        },
      },
    );

    return response.data;
  }

  static async fetchMergeSuggestions(limit: number = 20, offset: number = 0, query: any = {}) {
    const lsSegmentsStore = useLfSegmentsStore();
    const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

    const segments = [
      ...getSegmentsFromProjectGroup(selectedProjectGroup.value),
      selectedProjectGroup.value?.id,
    ];

    const data = {
      limit,
      offset,
      segments,
      ...query,
    };

    return authAxios.post(
      '/organizationsToMerge',
      data,
    )
      .then(({ data }) => Promise.resolve(data));
  }

  static create(data: Partial<Organization>) {
    return authAxios.post(
      '/organization',
      {
        ...data,
        segments: getSelectedProjectGroupId(),
      },
    )
      .then(({ data }) => Promise.resolve(data));
  }
}

import { storeToRefs } from 'pinia';
import authAxios from '@/shared/axios/auth-axios';
import { Contributor } from '@/modules/contributor/types/Contributor';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

const getSegments = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value ? [selectedProjectGroup.value.id] : null;
};

export class ContributorApiService {
  static async find(id: string, segments: string[]): Promise<Contributor> {
    const response = await authAxios.get(
      `/member/${id}`,
      {
        params: {
          segments,
        },
      },
    );

    return response.data;
  }

  static async mergeSuggestions(limit: number, offset: number, query: any, segments: string[]) {
    const resolvedSegments = segments.length
      ? segments
      : (getSegments() ?? []);

    const data = {
      limit,
      offset,
      segments: resolvedSegments,
      detail: true,
      ...query,
    };

    return authAxios.post(
      '/membersToMerge',
      data,
    )
      .then(({ data }) => Promise.resolve(data));
  }

  static async update(id: string, data: Partial<Contributor>) {
    return authAxios.put(
      `/member/${id}`,
      {
        ...data,
        segments: getSegments(),
      },
    ).then(({ data }) => Promise.resolve(data));
  }

  static async create(data: Partial<Contributor>, segments: string[]) {
    const response = await authAxios.post(
      '/member',
      {
        ...data,
        segments,
      },
    );

    return response.data;
  }
}

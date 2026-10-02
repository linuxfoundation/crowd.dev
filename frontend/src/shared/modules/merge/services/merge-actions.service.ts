import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import authAxios from '@/shared/axios/auth-axios';
import type { MergeAction } from '@/shared/modules/merge/types/MemberActions';
import { storeToRefs } from 'pinia';

const getSelectedProjectGroup = () => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

  return selectedProjectGroup.value;
};

export class MergeActionsService {
  static async list(entityId: string, type: string = 'member', limit = 1): Promise<MergeAction[]> {
    const response = await authAxios.get(
      '/mergeActions',
      {
        params: {
          entityId,
          type,
          limit,
          segments: getSelectedProjectGroup()?.id ? [getSelectedProjectGroup()?.id] : [],
        },
      },
    );

    return response.data;
  }
}

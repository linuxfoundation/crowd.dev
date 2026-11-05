import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import type {
  MultiSelectAsyncFilterConfig,
  MultiSelectAsyncFilterData,
  MultiSelectAsyncFilterOptions,
  MultiSelectAsyncFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectAsyncFilterConfig';
import { MemberService } from '@/modules/member/member-service';
import { DEFAULT_MEMBER_FILTERS } from '@/modules/member/store/constants';
import type { Member } from '@/modules/member/types/Member';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

const member: MultiSelectAsyncFilterConfig = {
  id: 'member',
  label: 'Person',
  iconClass: 'circle-user',
  type: FilterConfigType.MULTISELECT_ASYNC,
  options: {
    remoteMethod: (query) => {
      const lsSegmentsStore = useLfSegmentsStore();
      const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

      return MemberService.listMembersAutocomplete({
        query,
        limit: 10,
        segments: selectedProjectGroup.value ? [selectedProjectGroup.value.id] : null,
      });
    },
    remotePopulateItems: (ids: string[]) =>
      MemberService.listMembers({
        filter: {
          and: [
            ...DEFAULT_MEMBER_FILTERS,
            {
              id: { in: ids },
            },
          ],
        },
        orderBy: null,
        limit: ids.length,
        offset: 0,
      }).then(({ rows }: { rows: Member[] }) =>
        rows.map((member) => ({
          label: member.displayName,
          value: member.id,
          logo: member.attributes?.avatarUrl?.default || null,
        })),
      ),
  },
  itemLabelRenderer(
    value: MultiSelectAsyncFilterValue,
    options: MultiSelectAsyncFilterOptions,
    data: MultiSelectAsyncFilterData,
  ): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT_ASYNC](
      'Person',
      value,
      options,
      data,
    );
  },
  apiFilterRenderer({ value, include }: MultiSelectAsyncFilterValue): ApiFilter[] {
    const filter = {
      memberId: { in: value },
    };
    return [include ? filter : { not: filter }];
  },
};

export default member;

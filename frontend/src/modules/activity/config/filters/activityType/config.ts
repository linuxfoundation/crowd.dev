import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import ActivityTypeFilter from '@/shared/modules/filters/components/filterTypes/ActivityTypeFilter.vue';
import type {
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { queryUrlParserByType } from '@/shared/modules/filters/config/queryUrlParserByType';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

const activityType: CustomFilterConfig = {
  id: 'activityType',
  label: 'Activity type',
  iconClass: 'list-timeline',
  type: FilterConfigType.CUSTOM,
  component: ActivityTypeFilter,
  options: {},
  queryUrlParser: queryUrlParserByType[FilterConfigType.MULTISELECT],
  itemLabelRenderer(
    value: MultiSelectFilterValue,
    options: MultiSelectFilterOptions,
    data: unknown,
  ): string {
    // shortcut: passes data where options are expected, as on main. revisit: when custom label renderers take data separately
    return itemLabelRendererByType[FilterConfigType.MULTISELECT](
      'Activity type',
      value,
      data as MultiSelectFilterOptions,
      null,
      {
        addGroupLabel: true,
      },
    );
  },
  apiFilterRenderer({ value, include }: MultiSelectFilterValue): ApiFilter[] {
    const filter = {
      or: value.map((type) => {
        const [platform, activityType] = type.split(':');
        return {
          platform: { eq: platform },
          type: { eq: activityType },
        };
      }),
    };
    return [include ? filter : { not: filter }];
  },
};

export default activityType;

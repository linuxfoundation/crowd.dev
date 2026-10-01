import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import ActivityTypeFilter from '@/modules/activity/config/filters/activityType/ActivityTypeFilter.vue';
import type {
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { queryUrlParserByType } from '@/shared/modules/filters/config/queryUrlParserByType';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';

const activityType: CustomFilterConfig = {
  id: 'activityType',
  label: 'Activity type',
  iconClass: 'list-timeline',
  type: FilterConfigType.CUSTOM,
  component: ActivityTypeFilter,
  options: {
  },
  queryUrlParser: queryUrlParserByType[FilterConfigType.MULTISELECT],
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions, data: any): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Activity type', value, data, null, {
      addGroupLabel: true,
    });
  },
  apiFilterRenderer({ value, include }:MultiSelectFilterValue): any[] {
    const filter = {
      or: value.map((type) => {
        const [platform, activityType] = type.split(':');
        return {
          platform: { eq: platform },
          type: { eq: activityType },
        };
      }),
    };
    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default activityType;

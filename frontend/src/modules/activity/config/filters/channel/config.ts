import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import ChannelFilter from '@/modules/activity/config/filters/channel/ChannelFilter.vue';
import { queryUrlParserByType } from '@/shared/modules/filters/config/queryUrlParserByType';
import type {
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';

const channel: CustomFilterConfig = {
  id: 'channel',
  label: 'Channel',
  iconClass: 'messages',
  type: FilterConfigType.CUSTOM,
  component: ChannelFilter,
  options: {
  },
  queryUrlParser: queryUrlParserByType[FilterConfigType.MULTISELECT],
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions, data: any): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Channel', value, data);
  },
  apiFilterRenderer({ value, include }:MultiSelectFilterValue): any[] {
    const filter = {
      or: value.map((v) => ({ channel: { eq: v } })),
    };

    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default channel;

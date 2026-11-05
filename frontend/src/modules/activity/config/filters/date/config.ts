import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  SelectFilterConfig,
  SelectFilterOptions,
  SelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import options from './options';

const date: SelectFilterConfig = {
  id: 'date',
  label: 'Date',
  iconClass: 'calendar',
  type: FilterConfigType.SELECT,
  options: {
    options,
  },
  itemLabelRenderer(value: SelectFilterValue, options: SelectFilterOptions, data: unknown): string {
    return itemLabelRendererByType[FilterConfigType.SELECT]('Date', value, options, data);
  },
  apiFilterRenderer({ value, include }: SelectFilterValue): ApiFilter[] {
    const filter = {
      timestamp: { gte: value },
    };
    return [include ? filter : { not: filter }];
  },
};

export default date;

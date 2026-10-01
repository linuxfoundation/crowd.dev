import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  DateFilterConfig,
  DateFilterOptions,
  DateFilterValue,
} from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';

const lastActivityDate: DateFilterConfig = {
  id: 'lastActivityDate',
  label: 'Last activity date',
  iconClass: 'calendar',
  type: FilterConfigType.DATE,
  options: {},
  itemLabelRenderer(value: DateFilterValue, options: DateFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.DATE]('Last activity date', value, options);
  },
  apiFilterRenderer(value: DateFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.DATE]('lastActive', value);
  },
};

export default lastActivityDate;

import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  DateFilterConfig,
  DateFilterOptions,
  DateFilterValue,
} from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

const joinedDate: DateFilterConfig = {
  id: 'joinedDate',
  label: 'Joined date',
  iconClass: 'calendar',
  type: FilterConfigType.DATE,
  options: {},
  itemLabelRenderer(value: DateFilterValue, options: DateFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.DATE]('Joined date', value, options);
  },
  apiFilterRenderer(value: DateFilterValue): ApiFilter[] {
    return apiFilterRendererByType[FilterConfigType.DATE]('joinedAt', value);
  },
};

export default joinedDate;

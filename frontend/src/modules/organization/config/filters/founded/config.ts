import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';
import type {
  DateFilterConfig,
  DateFilterOptions,
  DateFilterValue,
} from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';

const founded: DateFilterConfig = {
  id: 'founded',
  label: 'Founded',
  iconClass: 'flag',
  type: FilterConfigType.DATE,
  options: {
    datepickerType: 'year',
    dateFormat: 'YYYY',
  },
  itemLabelRenderer(value: DateFilterValue, options: DateFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.DATE]('Founded', value, options);
  },
  apiFilterRenderer(value: DateFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.NUMBER]('founded', value);
  },
};

export default founded;

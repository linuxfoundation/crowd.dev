import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';
import type {
  DateFilterConfig,
  DateFilterOptions,
  DateFilterValue,
} from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import type { NumberFilterValue } from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

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
  apiFilterRenderer(value: DateFilterValue): ApiFilter[] {
    // shortcut: date value goes through the number renderer, as before. revisit: fix the founded filter renderer
    return apiFilterRendererByType[FilterConfigType.NUMBER](
      'founded',
      value as unknown as NumberFilterValue,
    );
  },
};

export default founded;

import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  BooleanFilterConfig, BooleanFilterOptions,
  BooleanFilterValue,
} from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';

const unaffiliated: BooleanFilterConfig = {
  id: 'unaffiliated',
  label: 'Unaffiliated profile',
  iconClass: 'id-card',
  type: FilterConfigType.BOOLEAN,
  options: {},
  itemLabelRenderer(value: BooleanFilterValue, options: BooleanFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.BOOLEAN]('Unaffiliated profile', value, options);
  },
  apiFilterRenderer({ value }: BooleanFilterValue): any[] {
    const filter = {
      organizations: {
        [value ? 'eq' : 'ne']: null,
      },
    };
    return [
      filter,
    ];
  },
};

export default unaffiliated;

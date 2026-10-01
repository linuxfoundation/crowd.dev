import type {
  BooleanFilterConfig,
  BooleanFilterOptions,
  BooleanFilterValue,
} from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';

const location: BooleanFilterConfig = {
  id: 'lfxMembership',
  label: 'Linux Foundation member',
  iconClass: 'circle-star',
  type: FilterConfigType.BOOLEAN,
  options: {},
  itemLabelRenderer(value: BooleanFilterValue, options: BooleanFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.BOOLEAN]('Linux Foundation member', value, options);
  },
  apiFilterRenderer({ value }: BooleanFilterValue): any[] {
    const filter = {
      lfxMembership: {
        [value ? 'ne' : 'eq']: null,
      },
    };

    return [
      filter,
    ];
  },
};

export default location;

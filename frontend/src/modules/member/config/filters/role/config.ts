import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import options from './options';

const role: MultiSelectFilterConfig = {
  id: 'role',
  label: 'Role',
  iconClass: 'id-card',
  type: FilterConfigType.MULTISELECT,
  options: {
    options,
  },
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Role', value, options);
  },
  apiFilterRenderer({ value, include }: MultiSelectFilterValue): any[] {
    const filter = {
      role: {
        in: value,
      },
    };
    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default role;

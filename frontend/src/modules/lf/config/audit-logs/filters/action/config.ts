import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import type {
  MultiSelectFilterConfig, MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import options from './options';

const action: MultiSelectFilterConfig = {
  id: 'action',
  label: 'Action',
  iconClass: 'arrow-pointer',
  type: FilterConfigType.MULTISELECT,
  options: {
    options,
  },
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions, data: any): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Action', value, options, data);
  },
  apiFilterRenderer({ value, include }: MultiSelectFilterValue): any[] {
    const filter = {
      actionType: { in: value },
    };
    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default action;

import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import options from './options';

const type: MultiSelectFilterConfig = {
  id: 'type',
  label: 'Type',
  iconClass: 'landmark',
  type: FilterConfigType.MULTISELECT,
  options: {
    options,
  },
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Type', value, options);
  },
  apiFilterRenderer({ value, include }: MultiSelectFilterValue): any[] {
    const filter = {
      or: value.map((t) => ({ type: { eq: t } })),
    };
    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default type;

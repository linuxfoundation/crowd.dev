import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import options from './options';

const engagementLevel: MultiSelectFilterConfig = {
  id: 'engagementLevel',
  label: 'Engagement level',
  iconClass: 'signal-bars',
  type: FilterConfigType.MULTISELECT,
  options: {
    options,
  },
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Engagement level', value, options);
  },
  apiFilterRenderer({ value, include }: MultiSelectFilterValue): any[] {
    const filter = {
      score: {
        in: [
          ...(value.includes('silent') ? [0, 1] : []),
          ...(value.includes('quiet') ? [2, 3] : []),
          ...(value.includes('engaged') ? [4, 5, 6] : []),
          ...(value.includes('fan') ? [7, 8] : []),
          ...(value.includes('ultra') ? [9, 10] : []),
        ],
      },
    };
    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default engagementLevel;

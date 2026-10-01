import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import options from './options';

const avgSentiment: MultiSelectFilterConfig = {
  id: 'avgSentiment',
  label: 'Avg. sentiment',
  iconClass: 'gauge-high',
  type: FilterConfigType.MULTISELECT,
  options: {
    options,
  },
  itemLabelRenderer(
    value: MultiSelectFilterValue,
    options: MultiSelectFilterOptions,
  ): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT](
      'Avg. sentiment',
      value,
      options,
    );
  },
  apiFilterRenderer({ value, include }: MultiSelectFilterValue): any[] {
    const filter = {
      or: [
        ...(value.includes('positive')
          ? [{ averageSentiment: { gte: 67 } }]
          : []),
        ...(value.includes('negative')
          ? [{ averageSentiment: { lte: 33 } }]
          : []),
        ...(value.includes('neutral')
          ? [{ averageSentiment: { between: [33, 67] } }]
          : []),
      ],
    };
    return [include ? filter : { not: filter }];
  },
};

export default avgSentiment;

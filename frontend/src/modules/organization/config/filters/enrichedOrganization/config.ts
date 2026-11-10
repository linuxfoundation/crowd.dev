import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  BooleanFilterConfig, BooleanFilterOptions,
  BooleanFilterValue,
} from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';

const enrichedOrganization: BooleanFilterConfig = {
  id: 'enrichedOrganization',
  label: 'Enriched company',
  iconClass: 'sparkles fa-regular',
  type: FilterConfigType.BOOLEAN,
  options: {},
  itemLabelRenderer(value: BooleanFilterValue, options: BooleanFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.BOOLEAN]('Enriched company', value, options);
  },
  apiFilterRenderer({ value }: BooleanFilterValue) {
    const filter = {
      lastEnrichedAt: {
        [value ? 'eq' : 'ne']: null,
      },
    };
    return [
      { not: filter },
    ];
  },
};

export default enrichedOrganization;

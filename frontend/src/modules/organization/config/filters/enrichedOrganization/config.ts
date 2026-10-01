import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  BooleanFilterConfig, BooleanFilterOptions,
  BooleanFilterValue,
} from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';

const enrichedOrganization: BooleanFilterConfig = {
  id: 'enrichedOrganization',
  label: 'Enriched organization',
  iconClass: 'sparkles fa-regular',
  type: FilterConfigType.BOOLEAN,
  options: {},
  itemLabelRenderer(value: BooleanFilterValue, options: BooleanFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.BOOLEAN]('Enriched organization', value, options);
  },
  apiFilterRenderer({ value }: BooleanFilterValue): any[] {
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

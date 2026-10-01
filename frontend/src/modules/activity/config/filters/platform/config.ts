import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { lfIdentities } from '@/config/identities';

const platform: MultiSelectFilterConfig = {
  id: 'platform',
  label: 'Platform',
  iconClass: 'grid-round-2',
  type: FilterConfigType.MULTISELECT,
  options: {
    options: [
      {
        options: [
          ...(Object.values(lfIdentities).map((identity) => ({
            label: identity.name,
            value: identity.key,
          }))),
          {
            label: 'Other',
            value: 'other',
          },
        ],
      },
    ],
  },
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions, data: any): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Platform', value, options, data);
  },
  apiFilterRenderer({ value, include }: MultiSelectFilterValue): any[] {
    const filter = {
      platform: { in: value },
    };

    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default platform;

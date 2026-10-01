import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';
import { lfIdentities } from '@/config/identities';

const identities: MultiSelectFilterConfig = {
  id: 'identities',
  label: 'Identities',
  iconClass: 'fingerprint',
  type: FilterConfigType.MULTISELECT,
  options: {
    options: [
      {
        options: [
          ...Object.values(lfIdentities).map((identity) => ({
            label: identity.name,
            value: identity.key,
          })),
        ],
      },
    ],
  },
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Identities', value, options);
  },
  apiFilterRenderer(value: MultiSelectFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.MULTISELECT]('identityPlatforms', value);
  },
};

export default identities;

import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { lfIdentities } from '@/config/identities';

const activeOn: MultiSelectFilterConfig = {
  id: 'activeOn',
  label: 'Active on',
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
        ],
      },
    ],
  },
  itemLabelRenderer(value: MultiSelectFilterValue, options: MultiSelectFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT]('Active on', value, options);
  },
  apiFilterRenderer(value: MultiSelectFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.MULTISELECT]('activeOn', value);
  },
};

export default activeOn;

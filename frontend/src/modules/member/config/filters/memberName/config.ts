import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  StringFilterConfig,
  StringFilterOptions,
  StringFilterValue,
} from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';

const memberName: StringFilterConfig = {
  id: 'memberName',
  label: 'Person name',
  iconClass: 'circle-user',
  type: FilterConfigType.STRING,
  options: {},
  itemLabelRenderer(
    value: StringFilterValue,
    options: StringFilterOptions,
  ): string {
    return itemLabelRendererByType[FilterConfigType.STRING](
      'Person name',
      value,
      options,
    );
  },
  apiFilterRenderer(value: StringFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.STRING]('displayName', value);
  },
};

export default memberName;

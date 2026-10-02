import type {
  StringFilterConfig,
  StringFilterOptions,
  StringFilterValue,
} from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';

const headline: StringFilterConfig = {
  id: 'headline',
  label: 'Headline',
  iconClass: 'comment-lines',
  type: FilterConfigType.STRING,
  options: {},
  itemLabelRenderer(value: StringFilterValue, options: StringFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.STRING]('Headline', value, options);
  },
  apiFilterRenderer(value: StringFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.STRING]('headline', value);
  },
};

export default headline;

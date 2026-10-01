import type {
  NumberFilterConfig,
  NumberFilterOptions,
  NumberFilterValue,
} from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';

const noOfMembers: NumberFilterConfig = {
  id: 'noOfMembers',
  label: '# of people',
  iconClass: 'people-group',
  type: FilterConfigType.NUMBER,
  options: {},
  itemLabelRenderer(value: NumberFilterValue, options: NumberFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.NUMBER]('# of people', value, options);
  },
  apiFilterRenderer(value: NumberFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.NUMBER]('memberCount', value);
  },
};

export default noOfMembers;

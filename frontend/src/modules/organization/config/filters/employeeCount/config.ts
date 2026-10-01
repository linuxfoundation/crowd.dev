import type {
  NumberFilterConfig,
  NumberFilterOptions,
  NumberFilterValue,
} from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';

const employeeCount: NumberFilterConfig = {
  id: 'employeeCount',
  label: 'Employee Count',
  iconClass: 'building-user',
  type: FilterConfigType.NUMBER,
  options: {},
  itemLabelRenderer(value: NumberFilterValue, options: NumberFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.NUMBER]('Employee Count', value, options);
  },
  apiFilterRenderer(value: NumberFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.NUMBER]('employees', value);
  },
};

export default employeeCount;

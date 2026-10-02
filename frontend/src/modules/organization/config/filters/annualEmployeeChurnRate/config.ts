import type {
  NumberFilterConfig,
  NumberFilterOptions,
  NumberFilterValue,
} from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { apiFilterRendererByType } from '@/shared/modules/filters/config/apiFilterRendererByType';
import {
  decimal, required,
} from '@vuelidate/validators';

const valueParser = (value) => value / 100;

const annualEmployeeChurnRate: NumberFilterConfig = {
  id: 'annualEmployeeChurnRate',
  label: 'Annual Employee Churn Rate',
  iconClass: 'circle-arrow-down-right',
  type: FilterConfigType.NUMBER,
  options: {
    suffix: '%',
    validators: {
      required,
      decimal,
    },
  },
  itemLabelRenderer(value: NumberFilterValue, options: NumberFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.NUMBER]('Annual Employee Churn Rate', value, options);
  },
  apiFilterRenderer(value: NumberFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.NUMBER]('employeeChurnRate12Month', value, valueParser);
  },
};

export default annualEmployeeChurnRate;

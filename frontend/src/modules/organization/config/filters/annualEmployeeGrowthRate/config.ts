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

const annualEmployeeGrowthRate: NumberFilterConfig = {
  id: 'annualEmployeeGrowthRate',
  label: 'Annual Employee Growth Rate',
  iconClass: 'circle-arrow-up-right',
  type: FilterConfigType.NUMBER,
  options: {
    suffix: '%',
    validators: {
      required,
      decimal,
    },
  },
  itemLabelRenderer(value: NumberFilterValue, options: NumberFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.NUMBER]('Annual Employee Growth Rate', value, options);
  },
  apiFilterRenderer(value: NumberFilterValue): any[] {
    return apiFilterRendererByType[FilterConfigType.NUMBER]('employeeGrowthRate12Month', value, valueParser);
  },
};

export default annualEmployeeGrowthRate;

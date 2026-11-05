import type {
  NumberFilterConfig,
  NumberFilterOptions,
  NumberFilterValue,
} from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { numberApiFilterRenderer } from '@/shared/modules/filters/config/apiFilterRenderer/number.filter.renderer';
import {
  decimal, required,
} from '@vuelidate/validators';

const valueParser = (value: number) => value / 100;

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
  apiFilterRenderer(value: NumberFilterValue) {
    return numberApiFilterRenderer('employeeGrowthRate12Month', value, valueParser);
  },
};

export default annualEmployeeGrowthRate;

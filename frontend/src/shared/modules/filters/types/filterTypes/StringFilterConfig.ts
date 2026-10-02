/* eslint-disable no-unused-vars */
import type { BaseFilterConfig, FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { FilterStringOperator } from '@/shared/modules/filters/config/constants/string.constants';

export interface StringFilterOptions {
  fixedOperator?: FilterStringOperator;
}

export interface StringFilterValue {
  operator: FilterStringOperator,
  value: string,
}

export interface StringFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.STRING;
  options: StringFilterOptions;
  itemLabelRenderer: (value: StringFilterValue, options: StringFilterOptions, data: any) => string;
  apiFilterRenderer: (value: StringFilterValue) => any[];
}

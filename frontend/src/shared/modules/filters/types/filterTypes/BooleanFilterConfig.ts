/* eslint-disable no-unused-vars */
import type { BaseFilterConfig, FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';

export interface BooleanFilterOptions {
}

export interface BooleanFilterValue {
  value: boolean,
}

export interface BooleanFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.BOOLEAN;
  options: BooleanFilterOptions;
  itemLabelRenderer: (value: BooleanFilterValue, options: BooleanFilterOptions, data: any) => string;
  apiFilterRenderer: (value: BooleanFilterValue) => any[];
}

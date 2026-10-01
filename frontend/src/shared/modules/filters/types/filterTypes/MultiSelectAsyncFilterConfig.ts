/* eslint-disable no-unused-vars */
import type { BaseFilterConfig, FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';

export interface MultiSelectAsyncFilterOption {
  label: string;
  value: string;
  prefix?: string;
  description?: string;
}
export interface MultiSelectAsyncFilterOptions {
  hideIncludeSwitch?: boolean;
  remoteMethod: (query: string) => Promise<MultiSelectAsyncFilterOption[]>
  remotePopulateItems: (values: string[]) => Promise<MultiSelectAsyncFilterOption[]>
}

export interface MultiSelectAsyncFilterValue {
  value: string[],
  include: boolean,
}

export interface MultiSelectAsyncFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.MULTISELECT_ASYNC;
  options: MultiSelectAsyncFilterOptions;
  itemLabelRenderer: (value: MultiSelectAsyncFilterValue, options: MultiSelectAsyncFilterOptions, data: any) => string;
  apiFilterRenderer: (value: MultiSelectAsyncFilterValue) => any[];
}

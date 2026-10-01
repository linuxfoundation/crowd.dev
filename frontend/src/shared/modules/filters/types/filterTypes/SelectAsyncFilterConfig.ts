/* eslint-disable no-unused-vars */
import type { BaseFilterConfig, FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';

export interface SelectAsyncFilterOption {
  label: string;
  value: string;
  prefix?: string;
  description?: string;
}
export interface SelectAsyncFilterOptions {
  hideIncludeSwitch?: boolean;
  remoteMethod: (query: string) => Promise<SelectAsyncFilterOption[]>
  remotePopulateItems: (values: string) => Promise<SelectAsyncFilterOption>
}

export interface SelectAsyncFilterValue {
  value: string,
  include: boolean,
}

export interface SelectAsyncFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.SELECT_ASYNC;
  options: SelectAsyncFilterOptions;
  itemLabelRenderer: (value: SelectAsyncFilterValue, options: SelectAsyncFilterOptions, data: any) => string;
  apiFilterRenderer: (value: SelectAsyncFilterValue) => any[];
}

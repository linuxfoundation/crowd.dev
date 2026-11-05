import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type {
  BaseFilterConfig,
  FilterConfigType,
} from '@/shared/modules/filters/types/FilterConfig';
import type { Organization } from '@/modules/organization/types/Organization';

export interface MultiSelectAsyncFilterOption {
  label: string;
  value: string;
  prefix?: string;
  description?: string;
  lfxMembership?: Organization['lfxMembership'];
}
export interface MultiSelectAsyncFilterOptions {
  hideIncludeSwitch?: boolean;
  remoteMethod: (query: string) => Promise<MultiSelectAsyncFilterOption[]>;
  remotePopulateItems: (values: string[]) => Promise<MultiSelectAsyncFilterOption[]>;
}

export interface MultiSelectAsyncFilterValue {
  value: string[];
  include: boolean;
}

export interface MultiSelectAsyncFilterData {
  selected: MultiSelectAsyncFilterOption[];
}

export interface MultiSelectAsyncFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.MULTISELECT_ASYNC;
  options: MultiSelectAsyncFilterOptions;
  itemLabelRenderer: (
    value: MultiSelectAsyncFilterValue,
    options: MultiSelectAsyncFilterOptions,
    data: MultiSelectAsyncFilterData,
  ) => string;
  apiFilterRenderer: (value: MultiSelectAsyncFilterValue) => ApiFilter[];
}

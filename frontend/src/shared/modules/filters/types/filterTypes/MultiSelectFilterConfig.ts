import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type {
  BaseFilterConfig,
  FilterConfigType,
} from '@/shared/modules/filters/types/FilterConfig';

export interface MultiSelectFilterOption {
  label: string;
  value: string;
}
export interface MultiSelectFilterOptionGroup {
  label?: string;
  options: MultiSelectFilterOption[];
}
export interface MultiSelectFilterOptions {
  hideIncludeSwitch?: boolean;
  options: MultiSelectFilterOptionGroup[];
}

export interface MultiSelectFilterValue {
  value: string[];
  include: boolean;
}

export interface MultiSelectFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.MULTISELECT;
  options: MultiSelectFilterOptions;
  itemLabelRenderer: (
    value: MultiSelectFilterValue,
    options: MultiSelectFilterOptions,
    data: unknown,
  ) => string;
  apiFilterRenderer: (value: MultiSelectFilterValue) => ApiFilter[];
}

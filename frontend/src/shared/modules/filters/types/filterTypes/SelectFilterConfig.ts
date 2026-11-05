import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type {
  BaseFilterConfig,
  FilterConfigType,
} from '@/shared/modules/filters/types/FilterConfig';

interface SelectFilterOption {
  label: string;
  value: string;
}
export interface SelectFilterOptionGroup {
  label?: string;
  options: SelectFilterOption[];
}

export interface SelectFilterOptions {
  hideIncludeSwitch?: boolean;
  options: SelectFilterOptionGroup[];
}

export interface SelectFilterValue {
  value: string;
  include: boolean;
}

export interface SelectFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.SELECT;
  options: SelectFilterOptions;
  itemLabelRenderer: (
    value: SelectFilterValue,
    options: SelectFilterOptions,
    data: unknown,
  ) => string;
  apiFilterRenderer: (value: SelectFilterValue) => ApiFilter[];
}

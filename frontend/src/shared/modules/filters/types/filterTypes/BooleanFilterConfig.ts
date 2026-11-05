import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type {
  BaseFilterConfig,
  FilterConfigType,
} from '@/shared/modules/filters/types/FilterConfig';

export interface BooleanFilterOptions {}

export interface BooleanFilterValue {
  value: boolean;
}

export interface BooleanFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.BOOLEAN;
  options: BooleanFilterOptions;
  itemLabelRenderer: (
    value: BooleanFilterValue,
    options: BooleanFilterOptions,
    data: unknown,
  ) => string;
  apiFilterRenderer: (value: BooleanFilterValue) => ApiFilter[];
}

import type { NumberFilterConfig } from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import type { MultiSelectFilterConfig } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import type { SelectFilterConfig } from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';
import type { BooleanFilterConfig } from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import type { DateFilterConfig } from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import type { StringFilterConfig } from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import type { MultiSelectAsyncFilterConfig } from '@/shared/modules/filters/types/filterTypes/MultiSelectAsyncFilterConfig';
import type { SelectAsyncFilterConfig } from '@/shared/modules/filters/types/filterTypes/SelectAsyncFilterConfig';

export enum FilterConfigType {
  NUMBER = 'number',
  SELECT = 'select',
  SELECT_ASYNC = 'select-async',
  MULTISELECT = 'multiselect',
  MULTISELECT_ASYNC = 'multiselect-async',
  BOOLEAN = 'boolean',
  DATE = 'date',
  STRING = 'string',
  CUSTOM = 'custom',
}

export interface BaseFilterConfig {
  id: string;
  label: string;
  iconClass: string;
  inBody?: boolean;
}

export type FilterConfig = NumberFilterConfig
  | MultiSelectFilterConfig
  | MultiSelectAsyncFilterConfig
  | SelectFilterConfig
  | SelectAsyncFilterConfig
  | BooleanFilterConfig
  | DateFilterConfig
  | StringFilterConfig
  | CustomFilterConfig

export interface FilterStatic {
  search: string;
  relation: 'and' | 'or',
  order: {
    prop: string,
    order: 'descending' | 'ascending'
  },
  settings?: Record<string, any>
}

export type Filter = FilterStatic & Record<string, any>

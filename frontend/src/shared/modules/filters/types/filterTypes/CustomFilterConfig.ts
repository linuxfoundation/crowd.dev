import type { Component } from 'vue';
import type {
  BaseFilterConfig,
  FilterConfigType,
} from '@/shared/modules/filters/types/FilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

// Renderers use method syntax so configs declaring narrower value/options types stay assignable
export interface CustomFilterConfig<
  TValue = unknown,
  TOptions = unknown,
  TData = unknown,
> extends BaseFilterConfig {
  type: FilterConfigType.CUSTOM;
  component: Component;
  options: TOptions;
  queryUrlParser: ((value: never) => unknown) | null;
  itemLabelRenderer(value: TValue, options: TOptions, data: TData): string;
  apiFilterRenderer(value: TValue): ApiFilter[];
}

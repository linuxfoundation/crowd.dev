/* eslint-disable no-unused-vars */
import type {
  BaseFilterConfig,
  FilterConfigType,
} from '@/shared/modules/filters/types/FilterConfig';

export interface CustomFilterConfig extends BaseFilterConfig {
  type: FilterConfigType.CUSTOM;
  component: any;
  options: any;
  queryUrlParser: ((value: any) => Record<string, any>) | null;
  itemLabelRenderer: (value: any, options: any, data: any) => string;
  apiFilterRenderer: (value: any) => any[];
}

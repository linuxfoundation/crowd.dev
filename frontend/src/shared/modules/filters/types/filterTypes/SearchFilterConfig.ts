import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export interface SearchFilterConfig {
  placeholder?: string;
  apiFilterRenderer: (value: string) => ApiFilter[];
}

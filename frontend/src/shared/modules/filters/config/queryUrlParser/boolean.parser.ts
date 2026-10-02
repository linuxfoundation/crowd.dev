import type { BooleanFilterValue } from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';

interface QueryUrlBooleanValue {
  value: string,
}

export const booleanQueryUrlParser = (query: QueryUrlBooleanValue): BooleanFilterValue => ({
  value: query.value === 'true',
});

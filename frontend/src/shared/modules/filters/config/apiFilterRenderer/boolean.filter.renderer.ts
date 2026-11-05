import type { BooleanFilterValue } from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export const booleanApiFilterRenderer = (
  property: string,
  { value }: BooleanFilterValue,
): ApiFilter[] => [
  {
    [property]: value ? { eq: true } : { not: true },
  },
];

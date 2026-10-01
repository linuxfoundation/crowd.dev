import type { BooleanFilterValue } from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';

export const booleanApiFilterRenderer = (property: string, { value }: BooleanFilterValue): any[] => [
  {
    [property]: value ? { eq: true } : { not: true },
  },
];

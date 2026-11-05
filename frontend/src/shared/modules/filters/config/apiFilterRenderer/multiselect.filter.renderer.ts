import type { MultiSelectFilterValue } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export const multiSelectApiFilterRenderer = (
  property: string,
  { value, include }: MultiSelectFilterValue,
): ApiFilter[] => {
  const filter = {
    [property]: {
      contains: value,
    },
  };

  return [
    include
      ? filter
      : {
        not: filter,
      },
  ];
};

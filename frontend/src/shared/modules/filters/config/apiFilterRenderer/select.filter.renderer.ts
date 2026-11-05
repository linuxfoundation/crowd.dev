import type { SelectFilterValue } from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export const selectApiFilterRenderer = (
  property: string,
  { value, include }: SelectFilterValue,
): ApiFilter[] => {
  const filter = {
    [property]: { overlap: [value] },
  };
  return [include ? filter : { not: filter }];
};

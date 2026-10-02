import type { SelectFilterValue } from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';

export const selectApiFilterRenderer = (property: string, { value, include }: SelectFilterValue): any[] => {
  const filter = {
    [property]: { overlap: [value] },
  };
  return [
    (include ? filter : { not: filter }),
  ];
};

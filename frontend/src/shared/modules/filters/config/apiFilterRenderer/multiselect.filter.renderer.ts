import type { MultiSelectFilterValue } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

export const multiSelectApiFilterRenderer = (property: string, { value, include }: MultiSelectFilterValue): any[] => {
  const filter = {
    [property]: {
      contains: value,
    },
  };

  return [
    (include ? filter : {
      not: filter,
    }),
  ];
};

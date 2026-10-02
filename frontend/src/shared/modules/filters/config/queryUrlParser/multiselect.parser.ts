import type { MultiSelectFilterValue } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

interface QueryUrlMultiSelectValue {
  value: string,
  include: string,
}

export const multiSelectQueryUrlParser = (query: QueryUrlMultiSelectValue): MultiSelectFilterValue => ({
  ...query,
  value: query.value.split(','),
  include: query.include === 'true',
});

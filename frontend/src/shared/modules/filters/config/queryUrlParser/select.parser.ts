import type { SelectFilterValue } from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';

interface QueryUrlSelectValue {
  value: string,
  include: string,
}

export const selectQueryUrlParser = (query: QueryUrlSelectValue): SelectFilterValue => ({
  ...query,
  include: query.include === 'true',
});

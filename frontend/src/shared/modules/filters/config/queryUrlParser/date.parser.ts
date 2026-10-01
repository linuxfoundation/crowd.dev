import type { DateFilterValue } from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import { FilterDateOperator } from '@/shared/modules/filters/config/constants/date.constants';

interface QueryUrlDateValue {
  operator: string,
  value: string,
  include: string,
}

export const dateQueryUrlParser = (query: QueryUrlDateValue): DateFilterValue => ({
  ...query,
  operator: query.operator as FilterDateOperator,
  value: [FilterDateOperator.BETWEEN, FilterDateOperator.NOT_BETWEEN].includes(query.operator as FilterDateOperator)
    ? query.value.split(',')
    : query.value,
});

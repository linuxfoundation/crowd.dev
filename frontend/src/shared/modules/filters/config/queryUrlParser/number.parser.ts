import type { NumberFilterValue } from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterNumberOperator } from '@/shared/modules/filters/config/constants/number.constants';

interface QueryUrlNumberValue {
  operator: string,
  value: string,
  valueTo: string,
}

export const numberQueryUrlParser = (query: QueryUrlNumberValue): NumberFilterValue => {
  const obj: NumberFilterValue = {
    operator: query.operator as FilterNumberOperator,
    value: +query.value,
    valueTo: +query.valueTo || '',
  };
  if (![FilterNumberOperator.BETWEEN, FilterNumberOperator.NOT_BETWEEN].includes(obj.operator)) {
    delete obj.valueTo;
  }
  return obj;
};

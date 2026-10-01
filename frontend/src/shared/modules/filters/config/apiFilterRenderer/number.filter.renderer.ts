import type { NumberFilterValue } from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterNumberOperator } from '@/shared/modules/filters/config/constants/number.constants';

export const numberApiFilterRenderer = (
  property: string,
  {
    value, valueTo, operator,
  }: NumberFilterValue,
  parse: (value: number) => number,
): any[] => {
  const includeFilter = ![FilterNumberOperator.NE, FilterNumberOperator.NOT_BETWEEN].includes(operator);
  let filterOperator = operator;
  const parsedValue = parse ? parse(value) : +value;
  const parsedValueTo = parse ? parse(valueTo) : +valueTo;

  const filterValue = [FilterNumberOperator.BETWEEN, FilterNumberOperator.NOT_BETWEEN].includes(operator)
    ? [parsedValue, parsedValueTo!]
    : parsedValue;

  if (operator === FilterNumberOperator.NE) {
    filterOperator = FilterNumberOperator.EQ;
  } else if (operator === FilterNumberOperator.NOT_BETWEEN) {
    filterOperator = FilterNumberOperator.BETWEEN;
  }

  const filter = {
    [property]: {
      [filterOperator]: filterValue,
    },
  };

  return [
    (includeFilter ? filter : {
      not: filter,
    }),
  ];
};

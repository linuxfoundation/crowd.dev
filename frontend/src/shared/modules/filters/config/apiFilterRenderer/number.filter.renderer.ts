import type { NumberFilterValue } from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterNumberOperator } from '@/shared/modules/filters/config/constants/number.constants';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export const numberApiFilterRenderer = (
  property: string,
  { value, valueTo, operator }: NumberFilterValue,
  parse?: (value: number) => number,
): ApiFilter[] => {
  const includeFilter = ![FilterNumberOperator.NE, FilterNumberOperator.NOT_BETWEEN].includes(
    operator,
  );
  let filterOperator = operator;
  const parsedValue = parse ? parse(Number(value)) : Number(value);
  const parsedValueTo = parse ? parse(Number(valueTo)) : Number(valueTo);

  const filterValue = [FilterNumberOperator.BETWEEN, FilterNumberOperator.NOT_BETWEEN].includes(
    operator,
  )
    ? [parsedValue, parsedValueTo]
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
    includeFilter
      ? filter
      : {
        not: filter,
      },
  ];
};

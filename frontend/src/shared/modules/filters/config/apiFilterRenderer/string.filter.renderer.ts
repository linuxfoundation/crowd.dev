import type { StringFilterValue } from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import { FilterStringOperator } from '@/shared/modules/filters/config/constants/string.constants';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export const stringApiFilterRenderer = (
  property: string,
  { value, operator }: StringFilterValue,
): ApiFilter[] => {
  const includeFilter = ![FilterStringOperator.NLIKE].includes(operator);
  // Exception for "not contains" where there isn't a specific operator
  let filter: ApiFilter = {
    [operator]: value,
  };
  if (operator === FilterStringOperator.LIKE || operator === FilterStringOperator.NLIKE) {
    filter = {
      like: `${value}`,
    };
  }

  filter = {
    [property]: filter,
  };

  return [
    includeFilter
      ? filter
      : {
        not: filter,
      },
  ];
};

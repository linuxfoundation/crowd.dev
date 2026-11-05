import type { DateFilterValue } from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import {
  dateFilterTimePickerOptions,
  FilterDateOperator,
} from '@/shared/modules/filters/config/constants/date.constants';
import { dateHelper } from '@/shared/date-helper/date-helper';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export const dateApiFilterRenderer = (
  property: string,
  { value, operator }: DateFilterValue,
): ApiFilter[] => {
  const dateOption = dateFilterTimePickerOptions.find((option) => option.value === value);

  const mappedValue = dateOption ? dateOption.getDate() : value;

  const includeFilter = ![FilterDateOperator.NE, FilterDateOperator.NOT_BETWEEN].includes(operator);
  let filterOperator = operator;

  if (operator === FilterDateOperator.NE) {
    filterOperator = FilterDateOperator.EQ;
  } else if (operator === FilterDateOperator.NOT_BETWEEN) {
    filterOperator = FilterDateOperator.BETWEEN;
  }

  let filter;

  if ([FilterDateOperator.BETWEEN, FilterDateOperator.NOT_BETWEEN].includes(operator)) {
    filter = {
      [property]: {
        [filterOperator]: [
          dateHelper.utc(mappedValue[0]).startOf('day').toISOString(),
          dateHelper.utc(mappedValue[1]).endOf('day').toISOString(),
        ],
      },
    };
  } else if ([FilterDateOperator.EQ, FilterDateOperator.NE].includes(operator)) {
    // shortcut: these casts hide an array mappedValue, as on main. revisit: when filter values are typed per operator.
    filter = {
      [property]: {
        between: [
          dateHelper
            .utc(mappedValue as string)
            .startOf('day')
            .toISOString(),
          dateHelper
            .utc(mappedValue as string)
            .endOf('day')
            .toISOString(),
        ],
      },
    };
  } else {
    // shortcut: these casts hide an array mappedValue, as on main. revisit: when filter values are typed per operator.
    let parsedValue = dateHelper
      .utc(mappedValue as string)
      .startOf('day')
      .toISOString();

    if (['last24h'].includes(value as string)) {
      parsedValue = mappedValue as string;
    } else if ([FilterDateOperator.GT, FilterDateOperator.GTE].includes(operator)) {
      parsedValue = dateHelper
        .utc(mappedValue as string)
        .endOf('day')
        .toISOString();
    }

    filter = {
      [property]: {
        [filterOperator]: parsedValue,
      },
    };
  }

  return [
    includeFilter
      ? filter
      : {
        not: filter,
      },
  ];
};

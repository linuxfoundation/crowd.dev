import type { DateFilterValue } from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import {
  dateFilterTimePickerOptions,
  FilterDateOperator,
} from '@/shared/modules/filters/config/constants/date.constants';
import { dateHelper } from '@/shared/date-helper/date-helper';

export const dateApiFilterRenderer = (property: string, { value, operator }: DateFilterValue): any[] => {
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
    filter = {
      [property]: {
        between: [
          dateHelper.utc(mappedValue).startOf('day').toISOString(),
          dateHelper.utc(mappedValue).endOf('day').toISOString(),
        ],
      },
    };
  } else {
    let parsedValue = dateHelper.utc(mappedValue).startOf('day').toISOString();

    if (['last24h'].includes(value as string)) {
      parsedValue = mappedValue as string;
    } else if ([FilterDateOperator.GT, FilterDateOperator.GTE].includes(operator)) {
      parsedValue = dateHelper.utc(mappedValue).endOf('day').toISOString();
    }

    filter = {
      [property]: {
        [filterOperator]: parsedValue,
      },
    };
  }

  return [
    (includeFilter ? filter : {
      not: filter,
    }),
  ];
};

import type { FilterOperator } from '@/shared/modules/filters/types/FilterOperator';

export enum FilterNumberOperator {
  EQ = 'eq',
  NE = 'ne',
  LT = 'lt',
  LTE = 'lte',
  GT = 'gt',
  GTE = 'gte',
  BETWEEN = 'between',
  NOT_BETWEEN = 'not-between',
}

export const numberFilterOperators: FilterOperator[] = [
  {
    label: 'is',
    value: FilterNumberOperator.EQ,
  },
  {
    label: 'is not',
    value: FilterNumberOperator.NE,
  },
  {
    label: 'less than',
    subLabel: '<',
    value: FilterNumberOperator.LT,
  },
  {
    label: 'equal or less than',
    subLabel: '<=',
    value: FilterNumberOperator.LTE,
  },
  {
    label: 'greater than',
    subLabel: '>',
    value: FilterNumberOperator.GT,
  },
  {
    label: 'equal or greater than',
    subLabel: '>=',
    value: FilterNumberOperator.GTE,
  },
  {
    label: 'between',
    value: FilterNumberOperator.BETWEEN,
  },
  {
    label: 'not between',
    value: FilterNumberOperator.NOT_BETWEEN,
  },
];

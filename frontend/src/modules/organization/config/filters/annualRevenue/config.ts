import type {
  NumberFilterConfig,
  NumberFilterValue,
} from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import AnnualRevenueFilter from '@/modules/organization/config/filters/annualRevenue/AnnualRevenueFilter.vue';
import {
  FilterNumberOperator,
  numberFilterOperators,
} from '@/shared/modules/filters/config/constants/number.constants';

const annualRevenue: NumberFilterConfig = {
  id: 'annualRevenue',
  label: 'Annual revenue',
  iconClass: 'circle-dollar',
  type: FilterConfigType.CUSTOM,
  component: AnnualRevenueFilter,
  options: {},
  itemLabelRenderer({
    value, operator, valueTo, include,
  }: NumberFilterValue): string {
    const excludeText = !include ? ' (exclude)' : '';
    const operatorObject = numberFilterOperators.find((o) => o.value === operator);
    let operandText = (operatorObject?.subLabel ? `${operatorObject.subLabel} ` : `${operatorObject?.label} ` || '');

    if ([FilterNumberOperator.BETWEEN].includes(operator)) {
      operandText = '';
    }

    const parsedValue = `$${value}M`;
    const parsedValueTo = `$${valueTo}M`;

    const isBetween = [FilterNumberOperator.BETWEEN].includes(operator);
    const valueText = isBetween ? `${operandText}${parsedValue} - ${parsedValueTo}` : `${operandText}${parsedValue}`;

    return `<b>Annual revenue${excludeText}:</b>${valueText || '...'}`;
  },
  apiFilterRenderer({
    value, valueTo, operator, include,
  }: NumberFilterValue): any[] {
    let filter;

    if ([FilterNumberOperator.BETWEEN].includes(operator)) {
      filter = {
        or: [
          {
            revenueRangeMax: {
              lte: valueTo,
            },
          },
          {
            revenueRangeMin: {
              gte: value,
            },
          },
        ],
      };
    } else {
      filter = {
        or: [
          {
            revenueRangeMax: {
              [operator]: value,
            },
          },
          {
            and: [
              {
                revenueRangeMax: {
                  eq: null,
                },
              },
              {
                revenueRangeMin: {
                  [operator]: value,
                },
              },
            ],
          },
        ],
      };
    }

    return [
      (include ? filter : {
        not: filter,
      }),
    ];
  },
};

export default annualRevenue;

import type { NumberFilterOptions, NumberFilterValue } from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import {
  FilterNumberOperator,
  numberFilterOperators,
} from '@/shared/modules/filters/config/constants/number.constants';

export const numberItemLabelRenderer = (
  property: string,
  {
    value, operator, valueTo,
  }: NumberFilterValue,
  options: NumberFilterOptions,
): string => {
  const { suffix = '' } = options;
  const operatorObject = numberFilterOperators.find((o) => o.value === operator);
  let operandText = (operatorObject?.subLabel ? `${operatorObject.subLabel} ` : `${operatorObject?.label} ` || '');

  if ([FilterNumberOperator.EQ, FilterNumberOperator.BETWEEN].includes(operator)) {
    operandText = '';
  }

  const isBetween = [FilterNumberOperator.BETWEEN, FilterNumberOperator.NOT_BETWEEN].includes(operator);
  const valueText = isBetween ? `${operandText}${value} - ${valueTo}${suffix}` : `${operandText}${value}${suffix}`;

  return `<b>${property}:</b>${valueText || '...'}`;
};

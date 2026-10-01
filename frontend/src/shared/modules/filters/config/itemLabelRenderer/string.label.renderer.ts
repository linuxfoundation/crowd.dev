import type { StringFilterValue } from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import { FilterStringOperator, stringOperatorLabels } from '@/shared/modules/filters/config/constants/string.constants';

export const stringItemLabelRenderer = (
  property: string,
  { value, operator }: StringFilterValue,
): string => {
  const valueText = operator !== FilterStringOperator.EQ ? `${stringOperatorLabels[operator]} ${value}` : value;

  return `<b>${property}:</b>${valueText}`;
};

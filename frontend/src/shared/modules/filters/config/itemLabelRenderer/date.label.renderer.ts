import type { DateFilterValue } from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import {
  dateFilterTimePickerOptions,
  FilterDateOperator,
} from '@/shared/modules/filters/config/constants/date.constants';

// shortcut: gte has no label and throws below (legacy). revisit: when gte is offered in the UI.
const operatorText: Partial<Record<FilterDateOperator, string>> = {
  [FilterDateOperator.EQ]: '',
  [FilterDateOperator.NE]: 'is not',
  [FilterDateOperator.LT]: 'before',
  [FilterDateOperator.GT]: 'after',
  [FilterDateOperator.BETWEEN]: '',
  [FilterDateOperator.NOT_BETWEEN]: 'not between',
};

export const dateItemLabelRenderer = (
  property: string,
  { value, operator }: DateFilterValue,
): string => {
  const dateOption = dateFilterTimePickerOptions.find(
    (option) => option.value === value && option.operator === operator,
  );
  let valueText = `${value}`;
  let operatorTextDisplay = operatorText[operator]!.length > 0 ? `${operatorText[operator]} ` : '';

  if (dateOption) {
    valueText = dateOption.label;
    operatorTextDisplay = '';
  } else {
    const isBetween = [FilterDateOperator.BETWEEN, FilterDateOperator.NOT_BETWEEN].includes(
      operator,
    );
    if (isBetween) {
      const [from, to] = value;
      valueText = `${from}<span class="fa-arrow-right fa-light text-base ml-1">&nbsp;</span>${to}`;
    }
  }

  return `<b>${property}:</b>${operatorTextDisplay}${valueText || '...'}`;
};

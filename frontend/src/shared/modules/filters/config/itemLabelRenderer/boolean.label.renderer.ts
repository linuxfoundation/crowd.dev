import type { BooleanFilterValue } from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';

export const booleanItemLabelRenderer = (property: string, { value }: BooleanFilterValue): string => {
  const valueText = value ? 'True' : 'False';
  return `<b>${property}:</b>${valueText}`;
};

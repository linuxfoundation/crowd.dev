import type {
  SelectAsyncFilterOptions,
  SelectAsyncFilterValue,
} from '@/shared/modules/filters/types/filterTypes/SelectAsyncFilterConfig';

export const selectAsyncItemLabelRenderer = (
  property: string,
  { value, include }: SelectAsyncFilterValue,
  options: SelectAsyncFilterOptions,
  data: any,
): string => {
  const excludeText = !include ? ' (exclude)' : '';

  const valueText = data.selected.value === value ? data.selected.label : '';
  return `<b>${property}${excludeText}:</b>${valueText || '...'}`;
};

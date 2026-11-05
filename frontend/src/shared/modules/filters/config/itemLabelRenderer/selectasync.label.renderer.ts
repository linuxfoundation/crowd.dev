import type {
  SelectAsyncFilterData,
  SelectAsyncFilterOptions,
  SelectAsyncFilterValue,
} from '@/shared/modules/filters/types/filterTypes/SelectAsyncFilterConfig';

export const selectAsyncItemLabelRenderer = (
  property: string,
  { value, include }: SelectAsyncFilterValue,
  options: SelectAsyncFilterOptions,
  data: SelectAsyncFilterData,
): string => {
  const excludeText = !include ? ' (exclude)' : '';

  const valueText = data.selected && data.selected.value === value ? data.selected.label : '';
  return `<b>${property}${excludeText}:</b>${valueText || '...'}`;
};

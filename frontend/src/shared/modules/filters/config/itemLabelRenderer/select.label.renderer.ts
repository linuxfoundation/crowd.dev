import type {
  SelectFilterOptions,
  SelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';

export const selectItemLabelRenderer = (property: string, { value, include }: SelectFilterValue, options: SelectFilterOptions, data: any, {
  addGroupLabel,
} = { addGroupLabel: false }): string => {
  const excludeText = !include ? ' (exclude)' : '';
  const option = options.options.map((g) => g.options).flat().find((o) => o.value === value);
  const group = options.options.find((g) => g.options.find((o) => o.value === value));
  return `<b>${property}${excludeText}:</b>${option?.label || value || '...'}${addGroupLabel ? ` (${group?.label})` : ''}`;
};

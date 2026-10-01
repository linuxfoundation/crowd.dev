import type {
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

export const multiSelectItemLabelRenderer = (
  property: string,
  { value, include }: MultiSelectFilterValue,
  options: MultiSelectFilterOptions,
): string => {
  const excludeText = !include ? ' (exclude)' : '';
  const labelObject = options.options
    .map((g) => g.options)
    .flat()
    .filter((option) => value.includes(option.value))
    .reduce((obj, option) => {
      // eslint-disable-next-line no-param-reassign
      obj[option.value] = option.label;
      return obj;
    }, {} as Record<string, string>);

  const charLimit = 30;

  const valueText = value.map((val) => labelObject[val]).join(', ');
  const trimmedValueText = valueText.length > charLimit ? `${valueText.substring(0, charLimit - 3)}...` : valueText;
  const tooltip = trimmedValueText.length < valueText.length ? `data-tooltip="${valueText}"` : '';
  return `<span ${tooltip}><b>${property}${excludeText}:</b>${trimmedValueText || '...'}</span>`;
};

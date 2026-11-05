/**
 * Precision: 1 digit
 * 12 -> 12
 * 1,200 -> 1.2K
 * 12,200 -> 12.2K
 * 122,200 -> 122.2K
 * 1,222,200 -> 1.2M
 * 12,000,000 -> 12M
 * 122,000,000 -> 122M
 * 1,222,000,000 -> 1.2B
 * 1,022,222,222 -> 1B
 */
export const formatNumberToCompact = (number: number | string | null | undefined): string => {
  if (typeof number !== 'number') {
    return '-';
  }

  return Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(number);
};

export const formatNumber = (number: number | string): string => number.toLocaleString('en-US');

export const formatPercentage = (percentage: number): string => `${Math.ceil(
  percentage < 0 ? percentage * -1 : percentage,
)} %`;

export const formatFloatToYears = (years: number): string => {
  const integerPart = Math.floor(years);
  const fractionalPart = Math.round((years - integerPart) * 12);

  let result = '';

  if (integerPart > 0) {
    result += `${integerPart} year${integerPart > 1 ? 's' : ''}`;
  }

  if (fractionalPart > 0) {
    if (result.length > 0) {
      result += ' and ';
    }
    result += `${fractionalPart} month${fractionalPart > 1 ? 's' : ''}`;
  }

  return result;
};

export const formatFloatToPercentage = (value: number): string => {
  const percentage = (value * 100).toFixed(2);

  return `${percentage}%`;
};

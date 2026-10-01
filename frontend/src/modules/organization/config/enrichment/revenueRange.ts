import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const getValue = (value?: number | null): string => {
  if (value === undefined || value === null) {
    return '';
  }

  return `$${value >= 1000 ? value / 1000 : value}${value >= 1000 ? 'B' : 'M'}`;
};

const getMiddle = (min: string, max: string): string => {
  if (min && max) {
    return '-';
  }

  if (min && !max) {
    return '+';
  }

  return '';
};

const revenueRange: OrganizationEnrichmentConfig = {
  name: 'revenueRange',
  label: 'Annual Revenue',
  type: AttributeType.STRING,
  showInForm: false,
  showInAttributes: false,
  formatValue: (value) => {
    if (!Object.keys(value || {}).length) {
      return '-';
    }

    const min = getValue(value.min);
    const max = getValue(value.max);

    return `${min}${getMiddle(min, max)}${max}`;
  },
};

export default revenueRange;

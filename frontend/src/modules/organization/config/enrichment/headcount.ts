import { AttributeType } from '@/modules/organization/types/Attributes';
import { toSentenceCase } from '@/utils/string';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const size: OrganizationEnrichmentConfig = {
  name: 'size',
  label: 'Headcount',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: false,
  formatValue: (value) => toSentenceCase(value),
};

export default size;

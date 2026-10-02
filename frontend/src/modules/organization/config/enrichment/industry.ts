import { AttributeType } from '@/modules/organization/types/Attributes';
import { toSentenceCase } from '@/utils/string';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const industry: OrganizationEnrichmentConfig = {
  name: 'industry',
  label: 'Industry',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => toSentenceCase(value),
};

export default industry;

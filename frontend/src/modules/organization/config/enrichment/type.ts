import { AttributeType } from '@/modules/organization/types/Attributes';
import { toSentenceCase } from '@/utils/string';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const type: OrganizationEnrichmentConfig = {
  name: 'type',
  label: 'Type',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => toSentenceCase(value),
};

export default type;

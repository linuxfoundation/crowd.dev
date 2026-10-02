import { AttributeType } from '@/modules/organization/types/Attributes';
import { toSentenceCase } from '@/utils/string';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const gicsSector: OrganizationEnrichmentConfig = {
  name: 'gicsSector',
  label: 'GICS Sector',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => toSentenceCase(value),
};

export default gicsSector;

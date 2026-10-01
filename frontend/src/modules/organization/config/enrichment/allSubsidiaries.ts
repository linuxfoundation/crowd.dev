import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const allSubsidiaries: OrganizationEnrichmentConfig = {
  name: 'allSubsidiaries',
  label: 'All subsidiaries',
  type: AttributeType.ARRAY,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => value,
};

export default allSubsidiaries;

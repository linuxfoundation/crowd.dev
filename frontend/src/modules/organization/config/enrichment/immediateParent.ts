import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const immediateParent: OrganizationEnrichmentConfig = {
  name: 'immediateParent',
  label: 'Immediate Parent',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => value,
};

export default immediateParent;

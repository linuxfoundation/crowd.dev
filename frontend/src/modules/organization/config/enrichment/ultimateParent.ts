import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const ultimateParent: OrganizationEnrichmentConfig = {
  name: 'ultimateParent',
  label: 'Ultimate Parent',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value: string) => value,
};

export default ultimateParent;

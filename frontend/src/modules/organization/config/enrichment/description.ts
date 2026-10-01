import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const description: OrganizationEnrichmentConfig = {
  name: 'description',
  label: 'Description',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => value,
};

export default description;

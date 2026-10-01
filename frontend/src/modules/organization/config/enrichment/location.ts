import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const location: OrganizationEnrichmentConfig = {
  name: 'location',
  label: 'Location',
  type: AttributeType.STRING,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => value,
};

export default location;

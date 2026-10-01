import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const naics: OrganizationEnrichmentConfig = {
  name: 'naics',
  label: 'Naics',
  type: AttributeType.String,
  showInForm: false,
  showInAttributes: false,
};

export default naics;

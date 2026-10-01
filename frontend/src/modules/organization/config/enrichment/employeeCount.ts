import { AttributeType } from '@/modules/organization/types/Attributes';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const employeeCount: OrganizationEnrichmentConfig = {
  name: 'employeeCount',
  label: 'Employee Count',
  type: AttributeType.NUMBER,
  showInForm: true,
  showInAttributes: true,
  formatValue: (value) => value,
};

export default employeeCount;

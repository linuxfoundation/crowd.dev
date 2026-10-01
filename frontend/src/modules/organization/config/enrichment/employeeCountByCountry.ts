import { AttributeType } from '@/modules/organization/types/Attributes';
import { toSentenceCase } from '@/utils/string';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const employeeCountByCountry: OrganizationEnrichmentConfig = {
  name: 'employeeCountByCountry',
  label: 'Employee Count by Country',
  type: AttributeType.JSON,
  showInForm: true,
  showInAttributes: true,
  formatValue: (val) => Object.entries(val).reduce((final, [key, value]) => ({
    ...final,
    [toSentenceCase(key)]: value,
  }), {}),
};

export default employeeCountByCountry;

import { AttributeType } from '@/modules/organization/types/Attributes';
import { formatDate } from '@/utils/date';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const employeeCountByMonth: OrganizationEnrichmentConfig = {
  name: 'employeeCountByMonth',
  label: 'Employee Count by Month',
  type: AttributeType.JSON,
  showInForm: true,
  showInAttributes: true,
  formatValue: (val) => Object.entries(val).reduce((final, [key, value]) => ({
    ...final,
    [formatDate({
      timestamp: key,
      format: 'MMMM YYYY',
    } as any)]: value,
  }), {}),
};

export default employeeCountByMonth;

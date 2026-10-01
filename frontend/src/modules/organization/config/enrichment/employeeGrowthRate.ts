import { AttributeType } from '@/modules/organization/types/Attributes';
import { formatFloatToPercentage } from '@/utils/number';
import { snakeToSentenceCase } from '@/utils/string';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const employeeGrowthRate: OrganizationEnrichmentConfig = {
  name: 'employeeGrowthRate',
  label: 'Employee Growth Rate',
  type: AttributeType.JSON,
  showInForm: true,
  showInAttributes: true,
  formatValue: (val) => ({
    [`${snakeToSentenceCase('12_month')}s`]: formatFloatToPercentage(val['12_month']),
  }),
};

export default employeeGrowthRate;

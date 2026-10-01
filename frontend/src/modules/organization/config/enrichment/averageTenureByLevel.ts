import { AttributeType } from '@/modules/organization/types/Attributes';
import { formatFloatToYears } from '@/utils/number';
import { snakeToSentenceCase } from '@/utils/string';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment/index';

const averageTenureByLevelLabels: Record<string, string> = {
  vp: 'VP',
  cxo: 'CXO',
};

const averageTenureByLevel: OrganizationEnrichmentConfig = {
  name: 'averageTenureByLevel',
  label: 'Average Tenure by Level',
  type: AttributeType.JSON,
  showInForm: true,
  showInAttributes: true,
  formatValue: (val) => Object.entries(val).reduce((final, [key, value]) => ({
    ...final,
    [averageTenureByLevelLabels[key] || snakeToSentenceCase(key)]: formatFloatToYears(value),
  }), {}),
};

export default averageTenureByLevel;

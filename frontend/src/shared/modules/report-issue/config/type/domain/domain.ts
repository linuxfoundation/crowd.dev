import type { ReportDataTypeConfig } from '@/shared/modules/report-issue/config';
import Domain from './type-domain.vue';

const domain: ReportDataTypeConfig = {
  description: (attribute: any) => `Domain: ${attribute.value}`,
  display: Domain,
};

export default domain;

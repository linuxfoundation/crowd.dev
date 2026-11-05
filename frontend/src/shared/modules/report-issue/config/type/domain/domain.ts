import type { ReportDataTypeConfig } from '@/shared/modules/report-issue/config';
import type { ContributorIdentity } from '@/modules/contributor/types/Contributor';
import type { OrganizationIdentity } from '@/modules/organization/types/Organization';
import Domain from './type-domain.vue';

const domain: ReportDataTypeConfig = {
  description: (attribute: ContributorIdentity | OrganizationIdentity) =>
    `Domain: ${attribute.value}`,
  display: Domain,
};

export default domain;

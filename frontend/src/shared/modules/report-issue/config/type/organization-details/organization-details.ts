import type { ReportDataTypeConfig } from '@/shared/modules/report-issue/config';
import type { Organization } from '@/modules/organization/types/Organization';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import OrganizationDetails from './type-organization-details.vue';

const { displayName } = useOrganizationHelpers();

export const organizationDetails: ReportDataTypeConfig = {
  description: (attribute: any, entity: Organization) => `Organization: ${displayName(entity)}`,
  display: OrganizationDetails,
};

export default organizationDetails;

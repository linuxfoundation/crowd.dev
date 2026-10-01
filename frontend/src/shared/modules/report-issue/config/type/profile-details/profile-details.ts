import type { ReportDataTypeConfig } from '@/shared/modules/report-issue/config';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import ProfileDetails from './type-profile-details.vue';

export const profileDetails: ReportDataTypeConfig = {
  description: (attribute: any, entity: Contributor) => `Person: ${entity.displayName}`,
  display: ProfileDetails,
};

export default profileDetails;

import type { ReportDataTypeConfig } from '@/shared/modules/report-issue/config';
import { lfIdentities } from '@/config/identities';
import type { ContributorIdentity } from '@/modules/contributor/types/Contributor';
import type { OrganizationIdentity } from '@/modules/organization/types/Organization';
import Identity from './type-identity.vue';

const identity: ReportDataTypeConfig = {
  description: (identity: ContributorIdentity | OrganizationIdentity) => {
    const platform = lfIdentities[identity.platform]?.name || identity.platform;
    return `Identity: ${platform} ${identity.value}`;
  },
  display: Identity,
};

export default identity;

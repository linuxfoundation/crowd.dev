import type { IdentityConfig } from '@/config/identities/index';
import { lfIdentities } from '@/config/identities/index';

const useIdentitiesHelpers = () => {
  const getPlatformsLabel = (platforms: string[]) => platforms
    .filter((platform) => !['integration_or_enrichment', 'email', 'integration', 'unknown', 'delete'].includes(platform))
    .map((platform) => {
      if (['enrichment', 'peopledatalabs'].includes(platform)) {
        return '<span class="fa-sparkles fa-light mr-0.5"></span> Enrichment';
      }
      if (platform === 'custom') {
        return 'Manually added';
      }
      return lfIdentities[platform]?.name || platform;
    }).join(', ');

  const uniqueKeys = () => [...new Set(Object.values(lfIdentities).map((identity) => identity.key))];

  const memberIdentities: IdentityConfig[] = uniqueKeys().map((key) => lfIdentities[key]).filter((identity) => !!identity.member);

  const organizationIdentities: IdentityConfig[] = uniqueKeys().map((key) => lfIdentities[key]).filter((identity) => !!identity.organization);

  return {
    getPlatformsLabel,
    memberIdentities,
    organizationIdentities,
  };
};

export default useIdentitiesHelpers;

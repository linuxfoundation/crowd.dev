import type { Organization } from '@/modules/organization/types/Organization';
import { OrganizationIdentityType } from '@/modules/organization/types/Organization';

export const getOrganizationWebsite = (organization: Organization) => {
  if (!organization) return null;

  const { identities } = organization;

  const verifiedPrimaryDomainIdentity = identities.find((i) => [OrganizationIdentityType.PRIMARY_DOMAIN].includes(i.type) && i.verified);

  if (verifiedPrimaryDomainIdentity) {
    return verifiedPrimaryDomainIdentity.value;
  }

  const unverifiedPrimaryDomainIdentity = identities.find((i) => [OrganizationIdentityType.PRIMARY_DOMAIN].includes(i.type) && !i.verified);

  return unverifiedPrimaryDomainIdentity?.value ?? null;
};

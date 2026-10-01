import type { Organization } from '@/modules/organization/types/Organization';
import { OrganizationIdentityType } from '@/modules/organization/types/Organization';
import organizationOrder from '@/shared/modules/identities/config/identitiesOrder/organization';
import { lfIdentities } from '@/config/identities';
import { dateHelper } from '@/shared/date-helper/date-helper';

const useOrganizationHelpers = () => {
  const displayName = (organization: Organization) => organization.attributes?.name?.default || organization.displayName;

  const logo = (organization: Organization) => organization.attributes?.logo?.default || organization.logo;

  const isNew = (organization: Organization) => {
    if (!organization.joinedAt) {
      return false;
    }
    return dateHelper().diff(dateHelper(organization.joinedAt), 'days')
      <= 14;
  };

  const identities = (organization: Organization, sort: string[] = organizationOrder.list) => organization.identities
    .filter((i) => OrganizationIdentityType.USERNAME === i.type)
    .sort((a, b) => {
      const aIndex = sort.indexOf(a.platform);
      const bIndex = sort.indexOf(b.platform);
      const aOrder = aIndex !== -1 ? aIndex : sort.length;
      const bOrder = bIndex !== -1 ? bIndex : sort.length;
      return aOrder - bOrder;
    })
    .map((i) => ({
      ...i,
      handle: lfIdentities[i.platform]?.organization?.handle?.(i) || '',
      url: lfIdentities[i.platform]?.organization?.url?.(i) || '',
    }));

  const emails = (organization: Organization) => {
    const emailData: Record<string, any> = organization.identities
      .filter((i) => OrganizationIdentityType.EMAIL === i.type)
      .reduce((obj, identity) => {
        const emailObject: Record<string, any> = { ...obj };
        if (!(identity.value in emailObject)) {
          emailObject[identity.value] = {
            ...identity,
            platforms: [],
          };
        }
        emailObject[identity.value].platforms.push(identity.platform);
        emailObject[identity.value].verified = emailObject[identity.value].verified || identity.verified;

        return emailObject;
      }, {});
    return Object.keys(emailData).map((email) => ({
      value: email,
      ...emailData[email],
    }));
  };

  const primaryDomains = (organization: Organization) => organization.identities
    .filter((i) => OrganizationIdentityType.PRIMARY_DOMAIN === i.type);

  const alternativeDomains = (organization: Organization) => organization.identities
    .filter((i) => OrganizationIdentityType.ALTERNATIVE_DOMAIN === i.type);

  const domains = (organization: Organization) => [
    ...primaryDomains(organization),
    ...alternativeDomains(organization),
  ];

  const website = (organization: Organization) => {
    const primary = primaryDomains(organization);
    const verified = primary.find((i) => i.verified);
    if (verified) {
      return verified;
    }
    return primary?.[0];
  };

  const affiliatedProfiles = (organization: Organization) => organization.identities
    .filter((i) => OrganizationIdentityType.AFFILIATED_PROFILE === i.type);

  const phoneNumbers = (organization: Organization) => organization.attributes.phoneNumber?.default || [];

  return {
    displayName,
    logo,
    isNew,
    identities,
    emails,
    primaryDomains,
    alternativeDomains,
    website,
    domains,
    affiliatedProfiles,
    phoneNumbers,
  };
};

export default useOrganizationHelpers;

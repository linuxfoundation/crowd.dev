import type {
  Organization,
  OrganizationIdentity,
} from '@/modules/organization/types/Organization';
import {
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import { Platform } from '@/shared/modules/platform/types/Platform';
import { withHttp } from '@/utils/string';
import { lfIdentities } from '@/config/identities';

export default ({
  organization,
  order,
}: {
  organization: Partial<Organization>;
  order: Platform[];
}) => {
  const { identities = [], phoneNumbers = [] } = organization || {};

  const getIdentityHandles = (platform: Platform) => {
    if ([Platform.CUSTOM, Platform.ENRICHMENT].includes(platform)) {
      const mainPlatforms = (Object.values(Platform) as string[]).filter(
        (p) => p !== Platform.CUSTOM && p !== Platform.ENRICHMENT,
      );

      return (identities || [])
        .filter(
          (i) => !mainPlatforms.includes(i.platform)
            && [OrganizationIdentityType.USERNAME].includes(i.type),
        )
        .map((i) => ({
          ...i,
          value: lfIdentities[platform]?.organization?.handle
            ? lfIdentities[platform]?.organization?.handle?.(i)
            : i.value,
        }));
    }

    return (identities || [])
      .filter(
        (i) => i.platform === platform
          && [OrganizationIdentityType.USERNAME].includes(i.type),
      )
      .map((i) => ({
        ...i,
        value: lfIdentities[platform]?.organization?.handle
          ? lfIdentities[platform]?.organization?.handle?.(i)
          : i.value,
      }));
  };

  const getIdentityLink = (
    identity: OrganizationIdentity,
    platform: string,
  ) => {
    if (!lfIdentities[platform]?.organization?.url) {
      return null;
    }

    return lfIdentities[platform]?.organization?.url?.(identity);
  };

  const getIdentities = (): {
    [key: string]: {
      handle: string;
      link: string | null;
      verified: boolean;
    }[];
  } => order.reduce((acc, platform) => {
    const handles = getIdentityHandles(platform);

    if ([Platform.CUSTOM, Platform.ENRICHMENT].includes(platform) && handles.length) {
      const sortedCustomIdentities = handles.sort((a, b) => {
        const platformComparison = a.platform.localeCompare(b.platform);

        if (platformComparison === 0) {
          // If platforms are equal, sort by name
          return a.value.localeCompare(b.value);
        }

        return platformComparison; // Otherwise, sort by platform
      });

      sortedCustomIdentities.forEach((identity) => {
        if (acc[identity.platform]?.length) {
          acc[identity.platform].push({
            handle: identity.value,
            link: getIdentityLink(identity, platform),
            verified: identity.verified,
          });
        } else {
          acc[identity.platform] = [
            {
              handle: identity.value,
              link: getIdentityLink(identity, platform),
              verified: identity.verified,
            },
          ];
        }
      });
    } else {
      const platformHandlesValues = handles.map((identity) => ({
        handle: identity.value,
        link: getIdentityLink(identity, platform),
        verified: identity.verified,
      }));

      if (platformHandlesValues.length) {
        acc[platform] = platformHandlesValues;
      }
    }

    return acc;
  }, {} as Record<Platform, { handle: string; link: string; verified: boolean }[]>);

  const getAffiliatedProfiles = (): {
    handle: string;
    link: string | null;
    verified: boolean;
  }[] => {
    const parsedIdentities = identities?.length ? identities : [];

    const identitiesDomains = parsedIdentities
      .filter((identity) => [
        OrganizationIdentityType.AFFILIATED_PROFILE,
      ].includes(identity.type))
      .map((identity) => ({
        link: null,
        handle: identity.value,
        verified: identity.verified,
      }));

    return identitiesDomains;
  };

  const getDomains = (): {
    handle: string;
    link: string | null;
    verified: boolean;
  }[] => {
    const parsedIdentities = identities?.length ? identities : [];

    const identitiesDomains = parsedIdentities
      .filter((identity) => [
        OrganizationIdentityType.PRIMARY_DOMAIN,
        OrganizationIdentityType.ALTERNATIVE_DOMAIN,
      ].includes(identity.type))
      .map((identity) => ({
        link: withHttp(identity.value) as string | null,
        handle: identity.value,
        verified: identity.verified,
      }));

    return identitiesDomains;
  };

  const getEmails = (): {
    handle: string;
    link: string | null;
    verified: boolean;
  }[] => {
    const parsedIdentities = identities?.length ? identities : [];

    const identitiesDomains = parsedIdentities
      .filter((identity) => [
        OrganizationIdentityType.EMAIL,
      ].includes(identity.type))
      .map((identity) => ({
        link: `mailto:${identity.value}`,
        handle: identity.value,
        verified: identity.verified,
      }));

    return identitiesDomains;
  };

  const getPhoneNumbers = (): {
    handle: string;
    link: string | null;
    verified: boolean;
  }[] => (phoneNumbers || []).map((p) => ({
    link: `tel:${p}`,
    handle: p,
    verified: false,
  }));

  return {
    getIdentities,
    getAffiliatedProfiles,
    getEmails,
    getDomains,
    getPhoneNumbers,
  };
};

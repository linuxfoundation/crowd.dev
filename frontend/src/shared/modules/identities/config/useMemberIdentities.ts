import type { Member } from '@/modules/member/types/Member';
import { Platform } from '@/shared/modules/platform/types/Platform';
import { lfIdentities } from '@/config/identities';

export default ({
  member,
  order,
}: {
  member: Partial<Member>;
  order: Platform[];
}) => {
  const {
    attributes = {}, identities,
  } = member || {};

  const getIdentityHandles = (platform: string) => {
    if (platform === Platform.CUSTOM) {
      const mainPlatforms = (Object.values(Platform) as string[]).filter((p) => p !== 'custom');
      return (identities || [])
        .filter((i) => !mainPlatforms.includes(i.platform) && i.type !== 'email')
        .map((i) => ({
          platform: i.platform,
          url: null,
          name: i.value,
          verified: i.verified,
        }));
    }
    return (identities || [])
      .filter((i) => i.platform === platform && i.type !== 'email')
      .map((i) => ({
        platform: i.platform,
        url: null,
        name: i.value,
        verified: i.verified,
      }));
  };

  const getIdentityLink = (identity: {
    platform: Platform;
    url: string | null;
    name: string;
    verified: boolean;
  }, platform: string) => {
    if (!lfIdentities[platform]?.member?.url) {
      return null;
    }

    return (
      identity.url
      ?? lfIdentities[platform]?.member?.url?.({
        identity: {
          platform: identity.platform,
          value: identity.name,
          verified: identity.verified,
        } as any,
        attributes,
      })
      ?? attributes?.url?.[platform as keyof typeof attributes.url]
      ?? null
    );
  };

  const getIdentities = (): {
    [key: string]: {
      handle: string;
      link: string | null;
      verified: boolean;
    }[];
  } => order.reduce((acc, platform) => {
    const handles = getIdentityHandles(platform);

    if (platform === Platform.CUSTOM && handles.length) {
      const sortedCustomIdentities = handles.sort((a, b) => {
        const platformComparison = a.platform.localeCompare(b.platform);

        if (platformComparison === 0) {
          // If platforms are equal, sort by name
          return a.name.localeCompare(b.name);
        }

        return platformComparison; // Otherwise, sort by platform
      });

      sortedCustomIdentities.forEach((identity) => {
        if (acc[identity.platform]?.length) {
          acc[identity.platform].push({
            handle: identity.name,
            link: getIdentityLink(identity, platform),
            verified: identity.verified,
          });
        } else {
          acc[identity.platform] = [
            {
              handle: identity.name,
              link: getIdentityLink(identity, platform),
              verified: identity.verified,
            },
          ];
        }
      });
    } else {
      const platformHandlesValues = handles.map((identity) => ({
        handle: identity.name,
        link: getIdentityLink(identity, platform),
        verified: identity.verified,
      }));

      if (platformHandlesValues.length) {
        acc[platform] = platformHandlesValues;
      }
    }

    return acc;
  }, {} as Record<Platform, { handle: string; link: string | null; verified: boolean }[]>);

  const getEmails = (): {
    handle: string;
    link: string | null;
    verified: boolean;
  }[] => (identities || [])
    .filter((i) => i.type === 'email')
    .map((i) => ({
      link: `mailto:${i.value}`,
      handle: i.value,
      verified: i.verified,
      platform: i.platform,
    }))
    .sort((a, b) => {
      const indexA = order.findIndex((p) => p === a.platform);
      const indexB = order.findIndex((p) => p === b.platform);

      const orderA = indexA === -1 ? order.length : indexA;
      const orderB = indexB === -1 ? order.length : indexB;

      return orderA - orderB;
    });

  return {
    getIdentities,
    getEmails,
  };
};

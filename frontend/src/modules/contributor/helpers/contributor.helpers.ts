import { lfIdentities } from '@/config/identities';
import type { Contributor, ContributorIdentity } from '@/modules/contributor/types/Contributor';
import type { MemberOrganization } from '@/modules/organization/types/Organization';
import { dateHelper } from '@/shared/date-helper/date-helper';
import {
  getVerificationStatus,
  type VerificationEntity,
} from '@/shared/modules/verification/verification.helpers';
import memberOrder from '@/shared/modules/identities/config/identitiesOrder/member';

type IdentityFields = Pick<ContributorIdentity, 'platform' | 'type' | 'value'>;
type IdentitiesInput<I> = Pick<Contributor, 'attributes'> & { identities: I[] | null };

const VERIFICATION_RANK = { verified: 2, 'auto-verified': 1, unverified: 0 };
const verificationRank = (identity: VerificationEntity) =>
  VERIFICATION_RANK[getVerificationStatus(identity)];

const useContributorHelpers = () => {
  const avatar = (contributor: Pick<Contributor, 'attributes'>) =>
    contributor.attributes?.avatarUrl?.default;

  const isTeamMember = (contributor: Contributor) =>
    !!contributor.attributes?.isTeamMember?.default;

  const isBot = (contributor: Contributor) => !!contributor.attributes?.isBot?.default;

  const isNew = (contributor: Pick<Contributor, 'joinedAt'>) => {
    if (!contributor.joinedAt) {
      return false;
    }
    return dateHelper().diff(dateHelper(contributor.joinedAt), 'days') <= 14;
  };

  const identities = <I extends IdentityFields>(
    contributor: IdentitiesInput<I>,
    sort: string[] = memberOrder.list,
  ) =>
      contributor.identities
        ?.filter((i) => i.type !== 'email')
        .sort((a, b) => {
          const aIndex = sort.indexOf(a.platform);
          const bIndex = sort.indexOf(b.platform);
          const aOrder = aIndex !== -1 ? aIndex : sort.length;
          const bOrder = bIndex !== -1 ? bIndex : sort.length;
          return aOrder - bOrder;
        })
        .map((i): Omit<I, 'url'> & { url: string | null } => {
          const config = lfIdentities[i.platform];

          const link = config?.member?.url?.({
            identity: i,
            attributes: contributor.attributes,
          });
          return {
            ...i,
            url: link || null,
          };
        }) || [];

  const emails = (contributor: Contributor) => {
    const emailsdata = (contributor.identities || []).reduce(
      (
        obj: Record<
          string,
          ContributorIdentity & { platforms: string[]; best: ContributorIdentity }
        >,
        identity: ContributorIdentity,
      ) => {
        if (identity.type !== 'email') {
          return obj;
        }
        const emailObject = { ...obj };
        if (!(identity.value in emailObject)) {
          emailObject[identity.value] = {
            ...identity,
            platforms: [],
            best: identity,
          };
        }
        const merged = emailObject[identity.value];
        merged.platforms.push(identity.platform);
        merged.verified = merged.verified || identity.verified;

        if (verificationRank(identity) > verificationRank(merged.best)) {
          merged.best = identity;
        }

        return emailObject;
      },
      {},
    );
    return Object.keys(emailsdata).map((email) => {
      const { best, ...merged } = emailsdata[email];
      return {
        url: `mailto:${email}`,
        ...merged,
        value: email,
        verifiedBy: best.verifiedBy,
        verifiedAt: best.verifiedAt,
        updatedAt: best.updatedAt,
      };
    });
  };

  const activeOrganization = (contributor: Contributor) => contributor.organizations;

  // Mirrors sortActiveOrganizations in services/libs/data-access-layer/src/members/dataProcessor.ts
  const activeProfileOrganization = (contributor: Contributor) => {
    const time = (date?: string) => (date ? new Date(date).getTime() : 0);
    const range = (mo: MemberOrganization) =>
      (mo.dateEnd ? time(mo.dateEnd) : Date.parse('9999-12-31')) - time(mo.dateStart);

    return (
      (contributor.organizations || [])
        .filter((org) => !org.memberOrganizations.dateEnd)
        .sort((a, b) => {
          const aMo = a.memberOrganizations;
          const bMo = b.memberOrganizations;
          const rank =
            Number(bMo.affiliationOverride?.allowAffiliation !== false) -
              Number(aMo.affiliationOverride?.allowAffiliation !== false) ||
            Number(bMo.affiliationOverride?.isPrimaryWorkExperience === true) -
              Number(aMo.affiliationOverride?.isPrimaryWorkExperience === true) ||
            Number(!!bMo.dateStart) - Number(!!aMo.dateStart);

          if (rank) {
            return rank;
          }
          if (aMo.dateStart) {
            return range(bMo) - range(aMo) || aMo.id.localeCompare(bMo.id);
          }
          return (
            time(b.createdAt) - time(a.createdAt) ||
            (a.displayName || '').localeCompare(b.displayName || '', undefined, {
              sensitivity: 'base',
            }) ||
            aMo.id.localeCompare(bMo.id)
          );
        })[0] || null
    );
  };

  return {
    avatar,
    isTeamMember,
    isBot,
    isNew,
    identities,
    emails,
    activeOrganization,
    activeProfileOrganization,
  };
};

export default useContributorHelpers;

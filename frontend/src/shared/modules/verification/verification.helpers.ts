import { lfIdentities } from '@/config/identities';
import { OrganizationSource } from '@/modules/organization/types/Organization';
import { dateHelper } from '@/shared/date-helper/date-helper';

export type VerificationStatus = 'unverified' | 'verified' | 'auto-verified';
export type VerificationKind = 'identity' | 'work-experience';

export interface VerificationEntity {
  verified?: boolean | null;
  verifiedBy?: string | null;
  verifiedAt?: string | null;
  updatedAt?: string | null;
  source?: string | null;
  rowSource?: string | null;
}

export interface VerificationSource {
  label: string;
  icon?: string;
  image?: string;
}

// Backend PlatformType values (the FE Platform enum differs) plus non-platform system writers
const SYSTEM_VERIFIERS = new Set<string>([
  'devto',
  'slack',
  'discord',
  'github',
  'github-nango',
  'twitter',
  'reddit',
  'hackernews',
  'linkedin',
  'crowd',
  'enrichment',
  'hashnode',
  'kaggle',
  'medium',
  'producthunt',
  'youtube',
  'stackoverflow',
  'discourse',
  'git',
  'crunchbase',
  'groupsio',
  'mailinglist',
  'committees',
  'confluence',
  'gerrit',
  'jira',
  'lfid',
  'cvent',
  'tnc',
  'meetings',
  'gitlab',
  'facebook',
  'other',
  'octolens',
  'credly',
  'lf-auth0',
  'lfxOne',
]);

const SOURCE_LABELS: Record<string, string> = {
  [OrganizationSource.EMAIL_DOMAIN]: 'Email domain',
  [OrganizationSource.PROJECT_REGISTRY]: 'Project registry',
  [OrganizationSource.UI]: 'Manual',
  [OrganizationSource.LFX_ONE]: 'LFX One',
  [OrganizationSource.CVENT]: 'Cvent',
  [OrganizationSource.TNC]: 'TNC',
  [OrganizationSource.MEETINGS]: 'Meetings',
  [OrganizationSource.COMMITTEES]: 'Committees',
};

export const getVerificationStatus = (entity: VerificationEntity): VerificationStatus => {
  if (!entity.verified) {
    return 'unverified';
  }
  if (!entity.verifiedBy || SYSTEM_VERIFIERS.has(entity.verifiedBy)) {
    return 'auto-verified';
  }
  return 'verified';
};

export const getVerifiedOn = (entity: VerificationEntity): string | null =>
  entity.verifiedAt ?? null;

export const isUpdatedAfterVerification = (
  entity: VerificationEntity,
  kind: VerificationKind,
): boolean =>
  kind === 'identity' &&
  getVerificationStatus(entity) !== 'unverified' &&
  !!entity.verifiedAt &&
  !!entity.updatedAt &&
  dateHelper(entity.updatedAt).isAfter(dateHelper(entity.verifiedAt));

export const getVerificationSource = (entity: VerificationEntity): VerificationSource | null => {
  const source = 'rowSource' in entity ? entity.rowSource : entity.source;
  if (!source || source.includes(',')) {
    return null;
  }
  if (source.startsWith('enrichment')) {
    return { label: 'Enrichment', icon: 'sparkles' };
  }
  if (source === OrganizationSource.GITHUB) {
    return { label: lfIdentities.github.name, image: lfIdentities.github.image };
  }
  const label = SOURCE_LABELS[source];
  return label ? { label } : null;
};

export const formatVerificationDate = (date: string): string =>
  dateHelper(date).format('D MMM YYYY');

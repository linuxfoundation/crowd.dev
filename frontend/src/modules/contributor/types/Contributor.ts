import type { Organization } from '@/modules/organization/types/Organization';
import type { MergeAction } from '@/shared/modules/merge/types/MemberActions';

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- shortcut: untyped attribute values. revisit: per-attribute-type values
export type AttributeValue = any;

export interface ContributorAttribute {
  default: AttributeValue;
  custom: AttributeValue;
  github?: AttributeValue;
  twitter?: AttributeValue;
  hubspot?: AttributeValue;
  stackoverflow?: AttributeValue;
  discourse?: AttributeValue;
  credly?: AttributeValue;
}

export interface ContributorMaintainerRole {
  id: string;
  dateEnd: string | null;
  dateStart: string;
  memberId: string;
  repoType: string;
  role: string;
  segmentId: string;
  segmentName: string;
  url: string;
}

export interface ContributorContribution {
  firstCommitDate: string;
  id: number;
  lastCommitDate: string;
  numberCommits: number;
  summary: string;
  topics: string[];
  url: string;
}

export interface ContributorAffiliation {
  id: string;
  dateEnd: string | null;
  dateStart: string | null;
  organizationId: string | null;
  organizationLogo: string;
  organizationName: string;
  segmentId: string;
  segmentName: string;
  segmentParentName: string;
  segmentSlug: string;
  maintainerRoles: ContributorMaintainerRole[];
}

export interface ContributorReach {
  total: number;
  github: number;
  twitter: number;
}

export interface ContributorTag {
  id: string;
  name: string;
}

export interface ContributorIdentity {
  id: string;
  platform: string;
  type: string;
  value: string;
  verified: boolean;
  verifiedBy?: string | null;
  verifiedAt?: string | null;
  updatedAt?: string | null;
  source: string | null;
  sourceId: string | null;
  integrationId?: string | null;
  // Computed properties
  platforms?: string[];
  url?: string;
  duplicatedIdentities?: ContributorIdentity[];
}

export type CreateContributorIdentity = Omit<
  ContributorIdentity,
  'id' | 'platforms' | 'url' | 'duplicatedIdentities'
>;

export interface UpdateContributorIdentityPayload {
  value?: string;
  type?: string;
  platform?: string;
  verified?: boolean;
  source?: string | null;
  sourceId?: string | null;
  integrationId?: string | null;
}

export interface Contributor {
  activeDaysCount: string;
  activeOn: string[] | null;
  activityCount: string;
  activityTypes: string[] | null;
  attributes: Record<string, ContributorAttribute>;
  averageSentiment: number | null;
  contributions: ContributorContribution[];
  createdAt: string;
  displayName: string;
  emails: string[];
  id: string;
  identities: ContributorIdentity[];
  importHash: string | null;
  joinedAt: string;
  lastActive: string | null;
  lastEnrichedAt: string | null;
  noMergeIds: string[] | null;
  numberOfOpenSourceContributions: number | null;
  organizations: Organization[];
  reach: ContributorReach;
  score: number;
  toMergeIds: string[] | null;
  updatedAt: string;
  username: Record<string, string[]>;
  verifiedEmails: string[];
  unverifiedEmails: string[];
  affiliations: ContributorAffiliation[];
  segmentId?: string;
  segments: {
    id: string;
    name: string;
    activityCount: string;
  }[];
  maintainerRoles: ContributorMaintainerRole[];
  activitySycning: MergeAction | null;
}

export type CreateContributorPayload = Omit<Partial<Contributor>, 'identities'> & {
  identities?: CreateContributorIdentity[];
  manuallyCreated?: boolean;
};

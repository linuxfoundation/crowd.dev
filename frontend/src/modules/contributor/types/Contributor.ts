import { Organization } from '@/modules/organization/types/Organization';
import { MergeAction } from '@/shared/modules/merge/types/MemberActions';

export interface ContributorAttribute {
  default: any;
  custom: any;
  github?: any;
  twitter?: any;
  hubspot?: any;
  stackoverflow?: any;
  discourse?: any;
}

export interface ContributorMaintainerRole {
  id: string
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
  source: string | null;
  sourceId: string | null;
  integrationId?: string | null;
  // Computed properties
  platforms?: string[];
  url?: string;
  duplicatedIdentities?: ContributorIdentity[];
}

export interface Contributor {
  activeDaysCount: string;
  activeOn: string[] | null;
  activityCount: string;
  activityTypes:string[] | null;
  attributes: Record<string, ContributorAttribute>
  averageSentiment: number | null;
  contributions: ContributorContribution[]
  createdAt: string;
  displayName: string;
  emails: string[]
  id: string;
  identities: ContributorIdentity[];
  importHash: string | null;
  joinedAt: string;
  lastActive: string | null;
  lastEnrichedAt: string | null;
  noMergeIds: string[] | null;
  numberOfOpenSourceContributions: number | null;
  organizations: Organization[];
  reach:ContributorReach;
  score: number;
  toMergeIds: string[] | null;
  updatedAt: string;
  username: Record<string, string[]>
  verifiedEmails: string[];
  unverifiedEmails: string[];
  affiliations: ContributorAffiliation[];
  segments: {
    id: string;
    name: string;
    activityCount: string;
  }[],
  maintainerRoles: ContributorMaintainerRole[];
  activitySycning: MergeAction | null;
}

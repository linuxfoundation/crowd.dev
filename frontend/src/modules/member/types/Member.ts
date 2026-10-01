import type { Organization } from '@/modules/organization/types/Organization';
import type { Platform } from '@/shared/modules/platform/types/Platform';
import type { ContributorMaintainerRole } from '@/modules/contributor/types/Contributor';

export interface MemberAttribute {
  default: string;
  custom: string;
  github?: string;
  twitter?: string;
  hubspot?: any;
}

export interface MemberContribution {
  firstCommitDate: string;
  id: number;
  lastCommitDate: string;
  numberCommits: number;
  summary: string;
  topics: string[];
  url: string;
}

export interface MemberReach {
  total: number;
  github: number;
  twitter: number;
}

export interface MemberTag {
  id: string;
  name: string;
}

export interface MemberIdentity {
  platform: Platform;
  type: string;
  value: string;
  verified: boolean;
  sourceId: string | null;
}

export interface Member {
  activeDaysCount: string;
  activeOn: string[] | null;
  activityCount: string;
  activityTypes:string[] | null;
  attributes: Record<string, MemberAttribute>
  averageSentiment: number | null;
  contributions: MemberContribution[]
  createdAt: string;
  displayName: string;
  emails: string[]
  id: string;
  identities: MemberIdentity[] | null;
  importHash: string | null;
  joinedAt: string;
  lastActive: string | null;
  lastEnriched: string | null;
  noMergeIds: string[] | null;
  numberOfOpenSourceContributions: number | null;
  organizations: Organization[];
  reach:MemberReach;
  score: number;
  tags: MemberTag[];
  toMergeIds: string[] | null;
  updatedAt: string;
  username: Record<string, string[]>
  verifiedEmails: string[];
  unverifiedEmails: string[];
  segments: {
    id: string;
    name: string;
  }[],
  maintainerRoles: ContributorMaintainerRole[];
}

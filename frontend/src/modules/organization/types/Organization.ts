import type { Platform } from '@/shared/modules/platform/types/Platform';
import type { MergeAction } from '@/shared/modules/merge/types/MemberActions';

export interface OrganizationAttribute extends Record<string, unknown[] | string> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- shortcut: untyped attribute value. revisit: per-attribute value types
  default: any;
}

export enum OrganizationSource {
  EMAIL_DOMAIN = 'email-domain',
  ENRICHMENT = 'enrichment',
  HUBSPOT = 'hubspot',
  GITHUB = 'github',
  UI = 'ui',
  PROJECT_REGISTRY = 'project-registry',
  LFX_ONE = 'lfxOne',
  CVENT = 'cvent',
  TNC = 'tnc',
  MEETINGS = 'meetings',
  COMMITTEES = 'committees',
}

export enum OrganizationIdentityType {
  USERNAME = 'username',
  PRIMARY_DOMAIN = 'primary-domain',
  ALTERNATIVE_DOMAIN = 'alternative-domain',
  AFFILIATED_PROFILE = 'affiliated-profile',
  EMAIL = 'email',
}

export interface OrganizationIdentity {
  organizationId?: string | null;
  platform: Platform;
  type: OrganizationIdentityType;
  value: string;
  verified: boolean;
  source: string;
  sourceId?: string | null;
  integrationId?: string | null;
}
export interface OrganizationIdentityWithPlatforms extends OrganizationIdentity {
  platforms: Platform[];
}
export interface OrganizationIdentityParsed {
  id: string;
  displayValue: string;
  organizationId?: string | null;
  platform: Platform;
  type: OrganizationIdentityType;
  value: string;
  verified: boolean;
  source: string;
  sourceId?: string | null;
  integrationId?: string | null;
}

export interface MemberOrganizationAffiliationOverride {
  allowAffiliation: boolean;
  isPrimaryWorkExperience: boolean;
  memberId: string;
  memberOrganizationId: string;
}

export interface MemberOrganization {
  id: string;
  title: string;
  organizationId?: string;
  dateStart?: string;
  dateEnd?: string;
  source: OrganizationSource;
  rowSource?: OrganizationSource | null;
  verified?: boolean;
  verifiedBy?: string | null;
  verifiedAt?: string | null;
  updatedAt?: string;
  affiliationOverride: MemberOrganizationAffiliationOverride;
}

export interface Organization {
  attributes: Record<string, OrganizationAttribute>;
  activeOn: string[];
  activityCount: number;
  address: Record<string, string>;
  createdAt: string;
  createdById: string;
  deletedAt: string;
  description: string;
  displayName: string;
  emails: string[] | null;
  employeeCountByCountry: Record<string, number> | null;
  employees: number | null;
  founded: string | null;
  geoLocation: string | null;
  headline: string;
  id: string;
  identities: OrganizationIdentity[];
  importHash: string | null;
  industry: string;
  isAffiliationBlocked: boolean;
  isTeamOrganization: boolean;
  joinedAt: string;
  lastActive: string;
  lastEnrichedAt: string;
  location: string;
  logo: string;
  manuallyCreated?: boolean;
  memberCount: number;
  naics: Record<string, unknown>[];
  name: string;
  phoneNumbers: string[] | null;
  profiles: string[];
  revenueRange: string | null;
  size: string;
  tags: string[] | null;
  ticker: Record<string, string> | null;
  type: string;
  updatedAt: string;
  updatedById: string;
  url: string;
  memberOrganizations: MemberOrganization;
  segments?: string[];
  lfxMembership?: {
    accountDomain: string;
    accountName: string;
    createdAt: string;
    domainAlias: string[];
    id: string;
    installDate: string;
    organizationId: string;
    parentAccount: string;
    price: number;
    priceCurrency: string;
    productFamily: string;
    productName: string;
    project: string;
    purchaseHistoryName: string;
    segmentId: string;
    segments: string[];
    status: string;
    tier: string;
    updatedAt: string;
    usageEndDate: string;
  };
  activitySycning: MergeAction | null;
}

export interface OrganizationQueryBody {
  search?: string;
  filter?: Record<string, unknown>;
  offset?: number;
  limit?: number;
  orderBy?: string;
  segments?: string[];
  excludeSegments?: boolean;
}

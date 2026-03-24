import { Platform } from '@/shared/modules/platform/types/Platform';
import { MergeAction } from '@/shared/modules/merge/types/MemberActions';

export interface OrganizationAttribute extends Record<string, any[]>{
  default: any;
}

export enum OrganizationSource {
  EMAIL_DOMAIN = 'email-domain',
  ENRICHMENT = 'enrichment',
  HUBSPOT = 'hubspot',
  GITHUB = 'github',
  UI = 'ui',
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
  affiliationOverride: MemberOrganizationAffiliationOverride;
}

export interface Organization {
  attributes: Record<string, OrganizationAttribute>,
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
  memberCount: number;
  naics: any[]
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
  memberOrganizations: MemberOrganization
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
  }
  activitySycning: MergeAction | null;
}

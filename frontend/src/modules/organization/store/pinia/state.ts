import type { Filter } from '@/shared/modules/filters/types/FilterConfig';
import type { Organization } from '@/modules/organization/types/Organization';
import allOrganizations from '@/modules/organization/config/saved-views/views/all-organizations';

export interface OrganizationState {
  filters: Filter,
  savedFilterBody: any,
  organizations: Organization[];
  organization: Organization | null;
  selectedOrganizations: Organization[];
  totalOrganizations: number;
  mergedOrganizations: {
    [key: string]: string;
  };
  toMergeOrganizations: {
    originalId: string | null;
    toMergeId: string | null;
  }
}

const state: OrganizationState = {
  filters: {
    ...allOrganizations.config,
  },
  savedFilterBody: {},
  organizations: [],
  organization: null,
  selectedOrganizations: [],
  totalOrganizations: 0,
  mergedOrganizations: {},
  toMergeOrganizations: {
    originalId: null,
    toMergeId: null,
  },
};

export default () => state;

import type { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { SearchFilterConfig } from '@/shared/modules/filters/types/filterTypes/SearchFilterConfig';
import { trimAndReduceSpaces } from '@/utils/string';
import activeOn from './activeOn/config';
import enrichedOrganization from './enrichedOrganization/config';
import lfxMembership from './lfxMembership/config';
import noOfActivities from './noOfActivities/config';
import noOfMembers from './noOfMembers/config';
import organizations from './organizations/config';
import projects from './projects/config';

export const organizationFilters: Record<string, FilterConfig> = {
  organizations,
  noOfActivities,
  noOfMembers,
  // activeOn,
  // annualEmployeeChurnRate,
  // annualEmployeeGrowthRate,
  // annualRevenue,
  // employeeCount,
  enrichedOrganization,
  // founded,
  // headcount,
  // headline,
  // industry,
  // joinedDate,
  // lastActivityDate,
  lfxMembership,
  // location,
  projects,
  // tags,
  // type,
};
export const organizationCommonFilters: Record<string, FilterConfig> = {
  projects,
  enrichedOrganization,
  lfxMembership,
  activeOn,
};

export const organizationSearchFilter: SearchFilterConfig = {
  placeholder: 'Search organizations',
  apiFilterRenderer(value: string): any[] {
    return [
      {
        or: [
          { displayName: { textContains: trimAndReduceSpaces(value) } },
        ],
      },
    ];
  },
};

import unaffiliated from '@/modules/member/config/filters/unaffiliated/config';
import type { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { SearchFilterConfig } from '@/shared/modules/filters/types/filterTypes/SearchFilterConfig';
import identities from './identities/config';
import memberName from './memberName/config';
import noOfActivities from './noOfActivities/config';
import organizations from './organizations/config';
import projects from './projects/config';

export const memberFilters: Record<string, FilterConfig> = {
  memberName,
  organizations,
  noOfActivities,
  // noOfOSSContributions,
  // jobTitle,
  // activeOn,
  // activityType,
  // avgSentiment,
  // engagementLevel,
  identities,
  // joinedDate,
  // lastActivityDate,
  // reach,
  projects,
  unaffiliated,
};

export const memberSearchFilter: SearchFilterConfig = {
  placeholder: 'Search people',
  apiFilterRenderer(): any[] {
    return [];
  },
};

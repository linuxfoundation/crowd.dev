import type { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { SearchFilterConfig } from '@/shared/modules/filters/types/filterTypes/SearchFilterConfig';
import { trimAndReduceSpaces } from '@/utils/string';
import activityType from './activityType/config';
import channel from './channel/config';
import date from './date/config';
import member from './member/config';
import platform from './platform/config';
// import sentiment from './sentiment/config';
import projects from './projects/config';
import organizations from './organizations/config';

export const activityFilters: Record<string, FilterConfig> = {
  activityType,
  channel,
  date,
  member,
  organizations,
  platform,
  // sentiment,
  projects,
};
export const activitySearchFilter: SearchFilterConfig = {
  placeholder: 'Search activities',
  apiFilterRenderer(value: string): any[] {
    const trimmedValue = trimAndReduceSpaces(value);

    return [
      {
        or: [
          { channel: { textContains: trimmedValue } },
          { type: { textContains: trimmedValue } },
        ],
      },
    ];
  },
};

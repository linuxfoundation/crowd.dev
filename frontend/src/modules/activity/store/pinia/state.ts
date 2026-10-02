import type { Filter } from '@/shared/modules/filters/types/FilterConfig';
import { dateHelper } from '@/shared/date-helper/date-helper';

export interface ActivityState {
  filters: Filter,
  savedFilterBody: any,
  activities: any[],
  totalActivities: number,
  limit: number,
  timestamp: string,
  activityChannels: Record<string, string[]>,
}

export default () => ({
  filters: {
    search: '',
    relation: 'and',
    order: {
      prop: 'timestamp',
      order: 'descending',
    },
  } as Filter,
  limit: 20,
  timestamp: dateHelper().toISOString(),
  savedFilterBody: {},
  activities: [],
  totalActivities: 0,
  activityChannels: {},
} as ActivityState);

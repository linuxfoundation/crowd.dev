import { memberDefaultFilterRenderer } from '@/shared/modules/filters/config/defaultFilterRenderer/member.defaultFilter.renderer';
import type { SavedView, SavedViewsConfig } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import bot from './settings/bot/config';
import organization from './settings/organization/config';
import allMembers from './views/all-members';
import toReview from './views/to-review';
import unaffiliated from './views/unaffiliated';

export const memberSavedViews: SavedViewsConfig = {
  defaultView: allMembers,
  settings: {
    bot,
    organization,
  },
  defaultFilters: {
    render: memberDefaultFilterRenderer,
  },
  sorting: {
    displayName: 'Person',
    activityCount: '# of activities',
  },
};

export const memberStaticViews: SavedView[] = [
  unaffiliated,
  toReview,
];

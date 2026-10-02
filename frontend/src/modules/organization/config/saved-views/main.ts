import type { SavedViewsConfig } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import { organizationDefaultFilterRenderer } from '@/shared/modules/filters/config/defaultFilterRenderer/organization.defaultFilter.renderer';
import allOrganizations from './views/all-organizations';

import teamOrganization from './settings/teamOrganization/config';

export const organizationSavedViews: SavedViewsConfig = {
  defaultView: allOrganizations,
  settings: {
    teamOrganization,
  },
  defaultFilters: {
    render: organizationDefaultFilterRenderer,
  },
  sorting: {
    displayName: 'Organization',
    memberCount: '# people',
    activityCount: '# activities',
  },
};

export const commonOrganizationSavedViews: SavedViewsConfig = {
  defaultView: {
    id: 'common-organizations',
    name: 'Common organizations',
    visibility: 'tenant',
    placement: 'organization',
    config: {
      search: '',
      relation: 'and',
      order: {
        prop: 'activityCount',
        order: 'descending',
      },
      settings: {
        hasActivities: 'true',
      },
    },
  },
  settings: {
  },
  defaultFilters: {
    render: organizationDefaultFilterRenderer,
  },
  sorting: {},
};

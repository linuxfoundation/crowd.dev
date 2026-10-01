import type { SavedView } from '@/shared/modules/saved-views/types/SavedViewsConfig';

const unaffiliated: SavedView = {
  id: 'unaffiliated',
  name: 'Unaffiliated',
  placement: 'member',
  visibility: 'tenant',
  config: {
    search: '',
    relation: 'and',
    order: {
      prop: 'activityCount',
      order: 'descending',
    },
    settings: {
      bot: 'exclude',
      teamMember: 'include',
      organization: 'exclude',
    },
    unaffiliated: {
      value: true,
    },
  },
};

export default unaffiliated;

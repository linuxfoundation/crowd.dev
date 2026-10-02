import type { SavedView } from '@/shared/modules/saved-views/types/SavedViewsConfig';

const allMembers: SavedView = {
  id: 'all-members',
  name: 'All people',
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
  },
};

export default allMembers;

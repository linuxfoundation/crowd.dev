import { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';
import type { DefaultFiltersSettings } from '@/shared/modules/saved-views/types/SavedViewsConfig';

export const memberDefaultFilterRenderer = ({ teamMember, bot }: DefaultFiltersSettings) => {
  if (teamMember === IncludeEnum.EXCLUDE && bot === IncludeEnum.EXCLUDE) {
    return 'Excl. team members and bots';
  }

  if (teamMember === IncludeEnum.EXCLUDE && bot === IncludeEnum.INCLUDE) {
    return 'Incl. bots and excl. team members';
  }

  if (teamMember === IncludeEnum.INCLUDE && bot === IncludeEnum.EXCLUDE) {
    return 'Incl. team members and excl. bots';
  }

  if (teamMember === IncludeEnum.FILTER) {
    return 'Team members only';
  }

  return 'Incl. team members and bots';
};

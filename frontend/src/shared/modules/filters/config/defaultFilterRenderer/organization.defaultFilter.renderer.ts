import { IncludeEnum } from '@/modules/organization/config/saved-views/settings/types/IncludeEnum';
import type { DefaultFiltersSettings } from '@/shared/modules/saved-views/types/SavedViewsConfig';

export const organizationDefaultFilterRenderer = ({ teamOrganization }: DefaultFiltersSettings) => {
  if (teamOrganization === IncludeEnum.EXCLUDE) {
    return 'Excl. team organizations';
  }

  if (teamOrganization === IncludeEnum.INCLUDE) {
    return 'Incl. team organizations';
  }

  if (teamOrganization === IncludeEnum.FILTER) {
    return 'Team organizations only';
  }

  return '';
};

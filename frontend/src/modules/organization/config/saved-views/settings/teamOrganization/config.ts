import { includeFilterRenderer } from '@/modules/member/config/saved-views/settings/common/includeFilterRenderer';
import { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';
import type { SavedViewsSetting } from '@/shared/modules/saved-views/types/SavedViewsConfig';

const config: SavedViewsSetting<IncludeEnum> = {
  inSettings: true,
  defaultValue: IncludeEnum.EXCLUDE,
  queryUrlParser(value: string): IncludeEnum {
    return value as IncludeEnum;
  },
  apiFilterRenderer(value: IncludeEnum): any[] {
    return includeFilterRenderer('isTeamOrganization', value);
  },
};

export default config;

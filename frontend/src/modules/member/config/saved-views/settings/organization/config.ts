import type { SavedViewsSetting } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import { includeFilterRenderer } from '@/modules/member/config/saved-views/settings/common/includeFilterRenderer';
import { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';

const organization: SavedViewsSetting<IncludeEnum> = {
  inSettings: false,
  defaultValue: IncludeEnum.EXCLUDE,
  queryUrlParser(value: string): IncludeEnum {
    return value as IncludeEnum;
  },
  apiFilterRenderer(value: IncludeEnum): any[] {
    return includeFilterRenderer('isOrganization', value);
  },
};

export default organization;

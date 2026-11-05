import type { SavedViewsSetting } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import { includeFilterRenderer } from '@/modules/member/config/saved-views/settings/common/includeFilterRenderer';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';
import MemberTeamMemberSetting from './MemberTeamMemberSetting.vue';

const teamMember: SavedViewsSetting<IncludeEnum> = {
  inSettings: true,
  settingsComponent: MemberTeamMemberSetting,
  defaultValue: IncludeEnum.EXCLUDE,
  queryUrlParser(value: string): IncludeEnum {
    return value as IncludeEnum;
  },
  apiFilterRenderer(value: IncludeEnum): ApiFilter[] {
    return includeFilterRenderer('isTeamMember', value);
  },
};

export default teamMember;

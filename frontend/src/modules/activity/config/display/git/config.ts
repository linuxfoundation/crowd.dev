import { Platform } from '@/shared/modules/platform/types/Platform';
import type { ActivityDisplayConfig } from '@/shared/modules/activity/types/DisplayConfig';
import LfGitActivityContent from './git-activity-content.vue';
import LfGitActivityHeaderContent from './git-activity-header-content.vue';

const activityDisplay: ActivityDisplayConfig = {
  id: 'gitActivity',
  platform: Platform.GIT,
  activityHeaderContent: LfGitActivityHeaderContent,
  activityContent: LfGitActivityContent,
};

export default activityDisplay;

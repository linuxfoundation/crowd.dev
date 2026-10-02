import { Platform } from '@/shared/modules/platform/types/Platform';
import type { ActivityDisplayPlatformConfig } from '@/shared/modules/activity/types/DisplayConfig';
import gitDisplay from './git/config';

const config: ActivityDisplayPlatformConfig = {
  [Platform.GIT]: gitDisplay,
};

export default config;

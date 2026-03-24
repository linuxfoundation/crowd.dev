import shared from '@/shared/shared-module';
import layout from '@/modules/layout/layout-module';
import settings from '@/modules/settings/settings-module';
import integration from '@/modules/integration/integration-module';
import member from '@/modules/member/member-module';
import tag from '@/modules/tag/tag-module';
import activity from '@/modules/activity/activity-module';
import organization from '@/modules/organization/organization-module';

import admin from '@/modules/admin/admin.module';
import dataQuality from '@/modules/data-quality/data-quality.module';

const modules: Record<string, any> = {
  shared,
  settings,
  layout,
  integration,
  member,
  activity,
  tag,
  organization,
  admin,
  dataQuality,
};

export default modules;

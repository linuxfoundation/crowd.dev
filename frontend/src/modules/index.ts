import type { Component } from 'vue';
import type { RouteRecordRaw } from 'vue-router';
import shared from '@/shared/shared-module';
import layout from '@/modules/layout/layout-module';
import settings from '@/modules/settings/settings-module';
import integration from '@/modules/integration/integration-module';
import member from '@/modules/member/member-module';
import activity from '@/modules/activity/activity-module';
import organization from '@/modules/organization/organization-module';

import admin from '@/modules/admin/admin.module';
import dataQuality from '@/modules/data-quality/data-quality.module';

interface AppModule {
  // shortcut: untyped JS module routes allowed. revisit: when lfx-cdp-insights-ops#307 types every module file
  routes?: (RouteRecordRaw | Record<string, unknown>)[];
  components?: Record<string, Component>;
}

const modules: Record<string, AppModule> = {
  shared,
  settings,
  layout,
  integration,
  member,
  activity,
  organization,
  admin,
  dataQuality,
};

export default modules;

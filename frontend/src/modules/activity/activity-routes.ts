// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { RouteRecordRaw } from 'vue-router';
import Layout from '@/modules/layout/components/layout.vue';
import { PageEventKey } from '@/shared/modules/monitoring/types/event';
import { PermissionGuard } from '@/shared/modules/permissions/router/PermissionGuard';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';

const ActivityListPage = () => import('@/modules/activity/pages/activity-list-page.vue');

const routes: RouteRecordRaw[] = [
  {
    name: '',
    path: '',
    component: Layout,
    meta: {
      auth: true,
      title: 'Activities',
      eventKey: PageEventKey.ACTIVITIES,
      segments: {
        requireSelectedProjectGroup: true,
      },
    },
    children: [
      {
        name: 'activity',
        path: '/activities',
        component: ActivityListPage,
        meta: {
          auth: true,
        },
        beforeEnter: [PermissionGuard(LfPermission.activityRead)],
      },
    ],
  },
];

export default routes;

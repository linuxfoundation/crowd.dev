// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { NavigationGuardNext, RouteLocationNormalized, RouteRecordRaw } from 'vue-router';
import Layout from '@/modules/layout/components/layout.vue';
import { PageEventKey } from '@/shared/modules/monitoring/types/event';
import { PermissionGuard } from '@/shared/modules/permissions/router/PermissionGuard';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { consumeConnectSegment } from '@/config/integrations/connect-segment';

const IntegrationListPage = () =>
  import('@/modules/admin/modules/integration/pages/integration-list.page.vue');
const IntegrationSuccessPage = () =>
  import('@/modules/integration/pages/integration-success-page.vue');

const routes: RouteRecordRaw[] = [
  {
    name: '',
    path: '',
    component: Layout,
    meta: {
      title: 'Integrations',
      eventKey: PageEventKey.INTEGRATIONS,
      hideBanner: true,
    },
    children: [
      {
        name: 'integrationRedirect',
        path: '/integrations',
        meta: {},
        // RouteRecordRaw has no guard-only variant; normalizeRouteRecord defaults children to [].
        // https://github.com/vuejs/router/blob/v4.1.6/packages/router/src/matcher/index.ts#L384
        children: [],
        beforeEnter: [
          (
            to: RouteLocationNormalized,
            from: RouteLocationNormalized,
            next: NavigationGuardNext,
          ): void => {
            const connectSegment = Object.keys(to.query).length ? consumeConnectSegment() : null;
            const segmentId = connectSegment?.segmentId ?? localStorage.getItem('segmentId');
            const segmentGrandparentId =
              connectSegment?.grandparentId ?? localStorage.getItem('segmentGrandparentId');
            const state = to.query.state ?? '';

            // Redirect to integrations list page with correct id
            if (Object.keys(to.query).length) {
              if (state === 'noconnect') {
                next({
                  name: 'integrationSuccess',
                  query: to.query,
                });
                return;
              }
              // 'null' is the string earlier builds stored when clearing the segment.
              if (segmentId && segmentId !== 'null') {
                next({
                  name: 'integration',
                  params: {
                    id: segmentId,
                    grandparentId: segmentGrandparentId,
                  },
                  query: to.query,
                });
                return;
              }
            }

            localStorage.removeItem('segmentId');
            localStorage.removeItem('segmentGrandparentId');

            next({ name: 'projectGroupsList' });
          },
        ],
      },
      {
        name: 'integration',
        path: '/integrations/:grandparentId/:id',
        component: IntegrationListPage,
        meta: {
          auth: true,
          paramSegmentAccess: {
            name: 'child',
            parameter: 'id',
          },
        },
        beforeEnter: [PermissionGuard(LfPermission.integrationRead)],
      },
    ],
  },
  {
    name: 'integrationSuccess',
    path: '/integrations/success',
    component: IntegrationSuccessPage,
    meta: {
      hideLfxHeader: true,
    },
  },
];

export default routes;

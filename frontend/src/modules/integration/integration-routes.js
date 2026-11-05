import Layout from '@/modules/layout/components/layout.vue';
import { PageEventKey } from '@/shared/modules/monitoring/types/event';
import { PermissionGuard } from '@/shared/modules/permissions/router/PermissionGuard';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { consumeConnectSegment } from '@/config/integrations/connect-segment';

const IntegrationListPage = () =>
  import('@/modules/admin/modules/integration/pages/integration-list.page.vue');
const IntegrationSuccessPage = () =>
  import('@/modules/integration/pages/integration-success-page.vue');

export default [
  {
    name: '',
    path: '',
    component: Layout,
    exact: true,
    meta: {
      title: 'Integrations',
      eventKey: PageEventKey.INTEGRATIONS,
      hideBanner: true,
    },
    children: [
      {
        name: 'integrationRedirect',
        path: '/integrations',
        exact: true,
        meta: {},
        beforeEnter: [
          (to, from, next) => {
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
              if (segmentId !== 'null') {
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

            localStorage.setItem('segmentId', null);
            localStorage.setItem('segmentGrandparentId', null);

            next({ name: 'projectGroupsList' });
          },
        ],
      },
      {
        name: 'integration',
        path: '/integrations/:grandparentId/:id',
        component: IntegrationListPage,
        exact: true,
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
    exact: true,
    meta: {
      hideLfxHeader: true,
    },
  },
];

import Layout from '@/modules/layout/components/layout.vue';
import { PageEventKey } from '@/shared/modules/monitoring/types/event';
import { PermissionGuard } from '@/shared/modules/permissions/router/PermissionGuard';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';

const DataQualityPage = () => import('@/modules/data-quality/pages/data-quality.page.vue');

export default [
  {
    name: '',
    path: '',
    component: Layout,
    meta: {
      auth: true,
      title: 'Data Quality Copilot',
      segments: {
        optionalSelectedProjectGroup: true,
      },
    },
    children: [
      {
        name: 'data-quality-assistant',
        path: '/data-quality-assistant',
        component: DataQualityPage,
        meta: {
          auth: true,
          eventKey: PageEventKey.DATA_QUALITY_ASSISTANT,
        },
        beforeEnter: [PermissionGuard(LfPermission.dataQualityRead)],
      },
    ],
  },
];

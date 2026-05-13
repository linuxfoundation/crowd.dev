import Layout from '@/modules/layout/components/layout.vue';
import { PageEventKey } from '@/shared/modules/monitoring/types/event';
import { PermissionGuard } from '@/shared/modules/permissions/router/PermissionGuard';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';

const OverviewPage = () => import('@/modules/overview/pages/overview.vue');

// const ProjectGroupsListPage = () =>
//   import('@/modules/admin/modules/projects/pages/project-groups-list.page.vue')

const ProjectsPage = () => import('@/modules/admin/modules/projects/pages/projects.page.vue');

const AdminPanelPage = () => import('@/modules/admin/pages/admin-panel.page.vue');

export default [
  {
    name: '',
    path: '',
    component: Layout,
    meta: {
      auth: true,
      hideBanner: true,
    },
    children: [
      {
        name: 'overview',
        path: '/overview',
        component: OverviewPage,
        meta: {
          auth: true,
          title: 'Overview',
          eventKey: PageEventKey.OVERVIEW,
        },
      },
      // {
      //   name: 'projectGroupsList',
      //   path: '/project-groups',
      //   component: ProjectGroupsListPage,
      //   meta: {
      //     auth: true,
      //     title: 'Project Groups',
      //     eventKey: PageEventKey.PROJECT_GROUPS,
      //   },
      // },
      {
        name: 'adminPanel',
        path: '/admin',
        component: AdminPanelPage,
        meta: {
          title: 'Admin Panel',
          eventKey: PageEventKey.ADMIN_PANEL,
        },
        beforeEnter: [PermissionGuard(LfPermission.projectGroupEdit)],
      },
      {
        name: 'adminProjects',
        path: '/admin/project-groups/:id/projects',
        component: ProjectsPage,
        meta: {
          auth: true,
          title: 'Admin Panel',
          eventKey: PageEventKey.MANAGE_PROJECTS,
          paramSegmentAccess: {
            name: 'grandparent',
            parameter: 'id',
          },
        },
        beforeEnter: [PermissionGuard(LfPermission.projectEdit)],
      },
    ],
  },
];

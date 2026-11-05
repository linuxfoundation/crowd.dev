// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { RouteRecordRaw } from 'vue-router';
import Layout from '@/modules/layout/components/layout.vue';
import { PermissionGuard } from '@/shared/modules/permissions/router/PermissionGuard';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { PageEventKey } from '@/shared/modules/monitoring/types/event';

const OrganizationListPage = () =>
  import('@/modules/organization/pages/organization-list-page.vue');

const OrganizationDetailsPage = () =>
  import('@/modules/organization/pages/organization-details.page.vue');

const OrganizationMergeSuggestionsPage = () =>
  import('@/modules/organization/pages/organization-merge-suggestions-page.vue');

const OrganizationsMainPage = async () => OrganizationListPage();

const routes: RouteRecordRaw[] = [
  {
    name: '',
    path: '',
    component: Layout,
    meta: {
      auth: true,
      title: 'Organizations',
    },
    children: [
      {
        name: 'organization',
        path: '/organizations',
        component: OrganizationsMainPage,
        meta: {
          auth: true,
          eventKey: PageEventKey.ORGANIZATIONS,
          segments: {
            requireSelectedProjectGroup: true,
          },
        },
        props: {
          module: 'organizations',
        },
        beforeEnter: [PermissionGuard(LfPermission.organizationRead)],
      },
      {
        name: 'organizationView',
        path: '/organizations/:id',
        component: OrganizationDetailsPage,
        meta: {
          title: 'Organization',
          auth: true,
          eventKey: PageEventKey.ORGANIZATION_PROFILE,
          segments: {
            optionalSelectedProjectGroup: true,
          },
        },
        props: true,
        beforeEnter: [PermissionGuard(LfPermission.organizationRead)],
      },
      {
        name: 'organizationMergeSuggestions',
        path: '/organizations/merge-suggestions',
        component: OrganizationMergeSuggestionsPage,
        meta: {
          auth: true,
          eventKey: PageEventKey.ORGANIZATIONS_MERGE_SUGGESTIONS,
          segments: {
            requireSelectedProjectGroup: true,
          },
        },
        props: true,
        beforeEnter: [PermissionGuard(LfPermission.mergeOrganizations)],
      },
    ],
  },
];

export default routes;

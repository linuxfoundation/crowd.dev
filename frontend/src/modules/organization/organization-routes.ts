// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { RouteLocationNormalized, RouteRecordRaw } from 'vue-router';
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

// Old /organizations URLs redirect to /companies, keeping params, query and hash.
const legacyRedirect = (to: RouteLocationNormalized) => ({
  path: to.path.replace(/^\/organizations/, '/companies'),
  query: to.query,
  hash: to.hash,
});

const routes: RouteRecordRaw[] = [
  { path: '/organizations', redirect: legacyRedirect },
  { path: '/organizations/:id', redirect: legacyRedirect },
  {
    name: '',
    path: '',
    component: Layout,
    meta: {
      auth: true,
      title: 'Companies',
    },
    children: [
      {
        name: 'organization',
        path: '/companies',
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
        path: '/companies/:id',
        component: OrganizationDetailsPage,
        meta: {
          title: 'Company',
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
        path: '/companies/merge-suggestions',
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

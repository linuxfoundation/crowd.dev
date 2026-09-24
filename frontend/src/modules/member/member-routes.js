import Layout from '@/modules/layout/components/layout.vue';
import { PageEventKey } from '@/shared/modules/monitoring/types/event';
import { PermissionGuard } from '@/shared/modules/permissions/router/PermissionGuard';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';

const MemberListPage = () => import('@/modules/member/pages/member-list-page.vue');
const MemberMergeSuggestionsPage = () => import(
  '@/modules/member/pages/member-merge-suggestions-page.vue'
);
const PersonViewPage = () => import('@/modules/contributor/pages/contributor-details.page.vue');
export default [
  {
    name: '',
    path: '',
    component: Layout,
    meta: {
      auth: true,
      title: 'People',
    },
    children: [
      {
        name: 'member',
        path: '/people',
        component: MemberListPage,
        meta: {
          auth: true,
          eventKey: PageEventKey.MEMBERS,
          segments: {
            requireSelectedProjectGroup: true,
          },
        },
        beforeEnter: [
          PermissionGuard(LfPermission.memberRead),
        ],
      },
      {
        name: 'memberMergeSuggestions',
        path: '/people/merge-suggestions',
        component: MemberMergeSuggestionsPage,
        meta: {
          auth: true,
          eventKey: PageEventKey.MEMBERS_MERGE_SUGGESTIONS,
          segments: {
            requireSelectedProjectGroup: true,
          },
        },
        beforeEnter: [
          PermissionGuard(LfPermission.mergeMembers),
        ],
      },
      {
        name: 'memberView',
        path: '/people/:id',
        component: PersonViewPage,
        meta: {
          auth: true,
          title: 'Person profile',
          eventKey: PageEventKey.MEMBER_PROFILE,
          segments: {
            optionalSelectedProjectGroup: true,
          },
        },
        props: true,
        beforeEnter: [
          PermissionGuard(LfPermission.memberRead),
        ],
      },
      {
        name: 'memberMerge',
        path: '/people/:id/merge',
        component: MemberMergeSuggestionsPage,
        meta: {
          auth: true,
          eventKey: PageEventKey.MEMBERS_MERGE_SUGGESTIONS,
          segments: {
            requireSelectedProjectGroup: true,
          },
        },
        props: true,
        beforeEnter: [
          PermissionGuard(LfPermission.memberEdit),
        ],
      },
      {
        path: '/contributors',
        redirect: (to) => ({
          path: '/people',
          query: to.query,
        }),
      },
      {
        path: '/contributors/new',
        redirect: (to) => ({
          path: '/people/new',
          query: to.query,
        }),
      },
      {
        path: '/contributors/merge-suggestions',
        redirect: (to) => ({
          path: '/people/merge-suggestions',
          query: to.query,
        }),
      },
      {
        path: '/contributors/:id',
        redirect: (to) => ({
          path: `/people/${to.params.id}`,
          query: to.query,
        }),
      },
      {
        path: '/contributors/:id/merge',
        redirect: (to) => ({
          path: `/people/${to.params.id}/merge`,
          query: to.query,
        }),
      },
    ],
  },
];

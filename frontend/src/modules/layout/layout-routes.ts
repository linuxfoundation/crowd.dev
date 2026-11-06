import type { RouteRecordRaw } from 'vue-router';
import ErrorPage from '@/modules/layout/pages/error-page.vue';

export default [
  {
    name: 'error403',
    path: '/403',
    component: ErrorPage,
    meta: {
      auth: true,
    },
    props: {
      code: 403,
      title: 'Restricted access',
      subtitle: 'Sorry, you don’t have permissions to access this page.',
    },
  },
  {
    name: 'error404',
    path: '/404',
    component: ErrorPage,
    meta: {
      auth: true,
    },
    props: {
      code: 404,
      title: 'Page not found',
      subtitle: 'Sorry, the page you are looking for doesn’t exist or was removed.',
    },
  },
] satisfies RouteRecordRaw[];

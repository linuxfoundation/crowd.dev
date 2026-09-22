import { createRouter as createVueRouter, createWebHistory } from 'vue-router';
import { storeToRefs } from 'pinia';

import { store } from '@/store';
import authGuards from '@/middleware/auth';
import modules from '@/modules';
import ProgressBar from '@/shared/progress-bar/progress-bar';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import auth from '@/modules/auth';
import navigationGuard from '@/middleware/navigation/navigation-guard';

/**
 * Loads all the routes from src/modules/ folders, and adds the catch-all rule to handle 404s
 *
 * @type {[...*,{redirect: string, path: string}]}
 */
const routes = [
  {
    path: '',
    redirect: '/overview',
  },
  ...auth.routes,
  ...Object.keys(modules)
    .filter((key) => Boolean(modules[key].routes))
    .map((key) => modules[key].routes.map((r) => {
      // eslint-disable-next-line no-param-reassign
      r.meta = {
        ...r.meta,
        middleware: [...authGuards],
      };
      return r;
    }))
    .reduce((a, b) => a.concat(b), []),
  { path: '/:catchAll(.*)', redirect: '/404' },
];

// eslint-disable-next-line import/no-mutable-exports
let router;

/**
 * Creates/Sets Router
 * @returns {Router|{x: number, y: number}}
 */
export const createRouter = () => {
  if (!router) {
    router = createVueRouter({
      history: createWebHistory(),
      routes,
      scrollBehavior() {
        return { x: 0, y: 0 };
      },
    });

    const originalPush = router.push;
    router.push = function push(location) {
      return originalPush.call(this, location).catch((error) => {
        console.error(error);
        ProgressBar.done();
      });
    };

    router.beforeEach(async (to, from, next) => {
      const lsSegmentsStore = useLfSegmentsStore();
      const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
      const { updateSelectedProjectGroup } = lsSegmentsStore;

      // Set title to pages
      document.title = `LFX Community Data Platform${to.meta.title ? ` | ${to.meta.title}` : ''}`;

      if (to.name && to.query.menu === from.query.menu && to.name !== from.name) {
        ProgressBar.start();
      }

      const matchedRoute = to.matched.find((m) => m.meta.middleware);

      if (matchedRoute !== undefined) {
        const middlewareArray = Array.isArray(matchedRoute.meta.middleware)
          ? matchedRoute.meta.middleware
          : [matchedRoute.meta.middleware];

        const context = {
          from,
          router,
          to,
          store,
        };

        await middlewareArray.reduce(
          (promise, middleware) => promise.then(() => middleware(context)),
          Promise.resolve(),
        );

        // Redirect to project group landing pages if routes that require a selected project group
        // And no project group is selected
        if (
          to.meta.segments?.requireSelectedProjectGroup
          || to.meta.segments?.optionalSelectedProjectGroup
        ) {
          if (
            !selectedProjectGroup.value
            && !to.query.projectGroup
            && !to.meta.segments?.optionalSelectedProjectGroup
          ) {
            next('/project-groups');
            return;
          }

          if (!to.query.projectGroup && selectedProjectGroup.value?.id) {
            next({ ...to, query: { ...to.query, projectGroup: selectedProjectGroup.value?.id } });
            return;
          }

          if (!selectedProjectGroup.value) {
            try {
              await updateSelectedProjectGroup(to.query.projectGroup, false);
              if (!selectedProjectGroup.value) {
                next('/project-groups');
                return;
              }
            } catch (e) {
              next('/project-groups');
              return;
            }
          }
        }
      }

      next();
    });

    router.afterEach(async (to) => {
      ProgressBar.done();
      await navigationGuard({ to });
    });
  }

  return router;
};

export { router };

import { createRouter as createVueRouter, createWebHistory } from 'vue-router';
import type {
  NavigationFailure,
  NavigationGuardNext,
  RouteLocationMatched,
  RouteLocationNormalized,
  RouteLocationRaw,
  RouteRecordRaw,
  Router,
} from 'vue-router';
import { storeToRefs } from 'pinia';

import authGuards from '@/middleware/auth';
import modules from '@/modules';
import * as ProgressBar from '@/shared/progress-bar/progress-bar';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import auth from '@/modules/auth';
import navigationGuard from '@/middleware/navigation/navigation-guard';
import { setRouter } from '@/router/instance';
import type { Guard, GuardContext } from '@/middleware/types';

/**
 * Loads all the routes from src/modules/ folders, and adds the catch-all rule to handle 404s
 */
const routes: RouteRecordRaw[] = [
  {
    path: '',
    redirect: '/overview',
  },
  ...auth.routes,
  ...Object.keys(modules)
    .filter((key: string): boolean => Boolean(modules[key].routes))
    .map((key: string): RouteRecordRaw[] =>
      // shortcut: JS module descriptors are untyped. revisit: when CM-1607 adds a typed AppModule
      (modules[key].routes as unknown as RouteRecordRaw[]).map(
        (r: RouteRecordRaw): RouteRecordRaw => {
          // eslint-disable-next-line no-param-reassign
          r.meta = {
            ...r.meta,
            middleware: [...authGuards],
          };
          return r;
        },
      ),
    )
    .reduce((a: RouteRecordRaw[], b: RouteRecordRaw[]): RouteRecordRaw[] => a.concat(b), []),
  { path: '/:catchAll(.*)', redirect: '/404' },
];

// eslint-disable-next-line import/no-mutable-exports
let router!: Router;

/**
 * Creates/Sets Router
 */
export const createRouter = (): Router => {
  if (!router) {
    router = createVueRouter({
      history: createWebHistory(),
      routes,
      scrollBehavior(): { top: number; left: number } {
        return { top: 0, left: 0 };
      },
    });
    setRouter(router);

    const originalPush = router.push;
    router.push = function push(
      this: Router,
      location: RouteLocationRaw,
    ): Promise<NavigationFailure | void | undefined> {
      return originalPush.call(this, location).catch((error: unknown): void => {
        console.error(error);
        ProgressBar.done();
      });
    };

    router.beforeEach(
      async (
        to: RouteLocationNormalized,
        from: RouteLocationNormalized,
        next: NavigationGuardNext,
      ): Promise<void> => {
        const lsSegmentsStore = useLfSegmentsStore();
        const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
        const { updateSelectedProjectGroup } = lsSegmentsStore;

        // Set title to pages
        document.title = `LFX Community Data Platform${to.meta.title ? ` | ${to.meta.title}` : ''}`;

        if (to.name && to.query.menu === from.query.menu && to.name !== from.name) {
          ProgressBar.start();
        }

        const matchedRoute = to.matched.find(
          (m: RouteLocationMatched): Guard[] | undefined => m.meta.middleware,
        );

        if (matchedRoute !== undefined) {
          const middlewareArray = (
            Array.isArray(matchedRoute.meta.middleware)
              ? matchedRoute.meta.middleware
              : [matchedRoute.meta.middleware]
          ) as Guard[];

          const context: GuardContext = {
            from,
            router,
            to,
          };

          await middlewareArray.reduce(
            (promise: Promise<void>, middleware: Guard): Promise<void> =>
              promise.then((): Promise<void> => middleware(context)),
            Promise.resolve(),
          );

          // Redirect to project group landing pages if routes that require a selected project group
          // And no project group is selected
          if (
            to.meta.segments?.requireSelectedProjectGroup ||
            to.meta.segments?.optionalSelectedProjectGroup
          ) {
            if (
              !selectedProjectGroup.value &&
              !to.query.projectGroup &&
              !to.meta.segments?.optionalSelectedProjectGroup
            ) {
              next('/project-groups');
              return;
            }

            if (!to.query.projectGroup && selectedProjectGroup.value?.id) {
              next({ ...to, query: { ...to.query, projectGroup: selectedProjectGroup.value?.id } });
              return;
            }

            if (!selectedProjectGroup.value && to.query.projectGroup) {
              try {
                await updateSelectedProjectGroup(to.query.projectGroup as string, false);
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
      },
    );

    router.afterEach(async (to: RouteLocationNormalized): Promise<void> => {
      ProgressBar.done();
      await navigationGuard({ to });
    });
  }

  return router;
};

export { router };

import type { RouteLocationNormalized, Router } from 'vue-router';

export interface GuardContext {
  to: RouteLocationNormalized;
  from: RouteLocationNormalized;
  router: Router;
}

export type Guard = (ctx: GuardContext) => Promise<void>;

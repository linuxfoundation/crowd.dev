import type { Guard } from '@/middleware/types';

declare module 'vue-router' {
  interface RouteMeta {
    auth?: boolean;
    unauth?: boolean;
    title?: string;
    middleware?: Guard[];
    segments?: {
      requireSelectedProjectGroup?: boolean;
      optionalSelectedProjectGroup?: boolean;
    };
    paramSegmentAccess?: {
      name: 'grandparent' | 'child';
      parameter: string;
    };
  }
}

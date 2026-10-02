/**
 * Nativation Guard
 *
 * TBD
 *
 * @param to
 * @param router
 * @returns {Promise<*>}
 */

import {
  EventType,
  PageEventKey,
} from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import type { RouteLocationNormalized } from 'vue-router';
import { useAuthStore } from '@/modules/auth/store/auth.store';

export default async function ({ to }: { to: RouteLocationNormalized }) {
  const { trackEvent } = useProductTracking();

  const authStore = useAuthStore();
  const { ensureLoaded, ensureTrackingSession } = authStore;

  await ensureLoaded();
  await ensureTrackingSession();

  if (to.meta.eventKey && !to.redirectedFrom) {
    let eventKey = to.meta.eventKey as PageEventKey;

    if (eventKey === PageEventKey.ADMIN_PANEL) {
      if (to.hash === '#project-groups') {
        eventKey = PageEventKey.ADMIN_PANEL_PROJECT_GROUPS;
      } else if (to.hash === '#api-keys') {
        eventKey = PageEventKey.ADMIN_PANEL_API_KEYS;
      } else if (to.hash === '#audit-logs') {
        eventKey = PageEventKey.ADMIN_PANEL_AUDIT_LOGS;
      } else {
        return;
      }
    }

    trackEvent({
      type: EventType.PAGE,
      key: eventKey,
      properties: {
        path: to.path,
        name: to.meta.title,
        url: window.location.origin + to.fullPath,
      },
    });
  }
}

import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import { ToastStore } from '@/shared/message/notification';

/**
 * Segment Guard
 *
 * This middleware runs before rendering any route that has meta.paramSegmentAccess = (route param that holds segmentId)
 *
 * It checks if user has access to segment (if not, redirects to /403)
 *
 * @param to
 * @param router
 * @returns {Promise<*>}
 */
export default async function ({ to, router }) {
  if (!to.meta || !to.meta.paramSegmentAccess) {
    return;
  }

  const { ensureLoaded } = useAuthStore();
  await ensureLoaded();

  const lsSegmentsStore = useLfSegmentsStore();

  const { hasAccessToProjectGroup, hasAccessToSegmentId } = usePermissions();
  const isCheckingProjectGroup = to.meta.paramSegmentAccess.name === 'grandparent';
  let hasPermission;

  if (isCheckingProjectGroup) {
    await lsSegmentsStore.listAdminProjectGroups();

    hasPermission = hasAccessToProjectGroup(to.params[to.meta.paramSegmentAccess.parameter]);
  } else {
    hasPermission = hasAccessToSegmentId(to.params[to.meta.paramSegmentAccess.parameter]);
  }

  if (!hasPermission) {
    ToastStore.error('You don\'t have access to this page');
    router.push('/');
  }
}

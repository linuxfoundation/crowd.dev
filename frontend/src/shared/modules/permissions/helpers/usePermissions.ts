import { useAuthStore } from '@/modules/auth/store/auth.store';
import { storeToRefs } from 'pinia';
import type { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { lfPermissions } from '@/config/permissions';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { LfRole } from '@/shared/modules/permissions/types/Roles';

export default function usePermissions() {
  // Auth store
  const authStore = useAuthStore();
  const {
    roles,
    tenantUser,
  } = storeToRefs(authStore);

  // Segment store
  const lsSegmentsStore = useLfSegmentsStore();
  const { adminProjectGroups } = storeToRefs(lsSegmentsStore);

  const hasRole = (role: LfRole): boolean => roles.value.includes(role);

  const hasPermission = (permission: LfPermission): boolean => (roles.value || []).some((role) => lfPermissions[role][permission]);

  const hasAccessToProjectGroup = (segmentId: string) => {
    if (roles.value.includes(LfRole.admin)) {
      return true;
    }
    if (!tenantUser.value?.adminSegments.length) {
      return false;
    }
    return adminProjectGroups.value.list
      .map((p) => p.id)
      .includes(segmentId);
  };

  const hasAccessToSegmentId = (segmentId: string) => {
    if (roles.value.includes(LfRole.admin)) {
      return true;
    }
    return tenantUser.value?.adminSegments.includes(segmentId);
  };

  return {
    hasRole,
    hasPermission,
    hasAccessToSegmentId,
    hasAccessToProjectGroup,
  };
}

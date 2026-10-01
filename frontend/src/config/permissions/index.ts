import type { LfRole } from '@/shared/modules/permissions/types/Roles';
import type { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import readonly from './readonly';
import admin from './admin';
import projectAdmin from './projectAdmin';

export const lfPermissions: Record<LfRole, Record<LfPermission, boolean>> = {
  readonly,
  admin,
  projectAdmin,
};

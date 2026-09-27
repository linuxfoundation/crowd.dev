import type { Tenant } from '@/modules/auth/types/Tenant.type';
import type { TenantUserSettings } from '@/modules/auth/types/TenantUserSettings.type';
import type { LfRole } from '@/shared/modules/permissions/types/Roles';

export interface TenantUser {
  createdAt: string;
  createdById: string | null;
  deletedAt: string | null;
  id: string;
  invitationToken: string | null;
  invitedById: string | null;
  roles: LfRole[];
  settings: TenantUserSettings
  status: string;
  tenant: Tenant;
  adminSegments: string[];
  tenantId: string;
  updatedAt: string;
  updatedById: string | null;
  userId: string;
}

import type { TenantUser } from '@/modules/auth/types/TenantUser.type';

export interface User {
  acceptedTermsAndPrivacy: boolean;
  createdAt: string;
  createdById: string;
  deletedAt: string | null;
  email: string;
  emailVerified: boolean;
  firstName: string;
  fullName: string;
  id: string;
  importHash: string | null;
  lastName: string;
  login: string | null;
  phoneNumber: string | null;
  provider: string;
  tenants: TenantUser[];
  updatedAt: string;
  updatedById: string | null;
}

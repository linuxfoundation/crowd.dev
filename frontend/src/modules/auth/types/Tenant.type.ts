import type { TenantSettings } from '@/modules/auth/types/TenantSettings.type';

export interface Tenant {
  communitySize: number | null;
  createdAt: string;
  createdById: string;
  csvExportCount: number;
  deletedAt: string | null;
  hasSampleData: boolean;
  id: string;
  integrationsRequired: string[] | null;
  isTrialPlan: boolean;
  memberEnrichmentCount: number;
  name: string;
  onboardedAt: string | null;
  plan: string | null;
  planSubscriptionEndsAt: string | null;
  reasonForUsingCrowd: string | null;
  settings: TenantSettings[];
  stripeSubscriptionId: string | null;
  trialEndsAt: string | null;
  updatedAt: string;
  updatedById: string | null;
  url: string;
}

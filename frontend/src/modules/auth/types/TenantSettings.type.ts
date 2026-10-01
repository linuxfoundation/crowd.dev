import type { TenantSettingsAttributeSettings } from '@/modules/auth/types/TenantSettingsAttributeSettings.type';

export interface TenantSettings {
  attributeSettings: TenantSettingsAttributeSettings;
  backgroundImageUrl: string | null;
  contactsViewed: boolean | null;
  createdAt: string;
  createdById: string | null;
  deletedAt: string | null;
  id: string;
  logoUrl: string | null;
  organizationsViewed: boolean | null;
  slackWebHook: string | null;
  tenantId: string;
  updatedAt: string;
  updatedById: string | null;
  website: string | null;
}

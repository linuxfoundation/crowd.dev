import type { AuditLog } from '@/modules/lf/segments/types/AuditLog';
import { ActionType } from '@/modules/lf/segments/types/AuditLog';
import integrationsConnect from './integrations-connect';
import integrationsReconnect from './integrations-reconnect';
import membersCreate from './members-create';
import membersEditIdentities from './members-edit-identities';
import membersEditManualAffiliation from './members-edit-manual-affiliation';
import membersEditOrganizations from './members-edit-organizations';
import membersEditProfile from './members-edit-profile';
import membersMerge from './members-merge';
import membersUnmerge from './members-unmerge';
import organizationsCreate from './organizations-create';
import organizationsEditIdentities from './organizations-edit-identities';
import organizationsEditProfile from './organizations-edit-profile';
import organizationsMerge from './organizations-merge';
import organizationsUnmerge from './organizations-unmerge';

export interface LogRenderingConfig {
  label: string;
  description: (log: AuditLog) => string;
  properties?: (log: AuditLog) => {label: string, value: string;}[];
  changes?: (log: AuditLog) => Promise<{
    removals: string[]
    additions: string[]
    changes: string[]
  } | null> | {
    removals: string[]
    additions: string[]
    changes: string[]
  } | null
}

export const logRenderingConfig: Record<ActionType, LogRenderingConfig> = {
  // Integrations
  [ActionType.INTEGRATIONS_CONNECT]: integrationsConnect,
  [ActionType.INTEGRATIONS_RECONNECT]: integrationsReconnect,

  // Members
  [ActionType.MEMBERS_CREATE]: membersCreate,
  [ActionType.MEMBERS_EDIT_IDENTITIES]: membersEditIdentities,
  [ActionType.MEMBERS_EDIT_MANUAL_AFFILIATION]: membersEditManualAffiliation,
  [ActionType.MEMBERS_EDIT_ORGANIZATIONS]: membersEditOrganizations,
  [ActionType.MEMBERS_EDIT_PROFILE]: membersEditProfile,
  [ActionType.MEMBERS_MERGE]: membersMerge,
  [ActionType.MEMBERS_UNMERGE]: membersUnmerge,

  // Organizations
  [ActionType.ORGANIZATIONS_CREATE]: organizationsCreate,
  [ActionType.ORGANIZATIONS_EDIT_IDENTITIES]: organizationsEditIdentities,
  [ActionType.ORGANIZATIONS_EDIT_PROFILE]: organizationsEditProfile,
  [ActionType.ORGANIZATIONS_MERGE]: organizationsMerge,
  [ActionType.ORGANIZATIONS_UNMERGE]: organizationsUnmerge,
};

export enum ActionType {
  MEMBERS_MERGE = 'members-merge',
  MEMBERS_UNMERGE = 'members-unmerge',
  MEMBERS_EDIT_IDENTITIES = 'members-edit-identities',
  MEMBERS_EDIT_ORGANIZATIONS = 'members-edit-organizations',
  MEMBERS_EDIT_MANUAL_AFFILIATION = 'members-edit-manual-affiliation',
  MEMBERS_EDIT_PROFILE = 'members-edit-profile',
  MEMBERS_CREATE = 'members-create',
  ORGANIZATIONS_MERGE = 'organizations-merge',
  ORGANIZATIONS_UNMERGE = 'organizations-unmerge',
  ORGANIZATIONS_EDIT_IDENTITIES = 'organizations-edit-identities',
  ORGANIZATIONS_EDIT_PROFILE = 'organizations-edit-profile',
  ORGANIZATIONS_CREATE = 'organizations-create',
  INTEGRATIONS_CONNECT = 'integrations-connect',
  INTEGRATIONS_RECONNECT = 'integrations-reconnect',
}

export interface Actor {
  id: string;
  type: 'user' | 'service';
  fullName: string;
  email: string | null;
}

export interface AuditLog{
  id: string;
  timestamp: string;
  actor: Actor;
  ipAddress?: string;
  userAgent?: string;
  requestId: string;
  actionType: ActionType;
  success: boolean;
  entityId: string;
  oldState: any;
  newState: any;
  diff: any;
}

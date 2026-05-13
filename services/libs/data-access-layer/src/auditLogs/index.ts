import validator from 'validator'

import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/database'

import { QueryExecutor, connQx } from '../queryExecutor'

export enum ActorType {
  USER = 'user',
  SERVICE = 'service',
}

export interface AuditLogRequestOptions {
  actorId: string
  actorType: ActorType
  ipAddress: string
  userAgent: string
  requestId: string
}

export interface AuditLogAction {
  actionType: ActionType
  success: boolean
  entityId: string
  oldState: object
  newState: object
  diff: object
  error?: object
}

export enum EntityType {
  MEMBER = 'member',
  ORGANIZATION = 'organization',
  INTEGRATION = 'integration',
}

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
  MEMBERS_VERIFY_IDENTITY = 'members-verify-identity',
  MEMBERS_VERIFY_WORK_EXPERIENCE = 'members-verify-work-experience',
}

const ACTION_TYPES_ENTITY_TYPES = {
  [ActionType.MEMBERS_MERGE]: EntityType.MEMBER,
  [ActionType.MEMBERS_UNMERGE]: EntityType.MEMBER,
  [ActionType.MEMBERS_EDIT_IDENTITIES]: EntityType.MEMBER,
  [ActionType.MEMBERS_EDIT_ORGANIZATIONS]: EntityType.MEMBER,
  [ActionType.MEMBERS_EDIT_MANUAL_AFFILIATION]: EntityType.MEMBER,
  [ActionType.MEMBERS_EDIT_PROFILE]: EntityType.MEMBER,
  [ActionType.MEMBERS_CREATE]: EntityType.MEMBER,
  [ActionType.ORGANIZATIONS_MERGE]: EntityType.ORGANIZATION,
  [ActionType.ORGANIZATIONS_UNMERGE]: EntityType.ORGANIZATION,
  [ActionType.ORGANIZATIONS_EDIT_IDENTITIES]: EntityType.ORGANIZATION,
  [ActionType.ORGANIZATIONS_EDIT_PROFILE]: EntityType.ORGANIZATION,
  [ActionType.ORGANIZATIONS_CREATE]: EntityType.ORGANIZATION,
  [ActionType.INTEGRATIONS_CONNECT]: EntityType.INTEGRATION,
  [ActionType.INTEGRATIONS_RECONNECT]: EntityType.INTEGRATION,
  [ActionType.MEMBERS_VERIFY_IDENTITY]: EntityType.MEMBER,
  [ActionType.MEMBERS_VERIFY_WORK_EXPERIENCE]: EntityType.MEMBER,
}

let qx: QueryExecutor | undefined = undefined
export async function addAuditAction(options: AuditLogRequestOptions, action: AuditLogAction) {
  if (!qx) {
    const conn = await getDbConnection(WRITE_DB_CONFIG())
    qx = connQx(conn)
  }

  await qx.result(
    `
      INSERT INTO "auditLogAction" (
        "actorId",
        "actorType",
        "ipAddress",
        "userAgent",
        "requestId",
        "actionType",
        success,
        "entityId",
        "oldState",
        "newState",
        "diff",
        "error"
      )
      VALUES (
        $(actorId),
        $(actorType),
        $(ipAddress),
        $(userAgent),
        $(requestId),
        $(actionType),
        $(success),
        $(entityId),
        $(oldState),
        $(newState),
        $(diff),
        $(error)
      )
    `,
    {
      actorId: options.actorId,
      actorType: options.actorType,
      ipAddress: options.ipAddress,
      userAgent: options.userAgent,
      requestId: options.requestId,
      actionType: action.actionType,
      success: action.success,
      entityId: action.entityId,
      oldState: JSON.stringify(action.oldState),
      newState: JSON.stringify(action.newState),
      diff: JSON.stringify(action.diff),
      error: action.error ? JSON.stringify(action.error) : null,
    },
  )
}

export async function queryAuditLogs(qx: QueryExecutor, { limit, offset, filter }) {
  let actionType: string[] = undefined
  let where = ''

  if (filter?.entityId) {
    if (!validator.isUUID(filter?.entityId)) {
      return []
    }
    where += ` AND a."entityId" = $(entityId)`
  }

  if (filter?.actorId) {
    where += ` AND a."actorId" = $(actorId)`
  }

  if (filter?.actionType || filter?.not?.actionType) {
    const condition = filter.not ? 'NOT IN' : 'IN'
    where += ` AND a."actionType" ${condition} ($(actionType:csv))`
    actionType = filter.not ? filter.not.actionType.in : filter.actionType.in
  }

  const result = await qx.select(
    `
      SELECT
        a.*,
        JSON_BUILD_OBJECT(
          'id', a."actorId",
          'type', a."actorType",
          'fullName', COALESCE(u."fullName", a."userAgent"),
          'email', u.email
        ) AS "actor"
      FROM "auditLogAction" a
      LEFT JOIN users u ON a."actorType" = 'user' AND a."actorId" = u.id::text
      WHERE 1 = 1
        ${where}
      ORDER BY a.timestamp DESC
      LIMIT $(limit)
      OFFSET $(offset)
    `,
    {
      limit,
      offset,
      actorId: filter?.actorId,
      entityId: filter?.entityId,
      actionType,
    },
  )

  return result.map((row) => {
    row.entityType = ACTION_TYPES_ENTITY_TYPES[row.actionType]
    return row
  })
}

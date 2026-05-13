import validator from 'validator'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import {
  IMemberIdentity,
  IMemberUnmergeBackup,
  IMergeAction,
  IMergeActionColumns,
  IOrganizationUnmergeBackup,
  IUnmergeBackup,
  MemberIdentityType,
  MergeActionState,
  MergeActionStep,
  MergeActionType,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { QueryOptions, QueryResult, queryTable } from '../utils'

const MERGE_ACTIONS_COLUMNS: IMergeActionColumns[] = [
  'id',
  'type',
  'primaryId',
  'secondaryId',
  'createdAt',
  'updatedAt',
  'state',
  'step',
]

export async function queryMergeActions<T extends IMergeActionColumns>(
  qx: QueryExecutor,
  opts: QueryOptions<T>,
): Promise<QueryResult<T>[]> {
  return queryTable(qx, 'mergeActions', MERGE_ACTIONS_COLUMNS, opts)
}

export async function findEntityMergeActions(
  qx: QueryExecutor,
  entityId: string,
  type: MergeActionType,
  { state = [], limit = 20, offset = 0 },
): Promise<IMergeAction[]> {
  if (!validator.isUUID(entityId)) {
    throw new Error('Invalid entityId')
  }

  const conditions = [`(ma."primaryId" = $(entityId) OR ma."secondaryId" = $(entityId))`]

  if (type) {
    conditions.push(`ma.type = $(type)`)
  }

  if (state.length > 0) {
    conditions.push(`ma.state IN ($(state:csv))`)
  }

  const result = await qx.select(
    `
      SELECT
        ma."primaryId",
        ma."secondaryId",
        ma."state",
        ma."step"
      FROM "mergeActions" ma
      WHERE ${conditions.join(' AND ')}
      ORDER BY ma."createdAt" DESC
      LIMIT $(limit)
      OFFSET $(offset)
    `,
    {
      entityId,
      type,
      state,
      limit,
      offset,
    },
  )

  return result
}

export async function setMergeAction(
  qx: QueryExecutor,
  type: MergeActionType,
  primaryId: string,
  secondaryId: string,
  data: {
    step?: MergeActionStep
    state?: MergeActionState
  },
): Promise<boolean> {
  const setClauses = []
  const replacements: Record<string, unknown> = {
    tenantId: DEFAULT_TENANT_ID,
    type,
    primaryId,
    secondaryId,
  }

  if (data.step) {
    setClauses.push(`step = $(step)`)
    replacements.step = data.step
  }

  if (data.state) {
    setClauses.push(`state = $(state)`)
    replacements.state = data.state
  }

  const rowCount = await qx.result(
    `
        UPDATE "mergeActions"
        SET ${setClauses.join(', ')}
        WHERE "tenantId" = $(tenantId)
          AND type = $(type)
          AND "primaryId" = $(primaryId)
          AND "secondaryId" = $(secondaryId)
      `,
    replacements,
  )

  return rowCount > 0
}

export async function addMergeAction(
  qx: QueryExecutor,
  type: MergeActionType,
  primaryId: string,
  secondaryId: string,
  step: MergeActionStep,
  state: MergeActionState = MergeActionState.IN_PROGRESS,
  backup: IUnmergeBackup<IMemberUnmergeBackup | IOrganizationUnmergeBackup> = undefined,
  userId?: string,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO "mergeActions" ("tenantId", "type", "primaryId", "secondaryId", state, step, "unmergeBackup", "actionBy")
    VALUES ($(tenantId), $(type), $(primaryId), $(secondaryId), $(state), $(step), $(backup), $(userId))
    ON CONFLICT ("tenantId", "type", "primaryId", "secondaryId")
    DO UPDATE SET state = $(state), step = $(step), "unmergeBackup" = $(backup), "actionBy" = $(userId), "updatedAt" = now()
    `,
    {
      tenantId: DEFAULT_TENANT_ID,
      type,
      primaryId,
      secondaryId,
      step,
      state,
      backup: backup ? JSON.stringify(backup) : null,
      userId,
    },
  )
}

export async function findMergeBackup(
  qx: QueryExecutor,
  primaryId: string,
  type: MergeActionType.MEMBER,
  identity: IMemberIdentity,
): Promise<IMergeAction | null> {
  const records: IMergeAction[] = await qx.select(
    `
    SELECT *
    FROM "mergeActions" ma
    WHERE ma."primaryId" = $(primaryId)
      AND EXISTS(
        SELECT 1
        FROM jsonb_array_elements(ma."unmergeBackup" -> 'secondary' -> 'identities') AS identities
        WHERE (identities ->> 'username' = $(value)
               OR (identities ->> 'type' = $(type) AND identities ->> 'value' = $(value)))
          AND identities ->> 'platform' = $(platform)
      )
    `,
    {
      primaryId,
      value: identity.value,
      type: identity.type,
      platform: identity.platform,
    },
  )

  for (const record of records) {
    if (record.type === MergeActionType.MEMBER && record.unmergeBackup) {
      const backup = record.unmergeBackup as IUnmergeBackup<IMemberUnmergeBackup>
      for (const side of [backup.primary, backup.secondary]) {
        if (!side) continue
        for (const id of side.identities) {
          if ('username' in id) {
            id.value = (id as Record<string, unknown>).username as string
            id.type = MemberIdentityType.USERNAME
            delete (id as Record<string, unknown>).username
          }
        }
      }
    }
  }

  return records.length > 0 ? records[0] : null
}

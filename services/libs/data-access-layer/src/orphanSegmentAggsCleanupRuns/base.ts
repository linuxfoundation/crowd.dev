import { QueryExecutor } from '../queryExecutor'

export interface IOrphanCleanupRun {
  id?: string
  aggregateName: string
  startedAt: Date
  completedAt?: Date
  status: 'running' | 'completed' | 'failed'
  orphansFound: number
  orphansDeleted: number
  executionTimeMs?: number
  errorMessage?: string
}

export interface IOrphanCleanupRunIncrementalUpdate {
  incrementOrphansFound?: number
  incrementOrphansDeleted?: number
}

export async function startOrphanCleanupRun(
  qx: QueryExecutor,
  aggregateName: string,
): Promise<string> {
  // Check if there's already a running cleanup for this aggregate
  const existingRun = await qx.selectOneOrNone(
    `
    SELECT id, "startedAt"
    FROM "orphanSegmentAggsCleanupRuns"
    WHERE "tableName" = $(aggregateName)
      AND "status" = 'running'
    LIMIT 1
    `,
    { aggregateName },
  )

  if (existingRun) {
    return existingRun.id
  }

  // Create new cleanup run
  const result = await qx.selectOne(
    `
    INSERT INTO "orphanSegmentAggsCleanupRuns" (
      "tableName",
      "startedAt",
      "status",
      "orphansFound",
      "orphansDeleted"
    ) VALUES ($(aggregateName), NOW(), 'running', 0, 0)
    RETURNING id
    `,
    { aggregateName },
  )

  return result.id
}

export async function updateOrphanCleanupRun(
  qx: QueryExecutor,
  runId: string,
  updates: Partial<IOrphanCleanupRun> & IOrphanCleanupRunIncrementalUpdate,
): Promise<void> {
  const setClauses: string[] = []
  const params: Record<string, unknown> = { runId }

  if (updates.completedAt !== undefined) {
    setClauses.push(`"completedAt" = $(completedAt)`)
    params.completedAt = updates.completedAt
  }
  if (updates.status !== undefined) {
    setClauses.push(`"status" = $(status)`)
    params.status = updates.status
  }
  if (updates.orphansFound !== undefined) {
    setClauses.push(`"orphansFound" = $(orphansFound)`)
    params.orphansFound = updates.orphansFound
  }
  if (updates.orphansDeleted !== undefined) {
    setClauses.push(`"orphansDeleted" = $(orphansDeleted)`)
    params.orphansDeleted = updates.orphansDeleted
  }
  if (updates.incrementOrphansFound !== undefined) {
    setClauses.push(`"orphansFound" = "orphansFound" + $(incrementOrphansFound)`)
    params.incrementOrphansFound = updates.incrementOrphansFound
  }
  if (updates.incrementOrphansDeleted !== undefined) {
    setClauses.push(`"orphansDeleted" = "orphansDeleted" + $(incrementOrphansDeleted)`)
    params.incrementOrphansDeleted = updates.incrementOrphansDeleted
  }
  if (updates.executionTimeMs !== undefined) {
    setClauses.push(`"executionTimeMs" = $(executionTimeMs)`)
    params.executionTimeMs = updates.executionTimeMs
  }
  if (updates.errorMessage !== undefined) {
    setClauses.push(`"errorMessage" = $(errorMessage)`)
    params.errorMessage = updates.errorMessage
  }

  if (setClauses.length === 0) {
    return
  }

  await qx.result(
    `
    UPDATE "orphanSegmentAggsCleanupRuns"
    SET ${setClauses.join(', ')}
    WHERE id = $(runId)
    `,
    params,
  )
}

import type { ICredlyExpiryCursor } from '@crowd/data-access-layer/src/credly/badges'

import { assertCredentialKind } from '../../../types'
import type { SyncContext, SyncOutcome } from '../../../types'
import type { CredlyDeps } from '../deps'
import { credlyActivitySchema, expiredBadgeActivityFromRow } from '../mapper'
import type { CredlyActivity } from '../mapper'

export const EXPIRY_BATCH_SIZE = 500

interface ExpiryGroup {
  target: { segmentId: string; integrationId: string }
  badgeIds: string[]
  activities: CredlyActivity[]
}

// Expiries come from our own table, never from Credly: a badge is emitted as expired once, on the
// segment it was first routed to.
export async function runExpirySync(ctx: SyncContext<CredlyDeps>): Promise<SyncOutcome> {
  assertCredentialKind(await ctx.getCredential(), 'credly-oauth-client')
  const counters = { batches: 0, due: 0, emitted: 0, skipped: 0, marked: 0 }
  let cursor: ICredlyExpiryCursor | undefined
  let drained = true

  while (true) {
    if (!ctx.hasRunBudget()) {
      drained = false
      break
    }
    const due = await ctx.deps.listDueExpiries(new Date(), EXPIRY_BATCH_SIZE, cursor)
    if (due.length === 0) break
    counters.batches += 1
    counters.due += due.length
    const last = due[due.length - 1]
    cursor = { expiresAt: last.expiresAt as string, badgeId: last.badgeId }

    const groups = new Map<string, ExpiryGroup>()
    for (const row of due) {
      let activity: CredlyActivity
      try {
        activity = credlyActivitySchema.parse(expiredBadgeActivityFromRow(row))
      } catch (err) {
        counters.skipped += 1
        ctx.log.warn(
          { badgeId: row.badgeId, errMsg: (err as Error).message },
          'skipping credly expiry: stored badge cannot be mapped',
        )
        continue
      }
      const key = `${row.integrationId}:${row.segmentId}`
      const group = groups.get(key) ?? {
        target: { segmentId: row.segmentId, integrationId: row.integrationId },
        badgeIds: [],
        activities: [],
      }
      group.badgeIds.push(row.badgeId)
      group.activities.push(activity)
      groups.set(key, group)
    }
    for (const { target, badgeIds, activities } of groups.values()) {
      await ctx.emit(activities, target)
      counters.emitted += activities.length
      if (ctx.emitEnabled) counters.marked += await ctx.deps.markExpiriesEmitted(badgeIds)
    }
    if (!ctx.emitEnabled || due.length < EXPIRY_BATCH_SIZE) break
  }

  ctx.log.info({ event: 'credly_expiry_sync_run', ...counters, drained }, 'credly expiry sync run')
  return { complete: drained }
}

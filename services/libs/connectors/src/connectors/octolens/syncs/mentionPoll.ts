import { assertCredentialKind } from '../../../types'
import type { SyncContext, SyncOutcome } from '../../../types'
import type { OctolensDeps, OctolensKeywordTarget } from '../deps'
import { mapMentionToActivity, octolensActivitySchema, toIsoTimestamp } from '../mapper'
import type { OctolensActivity } from '../mapper'
import { fetchMentionsPage } from '../mentionsClient'
import type { OctolensMention } from '../mentionsClient'

// Worst case per run is 20 + 8 = 28 pages, 336/hour at the 5-minute cadence before retries, against
// Octolens's shared 500/hour budget; a regular cycle longer than 20 pages resumes next cadence.
const MAX_POLL_PAGES_PER_RUN = 20
const MAX_BACKFILL_PAGES_PER_RUN = 8

interface KeywordBackfill {
  keywordId: number
  cursor: string | null
}

interface OctolensWatermark {
  startDate: string | null
  cursor: string | null
  // Running max timestamp for the poll cycle in progress, separate from startDate (which
  // must stay frozen all cycle) so a resumed cycle doesn't lose earlier pages' progress.
  pendingMaxTimestamp: string | null
  // Filled only by the manual backfill script; each keyword's whole history is re-fetched.
  backfill: KeywordBackfill[]
}

function readWatermark(raw: Record<string, unknown> | null): OctolensWatermark {
  return {
    startDate: typeof raw?.startDate === 'string' ? raw.startDate : null,
    cursor: typeof raw?.cursor === 'string' ? raw.cursor : null,
    pendingMaxTimestamp:
      typeof raw?.pendingMaxTimestamp === 'string' ? raw.pendingMaxTimestamp : null,
    backfill: Array.isArray(raw?.backfill)
      ? raw.backfill
          .filter(
            (item): item is KeywordBackfill =>
              Number.isInteger(item?.keywordId) &&
              (item.cursor === null || typeof item.cursor === 'string'),
          )
          .map(({ keywordId, cursor }) => ({ keywordId, cursor }))
      : [],
  }
}

export async function runMentionPoll(ctx: SyncContext<OctolensDeps>): Promise<SyncOutcome> {
  const credential = await ctx.getCredential()
  assertCredentialKind(credential, 'octolens-token')
  const { token } = credential.data

  const watermark = readWatermark(ctx.watermark)
  const backfill: KeywordBackfill[] = []
  for (const item of watermark.backfill) {
    if (await ctx.deps.resolveKeywordTarget(item.keywordId)) {
      backfill.push(item)
    } else {
      ctx.log.warn({ keywordId: item.keywordId }, 'dropping octolens keyword backfill: no mapping')
    }
  }

  // Passed on every page this run: undocumented whether Octolens's cursor keeps the original filters.
  const startDate = watermark.startDate ?? firstRunStartDate()
  let cursor: string | undefined = watermark.cursor ?? undefined
  let maxTimestampSeen = watermark.pendingMaxTimestamp ?? startDate
  // Not read via ctx.http (its token pool is unseeded here, see Manifest), so
  // sync_run_summary's requestCount always reads 0 for octolens — logged separately below.
  let pagesFetched = 0

  const commit = (poll: Pick<OctolensWatermark, 'startDate' | 'cursor' | 'pendingMaxTimestamp'>) =>
    ctx.commitWatermark({
      ...poll,
      backfill: backfill.map((item) => ({ ...item })),
    })

  const emitMentions = async (
    mentions: OctolensMention[],
    onTimestamp?: (isoTimestamp: string) => void,
  ) => {
    const byTarget = new Map<
      string,
      { target: OctolensKeywordTarget; activities: Record<string, unknown>[] }
    >()
    for (const mention of mentions) {
      try {
        const isoTimestamp = toIsoTimestamp(mention.timestamp)
        onTimestamp?.(isoTimestamp)
      } catch (err) {
        // Optional chaining: a null/non-object page.data entry must not crash this handler too.
        ctx.log.warn(
          { sourceId: mention?.sourceId, errMsg: (err as Error).message },
          'skipping octolens mention: failed to process (malformed provider data)',
        )
        continue
      }

      // The request already excludes these; this keeps them out if Octolens's default changes.
      if (mention.relevance === 'not_relevant') {
        ctx.log.warn({ sourceId: mention.sourceId }, 'skipping octolens mention: not relevant')
        continue
      }

      // Not wrapped above: a transient DB failure here must propagate as a real run
      // failure to retry, not be swallowed as "malformed mention data."
      const keywordIds = mentionKeywordIds(mention)
      let chosen: { keywordId: number; target: OctolensKeywordTarget } | null = null
      for (const keywordId of keywordIds) {
        const target = await ctx.deps.resolveKeywordTarget(keywordId)
        if (target) {
          chosen = { keywordId, target }
          break
        }
      }
      if (!chosen) {
        ctx.log.debug({ keywordIds }, 'skipping octolens mention: no mapped keyword')
        continue
      }

      let mapped: OctolensActivity | null
      try {
        mapped = mapMentionToActivity({ ...mention, keywordId: chosen.keywordId })
        // Catches a malformed title/body/url that passes the mapper but would otherwise
        // crash createEmit's own schema parse, uncaught, outside this loop.
        if (mapped) octolensActivitySchema.parse(mapped)
      } catch (err) {
        ctx.log.warn(
          { sourceId: mention?.sourceId, errMsg: (err as Error).message },
          'skipping octolens mention: failed to process (malformed provider data)',
        )
        continue
      }

      if (!mapped) {
        ctx.log.debug(
          { source: mention.source, authorUrl: mention.authorUrl },
          'skipping octolens mention: platform not ingested or no author identity',
        )
        continue
      }

      const key = `${chosen.target.integrationId}:${chosen.target.segmentId}`
      const group = byTarget.get(key) ?? { target: chosen.target, activities: [] }
      group.activities.push(mapped)
      byTarget.set(key, group)
    }

    for (const { target, activities } of byTarget.values()) {
      await ctx.emit(activities, target)
    }
  }

  // Saved before any fetch, so a failed first run retries the same window instead of a later "now".
  if (!watermark.startDate) {
    await commit({ startDate, cursor: null, pendingMaxTimestamp: null })
  }

  let polled = false
  while (pagesFetched < MAX_POLL_PAGES_PER_RUN && ctx.hasRunBudget()) {
    const page = await fetchMentionsPage(token, { startDate, cursor })
    pagesFetched += 1

    await emitMentions(page.data, (isoTimestamp) => {
      if (!maxTimestampSeen || isoTimestamp > maxTimestampSeen) {
        maxTimestampSeen = isoTimestamp
      }
    })

    if (!page.nextCursor) {
      // Fully drained, so safe to advance regardless of Octolens's sort order. Inclusive
      // startDate re-fetches the boundary mention next run, harmlessly deduped downstream.
      await commit({ startDate: maxTimestampSeen, cursor: null, pendingMaxTimestamp: null })
      polled = true
      break
    }

    // Not drained: resume via cursor rather than advancing startDate, which (if newest-first)
    // could skip older, unseen pages forever if this run is cut short here.
    cursor = page.nextCursor
    await commit({
      startDate,
      cursor,
      pendingMaxTimestamp: maxTimestampSeen,
    })
  }

  if (!polled && pagesFetched >= MAX_POLL_PAGES_PER_RUN) {
    ctx.log.info(
      { pagesFetched },
      'octolens mention poll paused at the page cap until next cadence',
    )
    return { complete: false, resumeAtCadence: true }
  }
  if (!polled) {
    ctx.log.info({ pagesFetched }, 'octolens mention poll deferred: run budget exhausted')
    return { complete: false }
  }

  // Re-fetched mentions dedupe downstream by sourceId.
  const drained = { startDate: maxTimestampSeen, cursor: null, pendingMaxTimestamp: null }
  let backfillPages = 0
  while (backfill.length > 0 && backfillPages < MAX_BACKFILL_PAGES_PER_RUN && ctx.hasRunBudget()) {
    const item = backfill[0]
    const page = await fetchMentionsPage(token, {
      keywordIds: [item.keywordId],
      cursor: item.cursor ?? undefined,
    })
    pagesFetched += 1
    backfillPages += 1

    await emitMentions(page.data)

    if (page.nextCursor && page.nextCursor !== item.cursor) {
      item.cursor = page.nextCursor
    } else {
      if (page.nextCursor) {
        ctx.log.warn(
          { keywordId: item.keywordId },
          'octolens keyword backfill cursor did not advance',
        )
      }
      backfill.shift()
      ctx.log.info({ keywordId: item.keywordId }, 'octolens keyword backfill complete')
    }
    await commit(drained)
  }

  // Unfinished backfill resumes at the next regular run rather than a short defer, which is the pacing.
  ctx.log.info(
    { pagesFetched, backfillKeywords: backfill.map((item) => item.keywordId) },
    'octolens mention poll complete',
  )
  return { complete: true }
}

// Octolens lists a mention's keywords in keywords[]; the top-level keywordId is optional and in
// practice absent. Order matters: the first mapped one decides the project.
function mentionKeywordIds(mention: OctolensMention): number[] {
  const ids = [
    mention.keywordId,
    ...(Array.isArray(mention.keywords) ? mention.keywords.map((k) => k?.id) : []),
  ]
  return [...new Set(ids.filter((id): id is number => Number.isInteger(id)))]
}

// Older mentions arrive only through a manually queued backfill.
function firstRunStartDate(): string {
  return new Date().toISOString()
}

export interface CoveredWindow {
  confirmedThrough: string
  coveredUntil: string
}

export interface GithubWatermark {
  phase: 'backfill' | 'incremental'
  since: string | null
  cursor: string | null
  coveredWindows: CoveredWindow[]
}

export const PAGE_SIZE = 100
// GitHub GraphQL silently omits timeline/connection items from heavy nodes(ids:) batches
// (no error, pageInfo claims completeness) — fetch per item, bounded by this concurrency.
export const ITEM_FETCH_CONCURRENCY = 5

function readCoveredWindows(raw: Record<string, unknown>): CoveredWindow[] {
  if (Array.isArray(raw.coveredWindows)) {
    return raw.coveredWindows.filter(
      (w): w is CoveredWindow =>
        typeof w === 'object' &&
        w !== null &&
        typeof (w as CoveredWindow).confirmedThrough === 'string' &&
        typeof (w as CoveredWindow).coveredUntil === 'string',
    )
  }
  // legacy single-window watermark, carried over so existing units keep their progress
  if (typeof raw.confirmedThrough === 'string' && typeof raw.coveredUntil === 'string') {
    return [{ confirmedThrough: raw.confirmedThrough, coveredUntil: raw.coveredUntil }]
  }
  return []
}

export function readWatermark(raw: Record<string, unknown> | null): GithubWatermark {
  if (raw && (raw.phase === 'backfill' || raw.phase === 'incremental')) {
    return {
      phase: raw.phase,
      since: typeof raw.since === 'string' ? raw.since : null,
      cursor: typeof raw.cursor === 'string' ? raw.cursor : null,
      coveredWindows: readCoveredWindows(raw),
    }
  }
  return {
    phase: 'backfill',
    since: null,
    cursor: null,
    coveredWindows: [],
  }
}

export function parseRepoChannel(channelName: string): { owner: string; repo: string } {
  const path = channelName.replace(/^https:\/\/github\.com\//, '').replace(/\/+$/, '')
  const parts = path.split('/')
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error(`invalid github repo channel name: ${channelName}`)
  }
  return { owner: parts[0], repo: parts[1] }
}

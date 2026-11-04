import {
  IDbRepoScorecardCheckUpsert,
  IDbRepoScorecardUpdate,
} from '@crowd/data-access-layer/src/osspckgs/types'

import { stripNullBytesDeep } from '../utils/stripNullBytesDeep'
import { ScorecardPayload } from './scorecardApi'

export interface ScorecardRows {
  repo: IDbRepoScorecardUpdate
  checks: IDbRepoScorecardCheckUpsert[]
}

const NOT_APPLICABLE_CHECK_SCORE = -1

export function scorecardProjectPath(cdpUrl: string): string | null {
  let parsed: URL
  try {
    parsed = new URL(cdpUrl)
  } catch {
    return null
  }
  if (parsed.hostname !== 'github.com') return null

  const segments = parsed.pathname
    .replace(/\.git$/, '')
    .replace(/\/+$/, '')
    .split('/')
    .filter(Boolean)
  if (segments.length !== 2) return null

  return `github.com/${segments[0]}/${segments[1]}`
}

export function toScorecardRows(repoId: number, payload: ScorecardPayload): ScorecardRows | null {
  const lastRunAt = new Date(payload.date)
  if (Number.isNaN(lastRunAt.getTime())) return null

  const score = Math.round(payload.score * 10) / 10
  if (!Number.isFinite(score)) return null

  const checksByName = new Map<string, IDbRepoScorecardCheckUpsert>()
  for (const check of payload.checks) {
    checksByName.set(check.name, {
      repoId,
      checkName: check.name,
      score: check.score === NOT_APPLICABLE_CHECK_SCORE ? null : check.score,
      reason: stripNullBytesDeep(check.reason ?? ''),
    })
  }

  return {
    repo: { repoId, score, lastRunAt },
    checks: [...checksByName.values()],
  }
}

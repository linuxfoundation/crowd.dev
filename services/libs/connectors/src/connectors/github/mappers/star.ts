import { createHash } from 'crypto'

import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { StargazerEdge } from '../graphql/stars'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

const DEFAULT_TIMESTAMP = '1970-01-01T00:00:00Z'

export function toStar(edge: StargazerEdge): GithubActivity {
  const timestamp = edge.starredAt ?? DEFAULT_TIMESTAMP
  const sourceIdData = `${edge.node?.login}-star-${Math.floor(
    new Date(timestamp).getTime() / 1000,
  )}-github`

  return {
    type: GithubActivityType.STAR,
    timestamp,
    sourceId: `gen-${createHash('md5').update(sourceIdData).digest('hex')}`,
    score: GITHUB_GRID[GithubActivityType.STAR].score,
    attributes: {},
    member: toMember(edge.node),
  }
}

import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { ForkNode } from '../graphql/forks'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

export function toFork(node: ForkNode): GithubActivity {
  return {
    type: GithubActivityType.FORK,
    timestamp: node.createdAt,
    sourceId: node.id,
    score: GITHUB_GRID[GithubActivityType.FORK].score,
    attributes: node.parent?.isFork
      ? {
          isForkByOrg: node.isInOrganization,
          directParent: node.parent.nameWithOwner,
          isIndirectFork: String(node.parent.isFork),
        }
      : {},
    member: toMember(node.owner),
  }
}

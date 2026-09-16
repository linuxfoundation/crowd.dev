import { ICanonicalRepoUrl } from '@crowd/common'

export const PRECHECK_SKIP_REASONS = {
  notGithub: 'evaluation pre-check: repository is not hosted on GitHub',
  alreadyInCdp: 'evaluation pre-check: repository already tracked in CDP',
  lfOwner: 'evaluation pre-check: owner is exclusively mapped to LF projects in CDP',
} as const

export interface IPrecheckFacts {
  reposInCdp: Set<string>
  exclusivelyLfOwners: Set<string>
}

// Order matters: cheapest/most certain evidence first. A URL that fails to
// canonicalize is not a skip — it falls through to the agent.
export function resolvePrecheckSkipReason(
  canonical: ICanonicalRepoUrl | null,
  facts: IPrecheckFacts,
): string | null {
  if (!canonical) {
    return null
  }

  if (!canonical.isGithub) {
    return PRECHECK_SKIP_REASONS.notGithub
  }

  if (facts.reposInCdp.has(canonical.url)) {
    return PRECHECK_SKIP_REASONS.alreadyInCdp
  }

  if (facts.exclusivelyLfOwners.has(canonical.owner)) {
    return PRECHECK_SKIP_REASONS.lfOwner
  }

  return null
}

export function computeExclusivelyLfOwners(
  lfOwners: Set<string>,
  nonLfOwners: Set<string>,
): Set<string> {
  return new Set([...lfOwners].filter((owner) => !nonLfOwners.has(owner)))
}

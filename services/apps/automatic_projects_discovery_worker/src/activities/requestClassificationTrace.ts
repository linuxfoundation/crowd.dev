import {
  CdpIntegrationAction,
  CdpSegmentLookupResult,
  IOnboardingRequestLookups,
  IParsedOnboardingRequest,
  IPccCandidate,
  OnboardingResolution,
  assessPccCandidates,
} from '@crowd/project-onboarding'

export type ClassificationFailureStage = 'parse' | 'resolve'

export interface IClassificationFailure {
  stage: ClassificationFailureStage
  reason: string
}

interface IPccLookupTrace {
  projectName: string
  candidates: IPccCandidate[]
}

export interface IClassificationTrace {
  parsed: IParsedOnboardingRequest | null
  pccLookup: IPccLookupTrace | null
  cdpLookup: { result: CdpSegmentLookupResult } | null
  failure: IClassificationFailure | null
}

export type ClassificationNode =
  | 'not_github_source'
  | 'non_lf_create_in_external_group'
  | 'lf_not_in_pcc_flag_human'
  | 'lf_in_pcc_not_in_cdp_human_review'
  | 'lf_in_cdp_integration_none_create'
  | 'lf_in_cdp_github_nango_update'
  | 'lf_in_cdp_github_v1_human_review'
  | 'ambiguous_human_review'

const LF_IN_CDP_NODES: Record<CdpIntegrationAction, ClassificationNode> = {
  create_integration: 'lf_in_cdp_integration_none_create',
  update_integration: 'lf_in_cdp_github_nango_update',
  human_review: 'lf_in_cdp_github_v1_human_review',
}

export function createClassificationTrace(): IClassificationTrace {
  return { parsed: null, pccLookup: null, cdpLookup: null, failure: null }
}

export function traceLookups(
  lookups: IOnboardingRequestLookups,
  trace: IClassificationTrace,
): IOnboardingRequestLookups {
  const { findPccCandidates, findCdpSegmentByPccProject } = lookups

  return {
    findPccCandidates: findPccCandidates
      ? async (projectName) => {
          const candidates = await findPccCandidates(projectName)
          trace.pccLookup = { projectName, candidates }
          return candidates
        }
      : undefined,
    findCdpSegmentByPccProject: async (pccProjectId) => {
      const result = await findCdpSegmentByPccProject(pccProjectId)
      trace.cdpLookup = { result }
      return result
    },
  }
}

export function countNodes(
  nodes: ClassificationNode[],
): Partial<Record<ClassificationNode, number>> {
  const counts: Partial<Record<ClassificationNode, number>> = {}
  for (const node of nodes) {
    counts[node] = (counts[node] ?? 0) + 1
  }
  return counts
}

export function toClassificationNode(resolution: OnboardingResolution): ClassificationNode {
  switch (resolution.kind) {
    case 'non_github_source':
      return 'not_github_source'
    case 'non_lf_new_project':
      return 'non_lf_create_in_external_group'
    case 'lf_not_in_pcc':
      return 'lf_not_in_pcc_flag_human'
    case 'lf_not_in_cdp':
      return 'lf_in_pcc_not_in_cdp_human_review'
    case 'lf_in_cdp':
      return LF_IN_CDP_NODES[resolution.action]
    case 'ambiguous':
      return 'ambiguous_human_review'
  }
}

function describeRequest(parsed: IParsedOnboardingRequest | null) {
  return parsed
    ? {
        githubRepos: parsed.githubRepoUrls.length,
        nonGithubRepos: parsed.nonGithubRepoUrls.length,
        linksToFollow: parsed.linksToFollow.length,
        projectName: parsed.projectName,
        declaredLf: parsed.declaredLf,
        asksAboutHierarchy: parsed.asksAboutHierarchy,
      }
    : null
}

function describePccLookup(pccLookup: IPccLookupTrace | null) {
  if (!pccLookup) {
    return null
  }

  const assessment = assessPccCandidates(pccLookup.projectName, pccLookup.candidates)

  return {
    level: assessment.level,
    margin: assessment.margin,
    thresholds: assessment.thresholds,
    tiedCandidates: assessment.tiedCandidates.length,
    best: assessment.best,
    candidates: pccLookup.candidates,
  }
}

function describeCdpLookup(cdpLookup: IClassificationTrace['cdpLookup']) {
  return cdpLookup ? { result: cdpLookup.result } : null
}

export function buildClassificationLogEntry(
  sourceUrl: string,
  resolution: OnboardingResolution,
  trace: IClassificationTrace,
) {
  return {
    sourceUrl,
    node: toClassificationNode(resolution),
    kind: resolution.kind,
    failure: trace.failure,
    request: describeRequest(trace.parsed),
    pcc: describePccLookup(trace.pccLookup),
    cdp: describeCdpLookup(trace.cdpLookup),
  }
}

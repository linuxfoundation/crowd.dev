import { canonicalizeRepoUrl } from '@crowd/common'
import {
  IRequestHints,
  IRequestQuestion,
  IRequestQuestionOption,
} from '@crowd/data-access-layer/src/project-catalog/types'

import { MAX_BUTTON_TEXT_LENGTH } from './botCopy'
import { OnboardingConfigurationError } from './errors'
import { IParsedOnboardingRequest } from './requestParser'

export const PCC_MATCH_THRESHOLDS = { strong: 0.97, weak: 0.85 } as const

const GITHUB_V1_PLATFORM = 'github'
const GITHUB_NANGO_PLATFORM = 'github-nango'

export interface IPccCandidate {
  projectId: string
  name: string
  slug: string
  score: number
  isLeaf: boolean
}

export type CdpIntegrationState = 'none' | 'github-nango' | 'github-v1'

export interface ICdpSegmentMatch {
  segmentId: string
  name: string
  slug: string
  integration: CdpIntegrationState
  repositoryCount?: number
}

export interface IDuplicateCdpSegments {
  duplicates: ICdpSegmentMatch[]
}

export type CdpSegmentLookupResult = ICdpSegmentMatch | IDuplicateCdpSegments | null

export interface IOnboardingRequestLookups {
  findPccCandidates?: (projectName: string) => Promise<IPccCandidate[]>
  findCdpSegmentByPccProject: (pccProjectId: string) => Promise<CdpSegmentLookupResult>
  findTrackedRepoUrls?: (canonicalGithubRepoUrls: string[]) => Promise<Set<string>>
  findExclusivelyLfOwners?: (owners: string[]) => Promise<Set<string>>
}

export interface IResolveOptions {
  canAsk?: boolean
}

export type CdpIntegrationAction = 'create_integration' | 'update_integration' | 'human_review'

export type PccMatchLevel = 'exact' | 'strong' | 'weak' | 'none'

export interface IPccMatchAssessment {
  level: PccMatchLevel
  best: IPccCandidate | null
  weakCandidates: IPccCandidate[]
  tiedCandidates: IPccCandidate[]
  margin: number | null
  thresholds: typeof PCC_MATCH_THRESHOLDS
}

export type OnboardingResolution =
  | { kind: 'non_github_source'; nonGithubRepoUrls: string[] }
  | { kind: 'already_tracked'; repoUrls: string[] }
  | { kind: 'non_lf_new_project'; projectName: string | null }
  | { kind: 'lf_not_in_pcc'; projectName: string }
  | { kind: 'lf_not_in_cdp'; pccProject: IPccCandidate }
  | {
      kind: 'lf_in_cdp'
      pccProject: IPccCandidate
      segment: ICdpSegmentMatch
      action: CdpIntegrationAction
    }
  | { kind: 'lf_owner_in_cdp'; owners: string[] }
  | { kind: 'ambiguous'; reason: string; candidates: IPccCandidate[] }
  | {
      kind: 'needs_input'
      question: IRequestQuestion
      fallbackReason: string
      candidates: IPccCandidate[]
    }

export type OnboardingResolutionKind = OnboardingResolution['kind']

export const INTEGRATION_ACTIONS: Record<CdpIntegrationState, CdpIntegrationAction> = {
  none: 'create_integration',
  'github-nango': 'update_integration',
  'github-v1': 'human_review',
}

export function toCdpIntegrationState(platforms: string[]): CdpIntegrationState {
  if (platforms.includes(GITHUB_V1_PLATFORM)) {
    return 'github-v1'
  }

  return platforms.includes(GITHUB_NANGO_PLATFORM) ? 'github-nango' : 'none'
}

export function normalizeName(value: string): string {
  return value.toLowerCase().replace(/[^\p{L}\p{N}+#]/gu, '')
}

const LEADING_WRAPPER_WORD = 'the'
const TRAILING_WRAPPER_WORD = 'project'

export function coreProjectName(value: string): string {
  const lowered = value.trim().toLowerCase()
  const words = lowered.split(/\s+/)
  const core = words
    .filter(
      (word, index) =>
        !(index === 0 && word === LEADING_WRAPPER_WORD) &&
        !(index === words.length - 1 && word === TRAILING_WRAPPER_WORD),
    )
    .join(' ')
  return core || lowered
}

export function sameProjectName(requested: string, candidateName: string): boolean {
  const candidateNames = new Set([
    normalizeName(candidateName),
    normalizeName(coreProjectName(candidateName)),
  ])
  return (
    candidateNames.has(normalizeName(requested)) ||
    candidateNames.has(normalizeName(coreProjectName(requested)))
  )
}

function isExactMatch(projectName: string, candidate: IPccCandidate): boolean {
  const requested = normalizeName(projectName)
  if (!requested) {
    return false
  }

  const slug = normalizeName(candidate.slug)
  return (
    sameProjectName(projectName, candidate.name) ||
    requested === slug ||
    normalizeName(coreProjectName(projectName)) === slug
  )
}

function rankCandidates(projectName: string, candidates: IPccCandidate[]): IPccCandidate[] {
  return [...candidates].sort(
    (a, b) =>
      Number(isExactMatch(projectName, b)) - Number(isExactMatch(projectName, a)) ||
      b.score - a.score ||
      Number(b.isLeaf) - Number(a.isLeaf),
  )
}

function findTiedCandidates(
  projectName: string,
  ranked: IPccCandidate[],
  best: IPccCandidate | undefined,
): IPccCandidate[] {
  if (!best) {
    return []
  }

  const bestIsExact = isExactMatch(projectName, best)

  return ranked.filter(
    (candidate) =>
      candidate.isLeaf === best.isLeaf &&
      isExactMatch(projectName, candidate) === bestIsExact &&
      (bestIsExact || candidate.score === best.score),
  )
}

function toMatchLevel(projectName: string, best: IPccCandidate | undefined): PccMatchLevel {
  if (!best) {
    return 'none'
  }

  if (isExactMatch(projectName, best)) {
    return 'exact'
  }

  if (best.score >= PCC_MATCH_THRESHOLDS.strong) {
    return 'strong'
  }

  return best.score >= PCC_MATCH_THRESHOLDS.weak ? 'weak' : 'none'
}

export function assessPccCandidates(
  projectName: string,
  candidates: IPccCandidate[],
): IPccMatchAssessment {
  const ranked = rankCandidates(projectName, candidates)
  const [best, runnerUp] = ranked

  return {
    level: toMatchLevel(projectName, best),
    best: best ?? null,
    weakCandidates: ranked.filter((candidate) => candidate.score >= PCC_MATCH_THRESHOLDS.weak),
    tiedCandidates: findTiedCandidates(projectName, ranked, best),
    margin: best ? best.score - (runnerUp?.score ?? 0) : null,
    thresholds: PCC_MATCH_THRESHOLDS,
  }
}

function hasDuplicateSegments(result: CdpSegmentLookupResult): result is IDuplicateCdpSegments {
  return result !== null && 'duplicates' in result
}

function hasNoRepositories(request: IParsedOnboardingRequest): boolean {
  return request.githubRepoUrls.length === 0 && request.nonGithubRepoUrls.length === 0
}

function hasOnlyNonGithubRepositories(request: IParsedOnboardingRequest): boolean {
  return request.githubRepoUrls.length === 0 && request.nonGithubRepoUrls.length > 0
}

function ambiguous(reason: string, candidates: IPccCandidate[] = []): OnboardingResolution {
  return { kind: 'ambiguous', reason, candidates }
}

const NOT_SURE_OPTION: IRequestQuestionOption = {
  id: 'not_sure',
  label: 'Not sure, ask the CDP team',
  handOff: true,
}

function toHintProject({ projectId, name, slug, isLeaf }: IPccCandidate) {
  return { projectId, name, slug, isLeaf }
}

function candidateLabel(candidate: IPccCandidate): string {
  return `${candidate.name} (${candidate.slug})`
}

function needsInput(
  question: IRequestQuestion,
  fallbackReason: string,
  candidates: IPccCandidate[] = [],
): OnboardingResolution {
  return { kind: 'needs_input', question, fallbackReason, candidates }
}

function didYouMean(projectName: string, best: IPccCandidate): OnboardingResolution {
  return needsInput(
    {
      kind: 'did_you_mean',
      text: `I couldn't find a project called "${projectName}". Did you mean ${best.name}?`,
      options: [
        {
          id: 'yes',
          label: `Yes, ${best.name}`,
          hints: { pccProject: toHintProject(best) },
          suggested: true,
        },
        { id: 'no', label: 'No, ask the CDP team', handOff: true },
      ],
    },
    'the name only loosely matches projects in the foundation project list',
    [best],
  )
}

function pickAmongProjects(projectName: string, candidates: IPccCandidate[]): OnboardingResolution {
  return needsInput(
    {
      kind: 'pick_project',
      text: `Several LF projects are called ${projectName}. Which one is this repo part of?`,
      options: [
        ...candidates.map((candidate) => ({
          id: candidate.projectId,
          label: candidateLabel(candidate),
          hints: { pccProject: toHintProject(candidate) },
        })),
        NOT_SURE_OPTION,
      ],
    },
    'several projects in the foundation project list match this name equally',
    candidates,
  )
}

function lfConflict(
  request: IParsedOnboardingRequest,
  pccProject: IPccCandidate,
): OnboardingResolution {
  const name = request.projectName ?? pccProject.name
  return needsInput(
    {
      kind: 'lf_conflict',
      text: `You said ${name} isn't an LF project, but I found an LF project called ${pccProject.name}. Which is right?`,
      options: [
        {
          id: 'use_lf',
          label: `Use the LF project ${pccProject.name}`,
          hints: { declaredLf: true, pccProject: toHintProject(pccProject) },
        },
        { id: 'not_lf', label: "It's not LF, treat it as new", hints: { treatAsNew: true } },
      ],
    },
    `the request says it's not an LF project, but the foundation project list has a project called ${pccProject.name}`,
    [pccProject],
  )
}

function lfConfirm(projectName: string): OnboardingResolution {
  return needsInput(
    {
      kind: 'lf_confirm',
      text: `I couldn't find ${projectName} in the foundation project list. Is it a Linux Foundation project?`,
      options: [
        { id: 'lf', label: "Yes, it's an LF project", hints: { declaredLf: true } },
        { id: 'not_lf', label: "No, it's not LF", hints: { treatAsNew: true } },
        NOT_SURE_OPTION,
      ],
    },
    `${projectName} isn't in the foundation project list and the request doesn't say whether it is an LF project`,
  )
}

function repositoriesLabel(count: number | undefined): string {
  if (count === undefined) {
    return ''
  }
  return count === 0 ? ' · no repos yet' : ` · ${count} repo${count === 1 ? '' : 's'}`
}

const SEGMENT_LABEL_SUFFIXES: Array<(segment: ICdpSegmentMatch) => string> = [
  () => '',
  (segment) => repositoriesLabel(segment.repositoryCount),
  (segment) => ` (${segment.slug})${repositoriesLabel(segment.repositoryCount)}`,
]

function fitButtonLabel(name: string, suffix: string): string {
  const label = `${name}${suffix}`
  if (label.length <= MAX_BUTTON_TEXT_LENGTH) {
    return label
  }
  const room = Math.max(MAX_BUTTON_TEXT_LENGTH - suffix.length - 1, 1)
  return `${name.slice(0, room)}…${suffix}`
}

function segmentLabels(segments: ICdpSegmentMatch[]): Map<string, string> {
  const candidates = SEGMENT_LABEL_SUFFIXES.map((suffix) =>
    segments.map((segment) => fitButtonLabel(segment.name, suffix(segment))),
  )
  const labels =
    candidates.find((level) => new Set(level).size === level.length) ??
    candidates[candidates.length - 1]
  return new Map(segments.map((segment, index) => [segment.segmentId, labels[index]]))
}

function pickAmongSegments(
  pccProject: IPccCandidate,
  segments: ICdpSegmentMatch[],
): OnboardingResolution {
  const labels = segmentLabels(segments)
  return needsInput(
    {
      kind: 'pick_project',
      text: `There are ${segments.length} ${pccProject.name} projects in LFX Insights. Which one should this repo go in?`,
      options: [
        ...segments.map((segment) => ({
          id: segment.segmentId,
          label: labels.get(segment.segmentId) ?? segment.name,
          hints: { segmentId: segment.segmentId, pccProject: toHintProject(pccProject) },
        })),
        NOT_SURE_OPTION,
      ],
    },
    'more than one LFX Insights project shares this foundation project',
    [pccProject],
  )
}

function chooseSegment(
  segments: ICdpSegmentMatch[],
  hints: IRequestHints,
): ICdpSegmentMatch | null {
  return segments.find((segment) => segment.segmentId === hints.segmentId) ?? null
}

async function resolveStrongMatch(
  request: IParsedOnboardingRequest,
  assessment: IPccMatchAssessment,
  pccProject: IPccCandidate,
  lookups: IOnboardingRequestLookups,
  hints: IRequestHints,
): Promise<OnboardingResolution> {
  if (assessment.tiedCandidates.length > 1) {
    return pickAmongProjects(request.projectName ?? pccProject.name, assessment.tiedCandidates)
  }

  const segment = await lookups.findCdpSegmentByPccProject(pccProject.projectId)
  if (!pccProject.isLeaf && !segment) {
    return ambiguous(
      `${pccProject.name} groups several projects, I need the name of the project itself`,
      [pccProject],
    )
  }

  if (request.declaredLf === false) {
    return lfConflict(request, pccProject)
  }

  if (!segment) {
    return { kind: 'lf_not_in_cdp', pccProject }
  }

  const chosen = hasDuplicateSegments(segment) ? chooseSegment(segment.duplicates, hints) : segment
  if (!chosen) {
    return pickAmongSegments(pccProject, (segment as IDuplicateCdpSegments).duplicates)
  }

  return {
    kind: 'lf_in_cdp',
    pccProject,
    segment: chosen,
    action: INTEGRATION_ACTIONS[chosen.integration],
  }
}

function hintedCandidate(hints: IRequestHints): IPccCandidate | null {
  return hints.pccProject ? { ...hints.pccProject, score: 1 } : null
}

function withHints(
  request: IParsedOnboardingRequest,
  hints: IRequestHints,
): IParsedOnboardingRequest {
  return hints.declaredLf === undefined ? request : { ...request, declaredLf: hints.declaredLf }
}

function extractGithubOwners(githubRepoUrls: string[]): string[] {
  const owners = githubRepoUrls
    .map((url) => canonicalizeRepoUrl(url)?.owner ?? null)
    .filter((owner): owner is string => owner !== null)

  return [...new Set(owners)]
}

async function areAllRepositoriesTracked(
  request: IParsedOnboardingRequest,
  lookups: IOnboardingRequestLookups,
): Promise<boolean> {
  if (!lookups.findTrackedRepoUrls) {
    return false
  }

  const tracked = await lookups.findTrackedRepoUrls(request.githubRepoUrls)

  return request.githubRepoUrls.every((url) => tracked.has(url))
}

async function resolveByOwnerHeuristic(
  request: IParsedOnboardingRequest,
  lookups: IOnboardingRequestLookups,
): Promise<OnboardingResolution | null> {
  const owners = extractGithubOwners(request.githubRepoUrls)
  if (owners.length === 0 || !lookups.findExclusivelyLfOwners) {
    return null
  }

  const lfOwners = await lookups.findExclusivelyLfOwners(owners)
  if (!owners.every((owner) => lfOwners.has(owner))) {
    return null
  }

  return { kind: 'lf_owner_in_cdp', owners }
}

async function resolveWithoutPccMatch(
  request: IParsedOnboardingRequest,
  lookups: IOnboardingRequestLookups,
  pccConfigured: boolean,
  canAsk: boolean,
): Promise<OnboardingResolution> {
  const byOwner = await resolveByOwnerHeuristic(request, lookups)
  if (byOwner) {
    return byOwner
  }

  if (!pccConfigured) {
    throw new OnboardingConfigurationError(
      'the foundation project lookup is not set up on our side',
    )
  }

  if (request.declaredLf) {
    return request.projectName
      ? { kind: 'lf_not_in_pcc', projectName: request.projectName }
      : ambiguous("I couldn't tell which project this repository belongs to")
  }

  if (request.declaredLf === null && request.projectName && canAsk) {
    return lfConfirm(request.projectName)
  }

  return { kind: 'non_lf_new_project', projectName: request.projectName }
}

export async function resolveOnboardingRequest(
  original: IParsedOnboardingRequest,
  lookups: IOnboardingRequestLookups,
  hints: IRequestHints = {},
  { canAsk = false }: IResolveOptions = {},
): Promise<OnboardingResolution> {
  const request = withHints(original, hints)
  if (hasNoRepositories(request)) {
    return ambiguous(
      request.linksToFollow.length > 0
        ? "repositories would have to be read from linked pages, which I don't open"
        : "the request doesn't contain any repository",
    )
  }

  if (hasOnlyNonGithubRepositories(request)) {
    return { kind: 'non_github_source', nonGithubRepoUrls: request.nonGithubRepoUrls }
  }

  if (await areAllRepositoriesTracked(request, lookups)) {
    return { kind: 'already_tracked', repoUrls: request.githubRepoUrls }
  }

  if (request.asksAboutHierarchy) {
    return ambiguous(
      "that's a question about which foundation the project belongs to, which I can't decide",
    )
  }

  if (hints.treatAsNew) {
    return { kind: 'non_lf_new_project', projectName: request.projectName }
  }

  const hinted = hintedCandidate(hints)
  if (hinted) {
    const assessment = assessPccCandidates(hinted.name, [hinted])
    return resolveStrongMatch(request, assessment, hinted, lookups, hints)
  }

  const { projectName } = request
  const pccConfigured = lookups.findPccCandidates !== undefined
  if (!projectName || !lookups.findPccCandidates) {
    return resolveWithoutPccMatch(request, lookups, pccConfigured, canAsk)
  }

  const assessment = assessPccCandidates(projectName, await lookups.findPccCandidates(projectName))

  if (assessment.best && (assessment.level === 'exact' || assessment.level === 'strong')) {
    return resolveStrongMatch(request, assessment, assessment.best, lookups, hints)
  }

  if (assessment.level === 'weak' && assessment.best) {
    return didYouMean(projectName, assessment.best)
  }

  return resolveWithoutPccMatch(request, lookups, pccConfigured, canAsk)
}

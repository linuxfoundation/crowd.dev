import { getErrorMessage } from '@crowd/common'
import { IRequestHints } from '@crowd/data-access-layer/src/project-catalog/types'

import {
  ClassificationNode,
  IClassificationTrace,
  createClassificationTrace,
  toClassificationNode,
  traceLookups,
} from './classificationTrace'
import {
  IParsedOnboardingRequest,
  OnboardingRequestLlm,
  parseOnboardingRequest,
} from './requestParser'
import {
  IOnboardingRequestLookups,
  IResolveOptions,
  OnboardingResolution,
  resolveOnboardingRequest,
} from './requestResolver'

export interface IRequestClassificationDeps {
  queryLlm: OnboardingRequestLlm
  lookups: IOnboardingRequestLookups
}

export interface IRequestClassification {
  resolution: OnboardingResolution
  node: ClassificationNode
  trace: IClassificationTrace
}

export type ParsedRequestSource = () => Promise<
  { ok: true; request: IParsedOnboardingRequest } | { ok: false; reason: string }
>

export const PARSE_FAILURE_REASON = "I couldn't make sense of the request"
export const CLASSIFICATION_FAILURE_REASON =
  'something went wrong on our side while checking the request'

export function unresolved(reason: string): OnboardingResolution {
  return { kind: 'ambiguous', reason, candidates: [] }
}

export async function resolveParsedRequest(
  parse: ParsedRequestSource,
  lookups: IOnboardingRequestLookups,
  trace: IClassificationTrace,
  hints: IRequestHints = {},
  options: IResolveOptions = {},
): Promise<OnboardingResolution> {
  const parsed = await parse()
  if (parsed.ok === false) {
    trace.failure = { stage: 'parse', reason: parsed.reason }
    return unresolved(PARSE_FAILURE_REASON)
  }

  trace.parsed = parsed.request
  return resolveOnboardingRequest(parsed.request, traceLookups(lookups, trace), hints, options)
}

export async function resolveWithTrace(
  parse: ParsedRequestSource,
  lookups: IOnboardingRequestLookups,
  trace: IClassificationTrace,
): Promise<OnboardingResolution> {
  try {
    return await resolveParsedRequest(parse, lookups, trace)
  } catch (err) {
    const reason = getErrorMessage(err)
    trace.failure = { stage: 'resolve', reason }
    return unresolved(CLASSIFICATION_FAILURE_REASON)
  }
}

export async function classifyOnboardingRequest(
  requestText: string,
  deps: IRequestClassificationDeps,
): Promise<IRequestClassification> {
  const trace = createClassificationTrace()
  const resolution = await resolveWithTrace(
    () => parseOnboardingRequest(requestText, deps.queryLlm),
    deps.lookups,
    trace,
  )

  return { resolution, node: toClassificationNode(resolution), trace }
}

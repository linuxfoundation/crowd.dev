import { getErrorMessage } from '@crowd/common'

import {
  ClassificationNode,
  IClassificationTrace,
  createClassificationTrace,
  toClassificationNode,
  traceLookups,
} from './classificationTrace'
import { OnboardingRequestLlm, parseOnboardingRequest } from './requestParser'
import {
  IOnboardingRequestLookups,
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

function unresolved(reason: string): OnboardingResolution {
  return { kind: 'ambiguous', reason, candidates: [] }
}

async function resolveRequestText(
  requestText: string,
  deps: IRequestClassificationDeps,
  trace: IClassificationTrace,
): Promise<OnboardingResolution> {
  try {
    const parsed = await parseOnboardingRequest(requestText, deps.queryLlm)
    if (parsed.ok === false) {
      trace.failure = { stage: 'parse', reason: parsed.reason }
      return unresolved(`Request could not be parsed: ${parsed.reason}`)
    }

    trace.parsed = parsed.request
    return await resolveOnboardingRequest(parsed.request, traceLookups(deps.lookups, trace))
  } catch (err) {
    const reason = getErrorMessage(err)
    trace.failure = { stage: 'resolve', reason }
    return unresolved(`Classification failed: ${reason}`)
  }
}

export async function classifyOnboardingRequest(
  requestText: string,
  deps: IRequestClassificationDeps,
): Promise<IRequestClassification> {
  const trace = createClassificationTrace()
  const resolution = await resolveRequestText(requestText, deps, trace)

  return { resolution, node: toClassificationNode(resolution), trace }
}

import { describe, expect, it, vi } from 'vitest'

import { classifyOnboardingRequest } from './classifyRequest'
import { IPccCandidate } from './requestResolver'

const REQUEST_TEXT = 'Please onboard https://github.com/acme/tool'

const PCC_CANDIDATE: IPccCandidate = {
  projectId: 'pcc-1',
  name: 'Tool',
  slug: 'tool',
  score: 1,
  isLeaf: true,
}

function llmAnswering(overrides: Record<string, unknown> = {}) {
  return vi.fn().mockResolvedValue(
    JSON.stringify({
      repoUrls: ['https://github.com/acme/tool'],
      linkUrls: [],
      projectName: 'Tool',
      declaredLf: null,
      asksAboutHierarchy: false,
      ...overrides,
    }),
  )
}

function lookupsFinding(candidates: IPccCandidate[]) {
  return {
    findPccCandidates: vi.fn().mockResolvedValue(candidates),
    findCdpSegmentByPccProject: vi.fn().mockResolvedValue(null),
  }
}

describe('classifyOnboardingRequest', () => {
  it('returns the node and the populated trace for a classified request', async () => {
    const lookups = lookupsFinding([])

    const result = await classifyOnboardingRequest(REQUEST_TEXT, {
      queryLlm: llmAnswering(),
      lookups,
    })

    expect(result.resolution).toEqual({ kind: 'non_lf_new_project', projectName: 'Tool' })
    expect(result.node).toBe('non_lf_create_in_external_group')
    expect(result.trace).toMatchObject({
      parsed: { projectName: 'Tool', githubRepoUrls: ['https://github.com/acme/tool'] },
      pccLookup: { projectName: 'Tool', candidates: [] },
      failure: null,
    })
  })

  it('records the CDP lookup when a PCC project matches', async () => {
    const lookups = lookupsFinding([PCC_CANDIDATE])

    const result = await classifyOnboardingRequest(REQUEST_TEXT, {
      queryLlm: llmAnswering(),
      lookups,
    })

    expect(result.node).toBe('lf_in_pcc_not_in_cdp_human_review')
    expect(lookups.findCdpSegmentByPccProject).toHaveBeenCalledWith('pcc-1')
    expect(result.trace.cdpLookup).toEqual({ result: null })
  })

  it('is ambiguous and traces a parse failure when the LLM answer is unusable', async () => {
    const lookups = lookupsFinding([])

    const result = await classifyOnboardingRequest(REQUEST_TEXT, {
      queryLlm: vi.fn().mockResolvedValue('not json'),
      lookups,
    })

    expect(result.node).toBe('ambiguous_human_review')
    expect(result.resolution).toMatchObject({
      kind: 'ambiguous',
      reason: expect.stringContaining('Request could not be parsed'),
    })
    expect(result.trace.failure).toMatchObject({ stage: 'parse' })
    expect(result.trace.parsed).toBeNull()
    expect(lookups.findPccCandidates).not.toHaveBeenCalled()
  })

  it('is ambiguous and traces a resolve failure when a lookup throws', async () => {
    const lookups = {
      findPccCandidates: vi.fn().mockRejectedValue(new Error('snowflake down')),
      findCdpSegmentByPccProject: vi.fn(),
    }

    const result = await classifyOnboardingRequest(REQUEST_TEXT, {
      queryLlm: llmAnswering(),
      lookups,
    })

    expect(result.node).toBe('ambiguous_human_review')
    expect(result.resolution).toMatchObject({
      kind: 'ambiguous',
      reason: 'Classification failed: snowflake down',
    })
    expect(result.trace.failure).toEqual({ stage: 'resolve', reason: 'snowflake down' })
    expect(result.trace.parsed).toMatchObject({ projectName: 'Tool' })
  })
})
